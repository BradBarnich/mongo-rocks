/**
*    Copyright (C) 2014 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "rocks_recovery_unit.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "mongo/base/checked_cast.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/util/log.h"
#include "mongo/util/debugger.h"

#include "rocks_transaction.h"
#include "rocks_util.h"

#include "rocks_snapshot_manager.h"

namespace mongo {
    namespace {
        // SnapshotIds need to be globally unique, as they are used in a WorkingSetMember to
        // determine if documents changed, but a different recovery unit may be used across a getMore,
        // so there is a chance the snapshot ID will be reused.
        AtomicWord<uint64_t> nextSnapshotId{1};

        logger::LogSeverity kSlowTransactionSeverity = logger::LogSeverity::Debug(1);

        class PrefixStrippingIterator : public RocksIterator {
        public:
            // baseIterator is consumed
            PrefixStrippingIterator(std::string prefix, Iterator* baseIterator,
                                    RocksCompactionScheduler* compactionScheduler,
                                    std::unique_ptr<rocksdb::Slice> upperBound,
                                    std::string timestamp, std::unique_ptr<rocksdb::Slice> timestampSlice )
                : _rocksdbSkippedDeletionsInitial(0),
                  _prefix(std::move(prefix)),
                  _nextPrefix(rocksGetNextPrefix(_prefix)),
                  _prefixSlice(_prefix),
                  _prefixEpsilon(_prefix),
                  _baseIterator(baseIterator),
                  _compactionScheduler(compactionScheduler),
                  _upperBound(std::move(upperBound)),
                  _timestamp(std::move(timestamp)),
                  _timestampSlice(std::move(timestampSlice)) {
                
                _prefixEpsilon.append(1, '\0');
                _prefixEpsilon.append(sizeof(uint64_t), '\xff');
                _prefixSliceEpsilon = rocksdb::Slice(_prefixEpsilon);
                _nextPrefix.append(sizeof(uint64_t), '\xff'); 
                *_upperBound.get() = rocksdb::Slice(_nextPrefix);
                *_timestampSlice.get() = rocksdb::Slice(_timestamp);
            }
            ~PrefixStrippingIterator() {}

            virtual bool Valid() const {
                return _baseIterator->Valid() && _baseIterator->key().starts_with(_prefixSlice) &&
                       _baseIterator->key().size() > _prefixSlice.size() + sizeof(uint64_t);
            }

            virtual void SeekToFirst() {
                startOp();

                // seek to first key bigger than prefix
                _baseIterator->Seek(_prefixSliceEpsilon);
                endOp();
            }
            virtual void SeekToLast() {
                startOp();
                // we can't have upper bound set to _nextPrefix since we need to seek to it
                *_upperBound.get() = rocksdb::Slice("\xFF\xFF\xFF\xFF\0\0\0\0\0\0\0\0", 12);
                _baseIterator->Seek(rocksdb::Slice(_nextPrefix));
                // reset back to original value
                *_upperBound.get() = rocksdb::Slice(_nextPrefix);
                if (!_baseIterator->Valid()) {
                    _baseIterator->SeekToLast();
                }
                if (_baseIterator->Valid() && !_baseIterator->key().starts_with(_prefixSlice)) {
                    _baseIterator->Prev();
                }
                endOp();
            }

            virtual void Seek(const rocksdb::Slice& target) {
                startOp();
                std::unique_ptr<char[]> buffer(new char[_prefix.size() + target.size() + sizeof(uint64_t)]);
                memcpy(buffer.get(), _prefix.data(), _prefix.size());
                memcpy(buffer.get() + _prefix.size(), target.data(), target.size());
                memset(buffer.get() + _prefix.size() + target.size(), 0xff, sizeof(uint64_t));
                _baseIterator->Seek(rocksdb::Slice(buffer.get(), _prefix.size() + target.size() + sizeof(uint64_t)));
                endOp();
            }

            virtual void Next() {
                startOp();
                _baseIterator->Next();
                endOp();
            }

            virtual void Prev() {
                startOp();
                _baseIterator->Prev();
                endOp();
            }

            virtual void SeekForPrev(const rocksdb::Slice& target) {
              // noop since we don't use it and it's only available in
              // RocksDB 4.12 and higher
            }

            virtual rocksdb::Slice key() const {
                rocksdb::Slice strippedKey = _baseIterator->key();
                strippedKey.remove_prefix(_prefix.size());
                return StripTimestampFromUserKey(strippedKey);
            }
            virtual rocksdb::Slice value() const { return _baseIterator->value(); }
            virtual rocksdb::Status status() const { return _baseIterator->status(); }

            // RocksIterator specific functions

            // This Seek is specific because it will succeed only if it finds a key with `target`
            // prefix. If there is no such key, it will be !Valid()
            virtual void SeekPrefix(const rocksdb::Slice& target) {
                std::unique_ptr<char[]> buffer(new char[_prefix.size() + target.size() + sizeof(uint64_t)]);
                memcpy(buffer.get(), _prefix.data(), _prefix.size());
                memcpy(buffer.get() + _prefix.size(), target.data(), target.size());
                memset(buffer.get() + _prefix.size() + target.size(), 0xff, sizeof(uint64_t));

                std::string tempUpperBound = rocksGetNextPrefix(
                    rocksdb::Slice(buffer.get(), _prefix.size() + target.size()));
                tempUpperBound.append(sizeof(uint64_t), '\xff');

                *_upperBound.get() = rocksdb::Slice(tempUpperBound);
                if (target.size() == 0) {
                    // if target is empty, we'll try to seek to <prefix>, which is not good
                    _baseIterator->Seek(_prefixSliceEpsilon);
                } else {
                    _baseIterator->Seek(
                        rocksdb::Slice(buffer.get(), _prefix.size() + target.size() + sizeof(uint64_t)));
                }
                // reset back to original value
                *_upperBound.get() = rocksdb::Slice(_nextPrefix);
            }

        private:
            void startOp() {
                if (_compactionScheduler == nullptr) {
                    return;
                }
                if (rocksdb::GetPerfLevel() == rocksdb::PerfLevel::kDisable) {
                    rocksdb::SetPerfLevel(rocksdb::kEnableCount);
                }
                _rocksdbSkippedDeletionsInitial = get_internal_delete_skipped_count();
            }
            void endOp() {
                if (_compactionScheduler == nullptr) {
                    return;
                }
                int skippedDeletionsOp = get_internal_delete_skipped_count() -
                                         _rocksdbSkippedDeletionsInitial;
                if (skippedDeletionsOp >=
                    RocksCompactionScheduler::getSkippedDeletionsThreshold()) {
                    _compactionScheduler->reportSkippedDeletionsAboveThreshold(_prefix);
                }
            }

            int _rocksdbSkippedDeletionsInitial;

            std::string _prefix;
            std::string _nextPrefix;
            rocksdb::Slice _prefixSlice;
            // the first possible key bigger than prefix. we use this for SeekToFirst()
            std::string _prefixEpsilon;
            rocksdb::Slice _prefixSliceEpsilon;
            std::unique_ptr<Iterator> _baseIterator;

            // can be nullptr
            RocksCompactionScheduler* _compactionScheduler;  // not owned

            std::unique_ptr<rocksdb::Slice> _upperBound;

            std::string _timestamp;
            std::unique_ptr<rocksdb::Slice> _timestampSlice;
        };

    }  // anonymous namespace

    std::atomic<int> RocksRecoveryUnit::_totalLiveRecoveryUnits(0);

    RocksRecoveryUnit::RocksRecoveryUnit(RocksTransactionEngine* transactionEngine,
                                         RocksSnapshotManager* snapshotManager, rocksdb::DB* db,
                                         RocksCounterManager* counterManager,
                                         RocksCompactionScheduler* compactionScheduler,
                                         RocksDurabilityManager* durabilityManager,
                                         bool durable)
        : _transactionEngine(transactionEngine),
          _snapshotManager(snapshotManager),
          _db(db),
          _counterManager(counterManager),
          _compactionScheduler(compactionScheduler),
          _durabilityManager(durabilityManager),
          _durable(durable),
          _transaction(transactionEngine),
          _writeBatch(TimestampComparator(), 0, true, 0, sizeof(uint64_t) ),
          _snapshot(nullptr),
          _preparedSnapshot(nullptr),
          _mySnapshotId(nextSnapshotId.fetchAndAdd(1)) {
        RocksRecoveryUnit::_totalLiveRecoveryUnits.fetch_add(1, std::memory_order_relaxed);
    }

    RocksRecoveryUnit::~RocksRecoveryUnit() {
        if (_preparedSnapshot != nullptr) {
            // somebody didn't call getPreparedSnapshot() after prepareForCreateSnapshot()
            _db->ReleaseSnapshot(_preparedSnapshot);
            _preparedSnapshot = nullptr;
        }
        _abort();
        RocksRecoveryUnit::_totalLiveRecoveryUnits.fetch_sub(1, std::memory_order_relaxed);
    }

    void RocksRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
        invariant(!_areWriteUnitOfWorksBanned);
    }

    void RocksRecoveryUnit::commitUnitOfWork() {
        _commit();

        if(_lastTimestampSet)
        {
            log() << "commit: " << _lastTimestampSet;
        } else {
           log() << "no commit timestamp";
        }

        try {
            for (Changes::const_iterator it = _changes.begin(), end = _changes.end(); it != end;
                    ++it) {
                (*it)->commit(_lastTimestampSet);
            }
            _changes.clear();
        }
        catch (...) {
            std::terminate();
        }
        
        _lastTimestampSet = boost::none;
        _releaseSnapshot();
    }

    void RocksRecoveryUnit::abortUnitOfWork() {
        _abort();
    }

    bool RocksRecoveryUnit::waitUntilDurable() {
        _durabilityManager->waitUntilDurable(false);
        return true;
    }

    void RocksRecoveryUnit::abandonSnapshot() {
        _deltaCounters.clear();
        _writeBatch.Clear();
        _timestamps.clear();
        _releaseSnapshot();
        _areWriteUnitOfWorksBanned = false;
    }

    void RocksRecoveryUnit::Put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
        //log() << "put: " << key.ToString(true) << ", timestamp: " << _lastTimestampSet.value_or(Timestamp());
        invariantRocksOK(_writeBatch.Put(key, value));
        _timestamps.push_back(encodeTimestamp(_lastTimestampSet.value_or(Timestamp()).asULL()));
    }

    void RocksRecoveryUnit::Put(const rocksdb::Slice& key, const rocksdb::Slice& value, const Timestamp timestamp) {
        //log() << "put: " << key.ToString(true) << ", timestamp: " << timestamp;
        invariantRocksOK(_writeBatch.Put(key, value));
        _timestamps.push_back(encodeTimestamp(timestamp.asULL()));
    }

    void RocksRecoveryUnit::Delete(const rocksdb::Slice& key) {
        //log() << "delete: " << key.ToString(true) << ", timestamp: " << _lastTimestampSet.value_or(Timestamp());
        invariantRocksOK(_writeBatch.Delete(key));
        _timestamps.push_back(encodeTimestamp(_lastTimestampSet.value_or(Timestamp()).asULL()));
    }

    void RocksRecoveryUnit::DeleteRange(const rocksdb::Slice& begin_key, const rocksdb::Slice& end_key) {
        //log() << "delete range: " << begin_key.ToString(true) << ", timestamp: " << _lastTimestampSet.value_or(Timestamp());
        invariantRocksOK(_writeBatch.DeleteRange(begin_key, end_key));
        _timestamps.push_back(encodeTimestamp(_lastTimestampSet.value_or(Timestamp()).asULL()));
    }

    void RocksRecoveryUnit::SingleDelete(const rocksdb::Slice& key) {
        invariantRocksOK(_writeBatch.SingleDelete(key));
        _timestamps.push_back(encodeTimestamp(_lastTimestampSet.value_or(Timestamp()).asULL()));
    }

    void RocksRecoveryUnit::TruncatePrefix(std::string prefix) {
        std::string beginKey(prefix);
        //beginKey.append(sizeof(uint64_t), '\xff');

        std::string endKey(rocksGetNextPrefix(prefix));
        //endKey.append(sizeof(uint64_t), '\xff');

        rocksdb::Range prefixRange(beginKey, endKey);
        log() << "delete range: " << prefixRange.start.ToString(true) << " - " << prefixRange.limit.ToString(true);

        invariantRocksOK(_writeBatch.DeleteRange(prefixRange.start, prefixRange.limit));
        _timestamps.push_back(encodeTimestamp(UINT64_MAX));

        // return _db->DeleteRange(
        //     writeOptions,
        //     _db->DefaultColumnFamily(),
        //     prefixRange.start, prefixRange.limit);
    }

    void RocksRecoveryUnit::setOplogReadTill(const RecordId& record) { _oplogReadTill = record; }

    void RocksRecoveryUnit::registerChange(Change* change) { _changes.push_back(change); }

    Status RocksRecoveryUnit::obtainMajorityCommittedSnapshot() {
        invariant(_timestampReadSource == ReadSource::kMajorityCommitted);

        if (!_snapshotManager->haveCommittedSnapshot()) {
            return {ErrorCodes::ReadConcernMajorityNotAvailableYet,
                    "Read concern majority reads are currently not possible."};
        }
        invariant(_snapshot == nullptr);

        _readFromMajorityCommittedSnapshot = _snapshotManager->getCommittedSnapshot();
        return Status::OK();
    }

    boost::optional<Timestamp> RocksRecoveryUnit::getPointInTimeReadTimestamp() {
        // After a ReadSource has been set on this RecoveryUnit, callers expect that this method returns
        // the read timestamp that will be used for current or future transactions. Because callers use
        // this timestamp to inform visiblity of operations, it is therefore necessary to open a
        // transaction to establish a read timestamp, but only for ReadSources that are expected to have
        // read timestamps.
        switch (_timestampReadSource) {
            case ReadSource::kUnset:
            case ReadSource::kNoTimestamp:
                return boost::none;
            case ReadSource::kMajorityCommitted:
                // This ReadSource depends on a previous call to obtainMajorityCommittedSnapshot() and
                // does not require an open transaction to return a valid timestamp.
                if(!_readFromMajorityCommittedSnapshot)
                {
                    uassert(ErrorCodes::ReadConcernMajorityNotAvailableYet,
                        "Committed view disappeared while running operation",
                        _snapshotManager->haveCommittedSnapshot());
                        
                    return _snapshotManager->getCommittedSnapshot();
                }
                return *_readFromMajorityCommittedSnapshot;
            case ReadSource::kProvided:
                // The read timestamp is set by the user and does not require a transaction to be open.
                invariant(!_readAtTimestamp.isNull());
                return _readAtTimestamp;
            case ReadSource::kLastApplied:
                return _snapshotManager->getLocalSnapshot();
            case ReadSource::kNoOverlap:
            case ReadSource::kAllDurableSnapshot:
                MONGO_UNREACHABLE;
        }
    }

    SnapshotId RocksRecoveryUnit::getSnapshotId() const { return SnapshotId(_mySnapshotId); }

    void RocksRecoveryUnit::_releaseSnapshot() {
        if (_timer) {
            const int transactionTime = _timer->millis();
            _timer.reset();
            if (transactionTime >= serverGlobalParams.slowMS) {
                LOG(kSlowTransactionSeverity) << "Slow transaction. Lifetime of SnapshotId "
                                              << _mySnapshotId << " was " << transactionTime
                                              << " ms";
            }
        }

        if (_snapshot) {
            _transaction.abort();
            _db->ReleaseSnapshot(_snapshot);
            _snapshot = nullptr;
        }
        _snapshotHolder.reset();
        _readFromMajorityCommittedSnapshot.reset();

        _mySnapshotId = nextSnapshotId.fetchAndAdd(1);
    }

    void RocksRecoveryUnit::_commit() {
        rocksdb::WriteBatch* wb = _writeBatch.GetWriteBatch();
        
        std::vector<rocksdb::Slice> timestampSlices;;
        for (const auto& ts : _timestamps) {
            timestampSlices.emplace_back(ts);
        }

        wb->AssignTimestamps(timestampSlices);
        for (auto pair : _deltaCounters) {
            auto& counter = pair.second;
            counter._value->fetch_add(counter._delta);
            long long newValue = counter._value->load();
            _counterManager->updateCounter(pair.first, newValue, wb);
        }

        if (wb->Count() != 0) {
            // Order of operations here is important. It needs to be synchronized with
            // _transaction.recordSnapshotId() and _db->GetSnapshot() and
            rocksdb::WriteOptions writeOptions;
            writeOptions.disableWAL = !_durable;
            auto status = _db->Write(writeOptions, wb);
            invariantRocksOK(status);
            _transaction.commit();
        }
        _deltaCounters.clear();
        _writeBatch.Clear();
        _timestamps.clear();
        
        //_db->Flush(rocksdb::FlushOptions());
    }

    void RocksRecoveryUnit::_abort() {
        try {
            for (Changes::const_reverse_iterator it = _changes.rbegin(), end = _changes.rend();
                    it != end; ++it) {
                Change* change = *it;
                LOG(2) << "CUSTOM ROLLBACK " << redact(demangleName(typeid(*change)));
                change->rollback();
            }
            _changes.clear();
        }
        catch (...) {
            std::terminate();
        }

        _deltaCounters.clear();
        _writeBatch.Clear();
        _timestamps.clear();

        _releaseSnapshot();
    }

    const rocksdb::Snapshot* RocksRecoveryUnit::getPreparedSnapshot() {
        auto ret = _preparedSnapshot;
        _preparedSnapshot = nullptr;
        return ret;
    }

    void RocksRecoveryUnit::dbReleaseSnapshot(const rocksdb::Snapshot* snapshot) {
        _db->ReleaseSnapshot(snapshot);
    }

    const rocksdb::Snapshot* RocksRecoveryUnit::snapshot() {
        // Only start a timer for transaction's lifetime if we're going to log it.
        if (shouldLog(kSlowTransactionSeverity)) {
            _timer.reset(new Timer());
        }

        if (!_snapshot) {
            // RecoveryUnit might be used for writing, so we need to call recordSnapshotId().
            // Order of operations here is important. It needs to be synchronized with
            // _db->Write() and _transaction.commit()
            _transaction.recordSnapshotId();
            _snapshot = _db->GetSnapshot();
        }
        return _snapshot;
    }

    rocksdb::Status RocksRecoveryUnit::Get(const rocksdb::Slice& key, std::string* value) {
        if (_writeBatch.GetWriteBatch()->Count() > 0) {
            std::unique_ptr<rocksdb::WBWIIterator> wb_iterator(_writeBatch.NewIterator());
            auto keyString = std::string(key.data(), key.size());
            keyString.append(sizeof(uint64_t), '\0');
            auto keyWithTimestamp = rocksdb::Slice(keyString);
            wb_iterator->SeekToLast();
            wb_iterator->SeekForPrev(keyWithTimestamp);
            if (wb_iterator->Valid() && wb_iterator->Entry().key == keyWithTimestamp) {
                const auto& entry = wb_iterator->Entry();
                if (entry.type == rocksdb::WriteType::kDeleteRecord) {
                    return rocksdb::Status::NotFound();
                }
                *value = std::string(entry.value.data(), entry.value.size());
                return rocksdb::Status::OK();
            }
        }
        rocksdb::ReadOptions options;
        options.snapshot = snapshot();
        //log() << "Get at snapshot" << options.snapshot->GetSequenceNumber();
        std::string timestamp = getReadTimestamp();
        rocksdb::Slice readTimestamp(timestamp);
        options.timestamp = &readTimestamp;
        
        return _db->Get(options, key, value);
    }

    RocksIterator* RocksRecoveryUnit::NewIterator(std::string prefix, bool isOplog) {
        std::unique_ptr<rocksdb::Slice> upperBound(new rocksdb::Slice());
        std::unique_ptr<rocksdb::Slice> timestampSlice(new rocksdb::Slice());
        std::string timestamp = getReadTimestamp();
        rocksdb::ReadOptions options;
        options.iterate_upper_bound = upperBound.get();
        options.snapshot = snapshot();
        options.timestamp = timestampSlice.get(); 
        auto iterator = _writeBatch.NewIteratorWithBase(_db->NewIterator(options));
        auto prefixIterator = new PrefixStrippingIterator(std::move(prefix), iterator,
                                                          isOplog ? nullptr : _compactionScheduler,
                                                          std::move(upperBound),
                                                          std::move(timestamp), std::move(timestampSlice));
        return prefixIterator;
    }

    RocksIterator* RocksRecoveryUnit::NewIteratorNoSnapshot(rocksdb::DB* db, std::string prefix) {
        std::unique_ptr<rocksdb::Slice> upperBound(new rocksdb::Slice());
        std::unique_ptr<rocksdb::Slice> timestampSlice(new rocksdb::Slice());
        std::string timestamp(encodeTimestamp(ULLONG_MAX));
        rocksdb::ReadOptions options;
        options.iterate_upper_bound = upperBound.get();
        options.timestamp = timestampSlice.get();
        auto iterator = db->NewIterator(options);
        return new PrefixStrippingIterator(std::move(prefix), iterator, nullptr,
                                           std::move(upperBound),
                                           std::move(timestamp), std::move(timestampSlice));
    }

    void RocksRecoveryUnit::incrementCounter(const rocksdb::Slice& counterKey,
                                             std::atomic<long long>* counter, long long delta) {
        if (delta == 0) {
            return;
        }

        auto pair = _deltaCounters.find(counterKey.ToString());
        if (pair == _deltaCounters.end()) {
            _deltaCounters[counterKey.ToString()] =
                mongo::RocksRecoveryUnit::Counter(counter, delta);
        } else {
            pair->second._delta += delta;
        }
    }

    long long RocksRecoveryUnit::getDeltaCounter(const rocksdb::Slice& counterKey) {
        auto counter = _deltaCounters.find(counterKey.ToString());
        if (counter == _deltaCounters.end()) {
            return 0;
        } else {
            return counter->second._delta;
        }
    }

    void RocksRecoveryUnit::resetDeltaCounters() {
        _deltaCounters.clear();
    }

    RocksRecoveryUnit* RocksRecoveryUnit::getRocksRecoveryUnit(OperationContext* opCtx) {
        return checked_cast<RocksRecoveryUnit*>(opCtx->recoveryUnit());
    }

    const rocksdb::Comparator* TimestampComparator() {
      static TimestampComparatorImpl timestamp;
      return &timestamp;
    }

    Status RocksRecoveryUnit::setTimestamp(Timestamp timestamp) {
        LOG(3) << "RocksDb set timestamp of future write operations to " << timestamp;
        invariant(_prepareTimestamp.isNull());
        invariant(_commitTimestamp.isNull(),
                str::stream() << "Commit timestamp set to " << _commitTimestamp.toString()
                                << " and trying to set WUOW timestamp to "
                                << timestamp.toString());
        invariant(_readAtTimestamp.isNull() || timestamp >= _readAtTimestamp,
                str::stream() << "future commit timestamp " << timestamp.toString()
                                << " cannot be older than read timestamp "
                                << _readAtTimestamp.toString());

        _lastTimestampSet = timestamp;

        return Status::OK();
    }

    void RocksRecoveryUnit::setCommitTimestamp(Timestamp timestamp) {
        // This can be called either outside of a WriteUnitOfWork or in a prepared transaction after
        // setPrepareTimestamp() is called. Prepared transactions ensure the correct timestamping
        // semantics and the set-once commitTimestamp behavior is exactly what prepared transactions
        // want.
        invariant(_commitTimestamp.isNull(),
                str::stream() << "Commit timestamp set to " << _commitTimestamp.toString()
                                << " and trying to set it to "
                                << timestamp.toString());
        invariant(!_lastTimestampSet,
                str::stream() << "Last timestamp set is " << _lastTimestampSet->toString()
                                << " and trying to set commit timestamp to "
                                << timestamp.toString());

        _commitTimestamp = timestamp;
    }

    Timestamp RocksRecoveryUnit::getCommitTimestamp() const {
        return _commitTimestamp;
    }

    void RocksRecoveryUnit::setDurableTimestamp(Timestamp timestamp) {
        invariant(
            _durableTimestamp.isNull(),
            str::stream() << "Trying to reset durable timestamp when it was already set. wasSetTo: "
                        << _durableTimestamp.toString()
                        << " setTo: "
                        << timestamp.toString());

        _durableTimestamp = timestamp;
    }

    Timestamp RocksRecoveryUnit::getDurableTimestamp() const {
        return _durableTimestamp;
    }

    void RocksRecoveryUnit::clearCommitTimestamp() {
        invariant(!_commitTimestamp.isNull());
        invariant(!_lastTimestampSet,
                str::stream() << "Last timestamp set is " << _lastTimestampSet->toString()
                                << " and trying to clear commit timestamp.");

        _commitTimestamp = Timestamp();
    }

    void RocksRecoveryUnit::setPrepareTimestamp(Timestamp timestamp) {
        invariant(_prepareTimestamp.isNull(),
                str::stream() << "Trying to set prepare timestamp to " << timestamp.toString()
                                << ". It's already set to "
                                << _prepareTimestamp.toString());
        invariant(_commitTimestamp.isNull(),
                str::stream() << "Commit timestamp is " << _commitTimestamp.toString()
                                << " and trying to set prepare timestamp to "
                                << timestamp.toString());
        invariant(!_lastTimestampSet,
                str::stream() << "Last timestamp set is " << _lastTimestampSet->toString()
                                << " and trying to set prepare timestamp to "
                                << timestamp.toString());

        _prepareTimestamp = timestamp;
    }

    Timestamp RocksRecoveryUnit::getPrepareTimestamp() const {
        invariant(!_prepareTimestamp.isNull());
        invariant(_commitTimestamp.isNull(),
                str::stream() << "Commit timestamp is " << _commitTimestamp.toString()
                                << " and trying to get prepare timestamp of "
                                << _prepareTimestamp.toString());
        invariant(!_lastTimestampSet,
                str::stream() << "Last timestamp set is " << _lastTimestampSet->toString()
                                << " and trying to get prepare timestamp of "
                                << _prepareTimestamp.toString());

        return _prepareTimestamp;
    }

    void RocksRecoveryUnit::setTimestampReadSource(ReadSource readSource,
                                                        boost::optional<Timestamp> provided) {
        LOG(3) << "setting timestamp read source: " << static_cast<int>(readSource)
            << ", provided timestamp: " << ((provided) ? provided->toString() : "none");

        invariant(!provided == (readSource != ReadSource::kProvided));
        invariant(!(provided && provided->isNull()));

        _timestampReadSource = readSource;
        _readAtTimestamp = (provided) ? *provided : Timestamp();
    }

    RecoveryUnit::ReadSource RocksRecoveryUnit::getTimestampReadSource() const {
        return _timestampReadSource;
    }

    std::string RocksRecoveryUnit::getReadTimestamp() {
        auto timestamp = getPointInTimeReadTimestamp();
        if(!timestamp)
        {
            return encodeTimestamp(ULLONG_MAX);
        }

        return encodeTimestamp(timestamp.get().asULL());
    }
}
