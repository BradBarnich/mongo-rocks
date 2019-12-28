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

#pragma once

#include "mongo/util/debugger.h"
#include <atomic>
#include <csignal>
#include <map>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include <rocksdb/slice.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>

#include "mongo/base/owned_pointer_vector.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/util/timer.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_durability_manager.h"
#include "rocks_snapshot_manager.h"
#include "rocks_transaction.h"
#include "rocks_util.h"

namespace rocksdb {
class DB;
class Snapshot;
class WriteBatchWithIndex;
class Comparator;
class Status;
class Slice;
class Iterator;
}  // namespace rocksdb

namespace mongo {
class TimestampComparatorImpl : public rocksdb::Comparator {
private:
    const Comparator* cmp_without_ts_;

public:
    explicit TimestampComparatorImpl() : Comparator(sizeof(uint64_t)), cmp_without_ts_(nullptr) {
        cmp_without_ts_ = rocksdb::BytewiseComparator();
    }

    const char* Name() const override {
        return "TimestampComparator";
    }

    void FindShortSuccessor(std::string* key) const override {}

    void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {}

    int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
        int r = CompareWithoutTimestamp(a, b);
        if (r != 0 || 0 == timestamp_size()) {
            return r;
        }
        return CompareTimestamp(
            rocksdb::Slice(a.data() + a.size() - timestamp_size(), timestamp_size()),
            rocksdb::Slice(b.data() + b.size() - timestamp_size(), timestamp_size()));
    }

    int CompareWithoutTimestamp(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
        // if(a.size() > b.size() && a.size() - b.size() == 8 && b[b.size()-1] != '\0' &&
        // b[b.size()-1] != '\xff' ) {
        //     //breakpoint();
        // }
        if (b.size() > a.size() && b.size() - a.size() == 8 && a[a.size() - 1] != '\0' &&
            a[a.size() - 1] != '\xff') {
            // breakpoint();
        }
        if (a.size() < timestamp_size() || b.size() < timestamp_size()) {
            breakpoint();
        }
        if (a.size() < timestamp_size() && b.size() < timestamp_size()) {
            return cmp_without_ts_->Compare(a, b);
        }
        if (a.size() < timestamp_size()) {
            rocksdb::Slice k2 = StripTimestampFromUserKey(b);
            return cmp_without_ts_->Compare(a, k2);
        }
        if (b.size() < timestamp_size()) {
            rocksdb::Slice k1 = StripTimestampFromUserKey(a);
            return cmp_without_ts_->Compare(k1, b);
        }
        assert(a.size() >= timestamp_size());
        assert(b.size() >= timestamp_size());
        rocksdb::Slice k1 = StripTimestampFromUserKey(a);
        rocksdb::Slice k2 = StripTimestampFromUserKey(b);

        return cmp_without_ts_->Compare(k1, k2);
    }

    int CompareTimestamp(const rocksdb::Slice& ts1, const rocksdb::Slice& ts2) const override {
        if (!ts1.data() && !ts2.data()) {
            return 0;
        } else if (ts1.data() && !ts2.data()) {
            return 1;
        } else if (!ts1.data() && ts2.data()) {
            return -1;
        }
        assert(ts1.size() == ts2.size());
        uint64_t high1 = 0;
        uint64_t high2 = 0;
        auto* ptr1 = const_cast<rocksdb::Slice*>(&ts1);
        auto* ptr2 = const_cast<rocksdb::Slice*>(&ts2);
        if (!GetFixed64(ptr1, &high1) || !GetFixed64(ptr2, &high2)) {
            assert(false);
        }
        if (high1 < high2) {
            return 1;
        } else if (high1 > high2) {
            return -1;
        }
        return 0;
    }

    int CompareKey(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
        return cmp_without_ts_->Compare(a, b);
    }
};

// Same as rocksdb::Iterator, but adds couple more useful functions
class RocksIterator : public rocksdb::Iterator {
public:
    virtual ~RocksIterator() {}

    // This Seek is specific because it will succeed only if it finds a key with `target`
    // prefix. If there is no such key, it will be !Valid()
    virtual void SeekPrefix(const rocksdb::Slice& target) = 0;
};

class OperationContext;

class RocksRecoveryUnit final : public RecoveryUnit {
    RocksRecoveryUnit(const RocksRecoveryUnit&) = delete;
    RocksRecoveryUnit& operator=(const RocksRecoveryUnit&) = delete;

public:
    RocksRecoveryUnit(RocksTransactionEngine* transactionEngine,
                      RocksSnapshotManager* snapshotManager,
                      rocksdb::DB* db,
                      RocksCounterManager* counterManager,
                      RocksCompactionScheduler* compactionScheduler,
                      RocksDurabilityManager* durabilityManager,
                      bool durable);
    ~RocksRecoveryUnit();

    void beginUnitOfWork(OperationContext* opCtx);
    // void prepareUnitOfWork() override;


    bool waitUntilDurable(OperationContext* opCtx) override;

    // bool waitUntilUnjournaledWritesDurable(OperationContext* opCtx,
    //                                        bool stableCheckpoint = true) override;

    // void preallocateSnapshot() override;

    Status obtainMajorityCommittedSnapshot() override;

    boost::optional<Timestamp> getPointInTimeReadTimestamp() override;

    Status setTimestamp(Timestamp timestamp) override;

    void setCommitTimestamp(Timestamp timestamp) override;

    void clearCommitTimestamp() override;

    Timestamp getCommitTimestamp() const override;

    void setDurableTimestamp(Timestamp timestamp) override;

    Timestamp getDurableTimestamp() const override;

    void setPrepareTimestamp(Timestamp timestamp) override;

    Timestamp getPrepareTimestamp() const override;

    // void setPrepareConflictBehavior(PrepareConflictBehavior behavior) override;

    // PrepareConflictBehavior getPrepareConflictBehavior() const override;

    // void setRoundUpPreparedTimestamps(bool value) override;

    void setTimestampReadSource(ReadSource source,
                                boost::optional<Timestamp> provided = boost::none) override;

    ReadSource getTimestampReadSource() const override;

    bool inActiveTxn() const {
        return _isActive();
    }

    void setOrderedCommit(bool orderedCommit) override{};

    // local api

    void Put(const rocksdb::Slice& key, const rocksdb::Slice& value);

    void Delete(const rocksdb::Slice& key);

    void DeleteRange(const rocksdb::Slice& begin_key, const rocksdb::Slice& end_key);

    void SingleDelete(const rocksdb::Slice& key);

    void TruncatePrefix(std::string prefix);

    const rocksdb::Snapshot* getPreparedSnapshot();
    void dbReleaseSnapshot(const rocksdb::Snapshot* snapshot);

    // Returns snapshot, creating one if needed. Considers _readFromMajorityCommittedSnapshot.
    const rocksdb::Snapshot* snapshot();

    bool hasSnapshot() {
        return _snapshot != nullptr || _snapshotHolder.get() != nullptr;
    }

    RocksTransaction* transaction() {
        return &_transaction;
    }

    rocksdb::Status Get(const rocksdb::Slice& key, std::string* value);

    RocksIterator* NewIterator(std::string prefix, bool isOplog = false);

    static RocksIterator* NewIteratorNoSnapshot(rocksdb::DB* db, std::string prefix);

    void incrementCounter(const rocksdb::Slice& counterKey,
                          std::atomic<long long>* counter,
                          long long delta);

    long long getDeltaCounter(const rocksdb::Slice& counterKey);

    void resetDeltaCounters();

    void setOplogReadTill(const RecordId& record);
    RecordId getOplogReadTill() const {
        return _oplogReadTill;
    }

    RocksRecoveryUnit* newRocksRecoveryUnit() {
        return new RocksRecoveryUnit(_transactionEngine,
                                     _snapshotManager,
                                     _db,
                                     _counterManager,
                                     _compactionScheduler,
                                     _durabilityManager,
                                     _durable);
    }

    struct Counter {
        std::atomic<long long>* _value;
        long long _delta;
        Counter() : Counter(nullptr, 0) {}
        Counter(std::atomic<long long>* value, long long delta) : _value(value), _delta(delta) {}
    };

    typedef std::unordered_map<std::string, Counter> CounterMap;

    static RocksRecoveryUnit* getRocksRecoveryUnit(OperationContext* opCtx);

    static int getTotalLiveRecoveryUnits() {
        return _totalLiveRecoveryUnits.load();
    }

    void setCommittedSnapshot(const rocksdb::Snapshot* committedSnapshot);

    rocksdb::DB* getDB() const {
        return _db;
    }

    std::string getReadTimestamp();

private:
    void doCommitUnitOfWork() override;
    void doAbortUnitOfWork() override;

    void doAbandonSnapshot() override;

    void _releaseSnapshot();

    void _commit(boost::optional<Timestamp> commitTime);

    void _abort();
    RocksTransactionEngine* _transactionEngine;      // not owned
    RocksSnapshotManager* _snapshotManager;          // not owned
    rocksdb::DB* _db;                                // not owned
    RocksCounterManager* _counterManager;            // not owned
    RocksCompactionScheduler* _compactionScheduler;  // not owned
    RocksDurabilityManager* _durabilityManager;      // not owned
    OperationContext* _opCtx;                        // not owned
    bool _isTimestamped = false;

    const bool _durable;

    RocksTransaction _transaction;

    rocksdb::WriteBatchWithIndex _writeBatch;

    // bare because we need to call ReleaseSnapshot when we're done with this
    const rocksdb::Snapshot* _snapshot;  // owned

    // snapshot that got prepared in prepareForCreateSnapshot
    // it is consumed by getPreparedSnapshot()
    const rocksdb::Snapshot* _preparedSnapshot;  // owned

    std::unique_ptr<Timer> _timer;
    CounterMap _deltaCounters;

    RecordId _oplogReadTill;

    static std::atomic<int> _totalLiveRecoveryUnits;

    // If we read from a committed snapshot, then ownership of the snapshot
    // should be shared here to ensure that it is not released early
    std::shared_ptr<RocksSnapshotManager::SnapshotHolder> _snapshotHolder;

    boost::optional<Timestamp> _readFromMajorityCommittedSnapshot;

    std::vector<std::string> _timestamps;

    Timestamp _commitTimestamp;
    Timestamp _durableTimestamp;
    Timestamp _prepareTimestamp;
    boost::optional<Timestamp> _lastTimestampSet;
    Timestamp _readAtTimestamp;

    ReadSource _timestampReadSource = ReadSource::kUnset;
};

extern const rocksdb::Comparator* TimestampComparator();
}  // namespace mongo
