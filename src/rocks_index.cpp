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

#include "rocks_index.h"

#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"

#include "rocks_engine.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"

#define TRACING_ENABLED 0

#if TRACING_ENABLED
#define TRACE_CURSOR log() << "Rocks index"
#define TRACE_INDEX log() << "Rocks index (" << (const void*)this << ") "
#else
#define TRACE_CURSOR \
    if (0)           \
    log()
#define TRACE_INDEX \
    if (0)          \
    log()
#endif

namespace mongo {

using std::string;
using std::stringstream;
using std::vector;

namespace {

// for non-unique and id indexes
static const int kDataFormatV2KeyStringV1IndexVersionV2 = 2;
// for non-id unique indexes, includes the RecordId in the key
static const int kDataFormatV3KeyStringV1UniqueIndexVersionV2 = 3;

static const int kMinimumIndexVersion = kDataFormatV2KeyStringV1IndexVersionV2;
static const int kMaximumIndexVersion = kDataFormatV3KeyStringV1UniqueIndexVersionV2;

bool hasFieldNames(const BSONObj& obj) {
    BSONForEach(e, obj) {
        if (e.fieldName()[0])
            return true;
    }
    return false;
}

BSONObj stripFieldNames(const BSONObj& query) {
    if (!hasFieldNames(query))
        return query;

    BSONObjBuilder bb;
    BSONForEach(e, query) {
        bb.appendAs(e, StringData());
    }
    return bb.obj();
}

/**
 * Functionality shared by both unique and standard index
 */
class RocksCursorBase : public SortedDataInterface::Cursor {
public:
    RocksCursorBase(const RocksIndexBase& idx,
                    OperationContext* opCtx,
                    bool forward,
                    std::string prefix)
        : _opCtx(opCtx),
          _idx(idx),
          _forward(forward),
          _key(idx.getKeyStringVersion()),
          _typeBits(idx.getKeyStringVersion()),
          _query(idx.getKeyStringVersion()),
          _prefix(prefix) {
        _currentSequenceNumber =
            RocksRecoveryUnit::getRocksRecoveryUnit(opCtx)->snapshot()->GetSequenceNumber();
    }

    boost::optional<IndexKeyEntry> next(RequestedInfo parts) override {
        if (!advanceNext()) {
            return {};
        }
        return curr(parts);
    }

    boost::optional<KeyStringEntry> nextKeyString() override {
        if (!advanceNext()) {
            return {};
        }
        if (_eof) {
            return {};
        }

        return getKeyStringEntry();
    }

    void setEndPosition(const BSONObj& key, bool inclusive) override {
        TRACE_CURSOR << "setEndPosition inclusive: " << inclusive << ' ' << key;
        if (key.isEmpty()) {
            // This means scan to end of index.
            _endPosition.reset();
            return;
        }

        // NOTE: this uses the opposite rules as a normal seek because a forward scan should
        // end after the key if inclusive and before if exclusive.
        const auto discriminator = _forward == inclusive
            ? KeyString::Discriminator::kExclusiveAfter
            : KeyString::Discriminator::kExclusiveBefore;
        _endPosition = std::make_unique<KeyString::Builder>(_idx.getKeyStringVersion());
        _endPosition->resetToKey(BSONObj::stripFieldNames(key), _idx.getOrdering(), discriminator);
    }

    boost::optional<IndexKeyEntry> seek(const KeyString::Value& keyString,
                                        RequestedInfo parts = kKeyAndLoc) override {
        seekForKeyString(keyString);
        return curr(parts);
    }

    boost::optional<KeyStringEntry> seekForKeyString(
        const KeyString::Value& keyStringValue) override {
        dassert(_opCtx->lockState()->isReadLocked());
        seekCursor(keyStringValue);

        updatePosition();
        if (_eof)
            return {};

        dassert(!atOrPastEndPointAfterSeeking());
        dassert(!_id.isNull());

        return getKeyStringEntry();
    }

    boost::optional<KeyStringEntry> seekExactForKeyString(const KeyString::Value& key) override {
        dassert(KeyString::decodeDiscriminator(
                    key.getBuffer(), key.getSize(), _idx.getOrdering(), key.getTypeBits()) ==
                KeyString::Discriminator::kInclusive);

        auto ksEntry = [&]() {
            if (_forward) {
                return seekForKeyString(key);
            }

            // Append a kExclusiveAfter discriminator if it's a reverse cursor to ensure that the
            // KeyString we construct will always be greater than the KeyString that we retrieve
            // (even when it has a RecordId).
            KeyString::Builder keyCopy(_idx.getKeyStringVersion(), _idx.getOrdering());

            // Reset by copying all but the last byte, the kEnd byte.
            keyCopy.resetFromBuffer(key.getBuffer(), key.getSize() - 1);

            // Append a different discriminator and new end byte.
            keyCopy.appendDiscriminator(KeyString::Discriminator::kExclusiveAfter);
            return seekForKeyString(keyCopy.getValueCopy());
        }();

        if (!ksEntry) {
            return {};
        }

        if (KeyString::compare(ksEntry->keyString.getBuffer(),
                               key.getBuffer(),
                               KeyString::sizeWithoutRecordIdAtEnd(ksEntry->keyString.getBuffer(),
                                                                   ksEntry->keyString.getSize()),
                               key.getSize()) == 0) {
            return KeyStringEntry(ksEntry->keyString, ksEntry->loc);
        }
        return {};
    }

    boost::optional<IndexKeyEntry> seekExact(const KeyString::Value& keyStringValue,
                                             RequestedInfo parts) override {
        auto ksEntry = seekExactForKeyString(keyStringValue);
        if (ksEntry) {
            auto kv = curr(parts);
            invariant(kv);
            return kv;
        }
        return {};
    }

    void save() override {
        try {
            if (_iterator)
                _iterator.reset();
        } catch (const WriteConflictException&) {
            // Ignore since this is only called when we are about to kill our transaction
            // anyway.
        }

        // Our saved position is wherever we were when we last called updatePosition().
        // Any partially completed repositions should not effect our saved position.
    }

    void saveUnpositioned() override {
        save();
        _eof = true;
    }

    void restore() override {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
        if (!_iterator.get() || _currentSequenceNumber != ru->snapshot()->GetSequenceNumber()) {
            _iterator.reset(ru->NewIterator(_prefix));
            _currentSequenceNumber = ru->snapshot()->GetSequenceNumber();
        }

        if (!_eof) {
            _lastMoveSkippedKey = !seekCursor(_key.getValueCopy());
            TRACE_CURSOR << "restore _lastMoveSkippedKey:" << _lastMoveSkippedKey;
        }
    }

    void detachFromOperationContext() final {
        _opCtx = nullptr;
        _iterator.reset();
    }

    void reattachToOperationContext(OperationContext* opCtx) final {
        _opCtx = opCtx;
        // iterator recreated in restore()
    }

protected:
    // Called after _key has been filled in, ie a new key to be processed has been fetched.
    // Must not throw WriteConflictException, throwing a WriteConflictException will retry the
    // operation effectively skipping over this key.
    virtual void updateIdAndTypeBits() {
        _id = KeyString::decodeRecordIdAtEnd(_key.getBuffer(), _key.getSize());
        BufReader br(_valueSlice().data(), _valueSlice().size());
        _typeBits.resetFromBuffer(&br);
    }

    boost::optional<IndexKeyEntry> curr(RequestedInfo parts) const {
        if (_eof) {
            return {};
        }

        dassert(!atOrPastEndPointAfterSeeking());
        dassert(!_id.isNull());

        BSONObj bson;
        if (TRACING_ENABLED || (parts & kWantKey)) {
            bson =
                KeyString::toBson(_key.getBuffer(), _key.getSize(), _idx.getOrdering(), _typeBits);

            TRACE_CURSOR << " returning " << bson << ' ' << _id;
        }

        return {{std::move(bson), _id}};
    }

    bool atOrPastEndPointAfterSeeking() const {
        if (_eof)
            return true;
        if (!_endPosition)
            return false;

        const int cmp = _key.compare(*_endPosition);

        // We set up _endPosition to be in between the last in-range value and the first
        // out-of-range value. In particular, it is constructed to never equal any legal index
        // key.
        dassert(cmp != 0);

        if (_forward) {
            // We may have landed after the end point.
            return cmp > 0;
        } else {
            // We may have landed before the end point.
            return cmp < 0;
        }
    }

    void advanceCursor() {
        if (_eof) {
            return;
        }
        if (_iterator.get() == nullptr) {
            _iterator.reset(RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(_prefix));
            _iterator->SeekPrefix(rocksdb::Slice(_key.getBuffer(), _key.getSize()));
            // advanceCursor() should only ever be called in states where the above seek
            // will succeed in finding the exact key
            invariant(_iterator->Valid());
        }
        if (_forward) {
            _iterator->Next();
        } else {
            _iterator->Prev();
        }
        _updateOnIteratorValidity();
    }

    // Seeks to query. Returns true on exact match.
    bool seekCursor(const KeyString::Value& query) {
        auto* iter = iterator();
        auto keyString = std::string(query.getBuffer(), query.getSize());
        const rocksdb::Slice keySlice(keyString);
        auto keyWithTimestamp = std::string(query.getBuffer(), query.getSize());
        // keyWithTimestamp.append(sizeof(uint64_t), '\xff');
        const rocksdb::Slice keySliceWithTimestamp(keyWithTimestamp);
        iter->Seek(keySliceWithTimestamp);
        if (!_updateOnIteratorValidity()) {
            if (!_forward) {
                // this will give lower bound behavior for backwards
                iter->SeekToLast();
                _updateOnIteratorValidity();
            }
            return false;
        }

        auto key = iter->key();

        if (key == keySlice) {
            return true;
        }

        if (!_forward) {
            // if we can't find the exact result going backwards, we
            // need to call Prev() so that we're at the first value
            // less than (to the left of) what we were searching for,
            // rather than the first value greater than (to the right
            // of) the value we were searching for.
            iter->Prev();
            _updateOnIteratorValidity();
        }

        return false;
    }

    void updatePosition() {
        _lastMoveSkippedKey = false;
        if (_cursorAtEof) {
            _eof = true;
            _id = RecordId();
            return;
        }

        _eof = false;

        auto key = _iterator->key();
        _key.resetFromBuffer(key.data(), key.size());

        if (atOrPastEndPointAfterSeeking()) {
            _eof = true;
            return;
        }

        updateIdAndTypeBits();
    }

    bool advanceNext() {
        // Advance on a cursor at the end is a no-op.
        if (_eof) {
            return false;
        }
        if (!_lastMoveSkippedKey) {
            advanceCursor();
        }
        updatePosition();
        return true;
    }

    KeyStringEntry getKeyStringEntry() {
        // Most keys will have a RecordId appended to the end, with the exception of the _id index
        // and timestamp unsafe unique indexes. The contract of this function is to always return a
        // KeyString with a RecordId, so append one if it does not exists already.
        if (_idx.unique() &&
            (_idx.isIdIndex() ||
             _key.getSize() ==
                 KeyString::getKeySize(
                     _key.getBuffer(), _key.getSize(), _idx.getOrdering(), _typeBits))) {
            // Create a copy of _key with a RecordId. Because _key is used during cursor restore(),
            // appending the RecordId would cause the cursor to be repositioned incorrectly.
            KeyString::Builder keyWithRecordId(_key);
            keyWithRecordId.appendRecordId(_id);
            keyWithRecordId.setTypeBits(_typeBits);

            TRACE_CURSOR << " returning " << keyWithRecordId << ' ' << _id;
            return KeyStringEntry(keyWithRecordId.getValueCopy(), _id);
        }

        _key.setTypeBits(_typeBits);
        TRACE_CURSOR << " returning " << _key << ' ' << _id;
        return KeyStringEntry(_key.getValueCopy(), _id);
    }

    // ensure that _iterator is initialized and return a pointer to it
    RocksIterator* iterator() {
        if (_iterator.get() == nullptr) {
            _iterator.reset(RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(_prefix));
        }
        return _iterator.get();
    }

    // Update _eof based on _iterator->Valid() and return _iterator->Valid()
    bool _updateOnIteratorValidity() {
        if (_iterator->Valid()) {
            _cursorAtEof = false;
            return true;
        } else {
            _cursorAtEof = true;
            invariantRocksOK(_iterator->status());
            return false;
        }
    }

    rocksdb::Slice _valueSlice() {
        if (_iterator.get() == nullptr) {
            return rocksdb::Slice(_value);
        }
        return rocksdb::Slice(_iterator->value());
    }

    OperationContext* _opCtx;
    std::unique_ptr<RocksIterator> _iterator;
    const RocksIndexBase& _idx;  // not owned
    const bool _forward;

    // These are where this cursor instance is. They are not changed in the face of a failing
    // next().
    KeyString::Builder _key;
    KeyString::TypeBits _typeBits;
    RecordId _id;
    bool _eof = true;

    // This differs from _eof in that it always reflects the result of the most recent call to
    // reposition _cursor.
    bool _cursorAtEof = false;

    // Used by next to decide to return current position rather than moving. Should be reset to
    // false by any operation that moves the cursor, other than subsequent save/restore pairs.
    bool _lastMoveSkippedKey = false;

    KeyString::Builder _query;
    std::string _prefix;

    std::unique_ptr<KeyString::Builder> _endPosition;

    // These are for storing savePosition/restorePosition state
    rocksdb::SequenceNumber _currentSequenceNumber;

    // stores the value associated with the latest call to seekExact()
    std::string _value;
};

class RocksStandardCursor final : public RocksCursorBase {
public:
    RocksStandardCursor(const RocksIndexBase& idx,
                        OperationContext* opCtx,
                        bool forward,
                        std::string prefix)
        : RocksCursorBase(idx, opCtx, forward, prefix) {
        iterator();
    }
};

class RocksUniqueCursor final : public RocksCursorBase {
public:
    RocksUniqueCursor(const RocksIndexBase& idx,
                      OperationContext* opCtx,
                      bool forward,
                      std::string prefix)
        : RocksCursorBase(idx, opCtx, forward, prefix) {}

    // boost::optional<IndexKeyEntry> seekExact(const BSONObj& key, RequestedInfo parts) override {
    //     _cursorAtEof = false;
    //     _iterator.reset();

    //     std::string prefixedKey(_prefix);
    //     _query.resetToKey(stripFieldNames(key), _idx.ordering());
    //     prefixedKey.append(_query.getBuffer(), _query.getSize());
    //     rocksdb::Status status =
    //         RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->Get(prefixedKey, &_value);

    //     if (status.IsNotFound()) {
    //         _cursorAtEof = true;
    //     } else if (!status.ok()) {
    //         invariantRocksOK(status);
    //     }
    //     updatePosition();
    //     return curr(parts);
    // }

    // Called after _key has been filled in, ie a new key to be processed has been fetched.
    // Must not throw WriteConflictException, throwing a WriteConflictException will retry the
    // operation effectively skipping over this key.
    void updateIdAndTypeBits() {
        TRACE_INDEX << "Unique Index KeyString: [" << _key.toString() << "]";

        if (_idx.isIdIndex() || !_idx.isTimestampSafeUniqueIdx()) {
            _updateIdAndTypeBitsFromValue();
            return;
        }

        RocksCursorBase::updateIdAndTypeBits();
    }

    void restore() override {
        // Lets begin by calling the base implementaion
        RocksCursorBase::restore();

        // If this is not timestamp safe unique index, we are done
        if (_idx.isIdIndex() || !_idx.isTimestampSafeUniqueIdx()) {
            return;
        }

        if (_lastMoveSkippedKey && !_eof && !_cursorAtEof) {
            // We did not get an exact match for the saved key. We need to determine if we
            // skipped a record while trying to position the cursor.
            // After a rolling upgrade an index can have keys from both timestamp unsafe (old)
            // and timestamp safe (new) unique indexes. An older styled index entry key is
            // KeyString of the prefix key only, whereas a newer styled index entry key is
            // KeyString of the prefix key + RecordId.
            // In either case we compare the prefix key portion of the saved index entry
            // key against the current key that we are positioned on, if there is a match we
            // know we are positioned correctly and have not skipped a record.

            // Get the size of the prefix key
            auto keySize = KeyString::getKeySize(
                _key.getBuffer(), _key.getSize(), _idx.getOrdering(), _key.getTypeBits());

            // This check is only to avoid returning the same key again after a restore. Keys
            // shorter than _key cannot have "prefix key" same as _key. Therefore we care only about
            // the keys with size greater than or equal to that of the _key.
            if (_iterator->key().size() >= keySize &&
                std::memcmp(_key.getBuffer(), _iterator->key().data(), keySize) == 0) {
                _lastMoveSkippedKey = false;
                TRACE_CURSOR << "restore _lastMoveSkippedKey changed to false.";
            }
        }
    }

private:
    // Called after _key has been filled in, ie a new key to be processed has been fetched.
    // Must not throw WriteConflictException, throwing a WriteConflictException will retry the
    // operation effectively skipping over this key.
    void _updateIdAndTypeBitsFromValue() {

        // We assume that cursors can only ever see unique indexes in their "pristine" state,
        // where no duplicates are possible. The cases where dups are allowed should hold
        // sufficient locks to ensure that no cursor ever sees them.
        std::string value(_valueSlice().data(), _valueSlice().size());
        BufReader br(_valueSlice().data(), _valueSlice().size());
        _id = KeyString::decodeRecordId(&br);
        _typeBits.resetFromBuffer(&br);

        if (!br.atEof()) {
            severe() << "Unique index cursor seeing multiple records for key "
                     << redact(curr(kWantKey)->key) << " in index " << _idx.indexName()
                     << " belonging to collection " << _idx.collectionNamespace();
            fassertFailed(28609);
        }
    }
};

}  // namespace

/**
 * Bulk builds a non-unique index.
 */
class RocksIndexBase::StandardBulkBuilder : public SortedDataBuilderInterface {
public:
    StandardBulkBuilder(RocksStandardIndex* index, OperationContext* opCtx)
        : _index(index), _opCtx(opCtx) {}

    Status addKey(const KeyString::Value& keyString) {
        return _index->insert(_opCtx, keyString, true);
    }

    void commit(bool mayInterrupt) {
        WriteUnitOfWork uow(_opCtx);
        uow.commit();
    }

private:
    RocksStandardIndex* _index;
    OperationContext* _opCtx;
};

/**
 * Bulk builds a unique index.
 *
 * In order to support unique indexes in dupsAllowed mode this class only does an actual insert
 * after it sees a key after the one we are trying to insert. This allows us to gather up all
 * duplicate ids and insert them all together. This is necessary since bulk cursors can only
 * append data.
 */
class RocksIndexBase::UniqueBulkBuilder : public SortedDataBuilderInterface {
public:
    UniqueBulkBuilder(RocksIndexBase* idx,
                      OperationContext* opCtx,
                      bool dupsAllowed,
                      std::string prefix)
        : _prefix(std::move(prefix)),
          _opCtx(opCtx),
          _idx(idx),
          _dupsAllowed(dupsAllowed),
          _previousKeyString(idx->getKeyStringVersion()) {}

    Status addKey(const KeyString::Value& newKeyString) override {
        if (_idx->isTimestampSafeUniqueIdx()) {
            return addKeyTimestampSafe(newKeyString);
        }
        return addKeyTimestampUnsafe(newKeyString);
    }

    void commit(bool mayInterrupt) override {
        WriteUnitOfWork uow(_opCtx);
        if (!_records.empty()) {
            // This handles inserting the last unique key.
            doInsert();
        }
        uow.commit();
    }

private:
    Status addKeyTimestampSafe(const KeyString::Value& newKeyString) {

        // Do a duplicate check, but only if dups aren't allowed.
        if (!_dupsAllowed) {
            const int cmp = newKeyString.compareWithoutRecordId(_previousKeyString);
            if (cmp == 0) {
                // Duplicate found!
                auto newKey = KeyString::toBson(newKeyString, _idx->_ordering);
                return buildDupKeyErrorStatus(
                    newKey, _idx->collectionNamespace(), _idx->indexName(), _idx->keyPattern());
            } else {
                /*
                 * _previousKeyString.isEmpty() is only true on the first call to addKey().
                 * newKeyString must be greater than previous key.
                 */
                invariant(_previousKeyString.isEmpty() || cmp > 0);
            }
        }

        std::string prefixedKey(RocksIndexBase::_makePrefixedKey(_prefix, newKeyString));
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
        if (!ru->transaction()->registerWrite(prefixedKey)) {
            throw WriteConflictException();
        }

        const KeyString::TypeBits typeBits = newKeyString.getTypeBits();
        rocksdb::Slice value;
        if (!typeBits.isAllZeros()) {
            value = rocksdb::Slice(typeBits.getBuffer()), typeBits.getSize();
        }

        ru->Put(prefixedKey, value);

        // Don't copy the key again if dups are allowed.
        if (!_dupsAllowed)
            _previousKeyString.resetFromBuffer(newKeyString.getBuffer(), newKeyString.getSize());

        return Status::OK();
    }

    Status addKeyTimestampUnsafe(const KeyString::Value& newKeyString) {
        const int cmp = newKeyString.compareWithoutRecordId(_previousKeyString);
        if (cmp != 0) {
            if (!_previousKeyString.isEmpty()) {
                // _previousKeyString.isEmpty() is only true on the first call to addKey().
                invariant(cmp > 0);  // newKey must be > the last key.
                // We are done with dups of the last key so we can insert it now.
                doInsert();
            }
            invariant(_records.empty());
        } else {
            // Dup found!
            if (!_dupsAllowed) {
                auto newKey = KeyString::toBson(newKeyString, _idx->_ordering);
                return buildDupKeyErrorStatus(
                    newKey, _idx->collectionNamespace(), _idx->indexName(), _idx->keyPattern());
            }

            // If we get here, we are in the weird mode where dups are allowed on a unique
            // index, so add ourselves to the list of duplicate ids. This also replaces the
            // _previousKey which is correct since any dups seen later are likely to be newer.
        }

        RecordId id =
            KeyString::decodeRecordIdAtEnd(newKeyString.getBuffer(), newKeyString.getSize());
        _records.push_back(std::make_pair(id, newKeyString.getTypeBits()));
        _previousKeyString.resetFromBuffer(newKeyString.getBuffer(), newKeyString.getSize());

        return Status::OK();
    }

    void doInsert() {
        invariant(!_records.empty());

        KeyString::Builder value(_idx->getKeyStringVersion());
        for (size_t i = 0; i < _records.size(); i++) {
            value.appendRecordId(_records[i].first);
            // When there is only one record, we can omit AllZeros TypeBits. Otherwise they need
            // to be included.
            if (!(_records[i].second.isAllZeros() && _records.size() == 1)) {
                value.appendTypeBits(_records[i].second);
            }
        }

        auto sizeWithoutRecordId = KeyString::sizeWithoutRecordIdAtEnd(
            _previousKeyString.getBuffer(), _previousKeyString.getSize());
        std::string prefixedKey(
            RocksIndexBase::_makePrefixedKey(_prefix, _previousKeyString.getValueCopy()));
        rocksdb::Slice keySlice(prefixedKey.data(), sizeWithoutRecordId + _prefix.size());
        rocksdb::Slice valueSlice(value.getBuffer(), value.getSize());

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
        ru->Put(keySlice, valueSlice);

        _records.clear();
    }

    std::string _prefix;
    OperationContext* _opCtx;

    RocksIndexBase* _idx;
    const bool _dupsAllowed;
    KeyString::Builder _previousKeyString;
    std::vector<std::pair<RecordId, KeyString::TypeBits>> _records;
};

/// RocksIndexBase

RocksIndexBase::RocksIndexBase(rocksdb::DB* db,
                               std::string prefix,
                               std::string ident,
                               const IndexDescriptor* desc,
                               const BSONObj& config)
    : SortedDataInterface(KeyString::Version::V1, Ordering::make(desc->keyPattern())),
      _db(db),
      _ident(std::move(ident)),
      _collectionNamespace(desc->parentNS()),
      _indexName(desc->indexName()),
      _keyPattern(desc->keyPattern()),
      _prefix(prefix),
      _isIdIndex(desc->isIdIndex()) {
    uint64_t storageSize;
    std::string beginKey(_prefix);
    beginKey.append(sizeof(uint64_t), '\xff');
    std::string endKey = rocksGetNextPrefix(_prefix);
    endKey.append(sizeof(uint64_t), '\xff');
    rocksdb::Range wholeRange(beginKey, endKey);

    _db->GetApproximateSizes(&wholeRange, 1, &storageSize);
    _indexStorageSize.store(static_cast<long long>(storageSize), std::memory_order_relaxed);

    int indexFormatVersion = kDataFormatV2KeyStringV1IndexVersionV2;  // default
    if (config.hasField("index_format_version")) {
        indexFormatVersion = config.getField("index_format_version").numberInt();
    }

    if (indexFormatVersion < kMinimumIndexVersion || indexFormatVersion > kMaximumIndexVersion) {
        Status indexVersionStatus(ErrorCodes::UnsupportedFormat,
                                  "Unrecognized index format -- you might want to upgrade MongoDB");
        fassertFailedWithStatusNoTrace(40264, indexVersionStatus);
    }

    _dataFormatVersion = indexFormatVersion;
}

Status RocksIndexBase::insert(OperationContext* opCtx,
                              const KeyString::Value& keyString,
                              bool dupsAllowed) {
    dassert(opCtx->lockState()->isWriteLocked());
    dassert(KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize()).isValid());

    TRACE_INDEX << " KeyString: " << keyString;

    return _insert(opCtx, keyString, dupsAllowed);
}

void RocksIndexBase::unindex(OperationContext* opCtx,
                             const KeyString::Value& keyString,
                             bool dupsAllowed) {
    dassert(opCtx->lockState()->isWriteLocked());
    dassert(KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize()).isValid());

    _unindex(opCtx, keyString, dupsAllowed);
}

void RocksIndexBase::fullValidate(OperationContext* opCtx,
                                  long long* numKeysOut,
                                  ValidateResults* fullResults) const {

    long long count = 0;
    TRACE_INDEX << " fullValidate";

    const auto requestedInfo = TRACING_ENABLED ? Cursor::kKeyAndLoc : Cursor::kJustExistance;

    KeyString::Value keyStringForSeek =
        IndexEntryComparison::makeKeyStringFromBSONKeyForSeek(BSONObj(),
                                                              getKeyStringVersion(),
                                                              getOrdering(),
                                                              true, /* forward */
                                                              true  /* inclusive */
        );

    std::unique_ptr<SortedDataInterface::Cursor> cursor(newCursor(opCtx, true));
    for (auto kv = cursor->seek(keyStringForSeek, requestedInfo); kv; kv = cursor->next()) {
        TRACE_INDEX << "\t" << kv->key << ' ' << kv->loc;
        count++;
    }
    if (numKeysOut) {
        *numKeysOut = count;
    }
}

Status RocksIndexBase::dupKeyCheck(OperationContext* opCtx, const KeyString::Value& key) {
    invariant(unique());

    if (isDup(opCtx, key))
        return buildDupKeyErrorStatus(
            key, _collectionNamespace, _indexName, _keyPattern, _ordering);
    return Status::OK();
}

bool RocksIndexBase::isEmpty(OperationContext* opCtx) {
    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    std::unique_ptr<rocksdb::Iterator> it(ru->NewIterator(_prefix));

    it->SeekToFirst();
    return !it->Valid();
}

long long RocksIndexBase::getSpaceUsedBytes(OperationContext* opCtx) const {
    // There might be some bytes in the WAL that we don't count here. Some
    // tests depend on the fact that non-empty indexes have non-zero sizes
    return static_cast<long long>(
        std::max(_indexStorageSize.load(std::memory_order_relaxed), static_cast<long long>(1)));
}

bool RocksIndexBase::isDup(OperationContext* opCtx, const KeyString::Value& key) {
    dassert(opCtx->lockState()->isReadLocked());
    invariant(unique());

    std::string prefixedKey(_makePrefixedKey(_prefix, key));

    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    std::string value;
    auto getStatus = ru->Get(prefixedKey, &value);
    if (!getStatus.ok() && !getStatus.IsNotFound()) {
        invariantRocksOK(getStatus);
    } else if (getStatus.IsNotFound()) {
        // not found, not duplicate key
        return false;
    }

    // If the key exists, check if we already have this id at this key. If so, we don't
    // consider that to be a dup.
    BufReader br(value.data(), value.size());
    int records = 0;
    while (br.remaining()) {
        KeyString::decodeRecordId(&br);
        records++;

        KeyString::TypeBits::fromBuffer(getKeyStringVersion(), &br);  // Just advance the reader.
    }
    return records > 1;
}

Status RocksIndexBase::initAsEmpty(OperationContext* opCtx) {
    // no-op
    return Status::OK();
}

void RocksIndexBase::generateConfig(BSONObjBuilder* configBuilder,
                                    int formatVersion,
                                    const IndexDescriptor* desc) {
    if (desc->unique() && !desc->isIdIndex()) {
        configBuilder->append("index_format_version",
                              static_cast<int32_t>(kDataFormatV3KeyStringV1UniqueIndexVersionV2));
    } else {
        configBuilder->append("index_format_version",
                              static_cast<int32_t>(kDataFormatV2KeyStringV1IndexVersionV2));
    }
}

std::string RocksIndexBase::_makePrefixedKey(const std::string& prefix,
                                             const KeyString::Value& key) {
    std::string keyString(prefix);
    keyString.append(key.getBuffer(), key.getSize());
    return keyString;
}

/// RocksUniqueIndex

RocksUniqueIndex::RocksUniqueIndex(rocksdb::DB* db,
                                   std::string prefix,
                                   std::string ident,
                                   const IndexDescriptor* desc,
                                   const BSONObj& config)
    : RocksIndexBase(db, prefix, ident, desc, config),

      _partial(desc->isPartial()) {}

std::unique_ptr<SortedDataInterface::Cursor> RocksUniqueIndex::newCursor(OperationContext* opCtx,
                                                                         bool forward) const {
    return std::make_unique<RocksUniqueCursor>(*this, opCtx, forward, _prefix);
}

SortedDataBuilderInterface* RocksUniqueIndex::getBulkBuilder(OperationContext* opCtx,
                                                             bool dupsAllowed) {
    return new RocksIndexBase::UniqueBulkBuilder(this, opCtx, dupsAllowed, _prefix);
}

bool RocksUniqueIndex::isTimestampSafeUniqueIdx() const {
    if (_dataFormatVersion == kDataFormatV2KeyStringV1IndexVersionV2) {
        return false;
    }
    return true;
}

bool RocksUniqueIndex::_keyExists(OperationContext* opCtx,
                                  RocksIterator* it,
                                  const rocksdb::Slice& key) {
    it->SeekPrefix(key);

    if (!it->Valid())
        return false;

    // Obtain the key from the record returned by search near.
    if (std::memcmp(key.data(), it->value().data(), std::min(key.size(), it->value().size())) ==
        0) {
        return true;
    }

    return false;
}

bool RocksUniqueIndex::isDup(OperationContext* opCtx, const KeyString::Value& prefixKey) {
    if (!isTimestampSafeUniqueIdx()) {
        // The parent class provides a functionality that works fine, just use that.
        return RocksIndexBase::isDup(opCtx, prefixKey);
    }

    // This procedure to determine duplicates is exclusive for timestamp safe unique indexes.

    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    std::unique_ptr<RocksIterator> it(ru->NewIterator(_prefix));

    // Check if a prefix key already exists in the index. When keyExists() returns true, the cursor
    // will be positioned on the first occurence of the 'prefixKey'.
    rocksdb::Slice keySlice(prefixKey.getBuffer(), prefixKey.getSize());
    if (!_keyExists(opCtx, it.get(), keySlice)) {
        return false;
    }

    it->Next();

    if (!it->Valid()) {
        return false;
    }

    return std::memcmp(prefixKey.getBuffer(),
                       it->value().data(),
                       std::min(prefixKey.getSize(), it->value().size())) == 0;
}

Status RocksUniqueIndex::_insert(OperationContext* opCtx,
                                 const KeyString::Value& keyString,
                                 bool dupsAllowed) {
    if (isTimestampSafeUniqueIdx()) {
        return _insertTimestampSafe(opCtx, keyString, dupsAllowed);
    }
    return _insertTimestampUnsafe(opCtx, keyString, dupsAllowed);
}

Status RocksUniqueIndex::_insertTimestampUnsafe(OperationContext* opCtx,
                                                const KeyString::Value& keyString,
                                                bool dupsAllowed) {

    const RecordId id = KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());
    invariant(id.isValid());

    std::string prefixedKey(_makePrefixedKey(_prefix, keyString));

    auto sizeWithoutRecordId =
        KeyString::sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());
    rocksdb::Slice keySlice(prefixedKey.data(), sizeWithoutRecordId + _prefix.size());

    KeyString::Builder value(getKeyStringVersion(), id);
    const KeyString::TypeBits typeBits = keyString.getTypeBits();
    if (!typeBits.isAllZeros())
        value.appendTypeBits(typeBits);

    // rocksdb: conflict detection
    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    if (!ru->transaction()->registerWrite(prefixedKey)) {
        throw WriteConflictException();
    }

    std::string currentValue;
    auto getStatus = ru->Get(keySlice, &currentValue);
    if (!getStatus.ok() && !getStatus.IsNotFound()) {
        return rocksToMongoStatus(getStatus);
    } else if (getStatus.IsNotFound()) {
        // nothing here. just insert the value
        rocksdb::Slice valueSlice(value.getBuffer(), value.getSize());
        ru->Put(keySlice, valueSlice);

        // rocksdb: manually track size
        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size() + value.getSize()),
                                    std::memory_order_relaxed);

        return Status::OK();
    }

    // we are in a weird state where there might be multiple values for a key
    // we put them all in the "list"
    // Note that we can't omit AllZeros when there are multiple ids for a value. When we remove
    // down to a single value, it will be cleaned up.

    bool insertedId = false;

    value.resetToEmpty();
    BufReader br(currentValue.data(), currentValue.size());
    while (br.remaining()) {
        RecordId idInIndex = KeyString::decodeRecordId(&br);
        if (id == idInIndex)
            return Status::OK();


        if (!insertedId && id < idInIndex) {
            value.appendRecordId(id);
            value.appendTypeBits(typeBits);
            insertedId = true;
        }

        // Copy from old to new value
        value.appendRecordId(idInIndex);
        value.appendTypeBits(KeyString::TypeBits::fromBuffer(getKeyStringVersion(), &br));
    }

    if (!dupsAllowed) {
        auto key = KeyString::toBson(keyString, _ordering);
        return buildDupKeyErrorStatus(key, _collectionNamespace, _indexName, _keyPattern);
    }

    if (!insertedId) {
        // This id is higher than all currently in the index for this key
        value.appendRecordId(id);
        value.appendTypeBits(typeBits);
    }

    rocksdb::Slice valueSlice(value.getBuffer(), value.getSize());
    ru->Put(prefixedKey, valueSlice);

    // rocksdb: manually track size
    _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size() + value.getSize()),
                                std::memory_order_relaxed);

    return Status::OK();
}

Status RocksUniqueIndex::_insertTimestampSafe(OperationContext* opCtx,
                                              const KeyString::Value& keyString,
                                              bool dupsAllowed) {
    TRACE_INDEX << "Timestamp safe unique idx KeyString: " << keyString;

    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);

    // Pre-checks before inserting on a primary.
    if (!dupsAllowed) {
        // A prefix key is KeyString of index key. It is the component of the index entry that
        // should be unique.
        auto sizeWithoutRecordId =
            KeyString::sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());
        rocksdb::Slice keySlice(keyString.getBuffer(), sizeWithoutRecordId);

        std::string prefixedKey(_makePrefixedKey(_prefix, keyString),
                                _prefix.size() + sizeWithoutRecordId);
        if (!ru->transaction()->registerWrite(prefixedKey)) {
            throw WriteConflictException();
        }

        // Second phase looks up for existence of key to avoid insertion of duplicate key
        std::unique_ptr<RocksIterator> it(ru->NewIterator(_prefix));
        if (_keyExists(opCtx, it.get(), keySlice)) {
            auto key = KeyString::toBson(
                keyString.getBuffer(), sizeWithoutRecordId, _ordering, keyString.getTypeBits());
            return buildDupKeyErrorStatus(key, _collectionNamespace, _indexName, _keyPattern);
        }
    }

    // Now create the table key/value, the actual data record.
    std::string prefixedTableKey(_makePrefixedKey(_prefix, keyString));

    const KeyString::TypeBits typeBits = keyString.getTypeBits();
    rocksdb::Slice value;
    if (!typeBits.isAllZeros()) {
        value =
            rocksdb::Slice(reinterpret_cast<const char*>(typeBits.getBuffer()), typeBits.getSize());
    }

    _indexStorageSize.fetch_add(static_cast<long long>(prefixedTableKey.size() + value.size()),
                                std::memory_order_relaxed);

    ru->Put(prefixedTableKey, value);

    return Status::OK();
}

void RocksUniqueIndex::_unindex(OperationContext* opCtx,
                                const KeyString::Value& keyString,
                                bool dupsAllowed) {
    if (isTimestampSafeUniqueIdx()) {
        return _unindexTimestampSafe(opCtx, keyString, dupsAllowed);
    }
    return _unindexTimestampUnsafe(opCtx, keyString, dupsAllowed);
}

void RocksUniqueIndex::_unindexTimestampUnsafe(OperationContext* opCtx,
                                               const KeyString::Value& keyString,
                                               bool dupsAllowed) {
    const RecordId id = KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());
    invariant(id.isValid());

    auto sizeWithoutRecordId =
        KeyString::sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());
    std::string prefixedKey(_makePrefixedKey(_prefix, keyString));
    rocksdb::Slice keySlice(prefixedKey.data(), sizeWithoutRecordId + _prefix.size());

    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    // We can't let two threads unindex the same key
    if (!ru->transaction()->registerWrite(prefixedKey)) {
        throw WriteConflictException();
    }

    if (!dupsAllowed) {
        if (_partial) {
            // Check that the record id matches.  We may be called to unindex records that are
            // not present in the index due to the partial filter expression.
            std::string val;
            auto s = ru->Get(keySlice, &val);
            if (s.IsNotFound()) {
                return;
            }
            BufReader br(val.data(), val.size());
            fassert(90416, br.remaining());
            if (KeyString::decodeRecordId(&br) != id) {
                return;
            }
            // Ensure there aren't any other values in here.
            KeyString::TypeBits::fromBuffer(_keyStringVersion, &br);
            fassert(90417, !br.remaining());
        }
        _indexStorageSize.fetch_sub(static_cast<long long>(keySlice.size()),
                                    std::memory_order_relaxed);
        ru->Delete(keySlice);
        return;
    }

    // dups are allowed, so we have to deal with a vector of RecordIds.
    std::string currentValue;
    auto getStatus = ru->Get(keySlice, &currentValue);
    if (getStatus.IsNotFound()) {
        return;
    }
    invariantRocksOK(getStatus);

    bool foundId = false;
    std::vector<std::pair<RecordId, KeyString::TypeBits>> records;

    BufReader br(currentValue.data(), currentValue.size());
    while (br.remaining()) {
        RecordId idInIndex = KeyString::decodeRecordId(&br);
        KeyString::TypeBits typeBits = KeyString::TypeBits::fromBuffer(_keyStringVersion, &br);

        if (id == idInIndex) {
            if (records.empty() && !br.remaining()) {
                // This is the common case: we are removing the only id for this key.
                // Remove the whole entry.
                _indexStorageSize.fetch_sub(static_cast<long long>(keySlice.size()),
                                            std::memory_order_relaxed);
                ru->Delete(keySlice);
                return;
            }

            foundId = true;
            continue;
        }

        records.push_back(std::make_pair(idInIndex, typeBits));
    }

    if (!foundId) {
        auto key = KeyString::toBson(keyString, _ordering);
        warning().stream() << id << " not found in the index for key " << redact(key);
        return;  // nothing to do
    }

    // Put other ids for this key back in the index.
    KeyString::Builder newValue(getKeyStringVersion());
    invariant(!records.empty());
    for (size_t i = 0; i < records.size(); i++) {
        newValue.appendRecordId(records[i].first);
        // When there is only one record, we can omit AllZeros TypeBits. Otherwise they need
        // to be included.
        if (!(records[i].second.isAllZeros() && records.size() == 1)) {
            newValue.appendTypeBits(records[i].second);
        }
    }

    rocksdb::Slice newValueSlice(newValue.getBuffer(), newValue.getSize());
    ru->Put(prefixedKey, newValueSlice);
    _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size() + newValueSlice.size()),
                                std::memory_order_relaxed);
}

void RocksUniqueIndex::_unindexTimestampSafe(OperationContext* opCtx,
                                             const KeyString::Value& keyString,
                                             bool dupsAllowed) {
    std::string prefixedKey(_makePrefixedKey(_prefix, keyString));

    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    // We can't let two threads unindex the same key
    if (!ru->transaction()->registerWrite(prefixedKey)) {
        throw WriteConflictException();
    }

    _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                std::memory_order_relaxed);
    ru->Delete(prefixedKey);
}

/// RocksStandardIndex
RocksStandardIndex::RocksStandardIndex(rocksdb::DB* db,
                                       std::string prefix,
                                       std::string ident,
                                       const IndexDescriptor* desc,
                                       const BSONObj& config)
    : RocksIndexBase(db, prefix, ident, desc, config), useSingleDelete(false) {}

Status RocksStandardIndex::_insert(OperationContext* opCtx,
                                   const KeyString::Value& keyString,
                                   bool dupsAllowed) {
    invariant(dupsAllowed);

    std::string prefixedKey(_makePrefixedKey(_prefix, keyString));
    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    if (!ru->transaction()->registerWrite(prefixedKey)) {
        throw WriteConflictException();
    }

    rocksdb::Slice value;
    const KeyString::TypeBits typeBits = keyString.getTypeBits();
    if (!typeBits.isAllZeros()) {
        value = rocksdb::Slice(typeBits.getBuffer()), typeBits.getSize();
    }

    _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                std::memory_order_relaxed);

    ru->Put(prefixedKey, value);

    return Status::OK();
}

void RocksStandardIndex::_unindex(OperationContext* opCtx,
                                  const KeyString::Value& keyString,
                                  bool dupsAllowed) {
    invariant(dupsAllowed);

    std::string prefixedKey(_makePrefixedKey(_prefix, keyString));

    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    if (!ru->transaction()->registerWrite(prefixedKey)) {
        throw WriteConflictException();
    }

    _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                std::memory_order_relaxed);
    if (useSingleDelete) {
        ru->SingleDelete(prefixedKey);
    } else {
        ru->Delete(prefixedKey);
    }
}

std::unique_ptr<SortedDataInterface::Cursor> RocksStandardIndex::newCursor(OperationContext* opCtx,
                                                                           bool forward) const {
    return std::make_unique<RocksStandardCursor>(*this, opCtx, forward, _prefix);
}

SortedDataBuilderInterface* RocksStandardIndex::getBulkBuilder(OperationContext* opCtx,
                                                               bool dupsAllowed) {
    invariant(dupsAllowed);
    return new RocksIndexBase::StandardBulkBuilder(this, opCtx);
}

}  // namespace mongo
