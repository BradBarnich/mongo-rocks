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


#include <atomic>
#include <string>

#include <rocksdb/db.h>

#include "mongo/bson/ordering.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/sorted_data_interface.h"

#include "rocks_recovery_unit.h"

#pragma once

namespace rocksdb {
class DB;
}

namespace mongo {

class RocksRecoveryUnit;

class RocksIndexBase : public SortedDataInterface {
    RocksIndexBase(const RocksIndexBase&) = delete;
    RocksIndexBase& operator=(const RocksIndexBase&) = delete;

public:
    RocksIndexBase(rocksdb::DB* db,
                   std::string prefix,
                   std::string ident,
                   const IndexDescriptor* desc,
                   const BSONObj& config);

    virtual StatusWith<SpecialFormatInserted> insert(OperationContext* opCtx,
                                                     const BSONObj& key,
                                                     const RecordId& id,
                                                     bool dupsAllowed);

    virtual void unindex(OperationContext* opCtx,
                         const BSONObj& key,
                         const RecordId& id,
                         bool dupsAllowed);

    virtual void fullValidate(OperationContext* opCtx,
                              long long* numKeysOut,
                              ValidateResults* fullResults) const;

    virtual bool appendCustomStats(OperationContext* opCtx,
                                   BSONObjBuilder* output,
                                   double scale) const {
        // nothing to say here, really
        output->append("ident", _ident);
        return true;
    }

    virtual Status dupKeyCheck(OperationContext* opCtx, const BSONObj& key);

    virtual bool isEmpty(OperationContext* opCtx);

    virtual long long getSpaceUsedBytes(OperationContext* opCtx) const;

    virtual Status initAsEmpty(OperationContext* opCtx);

    virtual bool isDup(OperationContext* opCtx, const BSONObj& key);

    static void generateConfig(BSONObjBuilder* configBuilder,
                               int formatVersion,
                               const IndexDescriptor* desc);

    Ordering ordering() const {
        return _ordering;
    }

    KeyString::Version keyStringVersion() const {
        return _keyStringVersion;
    }

    const NamespaceString& collectionNamespace() const {
        return _collectionNamespace;
    }

    std::string indexName() const {
        return _indexName;
    }

    const BSONObj& keyPattern() const {
        return _keyPattern;
    }

    bool isIdIndex() const {
        return _isIdIndex;
    }

    rocksdb::DB* db() const {
        return _db;
    }

    virtual bool unique() const = 0;
    virtual bool isTimestampSafeUniqueIdx() const = 0;

protected:
    static std::string _makePrefixedKey(const std::string& prefix, const KeyString& encodedKey);

    virtual StatusWith<SpecialFormatInserted> _insert(OperationContext* opCtx,
                                                      const BSONObj& key,
                                                      const RecordId& id,
                                                      bool dupsAllowed) = 0;

    virtual void _unindex(OperationContext* opCtx,
                          const BSONObj& key,
                          const RecordId& id,
                          bool dupsAllowed) = 0;

    rocksdb::DB* _db;  // not owned

    std::string _ident;

    const Ordering _ordering;
    // The keystring and data format version are effectively const after the WiredTigerIndex
    // instance is constructed.
    KeyString::Version _keyStringVersion;
    int _dataFormatVersion;

    const NamespaceString _collectionNamespace;
    const std::string _indexName;
    const BSONObj _keyPattern;
    // Each key in the index is prefixed with _prefix
    std::string _prefix;
    bool _isIdIndex;


    // very approximate index storage size
    std::atomic<long long> _indexStorageSize;

    class StandardBulkBuilder;
    class UniqueBulkBuilder;
    friend class UniqueBulkBuilder;
};

class RocksUniqueIndex : public RocksIndexBase {
public:
    RocksUniqueIndex(rocksdb::DB* db,
                     std::string prefix,
                     std::string ident,
                     const IndexDescriptor* desc,
                     const BSONObj& config);

    std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                           bool forward) const override;

    SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) override;

    bool unique() const override {
        return true;
    }

    bool isTimestampSafeUniqueIdx() const override;

    bool isDup(OperationContext* opCtx, const BSONObj& key) override;

    StatusWith<SpecialFormatInserted> _insert(OperationContext* opCtx,
                                                     const BSONObj& key,
                                                     const RecordId& id,
                                                     bool dupsAllowed) override;
    
    StatusWith<SpecialFormatInserted> _insertTimestampUnsafe(OperationContext* opCtx,
                                                             const BSONObj& key,
                                                             const RecordId& id,
                                                             bool dupsAllowed);

    StatusWith<SpecialFormatInserted> _insertTimestampSafe(OperationContext* opCtx,
                                                           const BSONObj& key,
                                                           const RecordId& id,
                                                           bool dupsAllowed);

    void _unindex(OperationContext* opCtx,
                         const BSONObj& key,
                         const RecordId& id,
                         bool dupsAllowed) override;

    void _unindexTimestampUnsafe(OperationContext* opCtx,
                                 const BSONObj& key,
                                 const RecordId& id,
                                 bool dupsAllowed);

    void _unindexTimestampSafe(OperationContext* opCtx,
                               const BSONObj& key,
                               const RecordId& id,
                               bool dupsAllowed);

private:
    /**
     * If this returns true, the iterator will be positioned on the first matching the input 'key'.
     */
    bool _keyExists(OperationContext* opCtx, RocksIterator* it, const KeyString& key);

    const bool _partial;
};

class RocksStandardIndex : public RocksIndexBase {
public:
    RocksStandardIndex(rocksdb::DB* db,
                       std::string prefix,
                       std::string ident,
                       const IndexDescriptor* desc,
                       const BSONObj& config);

    std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                           bool forward) const override;

    SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) override;

    bool unique() const override {
        return false;
    }

    bool isTimestampSafeUniqueIdx() const override {
        return false;
    }

    StatusWith<SpecialFormatInserted> _insert(OperationContext* opCtx,
                                                     const BSONObj& key,
                                                     const RecordId& id,
                                                     bool dupsAllowed);
    void _unindex(OperationContext* opCtx,
                         const BSONObj& key,
                         const RecordId& id,
                         bool dupsAllowed);

    void enableSingleDelete() {
        useSingleDelete = true;
    }

private:
    bool useSingleDelete;
};

}  // namespace mongo
