/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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

#include <list>
#include <map>
#include <memory>
#include <string>

#include <boost/optional.hpp>

#include <rocksdb/cache.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>
#include <rocksdb/status.h>

#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/string_map.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_durability_manager.h"
#include "rocks_snapshot_manager.h"
#include "rocks_transaction.h"

namespace rocksdb {
class ColumnFamilyHandle;
struct ColumnFamilyDescriptor;
struct ColumnFamilyOptions;
class DB;
class Comparator;
class Iterator;
struct Options;
struct ReadOptions;
}  // namespace rocksdb

namespace mongo {

struct CollectionOptions;
class RocksIndexBase;
class RocksRecordStore;
class JournalListener;

class RocksEngine final : public KVEngine {
    RocksEngine(const RocksEngine&) = delete;
    RocksEngine& operator=(const RocksEngine&) = delete;

public:
    RocksEngine(const std::string& path, bool durable, int formatVersion, bool readOnly);
    ~RocksEngine();

    static void appendGlobalStats(BSONObjBuilder& b);

    RecoveryUnit* newRecoveryUnit() override;

    Status createRecordStore(OperationContext* opCtx,
                             StringData ns,
                             StringData ident,
                             const CollectionOptions& options) override;

    std::unique_ptr<RecordStore> makeTemporaryRecordStore(OperationContext* opCtx,
                                                          StringData ident) override;

    std::unique_ptr<RecordStore> getRecordStore(OperationContext* opCtx,
                                                StringData ns,
                                                StringData ident,
                                                const CollectionOptions& options) override;

    Status createSortedDataInterface(OperationContext* opCtx,
                                     const CollectionOptions& collOptions,
                                     StringData ident,
                                     const IndexDescriptor* desc) override;

    SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                StringData ident,
                                                const IndexDescriptor* desc) override;

    Status dropIdent(OperationContext* opCtx, StringData ident) override;

    bool hasIdent(OperationContext* opCtx, StringData ident) const override;

    std::vector<std::string> getAllIdents(OperationContext* opCtx) const override;

    bool supportsDocLocking() const override {
        return true;
    }

    bool supportsDirectoryPerDB() const override {
        return false;
    }

    int flushAllFiles(OperationContext* opCtx, bool sync) override;

    Status beginBackup(OperationContext* opCtx) override;

    void endBackup(OperationContext* opCtx) override;

    Status hotBackup(const std::string& path);

    bool isDurable() const override {
        return _durable;
    }

    bool isEphemeral() const override {
        return false;
    }

    int64_t getIdentSize(OperationContext* opCtx, StringData ident);

    Status repairIdent(OperationContext* opCtx, StringData ident) {
        return Status::OK();
    }

    void cleanShutdown();

    SnapshotManager* getSnapshotManager() const final {
        return (SnapshotManager*)&_snapshotManager;
    }

    /**
     * Initializes a background job to remove excess documents in the oplog collections.
     * This applies to the capped collections in the local.oplog.* namespaces (specifically
     * local.oplog.rs for replica sets and local.oplog.$main for master/slave replication).
     * Returns true if a background job is running for the namespace.
     */
    static bool initRsOplogBackgroundThread(StringData ns);

    void setJournalListener(JournalListener* jl);

    Timestamp getAllDurableTimestamp() const override {
        // if (_snapshotManager.haveCommittedSnapshot()) {
        //     return *_snapshotManager.getCommittedSnapshot();
        // }
        return Timestamp();
    }

    Timestamp getOldestOpenReadTimestamp() const override {
        return Timestamp();
    }

    bool supportsReadConcernSnapshot() const override {
        return true;
    }

    bool supportsReadConcernMajority() const override {
        return true;
    }

    /**
     * Returns the minimum possible Timestamp value in the oplog that replication may need for
     * recovery in the event of a crash. This value gets updated every time a checkpoint is
     * completed. This value is typically a lagged version of what's needed for rollback.
     *
     * Returns boost::none when called on an ephemeral database.
     */
    boost::optional<Timestamp> getOplogNeededForCrashRecovery() const override {
        return boost::none;
    }

    // rocks specific api

    /*
     * An oplog manager is always accessible, but this method will start the background thread to
     * control oplog entry visibility for reads.
     *
     * On mongod, the background thread will be started when the first oplog record store is
     * created, and stopped when the last oplog record store is destroyed, at shutdown time. For
     * unit tests, the background thread may be started and stopped multiple times as tests create
     * and destroy oplog record stores.
     */
    // void startOplogManager(OperationContext* opCtx,
    //                        const std::string& uri,
    //                        WiredTigerRecordStore* oplogRecordStore);
    // void haltOplogManager();

    /*
     * Always returns a non-nil pointer. However, the WiredTigerOplogManager may not have been
     * initialized and its background refreshing thread may not be running.
     *
     * A caller that wants to get the oplog read timestamp, or call
     * `waitForAllEarlierOplogWritesToBeVisible`, is advised to first see if the oplog manager is
     * running with a call to `isRunning`.
     *
     * A caller that simply wants to call `triggerJournalFlush` may do so without concern.
     */
    // WiredTigerOplogManager* getOplogManager() const {
    //     return _oplogManager.get();
    // }

    rocksdb::DB* getDB() {
        return _db.get();
    }
    const rocksdb::DB* getDB() const {
        return _db.get();
    }
    size_t getBlockCacheUsage() const {
        return _block_cache->GetUsage();
    }
    std::shared_ptr<rocksdb::Cache> getBlockCache() {
        return _block_cache;
    }

    RocksTransactionEngine* getTransactionEngine() {
        return &_transactionEngine;
    }

    RocksCompactionScheduler* getCompactionScheduler() const {
        return _compactionScheduler.get();
    }

    int getMaxWriteMBPerSec() const {
        return _maxWriteMBPerSec;
    }
    void setMaxWriteMBPerSec(int maxWriteMBPerSec);

    Status backup(const std::string& path);

    rocksdb::Statistics* getStatistics() const {
        return _statistics.get();
    }

private:
    Status _createIdent(StringData ident, BSONObjBuilder* configBuilder);
    BSONObj _getIdentConfig(StringData ident);
    BSONObj _tryGetIdentConfig(StringData ident);
    std::string _extractPrefix(const BSONObj& config);

    rocksdb::Options _options() const;

    std::string _path;
    std::unique_ptr<rocksdb::DB> _db;
    std::shared_ptr<rocksdb::Cache> _block_cache;
    int _maxWriteMBPerSec;
    std::shared_ptr<rocksdb::RateLimiter> _rateLimiter;
    // can be nullptr
    std::shared_ptr<rocksdb::Statistics> _statistics;

    const bool _durable;
    const int _formatVersion;

    // ident map stores mapping from ident to a BSON config
    mutable stdx::mutex _identMapMutex;
    typedef StringMap<BSONObj> IdentMap;
    IdentMap _identMap;
    std::string _oplogIdent;

    // protected by _identMapMutex
    uint32_t _maxPrefix;

    // _identObjectMapMutex protects both _identIndexMap and _identCollectionMap. It should
    // never be locked together with _identMapMutex
    mutable stdx::mutex _identObjectMapMutex;
    // mapping from ident --> index object. we don't own the object
    StringMap<RocksIndexBase*> _identIndexMap;
    // mapping from ident --> collection object
    StringMap<RocksRecordStore*> _identCollectionMap;

    // This is for concurrency control
    RocksTransactionEngine _transactionEngine;

    RocksSnapshotManager _snapshotManager;

    // CounterManages manages counters like numRecords and dataSize for record stores
    std::unique_ptr<RocksCounterManager> _counterManager;

    std::unique_ptr<RocksCompactionScheduler> _compactionScheduler;

    static const std::string kMetadataPrefix;
    static const std::string kMetadataPrefixWithTimestamp;

    std::unique_ptr<RocksDurabilityManager> _durabilityManager;
    class RocksJournalFlusher;
    std::unique_ptr<RocksJournalFlusher> _journalFlusher;  // Depends on _durabilityManager
};

}  // namespace mongo
