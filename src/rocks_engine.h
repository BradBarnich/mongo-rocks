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
#include <string>
#include <memory>

#include <boost/optional.hpp>

#include <rocksdb/cache.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/status.h>
#include <rocksdb/statistics.h>

#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/string_map.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_transaction.h"
#include "rocks_snapshot_manager.h"
#include "rocks_durability_manager.h"

namespace rocksdb {
    class ColumnFamilyHandle;
    struct ColumnFamilyDescriptor;
    struct ColumnFamilyOptions;
    class DB;
    class Comparator;
    class Iterator;
    struct Options;
    struct ReadOptions;
}

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
        virtual ~RocksEngine();

        static void appendGlobalStats(BSONObjBuilder& b);

        virtual RecoveryUnit* newRecoveryUnit() override;

        virtual Status createRecordStore(OperationContext* opCtx,
                                         StringData ns,
                                         StringData ident,
                                         const CollectionOptions& options) override;

        virtual std::unique_ptr<RecordStore> makeTemporaryRecordStore(OperationContext* opCtx,
                                                                  StringData ident) override;

        virtual std::unique_ptr<RecordStore> getRecordStore(OperationContext* opCtx, StringData ns,
                                            StringData ident,
                                            const CollectionOptions& options) override;

        virtual Status createSortedDataInterface(OperationContext* opCtx,
                                     const CollectionOptions& collOptions,
                                     StringData ident,
                                     const IndexDescriptor* desc) override;

        virtual SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                            StringData ident,
                                                            const IndexDescriptor* desc) override;

        virtual Status dropIdent(OperationContext* opCtx, StringData ident) override;

        virtual bool hasIdent(OperationContext* opCtx, StringData ident) const override;

        virtual std::vector<std::string> getAllIdents( OperationContext* opCtx ) const override;

        virtual bool supportsDocLocking() const override {
            return true;
        }

        virtual bool supportsDirectoryPerDB() const override {
            return false;
        }

        virtual int flushAllFiles(OperationContext* opCtx, bool sync) override;

        virtual Status beginBackup(OperationContext* opCtx) override;

        virtual void endBackup(OperationContext* opCtx) override;

        virtual Status hotBackup(const std::string& path);

        virtual bool isDurable() const override { return _durable; }

        virtual bool isEphemeral() const override { return false; }

        virtual int64_t getIdentSize(OperationContext* opCtx, StringData ident);

        virtual Status repairIdent(OperationContext* opCtx,
                                    StringData ident) {
            return Status::OK();
        }

        virtual void cleanShutdown();

        virtual SnapshotManager* getSnapshotManager() const final {
            return (SnapshotManager*) &_snapshotManager;
        }

        /**
         * Initializes a background job to remove excess documents in the oplog collections.
         * This applies to the capped collections in the local.oplog.* namespaces (specifically
         * local.oplog.rs for replica sets and local.oplog.$main for master/slave replication).
         * Returns true if a background job is running for the namespace.
         */
        static bool initRsOplogBackgroundThread(StringData ns);

        virtual void setJournalListener(JournalListener* jl);

        virtual Timestamp getAllCommittedTimestamp() const override {
            MONGO_UNREACHABLE;
        }

        virtual Timestamp getOldestOpenReadTimestamp() const override {
            return Timestamp();
        }

        boost::optional<Timestamp> getOplogNeededForCrashRecovery() const final {
            return boost::none;
        }

        // rocks specific api

        rocksdb::DB* getDB() { return _db.get(); }
        const rocksdb::DB* getDB() const { return _db.get(); }
        size_t getBlockCacheUsage() const { return _block_cache->GetUsage(); }
        std::shared_ptr<rocksdb::Cache> getBlockCache() { return _block_cache; }

        RocksTransactionEngine* getTransactionEngine() { return &_transactionEngine; }

        RocksCompactionScheduler* getCompactionScheduler() const { return _compactionScheduler.get(); }

        int getMaxWriteMBPerSec() const { return _maxWriteMBPerSec; }
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

        std::unique_ptr<RocksDurabilityManager> _durabilityManager;
        class RocksJournalFlusher;
        std::unique_ptr<RocksJournalFlusher> _journalFlusher;  // Depends on _durabilityManager
    };

}
