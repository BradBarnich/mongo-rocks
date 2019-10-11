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

#include "mongo/platform/basic.h"

#include <boost/filesystem/operations.hpp>
#include <memory>
#include <vector>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include "mongo/base/init.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_snapshot_manager.h"
#include "rocks_transaction.h"

namespace mongo {
namespace {

using std::string;
using std::stringstream;
using std::unique_ptr;

class RocksRecordStoreHarnessHelper final : public RecordStoreHarnessHelper {
public:
    RocksRecordStoreHarnessHelper() : _tempDir(_testNamespace) {
        boost::filesystem::remove_all(_tempDir.path());
        rocksdb::DB* db;
        rocksdb::Options options;
        options.comparator = TimestampComparator();
        options.create_if_missing = true;
        auto s = rocksdb::DB::Open(options, _tempDir.path(), &db);
        ASSERT(s.ok());
        _db.reset(db);
        _counterManager.reset(new RocksCounterManager(_db.get(), true));
        _durabilityManager.reset(new RocksDurabilityManager(_db.get(), true));
        _compactionScheduler.reset(new RocksCompactionScheduler());
        _compactionScheduler->start(_db.get());
    }

    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore() {
        return newNonCappedRecordStore("foo.bar");
    }
    std::unique_ptr<RecordStore> newNonCappedRecordStore(const std::string& ns) {
        return stdx::make_unique<RocksRecordStore>(ns,
                                                   "1",
                                                   _db.get(),
                                                   _counterManager.get(),
                                                   _durabilityManager.get(),
                                                   _compactionScheduler.get(),
                                                   "prefix");
    }

    std::unique_ptr<RecordStore> newCappedRecordStore(int64_t cappedMaxSize,
                                                      int64_t cappedMaxDocs) final {
        return newCappedRecordStore("a.b", cappedMaxSize, cappedMaxDocs);
    }

    std::unique_ptr<RecordStore> newCappedRecordStore(const std::string& ns,
                                                      int64_t cappedMaxSize,
                                                      int64_t cappedMaxDocs) {
        return stdx::make_unique<RocksRecordStore>(ns,
                                                   "1",
                                                   _db.get(),
                                                   _counterManager.get(),
                                                   _durabilityManager.get(),
                                                   _compactionScheduler.get(),
                                                   "prefix",
                                                   true,
                                                   cappedMaxSize,
                                                   cappedMaxDocs);
    }

    std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
        return stdx::make_unique<RocksRecoveryUnit>(&_transactionEngine,
                                                    &_snapshotManager,
                                                    _db.get(),
                                                    _counterManager.get(),
                                                    nullptr,
                                                    _durabilityManager.get(),
                                                    true);
    }

    bool supportsDocLocking() final {
        return true;
    }

private:
    string _testNamespace = "mongo-rocks-record-store-test";
    unittest::TempDir _tempDir;
    std::unique_ptr<rocksdb::DB> _db;
    RocksTransactionEngine _transactionEngine;
    RocksSnapshotManager _snapshotManager;
    std::unique_ptr<RocksDurabilityManager> _durabilityManager;
    std::unique_ptr<RocksCounterManager> _counterManager;
    std::unique_ptr<RocksCompactionScheduler> _compactionScheduler;
};

std::unique_ptr<HarnessHelper> makeHarnessHelper() {
    return stdx::make_unique<RocksRecordStoreHarnessHelper>();
}

MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
    mongo::registerHarnessHelperFactory(makeHarnessHelper);
    return Status::OK();
}

TEST(RocksRecordStoreTest, Isolation1) {
    const auto harnessHelper(newRecordStoreHarnessHelper());
    unique_ptr<RecordStore> rs(harnessHelper->newNonCappedRecordStore());

    RecordId id1;
    RecordId id2;

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        {
            WriteUnitOfWork uow(opCtx.get());

            StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, Timestamp());
            ASSERT_OK(res.getStatus());
            id1 = res.getValue();

            res = rs->insertRecord(opCtx.get(), "a", 2, Timestamp());
            ASSERT_OK(res.getStatus());
            id2 = res.getValue();

            uow.commit();
        }
    }

    {
        ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
        auto client2 = harnessHelper->serviceContext()->makeClient("c2");
        auto t2 = harnessHelper->newOperationContext(client2.get());

        unique_ptr<WriteUnitOfWork> w1(new WriteUnitOfWork(t1.get()));
        unique_ptr<WriteUnitOfWork> w2(new WriteUnitOfWork(t2.get()));

        rs->dataFor(t1.get(), id1);
        rs->dataFor(t2.get(), id1);

        ASSERT_OK(rs->updateRecord(t1.get(), id1, "b", 2));
        ASSERT_OK(rs->updateRecord(t1.get(), id2, "B", 2));

        try {
            // this should fail
            rs->updateRecord(t2.get(), id1, "c", 2).transitional_ignore();
            ASSERT(0);
        } catch (WriteConflictException&) {
            w2.reset(NULL);
            t2.reset(NULL);
        }

        w1->commit();  // this should succeed
    }
}

TEST(RocksRecordStoreTest, Isolation2) {
    const auto harnessHelper(newRecordStoreHarnessHelper());
    unique_ptr<RecordStore> rs(harnessHelper->newNonCappedRecordStore());

    RecordId id1;
    RecordId id2;

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        {
            WriteUnitOfWork uow(opCtx.get());

            StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, Timestamp());
            ASSERT_OK(res.getStatus());
            id1 = res.getValue();

            res = rs->insertRecord(opCtx.get(), "a", 2, Timestamp());
            ASSERT_OK(res.getStatus());
            id2 = res.getValue();

            uow.commit();
        }
    }

    {
        ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
        auto client2 = harnessHelper->serviceContext()->makeClient("c2");
        auto t2 = harnessHelper->newOperationContext(client2.get());

        // ensure we start transactions
        rs->dataFor(t1.get(), id2);
        rs->dataFor(t2.get(), id2);

        {
            WriteUnitOfWork w(t1.get());
            ASSERT_OK(rs->updateRecord(t1.get(), id1, "b", 2));
            w.commit();
        }

        {
            WriteUnitOfWork w(t2.get());
            ASSERT_EQUALS(string("a"), rs->dataFor(t2.get(), id1).data());
            try {
                // this should fail as our version of id1 is too old
                rs->updateRecord(t2.get(), id1, "c", 2).transitional_ignore();
                ASSERT(0);
            } catch (WriteConflictException&) {
            }
        }
    }
}

TEST(RocksRecordStoreTest, CappedCursorRollover) {
    unique_ptr<RecordStoreHarnessHelper> harnessHelper(newRecordStoreHarnessHelper());
    unique_ptr<RecordStore> rs(harnessHelper->newCappedRecordStore("a.b", 10000, 5));

    {  // first insert 3 documents
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        for (int i = 0; i < 3; ++i) {
            WriteUnitOfWork uow(opCtx.get());
            StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, Timestamp());
            ASSERT_OK(res.getStatus());
            uow.commit();
        }
    }

    // set up our cursor that should rollover

    auto client2 = harnessHelper->serviceContext()->makeClient("c2");
    auto cursorCtx = harnessHelper->newOperationContext(client2.get());
    auto cursor = rs->getCursor(cursorCtx.get());
    ASSERT(cursor->next());
    cursor->save();
    cursorCtx->recoveryUnit()->abandonSnapshot();

    {  // insert 100 documents which causes rollover
        auto client3 = harnessHelper->serviceContext()->makeClient("c3");
        auto opCtx = harnessHelper->newOperationContext(client3.get());
        for (int i = 0; i < 100; i++) {
            WriteUnitOfWork uow(opCtx.get());
            StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, Timestamp());
            ASSERT_OK(res.getStatus());
            uow.commit();
        }
    }

    // cursor should now be dead
    ASSERT_FALSE(cursor->restore());
    ASSERT(!cursor->next());
}

// TODO: no failpoint
// TEST(WiredTigerRecordStoreTest, OplogDurableVisibilityInOrder)

// TODO: no failpoint
// TEST(WiredTigerRecordStoreTest, OplogDurableVisibilityOutOfOrder)

// TODO: no metadata
// TEST(WiredTigerRecordStoreTest, AppendCustomStatsMetadata)

TEST(RocksRecordStoreTest, CappedCursorYieldFirst) {
    unique_ptr<RecordStoreHarnessHelper> harnessHelper(newRecordStoreHarnessHelper());
    unique_ptr<RecordStore> rs(harnessHelper->newCappedRecordStore("a.b", 10000, 50));

    RecordId id1;

    {  // first insert a document
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        WriteUnitOfWork uow(opCtx.get());
        StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, Timestamp());
        ASSERT_OK(res.getStatus());
        id1 = res.getValue();
        uow.commit();
    }

    ServiceContext::UniqueOperationContext cursorCtx(harnessHelper->newOperationContext());
    auto cursor = rs->getCursor(cursorCtx.get());

    // See that things work if you yield before you first call next().
    cursor->save();
    cursorCtx->recoveryUnit()->abandonSnapshot();
    ASSERT_TRUE(cursor->restore());
    auto record = cursor->next();
    ASSERT(record);
    ASSERT_EQ(id1, record->id);
    ASSERT(!cursor->next());
}

}  // namespace
}  // namespace mongo
