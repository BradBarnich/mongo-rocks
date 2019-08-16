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

#include <forward_list>

#include <rocksdb/db.h>

#include "mongo/db/storage/snapshot_manager.h"

#include "mongo/stdx/mutex.h"

#pragma once

namespace mongo {

class RocksRecoveryUnit;

class RocksSnapshotManager final : public SnapshotManager {
    RocksSnapshotManager(const RocksSnapshotManager&) = delete;
    RocksSnapshotManager& operator=(const RocksSnapshotManager&) = delete;

public:
    struct SnapshotHolder {
        uint64_t name;
        const rocksdb::Snapshot* snapshot;
        rocksdb::DB* db;
        SnapshotHolder(OperationContext* opCtx, uint64_t name_);
        ~SnapshotHolder();
    };

    RocksSnapshotManager() {}

    ~RocksSnapshotManager() {
        dropAllSnapshots();
    }

    void setCommittedSnapshot(const Timestamp& timestamp) final;
    void setLocalSnapshot(const Timestamp& timestamp) final;
    void dropAllSnapshots() final;

    //
    // Rocks-specific members
    //

    bool haveCommittedSnapshot() const;

    boost::optional<Timestamp> getCommittedSnapshot() const;

    boost::optional<Timestamp> getLocalSnapshot() const;

private:
    mutable stdx::mutex _committedSnapshotMutex;  // Guards _committedSnapshot
    boost::optional<Timestamp> _committedSnapshot;

    // Snapshot to use for reads at a local stable timestamp.
    mutable stdx::mutex _localSnapshotMutex;  // Guards _localSnapshot.
    boost::optional<Timestamp> _localSnapshot;
};
} // namespace mongo
