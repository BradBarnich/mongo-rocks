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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "rocks_recovery_unit.h"
#include "rocks_snapshot_manager.h"

#include <rocksdb/db.h>

#include "mongo/base/checked_cast.h"
#include "mongo/logv2/log.h"

namespace mongo {
void RocksSnapshotManager::setCommittedSnapshot(const Timestamp& timestamp) {
    stdx::lock_guard<Latch> lock(_committedSnapshotMutex);

    invariant(!_committedSnapshot || *_committedSnapshot <= timestamp);
    _committedSnapshot = timestamp;
}

void RocksSnapshotManager::setLocalSnapshot(const Timestamp& timestamp) {
    stdx::lock_guard<Latch> lock(_localSnapshotMutex);
    if (timestamp.isNull())
        _localSnapshot = boost::none;
    else
        _localSnapshot = timestamp;
}

boost::optional<Timestamp> RocksSnapshotManager::getLocalSnapshot() {
    stdx::lock_guard<Latch> lock(_localSnapshotMutex);
    return _localSnapshot;
}

void RocksSnapshotManager::dropAllSnapshots() {
    stdx::lock_guard<Latch> lock(_committedSnapshotMutex);
    _committedSnapshot = boost::none;
}

bool RocksSnapshotManager::haveCommittedSnapshot() const {
    stdx::lock_guard<Latch> lock(_committedSnapshotMutex);
    return bool(_committedSnapshot);
}

boost::optional<Timestamp> RocksSnapshotManager::getCommittedSnapshot() const {
    stdx::lock_guard<Latch> lock(_committedSnapshotMutex);

    uassert(ErrorCodes::ReadConcernMajorityNotAvailableYet,
            "Committed view disappeared while running operation",
            _committedSnapshot);

    return _committedSnapshot;
}

RocksSnapshotManager::SnapshotHolder::SnapshotHolder(OperationContext* opCtx, uint64_t name_) {
    name = name_;
    auto rru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
    snapshot = rru->getPreparedSnapshot();
    db = rru->getDB();
}

RocksSnapshotManager::SnapshotHolder::~SnapshotHolder() {
    if (snapshot != nullptr) {
        invariant(db != nullptr);
        db->ReleaseSnapshot(snapshot);
    }
}

}  // namespace mongo
