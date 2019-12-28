Import("env")

env = env.Clone()

env.Library(
    target= 'storage_rocks_global_options',
    source= [
        'src/rocks_global_options.cpp',
        ],
    LIBDEPS_PRIVATE= [
        '$BUILD_DIR/mongo/util/options_parser/options_parser',
        ],
    )

baselibs = ["rocksdb","zstd","lz4","z","bz2"];
if env.TargetOSIs('windows'):
    baselibs.extend(["Shlwapi","Rpcrt4"])

env.Library(
    target= 'storage_rocks_base',
    source= [
        'src/rocks_compaction_scheduler.cpp',
        'src/rocks_counter_manager.cpp',
        'src/rocks_engine.cpp',
        'src/rocks_record_store.cpp',
        'src/rocks_record_store_mongod.cpp',
        'src/rocks_recovery_unit.cpp',
        'src/rocks_index.cpp',
        'src/rocks_durability_manager.cpp',
        'src/rocks_transaction.cpp',
        'src/rocks_snapshot_manager.cpp',
        'src/rocks_util.cpp',
        ],
    LIBDEPS= [
        '$BUILD_DIR/mongo/base',
        '$BUILD_DIR/mongo/db/namespace_string',
        '$BUILD_DIR/mongo/db/catalog/collection_options',
        '$BUILD_DIR/mongo/db/concurrency/lock_manager',
        '$BUILD_DIR/mongo/db/concurrency/write_conflict_exception',
        '$BUILD_DIR/mongo/db/index/index_descriptor',
        '$BUILD_DIR/mongo/db/server_options_core',
        '$BUILD_DIR/mongo/db/storage/bson_collection_catalog_entry',
        '$BUILD_DIR/mongo/db/storage/index_entry_comparison',
        '$BUILD_DIR/mongo/db/storage/journal_listener',
        '$BUILD_DIR/mongo/db/storage/key_string',
        '$BUILD_DIR/mongo/db/storage/oplog_hack',
        '$BUILD_DIR/mongo/util/background_job',
        '$BUILD_DIR/mongo/util/concurrency/ticketholder',
        '$BUILD_DIR/mongo/util/processinfo',
        '$BUILD_DIR/third_party/shim_snappy',
        'storage_rocks_global_options',
        ],
    LIBDEPS_PRIVATE= [
        '$BUILD_DIR/mongo/db/db_raii',
        '$BUILD_DIR/mongo/db/commands/server_status',
        '$BUILD_DIR/mongo/db/snapshot_window_options',
        '$BUILD_DIR/mongo/db/storage/storage_repair_observer',
        '$BUILD_DIR/mongo/util/options_parser/options_parser',
        '$BUILD_DIR/mongo/util/debugger',
        ],
    SYSLIBDEPS=baselibs
    )

env.Library(
    target= 'storage_rocks',
    source= [
        'src/rocks_init.cpp',
        'src/rocks_options_init.cpp',
        'src/rocks_parameters.cpp',
        #'src/rocks_record_store_mongod.cpp',
        'src/rocks_server_status.cpp',
        'src/rocks_stats_parser.cpp',
        ],
    LIBDEPS= [
        'storage_rocks_base',
        '$BUILD_DIR/mongo/db/db_raii',
        '$BUILD_DIR/mongo/db/storage/storage_engine_impl',
        '$BUILD_DIR/mongo/db/storage/storage_engine_lock_file',
        '$BUILD_DIR/mongo/db/storage/storage_engine_metadata',
        ],
    LIBDEPS_PRIVATE= [
        '$BUILD_DIR/mongo/db/catalog/database_holder',
        '$BUILD_DIR/mongo/db/commands/server_status',
        '$BUILD_DIR/mongo/db/concurrency/lock_manager',
        '$BUILD_DIR/mongo/db/storage/storage_engine_common',
        '$BUILD_DIR/mongo/util/options_parser/options_parser',
        ],
    LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/db/serveronly']
    )

env.Library(
    target= 'storage_rocks_mock',
    source= [
        'src/rocks_record_store_mock.cpp',
        ],
    LIBDEPS= [
        'storage_rocks_base',
        # Temporary crutch since the ssl cleanup is hard coded in background.cpp
        '$BUILD_DIR/mongo/util/net/network',
        ]
    )


env.CppUnitTest(
   target='storage_rocks_index_test',
   source=['src/rocks_index_test.cpp'
           ],
   LIBDEPS=[
        'storage_rocks_mock',
        '$BUILD_DIR/mongo/db/storage/durable_catalog_impl',
        '$BUILD_DIR/mongo/db/storage/sorted_data_interface_test_harness'
        ],
    LIBDEPS_PRIVATE=[
        '$BUILD_DIR/mongo/db/auth/authmocks',
        '$BUILD_DIR/mongo/db/repl/replmocks',
        '$BUILD_DIR/mongo/db/repl/repl_coordinator_interface',
        '$BUILD_DIR/mongo/db/service_context_test_fixture',
    ],
   )


env.CppUnitTest(
   target='storage_rocks_record_store_test',
   source=['src/rocks_record_store_test.cpp'
           ],
   LIBDEPS=[
        'storage_rocks_mock',
        '$BUILD_DIR/mongo/db/storage/durable_catalog_impl',
        '$BUILD_DIR/mongo/db/storage/record_store_test_harness'
        ],
    LIBDEPS_PRIVATE=[
        '$BUILD_DIR/mongo/db/auth/authmocks',
        '$BUILD_DIR/mongo/db/repl/replmocks',
        '$BUILD_DIR/mongo/db/repl/repl_coordinator_interface',
        '$BUILD_DIR/mongo/db/service_context_test_fixture',
    ],
   )

env.CppUnitTest(
   target='storage_rocks_engine_test',
   source=['src/rocks_engine_test.cpp'
           ],
   LIBDEPS=[
        'storage_rocks_mock',
        '$BUILD_DIR/mongo/db/storage/kv/kv_engine_test_harness',
        '$BUILD_DIR/mongo/db/storage/storage_options'
        ],
    LIBDEPS_PRIVATE=[
        '$BUILD_DIR/mongo/db/auth/authmocks',
        '$BUILD_DIR/mongo/db/repl/replmocks',
        '$BUILD_DIR/mongo/db/repl/repl_coordinator_interface',
        '$BUILD_DIR/mongo/db/service_context_test_fixture',
    ],
   )

