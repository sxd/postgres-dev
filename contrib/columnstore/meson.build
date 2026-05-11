# Copyright (c) 2022-2026, PostgreSQL Global Development Group

columnstore_sources = files(
  'columnstore.c',
  'cs_compact.c',
  'cs_insert.c',
  'cs_reloptions.c',
  'cs_scan.c',
  'cs_slot.c',
  'cs_storage.c',
  'cs_vacuum.c',
)

if host_system == 'windows'
  columnstore_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'columnstore',
    '--FILEDESC', 'columnstore - columnar storage table access method',])
endif

columnstore = shared_module('columnstore',
  columnstore_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += columnstore

install_data(
  'columnstore.control',
  'columnstore--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'columnstore',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'columnstore',
      'cs_compaction',
      'cs_delete_update',
      'cs_delta_columnar',
      'cs_dictionary',
      'cs_encoding',
      'cs_index',
      'cs_misc',
      'cs_pushdown',
      'cs_scan_paths',
      'cs_zonemap',
    ],
  },
  'isolation': {
    'specs': [
      'cs_vacuum_inflight',
      'cs_index_vacuum_window',
      'cs_delete_visibility',
      'cs_select_for_update',
      'cs_vacuum_concurrent',
      'cs_vacuum_snapshot',
      'cs_on_conflict_rr',
      'cs_index_visibility',
      'cs_unique_dirty',
      'cs_lock_for_update',
      'cs_fk_locking',
    ],
  },
  'tap': {
    'tests': [
      't/001_concurrent_inserts.pl',
      't/002_wal_consistency.pl',
      't/003_two_phase.pl',
    ],
  },
}
