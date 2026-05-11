# Copyright (c) 2022-2026, PostgreSQL Global Development Group

columnstore_sources = files(
  'columnstore.c',
  'cs_insert.c',
  'cs_reloptions.c',
  'cs_scan.c',
  'cs_slot.c',
  'cs_storage.c',
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
    ],
  },
}
