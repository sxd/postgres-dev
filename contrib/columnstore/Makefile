# contrib/columnstore/Makefile

MODULE_big = columnstore
OBJS = \
	$(WIN32RES) \
	columnstore.o \
	cs_compact.o \
	cs_insert.o \
	cs_reloptions.o \
	cs_scan.o \
	cs_slot.o \
	cs_storage.o \
	cs_vacuum.o

EXTENSION = columnstore
DATA = columnstore--1.0.sql
PGFILEDESC = "columnstore - columnar storage table access method"

REGRESS = columnstore cs_compaction cs_delta_columnar cs_dictionary cs_encoding

ISOLATION = \
	cs_vacuum_inflight

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/columnstore
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
