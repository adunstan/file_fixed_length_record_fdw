# contrib/file_fdw/Makefile

MODULES = file_fixed_length_fdw

EXTENSION = file_fixed_length_fdw
DATA = file_fixed_length_fdw--1.0.sql

REGRESS = file_fixed_length_fdw

EXTRA_CLEAN = sql/file_fixed_length_fdw.sql expected/file_fixed_length_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/file_fixed_length_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
