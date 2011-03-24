/* file_fixed_length_fdw--1.0.sql */

CREATE FUNCTION file_fixed_length_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION file_fixed_length_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER file_fixed_length_fdw
  HANDLER file_fixed_length_fdw_handler
  VALIDATOR file_fixed_length_fdw_validator;
