/*-------------------------------------------------------------------------
 *
 * file_fixed_length_fdw.c
 *		  foreign-data wrapper for files with fixed length fields and
 *        no field delimiter/separator
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  file_fixed_length_fdw/file_fixed_length_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "mb/pg_wchar.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"

#define MAX_FIXED_FIELDS 1024

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct FileFixedLengthFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Valid options for file_fixed_length_fdw.
 *
 */
static struct FileFixedLengthFdwOption valid_options[] = {
    /* File options */
    { "filename",       ForeignTableRelationId },
	{ "field_lengths",  ForeignTableRelationId },
	{ "trim",           ForeignTableRelationId }, 
	{ "record_separator",  ForeignTableRelationId },
	{ "encoding",       ForeignTableRelationId }, 
	/* Sentinel */
	{ NULL,			InvalidOid }
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef enum record_sep
{
	RS_NONE, 
	RS_LF,
	RS_CR,
	RS_CRLF
} record_sep;

typedef struct FileFixedLengthFdwExecutionState
{
	char *filename;
	FILE *source;
	long int *field_lengths;
	int   nfields;
	int   total_field_length;
	int read_len;
	record_sep sep;
	int   encoding;
	bool  trim_all_fields;
	/* work space */
	char           *read_buf;
    Datum          *text_array_values;
    bool           *text_array_nulls;	
	int             recnum;
} FileFixedLengthFdwExecutionState;

/*
 * SQL functions*/
extern Datum file_fixed_length_fdw_handler(PG_FUNCTION_ARGS);
extern Datum file_fixed_length_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(file_fixed_length_fdw_handler);
PG_FUNCTION_INFO_V1(file_fixed_length_fdw_validator);

/*
 * FDW callback routines
 */
static FdwPlan *file_fixed_lengthPlanForeignScan(Oid foreigntableid,
									PlannerInfo *root,
									RelOptInfo *baserel);
static void file_fixed_lengthExplainForeignScan(ForeignScanState *node, 
												ExplainState *es);
static void file_fixed_lengthBeginForeignScan(ForeignScanState *node, 
											  int eflags);
static TupleTableSlot *file_fixed_lengthIterateForeignScan(
	ForeignScanState *node);
static void file_fixed_lengthReScanForeignScan(ForeignScanState *node);
static void file_fixed_lengthEndForeignScan(ForeignScanState *node);
static void file_fixed_lengthErrorCallback(void *arg);
;
static void check_table_shape(Relation rel);

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);
static void file_fixed_lengthGetOptions(Oid foreigntableid,
			   char **filename, List **other_options);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
						   const char *filename, const int tuple_width,
						   Cost *startup_cost, Cost *total_cost);
static int getFieldLengths(char *lengths, long int **vals);
static bool NextFixedLengthRawFields(
	FileFixedLengthFdwExecutionState *festate);
static void makeTextArray(FileFixedLengthFdwExecutionState *fdw_private, 
						  TupleTableSlot *slot);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
file_fixed_length_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->PlanForeignScan = file_fixed_lengthPlanForeignScan;
	fdwroutine->ExplainForeignScan = file_fixed_lengthExplainForeignScan;
	fdwroutine->BeginForeignScan = file_fixed_lengthBeginForeignScan;
	fdwroutine->IterateForeignScan = file_fixed_lengthIterateForeignScan;
	fdwroutine->ReScanForeignScan = file_fixed_lengthReScanForeignScan;
	fdwroutine->EndForeignScan = file_fixed_lengthEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
file_fixed_length_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *filename = NULL;
	char       *encoding = NULL;
	long int   *field_lengths = NULL;
	bool        got_trim = false;
    int         rsep = -1;
	ListCell   *cell;

	/*
	 * Only superusers are allowed to set options of a file_fdw foreign table.
	 * This is because the filename is one of those options, and we don't
	 * want non-superusers to be able to determine which file gets read.
	 *
	 * Note that the valid_options[] array disallows setting filename at
	 * any options level other than foreign table --- otherwise there'd
	 * still be a security hole.
	 */
	if (catalog == ForeignTableRelationId && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only a superuser can change options of a file_fixed_length foreign table")));

	/*
	 * Check that only options supported by file_fixed_length_fdw, 
	 * and allowed for the current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem	   *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			struct FileFixedLengthFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s",
							 buf.data)));
		}

		if (strcmp(def->defname, "filename") == 0)
		{
			if (filename)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			filename = defGetString(def);
		}
		else if (strcmp(def->defname, "field_lengths") == 0)
		{
			if (field_lengths)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			(void) getFieldLengths(defGetString(def), &field_lengths);
		}
		else if (strcmp(def->defname, "trim") == 0)
		{
			if (got_trim)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			got_trim = true;
			(void) defGetBoolean(def);
		}
		else if (strcmp(def->defname, "encoding") == 0)
		{
			if (encoding != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			encoding = defGetString(def);
			if (pg_char_to_encoding(encoding) == -1)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid encoding name '%s'", encoding)));
		}
		else if (strcmp(def->defname, "record_separator") == 0)
		{
			char * rsval;
			if (rsep != -1)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			rsval = defGetString(def);
			if (pg_strcasecmp(rsval,"none") != 0 &&
				pg_strcasecmp(rsval,"lf")   != 0 &&
				pg_strcasecmp(rsval,"cr")   != 0 &&
				pg_strcasecmp(rsval,"crlf") != 0)												ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid value for record_separator")));
		}
	}

	if (field_lengths)
		pfree(field_lengths);

	PG_RETURN_VOID();
}


static int
getFieldLengths(char *lengths, long int **vals)
{

	char *start = lengths, *end;
    long int result, *lvals;
	int curval = 0;

	*vals = palloc(MAX_FIXED_FIELDS * sizeof(long int));
	lvals = *vals;

	while (true)
	{
		result = strtol(start,&end,10);
		if (end == start)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("illegal lengths string")));
		if (curval >= MAX_FIXED_FIELDS)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("illegal lengths string")));
		lvals[curval++] = result;
		/* skip trailing spaces */
		while(*end == ' ')
			end++;
		if (*end == '\0')
			return curval;
		if (*end != ',')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("illegal lengths string")));
		start = end + 1;
	}
	
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *option, Oid context)
{
	struct FileFixedLengthFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a file_fdw foreign table.
 *
 * We have to separate out "filename" from the other options because
 * it must not appear in the options list passed to the core COPY code.
 */
static void
file_fixed_lengthGetOptions(Oid foreigntableid,
			   char **filename, List **other_options)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	UserMapping *usermap;
	List	   *options;
	ListCell   *lc,
			   *prev;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * file_fdw doesn't have any options that can be specified there.
	 *
	 * (XXX Actually, given the current contents of valid_options[], there's
	 * no point in examining anything except the foreign table's own options.
	 * Simplify?)
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);
	usermap = GetUserMapping(GetUserId(),table->serverid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);
	options = list_concat(options, usermap->options);
	
	/*
	 * Separate out the filename.
	 */
	*filename = NULL;
	prev = NULL;
	foreach(lc, options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "filename") == 0)
		{
			*filename = defGetString(def);
			options = list_delete_cell(options, lc, prev);
			break;
		}
		prev = lc;
	}
	if (*filename == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("filename is required for file_fdw foreign tables")));
	*other_options = options;
}

/*
 * filePlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
file_fixed_lengthPlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel)
{
	FdwPlan	   *fdwplan;
	char	   *filename;
	List	   *options;
	ListCell   *lc;
	int         tlen = 0;

	/* Fetch options  */
	file_fixed_lengthGetOptions(foreigntableid, &filename, &options);

	foreach(lc, options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "field_lengths") == 0)
		{

			long int *field_lengths;
			int  nfields, i;
			nfields = getFieldLengths(defGetString(def), &field_lengths);
			for (i = 0; i < nfields; i++)
				tlen += field_lengths[i];
			pfree(field_lengths);
		}
	}

	/* 
	 * Construct FdwPlan with cost estimates. Add one for the record
	 * separator. That way we'll be off by at most one, and we save ourselves
	 * the trouble of processing the option to resolve the possible difference.
	 */
	fdwplan = makeNode(FdwPlan);
	estimate_costs(root, baserel, filename, tlen + 1,
				   &fdwplan->startup_cost, &fdwplan->total_cost);
	fdwplan->fdw_private = NIL;				/* not used */

	return fdwplan;
}

/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
file_fixed_lengthExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	char	   *filename;
	List	   *options;

	/* Fetch options  */
	file_fixed_lengthGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &filename, &options);

	ExplainPropertyText("Foreign File", filename, es);

	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
		struct stat		stat_buf;

		if (stat(filename, &stat_buf) == 0)
			ExplainPropertyLong("Foreign File Size", (long) stat_buf.st_size,
								es);
	}
}

/*
 * file_fixed_lengthBeginForeignScan
 *		Initiate access to the file
 */
static void
file_fixed_lengthBeginForeignScan(ForeignScanState *node, int eflags)
{
	char	   *filename;
	List	   *options;
	FileFixedLengthFdwExecutionState *festate;
	ListCell   *lc;
	int         i;

	check_table_shape(node->ss.ss_currentRelation);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Fetch options of foreign table */
	file_fixed_lengthGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &filename, &options);

	/*
	 * Save state in node->fdw_state.
	 */
	festate = (FileFixedLengthFdwExecutionState *) palloc(sizeof(FileFixedLengthFdwExecutionState));
	festate->filename = filename;
	festate->source = AllocateFile(filename,PG_BINARY_R);
	if (festate->source == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("unable to open file")));

	festate->recnum = 0;
	festate->encoding = pg_get_client_encoding();
	festate->sep = RS_LF; /* default */
	festate->trim_all_fields = false;
	foreach(lc, options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "field_lengths") == 0)
		{
			int i, tlen = 0;

			festate->nfields = 
				getFieldLengths(defGetString(def), &(festate->field_lengths));
			for (i = 0; i < festate->nfields; i++)
				tlen += festate->field_lengths[i];
			festate->total_field_length = tlen;
		}
		else if (strcmp(def->defname, "trim") == 0)
		{
			festate->trim_all_fields = defGetBoolean(def);
		}
		else if (strcmp(def->defname, "encoding") == 0)
		{
			festate->encoding = pg_char_to_encoding(defGetString(def));
		}
		else if (strcmp(def->defname, "record_separator") == 0)
		{
			char * rsval;
			rsval = defGetString(def);
			if (pg_strcasecmp(rsval,"none") == 0)
				festate->sep = RS_NONE;
			else if (pg_strcasecmp(rsval,"lf") == 0)
				festate->sep = RS_LF;
			else if (pg_strcasecmp(rsval,"cr") == 0)
				festate->sep = RS_LF;
			else if (pg_strcasecmp(rsval,"crlf") == 0)
				festate->sep = RS_CRLF;
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid value for record_separator")));
		}
	}
	
	if (festate->sep == RS_CRLF)
		festate->read_len = festate->total_field_length + 2;
	else if (festate->sep == RS_NONE)
		festate->read_len = festate->total_field_length ;
	else
		festate->read_len = festate->total_field_length + 1;

	/* set up work space */

	festate->read_buf = palloc(festate->read_len * sizeof(char));
	festate->text_array_values = palloc(festate->nfields * sizeof(Datum));
	festate->text_array_nulls = palloc(festate->nfields * sizeof(bool));
	for (i = 0; i < festate->nfields; i++)
		festate->text_array_nulls[i] = false;

	node->fdw_state = (void *) festate;
}

/*
 * fileIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
file_fixed_lengthIterateForeignScan(ForeignScanState *node)
{
	FileFixedLengthFdwExecutionState *festate = (FileFixedLengthFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	bool			found;
	ErrorContextCallback errcontext;
        

	/* Set up callback to identify error line number. */
	errcontext.callback = file_fixed_lengthErrorCallback;
	errcontext.arg = (void *) festate;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the file,
	 * we just skip the last step, leaving the slot empty as required.
	 *
	 */
	ExecClearTuple(slot);

	festate->recnum += 1;
	found = NextFixedLengthRawFields(festate);

	if (found)
	{
		makeTextArray(festate, slot);
 		ExecStoreVirtualTuple(slot);
	}

	/* Remove error callback. */
	error_context_stack = errcontext.previous;

	return slot;
}

static void
file_fixed_lengthErrorCallback(void *arg)
{

	FileFixedLengthFdwExecutionState *festate = (FileFixedLengthFdwExecutionState *) arg;

	errcontext("Fixed Length Foreign Table filename: '%s' record: %d",
			   festate->filename, festate->recnum);

}

/*
 * fileEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
file_fixed_lengthEndForeignScan(ForeignScanState *node)
{
	FileFixedLengthFdwExecutionState *festate = (FileFixedLengthFdwExecutionState *) node->fdw_state;

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
		FreeFile(festate->source);
}

/*
 * fileReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
file_fixed_lengthReScanForeignScan(ForeignScanState *node)
{
	FileFixedLengthFdwExecutionState *festate = (FileFixedLengthFdwExecutionState *) node->fdw_state;

	FreeFile(festate->source);
	festate->source = AllocateFile(festate->filename,PG_BINARY_R);
	festate->recnum = 0;
}

/*
 * Estimate costs of scanning a foreign table.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   const char *filename, const int tuple_width,
			   Cost *startup_cost, Cost *total_cost)
{
	struct stat		stat_buf;
	BlockNumber		pages;
	double			ntuples;
	double			nrows;
	Cost			run_cost = 0;
	Cost			cpu_per_tuple;

	/*
	 * Get size of the file.  It might not be there at plan time, though,
	 * in which case we have to use a default estimate.
	 */
	if (stat(filename, &stat_buf) < 0)
		stat_buf.st_size = 10 * BLCKSZ;

	/*
	 * Convert size to pages for use in I/O cost estimate below.
	 */
	pages = (stat_buf.st_size + (BLCKSZ-1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;

	/*
	 * calculate the number of tuples in the file.  
	 * Since records are of fixed length we can do this 
	 * with complete accuracy. 
	 */

	ntuples = clamp_row_est((double) stat_buf.st_size / (double) tuple_width);

	/*
	 * Now estimate the number of rows returned by the scan after applying
	 * the baserestrictinfo quals.  This is pretty bogus too, since the
	 * planner will have no stats about the relation, but it's better than
	 * nothing.
	 */
	nrows = ntuples *
		clauselist_selectivity(root,
							   baserel->baserestrictinfo,
							   0,
							   JOIN_INNER,
							   NULL);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;

	/*
	 * Now estimate costs.  We estimate costs almost the same way as
	 * cost_seqscan(), thus assuming that I/O costs are equivalent to a
	 * regular table file of the same size.  However, we take per-tuple CPU
	 * costs as 10x of a seqscan, to account for the cost of parsing records.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}

/*
 * Make sure the table is the right shape. i.e. it must have exactly one column,
 * which must be of type text[]
 */

static void
check_table_shape(Relation rel)
{
	TupleDesc       tupDesc;
	Form_pg_attribute *attr; 
	int         attr_count;
	int         i;
	int         elem1 = -1;

	tupDesc = RelationGetDescr(rel);
	attr = tupDesc->attrs;
	attr_count  = tupDesc->natts;

	for (i = 0; i < attr_count; i++)
	{
		if (attr[i]->attisdropped)
			continue;
		if (elem1 > -1)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("table for file_textarray_fdw foreign tables must have only one column")));
		elem1 = i;
	}
	if (elem1 == -1)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("table for file_textarray_fdw foreign tables must have one column")));
		;
	if (tupDesc->attrs[elem1]->atttypid != TEXTARRAYOID)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("table for file_textarray_fdw foreign tables must consist of a text[] column")));

}

static bool
NextFixedLengthRawFields(FileFixedLengthFdwExecutionState *festate)
{
	int nread;

	nread = fread(festate->read_buf, sizeof(char), festate->read_len, festate->source);
	if (nread == 0 && feof(festate->source))
		return false;
	else if (nread != festate->read_len)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH),
				 errmsg("error reading fixed length record")));

	return true;
}

/*
 * Construct the text array from the read in data, and stash it in the slot 
 */ 

static void 
makeTextArray(FileFixedLengthFdwExecutionState *festate, TupleTableSlot *slot)
{
	Datum     *values;
	bool      *nulls;
	int        dims[1];
	int        lbs[1];
	int        fld;
	Datum      result;
	int        fldct = festate->nfields;
	char      *string = festate->read_buf;
	values = festate->text_array_values;
	nulls = festate->text_array_nulls;

	dims[0] = fldct;
	lbs[0] = 1; /* sql arrays typically start at 1 */

	for (fld=0; fld < fldct; fld++)
	{
		char * start = string;
		int  len = festate->field_lengths[fld];
		int  slen = len, i;

		if (festate->trim_all_fields)
		{
			/* skip leading and trailing spaces if required */
			for (i = 0; i < slen && *start == ' '; i++)
			{
					start++;
					len--;
			}
			while(len > 0 && start[len-1] == ' ')
				len--;
		}

		/*
		 * pg_any_to_server will both validate that the input is
		 * ok in the named encoding and translate it frpm that into the
		 * current server encoding.
		 */
		values[fld] = PointerGetDatum(
			cstring_to_text(pg_any_to_server(start, len, festate->encoding));

		string += festate->field_lengths[fld];
	}

	result = PointerGetDatum(construct_md_array(
								 values, 
								 nulls,
								 1,
								 dims,
								 lbs,
								 TEXTOID,
								 -1,
								 false,
								 'i'));

	slot->tts_values[0] = result;
	slot->tts_isnull[0] = false;

}
