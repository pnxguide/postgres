// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <string>

#include "dog.h"
#include "third_party/nlohmann/json.hpp"

extern "C" {
#include "access/relation.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "postgres.h"
#include "utils/rel.h"
}

using json = nlohmann::json;

std::string get_filepath(Oid foreigntableid) {
    ForeignTable *table;
    table = GetForeignTable(foreigntableid);

    std::string filepath;

    ListCell *lc;
    foreach (lc, table->options) {
        DefElem *def = (DefElem *)lfirst(lc);
        if (strcmp(def->defname, "filename") == 0) {
            filepath = defGetString(def);
        }
    }

    return filepath;
}

json get_foreign_table_metadata(std::string filename) {
    // Need to read them from the file
    std::string raw_data;
    std::string metadata;
    uint32_t metadata_size;
    constexpr uint32_t metadata_size_size = 4;

    // Read the file using filename
    std::FILE *db_file = std::fopen(filename.c_str(), "rb");
    if (db_file) {
        // Get "Metadata Size"
        std::fseek(db_file, -1 * (int)metadata_size_size, SEEK_END);
        assert(std::fread(&metadata_size, sizeof(uint32_t), 1, db_file) == 1);
        elog(INFO, "Metadata Size \t %d", metadata_size);

        // Get "Metadata"
        metadata.resize(metadata_size);
        std::fseek(db_file, -1 * (int)(metadata_size + metadata_size_size),
                   SEEK_END);
        assert(std::fread(&metadata[0], 1, metadata_size, db_file) ==
               metadata_size);

        // Get "Raw Data"
        std::fseek(db_file, 0, SEEK_END);
        raw_data.resize(std::ftell(db_file) -
                        (metadata_size + metadata_size_size));
        std::rewind(db_file);
        assert(std::fread(&raw_data[0], 1, raw_data.size(), db_file) ==
               raw_data.size());

        std::fclose(db_file);
    }

    return json::parse(metadata);
}

List *list_columns(List *target) {
    List *column_list = NIL;

    List *target_column_list = target;
    ListCell *target_column_cell = NULL;
    foreach (target_column_cell, target_column_list) {
        List *target_var_list = NIL;
        Node *target_expr = (Node *)lfirst(target_column_cell);

        target_var_list = pull_var_clause(
            target_expr, PVC_RECURSE_AGGREGATES | PVC_RECURSE_PLACEHOLDERS);

        column_list = list_union(column_list, target_var_list);
    }

    return column_list;
}

std::set<AttrNumber> list_projected_columns(RelOptInfo *baserel,
                                            Oid foreigntableid) {
    Relation relation = relation_open(foreigntableid, AccessShareLock);

    // List columns from baserel->reltarget->exprs
    // Have to be emitted by ForeignScan
    std::set<AttrNumber> column_list;

    elog(INFO, "list_projected_columns (1)");

    List *target_column_list = baserel->reltarget->exprs;
    ListCell *target_column_cell = NULL;
    foreach (target_column_cell, target_column_list) {
        elog(INFO, "list_projected_columns (2)");

        List *target_var_list = NIL;
        Node *target_expr = (Node *)lfirst(target_column_cell);
        target_var_list = pull_var_clause(
            target_expr, PVC_RECURSE_AGGREGATES | PVC_RECURSE_PLACEHOLDERS);

        elog(INFO, "list_projected_columns (3)");

        ListCell *target_var_cell = NULL;
        foreach (target_var_cell, target_var_list) {
            Var *attr = (Var *)lfirst(target_var_cell);
            column_list.insert(attr->varattno - 1);
        }
    }

    elog(INFO, "list_projected_columns (4)");

    relation_close(relation, AccessShareLock);

    elog(INFO, "list_projected_columns (5)");

    return column_list;
}

List *list_restricted_columns(RelOptInfo *baserel, Oid foreigntableid) {
    Relation relation = relation_open(foreigntableid, AccessShareLock);

    // List columns from baserel->reltarget->exprs
    // Have to be emitted by ForeignScan
    List *column_list = list_columns(baserel->baserestrictinfo);

    relation_close(relation, AccessShareLock);

    return column_list;
}

void print_column_list(List *column_list) {
    ListCell *column;
    foreach (column, column_list) {
        Node *target_var = (Node *)(column->ptr_value);
        elog(INFO, "column\t %s", nodeToString(target_var));
    }
}

Cardinality estimate_num_rows(RelOptInfo *baserel, Oid foreigntableid) {
    // TODO(Guide): Implement this! (Optional)

    // List *projected_columns = list_projected_columns(baserel,
    // foreigntableid); List *restricted_columns =
    // list_restricted_columns(baserel, foreigntableid); List *filtered_columns
    // = list_union(projected_columns, restricted_columns);

    // Restricted columns

    return 0.0;
}

struct db721_PlanState {
    std::string filename;
    std::set<AttrNumber> used_columns;
};

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid) {
    /**
     * This function should update baserel->rows to be the expected number of
     * rows returned by the table scan, after accounting for the filtering done
     * by the restriction quals.
     *
     * Source: https://www.postgresql.org/docs/15/fdw-callbacks.html
     */

    // Estimate a number of rows for a foreign table
    baserel->rows = estimate_num_rows(baserel, foreigntableid);
}

// // TODO: Predicate pushdown
// ListCell *lc;
// foreach (lc, baserel->baserestrictinfo) {
//     Expr *clause = (Expr *)lfirst(lc);
//     OpExpr *expr;
//     Expr *left, *right;

//     if (IsA(clause, RestrictInfo)) {

//     }

//     if (IsA(clause, OpExpr)) {
//         // TODO(pnx): Only need to support Variable OP Constant

//     }

//     else {
//         continue;
//     }
// }

// // TODO: Projection pushdown
// List *projection_list = NIL;
// List *target_column_list = baserel->reltarget->exprs;
// ListCell *target_column_cell = NULL;
// foreach (target_column_cell, target_column_list) {
//     List *target_var_list = NIL;
//     Node *target_expr = (Node *)lfirst(target_column_cell);
//     target_var_list = pull_var_clause(
//         target_expr, PVC_RECURSE_AGGREGATES | PVC_RECURSE_PLACEHOLDERS);
//     projection_list = list_union(projection_list, target_var_list);
// }

// ListCell *projection;
// foreach (projection, projection_list) {
//     Node *target_var = (Node *)(projection->ptr_value);
//     elog(INFO, "projection  \t %s", nodeToString(target_var));
// }

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
    /**
     * This function must generate at least one access path (ForeignPath node)
     * for a scan on the foreign table and must call add_path to add each such
     * path to baserel->pathlist.
     *
     * Source: https://www.postgresql.org/docs/15/fdw-callbacks.html
     */

    // Create a path to a foreign table
    Path *foreign_scan_path = NULL;
    foreign_scan_path = (Path *)create_foreignscan_path(
        root, baserel, NULL,          /* path target */
        baserel->rows, 0.0, 0.0, NIL, /* no known ordering */
        NULL,                         /* not parameterized */
        NULL,                         /* no outer path */
        NIL);                         /* no fdw_private */
    add_path(baserel, foreign_scan_path);
}

extern "C" ForeignScan *db721_GetForeignPlan(
    PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
    ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan) {
    elog(INFO, "db721_GetForeignPlan (1)");

    // Create a ForeignScan operator implementation in the foreign plan
    // This is based on GetForeignRelSize and GetForeignPaths
    List *foreign_scan_private = NIL;

    // Create db721_PlanState with file and columns used
    db721_PlanState *plan_state =
        (db721_PlanState *)palloc0(sizeof(db721_PlanState));

    elog(INFO, "db721_GetForeignPlan (2)");

    plan_state->filename = get_filepath(foreigntableid);

    elog(INFO, "db721_GetForeignPlan (3)");

    std::set<AttrNumber> query_column_list =
        list_projected_columns(baserel, foreigntableid);
    plan_state->used_columns = query_column_list;

    elog(INFO, "db721_GetForeignPlan (4)");

    foreign_scan_private = (List *)plan_state;

    // Create a ForeignScan
    ForeignScan *foreign_scan = NULL;
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    foreign_scan = make_foreignscan(tlist, scan_clauses, baserel->relid,
                                    NIL, /* no expressions to evaluate */
                                    foreign_scan_private, NIL, NIL,
                                    NULL); /* no outer path */
    
    elog(INFO, "db721_GetForeignPlan (5)");

    return foreign_scan;
}

struct db721_ScanState {
    std::FILE *db_file;
    int count;
};

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
    elog(INFO, "db721_BeginForeignScan");

    ForeignScan *plan = (ForeignScan *)node->ss.ps.plan;
    db721_PlanState *plan_state = (db721_PlanState *)plan->fdw_private;

    db721_ScanState *scan_state =
        (db721_ScanState *)palloc0(sizeof(db721_ScanState));
    scan_state->count = 0;
    scan_state->db_file = NULL;
    scan_state->db_file = std::fopen(plan_state->filename.c_str(), "rb");
    assert(scan_state->db_file != NULL);

    node->fdw_state = (void *)scan_state;
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
    elog(INFO, "db721_IterateForeignScan");

    ForeignScan *plan = (ForeignScan *)node->ss.ps.plan;
    db721_PlanState *plan_state = (db721_PlanState *)plan->fdw_private;
    db721_ScanState *scan_state = (db721_ScanState *)node->fdw_state;

    if (scan_state->count >= 10) {
        return NULL;
    }

    TupleTableSlot *tuple_slot = node->ss.ss_ScanTupleSlot;
    TupleDesc tuple_descriptor = tuple_slot->tts_tupleDescriptor;
    memset(tuple_slot->tts_values, 0,
           plan_state->used_columns.size() * sizeof(Datum));
    memset(tuple_slot->tts_isnull, true,
           plan_state->used_columns.size() * sizeof(bool));

    ExecClearTuple(tuple_slot);
    elog(INFO, "db721_IterateForeignScan Cleared");

    // Read each column
    json table_metadata = get_foreign_table_metadata(plan_state->filename);
    std::string buffer;

    for (AttrNumber column_attno : plan_state->used_columns) {
        std::string column_name = std::string(
            NameStr(TupleDescAttr(tuple_descriptor, column_attno)->attname));
        json column_metadata = table_metadata["Columns"][column_name];
        std::string type = column_metadata["type"];
        uint64_t num_blocks = std::stoul(column_metadata["num_blocks"].dump());
        uint64_t start_offset =
            std::stoul(column_metadata["start_offset"].dump());

        elog(INFO, "db721_IterateForeignScan %s %s", column_name.c_str(),
             type.c_str());

        Datum attribute = 0;

        if (type == "str") {
            int32_t vallen = 32;
            char value[32] = "H\0";

            int64 bytea_len = vallen + VARHDRSZ;
            bytea *b = (bytea *)palloc0(bytea_len);
            SET_VARSIZE(b, bytea_len);
            memcpy(VARDATA(b), value, vallen);

            attribute = PointerGetDatum(b);
        } else if (type == "float") {
            attribute = Float4GetDatum((float)scan_state->count);
        } else if (type == "int") {
            attribute = Int32GetDatum(scan_state->count);
        }

        tuple_slot->tts_values[column_attno] = attribute;
        tuple_slot->tts_isnull[column_attno] = false;

        // std::fseek(scan_state->db_file, 0, SEEK_SET);
        // assert(std::fread(&buffer, sizeof(uint32_t), 1, scan_state->db_file)
        // == 1);
    }
    elog(INFO, "db721_IterateForeignScan Stored");

    scan_state->count++;
    ExecStoreVirtualTuple(tuple_slot);
    elog(INFO, "db721_IterateForeignScan Virtual Tuple");

    return tuple_slot;
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
    elog(INFO, "db721_EndForeignScan");

    db721_ScanState *scan_state = (db721_ScanState *)node->fdw_state;

    if (scan_state != NULL) {
        std::fclose(scan_state->db_file);
        pfree(scan_state);
    }
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
    elog(INFO, "db721_ReScanForeignScan");

    db721_EndForeignScan(node);
    db721_BeginForeignScan(node, 0);
}