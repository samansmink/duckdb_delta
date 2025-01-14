#include "storage/delta_insert.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "functions/delta_scan.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

#include "storage/delta_catalog.hpp"
#include "storage/delta_transaction.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "storage/delta_table_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

DeltaInsert::DeltaInsert(LogicalOperator &op, TableCatalogEntry &table_p, physical_index_vector_t<idx_t> column_index_map_p,
    vector<LogicalType> types, CopyFunction function_p, unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality) :
    PhysicalCopyToFile(std::move(types), std::move(function_p), std::move(bind_data), estimated_cardinality), table(&table_p), schema(nullptr), column_index_map(std::move(column_index_map_p)) {

    type = PhysicalOperatorType::EXTENSION;
}

DeltaInsert::DeltaInsert(LogicalOperator &op, SchemaCatalogEntry &schema_p, unique_ptr<BoundCreateTableInfo> info,
        vector<LogicalType> types, CopyFunction function_p, unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality) :
        PhysicalCopyToFile(std::move(types), std::move(function_p), std::move(bind_data), estimated_cardinality), table(nullptr), schema(&schema_p), info(std::move(info)) {

    type = PhysicalOperatorType::EXTENSION;
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class DeltaInsertGlobalState : public GlobalSinkState {
public:
	explicit DeltaInsertGlobalState(ClientContext &context, DeltaTableEntry &table,
	                                const vector<LogicalType> &varchar_types)
	    : table(table), insert_count(0) {
	}

	DeltaTableEntry &table;
	idx_t insert_count;
};

// unique_ptr<GlobalSinkState> DeltaInsert::GetGlobalSinkState(ClientContext &context) const {
// 	return physical_copy_to_file->GetGlobalSinkState(context);
// }
//
// unique_ptr<LocalSinkState> DeltaInsert::GetLocalSinkState(ExecutionContext &context) const {
//     return physical_copy_to_file->GetLocalSinkState(context);
// }

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// SinkResultType DeltaInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
//     return physical_copy_to_file->Sink(context, chunk, input);
// }
//
// SinkCombineResultType DeltaInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
//     return physical_copy_to_file->Combine(context, input);
// }

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType DeltaInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
    // Call base class to finalize the PhysicalCopy
    auto res = PhysicalCopyToFile::Finalize(pipeline, event, context, input);

    if (res != SinkFinalizeType::READY) {
        throw NotImplementedException("Unknown SinkFinalizeType in DeltaInsert::Finalize: %s", EnumUtil::ToString(res));
    }

    auto &copy_global_state = input.global_state.Cast<CopyToFunctionGlobalState>();

    auto &transaction = DeltaTransaction::Get(context, table->catalog);
    vector<string> filenames;
    for (const auto& filename : copy_global_state.file_names) {
        filenames.push_back(filename.ToString());
    }
    transaction.Append(filenames);

    return res;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//


//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DeltaInsert::GetName() const {
	return table ? "DELTA_INSERT" : "DELTA_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> DeltaInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
static optional_ptr<CopyFunctionCatalogEntry> TryGetCopyFunction(DatabaseInstance &db, const string &name) {
    D_ASSERT(!name.empty());
    auto &system_catalog = Catalog::GetSystemCatalog(db);
    auto data = CatalogTransaction::GetSystemTransaction(db);
    auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
    return schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, name)->Cast<CopyFunctionCatalogEntry>();
}

unique_ptr<PhysicalOperator> DeltaCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                      unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into Delta table");
	}
	if (op.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into Delta table");
	}

    string delta_path =  op.table.Cast<DeltaTableEntry>().snapshot->GetPaths()[0]; // TODO unsafe?

    // Create Copy Info
    auto info = make_uniq<CopyInfo>();
    info->file_path = delta_path;
    info->format = "parquet";
    info->is_from = false;

    // Get Parquet Copy function
    auto copy_fun = TryGetCopyFunction(*context.db, "parquet");
    if (!copy_fun) {
        throw MissingExtensionException("Did not find parquet copy function required to write to delta table");
    }

    // Bind Copy Function
    auto &columns = op.table.Cast<DeltaTableEntry>().GetColumns();
    CopyFunctionBindInput bind_input(*info);
    auto function_data = copy_fun->function.copy_to_bind(context, bind_input, columns.GetColumnNames(), columns.GetColumnTypes());

    auto insert = make_uniq<DeltaInsert>(op, op.table, op.column_index_map,
        GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::CHANGED_ROWS), copy_fun->function, std::move(function_data), op.estimated_cardinality);

    insert->use_tmp_file = false;
    insert->file_path = delta_path;
    insert->filename_pattern.SetFilenamePattern("duckdb_data_file_{uuid}");
    insert->file_extension = "parquet";
    insert->overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
    insert->per_thread_output = true;
    insert->rotate = false;
    insert->return_type = CopyFunctionReturnType::CHANGED_ROWS;
    insert->partition_output = false;
    insert->write_partition_columns = false;
    insert->names = {};
    insert->expected_types = columns.GetColumnTypes();
    insert->children.push_back(std::move(plan));

	return std::move(insert);
}

unique_ptr<PhysicalOperator> DeltaCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                             unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("DeltaCatalog::PlanCreateTableAs");
	// auto insert = make_uniq<DeltaInsert>(op, op.schema, std::move(op.info));
	// insert->children.push_back(std::move(plan));
	// return std::move(insert);
}

} // namespace duckdb
