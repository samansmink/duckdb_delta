#include "storage/delta_insert.hpp"

#include <duckdb/common/sort/partition_state.hpp>

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

DeltaInsert::DeltaInsert(LogicalOperator &op, TableCatalogEntry &table,
                     physical_index_vector_t<idx_t> column_index_map_p)
: PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
  column_index_map(std::move(column_index_map_p)) {
}

DeltaInsert::DeltaInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class DeltaInsertGlobalState : public GlobalSinkState {
public:
	explicit DeltaInsertGlobalState()
	    : insert_count(0) {
	}
    vector<string> written_files;
	idx_t insert_count; // TODO: this needs to be per file
};

unique_ptr<GlobalSinkState> DeltaInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DeltaInsertGlobalState>();
}

//
// unique_ptr<LocalSinkState> DeltaInsert::GetLocalSinkState(ExecutionContext &context) const {
//     return physical_copy_to_file->GetLocalSinkState(context);
// }

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType DeltaInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    auto &global_state = input.global_state.Cast<DeltaInsertGlobalState>();

    if (chunk.size() != 1) {
        throw InternalException("DeltaInsert::Sink expects a single row containing output of the PhysicalCopy that should be its Source");
    }

    global_state.insert_count += chunk.GetValue(0,0).GetValue<idx_t>();

    auto files = chunk.GetValue(1, 0);
    for (const auto &val : ListValue::GetChildren(files)) {
        global_state.written_files.push_back(val.ToString());
    }

    return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DeltaInsert::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
    auto &global_state = sink_state->Cast<DeltaInsertGlobalState>();
    auto value = Value::BIGINT(global_state.insert_count);
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, value);
    return SourceResultType::FINISHED;
}
//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType DeltaInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
    auto &global_state = input.global_state.Cast<DeltaInsertGlobalState>();

    auto &transaction = DeltaTransaction::Get(context, table->catalog);
    vector<string> filenames;
    transaction.Append(global_state.written_files);

    return SinkFinalizeType::READY;
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

    auto partitions = op.table.Cast<DeltaTableEntry>().snapshot->GetPartitions();
    vector<idx_t> partition_columns;
    if (partitions.size() != 0) {
        auto column_names = op.table.Cast<DeltaTableEntry>().GetColumns().GetColumnNames();
        // TODO: yuck?
        for (int64_t i = 0; i < partitions.size(); i++) {
            for (int64_t j = 0; j < column_names.size(); j++) {
                if (column_names[j] == partitions[i]) {
                    partition_columns.push_back(j);
                    break;
                }
            }
        }
    }


    // Bind Copy Function
    auto &columns = op.table.Cast<DeltaTableEntry>().GetColumns();
    CopyFunctionBindInput bind_input(*info);

    // auto names_to_write = LogicalCopyToFile::GetNamesWithoutPartitions(columns.GetColumnNames(), partition_columns, false);
    // auto types_to_write = LogicalCopyToFile::GetTypesWithoutPartitions(columns.GetColumnTypes(), partition_columns, false);

    auto names_to_write = columns.GetColumnNames();
    auto types_to_write = columns.GetColumnTypes();


    auto function_data = copy_fun->function.copy_to_bind(context, bind_input, names_to_write, types_to_write);

    auto insert = make_uniq<DeltaInsert>(op, op.table, op.column_index_map);;

    auto physical_copy = make_uniq<PhysicalCopyToFile>(GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST), copy_fun->function, std::move(function_data), op.estimated_cardinality);

    auto current_write_uuid = UUID::ToString(UUID::GenerateRandomUUID());

    physical_copy->use_tmp_file = false;
    if (!partition_columns.empty()) {
        physical_copy->filename_pattern.SetFilenamePattern("duckdb_" + current_write_uuid + "_{i}");
        physical_copy->file_path = delta_path;
        physical_copy->partition_output = true;
        physical_copy->partition_columns = partition_columns;
    } else {
        physical_copy->file_path = delta_path + "/duckdb-" + current_write_uuid + ".parquet";
        physical_copy->partition_output = false;
    }

    physical_copy->file_extension = "parquet";
    physical_copy->overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
    physical_copy->per_thread_output = false;
    physical_copy->rotate = false;
    physical_copy->return_type = CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST;
    physical_copy->write_partition_columns = true; // TODO this is wrong! we don't write partition in delta
    physical_copy->children.push_back(std::move(plan));
    physical_copy->names = names_to_write;
    physical_copy->expected_types = types_to_write;

    insert->children.push_back(std::move(physical_copy));

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
