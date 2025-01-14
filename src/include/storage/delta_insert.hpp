//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

class DeltaInsert : public PhysicalCopyToFile {
public:
    DeltaInsert(LogicalOperator &op, TableCatalogEntry &table_p, physical_index_vector_t<idx_t> column_index_map_p,
        vector<LogicalType> types, CopyFunction function_p, unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality);

    DeltaInsert(LogicalOperator &op, SchemaCatalogEntry &schema_p, unique_ptr<BoundCreateTableInfo> info,
        vector<LogicalType> types, CopyFunction function_p, unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality);

	//! The table to insert into
	optional_ptr<TableCatalogEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! column_index_map
	physical_index_vector_t<idx_t> column_index_map;
    //! The physical copy used internally by this insert
    unique_ptr<PhysicalOperator> physical_copy_to_file;

public:
	// // Source interface
	// SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	//
	// bool IsSource() const override {
	// 	return true;
	// }

public:
	// Sink interface
    // SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    // SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;
    // unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
    // unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	// bool IsSink() const override {
	// 	return true;
	// }
 //
	// bool ParallelSink() const override {
	// 	return true;
	// }
 //
 //    bool SinkOrderDependent() const override {
	//     return true;
	// }

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
