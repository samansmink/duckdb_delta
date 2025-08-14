#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/function_set.hpp"

#include "delta_utils.hpp"
#include "delta_functions.hpp"

namespace duckdb {

struct TransactionVersionData : public GlobalTableFunctionState {
    TransactionVersionData() : finished(false) {
    }

    bool finished;
};

class TransactionVersionBindData : public TableFunctionData {
public:
    TransactionVersionBindData() {
    }

    string table_name;
    string app_id;
    idx_t version;
    idx_t expected_version;
};

static void DeltaGetTransactionVersionFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    // const auto &bind_data = data.bind_data->Cast<TransactionVersionBindData>();
    auto &global_state = data.global_state->Cast<TransactionVersionData>();
    if (global_state.finished) {
        return;
    }

    // TODO
    // - lookup table
    // - lookup version

    idx_t version = 1337;

    output.SetValue(0,0, Value::UBIGINT(version));
    output.SetCardinality(1);

    global_state.finished = true;
}

static void DeltaSetTransactionVersionFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    // const auto &bind_data = data.bind_data->Cast<TransactionVersionBindData>();
    auto &global_state = data.global_state->Cast<TransactionVersionData>();
    if (global_state.finished) {
        return;
    }

    // - lookup table
    // TODO
    // - lookup version

    idx_t version = 1337;

    output.SetValue(0,0, Value::UBIGINT(version));
    output.SetCardinality(1);

    global_state.finished = true;
}

static unique_ptr<FunctionData> DeltaGetTransactionVersionBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
    auto res = make_uniq<TransactionVersionBindData>();

    res->table_name = input.inputs[0].GetValue<string>();
    res->app_id = input.inputs[1].GetValue<string>();

    return_types.emplace_back(LogicalType::UBIGINT);
    names.emplace_back("version");

    return std::move(res);
}

static unique_ptr<FunctionData> DeltaSetTransactionVersionBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
    auto res = make_uniq<TransactionVersionBindData>();

    res->app_id = input.inputs[0].GetValue<string>();
    res->version = input.inputs[1].GetValue<idx_t>();
    res->expected_version = input.inputs[2].GetValue<idx_t>();

    return_types.emplace_back(LogicalType::BOOLEAN);
    names.emplace_back("Success");

    return std::move(res);
}

static unique_ptr<GlobalTableFunctionState> TransactionInitGlobalState(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<TransactionVersionData>();
}

vector<TableFunction> DeltaFunctions::GetTransactionIdempotencyHelpers(DatabaseInstance &instance) {
	vector<TableFunction> result;
	result.push_back(TableFunction("delta_get_transaction_version", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DeltaGetTransactionVersionFunction, DeltaGetTransactionVersionBind, TransactionInitGlobalState));
	result.push_back(TableFunction("delta_set_transaction_version", {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT}, DeltaSetTransactionVersionFunction, DeltaSetTransactionVersionBind, TransactionInitGlobalState));
	return result;
}

} // namespace duckdb
