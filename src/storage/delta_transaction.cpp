#include "storage/delta_transaction.hpp"

#include <duckdb/main/client_data.hpp>

#include "storage/delta_catalog.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "functions/delta_scan.hpp"
#include "storage/delta_table_entry.hpp"

namespace duckdb {

DeltaTransaction::DeltaTransaction(DeltaCatalog &delta_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(delta_catalog.access_mode) {
}

DeltaTransaction::~DeltaTransaction() {
}

void DeltaTransaction::Start() {
	transaction_state = DeltaTransactionState::TRANSACTION_NOT_YET_STARTED;
}

static void *allocate_string(const struct ffi::KernelStringSlice slice) {
    return new string(slice.ptr, slice.len);
}

struct CommitInfo {
    static vector<LogicalType> GetTypes() {
        return {LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)};
    };
    static vector<string> GetNames() {
        return {"engineCommitInfo"};
    };

    CommitInfo() {
        buffer.Initialize(Allocator::DefaultAllocator(), GetTypes());
    }

    void Append(Value commit_info_map) {
        idx_t current_size = buffer.size();
        idx_t current_capacity = buffer.GetCapacity();

        if (current_size == current_capacity) {
            buffer.SetCapacity(2*current_capacity);
        }

        buffer.SetValue(0, current_size, commit_info_map);
        buffer.SetCardinality(current_size+1);
    }

    ffi::ArrowFFIData ToArrow(ClientContext &context) {
        ffi::ArrowFFIData ffi_data;
        unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
        ClientProperties props("UTC", ArrowOffsetSize::REGULAR, false, false, false, context);
        ArrowConverter::ToArrowArray(buffer, (ArrowArray*)(&ffi_data.array), props, extension_types);
        ArrowConverter::ToArrowSchema((ArrowSchema*)(&ffi_data.schema), GetTypes(), GetNames(), props);
        return ffi_data;
    }

    DataChunk buffer;
};

struct WriteMetaData {
    static vector<LogicalType> GetTypes() {
        return {
            LogicalType::VARCHAR,
            LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR),
            LogicalType::BIGINT,
            LogicalType::BIGINT,
            LogicalType::BOOLEAN,
        };
    };
    static vector<string> GetNames() {
        return {
            "path",
            "partitionValues",
            "size",
            "modificationTime",
            "dataChange"
        };
    };

    WriteMetaData() {
        buffer.Initialize(Allocator::DefaultAllocator(), GetTypes());
    }

    void Append(const string &path, Value partition_values, idx_t size, idx_t modification_time, bool data_change) {
        idx_t current_size = buffer.size();
        idx_t current_capacity = buffer.GetCapacity();

        if (current_size == current_capacity) {
            buffer.SetCapacity(2*current_capacity);
        }

        buffer.SetValue(0, current_size, path);
        buffer.SetValue(1, current_size, partition_values);
        buffer.SetValue(2, current_size, Value::BIGINT(size));
        buffer.SetValue(3, current_size, Value::BIGINT(modification_time));
        buffer.SetValue(4, current_size, data_change);
        buffer.SetCardinality(current_size+1);
    }

    ffi::ArrowFFIData ToArrow(ClientContext &context) {
        ffi::ArrowFFIData ffi_data;
        unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
        ClientProperties props("UTC", ArrowOffsetSize::REGULAR, false, false, false, context);
        ArrowConverter::ToArrowArray(buffer, (ArrowArray*)(&ffi_data.array), props, extension_types);
        ArrowConverter::ToArrowSchema((ArrowSchema*)(&ffi_data.schema), GetTypes(), GetNames(), props);
        return ffi_data;
    }

    DataChunk buffer;
};

void DeltaTransaction::Commit(ClientContext &context) {
	if (transaction_state == DeltaTransactionState::TRANSACTION_STARTED) {
		transaction_state = DeltaTransactionState::TRANSACTION_FINISHED;

	    if (!outstanding_appends.empty()) {
	        // Create commit info
	        CommitInfo commit_info;
	        commit_info.Append(Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, {Value("engineInfo")}, {Value("DuckDB")}));
	        auto commit_info_arrow = commit_info.ToArrow(context);

	        // Convert arrow to Engine Data
	        KernelEngineData commit_info_engine_data = table_entry->snapshot->TryUnpackKernelResult(ffi::get_engine_data(&commit_info_arrow, table_entry->snapshot->extern_engine.get()));

	        KernelExclusiveTransaction transction_with_info = ffi::with_commit_info(kernel_transaction.release(), commit_info_engine_data.release());

	        auto write_context = ffi::get_write_context(transction_with_info.get());
	        auto write_schema = ffi::get_write_schema(write_context);
	        auto write_path = ffi::get_write_path(write_context, allocate_string);
	        string write_path_string;
	        if (write_path) {
	            write_path_string = *(string*)write_path;
	            delete (string*)write_path;
	        }

	        WriteMetaData meta_data;
	        for (const auto &file : outstanding_appends) {
	            // TODO: how to figure out how many tuples we've written?
	            // TODO: fix paths
	            auto table_path = table_entry->snapshot->GetPaths()[0];
	            auto file_without_double_slash = StringUtil::Replace(file, "\\", "/");
	            // auto file_split = StringUtil::Split(file, "/");
	            // auto file_name = file_split[file_split.size()-1];
	            auto file_name = file.substr(table_path.size());
	            unordered_map<string, string> partitions = {};
	            meta_data.Append(file_name, Value::MAP(partitions), 1, Timestamp::GetCurrentTimestamp().value, true);
	        }

	        auto write_metadata_ffi = meta_data.ToArrow(context);

	        KernelEngineData write_info_engine_data = table_entry->snapshot->TryUnpackKernelResult(ffi::get_engine_data(&write_metadata_ffi, table_entry->snapshot->extern_engine.get()));
	        ffi::add_write_metadata(transction_with_info.get(), write_info_engine_data.release());

	        auto commit_res = table_entry->snapshot->TryUnpackKernelResult(ffi::commit(transction_with_info.release(), table_entry->snapshot->extern_engine.get()));
	    }
	}
}

void DeltaTransaction::Rollback() {
	if (transaction_state == DeltaTransactionState::TRANSACTION_STARTED) {
		transaction_state = DeltaTransactionState::TRANSACTION_FINISHED;
		// NOP: we only support read-only transactions currently
	    // TODO: can we delete files we've written when aborting?
	}
}

void DeltaTransaction::Append(const vector<string> &append_files) {
    if (transaction_state == DeltaTransactionState::TRANSACTION_NOT_YET_STARTED) {
        if (access_mode == AccessMode::READ_ONLY) {
            throw InvalidInputException("Can not append to a read only table");
        }
        transaction_state = DeltaTransactionState::TRANSACTION_STARTED;

        // Start the kernel transaction
        string path =  table_entry->snapshot->GetPaths()[0];
        auto path_slice = KernelUtils::ToDeltaString(path);
        kernel_transaction = table_entry->snapshot->TryUnpackKernelResult(ffi::transaction(path_slice, table_entry->snapshot->extern_engine.get()));
    }

    // Append the newly inserted data
    outstanding_appends.insert(outstanding_appends.end(), append_files.begin(), append_files.end());
}

DeltaTransaction &DeltaTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<DeltaTransaction>();
}

AccessMode DeltaTransaction::GetAccessMode() const {
	return access_mode;
}

} // namespace duckdb
