//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "delta_utils.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class DeltaCatalog;
class DeltaSchemaEntry;
class DeltaTableEntry;
class DeltaMultiFileList;

enum class DeltaTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class DeltaTransaction : public Transaction {
public:
	DeltaTransaction(DeltaCatalog &delta_catalog, TransactionManager &manager, ClientContext &context);
	~DeltaTransaction() override;

	void Start();
	void Commit(ClientContext &context);
	void Rollback();

    void Append(const vector<string> &append_files);

	static DeltaTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const;

public:
	optional_ptr<DeltaTableEntry> GetTableEntry();
	DeltaTableEntry &InitializeTableEntry(ClientContext &context, DeltaSchemaEntry &schema_entry);

    vector<string> outstanding_appends;

private:
	mutex lock;
	unique_ptr<DeltaTableEntry> table_entry;

	//	DeltaConnection connection;
	DeltaTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
