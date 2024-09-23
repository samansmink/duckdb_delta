#include "storage/delta_schema_entry.hpp"

#include <functions/delta_scan.hpp>
#include <storage/delta_catalog.hpp>

#include "storage/delta_table_entry.hpp"
#include "storage/delta_transaction.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"


namespace duckdb {

DeltaSchemaEntry::DeltaSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {
}

DeltaSchemaEntry::~DeltaSchemaEntry() {
}

DeltaTransaction &GetDeltaTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<DeltaTransaction>();
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    throw BinderException("Delta tables do not support creating tables");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("Delta tables do not support creating functions");
}

void DeltaUnqualifyColumnRef(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, DeltaUnqualifyColumnRef);
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                      TableCatalogEntry &table) {
	throw NotImplementedException("CreateIndex");
}

string GetDeltaCreateView(CreateViewInfo &info) {
	throw NotImplementedException("GetCreateView");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
    throw BinderException("Delta tables do not support creating views");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Delta databases do not support creating types");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("Delta databases do not support creating sequences");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                              CreateTableFunctionInfo &info) {
	throw BinderException("Delta databases do not support creating table functions");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                             CreateCopyFunctionInfo &info) {
	throw BinderException("Delta databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                               CreatePragmaFunctionInfo &info) {
	throw BinderException("Delta databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("Delta databases do not support creating collations");
}

void DeltaSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
    throw NotImplementedException("Delta tables do not support altering");
}

bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return true;
	default:
		return false;
	}
}

void DeltaSchemaEntry::Scan(ClientContext &context, CatalogType type,
                         const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}

    // LoadTable(context);

	callback(*table);
}
void DeltaSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void DeltaSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    throw NotImplementedException("Delta tables do not support dropping");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type,
                                                   const string &name) {
    D_ASSERT(type == CatalogType::TABLE_ENTRY);
    if (type == CatalogType::TABLE_ENTRY && name == DEFAULT_DELTA_TABLE) {

        LoadTable(transaction);

        return *table;
    }
    return nullptr;
}

void DeltaSchemaEntry::LoadTable(CatalogTransaction &transaction) {
    if (table) {
#ifdef DEBUG
        //  Confirm that the transaction is looking at the same version of the table
        auto snapshot = GetDeltaTransaction(transaction).snapshot;
        if (snapshot && table->snapshot) {
            D_ASSERT(snapshot.get() == table->snapshot.get());
        }
#endif
        return;
    }

    auto& delta_transaction = GetDeltaTransaction(transaction);

    // This is the first time we fetch the snapshot during this transaction: we will store this in the transaction
    // making sure that any subsequent reads of the delta table will see the same snapshot
    if (!delta_transaction.snapshot) {
        auto &delta_catalog = catalog.Cast<DeltaCatalog>();
        delta_transaction.snapshot = make_shared_ptr<DeltaSnapshot>(transaction.GetContext(), delta_catalog.GetDBPath());
    }

    // Get the names and types from the delta snapshot
    vector<LogicalType> return_types;
    vector<string> names;
    delta_transaction.snapshot->Bind(return_types, names);

    CreateTableInfo table_info;
    for (idx_t i = 0; i < return_types.size(); i++) {
        table_info.columns.AddColumn(ColumnDefinition(names[i], return_types[i]));
    }
    table = make_uniq<DeltaTableEntry>(catalog, *this, table_info);
    table->snapshot = delta_transaction.snapshot;
}

} // namespace duckdb
