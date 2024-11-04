#pragma once

#include "delta_kernel_ffi.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/planner/expression.hpp"
#include <iostream>
#include "duckdb/parser/expression/constant_expression.hpp"
#include <duckdb/planner/filter/null_filter.hpp>

// TODO: clean up this file as we go

namespace duckdb {

class ExpressionVisitor : public ffi::EngineExpressionVisitor {
    using FieldList = vector<unique_ptr<ParsedExpression>>;

public:
    unique_ptr<vector<unique_ptr<ParsedExpression>>> VisitKernelExpression(const ffi::Handle<ffi::SharedExpression>* expression);

private:
    unordered_map<uintptr_t, unique_ptr<FieldList>> inflight_lists;
    uintptr_t next_id = 1;

    // Literals
    template <typename CPP_TYPE, Value (*CREATE_VALUE_FUN)(CPP_TYPE)>
    static ffi::VisitLiteralFn<CPP_TYPE> VisitPrimitiveLiteral() {
        return (ffi::VisitLiteralFn<CPP_TYPE>) &VisitPrimitiveLiteral<CPP_TYPE, CREATE_VALUE_FUN>;
    }
    template <typename CPP_TYPE, typename CREATE_VALUE_FUN>
    static void VisitPrimitiveLiteral(ExpressionVisitor* state, uintptr_t sibling_list_id, CPP_TYPE value) {
        auto duckdb_value = CREATE_VALUE_FUN(value);
        auto expression = make_uniq<ConstantExpression>(duckdb_value);
        state->AppendToList(sibling_list_id, std::move(expression));
    }
    static void VisitTimestampLiteral(void* state, uintptr_t sibling_list_id, int64_t value);
    static void VisitTimestampNtzLiteral(void* state, uintptr_t sibling_list_id, int64_t value);
    static void VisitDateLiteral(void* state, uintptr_t sibling_list_id, int32_t value);
    static void VisitStringLiteral(void* state, uintptr_t sibling_list_id, ffi::KernelStringSlice value);
    static void VisitBinaryLiteral(void* state, uintptr_t sibling_list_id, const uint8_t *buffer, uintptr_t len);
    static void VisitNullLiteral(void* state, uintptr_t sibling_list_id);
    static void VisitArrayLiteral(void* state, uintptr_t sibling_list_id, uintptr_t child_id);
    static void VisitStructLiteral(void *data, uintptr_t sibling_list_id, uintptr_t child_field_list_value, uintptr_t child_value_list_id);
    static void VisitDecimalLiteral(void *state, uintptr_t sibling_list_id, uint64_t value_ms, uint64_t value_ls, uint8_t precision, uint8_t scale);
    static void VisitColumnExpression(void *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name);
    static void VisitStructExpression(void *state, uintptr_t sibling_list_id, uintptr_t child_list_id);

    template <ExpressionType EXPRESSION_TYPE, typename EXPRESSION_TYPENAME>
    static ffi::VisitVariadicFn VisitUnaryExpression() {
        return (ffi::VisitVariadicFn) &VisitVariadicExpression<EXPRESSION_TYPE, EXPRESSION_TYPENAME, 1>;
    }
    template <ExpressionType EXPRESSION_TYPE, typename EXPRESSION_TYPENAME>
    static ffi::VisitVariadicFn VisitBinaryExpression() {
        return (ffi::VisitVariadicFn) &VisitVariadicExpression<EXPRESSION_TYPE, EXPRESSION_TYPENAME, 2>;
    }
    template <ExpressionType EXPRESSION_TYPE, typename EXPRESSION_TYPENAME, int32_t EXPECTED_CHILDREN = -1>
    static ffi::VisitVariadicFn VisitVariadicExpression() {
        return (ffi::VisitVariadicFn) &VisitVariadicExpression<EXPRESSION_TYPE, EXPRESSION_TYPENAME>;
    }
    template <ExpressionType EXPRESSION_TYPE, typename EXPRESSION_TYPENAME, int32_t EXPECTED_CHILDREN = -1>
    static void VisitVariadicExpression(void *state, uintptr_t sibling_list_id, uintptr_t child_list_id) {
        auto state_cast = static_cast<ExpressionVisitor*>(state);

        auto children = state_cast->TakeFieldList(child_list_id);

        if (EXPECTED_CHILDREN != -1) {
            D_ASSERT(children->size() == EXPECTED_CHILDREN);
        }

        if (EXPECTED_CHILDREN == 2) {
            auto &lhs = children->at(0);
            auto &rhs = children->at(1);
            unique_ptr<ParsedExpression> expression = make_uniq<EXPRESSION_TYPENAME>(EXPRESSION_TYPE, std::move(lhs), std::move(rhs));
            state_cast->AppendToList(sibling_list_id, std::move(expression));
        } else {
            unique_ptr<ParsedExpression> expression = make_uniq<EXPRESSION_TYPENAME>(EXPRESSION_TYPE, std::move(*children));
            state_cast->AppendToList(sibling_list_id, std::move(expression));
        }

    }

    // List functions
    static uintptr_t MakeFieldList(ExpressionVisitor* state, uintptr_t capacity_hint);
    void AppendToList(uintptr_t id, unique_ptr<ParsedExpression> child);
    uintptr_t MakeFieldListImpl(uintptr_t capacity_hint);
    unique_ptr<FieldList> TakeFieldList(uintptr_t id);
};

// SchemaVisitor is used to parse the schema of a Delta table from the Kernel
class SchemaVisitor {
public:
    using FieldList = child_list_t<LogicalType>;

    static unique_ptr<FieldList> VisitSnapshotSchema(ffi::SharedSnapshot* snapshot);

private:
    unordered_map<uintptr_t, unique_ptr<FieldList>> inflight_lists;
    uintptr_t next_id = 1;

    typedef void (SimpleTypeVisitorFunction)(void*, uintptr_t, ffi::KernelStringSlice);

    template <LogicalTypeId TypeId>
    static SimpleTypeVisitorFunction* VisitSimpleType() {
        return (SimpleTypeVisitorFunction*) &VisitSimpleTypeImpl<TypeId>;
    }
    template <LogicalTypeId TypeId>
    static void VisitSimpleTypeImpl(SchemaVisitor* state, uintptr_t sibling_list_id, ffi::KernelStringSlice name) {
        state->AppendToList(sibling_list_id, name, TypeId);
    }

    static void VisitDecimal(SchemaVisitor* state, uintptr_t sibling_list_id, ffi::KernelStringSlice name, uint8_t precision, uint8_t scale);
    static uintptr_t MakeFieldList(SchemaVisitor* state, uintptr_t capacity_hint);
    static void VisitStruct(SchemaVisitor* state, uintptr_t sibling_list_id, ffi::KernelStringSlice name, uintptr_t child_list_id);
    static void VisitArray(SchemaVisitor* state, uintptr_t sibling_list_id, ffi::KernelStringSlice name, bool contains_null, uintptr_t child_list_id);
    static void VisitMap(SchemaVisitor* state, uintptr_t sibling_list_id, ffi::KernelStringSlice name, bool contains_null, uintptr_t child_list_id);

    uintptr_t MakeFieldListImpl(uintptr_t capacity_hint);
    void AppendToList(uintptr_t id, ffi::KernelStringSlice name, LogicalType&& child);
    unique_ptr<FieldList> TakeFieldList(uintptr_t id);
};

// Allocator for errors that the kernel might throw
struct DuckDBEngineError : ffi::EngineError {
    // Allocate a DuckDBEngineError, function ptr passed to kernel for error allocation
    static ffi::EngineError* AllocateError(ffi::KernelError etype, ffi::KernelStringSlice msg);
    // Convert a kernel error enum to a string
    static string KernelErrorEnumToString(ffi::KernelError err);

    // Throw the error as an IOException
    [[noreturn]] void Throw(string from_info);

    // The error message from Kernel
    string error_message;
};

// RAII wrapper that returns ownership of a kernel pointer to kernel when it goes out of
// scope. Similar to std::unique_ptr. but does not define operator->() and does not require the
// kernel type to be complete.
template <typename KernelType>
struct UniqueKernelPointer {
    UniqueKernelPointer() : ptr(nullptr), free(nullptr) {}

    // Takes ownership of a pointer with associated deleter.
    UniqueKernelPointer(KernelType* ptr, void (*free)(KernelType*)) : ptr(ptr), free(free) {}

    // movable but not copyable
    UniqueKernelPointer(UniqueKernelPointer&& other) : ptr(other.ptr) {
        other.ptr = nullptr;
    }
    UniqueKernelPointer& operator=(UniqueKernelPointer&& other) {
        std::swap(ptr, other.ptr);
        std::swap(free, other.free);
        return *this;
    }
    UniqueKernelPointer(const UniqueKernelPointer&) = delete;
    UniqueKernelPointer& operator=(const UniqueKernelPointer&) = delete;

    ~UniqueKernelPointer() {
        if (ptr && free) {
            free(ptr);
        }
    }

    KernelType* get() const { return ptr; }

private:
    KernelType* ptr;
    void (*free)(KernelType*) = nullptr;
};

// Syntactic sugar around the different kernel types
template <typename KernelType, void (*DeleteFunction)(KernelType*)>
struct TemplatedUniqueKernelPointer : public UniqueKernelPointer<KernelType> {
    TemplatedUniqueKernelPointer() : UniqueKernelPointer<KernelType>() {
    };
    TemplatedUniqueKernelPointer(KernelType* ptr) : UniqueKernelPointer<KernelType>(ptr, DeleteFunction) {
    };
};

typedef TemplatedUniqueKernelPointer<ffi::SharedSnapshot, ffi::free_snapshot> KernelSnapshot;
typedef TemplatedUniqueKernelPointer<ffi::SharedExternEngine, ffi::free_engine> KernelExternEngine;
typedef TemplatedUniqueKernelPointer<ffi::SharedScan, ffi::free_scan> KernelScan;
typedef TemplatedUniqueKernelPointer<ffi::SharedGlobalScanState, ffi::free_global_scan_state> KernelGlobalScanState;
typedef TemplatedUniqueKernelPointer<ffi::SharedScanDataIterator, ffi::free_kernel_scan_data> KernelScanDataIterator;

struct KernelUtils {
    static ffi::KernelStringSlice ToDeltaString(const string &str);
    static string FromDeltaString(const struct ffi::KernelStringSlice slice);
    static vector<bool> FromDeltaBoolSlice(const struct ffi::KernelBoolSlice slice);

    // TODO: all kernel results need to be unpacked, not doing so will result in an error. This should be cleaned up
    template <class T>
    static T UnpackResult(ffi::ExternResult<T> result, const string &from_where) {
        if (result.tag == ffi::ExternResult<T>::Tag::Err) {
            if (result.err._0){
                auto error_cast = static_cast<DuckDBEngineError*>(result.err._0);
                error_cast->Throw(from_where);
            } else {
                throw IOException("Hit DeltaKernel FFI error (from: %s): Hit error, but error was nullptr", from_where.c_str());
            }
        } else if (result.tag == ffi::ExternResult<T>::Tag::Ok) {
            return result.ok._0;
        }
        throw IOException("Invalid error ExternResult tag found!");
    }
};

class PredicateVisitor : public ffi::EnginePredicate {
public:
    PredicateVisitor(const vector<string> &column_names, optional_ptr<TableFilterSet> filters);

private:
    unordered_map<string, TableFilter*> column_filters;

    static uintptr_t VisitPredicate(PredicateVisitor* predicate, ffi::KernelExpressionVisitorState* state);

    uintptr_t VisitConstantFilter(const string &col_name, const ConstantFilter &filter, ffi::KernelExpressionVisitorState* state);
    uintptr_t VisitAndFilter(const string &col_name, const ConjunctionAndFilter &filter, ffi::KernelExpressionVisitorState* state);

    uintptr_t VisitIsNull(const string &col_name, ffi::KernelExpressionVisitorState* state);
    uintptr_t VisitIsNotNull(const string &col_name, ffi::KernelExpressionVisitorState* state);

    uintptr_t VisitFilter(const string &col_name, const TableFilter &filter, ffi::KernelExpressionVisitorState* state);
};

} // namespace duckdb
