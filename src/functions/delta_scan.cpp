#include "duckdb/function/table_function.hpp"

#include "delta_functions.hpp"
#include "functions/delta_scan.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/secret/secret_manager.hpp"


#include <string>
#include <numeric>
#include <regex>

namespace duckdb {

static void* allocate_string(const struct ffi::KernelStringSlice slice) {
    return new string(slice.ptr, slice.len);
}

string url_decode(string input) {
    string result;
    result.reserve(input.size());
    char ch;
    replace(input.begin(), input.end(), '+', ' ');
    for (idx_t i = 0; i < input.length(); i++) {
        if (int(input[i]) == 37) {
            unsigned int ii;
            sscanf(input.substr(i + 1, 2).c_str(), "%x", &ii);
            ch = static_cast<char>(ii);
            result += ch;
            i += 2;
        } else {
            result += input[i];
        }
    }
    return result;
}

static void visit_callback(ffi::NullableCvoid engine_context, struct ffi::KernelStringSlice path, int64_t size, const ffi::Stats *, const ffi::DvInfo *dv_info, const struct ffi::CStringMap *partition_values) {
    auto context = (DeltaSnapshot *) engine_context;
    auto path_string =  context->GetPath();
    StringUtil::RTrim(path_string, "/");
    path_string += "/" + KernelUtils::FromDeltaString(path);

    path_string = url_decode(path_string);

    // First we append the file to our resolved files
    context->resolved_files.push_back(DeltaSnapshot::ToDuckDBPath(path_string));
    context->metadata.emplace_back(make_uniq<DeltaFileMetaData>());

    D_ASSERT(context->resolved_files.size() == context->metadata.size());

    // Initialize the file metadata
    context->metadata.back()->delta_snapshot_version = context->version;
    context->metadata.back()->file_number = context->resolved_files.size() - 1;

    // Fetch the deletion vector
    auto selection_vector_res = ffi::selection_vector_from_dv(dv_info, context->extern_engine.get(), context->global_state.get());
    auto selection_vector = KernelUtils::UnpackResult(selection_vector_res, "selection_vector_from_dv for path " + context->GetPath());
    if (selection_vector.ptr) {
        context->metadata.back()->selection_vector = selection_vector;
    }

    // Lookup all columns for potential hits in the constant map
    case_insensitive_map_t<string> constant_map;
    for (const auto &col: context->names) {
        auto key = KernelUtils::ToDeltaString(col);
        auto *partition_val = (string *) ffi::get_from_map(partition_values, key, allocate_string);
        if (partition_val) {
            constant_map[col] = *partition_val;
            delete partition_val;
        }
    }
    context->metadata.back()->partition_map = std::move(constant_map);
}

  static void visit_data(void *engine_context, ffi::EngineData* engine_data, const struct ffi::KernelBoolSlice selection_vec) {
    ffi::visit_scan_data(engine_data, selection_vec, engine_context, visit_callback);
}

string ParseAccountNameFromEndpoint(const string& endpoint) {
    if (!StringUtil::StartsWith(endpoint, "https://")) {
        return "";
    }
    auto result = endpoint.find('.', 8);
    if (result == endpoint.npos) {
        return "";
    }
    return endpoint.substr(8,result-8);
}

string parseFromConnectionString(const string& connectionString, const string& key) {
    std::regex pattern(key + "=([^;]+)(?=;|$)");
    std::smatch matches;
    if (std::regex_search(connectionString, matches, pattern) && matches.size() > 1) {
        // The second match ([1]) contains the access key
        return matches[1].str();
    }
    return "";
}

static ffi::EngineBuilder* CreateBuilder(ClientContext &context, const string &path) {
    ffi::EngineBuilder* builder;

    // For "regular" paths we early out with the default builder config
    if (!StringUtil::StartsWith(path, "s3://") &&
        !StringUtil::StartsWith(path, "gcs://") &&
        !StringUtil::StartsWith(path, "gs://") &&
        !StringUtil::StartsWith(path, "r2://") &&
        !StringUtil::StartsWith(path, "azure://") &&
        !StringUtil::StartsWith(path, "az://") &&
        !StringUtil::StartsWith(path, "abfs://") &&
        !StringUtil::StartsWith(path, "abfss://")) {
        auto interface_builder_res = ffi::get_engine_builder(KernelUtils::ToDeltaString(path), DuckDBEngineError::AllocateError);
        return KernelUtils::UnpackResult(interface_builder_res, "get_engine_interface_builder for path " + path);
    }

    string bucket;
    string path_in_bucket;
    string secret_type;

    if (StringUtil::StartsWith(path, "s3://")) {
        auto end_of_container = path.find('/',5);

        if(end_of_container == string::npos) {
            throw IOException("Invalid s3 url passed to delta scan: %s", path);
        }
        bucket = path.substr(5, end_of_container-5);
        path_in_bucket = path.substr(end_of_container);
        secret_type = "s3";
    } else if (StringUtil::StartsWith(path, "gcs://")) {
        auto end_of_container = path.find('/',6);

        if(end_of_container == string::npos) {
            throw IOException("Invalid gcs url passed to delta scan: %s", path);
        }
        bucket = path.substr(6, end_of_container-6);
        path_in_bucket = path.substr(end_of_container);
        secret_type = "gcs";
    } else if (StringUtil::StartsWith(path, "gs://")) {
        auto end_of_container = path.find('/',5);

        if(end_of_container == string::npos) {
            throw IOException("Invalid gcs url passed to delta scan: %s", path);
        }
        bucket = path.substr(5, end_of_container-5);
        path_in_bucket = path.substr(end_of_container);
        secret_type = "gcs";
    } else if (StringUtil::StartsWith(path, "r2://")) {
        auto end_of_container = path.find('/',5);

        if(end_of_container == string::npos) {
            throw IOException("Invalid gcs url passed to delta scan: %s", path);
        }
        bucket = path.substr(5, end_of_container-5);
        path_in_bucket = path.substr(end_of_container);
        secret_type = "r2";
    } else if ((StringUtil::StartsWith(path, "azure://")) || (StringUtil::StartsWith(path, "abfss://"))) {
        auto end_of_container = path.find('/',8);

        if(end_of_container == string::npos) {
            throw IOException("Invalid azure url passed to delta scan: %s", path);
        }
        bucket = path.substr(8, end_of_container-8);
        path_in_bucket = path.substr(end_of_container);
        secret_type = "azure";
    } else if (StringUtil::StartsWith(path, "az://")) {
        auto end_of_container = path.find('/',5);

        if(end_of_container == string::npos) {
            throw IOException("Invalid azure url passed to delta scan: %s", path);
        }
        bucket = path.substr(5, end_of_container-5);
        path_in_bucket = path.substr(end_of_container);
        secret_type = "azure";
    } else if (StringUtil::StartsWith(path, "abfs://")) {
        auto end_of_container = path.find('/',7);

        if(end_of_container == string::npos) {
            throw IOException("Invalid azure url passed to delta scan: %s", path);
        }
        bucket = path.substr(8, end_of_container-8);
        path_in_bucket = path.substr(end_of_container);
        secret_type = "azure";
    }

    // We need to substitute DuckDB's usage of s3 and r2 paths because delta kernel needs to just interpret them as s3 protocol servers.
    string cleaned_path;
    if (StringUtil::StartsWith(path, "r2://") || StringUtil::StartsWith(path, "gs://") ) {
        cleaned_path = "s3://" + path.substr(5);
    } else if (StringUtil::StartsWith(path, "gcs://")) {
        cleaned_path = "s3://" + path.substr(6);
    } else {
        cleaned_path = path;
    }

    auto interface_builder_res = ffi::get_engine_builder(KernelUtils::ToDeltaString(cleaned_path), DuckDBEngineError::AllocateError);
    builder = KernelUtils::UnpackResult(interface_builder_res, "get_engine_interface_builder for path " + cleaned_path);

    // For S3 or Azure paths we need to trim the url, set the container, and fetch a potential secret
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

    auto secret_match = secret_manager.LookupSecret(transaction, path, secret_type);

    // No secret: nothing left to do here!
    if (!secret_match.HasMatch()) {
        if (StringUtil::StartsWith(path, "r2://") || StringUtil::StartsWith(path, "gs://") || StringUtil::StartsWith(path, "gcs://")) {
            throw NotImplementedException("Can not scan a gcs:// gs:// or r2:// url without a secret providing its endpoint currently. Please create an R2 or GCS secret containing the credentials for this endpoint and try again.");
        }

        return builder;
    }
    const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);

    // Here you would need to add the logic for setting the builder options for Azure
    // This is just a placeholder and will need to be replaced with the actual logic
    if (secret_type == "s3" || secret_type == "gcs" || secret_type == "r2") {
        auto key_id = kv_secret.TryGetValue("key_id").ToString();
        auto secret = kv_secret.TryGetValue("secret").ToString();
        auto session_token = kv_secret.TryGetValue("session_token").ToString();
        auto region = kv_secret.TryGetValue("region").ToString();
        auto endpoint = kv_secret.TryGetValue("endpoint").ToString();
        auto use_ssl = kv_secret.TryGetValue("use_ssl").ToString();
        auto url_style = kv_secret.TryGetValue("url_style").ToString();

        if (key_id.empty() && secret.empty()) {
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("skip_signature"), KernelUtils::ToDeltaString("true"));
        }

        if (!key_id.empty()) {
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("aws_access_key_id"), KernelUtils::ToDeltaString(key_id));
        }
        if (!secret.empty()) {
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("aws_secret_access_key"), KernelUtils::ToDeltaString(secret));
        }
        if (!session_token.empty()) {
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("aws_session_token"), KernelUtils::ToDeltaString(session_token));
        }
        if (!endpoint.empty() && endpoint != "s3.amazonaws.com") {
            if(!StringUtil::StartsWith(endpoint, "https://") && !StringUtil::StartsWith(endpoint, "http://")) {
                if(use_ssl == "1" || use_ssl == "NULL") {
                    endpoint = "https://" + endpoint;
                } else {
                    endpoint = "http://" + endpoint;
                }
            }

            if (StringUtil::StartsWith(endpoint, "http://")) {
                ffi::set_builder_option(builder, KernelUtils::ToDeltaString("allow_http"), KernelUtils::ToDeltaString("true"));
            }
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("aws_endpoint"), KernelUtils::ToDeltaString(endpoint));
        }

        ffi::set_builder_option(builder, KernelUtils::ToDeltaString("aws_region"), KernelUtils::ToDeltaString(region));

    } else if (secret_type == "azure") {
        // azure seems to be super complicated as we need to cover duckdb azure plugin and delta RS builder
        // and both require different settings
        auto connection_string = kv_secret.TryGetValue("connection_string").ToString();
        auto account_name = kv_secret.TryGetValue("account_name").ToString();
        auto endpoint = kv_secret.TryGetValue("endpoint").ToString();
        auto client_id = kv_secret.TryGetValue("client_id").ToString();
        auto client_secret = kv_secret.TryGetValue("client_secret").ToString();
        auto tenant_id = kv_secret.TryGetValue("tenant_id").ToString();
        auto chain = kv_secret.TryGetValue("chain").ToString();
        auto provider = kv_secret.GetProvider();

        if (provider == "credential_chain") {
            // Authentication option 1a: using the cli authentication
            if (chain.find("cli") != std::string::npos) {
                ffi::set_builder_option(builder, KernelUtils::ToDeltaString("use_azure_cli"), KernelUtils::ToDeltaString("true"));
            }
            // Authentication option 1b: non-cli credential chains will just "hope for the best" technically since we are using the default
            // credential chain provider duckDB and delta-kernel-rs should find the same auth
        } else if (!connection_string.empty() && connection_string != "NULL") {

            // Authentication option 2: a connection string based on account key
            auto account_key = parseFromConnectionString(connection_string, "AccountKey");
            account_name = parseFromConnectionString(connection_string, "AccountName");
            // Authentication option 2: a connection string based on account key
            if (!account_name.empty() && !account_key.empty()) {
                ffi::set_builder_option(builder, KernelUtils::ToDeltaString("account_key"),
                                        KernelUtils::ToDeltaString(account_key));
            } else {
                // Authentication option 2b: a connection string based on SAS token
                endpoint = parseFromConnectionString(connection_string, "BlobEndpoint");
                if (account_name.empty()) {
                    account_name = ParseAccountNameFromEndpoint(endpoint);
                }
                auto sas_token = parseFromConnectionString(connection_string, "SharedAccessSignature");
                if (!sas_token.empty()) {
                    ffi::set_builder_option(builder, KernelUtils::ToDeltaString("sas_token"),
                                            KernelUtils::ToDeltaString(sas_token));
                }
            }
        } else if (provider == "service_principal") {
            if (!client_id.empty() && client_id != "NULL") {
                ffi::set_builder_option(builder, KernelUtils::ToDeltaString("azure_client_id"), KernelUtils::ToDeltaString(client_id));
            }
            if (!client_secret.empty() && client_secret != "NULL") {
                ffi::set_builder_option(builder, KernelUtils::ToDeltaString("azure_client_secret"), KernelUtils::ToDeltaString(client_secret));
            }
            if (!tenant_id.empty() && tenant_id != "NULL") {
                ffi::set_builder_option(builder, KernelUtils::ToDeltaString("azure_tenant_id"), KernelUtils::ToDeltaString(tenant_id));
            }
        } else {
            // Authentication option 3: no authentication, just an account name
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("azure_skip_signature"), KernelUtils::ToDeltaString("true"));
        }
        // Set the use_emulator option for when the azurite test server is used
        if (account_name == "devstoreaccount1" || connection_string.find("devstoreaccount1") != string::npos) {
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("use_emulator"), KernelUtils::ToDeltaString("true"));
        }
        if (!account_name.empty() && account_name != "NULL") {
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("account_name"), KernelUtils::ToDeltaString(account_name)); //needed for delta RS builder
        }
        if (!endpoint.empty() && endpoint != "NULL") {
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("azure_endpoint"), KernelUtils::ToDeltaString(endpoint));
        } else {
            ffi::set_builder_option(builder, KernelUtils::ToDeltaString("azure_endpoint"), KernelUtils::ToDeltaString("https://" + account_name + ".blob.core.windows.net/"));
        }
        ffi::set_builder_option(builder, KernelUtils::ToDeltaString("container_name"), KernelUtils::ToDeltaString(bucket));
    }
    return builder;
}

DeltaSnapshot::DeltaSnapshot(ClientContext &context_p, const string &path) : MultiFileList({ToDeltaPath(path)}, FileGlobOptions::ALLOW_EMPTY), context(context_p) {
}

string DeltaSnapshot::GetPath() {
    return GetPaths()[0];
}

string DeltaSnapshot::ToDuckDBPath(const string &raw_path) {
    if (StringUtil::StartsWith(raw_path, "file://")) {
        return raw_path.substr(7);
    }
    return raw_path;
}

string DeltaSnapshot::ToDeltaPath(const string &raw_path) {
    string path;
    if (StringUtil::StartsWith(raw_path, "./")) {
        LocalFileSystem fs;
        path = fs.JoinPath(fs.GetWorkingDirectory(), raw_path.substr(2));
        path = "file://" + path;
    } else {
        path = raw_path;
    }

    // Paths always end in a slash (kernel likes it that way for now)
    if (path[path.size()-1] != '/') {
        path = path + '/';
    }

    return path;
}

void DeltaSnapshot::Bind(vector<LogicalType> &return_types, vector<string> &names) {
    if (!initialized) {
        InitializeFiles();
    }
    auto schema = SchemaVisitor::VisitSnapshotSchema(snapshot.get());
    for (const auto &field: *schema) {
        names.push_back(field.first);
        return_types.push_back(field.second);
    }
    // Store the bound names for resolving the complex filter pushdown later
    this->names = names;
}

string DeltaSnapshot::GetFile(idx_t i) {
    if (!initialized) {
        InitializeFiles();
    }
    // We already have this file
    if (i < resolved_files.size()) {
        return resolved_files[i];
    }

    if (files_exhausted) {
        return "";
    }

    while(i >= resolved_files.size()) {
        auto have_scan_data_res = ffi::kernel_scan_data_next(scan_data_iterator.get(), this, visit_data);

        auto have_scan_data = TryUnpackKernelResult(have_scan_data_res);

        // kernel has indicated that we have no more data to scan
        if (!have_scan_data) {
            files_exhausted = true;
            return "";
        }
    }

    // The kernel scan visitor should have resolved a file OR returned
    if(i >= resolved_files.size()) {
        throw IOException("Delta Kernel seems to have failed to resolve a new file");
    }

    return resolved_files[i];
}

void DeltaSnapshot::InitializeFiles() {
    auto path_slice = KernelUtils::ToDeltaString(paths[0]);

    // Register engine
    auto interface_builder = CreateBuilder(context, paths[0]);
    extern_engine = TryUnpackKernelResult( ffi::builder_build(interface_builder));

    // Initialize Snapshot
    snapshot = TryUnpackKernelResult(ffi::snapshot(path_slice, extern_engine.get()));

    // Create Scan
    PredicateVisitor visitor(names, &table_filters);
    scan = TryUnpackKernelResult(ffi::scan(snapshot.get(), extern_engine.get(), &visitor));

    // Create GlobalState
    global_state = ffi::get_global_scan_state(scan.get());

    // Set version
    this->version = ffi::version(snapshot.get());

    // Create scan data iterator
    scan_data_iterator = TryUnpackKernelResult(ffi::kernel_scan_data_init(extern_engine.get(), scan.get()));

    initialized = true;
}

unique_ptr<MultiFileList> DeltaSnapshot::ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
                                               vector<unique_ptr<Expression>> &filters) {
    FilterCombiner combiner(context);
    for (const auto &filter : filters) {
        combiner.AddFilter(filter->Copy());
    }
    auto filterstmp = combiner.GenerateTableScanFilters(get.column_ids);

    // TODO: can/should we figure out if this filtered anything?
    auto filtered_list = make_uniq<DeltaSnapshot>(context, paths[0]);
    filtered_list->table_filters = std::move(filterstmp);
    filtered_list->names = names;

    return std::move(filtered_list);
}

vector<string> DeltaSnapshot::GetAllFiles() {
    idx_t i = resolved_files.size();
    // TODO: this can probably be improved
    while(!GetFile(i).empty()) {
        i++;
    }
    return resolved_files;
}

FileExpandResult DeltaSnapshot::GetExpandResult() {
    // GetFile(1) will ensure at least the first 2 files are expanded if they are available
    GetFile(1);

    if (resolved_files.size() > 1) {
        return FileExpandResult::MULTIPLE_FILES;
    } else if (resolved_files.size() == 1) {
        return FileExpandResult::SINGLE_FILE;
    }

    return FileExpandResult::NO_FILES;
}

idx_t DeltaSnapshot::GetTotalFileCount() {
    // TODO: this can probably be improved
    idx_t i = resolved_files.size();
    while(!GetFile(i).empty()) {
        i++;
    }
    return resolved_files.size();
}

unique_ptr<MultiFileReader> DeltaMultiFileReader::CreateInstance() {
    return std::move(make_uniq<DeltaMultiFileReader>());
}

bool DeltaMultiFileReader::Bind(MultiFileReaderOptions &options, MultiFileList &files,
              vector<LogicalType> &return_types, vector<string> &names, MultiFileReaderBindData &bind_data)  {
    auto &delta_snapshot = dynamic_cast<DeltaSnapshot&>(files);

    delta_snapshot.Bind(return_types, names);

    // We need to parse this option
    bool file_row_number_enabled = options.custom_options.find("file_row_number") != options.custom_options.end();
    if (file_row_number_enabled) {
        bind_data.file_row_number_idx = names.size();
        return_types.emplace_back(LogicalType::BIGINT);
        names.emplace_back("file_row_number");
    } else {
        // TODO: this is a bogus ID? Change for flag indicating it should be enabled?
        bind_data.file_row_number_idx = names.size();
    }

    return true;
};

void DeltaMultiFileReader::BindOptions(MultiFileReaderOptions &options, MultiFileList &files,
                 vector<LogicalType> &return_types, vector<string> &names, MultiFileReaderBindData& bind_data) {

    // Disable all other multifilereader options
    options.auto_detect_hive_partitioning = false;
    options.hive_partitioning = false;
    options.union_by_name = false;

    MultiFileReader::BindOptions(options, files, return_types, names, bind_data);

    auto demo_gen_col_opt = options.custom_options.find("delta_file_number");
    if (demo_gen_col_opt != options.custom_options.end()) {
        if (demo_gen_col_opt->second.GetValue<bool>()) {
            names.push_back("delta_file_number");
            return_types.push_back(LogicalType::UBIGINT);
        }
    }
}

void DeltaMultiFileReader::FinalizeBind(const MultiFileReaderOptions &file_options, const MultiFileReaderBindData &options,
                  const string &filename, const vector<string> &local_names,
                  const vector<LogicalType> &global_types, const vector<string> &global_names,
                  const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data,
                  ClientContext &context, optional_ptr<MultiFileReaderGlobalState> global_state) {
    MultiFileReader::FinalizeBind(file_options, options, filename, local_names, global_types, global_names, global_column_ids, reader_data, context, global_state);

    // Handle custom delta option set in MultiFileReaderOptions::custom_options
    auto file_number_opt = file_options.custom_options.find("delta_file_number");
    if (file_number_opt != file_options.custom_options.end()) {
        if (file_number_opt->second.GetValue<bool>()) {
            D_ASSERT(global_state);
            auto &delta_global_state = global_state->Cast<DeltaMultiFileReaderGlobalState>();
            D_ASSERT(delta_global_state.delta_file_number_idx != DConstants::INVALID_INDEX);

            // We add the constant column for the delta_file_number option
            // NOTE: we add a placeholder here, to demonstrate how we can also populate extra columns in the FinalizeChunk
            reader_data.constant_map.emplace_back(delta_global_state.delta_file_number_idx, Value::UBIGINT(0));
        }
    }

    // Get the metadata for this file
    D_ASSERT(global_state->file_list);
    const auto &snapshot = dynamic_cast<const DeltaSnapshot&>(*global_state->file_list);
    auto &file_metadata = snapshot.metadata[reader_data.file_list_idx.GetIndex()];

    if (!file_metadata->partition_map.empty()) {
        for (idx_t i = 0; i < global_column_ids.size(); i++) {
            column_t col_id = global_column_ids[i];
            auto col_partition_entry = file_metadata->partition_map.find(global_names[col_id]);
            if (col_partition_entry != file_metadata->partition_map.end()) {
                // Todo: use https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
                auto maybe_value = Value(col_partition_entry->second).DefaultCastAs(global_types[i]);
                reader_data.constant_map.emplace_back(i, maybe_value);
            }
        }
    }
}

unique_ptr<MultiFileList> DeltaMultiFileReader::CreateFileList(ClientContext &context, const vector<string>& paths, FileGlobOptions options) {
    if (paths.size() != 1) {
        throw BinderException("'delta_scan' only supports single path as input");
    }

    return make_uniq<DeltaSnapshot>(context, paths[0]);
}

// Generate the correct Selection Vector Based on the Raw delta KernelBoolSlice dv and the row_id_column
// TODO: this probably is slower than needed (we can do with less branches in the for loop for most cases)
static SelectionVector DuckSVFromDeltaSV(const ffi::KernelBoolSlice &dv, Vector row_id_column, idx_t count, idx_t &select_count) {
    D_ASSERT(row_id_column.GetType() == LogicalType::BIGINT);

    UnifiedVectorFormat data;
    row_id_column.ToUnifiedFormat(count, data);
    auto row_ids = UnifiedVectorFormat::GetData<int64_t>(data);

    SelectionVector result {count};
    idx_t current_select = 0;
    for (idx_t i = 0; i < count; i++) {
        auto row_id = row_ids[data.sel->get_index(i)];

        // TODO: why are deletion vectors not spanning whole data?
        if (row_id >= dv.len || dv.ptr[row_id]) {
            result.data()[current_select] = i;
            current_select++;
        }
    }

    select_count = current_select;

    return result;
}

// Parses the columns that are used by the delta extension into
void DeltaMultiFileReaderGlobalState::SetColumnIdx(const string &column, idx_t idx) {
    if (column == "file_row_number") {
        file_row_number_idx = idx;
        return;
    } else if (column == "delta_file_number") {
        delta_file_number_idx = idx;
        return;
    }
    throw IOException("Unknown column '%s' found as required by the DeltaMultiFileReader");
}

unique_ptr<MultiFileReaderGlobalState> DeltaMultiFileReader::InitializeGlobalState(duckdb::ClientContext &context,
                                                                                   const duckdb::MultiFileReaderOptions &file_options,
                                                                                   const duckdb::MultiFileReaderBindData &bind_data,
                                                                                   const duckdb::MultiFileList &file_list,
                                                                                   const vector<duckdb::LogicalType> &global_types,
                                                                                   const vector<std::string> &global_names,
                                                                                   const vector<duckdb::column_t> &global_column_ids) {
    vector<LogicalType> extra_columns;
    vector<pair<string, idx_t>> mapped_columns;

    // Create a map of the columns that are in the projection
    case_insensitive_map_t<idx_t> selected_columns;
    for (idx_t i = 0; i < global_column_ids.size(); i++) {
        auto global_id = global_column_ids[i];
        if (IsRowIdColumnId(global_id)) {
            continue;
        }

        auto &global_name = global_names[global_id];
        selected_columns.insert({global_name, i});
    }

    // TODO: only add file_row_number column if there are deletes
    case_insensitive_map_t<LogicalType> columns_to_map = {
            {"file_row_number", LogicalType::BIGINT},
    };

    // Add the delta_file_number column to the columns to map
    auto demo_gen_col_opt = file_options.custom_options.find("delta_file_number");
    if (demo_gen_col_opt != file_options.custom_options.end()) {
        if (demo_gen_col_opt->second.GetValue<bool>()) {
            columns_to_map.insert({"delta_file_number", LogicalType::UBIGINT});
        }
    }

    // Map every column to either a column in the projection, or add it to the extra columns if it doesn't exist
    idx_t col_offset = 0;
    for (const auto &required_column : columns_to_map) {
        // First check if the column is in the projection
        auto res = selected_columns.find(required_column.first);
        if (res != selected_columns.end()) {
            // The column is in the projection, no special handling is required; we simply store the index
            mapped_columns.push_back({required_column.first, res->second});
            continue;
        }

        // The column is NOT in the projection: it needs to be added as an extra_column

        // Calculate the index of the added column (extra columns are added after all other columns)
        idx_t current_col_idx = global_column_ids.size() + col_offset++;

        // Add column to the map, to ensure the MultiFileReader can find it when processing the Chunk
        mapped_columns.push_back({required_column.first, current_col_idx});

        // Ensure the result DataChunk has a vector of the correct type to store this column
        extra_columns.push_back(required_column.second);
    }

    auto res = make_uniq<DeltaMultiFileReaderGlobalState>(extra_columns, &file_list);

    // Parse all the mapped columns into the DeltaMultiFileReaderGlobalState for easy use;
    for (const auto& mapped_column : mapped_columns) {
        res->SetColumnIdx(mapped_column.first, mapped_column.second);
    }

    return std::move(res);
}

// This code is duplicated from MultiFileReader::CreateNameMapping the difference is that for columns that are not found
// in the parquet files, we just add null constant columns
static void CustomMulfiFileNameMapping(const string &file_name, const vector<LogicalType> &local_types,
                                        const vector<string> &local_names, const vector<LogicalType> &global_types,
                                        const vector<string> &global_names, const vector<column_t> &global_column_ids,
                                        MultiFileReaderData &reader_data, const string &initial_file,
                                        optional_ptr<MultiFileReaderGlobalState> global_state) {
    D_ASSERT(global_types.size() == global_names.size());
	D_ASSERT(local_types.size() == local_names.size());
	// we have expected types: create a map of name -> column index
	case_insensitive_map_t<idx_t> name_map;
	for (idx_t col_idx = 0; col_idx < local_names.size(); col_idx++) {
		name_map[local_names[col_idx]] = col_idx;
	}
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		// check if this is a constant column
		bool constant = false;
		for (auto &entry : reader_data.constant_map) {
			if (entry.column_id == i) {
				constant = true;
				break;
			}
		}
		if (constant) {
			// this column is constant for this file
			continue;
		}
		// not constant - look up the column in the name map
		auto global_id = global_column_ids[i];
		if (global_id >= global_types.size()) {
			throw InternalException(
			    "MultiFileReader::CreatePositionalMapping - global_id is out of range in global_types for this file");
		}
		auto &global_name = global_names[global_id];
		auto entry = name_map.find(global_name);
		if (entry == name_map.end()) {
			string candidate_names;
			for (auto &local_name : local_names) {
				if (!candidate_names.empty()) {
					candidate_names += ", ";
				}
				candidate_names += local_name;
			}
			// FIXME: this override is pretty hacky: for missing columns we just insert NULL constants
		    auto &global_type = global_types[global_id];
		    Value val (global_type);
		    reader_data.constant_map.push_back({i, val});
		    continue;
		}
		// we found the column in the local file - check if the types are the same
		auto local_id = entry->second;
		D_ASSERT(global_id < global_types.size());
		D_ASSERT(local_id < local_types.size());
		auto &global_type = global_types[global_id];
		auto &local_type = local_types[local_id];
		if (global_type != local_type) {
			reader_data.cast_map[local_id] = global_type;
		}
		// the types are the same - create the mapping
		reader_data.column_mapping.push_back(i);
		reader_data.column_ids.push_back(local_id);
	}

	reader_data.empty_columns = reader_data.column_ids.empty();
}

void DeltaMultiFileReader::CreateNameMapping(const string &file_name, const vector<LogicalType> &local_types,
                                        const vector<string> &local_names, const vector<LogicalType> &global_types,
                                        const vector<string> &global_names, const vector<column_t> &global_column_ids,
                                        MultiFileReaderData &reader_data, const string &initial_file,
                                        optional_ptr<MultiFileReaderGlobalState> global_state) {
    // First call the base implementation to do most mapping
    CustomMulfiFileNameMapping(file_name, local_types, local_names, global_types, global_names, global_column_ids, reader_data, initial_file, global_state);

    // Then we handle delta specific mapping
    D_ASSERT(global_state);
    auto &delta_global_state = global_state->Cast<DeltaMultiFileReaderGlobalState>();

    // Check if the file_row_number column is an "extra_column" which is not part of the projection
    if (delta_global_state.file_row_number_idx >= global_column_ids.size()) {
        D_ASSERT(delta_global_state.file_row_number_idx != DConstants::INVALID_INDEX);

        // Build the name map
        case_insensitive_map_t<idx_t> name_map;
        for (idx_t col_idx = 0; col_idx < local_names.size(); col_idx++) {
            name_map[local_names[col_idx]] = col_idx;
        }

        // Lookup the required column in the local map
        auto entry = name_map.find("file_row_number");
        if (entry == name_map.end()) {
            throw IOException("Failed to find the file_row_number column");
        }

        // Register the column to be scanned from this file
        reader_data.column_ids.push_back(entry->second);
        reader_data.column_mapping.push_back(delta_global_state.file_row_number_idx);
    }

    // This may have changed: update it
    reader_data.empty_columns = reader_data.column_ids.empty();
}

void DeltaMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
                   const MultiFileReaderData &reader_data, DataChunk &chunk, optional_ptr<MultiFileReaderGlobalState> global_state) {
    // Base class finalization first
    MultiFileReader::FinalizeChunk(context, bind_data, reader_data, chunk, global_state);

    D_ASSERT(global_state);
    auto &delta_global_state = global_state->Cast<DeltaMultiFileReaderGlobalState>();
    D_ASSERT(delta_global_state.file_list);

    // Get the metadata for this file
    const auto &snapshot = dynamic_cast<const DeltaSnapshot&>(*global_state->file_list);
    auto &metadata = snapshot.metadata[reader_data.file_list_idx.GetIndex()];

    if (metadata->selection_vector.ptr && chunk.size() != 0) {
        D_ASSERT(delta_global_state.file_row_number_idx != DConstants::INVALID_INDEX);
        auto &file_row_number_column = chunk.data[delta_global_state.file_row_number_idx];

        // Construct the selection vector using the file_row_number column and the raw selection vector from delta
        idx_t select_count;
        auto sv = DuckSVFromDeltaSV(metadata->selection_vector, file_row_number_column, chunk.size(), select_count);
        chunk.Slice(sv, select_count);
    }

    // Note: this demo function shows how we can use DuckDB's Binder create expression-based generated columns
    if (delta_global_state.delta_file_number_idx != DConstants::INVALID_INDEX) {
        //! Create Dummy expression (0 + file_number)
        vector<unique_ptr<ParsedExpression>> child_expr;
        child_expr.push_back(make_uniq<ConstantExpression>(Value::UBIGINT(0)));
        child_expr.push_back(make_uniq<ConstantExpression>(Value::UBIGINT(7)));
        unique_ptr<ParsedExpression> expr = make_uniq<FunctionExpression>("+", std::move(child_expr), nullptr, nullptr, false, true);

        //! s dummy expression
        auto binder = Binder::CreateBinder(context);
        ExpressionBinder expr_binder(*binder, context);
        auto bound_expr = expr_binder.Bind(expr, nullptr);

        //! Execute dummy expression into result column
        ExpressionExecutor expr_executor(context);
        expr_executor.AddExpression(*bound_expr);

        //! Execute the expression directly into the output Chunk
        expr_executor.ExecuteExpression(chunk.data[delta_global_state.delta_file_number_idx]);
    }
};

bool DeltaMultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options, ClientContext &context) {
    auto loption = StringUtil::Lower(key);

    if (loption == "delta_file_number") {
        options.custom_options[loption] = val;
        return true;
    }

    // We need to capture this one to know whether to emit
    if (loption == "file_row_number") {
        options.custom_options[loption] = val;
        return true;
    }

    return MultiFileReader::ParseOption(key, val, options, context);
}
//
//DeltaMultiFileReaderBindData::DeltaMultiFileReaderBindData(DeltaSnapshot & delta_snapshot): current_snapshot(delta_snapshot){
//
//}

TableFunctionSet DeltaFunctions::GetDeltaScanFunction(DatabaseInstance &instance) {
    // The delta_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
    // DeltaMultiFileReader into it to create a Delta-based multi file read

    auto &parquet_scan = ExtensionUtil::GetTableFunction(instance, "parquet_scan");
    auto parquet_scan_copy = parquet_scan.functions;

    for (auto &function : parquet_scan_copy.functions) {
        // Register the MultiFileReader as the driver for reads
        function.get_multi_file_reader = DeltaMultiFileReader::CreateInstance;

        // Unset all of these: they are either broken, very inefficient.
        // TODO: implement/fix these
        function.serialize = nullptr;
        function.deserialize = nullptr;
        function.statistics = nullptr;
        function.table_scan_progress = nullptr;
        function.get_bind_info = nullptr;

        // Schema param is just confusing here
        function.named_parameters.erase("schema");

        // Demonstration of a generated column based on information from DeltaSnapshot
        function.named_parameters["delta_file_number"] = LogicalType::BOOLEAN;

        function.name = "delta_scan";
    }

    parquet_scan_copy.name = "delta_scan";
    return parquet_scan_copy;
}

} // namespace duckdb
