#pragma once

#include "common/backend_api/object_storage/object_storage.h"
#include "common/response/response.h"
#include "common/range/range.h"

// For connecting to Azure Blob Storage:
//
// 1. Uri should be in the format az://container/path or https://account.blob.core.windows.net/container/path
//
// 2. Credentials can be provided in multiple ways:
//
//    Option 1 - Environment variables:
//    - Connection string: AZURE_STORAGE_CONNECTION_STRING
//    - SAS token: AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_SAS_TOKEN
//
//    Option 2 - DefaultAzureCredential (fallback when only AZURE_STORAGE_ACCOUNT_NAME is provided):
//    - Tries multiple authentication methods in order: environment variables, managed identity, Azure CLI, etc.
//    - Set AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET for service principal
//    - Or use managed identity (no env vars needed)
//    - Or authenticate via: az login, Connect-AzAccount, or azd auth login
//
// 3. Optional configuration:
//    - Custom endpoint: AZURE_STORAGE_ENDPOINT or "endpoint" param
//    - API version: AZURE_STORAGE_API_VERSION (default: "2023-11-03")
//
// Example usage:
// env vars:  AZURE_STORAGE_CONNECTION_STRING="..." <streamer app> az://container/path
// managed:   AZURE_STORAGE_ACCOUNT_NAME="account" <streamer app> az://container/path  # uses DefaultAzureCredential
// programmatic: Pass credentials in ObjectClientConfig_t.initial_params

namespace runai::llm::streamer::impl::azure
{

// --- Backend API ---

extern "C" common::backend_api::ResponseCode_t obj_open_backend(common::backend_api::ObjectBackendHandle_t* out_backend_handle);
extern "C" common::backend_api::ResponseCode_t obj_close_backend(common::backend_api::ObjectBackendHandle_t backend_handle);
extern "C" common::backend_api::ObjectShutdownPolicy_t obj_get_backend_shutdown_policy();

// --- Client API ---

extern "C" common::backend_api::ResponseCode_t obj_create_client(
    common::backend_api::ObjectBackendHandle_t backend_handle,
    const common::backend_api::ObjectClientConfig_t* client_initial_config,
    common::backend_api::ObjectClientHandle_t* out_client_handle
);

extern "C" common::backend_api::ResponseCode_t obj_remove_client(
    common::backend_api::ObjectClientHandle_t client_handle
);

extern "C" common::backend_api::ResponseCode_t obj_request_read(
    common::backend_api::ObjectClientHandle_t client_handle,
    const char* path,
    common::backend_api::ObjectRange_t range,
    char* destination_buffer,
    common::backend_api::ObjectRequestId_t request_id
);

extern "C" common::backend_api::ResponseCode_t obj_wait_for_completions(
    common::backend_api::ObjectClientHandle_t client_handle,
    common::backend_api::ObjectCompletionEvent_t* event_buffer,
    unsigned int max_events_to_retrieve,
    unsigned int* out_num_events_retrieved,
    common::backend_api::ObjectWaitMode_t wait_mode
);

// stop clients
// Stops the responder of each client, in order to notify callers which sent a request and are waiting for a response
extern "C" common::backend_api::ResponseCode_t obj_cancel_all_reads();

// release clients
extern "C" common::backend_api::ResponseCode_t obj_remove_all_clients();

}; //namespace runai::llm::streamer::impl::azure
