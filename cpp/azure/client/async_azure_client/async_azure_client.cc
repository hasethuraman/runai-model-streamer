#include <functional>
#include <utility>

#include "azure/client/async_azure_client/async_azure_client.h"
#include "utils/logging/logging.h"

#include <azure/core/http/http_status_code.hpp>

namespace runai::llm::streamer::impl::azure
{

void DownloadBlobTask::execute() {
    try {
        taskFn();
        callback(common::ResponseCode::Success, "");
    } catch (const Azure::Core::RequestFailedException& e) {
        std::string error_msg = "Azure RequestFailed: StatusCode=" + std::to_string(static_cast<int>(e.StatusCode)) + " " + e.what();
        // Map HTTP status codes to appropriate response codes
        common::ResponseCode code = common::ResponseCode::FileAccessError;
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
            code = common::ResponseCode::FileAccessError;
        } else if (e.StatusCode == Azure::Core::Http::HttpStatusCode::RangeNotSatisfiable) {
            // 416 Range Not Satisfiable - requested range is past EOF
            code = common::ResponseCode::EofError;
        }
        callback(code, error_msg);
    } catch (const std::exception& e) {
        callback(common::ResponseCode::FileAccessError, e.what());
    }
}

AsyncAzureClient::AsyncAzureClient(
    std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> client,
    unsigned int max_pool_size,
    std::shared_ptr<AzCacheProviderLoader> cache_loader) :
    _client(client),
    _cache_upload_pool([](WriteToCacheTask&& task, std::atomic<bool>& stopped) {
        task.execute();
    }, max_pool_size),
    _cache_download_pool([](ReadFromCacheTask&& task, std::atomic<bool>& stopped) {
        task.execute();
    }, max_pool_size),
    _download_pool([](DownloadBlobTask&& task, std::atomic<bool>& stopped) {
        task.execute();
    }, max_pool_size),
    _cache_loader(std::move(cache_loader)),
    _cache_enabled(_cache_loader && _cache_loader->is_enabled())
{
}

void AsyncAzureClient::push_task(DownloadBlobTask&& task)
{
    _download_pool.push(std::move(task));
}

void AsyncAzureClient::push_task(WriteToCacheTask&& task)
{
    _cache_upload_pool.push(std::move(task));
}

void AsyncAzureClient::push_task(ReadFromCacheTask&& task)
{
    _cache_download_pool.push(std::move(task));
}

void WriteToCacheTask::execute()
{
    try
    {
        taskFn();
        callback(common::ResponseCode::Success, "");
    }
    catch (const std::exception& e)
    {
        callback(common::ResponseCode::FileAccessError, e.what());
    }
}

void ReadFromCacheTask::execute()
{
    try
    {
        taskFn();
        callback(common::ResponseCode::Success, "");
    }
    catch (const std::exception& e)
    {
        callback(common::ResponseCode::FileAccessError, e.what());
    }
}

}; // namespace runai::llm::streamer::impl::azure
