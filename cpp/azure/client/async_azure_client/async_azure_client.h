#pragma once

#include <atomic>
#include <future>
#include <memory>
#include <string>
#include <functional>

#include <azure/storage/blobs.hpp>

#include "common/response_code/response_code.h"
#include "utils/logging/logging.h"
#include "utils/threadpool/threadpool.h"
#include "azure/azcache_provider/azcache_provider_loader.h"

namespace runai::llm::streamer::impl::azure
{

typedef std::function<void()> DownloadBlobFn;
typedef std::function<void(common::ResponseCode response_code, const std::string& error_msg)> CompletionCallback;

struct DownloadBlobTask {
    DownloadBlobFn taskFn;
    CompletionCallback callback;
    int chunk_counter;

public:
    void execute();
};

inline DownloadBlobFn createDownloadBlobFn(
    int chunk_counter,
    std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> client,
    const std::string& account_name,
    const std::string& container_name,
    const std::string& blob_name,
    char* buffer,
    size_t offset,
    size_t length)
{
    // Explicitly copy strings to ensure they're captured properly
    std::string account = account_name;
    std::string container = container_name;
    std::string blob = blob_name;
    
    return [client, account, container, blob, buffer, offset, length, chunk_counter]() {
        using namespace Azure::Storage::Blobs;
        auto container_client = client->GetBlobContainerClient(container);
        auto blob_client = container_client.GetBlockBlobClient(blob);
        LOG(INFO) << "Reading from blob: " << account << "/" << container << "/" << blob
            << " offset=" << offset << " length=" << length << " chunk_counter=" << chunk_counter;

        DownloadBlobToOptions download_options;
        download_options.Range = Azure::Core::Http::HttpRange();
        download_options.Range.Value().Offset = offset;
        download_options.Range.Value().Length = length;
        
        blob_client.DownloadTo(
            reinterpret_cast<uint8_t*>(buffer), length, download_options
        );
    };
}

/**
 * An async wrapper for Azure Blob Storage client operations.
 * Uses ThreadPool to manage concurrent blob download operations.
 * 
 * When a cache provider is configured, the client will read from
 * the cache instead of Azure Blob Storage.
 */
struct AsyncAzureClient
{
public:
    AsyncAzureClient(std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> client,
                     unsigned max_pool_size,
                     std::shared_ptr<AzCacheProviderLoader> cache_loader = nullptr);

    /** Returns true when a cache provider .so has been loaded. */
    bool is_cache_enabled() const { return _cache_enabled; }

    /**
     * Download a range of a blob asynchronously.
     * 
     * When a cache provider is loaded, all reads are served from the cache.
     * The cache provider is responsible for handling cache misses internally.
     */
    void DownloadBlobRangeAsync(
        const std::string& account_name,
        const std::string& container_name,
        const std::string& blob_name,
        char* buffer,
        size_t offset,
        size_t length,
        CompletionCallback callback)
    {
        int chunk_counter = _chunk_read_counter.fetch_add(1);
        if (_cache_enabled)
        {
            // Capture shared_ptr to loader to ensure lifetime
            auto loader = _cache_loader;
            DownloadBlobTask task{
                [loader, account_name, container_name, blob_name, buffer, offset, length, chunk_counter]() {
                    LOG(INFO) << "Reading from cache provider for blob: " << account_name << "/" << container_name << "/" << blob_name
                              << " offset=" << offset << " length=" << length << " chunk_counter=" << chunk_counter;
                    bool ok = loader->read(
                        account_name, container_name, blob_name, buffer, offset, length);
                    if (!ok)
                    {
                        throw std::runtime_error("Cache provider read failed");
                    }
                },
                std::move(callback),
                chunk_counter
            };
            push_task(std::move(task));
        }
        else
        {
            DownloadBlobFn downloadFn = createDownloadBlobFn(
                chunk_counter, _client, account_name, container_name, blob_name, buffer, offset, length);

            DownloadBlobTask task{
                std::move(downloadFn),
                std::move(callback),
                chunk_counter
            };
            push_task(std::move(task));
        }
    }

private:
    void push_task(DownloadBlobTask&& task);

    std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> _client;
    utils::ThreadPool<DownloadBlobTask> _pool;
    std::shared_ptr<AzCacheProviderLoader> _cache_loader;
    const bool _cache_enabled;
    std::atomic<unsigned> _chunk_read_counter{0};
};

}; // namespace runai::llm::streamer::impl::azure
