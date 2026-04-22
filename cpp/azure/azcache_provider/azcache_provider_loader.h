#pragma once

#include <atomic>
#include <mutex>
#include <string>

#include "azure/azcache_provider/runai_azcache_provider.h"

namespace runai::llm::streamer::impl::azure
{

/**
 * AzCacheProviderLoader dynamically loads a user-supplied cache provider .so
 * at runtime via dlopen/dlsym.
 *
 * When RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_LIB is set to a path, the loader opens the library
 * and resolves the az_cache_read symbol. All subsequent blob reads can be
 * served through the cache provider instead of Azure Blob Storage.
 *
 * Thread-safe singleton — initialization happens once on first access.
 */
class AzCacheProviderLoader
{
public:
    static AzCacheProviderLoader& instance();

    bool is_enabled() const { return _enabled.load(std::memory_order_relaxed); }

    /**
     * Read blob data through the cache provider.
     *
     * @param container  Azure container name
     * @param blob       Blob path within the container
     * @param buffer     Destination buffer (caller-allocated)
     * @param offset     Byte offset within the blob
     * @param length     Number of bytes to read
     * @return true on success, false on error
     */
    bool read(const std::string& container,
              const std::string& blob,
              char* buffer,
              size_t offset,
              size_t length);

    AzCacheProviderLoader(const AzCacheProviderLoader&) = delete;
    AzCacheProviderLoader& operator=(const AzCacheProviderLoader&) = delete;

private:
    AzCacheProviderLoader();
    ~AzCacheProviderLoader();

    void* _lib_handle;
    az_cache_read_fn _cache_read;
    std::atomic<bool> _enabled;
    std::string _lib_path;
};

} // namespace runai::llm::streamer::impl::azure
