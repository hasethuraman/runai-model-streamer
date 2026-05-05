#pragma once

#include <memory>
#include <string>

#include "azure/azcache_provider/runai_azcache_provider.h"

namespace runai::llm::streamer::impl::azure
{

/**
 * Cache mode matching RUNAI_STREAMER_DIST pattern.
 */
enum class CacheMode
{
    Disabled,  // "0" — never use cache
    Required,  // "1" — must load cache, fail if not found
    Auto       // "auto" or unset — use if available, silently disable otherwise
};

/**
 * Configuration for constructing a cache provider loader.
 */
struct CacheProviderConfig
{
    std::string lib_path;  // Path to the cache provider .so (empty = not configured)
    CacheMode mode = CacheMode::Auto;
};

/**
 * AzCacheProviderLoader dynamically loads a cache provider .so at runtime
 * via dlopen/dlsym.
 *
 * No longer a global singleton — owned by AzureClient for testability.
 * Use from_env() to create from environment variables.
 *
 * Thread-safe once constructed (immutable state after construction).
 */
class AzCacheProviderLoader
{
public:
    /**
     * Create a loader from environment variables:
     *   RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED: "0", "1", or "auto" (default)
     *   RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_LIB: explicit .so path override
     *
     * @throws common::Exception if mode=Required and library cannot be loaded.
     */
    static std::shared_ptr<AzCacheProviderLoader> from_env();

    /**
     * Create a loader with explicit configuration (for testing).
     */
    explicit AzCacheProviderLoader(const CacheProviderConfig& config);
    ~AzCacheProviderLoader();

    bool is_enabled() const { return _enabled; }

    /**
     * Read blob data through the cache provider.
     *
     * @param account    Azure Storage account name
     * @param container  Azure container name
     * @param blob       Blob path within the container
     * @param buffer     Destination buffer (caller-allocated)
     * @param offset     Byte offset within the blob
     * @param length     Number of bytes to read
     * @return true on success (length bytes read), false on error
     */
    bool read(const std::string& account,
              const std::string& container,
              const std::string& blob,
              char* buffer,
              size_t offset,
              size_t length);

    AzCacheProviderLoader(const AzCacheProviderLoader&) = delete;
    AzCacheProviderLoader& operator=(const AzCacheProviderLoader&) = delete;

private:
    void* _lib_handle;
    blob_read_fn _cache_read;
    bool _enabled;
    std::string _lib_path;
};

} // namespace runai::llm::streamer::impl::azure
