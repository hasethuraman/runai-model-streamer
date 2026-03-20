/*
 * RunAI Azure Cache Provider Interface
 *
 * This header defines the contract for Azure Blob Storage cache providers.
 * Any shared library (.so) that exports the function below can be used as a
 * cache provider for RunAI Model Streamer.
 *
 * To use a cache provider, set the environment variable:
 *   RUNAI_STREAMER_AZURE_CACHE_LIB=/path/to/your_cache_provider.so
 *
 * The cache provider is responsible for:
 *   - Serving cached blob data when available
 *   - Populating the cache (e.g., fetching from Azure Blob Storage on miss)
 *   - Managing its own lifecycle and resources
 *
 * RunAI is NOT responsible for populating the cache.
 *
 * Example usage in a custom cache provider:
 *
 *   #include "runai_azcache_provider.h"
 *
 *   extern "C" ssize_t runai_cache_read(
 *       const char* container, const char* blob,
 *       void* buf, size_t offset, size_t length,
 *       char** error_string)
 *   {
 *       // Your cache logic here
 *       return length;  // bytes read
 *   }
 */

#ifndef RUNAI_STREAMER_AZCACHE_PROVIDER_H
#define RUNAI_STREAMER_AZCACHE_PROVIDER_H

#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Read a range of bytes from a cached Azure blob.
 *
 * Parameters:
 *   container    - Azure Blob Storage container name
 *   blob         - Blob path within the container
 *   buf          - Output buffer (caller-allocated, at least 'length' bytes)
 *   offset       - Byte offset within the blob to start reading from
 *   length       - Number of bytes to read
 *   error_string - On failure, set to a malloc'd error message string.
 *                  The caller is responsible for calling free() on it.
 *                  May be set to NULL if no error detail is available.
 *
 * Returns:
 *   Number of bytes read on success (should equal 'length').
 *   -1 on error (error_string will be set if possible).
 */
typedef ssize_t (*runai_cache_read_fn)(
    const char* container,
    const char* blob,
    void* buf,
    size_t offset,
    size_t length,
    char** error_string);

/* Symbol name that the cache provider .so must export */
#define RUNAI_CACHE_READ_SYMBOL "runai_cache_read"

/* Environment variable pointing to the cache provider .so path */
#define RUNAI_AZURE_CACHE_LIB_ENV "RUNAI_STREAMER_AZURE_CACHE_LIB"

#ifdef __cplusplus
}
#endif

#endif /* RUNAI_STREAMER_AZCACHE_PROVIDER_H */
