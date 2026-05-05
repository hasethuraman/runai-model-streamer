/*
 * RunAI Azure Cache Provider Interface (Experimental)
 *
 * This header defines the contract for Azure Blob Storage cache providers.
 * A cache provider is a shared library (.so) that exports the blob_read symbol.
 * When installed as a Python package alongside runai-model-streamer, the cache
 * provider is auto-discovered and loaded at runtime — no configuration needed.
 *
 * Control via environment variables:
 *   RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED=0    — disable cache entirely
 *   RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED=1    — require cache (fail if not found)
 *   RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED=auto — use if available (default)
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
 *   extern "C" ssize_t blob_read(
 *       const char* account, const char* container,
 *       const char* blob,
 *       void* buf, size_t offset, size_t length,
 *       char* error_buf, size_t error_buf_size)
 *   {
 *       // Your cache logic here
 *       // On error: snprintf(error_buf, error_buf_size, "details...");
 *       return length;  // bytes read on success, -1 on error
 *   }
 */

#ifndef RUNAI_STREAMER_AZCACHE_PROVIDER_H
#define RUNAI_STREAMER_AZCACHE_PROVIDER_H

#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Recommended error buffer size for callers */
#define RUNAI_CACHE_ERROR_BUF_SIZE 512

/*
 * Read a range of bytes from a cached Azure blob.
 *
 * Parameters:
 *   account        - Azure Storage account name
 *   container      - Azure Blob Storage container name
 *   blob           - Blob path within the container
 *   buf            - Output buffer (caller-allocated, at least 'length' bytes)
 *   offset         - Byte offset within the blob to start reading from
 *   length         - Number of bytes to read
 *   error_buf      - Caller-owned buffer for error message (NUL-terminated on error).
 *                    May be NULL if error_buf_size is 0 (errors are silently discarded).
 *                    Message may be truncated if buffer is too small.
 *   error_buf_size - Size of the error_buf in bytes
 *
 * Returns:
 *   Number of bytes read on success (must equal 'length' for success).
 *   -1 on error (error_buf will contain a NUL-terminated message if provided).
 *
 * Contract:
 *   - A return value != length is treated as an error by the caller.
 *   - Providers must not return partial reads as success; return -1 instead.
 */
typedef ssize_t (*blob_read_fn)(
    const char* account,
    const char* container,
    const char* blob,
    void* buf,
    size_t offset,
    size_t length,
    char* error_buf,
    size_t error_buf_size);

/* Symbol name that the cache provider .so must export */
#define BLOB_READ_SYMBOL "blob_read"

/* Environment variable to control cache mode: "0", "1", or "auto" */
#define RUNAI_AZURE_CACHE_ENABLED_ENV "RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED"

/* Environment variable pointing to the cache provider .so path (explicit override) */
#define RUNAI_AZURE_CACHE_LIB_ENV "RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_LIB"

#ifdef __cplusplus
}
#endif

#endif /* RUNAI_STREAMER_AZCACHE_PROVIDER_H */
