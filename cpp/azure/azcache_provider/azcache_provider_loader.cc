#include "azure/azcache_provider/azcache_provider_loader.h"

#include <cstdlib>
#include <dlfcn.h>

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::azure
{

AzCacheProviderLoader& AzCacheProviderLoader::instance()
{
    static AzCacheProviderLoader instance;
    return instance;
}

AzCacheProviderLoader::AzCacheProviderLoader()
    : _lib_handle(nullptr),
      _cache_read(nullptr),
      _enabled(false)
{
    std::string lib_path;
    if (!utils::try_getenv<std::string>(RUNAI_AZURE_CACHE_LIB_ENV, lib_path))
    {
        LOG(DEBUG) << "AzCacheProvider: " << RUNAI_AZURE_CACHE_LIB_ENV << " not set — cache disabled";
        return;
    }

    _lib_path = lib_path;
    LOG(INFO) << "AzCacheProvider: loading cache library: " << _lib_path;

    _lib_handle = dlopen(_lib_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!_lib_handle)
    {
        LOG(WARNING) << "AzCacheProvider: dlopen failed for '" << _lib_path << "': " << dlerror();
        return;
    }

    _cache_read = reinterpret_cast<az_cache_read_fn>(
        dlsym(_lib_handle, AZ_CACHE_READ_SYMBOL));
    if (!_cache_read)
    {
        LOG(WARNING) << "AzCacheProvider: dlsym failed for '" << AZ_CACHE_READ_SYMBOL
                     << "': " << dlerror();
        dlclose(_lib_handle);
        _lib_handle = nullptr;
        return;
    }

    _enabled = true;
    LOG(INFO) << "AzCacheProvider: cache provider loaded successfully from " << _lib_path;
}

AzCacheProviderLoader::~AzCacheProviderLoader()
{
    // Intentionally do NOT dlclose — at static destruction time the loaded
    // library may have already torn down its own statics, leading to
    // use-after-free.  The OS reclaims everything on process exit.
}

bool AzCacheProviderLoader::read(
    const std::string& container,
    const std::string& blob,
    char* buffer,
    size_t offset,
    size_t length)
{
    if (!_enabled)
    {
        return false;
    }

    char* error_string = nullptr;
    ssize_t bytes_read = _cache_read(
        container.c_str(), blob.c_str(),
        buffer, offset, length, &error_string);

    if (bytes_read < 0 || static_cast<size_t>(bytes_read) != length)
    {
        if (error_string)
        {
            LOG(ERROR) << "AzCacheProvider: cache read failed for "
                       << container << "/" << blob
                       << " offset=" << offset << " length=" << length
                       << ": " << error_string;
            free(error_string);
        }
        else
        {
            LOG(ERROR) << "AzCacheProvider: cache read failed for "
                       << container << "/" << blob
                       << " offset=" << offset << " length=" << length;
        }
        return false;
    }

    LOG(SPAM) << "AzCacheProvider: cache read " << length << " bytes from "
              << container << "/" << blob << " offset=" << offset;
    return true;
}

} // namespace runai::llm::streamer::impl::azure
