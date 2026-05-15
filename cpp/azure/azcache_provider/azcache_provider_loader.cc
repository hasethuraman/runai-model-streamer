#include "azure/azcache_provider/azcache_provider_loader.h"

#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <algorithm>

#include "common/exception/exception.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::azure
{

namespace
{

// Known cache provider package/library names for auto-discovery
constexpr const char* CACHE_PROVIDER_PACKAGE = "py_tachyon_client";
constexpr const char* CACHE_PROVIDER_LIB = "libStorageDirect.so";

/**
 * Use dladdr on a symbol known to live in libstreamerazure.so to locate
 * the site-packages directory, then look for a known cache provider .so.
 *
 * Layout: <site-packages>/runai_model_streamer/libstreamer/lib/libstreamerazure.so
 *         <site-packages>/<CACHE_PROVIDER_PACKAGE>/<CACHE_PROVIDER_LIB>
 */
std::string autodiscover_cache_lib()
{
    Dl_info info;
    auto fn_ptr = reinterpret_cast<void*>(&autodiscover_cache_lib);
    if (!dladdr(fn_ptr, &info) || !info.dli_fname)
    {
        LOG(DEBUG) << "AzCacheProvider: dladdr failed — cannot auto-discover";
        return {};
    }

    std::error_code ec;
    auto azure_so = std::filesystem::weakly_canonical(info.dli_fname, ec);
    if (ec)
    {
        LOG(DEBUG) << "AzCacheProvider: canonical path failed for "
                   << info.dli_fname << ": " << ec.message();
        return {};
    }

    // libstreamerazure.so → lib/ → libstreamer/ → runai_model_streamer/ → site-packages/
    auto site_packages = azure_so.parent_path().parent_path().parent_path().parent_path();
    auto candidate = site_packages / CACHE_PROVIDER_PACKAGE / CACHE_PROVIDER_LIB;

    if (std::filesystem::exists(candidate, ec) && !ec)
    {
        return candidate.string();
    }

    LOG(DEBUG) << "AzCacheProvider: auto-discovery checked " << candidate.string()
               << " — not found" << (ec ? ": " + ec.message() : "");
    return {};
}

CacheMode parse_cache_mode(const std::string& val)
{
    std::string lower = val;
    std::transform(lower.begin(), lower.end(), lower.begin(),
        [](unsigned char c) { return std::tolower(c); });

    if (lower == "0")
    {
        return CacheMode::Disabled;
    }
    if (lower == "1")
    {
        return CacheMode::Required;
    }
    // "auto" or any other value → auto mode
    return CacheMode::Auto;
}

} // anonymous namespace

std::shared_ptr<AzCacheProviderLoader> AzCacheProviderLoader::from_env()
{
    CacheProviderConfig config;
    config.mode = CacheMode::Auto;

    // Parse mode from RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED
    std::string enabled_val;
    if (utils::try_getenv<std::string>(RUNAI_AZURE_CACHE_ENABLED_ENV, enabled_val))
    {
        config.mode = parse_cache_mode(enabled_val);
        if (config.mode == CacheMode::Disabled)
        {
            LOG(INFO) << "AzCacheProvider: disabled via " << RUNAI_AZURE_CACHE_ENABLED_ENV << "=" << enabled_val;
        }
    }

    // Parse explicit library path from RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_LIB
    std::string lib_path;
    if (utils::try_getenv<std::string>(RUNAI_AZURE_CACHE_LIB_ENV, lib_path))
    {
        if (lib_path.empty())
        {
            LOG(WARNING) << "AzCacheProvider: " << RUNAI_AZURE_CACHE_LIB_ENV
                         << " is set to empty string — ignoring";
        }
        else
        {
            config.lib_path = lib_path;
        }
    }

    // Auto-discover if no explicit path and not disabled
    if (config.lib_path.empty() && config.mode != CacheMode::Disabled)
    {
        std::string discovered = autodiscover_cache_lib();
        if (!discovered.empty())
        {
            LOG(INFO) << "AzCacheProvider: auto-discovered cache library: " << discovered;
            config.lib_path = discovered;
        }
    }

    return std::make_shared<AzCacheProviderLoader>(config);
}

AzCacheProviderLoader::AzCacheProviderLoader(const CacheProviderConfig& config)
    : _lib_handle(nullptr),
      _cache_read(nullptr),
      _enabled(false)
{
    if (config.mode == CacheMode::Disabled)
    {
        return;
    }

    if (config.lib_path.empty())
    {
        if (config.mode == CacheMode::Required)
        {
            LOG(ERROR) << "AzCacheProvider: mode=1 (required) but no cache library path configured. "
                       << "Set " << RUNAI_AZURE_CACHE_LIB_ENV << " or install a cache provider package.";
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        LOG(DEBUG) << "AzCacheProvider: no cache library configured — cache disabled";
        return;
    }

    _lib_path = config.lib_path;
    LOG(INFO) << "AzCacheProvider: loading cache library: " << _lib_path;

    _lib_handle = dlopen(_lib_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!_lib_handle)
    {
        std::string err = dlerror();
        if (config.mode == CacheMode::Required)
        {
            LOG(ERROR) << "AzCacheProvider: dlopen failed for '" << _lib_path << "': " << err;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        LOG(WARNING) << "AzCacheProvider: dlopen failed for '" << _lib_path << "': " << err;
        return;
    }

    // ABI version check — reject incompatible libraries early
    auto version_fn = reinterpret_cast<runai_cache_abi_version_fn>(
        dlsym(_lib_handle, RUNAI_CACHE_ABI_VERSION_SYMBOL));
    if (!version_fn)
    {
        if (config.mode == CacheMode::Required)
        {
            LOG(ERROR) << "AzCacheProvider: '" << _lib_path << "' does not export "
                       << RUNAI_CACHE_ABI_VERSION_SYMBOL << " — incompatible library";
            dlclose(_lib_handle);
            _lib_handle = nullptr;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        LOG(WARNING) << "AzCacheProvider: '" << _lib_path << "' does not export "
                     << RUNAI_CACHE_ABI_VERSION_SYMBOL << " — skipping (pre-versioning build)";
        dlclose(_lib_handle);
        _lib_handle = nullptr;
        return;
    }
    uint32_t lib_version = version_fn();
    if (lib_version != RUNAI_CACHE_ABI_VERSION)
    {
        if (config.mode == CacheMode::Required)
        {
            LOG(ERROR) << "AzCacheProvider: ABI version mismatch — expected "
                       << RUNAI_CACHE_ABI_VERSION << ", got " << lib_version;
            dlclose(_lib_handle);
            _lib_handle = nullptr;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        LOG(WARNING) << "AzCacheProvider: ABI version mismatch — expected "
                     << RUNAI_CACHE_ABI_VERSION << ", got " << lib_version << " — skipping";
        dlclose(_lib_handle);
        _lib_handle = nullptr;
        return;
    }

    _cache_read = reinterpret_cast<blob_read_fn>(
        dlsym(_lib_handle, BLOB_READ_SYMBOL));
    if (!_cache_read)
    {
        std::string err = dlerror();
        if (config.mode == CacheMode::Required)
        {
            LOG(ERROR) << "AzCacheProvider: dlsym failed for '" << BLOB_READ_SYMBOL << "': " << err;
            dlclose(_lib_handle);
            _lib_handle = nullptr;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        LOG(WARNING) << "AzCacheProvider: dlsym failed for '" << BLOB_READ_SYMBOL << "': " << err;
        dlclose(_lib_handle);
        _lib_handle = nullptr;
        return;
    }

    _enabled = true;
    LOG(INFO) << "AzCacheProvider: cache provider loaded successfully from " << _lib_path;
}

AzCacheProviderLoader::~AzCacheProviderLoader()
{
    // Intentionally do NOT dlclose — at destruction time the loaded
    // library may have already torn down its own statics, leading to
    // use-after-free.  The OS reclaims everything on process exit.
}

bool AzCacheProviderLoader::read(
    const std::string& account,
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

    char error_buf[RUNAI_CACHE_ERROR_BUF_SIZE] = {};
    ssize_t bytes_read = _cache_read(
        account.c_str(), container.c_str(), blob.c_str(),
        buffer, offset, length, error_buf, sizeof(error_buf));

    if (bytes_read < 0 || static_cast<size_t>(bytes_read) != length)
    {
        if (error_buf[0] != '\0')
        {
            LOG(ERROR) << "AzCacheProvider: cache read failed for "
                       << account << "/" << container << "/" << blob
                       << " offset=" << offset << " length=" << length
                       << ": " << error_buf;
        }
        else
        {
            LOG(ERROR) << "AzCacheProvider: cache read failed for "
                       << account << "/" << container << "/" << blob
                       << " offset=" << offset << " length=" << length;
        }
        return false;
    }

    LOG(SPAM) << "AzCacheProvider: cache read " << length << " bytes from "
              << account << "/" << container << "/" << blob << " offset=" << offset;
    return true;
}

} // namespace runai::llm::streamer::impl::azure
