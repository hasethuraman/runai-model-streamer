#include "azure/azcache_provider/azcache_provider_loader.h"

#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/exception/exception.h"
#include "utils/dylib/dylib.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::azure
{

struct CacheMarkerPublisher
{
    using Clock = std::chrono::steady_clock;
    static constexpr auto IdleDelay = std::chrono::seconds(5);

    struct Context
    {
        std::string account;
        std::string container;
        std::string model;
        Clock::time_point last_activity{};
        bool loading = true;
        bool enabled = false;
        bool pending = false;
    };

    CacheMarkerPublisher(blob_cache_marker_fn load_fn, blob_cache_marker_fn push_fn)
        : _load_fn(load_fn),
          _push_fn(push_fn),
          _worker([this] { run(); })
    {}

    ~CacheMarkerPublisher()
    {
        stop_and_flush();
    }

    bool initialize(
        const std::string& account,
        const std::string& container,
        const std::string& model)
    {
        const std::string key = make_key(account, container, model);
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto existing = _contexts.find(key);
            if (existing != _contexts.end())
            {
                _cv.wait(lock, [&] { return !_contexts.at(key).loading; });
                return _contexts.at(key).enabled;
            }
            _contexts.emplace(key, Context{account, container, model});
        }

        char error_buf[RUNAI_CACHE_ERROR_BUF_SIZE] = {};
        const int result = _load_fn(
            account.c_str(), container.c_str(), model.c_str(),
            error_buf, sizeof(error_buf));

        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto& context = _contexts.at(key);
            context.enabled = result == 0;
            context.loading = false;
            _cv.notify_all();
        }

        if (result < 0)
        {
            LOG(WARNING) << "AzCacheProvider: failed to load model markers for "
                         << account << "/" << container << "/" << model
                         << (error_buf[0] ? ": " + std::string(error_buf) : "");
        }
        else if (result == 0)
        {
            LOG(INFO) << "AzCacheProvider: loaded model markers for "
                      << account << "/" << container << "/" << model;
        }
        return result == 0;
    }

    void record_activity(
        const std::string& account,
        const std::string& container,
        const std::string& model)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto context = _contexts.find(make_key(account, container, model));
        if (context == _contexts.end() || !context->second.enabled)
        {
            return;
        }
        context->second.last_activity = Clock::now();
        context->second.pending = true;
        _cv.notify_all();
    }

    void stop_and_flush()
    {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_stopping)
            {
                return;
            }
            _stopping = true;
            _cv.notify_all();
        }
        if (_worker.joinable())
        {
            _worker.join();
        }

        std::vector<Context> pending;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            for (auto& [key, context] : _contexts)
            {
                if (context.enabled && context.pending)
                {
                    context.pending = false;
                    pending.push_back(context);
                }
            }
        }
        for (const auto& context : pending)
        {
            push(context);
        }
    }

private:
    static std::string make_key(
        const std::string& account,
        const std::string& container,
        const std::string& model)
    {
        return account + "\n" + container + "\n" + model;
    }

    bool push(const Context& context)
    {
        char error_buf[RUNAI_CACHE_ERROR_BUF_SIZE] = {};
        const int result = _push_fn(
            context.account.c_str(), context.container.c_str(), context.model.c_str(),
            error_buf, sizeof(error_buf));
        if (result != 0)
        {
            LOG(WARNING) << "AzCacheProvider: failed to push model markers for "
                         << context.account << "/" << context.container << "/" << context.model
                         << (error_buf[0] ? ": " + std::string(error_buf) : "");
            return false;
        }
        LOG(INFO) << "AzCacheProvider: pushed model markers after idle period for "
                  << context.account << "/" << context.container << "/" << context.model;
        return true;
    }

    void run()
    {
        std::unique_lock<std::mutex> lock(_mutex);
        while (!_stopping)
        {
            const auto now = Clock::now();
            auto next_deadline = Clock::time_point::max();
            std::vector<Context> due;
            for (auto& [key, context] : _contexts)
            {
                if (!context.enabled || !context.pending)
                {
                    continue;
                }
                const auto deadline = context.last_activity + IdleDelay;
                if (deadline <= now)
                {
                    context.pending = false;
                    due.push_back(context);
                }
                else
                {
                    next_deadline = std::min(next_deadline, deadline);
                }
            }

            if (due.empty())
            {
                if (next_deadline == Clock::time_point::max())
                {
                    _cv.wait(lock);
                }
                else
                {
                    _cv.wait_until(lock, next_deadline);
                }
                continue;
            }

            lock.unlock();
            for (const auto& context : due)
            {
                if (!push(context))
                {
                    std::lock_guard<std::mutex> retry_lock(_mutex);
                    auto& retry = _contexts.at(make_key(
                        context.account, context.container, context.model));
                    retry.pending = true;
                    retry.last_activity = Clock::now();
                }
            }
            lock.lock();
        }
    }

    blob_cache_marker_fn _load_fn;
    blob_cache_marker_fn _push_fn;
    std::mutex _mutex;
    std::condition_variable _cv;
    std::unordered_map<std::string, Context> _contexts;
    bool _stopping = false;
    std::thread _worker;
};

// Static members
std::weak_ptr<CacheLibHandle> AzCacheProviderLoader::s_shared_handle;
std::mutex AzCacheProviderLoader::s_handle_mutex;

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

// --- CacheLibHandle implementation ---

CacheLibHandle::CacheLibHandle(const std::string& path, CacheMode mode)
    : lib_handle(nullptr),
      read_fn(nullptr),
      close_fn(nullptr),
      marker_publisher(nullptr),
      lib_path(path)
{
    LOG(INFO) << "AzCacheProvider: loading cache library: " << lib_path;

    try
    {
        lib_handle = utils::Dylib::dlopen(lib_path, RTLD_NOW | RTLD_LOCAL);
    }
    catch (...)
    {
        if (mode == CacheMode::Required)
        {
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        return;
    }

    // ABI version check
    runai_cache_abi_version_fn version_fn = nullptr;
    try
    {
        version_fn = utils::Dylib::dlsym<runai_cache_abi_version_fn>(lib_handle, RUNAI_CACHE_ABI_VERSION_SYMBOL);
    }
    catch (...)
    {
        if (mode == CacheMode::Required)
        {
            LOG(ERROR) << "AzCacheProvider: '" << lib_path << "' does not export "
                       << RUNAI_CACHE_ABI_VERSION_SYMBOL << " — incompatible library";
            utils::Dylib::dlclose(lib_handle);
            lib_handle = nullptr;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        LOG(WARNING) << "AzCacheProvider: '" << lib_path << "' does not export "
                     << RUNAI_CACHE_ABI_VERSION_SYMBOL << " — skipping (pre-versioning build)";
        utils::Dylib::dlclose(lib_handle);
        lib_handle = nullptr;
        return;
    }

    uint32_t lib_version = version_fn();
    if (lib_version != RUNAI_CACHE_ABI_VERSION)
    {
        if (mode == CacheMode::Required)
        {
            LOG(ERROR) << "AzCacheProvider: ABI version mismatch — expected "
                       << RUNAI_CACHE_ABI_VERSION << ", got " << lib_version;
            utils::Dylib::dlclose(lib_handle);
            lib_handle = nullptr;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        LOG(WARNING) << "AzCacheProvider: ABI version mismatch — expected "
                     << RUNAI_CACHE_ABI_VERSION << ", got " << lib_version << " — skipping";
        utils::Dylib::dlclose(lib_handle);
        lib_handle = nullptr;
        return;
    }

    // Resolve blob_read (required)
    try
    {
        read_fn = utils::Dylib::dlsym<blob_read_fn>(lib_handle, BLOB_READ_SYMBOL);
    }
    catch (...)
    {
        if (mode == CacheMode::Required)
        {
            utils::Dylib::dlclose(lib_handle);
            lib_handle = nullptr;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        utils::Dylib::dlclose(lib_handle);
        lib_handle = nullptr;
        return;
    }

    // Resolve shutdown (optional — don't throw if missing)
    try
    {
        close_fn = utils::Dylib::dlsym<shutdown_fn>(lib_handle, SHUTDOWN_SYMBOL);
    }
    catch (...)
    {
        close_fn = nullptr; // not exported, that's fine
    }

    blob_cache_marker_fn load_markers_fn = nullptr;
    blob_cache_marker_fn push_markers_fn = nullptr;
    try
    {
        load_markers_fn = utils::Dylib::dlsym<blob_cache_marker_fn>(
            lib_handle, BLOB_CACHE_LOAD_MARKERS_SYMBOL);
        push_markers_fn = utils::Dylib::dlsym<blob_cache_marker_fn>(
            lib_handle, BLOB_CACHE_PUSH_MARKERS_SYMBOL);
    }
    catch (...)
    {
        load_markers_fn = nullptr;
        push_markers_fn = nullptr;
    }
    if (load_markers_fn && push_markers_fn)
    {
        marker_publisher = std::make_unique<CacheMarkerPublisher>(
            load_markers_fn, push_markers_fn);
    }

    LOG(INFO) << "AzCacheProvider: cache provider loaded successfully from " << lib_path
              << " (shutdown: " << (close_fn ? "yes" : "no") << ")";
}

CacheLibHandle::~CacheLibHandle()
{
    if (!lib_handle)
    {
        return;
    }

    marker_publisher.reset();

    // Call shutdown() for graceful shutdown before releasing the handle
    if (close_fn)
    {
        LOG(INFO) << "AzCacheProvider: calling shutdown() for graceful shutdown";
        try
        {
            close_fn();
        }
        catch (...)
        {
            LOG(ERROR) << "AzCacheProvider: shutdown() threw an exception";
        }
    }

    // Note: we intentionally do NOT call dlclose() here.
    lib_handle = nullptr;

    LOG(INFO) << "AzCacheProvider: released cache provider handle";
}

bool CacheLibHandle::initialize_markers(
    const std::string& account,
    const std::string& container,
    const std::string& model)
{
    return marker_publisher &&
        marker_publisher->initialize(account, container, model);
}

void CacheLibHandle::record_marker_activity(
    const std::string& account,
    const std::string& container,
    const std::string& model)
{
    if (marker_publisher)
    {
        marker_publisher->record_activity(account, container, model);
    }
}

// --- AzCacheProviderLoader implementation ---

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
    : _handle(nullptr),
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

    // Acquire or create the shared library handle
    {
        std::lock_guard<std::mutex> lock(s_handle_mutex);
        _handle = s_shared_handle.lock();

        if (_handle && _handle->lib_handle && _handle->lib_path == config.lib_path)
        {
            // Reuse existing handle for same library
            LOG(DEBUG) << "AzCacheProvider: reusing shared cache library handle for " << config.lib_path;
        }
        else
        {
            // Create new handle (first loader or path changed)
            _handle = std::make_shared<CacheLibHandle>(config.lib_path, config.mode);
            s_shared_handle = _handle;
        }
    }

    if (_handle && _handle->read_fn)
    {
        _enabled = true;
    }
    LOG(INFO) << "AzCacheProvider: cache provider is " << (_enabled ? "enabled" : "disabled");
}

AzCacheProviderLoader::~AzCacheProviderLoader()
{
    // Release under mutex so destruction of CacheLibHandle (which calls shutdown())
    // cannot race with a new loader trying to lock() the weak_ptr and recreate.
    std::lock_guard<std::mutex> lock(s_handle_mutex);
    _handle.reset();
}

void AzCacheProviderLoader::initialize_markers(
    const std::string& account,
    const std::string& container,
    const std::string& model)
{
    if (_handle && _handle->initialize_markers(account, container, model))
    {
        _marker_account = account;
        _marker_container = container;
        _marker_model = model;
    }
}

bool AzCacheProviderLoader::read(
    const std::string& account,
    const std::string& container,
    const std::string& blob,
    char* buffer,
    size_t offset,
    size_t length)
{
    if (!_enabled || !_handle || !_handle->read_fn)
    {
        return false;
    }

    char error_buf[RUNAI_CACHE_ERROR_BUF_SIZE] = {};
    ssize_t bytes_read = _handle->read_fn(
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
    if (!_marker_model.empty())
    {
        _handle->record_marker_activity(
            _marker_account, _marker_container, _marker_model);
    }
    return true;
}

} // namespace runai::llm::streamer::impl::azure
