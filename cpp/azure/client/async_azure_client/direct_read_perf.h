#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl::azure
{

enum class DirectReadMetric : size_t
{
    AsyncReadSubmit,
    QueueDelay,
    DownloadTask,
    BlobClientConstruction,
    DownloadTo,
    CompletionCallback,
    Count,
};

class DirectReadPerf
{
public:
    using Clock = std::chrono::steady_clock;

    static DirectReadPerf& instance()
    {
        static DirectReadPerf perf;
        return perf;
    }

    void record(DirectReadMetric metric, Clock::duration duration)
    {
        const auto index = static_cast<size_t>(metric);
        const auto nanoseconds =
            std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        _count[index].fetch_add(1, std::memory_order_relaxed);
        _total_nanoseconds[index].fetch_add(nanoseconds, std::memory_order_relaxed);

        auto maximum = _max_nanoseconds[index].load(std::memory_order_relaxed);
        while (maximum < nanoseconds &&
               !_max_nanoseconds[index].compare_exchange_weak(
                   maximum, nanoseconds, std::memory_order_relaxed))
        {
        }
    }

    void request_started()
    {
        _active_requests.fetch_add(1, std::memory_order_relaxed);
    }

    void request_finished()
    {
        const auto completed =
            _completed_requests.fetch_add(1, std::memory_order_relaxed) + 1;
        if (_active_requests.fetch_sub(1, std::memory_order_acq_rel) == 1 ||
            completed % 500 == 0)
        {
            log();
        }
    }

private:
    static constexpr std::array<const char*, static_cast<size_t>(DirectReadMetric::Count)> names = {
        "AsyncReadSubmit",
        "QueueDelay",
        "DownloadTask",
        "BlobClientConstruction",
        "DownloadTo",
        "CompletionCallback",
    };

    void log()
    {
        std::lock_guard<std::mutex> lock(_log_mutex);
        for (size_t i = 0; i < names.size(); ++i)
        {
            const auto count = _count[i].load(std::memory_order_relaxed);
            if (count == 0)
            {
                continue;
            }

            const auto total_nanoseconds =
                _total_nanoseconds[i].load(std::memory_order_relaxed);
            const auto max_nanoseconds =
                _max_nanoseconds[i].load(std::memory_order_relaxed);
            LOG(WARNING) << "DirectReadPerf metric=" << names[i]
                         << " count=" << count
                         << " total_ms=" << total_nanoseconds / 1'000'000
                         << " avg_us=" << total_nanoseconds / count / 1'000
                         << " max_us=" << max_nanoseconds / 1'000;
        }
    }

    std::array<std::atomic<uint64_t>, static_cast<size_t>(DirectReadMetric::Count)> _count{};
    std::array<std::atomic<int64_t>, static_cast<size_t>(DirectReadMetric::Count)>
        _total_nanoseconds{};
    std::array<std::atomic<int64_t>, static_cast<size_t>(DirectReadMetric::Count)>
        _max_nanoseconds{};
    std::atomic<uint64_t> _active_requests{0};
    std::atomic<uint64_t> _completed_requests{0};
    std::mutex _log_mutex;
};

class ScopedDirectReadMetric
{
public:
    explicit ScopedDirectReadMetric(DirectReadMetric metric)
        : _metric(metric), _start(DirectReadPerf::Clock::now())
    {
    }

    ~ScopedDirectReadMetric()
    {
        DirectReadPerf::instance().record(
            _metric, DirectReadPerf::Clock::now() - _start);
    }

private:
    DirectReadMetric _metric;
    DirectReadPerf::Clock::time_point _start;
};

} // namespace runai::llm::streamer::impl::azure
