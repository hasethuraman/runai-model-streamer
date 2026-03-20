load("@rules_cc//cc:defs.bzl", "cc_library", "cc_binary")
load("//:rules.bzl", "runai_cc_test")

cc_library(
    name = "azcache_provider",
    srcs = ["azcache_provider_loader.cc"],
    hdrs = [
        "runai_azcache_provider.h",
        "azcache_provider_loader.h",
    ],
    visibility = ["//azure:__subpackages__"],
    deps = [
        "//utils/logging",
        "//utils/env",
    ],
    linkopts = ["-ldl"],
)

# Build the example cache as a shared library for testing
cc_binary(
    name = "libsimple_file_cache.so",
    srcs = ["simple_file_cache.cc"],
    linkshared = True,
    linkstatic = False,
)

runai_cc_test(
    name = "azcache_provider_test",
    srcs = ["azcache_provider_test.cc"],
    data = [":libsimple_file_cache.so"],
    deps = [
        ":azcache_provider",
    ],
    linkopts = ["-ldl"],
)
