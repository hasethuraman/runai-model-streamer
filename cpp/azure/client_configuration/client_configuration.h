#pragma once

#include <string>
#include <optional>

namespace runai::llm::streamer::impl::azure
{

struct ClientConfiguration
{
    // Azure client configuration options
    std::optional<std::string> account_name;
    std::optional<std::string> account_key;
    std::optional<std::string> sas_token;
    std::string endpoint_suffix = "blob.core.windows.net";
#ifdef AZURITE_TESTING
    // Connection string is only available for Azurite/local testing
    std::optional<std::string> connection_string;
#endif
    
    // Concurrency settings
    unsigned int max_concurrency = 8;
    
    ClientConfiguration();
};

} // namespace runai::llm::streamer::impl::azure
