#pragma once

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

namespace SPTAG
{
namespace TPSQL
{

inline constexpr std::uint64_t UnknownBytes = std::numeric_limits<std::uint64_t>::max();

struct ProcessMemoryBytes
{
    std::uint64_t rss = UnknownBytes;
    std::uint64_t rssAnon = UnknownBytes;
    std::uint64_t rssFile = UnknownBytes;
    std::uint64_t rssShmem = UnknownBytes;
};

inline std::uint64_t ParseKbValue(const std::string& line)
{
    std::istringstream fields(line);
    std::string key;
    std::uint64_t kb = 0;
    fields >> key >> kb;
    if (!fields) {
        return UnknownBytes;
    }
    return kb * 1024;
}

inline ProcessMemoryBytes CurrentProcessMemoryBytes()
{
    ProcessMemoryBytes memory;
#ifdef __linux__
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            memory.rss = ParseKbValue(line);
        }
        else if (line.rfind("RssAnon:", 0) == 0) {
            memory.rssAnon = ParseKbValue(line);
        }
        else if (line.rfind("RssFile:", 0) == 0) {
            memory.rssFile = ParseKbValue(line);
        }
        else if (line.rfind("RssShmem:", 0) == 0) {
            memory.rssShmem = ParseKbValue(line);
        }
    }
#endif
    return memory;
}

inline std::uint64_t FileSizeBytes(const std::string& path)
{
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (error) {
        return UnknownBytes;
    }
    return static_cast<std::uint64_t>(size);
}

inline std::string FormatOptionalBytes(std::uint64_t bytes)
{
    if (bytes == UnknownBytes) {
        return "na";
    }
    return std::to_string(bytes);
}

inline bool MemoryLogEnabled()
{
    const char* value = std::getenv("TPSQL_DEBUG");
    if (value == nullptr) {
        return false;
    }
    const std::string text(value);
    return text == "1" || text == "true" || text == "TRUE" || text == "yes" || text == "on";
}

inline void LogMemoryEvent(
    const char* component,
    const char* event,
    const std::string& stage,
    const std::string& path = std::string(),
    std::uint64_t fileBytes = UnknownBytes,
    std::uint64_t heapBytes = UnknownBytes,
    std::uint64_t elapsedMs = UnknownBytes,
    const std::string& details = std::string())
{
    if (!MemoryLogEnabled()) {
        return;
    }
    const ProcessMemoryBytes memory = CurrentProcessMemoryBytes();
    std::ostringstream line;
    line << "tpsql spfresh_memory"
         << " component=" << component
         << " event=" << event
         << " stage=" << stage
         << " rss_bytes=" << FormatOptionalBytes(memory.rss)
         << " rss_anon_bytes=" << FormatOptionalBytes(memory.rssAnon)
         << " rss_file_bytes=" << FormatOptionalBytes(memory.rssFile)
         << " rss_shmem_bytes=" << FormatOptionalBytes(memory.rssShmem);
    if (!path.empty()) {
        line << " path=" << path;
    }
    if (fileBytes != UnknownBytes) {
        line << " file_bytes=" << fileBytes;
    }
    if (heapBytes != UnknownBytes) {
        line << " heap_bytes=" << heapBytes;
    }
    if (elapsedMs != UnknownBytes) {
        line << " elapsed_ms=" << elapsedMs;
    }
    if (!details.empty()) {
        line << " " << details;
    }
    std::cerr << line.str() << std::endl;
}

} // namespace TPSQL
} // namespace SPTAG
