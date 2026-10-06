#pragma once

#include "SlicePlatform.h"
#include <atomic>
#include <cctype>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace watermelondb {
namespace platform {

constexpr const char* kLocalFileScheme = "file://";

inline bool isLocalFileUrl(const std::string& url) {
    return url.rfind(kLocalFileScheme, 0) == 0;
}

// NSURL percent-encodes the path, so decode it before handing it to fopen.
inline std::string localPathFromFileUrl(const std::string& url) {
    const std::string encoded = url.substr(std::char_traits<char>::length(kLocalFileScheme));
    std::string path;
    path.reserve(encoded.size());
    for (size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '%' && i + 2 < encoded.size() &&
            std::isxdigit(static_cast<unsigned char>(encoded[i + 1])) &&
            std::isxdigit(static_cast<unsigned char>(encoded[i + 2]))) {
            path.push_back(static_cast<char>(std::stoi(encoded.substr(i + 1, 2), nullptr, 16)));
            i += 2;
        } else {
            path.push_back(encoded[i]);
        }
    }
    return path;
}

class LocalFileReadHandle : public DownloadHandle {
public:
    void cancel() override { cancelled.store(true); }
    std::atomic<bool> cancelled{false};
};

struct LocalFileStream : std::enable_shared_from_this<LocalFileStream> {
    static constexpr size_t kChunkBytes = 256 * 1024;

    FILE* file = nullptr;
    std::shared_ptr<LocalFileReadHandle> handle;
    std::function<void(std::function<void()>)> post;
    std::function<void(const uint8_t* data, size_t length)> onData;
    std::function<void(const std::string& errorMessage)> onComplete;
    std::vector<uint8_t> buffer = std::vector<uint8_t>(kChunkBytes);

    ~LocalFileStream() {
        if (file) std::fclose(file);
    }

    // One chunk per work-queue task, so cancel() and other queued work interleave.
    void readNext() {
        if (handle->cancelled.load()) return;
        const size_t length = std::fread(buffer.data(), 1, buffer.size(), file);
        if (length > 0) onData(buffer.data(), length);
        if (handle->cancelled.load()) return;
        if (std::ferror(file)) {
            onComplete("Failed to read local slice file");
            return;
        }
        if (std::feof(file)) {
            onComplete("");
            return;
        }
        auto self = shared_from_this();
        post([self] { self->readNext(); });
    }
};

// Same callback contract as a network download: `post` must run tasks serially on the import work queue.
inline std::shared_ptr<DownloadHandle> streamLocalFile(
    const std::string& url,
    std::function<void(std::function<void()>)> post,
    std::function<void(const uint8_t* data, size_t length)> onData,
    std::function<void(const std::string& errorMessage)> onComplete
) {
    const std::string path = localPathFromFileUrl(url);
    FILE* file = std::fopen(path.c_str(), "rb");
    if (!file) {
        onComplete("Cannot open local slice file: " + path);
        return nullptr;
    }

    auto stream = std::make_shared<LocalFileStream>();
    stream->file = file;
    stream->handle = std::make_shared<LocalFileReadHandle>();
    stream->post = std::move(post);
    stream->onData = std::move(onData);
    stream->onComplete = std::move(onComplete);

    stream->post([stream] { stream->readNext(); });
    return stream->handle;
}

} // namespace platform
} // namespace watermelondb
