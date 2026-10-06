#include "../SliceLocalFile.h"

#include <cstdio>
#include <deque>
#include <iostream>
#include <string>
#include <unistd.h>
#include <vector>

using namespace watermelondb::platform;

namespace {

int gFailures = 0;

void expectTrue(bool value, const std::string& message) {
    if (!value) {
        std::cerr << "FAIL: " << message << "\n";
        gFailures++;
    }
}

// Stands in for the serial import work queue: tasks run only when drained.
struct SerialQueue {
    std::deque<std::function<void()>> tasks;

    std::function<void(std::function<void()>)> poster() {
        return [this](std::function<void()> task) { tasks.push_back(std::move(task)); };
    }

    void drain() {
        while (!tasks.empty()) {
            auto task = std::move(tasks.front());
            tasks.pop_front();
            task();
        }
    }
};

struct Received {
    std::vector<size_t> chunkSizes;
    std::vector<uint8_t> bytes;
    std::vector<std::string> completions;
};

std::string writeTempFile(const std::string& name, const std::vector<uint8_t>& contents) {
    char dir[] = "/tmp/slice-local-file-XXXXXX";
    std::string path = std::string(mkdtemp(dir)) + "/" + name;
    FILE* file = std::fopen(path.c_str(), "wb");
    std::fwrite(contents.data(), 1, contents.size(), file);
    std::fclose(file);
    return path;
}

std::vector<uint8_t> patternBytes(size_t length) {
    std::vector<uint8_t> bytes(length);
    for (size_t i = 0; i < length; ++i) bytes[i] = static_cast<uint8_t>(i * 31 + 7);
    return bytes;
}

std::shared_ptr<DownloadHandle> stream(const std::string& url, SerialQueue& queue, Received& received) {
    return streamLocalFile(
        url,
        queue.poster(),
        [&received](const uint8_t* data, size_t length) {
            received.chunkSizes.push_back(length);
            received.bytes.insert(received.bytes.end(), data, data + length);
        },
        [&received](const std::string& error) { received.completions.push_back(error); }
    );
}

void testUrlDetection() {
    expectTrue(isLocalFileUrl("file:///var/slices/a.slice.zst"), "file:// URL is local");
    expectTrue(!isLocalFileUrl("https://bucket.s3.amazonaws.com/a.slice.zst"), "https URL is not local");
    expectTrue(!isLocalFileUrl("/var/slices/a.slice.zst"), "bare path is not a file:// URL");
}

void testStreamsWholeFileInChunks() {
    const auto contents = patternBytes(600 * 1024);
    const auto path = writeTempFile("slice.zst", contents);
    SerialQueue queue;
    Received received;

    auto handle = stream("file://" + path, queue, received);
    expectTrue(handle != nullptr, "handle returned for an existing file");
    expectTrue(received.bytes.empty(), "nothing is read until the work queue runs");

    queue.drain();

    expectTrue(received.chunkSizes.size() == 3, "600 KB arrives as 3 chunks of up to 256 KB");
    expectTrue(received.bytes == contents, "bytes arrive complete and in order");
    expectTrue(received.completions.size() == 1 && received.completions[0].empty(), "completes once with no error");
}

void testDecodesPercentEncodedPath() {
    const auto contents = patternBytes(10);
    const auto path = writeTempFile("my slice.zst", contents);
    std::string encoded = path;
    encoded.replace(encoded.find(' '), 1, "%20");
    SerialQueue queue;
    Received received;

    stream("file://" + encoded, queue, received);
    queue.drain();

    expectTrue(received.bytes == contents, "%20 in the URL is decoded to a space");
    expectTrue(received.completions.size() == 1 && received.completions[0].empty(), "encoded path completes cleanly");
}

void testMissingFileFails() {
    SerialQueue queue;
    Received received;

    auto handle = stream("file:///tmp/slice-local-file-missing/none.zst", queue, received);
    queue.drain();

    expectTrue(handle == nullptr, "no handle for a missing file");
    expectTrue(received.completions.size() == 1 && received.completions[0].find("Cannot open") != std::string::npos,
               "missing file completes with an open error");
}

void testCancelStopsReading() {
    const auto path = writeTempFile("slice.zst", patternBytes(600 * 1024));
    SerialQueue queue;
    Received received;

    auto handle = stream("file://" + path, queue, received);
    auto first = std::move(queue.tasks.front());
    queue.tasks.pop_front();
    first();
    handle->cancel();
    queue.drain();

    expectTrue(received.chunkSizes.size() == 1, "no chunks after cancel");
    expectTrue(received.completions.empty(), "a cancelled read does not complete");
}

void testEmptyFileCompletes() {
    const auto path = writeTempFile("empty.zst", {});
    SerialQueue queue;
    Received received;

    stream("file://" + path, queue, received);
    queue.drain();

    expectTrue(received.bytes.empty(), "empty file delivers no bytes");
    expectTrue(received.completions.size() == 1 && received.completions[0].empty(), "empty file completes with no error");
}

} // namespace

int main() {
    testUrlDetection();
    testStreamsWholeFileInChunks();
    testDecodesPercentEncodedPath();
    testMissingFileFails();
    testCancelStopsReading();
    testEmptyFileCompletes();

    if (gFailures > 0) {
        std::cerr << gFailures << " failure(s)\n";
        return 1;
    }
    std::cout << "SliceLocalFile tests passed\n";
    return 0;
}
