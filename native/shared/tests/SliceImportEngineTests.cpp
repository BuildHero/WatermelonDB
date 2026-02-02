#define private public
#include "../SliceImportEngine.h"
#undef private

#include <iostream>
#include <string>
#include <vector>

namespace watermelondb::platform {

void initializeWorkQueue() {}

unsigned long calculateOptimalBatchSize() {
    return 1000;
}

class DummyMemoryHandle : public MemoryAlertHandle {
public:
    void cancel() override {}
};

std::shared_ptr<MemoryAlertHandle> setupMemoryAlertCallback(const std::function<void(MemoryAlertLevel)>&) {
    return std::make_shared<DummyMemoryHandle>();
}

void cancelMemoryPressureMonitoring() {}

class DummyDownloadHandle : public DownloadHandle {
public:
    void cancel() override {}
};

std::shared_ptr<DownloadHandle> downloadFile(
    const std::string&,
    std::function<void(const uint8_t*, size_t)>,
    std::function<void(const std::string&)>
) {
    return std::make_shared<DummyDownloadHandle>();
}

void logInfo(const std::string&) {}
void logDebug(const std::string&) {}
void logError(const std::string&) {}

} // namespace watermelondb::platform

namespace {

static int gFailures = 0;

void expectTrue(bool value, const char* message) {
    if (!value) {
        std::cerr << "FAIL: " << message << "\n";
        gFailures++;
    }
}

void appendVarint(std::vector<uint8_t>& out, uint64_t value) {
    while (value >= 0x80) {
        out.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}

void appendString(std::vector<uint8_t>& out, const std::string& value) {
    appendVarint(out, static_cast<uint64_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

void appendTextField(std::vector<uint8_t>& out, const std::string& value) {
    appendVarint(out, static_cast<uint64_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
    out.push_back(static_cast<uint8_t>(watermelondb::TypeTag::TEXT));
}

struct FakeDb : public watermelondb::DatabaseInterface {
    int beginCount = 0;
    int commitCount = 0;
    int rollbackCount = 0;
    int insertBatchCount = 0;
    int createSavepointCount = 0;
    int releaseSavepointCount = 0;
    watermelondb::BatchData lastBatch;

    bool beginTransaction(std::string&) override {
        beginCount++;
        return true;
    }
    bool commitTransaction(std::string&) override {
        commitCount++;
        return true;
    }
    void rollbackTransaction() override {
        rollbackCount++;
    }
    bool insertRows(
        const std::string&,
        const std::vector<std::string>&,
        const std::vector<std::vector<watermelondb::FieldValue>>&,
        std::string&
    ) override {
        return true;
    }
    bool insertBatch(const watermelondb::BatchData& batch, std::string&) override {
        insertBatchCount++;
        lastBatch = batch;
        return true;
    }
    bool createSavepoint(std::string&) override {
        createSavepointCount++;
        return true;
    }
    bool releaseSavepoint(std::string&) override {
        releaseSavepointCount++;
        return true;
    }
};

void setupDecoderWithSingleRow(watermelondb::SliceImportEngine& engine) {
    std::vector<uint8_t> data;
    appendString(data, "slice1");
    appendVarint(data, 1);
    appendString(data, "high");
    appendVarint(data, 1);
    appendVarint(data, 1);

    appendString(data, "tasks");
    appendVarint(data, 2);
    appendString(data, "id");
    appendString(data, "name");
    appendTextField(data, "t1");
    appendTextField(data, "Alpha");
    data.push_back(watermelondb::END_OF_TABLE_DELIMITER);

    engine.decoder_ = std::make_unique<watermelondb::SliceDecoder>();
    engine.decoder_->streamInitialized_ = true;
    engine.decoder_->streamEnded_ = true;
    engine.decoder_->decompressedBuffer_ = data;
    engine.decoder_->decompressedSize_ = data.size();
    engine.decoder_->currentOffset_ = 0;
    engine.headerParsed_ = false;
    engine.failed_ = false;
}

void test_parse_decompressed_and_flush() {
    auto db = std::make_shared<FakeDb>();
    watermelondb::SliceImportEngine engine(db);
    engine.batchSize_ = 1;
    setupDecoderWithSingleRow(engine);

    engine.parseDecompressedData();

    expectTrue(db->insertBatchCount == 1, "insertBatch should be called");
    expectTrue(engine.totalRowsInserted_ == 1, "totalRowsInserted should increment");
}

void test_savepoint_cycle_on_flush() {
    auto db = std::make_shared<FakeDb>();
    watermelondb::SliceImportEngine engine(db);

    watermelondb::BatchData batch;
    batch.addRow("tasks", {"id"}, {watermelondb::FieldValue::makeText("t1")});
    engine.currentBatch_ = batch;
    engine.rowsSinceSavepoint_ = 9999;

    std::string error;
    engine.flushBatch(error);

    expectTrue(db->releaseSavepointCount == 1, "releaseSavepoint should be called");
    expectTrue(db->createSavepointCount == 1, "createSavepoint should be called");
}

void test_memory_pressure_adjusts_batch() {
    auto db = std::make_shared<FakeDb>();
    watermelondb::SliceImportEngine engine(db);
    engine.batchSize_ = 1000;

    engine.handleMemoryPressure(watermelondb::platform::MemoryAlertLevel::WARN);
    expectTrue(engine.batchSize_ == 500, "WARN halves batch size");

    engine.batchSize_ = 1000;
    engine.handleMemoryPressure(watermelondb::platform::MemoryAlertLevel::CRITICAL);
    expectTrue(engine.batchSize_ == 250, "CRITICAL quarters batch size");
}

} // namespace

int main() {
    test_parse_decompressed_and_flush();
    test_savepoint_cycle_on_flush();
    test_memory_pressure_adjusts_batch();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All SliceImportEngine tests passed\n";
    return 0;
}
