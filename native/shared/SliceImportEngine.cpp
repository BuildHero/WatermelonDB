#include "SliceImportEngine.h"
#include <algorithm>
#include <cmath>
#include <chrono>

namespace watermelondb {

// Savepoint interval (rows)
constexpr size_t SAVEPOINT_INTERVAL = 10000;

constexpr size_t MAX_BATCH_SIZE = 10000;
constexpr size_t COMPACT_EVERY_N_CHUNKS = 16;

#ifdef SLICE_IMPORT_VERBOSE_LOGS
static inline void verboseInfo(const std::string& message) { platform::logInfo(message); }
static inline void verboseDebug(const std::string& message) { platform::logDebug(message); }
#else
static inline void verboseInfo(const std::string&) {}
static inline void verboseDebug(const std::string&) {}
#endif

#ifdef SLICE_IMPORT_PROFILE_DECODER
static inline void logDecoderProfile(const SliceDecoder::DecodeProfile& profile) {
    platform::logInfo(
        "Decoder profile: rows=" + std::to_string(profile.rows) +
        ", fields=" + std::to_string(profile.fields) +
        ", null=" + std::to_string(profile.nullCount) +
        ", int=" + std::to_string(profile.intCount) +
        ", real=" + std::to_string(profile.realCount) +
        ", text=" + std::to_string(profile.textCount) +
        ", blob=" + std::to_string(profile.blobCount) +
        ", textBytes=" + std::to_string(profile.textBytes) +
        ", blobBytes=" + std::to_string(profile.blobBytes) +
        ", textCopyMs=" + std::to_string(profile.textCopyNs / 1000000) +
        ", blobCopyMs=" + std::to_string(profile.blobCopyNs / 1000000)
    );
}
#endif
SliceImportEngine::SliceImportEngine(std::shared_ptr<DatabaseInterface> db)
    : db_(db)
    , decoder_(nullptr)
    , downloadHandle_(nullptr)
    , memoryAlertHandle_(nullptr)
    , importing_(false)
    , failed_(false)
    , transactionStarted_(false)
    , headerParsed_(false)
    , parsingTable_(false)
    , batchSize_(0)
    , initialBatchSize_(0)
    , totalRowsInserted_(0)
    , rowsSinceSavepoint_(0)
    , importStart_()
    , totalParseMs_(0)
    , totalFlushMs_(0)
    , flushCount_(0)
    , chunksSinceCompaction_(0)
{
    platform::initializeWorkQueue();
    
    // Calculate optimal batch size based on device hardware
    unsigned long optimalSize = platform::calculateOptimalBatchSize();
    
    // Apply conservative cap
    batchSize_ = std::min(optimalSize, (unsigned long)MAX_BATCH_SIZE);
    initialBatchSize_ = batchSize_;
    
    platform::logInfo("SliceImportEngine initialized with batch size: " + std::to_string(batchSize_));
}

SliceImportEngine::~SliceImportEngine() {
    if (memoryAlertHandle_) {
        memoryAlertHandle_->cancel();
        memoryAlertHandle_.reset();
    }
    
    if (downloadHandle_) {
        downloadHandle_->cancel();
        downloadHandle_.reset();
    }
    
    if (decoder_) {
        decoder_.reset();
    }
}

void SliceImportEngine::startImport(
    const std::string& url,
    std::function<void(const std::string&)> completion
) {
    if (importing_) {
        completion("Import already in progress");
        return;
    }
    
    // Store completion callback
    completionCallback_ = completion;
    
    // Reset state
    importing_ = true;
    failed_ = false;
    transactionStarted_ = false;
    headerParsed_ = false;
    parsingTable_ = false;
    totalRowsInserted_ = 0;
    rowsSinceSavepoint_ = 0;
    batchSize_ = initialBatchSize_;
    currentBatch_.clear();
    totalParseMs_ = 0;
    totalFlushMs_ = 0;
    flushCount_ = 0;
    importStart_ = std::chrono::steady_clock::now();
    chunksSinceCompaction_ = 0;
    
    // Create decoder
    decoder_ = std::make_unique<SliceDecoder>();
    if (!decoder_->initializeDecompression()) {
        fail("Failed to initialize decompression: " + decoder_->getError());
        return;
    }
    
    // Setup memory pressure monitoring
    memoryAlertHandle_ = platform::setupMemoryAlertCallback([this](platform::MemoryAlertLevel level) {
        handleMemoryPressure(level);
    });
    
    // Begin transaction before starting download
    std::string error;
    if (!beginImportTransaction(error)) {
        fail("Failed to begin transaction: " + error);
        return;
    }
    
    platform::logInfo("Starting import from: " + url);
    
    std::shared_ptr<SliceImportEngine> self = shared_from_this();
    
    // Start download (platform-specific)
    downloadHandle_ = platform::downloadFile(
        url,
        [self](const uint8_t* data, size_t length) {
            self->handleDataChunk(data, length);
        },
        [self](const std::string& errorMessage) {
            self->handleDownloadComplete(errorMessage);
        }
    );
    
    if (!downloadHandle_) {
        fail("Failed to start download");
        return;
    }
}

void SliceImportEngine::cancel() {
    if (!importing_) {
        return;
    }
    
    failed_ = true;
    
    if (downloadHandle_) {
        downloadHandle_->cancel();
        downloadHandle_.reset();
    }
    
    if (memoryAlertHandle_) {
        memoryAlertHandle_->cancel();
        memoryAlertHandle_.reset();
    }
    
    if (transactionStarted_) {
        rollbackImportTransaction();
    }
    
    complete("Import cancelled");
}

void SliceImportEngine::handleDataChunk(const uint8_t* data, size_t length) {
    if (failed_) {
        return;
    }
    
    auto parseStart = std::chrono::steady_clock::now();
    
    // Feed to decompressor
    if (!decoder_->feedCompressedData(data, length)) {
        fail("Decompression failed: " + decoder_->getError());
        return;
    }
    
    // Parse decompressed data
    parseDecompressedData();
    
    // Compact buffer to prevent memory growth (throttled)
    chunksSinceCompaction_++;
    if (chunksSinceCompaction_ >= COMPACT_EVERY_N_CHUNKS) {
        decoder_->compactBuffer();
        chunksSinceCompaction_ = 0;
    }
    
    auto parseEnd = std::chrono::steady_clock::now();
    totalParseMs_ += (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(parseEnd - parseStart).count();
}

void SliceImportEngine::handleDownloadComplete(const std::string& errorMessage) {
    if (failed_) {
        return;
    }
    
    if (!errorMessage.empty()) {
        platform::logError("Download failed (engine): " + errorMessage);
        fail("Download failed: " + errorMessage);
        return;
    }
    
    // Parse any remaining data
    parseDecompressedData();
    decoder_->compactBuffer();
    
    // Validate stream completion
    if (!decoder_->isEndOfStream()) {
        fail("Download completed but decompression stream not finished");
        return;
    }
    
    if (decoder_->remainingBytes() > 0) {
        fail("Stream ended with unparsed bytes: " + std::to_string(decoder_->remainingBytes()));
        return;
    }
    
    // Flush final batch
    std::string error;
    if (currentBatch_.totalRows > 0) {
        if (!flushBatch(error)) {
            fail("Failed to flush final batch: " + error);
            return;
        }
    }
    
    // Commit transaction
    if (!commitImportTransaction(error)) {
        fail("Failed to commit transaction: " + error);
        return;
    }
    
    platform::logInfo("Import completed successfully. Total rows: " + std::to_string(totalRowsInserted_));
    auto importEnd = std::chrono::steady_clock::now();
    uint64_t totalMs = (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(importEnd - importStart_).count();
    platform::logInfo("Import timing: total=" + std::to_string(totalMs) + "ms, parse=" +
                      std::to_string(totalParseMs_) + "ms, flush=" + std::to_string(totalFlushMs_) +
                      "ms, flushes=" + std::to_string(flushCount_));
#ifdef SLICE_IMPORT_PROFILE_DECODER
    if (decoder_) {
        logDecoderProfile(decoder_->profile());
    }
#endif
    complete("");
}

void SliceImportEngine::parseDecompressedData() {
    if (!decoder_ || failed_) {
        return;
    }
    
    // Parse header if not done yet
    if (!headerParsed_) {
        SliceHeader header;
        ParseStatus status = decoder_->parseSliceHeader(header);
        
        switch (status) {
            case ParseStatus::Ok:
                verboseInfo("Parsed slice header: id=" + header.sliceId + 
                                ", version=" + std::to_string(header.version) +
                                ", priority=" + header.priority +
                                ", tables=" + std::to_string(header.numberOfTables));
                headerParsed_ = true;
                parseTables();
                break;
                
            case ParseStatus::NeedMoreData:
                // Wait for more data
                break;
                
            case ParseStatus::Error:
                fail("Failed to parse slice header: " + decoder_->getError());
                break;
                
            default:
                fail("Unexpected parse status for slice header");
                break;
        }
    } else {
        // Continue parsing tables
        parseTables();
    }
}

void SliceImportEngine::parseTables() {
    if (!decoder_ || failed_) {
        return;
    }
    
    // If in middle of table, continue with rows
    if (parsingTable_) {
        ParseStatus rowStatus = parseRowsForTable(currentTableHeader_);
        
        if (rowStatus == ParseStatus::Error) {
            return; // Error already handled
        } else if (rowStatus == ParseStatus::NeedMoreData) {
            return; // Wait for more data
        } else if (rowStatus == ParseStatus::EndOfTable) {
            // Table finished, clear state
            parsingTable_ = false;
            // Fall through to parse next table
        }
    }
    
    // Parse table headers and rows
    while (true) {
        TableHeader tableHeader;
        ParseStatus status = decoder_->parseTableHeader(tableHeader);
        
        switch (status) {
            case ParseStatus::Ok: {
                // Store current table
                currentTableHeader_ = tableHeader;
                parsingTable_ = true;
                
                verboseDebug("Parsing table: " + tableHeader.tableName + 
                                 " with " + std::to_string(tableHeader.columns.size()) + " columns");
                
                // Parse rows
                ParseStatus rowStatus = parseRowsForTable(tableHeader);
                
                if (rowStatus == ParseStatus::Error) {
                    return;
                } else if (rowStatus == ParseStatus::NeedMoreData) {
                    return;
                } else if (rowStatus == ParseStatus::EndOfTable) {
                    parsingTable_ = false;
                    continue; // Next table
                }
                break;
            }
                
            case ParseStatus::NeedMoreData:
                return;
                
            case ParseStatus::EndOfStream:
                verboseInfo("Successfully parsed all tables");
                return;
                
            case ParseStatus::Error:
                fail("Failed to parse table header: " + decoder_->getError());
                return;
                
            default:
                fail("Unexpected parse status for table header");
                return;
        }
    }
}

ParseStatus SliceImportEngine::parseRowsForTable(const TableHeader& tableHeader) {
    if (!decoder_ || failed_) {
        return ParseStatus::Error;
    }
    
    size_t rowCount = 0;
    
    std::vector<FieldValue> rowValues;
    rowValues.reserve(tableHeader.columns.size());
    
    while (true) {
        size_t remainingBefore = decoder_->remainingBytes();
        ParseStatus status = decoder_->parseRowValues(tableHeader.columns, rowValues);
        
        switch (status) {
            case ParseStatus::Ok: {
                size_t remainingAfter = decoder_->remainingBytes();
                if (remainingAfter >= remainingBefore) {
                    fail("Parser returned Ok but did not advance (possible infinite loop)");
                    return ParseStatus::Error;
                }
                
                // Add to batch
                currentBatch_.addRow(tableHeader.tableName, tableHeader.columns, rowValues);
                rowCount++;
                
                // Flush if batch full
                if (currentBatch_.totalRows >= batchSize_) {
                    std::string error;
                    if (!flushBatch(error)) {
                        fail("Failed to flush batch: " + error);
                        return ParseStatus::Error;
                    }
                }
                
                if (rowCount % 1000 == 0) {
                    verboseInfo("Parsed " + std::to_string(rowCount) + 
                                    " rows from " + tableHeader.tableName);
                }
                break;
            }
                
            case ParseStatus::NeedMoreData:
                return ParseStatus::NeedMoreData;
                
            case ParseStatus::EndOfTable:
                verboseDebug("Finished table " + tableHeader.tableName + 
                                 " with " + std::to_string(rowCount) + " rows");
                return ParseStatus::EndOfTable;
                
            case ParseStatus::Error:
                fail("Failed to parse row: " + decoder_->getError());
                return ParseStatus::Error;
                
            default:
                fail("Unexpected parse status for row");
                return ParseStatus::Error;
        }
    }
}

bool SliceImportEngine::flushBatch(std::string& errorMessage) {
    if (currentBatch_.totalRows == 0 || failed_) {
        return true;
    }
    
    verboseDebug("Flushing batch: " + std::to_string(currentBatch_.totalRows) + " rows");
    
    auto flushStart = std::chrono::steady_clock::now();
    if (!db_->insertBatch(currentBatch_, errorMessage)) {
        return false;
    }
    auto flushEnd = std::chrono::steady_clock::now();
    totalFlushMs_ += (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(flushEnd - flushStart).count();
    flushCount_++;
    
    // Update counters
    size_t batchRowCount = currentBatch_.totalRows;
    totalRowsInserted_ += batchRowCount;
    rowsSinceSavepoint_ += batchRowCount;
    
    // Clear batch
    currentBatch_.clear();
    
    // Handle savepoint cycling (every 10k rows)
    while (rowsSinceSavepoint_ >= SAVEPOINT_INTERVAL) {
        std::string error;
        
        // Release current savepoint
        if (!db_->releaseSavepoint(error)) {
            verboseDebug("Savepoint release failed (non-fatal): " + error);
        }
        
        // Create new savepoint
        if (!db_->createSavepoint(error)) {
            verboseDebug("Savepoint create failed (non-fatal): " + error);
        } else {
            verboseInfo("Savepoint cycled at " + std::to_string(totalRowsInserted_) + " rows");
        }
        
        rowsSinceSavepoint_ -= SAVEPOINT_INTERVAL;
    }
    
    return true;
}

bool SliceImportEngine::beginImportTransaction(std::string& errorMessage) {
    if (!db_) {
        errorMessage = "Database interface is null";
        return false;
    }
    
    if (!db_->beginTransaction(errorMessage)) {
        return false;
    }
    
    // Create initial savepoint
    std::string savepointError;
    if (!db_->createSavepoint(savepointError)) {
        platform::logDebug("Failed to create initial savepoint: " + savepointError);
        // Non-fatal
    }
    
    transactionStarted_ = true;
    rowsSinceSavepoint_ = 0;
    
    platform::logInfo("Import transaction started");
    return true;
}

bool SliceImportEngine::commitImportTransaction(std::string& errorMessage) {
    if (!db_ || !transactionStarted_) {
        errorMessage = "No transaction to commit";
        return false;
    }
    
    // Release final savepoint (best-effort)
    std::string savepointError;
    db_->releaseSavepoint(savepointError);
    
    // Commit
    if (!db_->commitTransaction(errorMessage)) {
        rollbackImportTransaction();
        return false;
    }
    
    transactionStarted_ = false;
    
    platform::logInfo("Import transaction committed (" + std::to_string(totalRowsInserted_) + " rows)");
    return true;
}

void SliceImportEngine::rollbackImportTransaction() {
    if (!db_ || !transactionStarted_) {
        return;
    }
    
    platform::logError("Rolling back import transaction");
    
    db_->rollbackTransaction();
    transactionStarted_ = false;
}

void SliceImportEngine::handleMemoryPressure(platform::MemoryAlertLevel level) {
    if (failed_) {
        return;
    }
    
    size_t newSize;
    
    switch (level) {
        case platform::MemoryAlertLevel::CRITICAL:
            // Reduce aggressively
            newSize = std::max((size_t)100, batchSize_ / 4);
            platform::logError("CRITICAL memory pressure! Reducing batch size: " + 
                             std::to_string(batchSize_) + " → " + std::to_string(newSize));
            break;
            
        case platform::MemoryAlertLevel::WARN:
            // Reduce moderately
            newSize = std::max((size_t)250, batchSize_ / 2);
            platform::logError("Memory pressure warning. Reducing batch size: " + 
                             std::to_string(batchSize_) + " → " + std::to_string(newSize));
            break;
    }
    
    adjustBatchSize(newSize);
}

void SliceImportEngine::adjustBatchSize(size_t newSize) {
    batchSize_ = newSize;
}

void SliceImportEngine::fail(const std::string& errorMessage) {
    if (failed_) {
        return; // Already failed
    }
    
    platform::logError("Import failed: " + errorMessage);
    auto importEnd = std::chrono::steady_clock::now();
    uint64_t totalMs = (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(importEnd - importStart_).count();
    platform::logError("Import timing (failed): total=" + std::to_string(totalMs) + "ms, parse=" +
                       std::to_string(totalParseMs_) + "ms, flush=" + std::to_string(totalFlushMs_) +
                       "ms, flushes=" + std::to_string(flushCount_));
#ifdef SLICE_IMPORT_PROFILE_DECODER
    if (decoder_) {
        logDecoderProfile(decoder_->profile());
    }
#endif
    
    failed_ = true;
    
    if (downloadHandle_) {
        downloadHandle_->cancel();
        downloadHandle_.reset();
    }
    
    // Rollback transaction if started
    if (transactionStarted_) {
        rollbackImportTransaction();
    }
    
    complete(errorMessage);
}

void SliceImportEngine::complete(const std::string& errorMessage) {
    importing_ = false;
    
    if (memoryAlertHandle_) {
        memoryAlertHandle_->cancel();
        memoryAlertHandle_.reset();
    }
    
    if (downloadHandle_) {
        downloadHandle_.reset();
    }
    
    // Clean up decoder
    if (decoder_) {
        decoder_.reset();
    }
    
    // Call completion callback
    if (completionCallback_) {
        completionCallback_(errorMessage);
        completionCallback_ = nullptr;
    }
}

} // namespace watermelondb
