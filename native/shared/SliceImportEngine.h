#pragma once

#include "SliceDecoder.h"
#include "SlicePlatform.h"
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <chrono>

namespace watermelondb {

// Forward declarations
class SliceImportEngine;
struct BatchData;

// Batch structure for accumulated rows
struct BatchData {
    std::unordered_map<std::string, std::vector<std::vector<FieldValue>>> tables;
    std::unordered_map<std::string, std::vector<std::string>> tableColumns;
    size_t totalRows = 0;
    
    void clear() {
        tables.clear();
        tableColumns.clear();
        totalRows = 0;
    }
    
    void addRow(const std::string& tableName, 
                const std::vector<std::string>& columns,
                const std::vector<FieldValue>& row) {
        tables[tableName].push_back(row);
        if (tableColumns.find(tableName) == tableColumns.end()) {
            tableColumns[tableName] = columns;
        }
        totalRows++;
    }
};

// Database interface - platform implements this
class DatabaseInterface {
public:
    virtual ~DatabaseInterface() = default;
    
    // Begin transaction (called once at start)
    virtual bool beginTransaction(std::string& errorMessage) = 0;
    
    // Commit transaction (called once at end)
    virtual bool commitTransaction(std::string& errorMessage) = 0;
    
    // Rollback transaction
    virtual void rollbackTransaction() = 0;
    
    // Insert rows using multi-row INSERT (batched for performance)
    // Returns true on success, false on failure
    virtual bool insertRows(
        const std::string& tableName,
        const std::vector<std::string>& columns,
        const std::vector<std::vector<FieldValue>>& rows,
        std::string& errorMessage
    ) = 0;
    
    // Insert full batch in a single DB-queue hop
    virtual bool insertBatch(
        const BatchData& batch,
        std::string& errorMessage
    ) = 0;
    
    // Create savepoint (for periodic checkpointing)
    virtual bool createSavepoint(std::string& errorMessage) = 0;
    
    // Release savepoint
    virtual bool releaseSavepoint(std::string& errorMessage) = 0;
};

// Main slice import orchestration engine
class SliceImportEngine : public std::enable_shared_from_this<SliceImportEngine> {
public:
    // Constructor
    // db: Platform-specific database interface
    explicit SliceImportEngine(std::shared_ptr<DatabaseInterface> db);
    
    // Destructor
    ~SliceImportEngine();
    
    // Start import from URL
    // completion: called with empty string on success, error message on failure
    void startImport(
        const std::string& url,
        std::function<void(const std::string& errorMessage)> completion
    );
    
    // Cancel ongoing import
    void cancel();
    
    // Get current state
    bool isImporting() const { return importing_; }
    bool hasFailed() const { return failed_; }
    
    // Get statistics
    size_t getTotalRowsInserted() const { return totalRowsInserted_; }
    size_t getBatchSize() const { return batchSize_; }
    
private:
    // Platform database interface
    std::shared_ptr<DatabaseInterface> db_;
    
    // Decoder
    std::unique_ptr<SliceDecoder> decoder_;
    
    // Download handle
    std::shared_ptr<platform::DownloadHandle> downloadHandle_;
    
    // Memory pressure handle
    std::shared_ptr<platform::MemoryAlertHandle> memoryAlertHandle_;
    
    // State
    bool importing_;
    bool failed_;
    bool transactionStarted_;
    bool headerParsed_;
    
    // Current table being parsed
    bool parsingTable_;
    TableHeader currentTableHeader_;
    
    // Batching
    BatchData currentBatch_;
    size_t batchSize_;
    size_t initialBatchSize_;
    
    // Transaction management
    size_t totalRowsInserted_;
    size_t rowsSinceSavepoint_;

    // Timing (milliseconds)
    std::chrono::steady_clock::time_point importStart_;
    uint64_t totalParseMs_;
    uint64_t totalFlushMs_;
    size_t flushCount_;

    // Compaction throttling
    size_t chunksSinceCompaction_;
    
    // Completion callback
    std::function<void(const std::string&)> completionCallback_;
    
    // Internal handlers
    void handleDataChunk(const uint8_t* data, size_t length);
    void handleDownloadComplete(const std::string& errorMessage);
    void parseDecompressedData();
    void parseTables();
    ParseStatus parseRowsForTable(const TableHeader& tableHeader);
    
    // Database operations
    bool flushBatch(std::string& errorMessage);
    bool beginImportTransaction(std::string& errorMessage);
    bool commitImportTransaction(std::string& errorMessage);
    void rollbackImportTransaction();
    
    // Memory management
    void handleMemoryPressure(platform::MemoryAlertLevel level);
    void adjustBatchSize(size_t newSize);
    
    // Error handling
    void fail(const std::string& errorMessage);
    void complete(const std::string& errorMessage);
};

} // namespace watermelondb
