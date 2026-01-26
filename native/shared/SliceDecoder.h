#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <libzstd/zstd.h>

namespace watermelondb {

// Parse status for streaming operations
enum class ParseStatus {
    Ok,              // Successfully parsed
    NeedMoreData,    // Need more bytes to complete parsing
    EndOfTable,      // Encountered end-of-table delimiter
    EndOfStream,     // Reached end of stream
    Error            // Fatal error occurred
};

// Type tags for binary slice format
enum class TypeTag : uint8_t {
    NULL_TYPE = 0x00,
    INT = 0x01,
    REAL = 0x02,
    TEXT = 0x03,
    BLOB = 0x04
};

// End-of-table delimiter
constexpr uint8_t END_OF_TABLE_DELIMITER = 0xFF;

// Memory management constants
constexpr size_t COMPACTION_THRESHOLD = 2 * 1024 * 1024; // 2MB - compact when offset exceeds this
constexpr size_t MAX_BUFFER_CAPACITY = 16 * 1024 * 1024; // 16MB - shrink if capacity exceeds this when empty

// Safety limits for varint decoding
constexpr size_t MAX_STRING_LENGTH = 1024 * 1024; // 1MB max for strings
constexpr size_t MAX_FIELD_SIZE = 10 * 1024 * 1024; // 10MB max for field values
constexpr size_t MAX_COLUMN_NAME_LENGTH = 256; // 256 bytes max for column names
constexpr size_t MAX_TABLE_NAME_LENGTH = 256; // 256 bytes max for table names

// Forward declarations
struct SliceHeader;
struct TableHeader;
class VarintDecoder;
class SliceDecoder;

// Slice header structure
struct SliceHeader {
    std::string sliceId;
    int64_t version;
    std::string priority;
    int64_t timestamp;
    int64_t numberOfTables;
};

// Table header structure
struct TableHeader {
    std::string tableName;
    std::vector<std::string> columns;
};

// Field value variant
struct FieldValue {
    enum class Type {
        NULL_VALUE,
        INT_VALUE,
        REAL_VALUE,
        TEXT_VALUE,
        BLOB_VALUE
    };
    
    Type type;
    union {
        int64_t intValue;
        double realValue;
    };
    std::string textValue;
    std::vector<uint8_t> blobValue;
    
    FieldValue() : type(Type::NULL_VALUE), intValue(0) {}
    
    static FieldValue makeNull() {
        FieldValue val;
        val.type = Type::NULL_VALUE;
        return val;
    }
    
    static FieldValue makeInt(int64_t value) {
        FieldValue val;
        val.type = Type::INT_VALUE;
        val.intValue = value;
        return val;
    }
    
    static FieldValue makeReal(double value) {
        FieldValue val;
        val.type = Type::REAL_VALUE;
        val.realValue = value;
        return val;
    }
    
    static FieldValue makeText(const std::string& value) {
        FieldValue val;
        val.type = Type::TEXT_VALUE;
        val.textValue = value;
        return val;
    }
    
    static FieldValue makeBlob(const std::vector<uint8_t>& value) {
        FieldValue val;
        val.type = Type::BLOB_VALUE;
        val.blobValue = value;
        return val;
    }
};

// Row data structure
using Row = std::map<std::string, FieldValue>;

// Varint decoder utility
class VarintDecoder {
public:
    struct DecodeResult {
        uint64_t value;
        size_t bytesRead;
        bool success;
        bool invalid;  // true if varint was decoded but value is invalid/corrupt
        
        DecodeResult() : value(0), bytesRead(0), success(false), invalid(false) {}
    };
    
    static DecodeResult decodeVarint(const uint8_t* buffer, size_t bufferSize, size_t offset);
    
    struct StringDecodeResult {
        std::string value;
        size_t bytesRead;
        bool success;
        bool invalid;  // true if length was decoded but is too large or corrupt
        
        StringDecodeResult() : value(""), bytesRead(0), success(false), invalid(false) {}
    };
    
    static StringDecodeResult decodeString(const uint8_t* buffer, size_t bufferSize, size_t offset);
};

// Main slice decoder class
class SliceDecoder {
public:
    SliceDecoder();
    ~SliceDecoder();
    
    // Initialize decompression stream for a new file
    bool initializeDecompression();
    
    // Reset decoder for a new file
    void reset();
    
    // Feed compressed data chunk
    bool feedCompressedData(const uint8_t* data, size_t size);
    
    // Parse slice header (must be called first after enough data)
    ParseStatus parseSliceHeader(SliceHeader& header);
    
    // Parse next table header
    ParseStatus parseTableHeader(TableHeader& header);
    
    // Parse next row
    ParseStatus parseRow(const std::vector<std::string>& columns, Row& row);
    
    // Check if end of stream
    bool isEndOfStream() const { return streamEnded_; }
    
    // Get current buffer size
    size_t getBufferSize() const { return decompressedSize_ - currentOffset_; }
    
    // Get remaining unparsed bytes
    size_t remainingBytes() const { return decompressedSize_ - currentOffset_; }
    
    // Get error message (if any)
    const std::string& getError() const { return errorMessage_; }
    
    // Compact buffer to free memory
    void compactBuffer();
    
private:
    // Decompression state
    ZSTD_DStream* dstream_;
    bool streamInitialized_;
    bool streamEnded_;
    
    // Decompressed data buffer
    std::vector<uint8_t> decompressedBuffer_;
    size_t decompressedSize_;
    size_t currentOffset_;
    
    // Parsing state
    bool headerParsed_;
    bool expectingTableHeader_;
    int64_t expectedTables_;
    int64_t tablesParsed_;
    
    // Error tracking
    std::string errorMessage_;
    
    // Internal helpers
    bool decompressChunk(const uint8_t* input, size_t inputSize);
    void setError(const std::string& error);
};

} // namespace watermelondb
