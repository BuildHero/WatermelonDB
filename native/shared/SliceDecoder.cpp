#include "SliceDecoder.h"
#include <cstring>
#include <algorithm>

namespace watermelondb {

// VarintDecoder implementation
VarintDecoder::DecodeResult VarintDecoder::decodeVarint(const uint8_t* buffer, size_t bufferSize, size_t offset) {
    DecodeResult result;
    
    if (offset >= bufferSize) {
        return result;
    }
    
    uint64_t value = 0;
    size_t shift = 0;
    size_t bytesRead = 0;
    uint8_t byte;
    
    do {
        if (offset + bytesRead >= bufferSize) {
            return result; // Not enough data
        }
        
        byte = buffer[offset + bytesRead];
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        shift += 7;
        bytesRead++;
        
        if (bytesRead > 10) {
            // Varint too long - this is corrupt data, not "need more"
            result.invalid = true;
            return result;
        }
    } while (byte & 0x80);
    
    result.value = value;
    result.bytesRead = bytesRead;
    result.success = true;
    return result;
}

VarintDecoder::StringDecodeResult VarintDecoder::decodeString(const uint8_t* buffer, size_t bufferSize, size_t offset) {
    StringDecodeResult result;
    
    // First decode the length
    auto lengthResult = decodeVarint(buffer, bufferSize, offset);
    if (lengthResult.invalid) {
        // Varint was corrupt (too long)
        result.invalid = true;
        return result;
    }
    if (!lengthResult.success) {
        // Need more data
        return result;
    }
    
    size_t length = static_cast<size_t>(lengthResult.value);
    
    // Sanity check: reject unreasonably large strings
    if (length > MAX_STRING_LENGTH) {
        result.invalid = true;
        return result;
    }
    
    size_t stringOffset = offset + lengthResult.bytesRead;
    
    if (stringOffset + length > bufferSize) {
        return result; // Not enough data
    }
    
    result.value = std::string(reinterpret_cast<const char*>(buffer + stringOffset), length);
    result.bytesRead = lengthResult.bytesRead + length;
    result.success = true;
    return result;
}

// SliceDecoder implementation
SliceDecoder::SliceDecoder()
    : dstream_(nullptr)
    , streamInitialized_(false)
    , streamEnded_(false)
    , decompressedSize_(0)
    , currentOffset_(0)
    , headerParsed_(false)
    , expectingTableHeader_(true)
    , expectedTables_(0)
    , tablesParsed_(0)
{
}

SliceDecoder::~SliceDecoder() {
    if (dstream_) {
        ZSTD_freeDStream(dstream_);
        dstream_ = nullptr;
    }
}

bool SliceDecoder::initializeDecompression() {
    if (streamInitialized_) {
        return true;
    }
    
    dstream_ = ZSTD_createDStream();
    if (!dstream_) {
        setError("Failed to create ZSTD decompression stream");
        return false;
    }
    
    size_t const initResult = ZSTD_initDStream(dstream_);
    if (ZSTD_isError(initResult)) {
        setError(std::string("Failed to initialize ZSTD stream: ") + ZSTD_getErrorName(initResult));
        ZSTD_freeDStream(dstream_);
        dstream_ = nullptr;
        return false;
    }
    
    streamInitialized_ = true;
    return true;
}

void SliceDecoder::reset() {
    if (dstream_) {
        ZSTD_freeDStream(dstream_);
        dstream_ = nullptr;
    }
    
    streamInitialized_ = false;
    streamEnded_ = false;
    decompressedBuffer_.clear();
    decompressedSize_ = 0;
    currentOffset_ = 0;
    headerParsed_ = false;
    expectingTableHeader_ = true;
    expectedTables_ = 0;
    tablesParsed_ = 0;
    errorMessage_.clear();
}

void SliceDecoder::compactBuffer() {
    // If fully consumed, clear and shrink only if buffer is large
    if (currentOffset_ == decompressedSize_) {
        if (decompressedBuffer_.capacity() > MAX_BUFFER_CAPACITY) {
            // Swap with empty vector to force deallocation
            std::vector<uint8_t>().swap(decompressedBuffer_);
        } else {
            decompressedBuffer_.clear();
        }
        decompressedSize_ = 0;
        currentOffset_ = 0;
        return;
    }
    
    // If offset is large (absolute or relative), compact by moving remaining bytes to front
    size_t remaining = decompressedSize_ - currentOffset_;
    bool shouldCompact = (currentOffset_ > COMPACTION_THRESHOLD) || 
                        (decompressedSize_ > 0 && currentOffset_ > decompressedSize_ / 2);
    
    if (shouldCompact && remaining > 0) {
        std::memmove(decompressedBuffer_.data(), 
                    decompressedBuffer_.data() + currentOffset_, 
                    remaining);
        decompressedBuffer_.resize(remaining);
        decompressedSize_ = remaining;
        currentOffset_ = 0;
    }
}

bool SliceDecoder::feedCompressedData(const uint8_t* data, size_t size) {
    if (!streamInitialized_) {
        setError("Decompression stream not initialized");
        return false;
    }
    
    return decompressChunk(data, size);
}

bool SliceDecoder::decompressChunk(const uint8_t* input, size_t inputSize) {
    ZSTD_inBuffer inBuffer = {input, inputSize, 0};
    
    // Allocate output buffer
    size_t const outputBufferSize = ZSTD_DStreamOutSize();
    std::vector<uint8_t> outputBuffer(outputBufferSize);
    
    while (inBuffer.pos < inBuffer.size) {
        ZSTD_outBuffer outBuffer = {outputBuffer.data(), outputBufferSize, 0};
        
        size_t const result = ZSTD_decompressStream(dstream_, &outBuffer, &inBuffer);
        
        if (ZSTD_isError(result)) {
            setError(std::string("Decompression error: ") + ZSTD_getErrorName(result));
            return false;
        }
        
        if (outBuffer.pos > 0) {
            // Append decompressed data to buffer
            size_t oldSize = decompressedBuffer_.size();
            decompressedBuffer_.resize(oldSize + outBuffer.pos);
            std::memcpy(decompressedBuffer_.data() + oldSize, outputBuffer.data(), outBuffer.pos);
            decompressedSize_ = decompressedBuffer_.size();
        }
        
        // Check if we've reached the end of the frame
        if (result == 0) {
            streamEnded_ = true;
        }
    }
    
    return true;
}

ParseStatus SliceDecoder::parseSliceHeader(SliceHeader& header) {
    if (headerParsed_) {
        setError("Slice header already parsed");
        return ParseStatus::Error;
    }
    
    size_t offset = currentOffset_;
    size_t available = decompressedSize_ - currentOffset_;
    
    if (available == 0) {
        if (streamEnded_) {
            setError("Unexpected end of stream while parsing slice header");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    
    // Decode sliceId (string)
    auto sliceIdResult = VarintDecoder::decodeString(decompressedBuffer_.data(), decompressedSize_, offset);
    if (sliceIdResult.invalid) {
        setError("Invalid sliceId: string too long or corrupt varint");
        return ParseStatus::Error;
    }
    if (!sliceIdResult.success) {
        if (streamEnded_) {
            setError("Failed to decode sliceId: truncated data");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    header.sliceId = sliceIdResult.value;
    offset += sliceIdResult.bytesRead;
    
    // Decode version (varint)
    auto versionResult = VarintDecoder::decodeVarint(decompressedBuffer_.data(), decompressedSize_, offset);
    if (versionResult.invalid) {
        setError("Invalid version: corrupt varint");
        return ParseStatus::Error;
    }
    if (!versionResult.success) {
        if (streamEnded_) {
            setError("Failed to decode version: truncated data");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    header.version = static_cast<int64_t>(versionResult.value);
    offset += versionResult.bytesRead;
    
    // Decode priority (string)
    auto priorityResult = VarintDecoder::decodeString(decompressedBuffer_.data(), decompressedSize_, offset);
    if (priorityResult.invalid) {
        setError("Invalid priority: string too long or corrupt varint");
        return ParseStatus::Error;
    }
    if (!priorityResult.success) {
        if (streamEnded_) {
            setError("Failed to decode priority: truncated data");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    header.priority = priorityResult.value;
    offset += priorityResult.bytesRead;
    
    // Decode timestamp (varint)
    auto timestampResult = VarintDecoder::decodeVarint(decompressedBuffer_.data(), decompressedSize_, offset);
    if (timestampResult.invalid) {
        setError("Invalid timestamp: corrupt varint");
        return ParseStatus::Error;
    }
    if (!timestampResult.success) {
        if (streamEnded_) {
            setError("Failed to decode timestamp: truncated data");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    header.timestamp = static_cast<int64_t>(timestampResult.value);
    offset += timestampResult.bytesRead;
    
    // Decode numberOfTables (varint)
    auto numberOfTablesResult = VarintDecoder::decodeVarint(decompressedBuffer_.data(), decompressedSize_, offset);
    if (numberOfTablesResult.invalid) {
        setError("Invalid numberOfTables: corrupt varint");
        return ParseStatus::Error;
    }
    if (!numberOfTablesResult.success) {
        if (streamEnded_) {
            setError("Failed to decode numberOfTables: truncated data");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    header.numberOfTables = static_cast<int64_t>(numberOfTablesResult.value);
    offset += numberOfTablesResult.bytesRead;
    
    // Validate and store expected table count
    // Note: Some slice files may have numberOfTables=0 and write tables until EOF
    // We'll handle this by not enforcing table count if it's 0
    if (header.numberOfTables < 0 || header.numberOfTables > 10000) {
        setError("Invalid numberOfTables: out of reasonable range");
        return ParseStatus::Error;
    }
    
    currentOffset_ = offset;
    headerParsed_ = true;
    expectingTableHeader_ = true;
    expectedTables_ = header.numberOfTables;
    tablesParsed_ = 0;
    
    return ParseStatus::Ok;
}

ParseStatus SliceDecoder::parseTableHeader(TableHeader& header) {
    size_t available = decompressedSize_ - currentOffset_;
    
    if (available == 0) {
        if (streamEnded_) {
            // Check if we've parsed expected number of tables
            if (tablesParsed_ < expectedTables_) {
                setError("Stream ended before all expected tables were parsed");
                return ParseStatus::Error;
            }
            return ParseStatus::EndOfStream;
        }
        return ParseStatus::NeedMoreData;
    }
    
    // Check if we've exceeded expected table count
    // Note: If expectedTables_ is 0, we read until EndOfStream (legacy format)
    if (expectedTables_ > 0 && tablesParsed_ >= expectedTables_) {
        // If we've parsed all expected tables, this is end of stream
        if (tablesParsed_ == expectedTables_) {
            return ParseStatus::EndOfStream;
        }
        // More tables than expected - error
        setError("More tables in stream than declared in header");
        return ParseStatus::Error;
    }
    
    // Check for delimiter if we're not expecting a table header
    if (!expectingTableHeader_) {
        if (decompressedBuffer_[currentOffset_] != END_OF_TABLE_DELIMITER) {
            setError("Expected end-of-table delimiter");
            return ParseStatus::Error;
        }
        currentOffset_++;
        available--;
        
        if (available == 0) {
            if (streamEnded_) {
                // Only check table count if expectedTables_ > 0
                if (expectedTables_ > 0 && tablesParsed_ < expectedTables_) {
                    setError("Stream ended before all expected tables were parsed");
                    return ParseStatus::Error;
                }
                return ParseStatus::EndOfStream;
            }
            return ParseStatus::NeedMoreData;
        }
        expectingTableHeader_ = true;
    } else {
        // Skip delimiter if present (for tables after the first one)
        if (decompressedBuffer_[currentOffset_] == END_OF_TABLE_DELIMITER) {
            currentOffset_++;
            available--;
            
            if (available == 0) {
                if (streamEnded_) {
                    // Only check table count if expectedTables_ > 0
                    if (expectedTables_ > 0 && tablesParsed_ < expectedTables_) {
                        setError("Stream ended before all expected tables were parsed");
                        return ParseStatus::Error;
                    }
                    return ParseStatus::EndOfStream;
                }
                return ParseStatus::NeedMoreData;
            }
        }
    }
    
    size_t offset = currentOffset_;
    
    // Decode table name (string)
    auto tableNameResult = VarintDecoder::decodeString(decompressedBuffer_.data(), decompressedSize_, offset);
    if (tableNameResult.invalid) {
        setError("Invalid table name: string too long or corrupt varint");
        return ParseStatus::Error;
    }
    if (!tableNameResult.success) {
        if (streamEnded_) {
            setError("Failed to decode table name: truncated data");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    
    // Validate table name
    if (tableNameResult.value.empty() || tableNameResult.value.length() > MAX_TABLE_NAME_LENGTH) {
        setError("Invalid table name length");
        return ParseStatus::Error;
    }
    
    header.tableName = tableNameResult.value;
    offset += tableNameResult.bytesRead;
    
    // Decode column count (varint)
    auto columnCountResult = VarintDecoder::decodeVarint(decompressedBuffer_.data(), decompressedSize_, offset);
    if (columnCountResult.invalid) {
        setError("Invalid column count: corrupt varint");
        return ParseStatus::Error;
    }
    if (!columnCountResult.success) {
        if (streamEnded_) {
            setError("Failed to decode column count: truncated data");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    size_t columnCount = static_cast<size_t>(columnCountResult.value);
    offset += columnCountResult.bytesRead;
    
    // Validate column count
    if (columnCount > 200 || columnCount < 1) {
        setError("Invalid column count");
        return ParseStatus::Error;
    }
    
    // Decode column names
    header.columns.clear();
    header.columns.reserve(columnCount);
    
    for (size_t i = 0; i < columnCount; i++) {
        auto columnResult = VarintDecoder::decodeString(decompressedBuffer_.data(), decompressedSize_, offset);
        if (columnResult.invalid) {
            setError("Invalid column name: string too long or corrupt varint");
            return ParseStatus::Error;
        }
        if (!columnResult.success) {
            if (streamEnded_) {
                setError("Failed to decode column name: truncated data");
                return ParseStatus::Error;
            }
            return ParseStatus::NeedMoreData;
        }
        
        // Validate column name length
        if (columnResult.value.empty() || columnResult.value.length() > MAX_COLUMN_NAME_LENGTH) {
            setError("Invalid column name length");
            return ParseStatus::Error;
        }
        
        header.columns.push_back(columnResult.value);
        offset += columnResult.bytesRead;
    }
    
    currentOffset_ = offset;
    expectingTableHeader_ = false;
    tablesParsed_++;
    
    return ParseStatus::Ok;
}

ParseStatus SliceDecoder::parseRow(const std::vector<std::string>& columns, Row& row) {
    size_t available = decompressedSize_ - currentOffset_;
    
    if (available == 0) {
        if (streamEnded_) {
            setError("Unexpected end of stream while parsing row");
            return ParseStatus::Error;
        }
        return ParseStatus::NeedMoreData;
    }
    
    // Check for end-of-table delimiter
    if (decompressedBuffer_[currentOffset_] == END_OF_TABLE_DELIMITER) {
        expectingTableHeader_ = true;
        return ParseStatus::EndOfTable;
    }
    
    row.clear();
    size_t offset = currentOffset_;
    
    for (const auto& column : columns) {
        // Decode field size (varint)
        auto sizeResult = VarintDecoder::decodeVarint(decompressedBuffer_.data(), decompressedSize_, offset);
        if (sizeResult.invalid) {
            setError("Invalid field size: corrupt varint");
            return ParseStatus::Error;
        }
        if (!sizeResult.success) {
            if (streamEnded_) {
                setError("Failed to decode field size: truncated data");
                return ParseStatus::Error;
            }
            return ParseStatus::NeedMoreData;
        }
        size_t fieldSize = static_cast<size_t>(sizeResult.value);
        offset += sizeResult.bytesRead;
        
        // Validate field size to prevent excessive memory allocation
        if (fieldSize > MAX_FIELD_SIZE) {
            setError("Field size exceeds maximum allowed");
            return ParseStatus::Error;
        }
        
        if (fieldSize == 0) {
            // NULL field: need type tag byte
            // Note: When size is 0, the value is NULL regardless of what the type tag says
            if (offset >= decompressedSize_) {
                if (streamEnded_) {
                    setError("Truncated NULL field: missing type tag");
                    return ParseStatus::Error;
                }
                return ParseStatus::NeedMoreData;
            }
            
            // Skip type tag (don't validate - NULL regardless of tag value)
            row[column] = FieldValue::makeNull();
            offset++; // Skip type tag
            continue;
        }
        
        // Ensure we have enough data for value + type tag
        if (decompressedSize_ < offset + fieldSize + 1) {
            if (streamEnded_) {
                setError("Truncated field: missing value or type tag");
                return ParseStatus::Error;
            }
            return ParseStatus::NeedMoreData;
        }
        
        // Read type tag
        uint8_t typeTag = decompressedBuffer_[offset + fieldSize];
        
        // Read value based on type
        switch (static_cast<TypeTag>(typeTag)) {
            case TypeTag::NULL_TYPE:
                row[column] = FieldValue::makeNull();
                break;
                
            case TypeTag::INT: {
                if (fieldSize != 8) {
                    setError("Invalid INT field size");
                    return ParseStatus::Error;
                }
                // Read as big-endian int64
                int64_t value = 0;
                for (size_t i = 0; i < 8; i++) {
                    value = (value << 8) | decompressedBuffer_[offset + i];
                }
                row[column] = FieldValue::makeInt(value);
                break;
            }
                
            case TypeTag::REAL: {
                if (fieldSize != 8) {
                    setError("Invalid REAL field size");
                    return ParseStatus::Error;
                }
                // Read as big-endian double
                uint64_t bits = 0;
                for (size_t i = 0; i < 8; i++) {
                    bits = (bits << 8) | decompressedBuffer_[offset + i];
                }
                double value;
                std::memcpy(&value, &bits, sizeof(double));
                row[column] = FieldValue::makeReal(value);
                break;
            }
                
            case TypeTag::TEXT: {
                std::string text(reinterpret_cast<const char*>(decompressedBuffer_.data() + offset), fieldSize);
                row[column] = FieldValue::makeText(text);
                break;
            }
                
            case TypeTag::BLOB: {
                std::vector<uint8_t> blob(decompressedBuffer_.begin() + offset, 
                                         decompressedBuffer_.begin() + offset + fieldSize);
                row[column] = FieldValue::makeBlob(blob);
                break;
            }
                
            default:
                setError("Unknown type tag");
                return ParseStatus::Error;
        }
        
        offset += fieldSize + 1; // Skip value + type tag
    }
    
    currentOffset_ = offset;
    return ParseStatus::Ok;
}

void SliceDecoder::setError(const std::string& error) {
    errorMessage_ = error;
}

} // namespace watermelondb
