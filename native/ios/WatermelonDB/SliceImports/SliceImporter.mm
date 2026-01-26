#import "SliceImporter.h"
#import "SliceDecoder.h"
#import <os/log.h>
#import <sqlite3.h>

// Required importing WatermelonDB-Swift
#import <React/RCTEventEmitter.h>

#if __has_include("WatermelonDB-Swift.h")
#import <WatermelonDB-Swift.h>
#else
#import "WatermelonDB-Swift.h"
#endif

using namespace watermelondb;

@interface SliceImporter ()

@property (nonatomic, weak) DatabaseBridge *db;
@property (nonatomic, strong) NSNumber *connectionTag;
@property (nonatomic, strong) NSURLSession *session;
@property (nonatomic, strong) os_log_t logger;
@property (nonatomic, strong) dispatch_queue_t workQueue;
@property (nonatomic, copy, nullable) SliceDownloadCompletion completion;

// Decoder (C++)
@property (nonatomic, assign) SliceDecoder *decoder;
@property (nonatomic, assign) size_t totalBytesReceived;

// Parsing state
@property (nonatomic, assign) BOOL headerParsed;
@property (nonatomic, assign) BOOL hasCalledCompletion;
@property (nonatomic, assign) BOOL didCancelTask;

// Current table being parsed (to resume after NeedMoreData)
@property (nonatomic, assign) BOOL parsingTable;
@property (nonatomic) TableHeader currentTableHeader;
@property (nonatomic, strong) NSArray<NSString *> *currentColumnsArray;

// Batching state
@property (nonatomic, strong) NSMutableArray<NSDictionary *> *pendingBatch;
@property (nonatomic, assign) NSInteger batchSize;
@property (nonatomic, strong) dispatch_semaphore_t inflightSemaphore;
@property (nonatomic, assign) NSInteger maxInflightBatches;
@property (nonatomic, assign) BOOL dbFailed;

@end

@implementation SliceImporter

- (instancetype)initWithDatabaseBridge:(DatabaseBridge *)db connectionTag:(NSNumber *)tag {
    self = [super init];
    
    if (self) {
        _db = db;
        _connectionTag = tag;
        
        _logger = os_log_create("com.buildops.watermelon.slice.importer", "SliceImporter");
        
        _workQueue = dispatch_queue_create("com.buildops.watermelon.slice.importer", DISPATCH_QUEUE_SERIAL);
        
        NSURLSessionConfiguration *config = [NSURLSessionConfiguration defaultSessionConfiguration];
        
        _session = [NSURLSession sessionWithConfiguration:config
                                                 delegate:self
                                            delegateQueue:nil];
        
        _decoder = nullptr;
        _headerParsed = NO;
        _totalBytesReceived = 0;
        _hasCalledCompletion = NO;
        _didCancelTask = NO;
        _parsingTable = NO;
        
        // Batching configuration
        _batchSize = 100;
        _maxInflightBatches = 4;
        _pendingBatch = [NSMutableArray array];
        _inflightSemaphore = dispatch_semaphore_create(_maxInflightBatches);
        _dbFailed = NO;
    }
    
    return self;
}

- (void)startWithURL:(NSURL *)url
          completion:(SliceDownloadCompletion)completion {
    // Store completion on work queue and initialize decoder there
    dispatch_async(self.workQueue, ^{
        // Store completion synchronously on work queue
        self.completion = completion;
        self.hasCalledCompletion = NO;
        self.didCancelTask = NO;
        
        // Clean up any existing decoder
        if (self.decoder) {
            delete self.decoder;
            self.decoder = nullptr;
        }
        
        self.decoder = new SliceDecoder();
        
        if (!self.decoder->initializeDecompression()) {
            os_log_error(self.logger, "Failed to initialize decoder: %{public}s", 
                        self.decoder->getError().c_str());
            
            [self completeWithError:[NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                                        code:-1
                                                    userInfo:@{NSLocalizedDescriptionKey: @"Failed to initialize decoder"}]];
            
            delete self.decoder;
            self.decoder = nullptr;
            return;
        }
        
        self.headerParsed = NO;
        self.totalBytesReceived = 0;
        self.parsingTable = NO;
        self.currentColumnsArray = nil;
        self.dbFailed = NO;
        [self.pendingBatch removeAllObjects];
        
        // Start download on main thread
        dispatch_async(dispatch_get_main_queue(), ^{
            NSURLSessionDataTask *task = [self.session dataTaskWithURL:url];
            [task resume];
        });
    });
}

#pragma mark - NSURLSessionDataDelegate

- (void)URLSession:(NSURLSession *)session
          dataTask:(NSURLSessionDataTask *)dataTask
    didReceiveData:(NSData *)data {
    dispatch_async(self.workQueue, ^{
        // Early exit if DB failed
        if (self.dbFailed) {
            return;
        }
        
        self.totalBytesReceived += data.length;
        os_log_debug(self.logger, "Received %lu bytes (total: %lu)", 
                     (unsigned long)data.length, (unsigned long)self.totalBytesReceived);
        
        // Debug: Log first few bytes of first chunk
        if (self.totalBytesReceived == data.length && data.length > 0) {
            const uint8_t *bytes = (const uint8_t *)data.bytes;
            size_t logLen = MIN(16, data.length);
            NSMutableString *hexStr = [NSMutableString string];
            for (size_t i = 0; i < logLen; i++) {
                [hexStr appendFormat:@"%02X ", bytes[i]];
            }
            os_log_info(self.logger, "First %lu bytes (hex): %{public}@", (unsigned long)logLen, hexStr);
            
            // Check for zstd magic number (0x28, 0xB5, 0x2F, 0xFD)
            if (data.length >= 4) {
                if (bytes[0] == 0x28 && bytes[1] == 0xB5 && bytes[2] == 0x2F && bytes[3] == 0xFD) {
                    os_log_info(self.logger, "Valid zstd magic number detected");
                } else {
                    os_log_error(self.logger, "Invalid zstd magic number! Expected: 28 B5 2F FD");
                }
            }
        }
        
        if (!self.decoder) {
            os_log_error(self.logger, "Decoder not initialized");
            [self failWithError:@"Decoder not initialized" task:dataTask];
            return;
        }
        
        // Feed compressed data to decoder
        if (!self.decoder->feedCompressedData((const uint8_t *)data.bytes, data.length)) {
            os_log_error(self.logger, "Failed to decompress data: %{public}s", 
                        self.decoder->getError().c_str());
            [self failWithError:[NSString stringWithFormat:@"Decompression failed: %s", 
                               self.decoder->getError().c_str()] task:dataTask];
            return;
        }
        
        os_log_debug(self.logger, "Decompressed buffer size: %lu", 
                    (unsigned long)self.decoder->getBufferSize());
        
        // Try to parse the decompressed data
        [self parseDecompressedDataWithTask:dataTask];
        
        // Compact buffer if needed to prevent memory growth
        if (self.decoder) {
            self.decoder->compactBuffer();
        }
    });
}

- (void)failWithError:(NSString *)errorMessage task:(NSURLSessionTask *)task {
    os_log_error(self.logger, "%{public}@", errorMessage);
    
    self.dbFailed = YES;
    self.didCancelTask = YES;
    [task cancel];
    
    [self completeWithError:[NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                                 code:-1
                                             userInfo:@{NSLocalizedDescriptionKey: errorMessage}]];
    
    // Clean up decoder
    if (self.decoder) {
        delete self.decoder;
        self.decoder = nullptr;
    }
}

- (void)completeWithError:(NSError * _Nullable)error {
    // Ensure we only call completion once
    if (self.hasCalledCompletion) {
        return;
    }
    
    self.hasCalledCompletion = YES;
    
    if (self.completion) {
        self.completion(error);
        self.completion = nil;
    }
}

#pragma mark - Parsing

- (void)parseDecompressedDataWithTask:(NSURLSessionTask *)task {
    if (!self.decoder) {
        return;
    }
    
    // Parse slice header if not yet done
    if (!self.headerParsed) {
        SliceHeader header;
        ParseStatus status = self.decoder->parseSliceHeader(header);
        
        switch (status) {
            case ParseStatus::Ok:
                os_log_info(self.logger, "Parsed slice header: id=%{public}s, version=%lld, priority=%{public}s, tables=%lld",
                           header.sliceId.c_str(), header.version, header.priority.c_str(), header.numberOfTables);
                
                // Debug: Log more info about what we parsed
                os_log_info(self.logger, "Decompressed buffer size after header parse: %lu bytes", 
                           (unsigned long)self.decoder->remainingBytes());
                
                // If numberOfTables is 0 but we have remaining data, something is wrong
                if (header.numberOfTables == 0 && self.decoder->remainingBytes() > 10) {
                    os_log_error(self.logger, "Header claims 0 tables but %lu bytes remain - possible parsing error", 
                               (unsigned long)self.decoder->remainingBytes());
                    
                    // Log first 32 bytes of remaining data
                    if (self.decoder->remainingBytes() >= 32) {
                        os_log_error(self.logger, "This might help debug the format issue");
                    }
                }
                
                self.headerParsed = YES;
                // Continue parsing tables
                [self parseTablesWithTask:task];
                break;
                
            case ParseStatus::NeedMoreData:
                // Normal - wait for more data
                break;
                
            case ParseStatus::Error:
                os_log_error(self.logger, "Failed to parse slice header: %{public}s", 
                           self.decoder->getError().c_str());
                [self failWithError:[NSString stringWithFormat:@"Slice header parse error: %s", 
                                   self.decoder->getError().c_str()] task:task];
                break;
                
            default:
                os_log_error(self.logger, "Unexpected parse status for slice header");
                [self failWithError:@"Unexpected parse status" task:task];
                break;
        }
    } else {
        // Header already parsed, continue parsing tables
        [self parseTablesWithTask:task];
    }
}

- (void)parseTablesWithTask:(NSURLSessionTask *)task {
    if (!self.decoder) {
        return;
    }
    
    // If we're in the middle of parsing a table, continue with that table's rows
    if (self.parsingTable) {
        ParseStatus rowStatus = [self parseRowsForTable:self.currentTableHeader task:task];
        
        if (rowStatus == ParseStatus::Error) {
            return; // Error already handled
        } else if (rowStatus == ParseStatus::NeedMoreData) {
            return; // Wait for more data
        } else if (rowStatus == ParseStatus::EndOfTable) {
            // Table finished, clear state and continue to next table
            self.parsingTable = NO;
            self.currentColumnsArray = nil;
            // Fall through to parse next table
        }
    }
    
    // Try to parse table headers and rows in a loop
    while (true) {
        TableHeader tableHeader;
        ParseStatus status = self.decoder->parseTableHeader(tableHeader);
        
        switch (status) {
            case ParseStatus::Ok: {
                // Successfully parsed table header
                os_log_info(self.logger, "Parsing table: %{public}s with %lu columns",
                           tableHeader.tableName.c_str(), (unsigned long)tableHeader.columns.size());
                
                // Store current table and mark as parsing
                self.currentTableHeader = tableHeader;
                self.currentColumnsArray = [self convertColumnsToArray:tableHeader.columns];
                self.parsingTable = YES;
                
                // Parse rows for this table
                ParseStatus rowStatus = [self parseRowsForTable:tableHeader task:task];
                
                if (rowStatus == ParseStatus::Error) {
                    return; // Error already handled
                } else if (rowStatus == ParseStatus::NeedMoreData) {
                    return; // Wait for more data (will resume with this table)
                } else if (rowStatus == ParseStatus::EndOfTable) {
                    // Table finished, clear state and continue to next table
                    self.parsingTable = NO;
                    self.currentColumnsArray = nil;
                    continue;
                }
                break;
            }
                
            case ParseStatus::NeedMoreData:
                // Normal - wait for more network data
                return;
                
            case ParseStatus::EndOfStream:
                // All tables parsed successfully
                os_log_info(self.logger, "Successfully parsed all tables");
                return;
                
            case ParseStatus::Error:
                os_log_error(self.logger, "Failed to parse table header: %{public}s", 
                           self.decoder->getError().c_str());
                [self failWithError:[NSString stringWithFormat:@"Table header parse error: %s", 
                                   self.decoder->getError().c_str()] task:task];
                return;
                
            default:
                os_log_error(self.logger, "Unexpected parse status for table header");
                [self failWithError:@"Unexpected parse status" task:task];
                return;
        }
    }
}

- (ParseStatus)parseRowsForTable:(const TableHeader&)tableHeader task:(NSURLSessionTask *)task {
    if (!self.decoder) {
        return ParseStatus::Error;
    }
    
    size_t rowCount = 0;
    
    while (true) {
        size_t remainingBefore = self.decoder->remainingBytes();
        
        Row row;
        ParseStatus status = self.decoder->parseRow(tableHeader.columns, row);
        
        switch (status) {
            case ParseStatus::Ok: {
                // Progress guard: ensure we advanced
                size_t remainingAfter = self.decoder->remainingBytes();
                if (remainingAfter >= remainingBefore) {
                    os_log_error(self.logger, "Parser returned Ok but did not advance (infinite loop detected)");
                    [self failWithError:@"Internal parser error: no progress" task:task];
                    return ParseStatus::Error;
                }
                
                // Accumulate row into batch
                [self.pendingBatch addObject:@{
                    @"table": [NSString stringWithUTF8String:tableHeader.tableName.c_str()],
                    @"columns": self.currentColumnsArray,  // Reuse cached array
                    @"values": [self convertRowToArray:row columns:tableHeader.columns]
                }];
                
                rowCount++;
                
                // Flush batch if full
                if (self.pendingBatch.count >= self.batchSize) {
                    [self flushBatchToDatabase:task];
                    
                    // Check if DB failed during flush
                    if (self.dbFailed) {
                        return ParseStatus::Error;
                    }
                }
                
                if (rowCount % 100 == 0) {
                    os_log_debug(self.logger, "Parsed %lu rows from %{public}s",
                                (unsigned long)rowCount, tableHeader.tableName.c_str());
                }
                break;
            }
                
            case ParseStatus::NeedMoreData:
                // Need more network data - not necessarily end of table
                return ParseStatus::NeedMoreData;
                
            case ParseStatus::EndOfTable:
                os_log_info(self.logger, "Finished table %{public}s with %lu rows",
                           tableHeader.tableName.c_str(), (unsigned long)rowCount);
                return ParseStatus::EndOfTable;
                
            case ParseStatus::Error:
                os_log_error(self.logger, "Failed to parse row: %{public}s", 
                           self.decoder->getError().c_str());
                [self failWithError:[NSString stringWithFormat:@"Row parse error: %s", 
                                   self.decoder->getError().c_str()] task:task];
                return ParseStatus::Error;
                
            default:
                os_log_error(self.logger, "Unexpected parse status for row");
                [self failWithError:@"Unexpected parse status" task:task];
                return ParseStatus::Error;
        }
    }
}

#pragma mark - Database Insertion

- (NSArray<NSString *> *)convertColumnsToArray:(const std::vector<std::string>&)columns {
    NSMutableArray *array = [NSMutableArray arrayWithCapacity:columns.size()];
    for (const auto& col : columns) {
        [array addObject:[NSString stringWithUTF8String:col.c_str()]];
    }
    return array;
}

- (NSArray *)convertRowToArray:(const Row&)row columns:(const std::vector<std::string>&)columns {
    NSMutableArray *array = [NSMutableArray arrayWithCapacity:columns.size()];
    
    for (const auto& colName : columns) {
        auto it = row.find(colName);
        if (it == row.end()) {
            [array addObject:[NSNull null]];
            continue;
        }
        
        const FieldValue& value = it->second;
        
        switch (value.type) {
            case FieldValue::Type::NULL_VALUE:
                [array addObject:[NSNull null]];
                break;
            case FieldValue::Type::INT_VALUE:
                [array addObject:@(value.intValue)];
                break;
            case FieldValue::Type::REAL_VALUE:
                [array addObject:@(value.realValue)];
                break;
            case FieldValue::Type::TEXT_VALUE:
                [array addObject:[NSString stringWithUTF8String:value.textValue.c_str()]];
                break;
            case FieldValue::Type::BLOB_VALUE:
                [array addObject:[NSData dataWithBytes:value.blobValue.data() 
                                              length:value.blobValue.size()]];
                break;
        }
    }
    
    return array;
}

- (void)flushBatchToDatabase:(NSURLSessionTask *)task {
    if (self.pendingBatch.count == 0 || self.dbFailed) {
        return;
    }
    
    NSArray *batchCopy = [self.pendingBatch copy];
    [self.pendingBatch removeAllObjects];
    
    NSInteger batchCount = batchCopy.count;
    os_log_debug(self.logger, "Flushing batch of %ld rows to database", (long)batchCount);
    
    // Backpressure: wait for available slot
    dispatch_semaphore_wait(self.inflightSemaphore, DISPATCH_TIME_FOREVER);
    
    // Check if failed while waiting
    if (self.dbFailed) {
        dispatch_semaphore_signal(self.inflightSemaphore);
        return;
    }
    
    // Strong-capture db to ensure methodQueue is valid
    DatabaseBridge *strongDB = self.db;
    if (!strongDB) {
        dispatch_semaphore_signal(self.inflightSemaphore);
        self.dbFailed = YES;
        [self.pendingBatch removeAllObjects]; // Prevent further parsing work
        [self failWithError:@"DatabaseBridge deallocated" task:task];
        return;
    }
    
    // Dispatch to database queue
    dispatch_async(strongDB.methodQueue, ^{
        sqlite3 *db = (sqlite3 *)[strongDB getRawConnectionWithConnectionTag:self.connectionTag];
        
        if (!db) {
            os_log_error(self.logger, "Lost database connection");
            dispatch_semaphore_signal(self.inflightSemaphore);
            
            dispatch_async(self.workQueue, ^{
                self.dbFailed = YES;
                [self failWithError:@"Lost database connection" task:task];
            });
            return;
        }
        
        // Begin transaction for this batch
        char *errMsg = nullptr;
        int result = sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, &errMsg);
        
        if (result != SQLITE_OK) {
            if (errMsg) {
                os_log_error(self.logger, "Failed to begin transaction: %{public}s", errMsg);
                sqlite3_free(errMsg);
            }
            dispatch_semaphore_signal(self.inflightSemaphore);
            
            dispatch_async(self.workQueue, ^{
                self.dbFailed = YES;
                [self failWithError:@"Failed to begin transaction" task:task];
            });
            return;
        }
        
        // Insert all rows in batch
        BOOL success = YES;
        for (NSDictionary *item in batchCopy) {
            if (![self insertRowSync:item[@"values"]
                         intoTable:[item[@"table"] UTF8String]
                           columns:item[@"columns"]
                                db:db]) {
                success = NO;
                break;
            }
        }
        
        if (success) {
            // Commit transaction
            result = sqlite3_exec(db, "COMMIT", nullptr, nullptr, &errMsg);
            if (result != SQLITE_OK) {
                if (errMsg) {
                    os_log_error(self.logger, "Failed to commit: %{public}s", errMsg);
                    sqlite3_free(errMsg);
                }
                success = NO;
            }
        }
        
        if (!success) {
            sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
            dispatch_semaphore_signal(self.inflightSemaphore);
            
            dispatch_async(self.workQueue, ^{
                self.dbFailed = YES;
                [self failWithError:@"Database insert failed" task:task];
            });
            return;
        }
        
        // Success - signal semaphore
        dispatch_semaphore_signal(self.inflightSemaphore);
    });
}

- (BOOL)insertRowSync:(NSArray *)values
            intoTable:(const char *)tableName
              columns:(NSArray<NSString *> *)columns
                   db:(sqlite3 *)db {
    // Build INSERT OR IGNORE statement
    // NOTE: Using IGNORE to avoid conflicts - slice rows already have correct IDs
    // If you need to update existing rows, change to INSERT OR REPLACE
    NSMutableString *columnNames = [NSMutableString string];
    NSMutableString *placeholders = [NSMutableString string];
    
    for (NSInteger i = 0; i < columns.count; i++) {
        if (i > 0) {
            [columnNames appendString:@", "];
            [placeholders appendString:@", "];
        }
        [columnNames appendFormat:@"\"%@\"", columns[i]];
        [placeholders appendString:@"?"];
    }
    
    NSString *sql = [NSString stringWithFormat:@"INSERT OR IGNORE INTO \"%s\" (%@, \"_status\") VALUES (%@, 'synced')",
                     tableName, columnNames, placeholders];
    
    // Prepare statement
    sqlite3_stmt *stmt = nullptr;
    int result = sqlite3_prepare_v2(db, [sql UTF8String], -1, &stmt, nullptr);
    if (result != SQLITE_OK) {
        NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
        os_log_error(self.logger, "Failed to prepare: %{public}@", errorString);
        return NO;
    }
    
    // Bind values
    for (NSInteger i = 0; i < values.count; i++) {
        id value = values[i];
        int bindResult = SQLITE_OK;
        
        if ([value isKindOfClass:[NSNull class]]) {
            bindResult = sqlite3_bind_null(stmt, (int)(i + 1));
        } else if ([value isKindOfClass:[NSNumber class]]) {
            NSNumber *num = (NSNumber *)value;
            const char *objCType = [num objCType];
            if (strcmp(objCType, @encode(double)) == 0 || strcmp(objCType, @encode(float)) == 0) {
                bindResult = sqlite3_bind_double(stmt, (int)(i + 1), [num doubleValue]);
            } else {
                bindResult = sqlite3_bind_int64(stmt, (int)(i + 1), [num longLongValue]);
            }
        } else if ([value isKindOfClass:[NSString class]]) {
            bindResult = sqlite3_bind_text(stmt, (int)(i + 1), [(NSString *)value UTF8String], -1, SQLITE_TRANSIENT);
        } else if ([value isKindOfClass:[NSData class]]) {
            NSData *data = (NSData *)value;
            bindResult = sqlite3_bind_blob(stmt, (int)(i + 1), [data bytes], (int)[data length], SQLITE_TRANSIENT);
        }
        
        if (bindResult != SQLITE_OK) {
            NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
            os_log_error(self.logger, "Failed to bind parameter %ld: %{public}@",
                       (long)(i + 1), errorString);
            sqlite3_finalize(stmt);
            return NO;
        }
    }
    
    // Execute
    result = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (result != SQLITE_DONE) {
        NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
        os_log_error(self.logger, "Failed to execute: %{public}@", errorString);
        return NO;
    }
    
    return YES;
}

- (void)URLSession:(NSURLSession *)session
              task:(NSURLSessionTask *)task
didCompleteWithError:(nullable NSError *)error {
    dispatch_async(self.workQueue, ^{
        if (error) {
            // Handle cancellation: skip if we already handled completion
            if (error.code == NSURLErrorCancelled) {
                if (self.didCancelTask || self.dbFailed || self.hasCalledCompletion) {
                    return;
                }
            }
            
            os_log_error(self.logger, "Network error: %{public}@", error);
            self.dbFailed = YES;
            [self completeWithError:error];
            return;
        }
        
        // Download completed successfully
        if (self.dbFailed) {
            // DB already failed, don't continue
            return;
        }
        
        if (self.decoder) {
            // Parse any remaining data
            [self parseDecompressedDataWithTask:task];
            
            // Compact buffer for final checks
            self.decoder->compactBuffer();
            
            // Validate completion state
            if (!self.decoder->isEndOfStream()) {
                os_log_error(self.logger, "Download completed but decompression stream not finished");
                self.dbFailed = YES;
                [self completeWithError:[NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                                            code:-1
                                                        userInfo:@{NSLocalizedDescriptionKey: @"Incomplete decompression"}]];
                return;
            }
            
            if (self.decoder->remainingBytes() > 0) {
                os_log_error(self.logger, "Stream ended with %lu unparsed bytes",
                           (unsigned long)self.decoder->remainingBytes());
                self.dbFailed = YES;
                [self completeWithError:[NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                                            code:-1
                                                        userInfo:@{NSLocalizedDescriptionKey: @"Truncated slice data"}]];
                return;
            }
        }
        
        // Flush final partial batch
        [self flushBatchToDatabase:task];
        
        // Drain semaphore: wait for all in-flight batches to complete
        os_log_debug(self.logger, "Draining in-flight batches");
        for (NSInteger i = 0; i < self.maxInflightBatches; i++) {
            dispatch_semaphore_wait(self.inflightSemaphore, DISPATCH_TIME_FOREVER);
        }
        
        // Restore semaphore permits
        for (NSInteger i = 0; i < self.maxInflightBatches; i++) {
            dispatch_semaphore_signal(self.inflightSemaphore);
        }
        
        // Check if DB failed during final flush
        if (self.dbFailed) {
            return; // Already called completeWithError in flush
        }
        
        // Clean up decoder before completion (prevents reentrancy issues)
        if (self.decoder) {
            delete self.decoder;
            self.decoder = nullptr;
        }
        
        os_log_info(self.logger, "Import completed successfully");
        [self completeWithError:nil];
    });
}

- (void)dealloc {
    [self.session invalidateAndCancel];
    
    if (self.decoder) {
        delete self.decoder;
        self.decoder = nullptr;
    }
}

@end
