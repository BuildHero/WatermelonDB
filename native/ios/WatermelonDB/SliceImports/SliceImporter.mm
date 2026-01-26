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

// Queue-specific key for workQueue identification
static void *kSliceImporterQueueKey = &kSliceImporterQueueKey;

@interface SliceImporter ()

@property (nonatomic, weak) DatabaseBridge *db;
@property (nonatomic, strong) NSNumber *connectionTag;
@property (nonatomic, strong) NSURLSession *session;
@property (nonatomic, atomic, strong, nullable) NSURLSessionDataTask *activeTask;  // Atomic for cross-queue cancellation
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
@property (nonatomic, strong) NSString *currentTableName;  // Cached to avoid per-row UTF8 conversion
@property (nonatomic, strong) NSArray<NSString *> *currentColumnsArray;
@property (nonatomic, strong) NSString *currentColumnsSignature;  // Cached for statement cache key

// Batching state
@property (nonatomic, strong) NSMutableDictionary *pendingBatch;  // {tableName, columnsArray, valuesRows: [[val,val,...], ...]}
@property (nonatomic, assign) NSInteger batchSize;
@property (nonatomic, assign) NSInteger initialBatchSize;  // Store initial for adaptive scaling
@property (nonatomic, assign) NSInteger pendingRowCount;  // Fast counter for flush threshold (avoid O(#tables) scan)
@property (nonatomic, strong) dispatch_semaphore_t inflightSemaphore;
@property (nonatomic, assign) NSInteger maxInflightBatches;
@property (atomic, assign) BOOL dbFailed;  // Atomic: read from multiple queues
@property (atomic, assign) BOOL transactionStarted;  // Atomic: tracks if BEGIN IMMEDIATE succeeded
@property (nonatomic, strong) dispatch_source_t memoryPressureSource;

// Transaction state (accessed only on db.methodQueue - atomic for safety)
@property (atomic, assign) NSInteger rowsSinceSavepoint;
@property (atomic, assign) NSInteger totalRowsInserted;  // For savepoint logic
@property (nonatomic, strong) NSMutableDictionary<NSString *, NSValue *> *statementCache;  // "table|columnsSignature" -> sqlite3_stmt* wrapped in NSValue

// Progress tracking
@property (nonatomic, assign) int64_t totalExpectedBytes;
@property (nonatomic, assign) int64_t bytesDownloaded;

@end

@implementation SliceImporter

- (instancetype)initWithDatabaseBridge:(DatabaseBridge *)db connectionTag:(NSNumber *)tag {
    self = [super init];
    
    if (self) {
        _db = db;
        _connectionTag = tag;
        
        _logger = os_log_create("com.buildops.watermelon.slice.importer", "SliceImporter");
        
        _workQueue = dispatch_queue_create("com.buildops.watermelon.slice.importer", DISPATCH_QUEUE_SERIAL);
        
        // Set queue-specific key for dispatch_get_specific checks
        dispatch_queue_set_specific(_workQueue, kSliceImporterQueueKey, kSliceImporterQueueKey, NULL);
        
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
        
        // Batching configuration - dynamically sized based on device
        _batchSize = [self calculateOptimalBatchSize];
        _initialBatchSize = _batchSize;
        _maxInflightBatches = 1;  // Serial DB queue = inflight=1 for max throughput
        _pendingBatch = [NSMutableDictionary dictionary];
        _pendingRowCount = 0;  // Fast counter for flush threshold
        _inflightSemaphore = dispatch_semaphore_create(_maxInflightBatches);
        _dbFailed = NO;
        
        // Statement cache and transaction state (accessed only on db.methodQueue)
        _statementCache = [NSMutableDictionary dictionary];
        _rowsSinceSavepoint = 0;
        _totalRowsInserted = 0;
        
        // Set up memory pressure monitoring
        [self setupMemoryPressureMonitoring];
    }
    
    return self;
}

- (NSInteger)calculateOptimalBatchSize {
    // Get device memory
    NSProcessInfo *processInfo = [NSProcessInfo processInfo];
    unsigned long long physicalMemory = processInfo.physicalMemory;
    
    // Get available processors for concurrency considerations
    NSInteger processorCount = processInfo.activeProcessorCount;
    
    // Calculate batch size based on memory tiers
    // Keep conservative to leave headroom for OS and other apps
    NSInteger batchSize;
    
    if (physicalMemory >= 6ULL * 1024 * 1024 * 1024) {
        // 6+ GB RAM (iPhone 12 Pro and newer, iPad Pro)
        // Can handle large batches, but cap at 2000 to be safe
        batchSize = 2000;
    } else if (physicalMemory >= 4ULL * 1024 * 1024 * 1024) {
        // 4-6 GB RAM (iPhone 11 Pro, iPhone 12, iPad Air)
        batchSize = 1500;
    } else if (physicalMemory >= 3ULL * 1024 * 1024 * 1024) {
        // 3-4 GB RAM (iPhone X, iPhone 11, older iPads)
        batchSize = 1000;
    } else if (physicalMemory >= 2ULL * 1024 * 1024 * 1024) {
        // 2-3 GB RAM (iPhone 8, iPhone SE 2)
        batchSize = 500;
    } else {
        // < 2 GB RAM (very old devices)
        batchSize = 250;
    }
    
    // Adjust for low processor count (older/slower devices)
    if (processorCount <= 2) {
        batchSize = batchSize / 2;
    }
    
    // Conservative cap to prevent blob spikes (memory pressure events come late)
    // Rows with large NSData blobs can spike memory before OS sends WARN/CRITICAL
    batchSize = MIN(batchSize, 500);
    
    os_log_info(self.logger,
                "Device: %.1f GB RAM, %ld cores → initial batch size: %ld",
                physicalMemory / (1024.0 * 1024.0 * 1024.0),
                (long)processorCount,
                (long)batchSize);
    
    return batchSize;
}

- (void)setupMemoryPressureMonitoring {
    // Monitor system memory pressure and adapt batch size accordingly
    dispatch_source_t source = dispatch_source_create(
                                                      DISPATCH_SOURCE_TYPE_MEMORYPRESSURE,
                                                      0,
                                                      DISPATCH_MEMORYPRESSURE_WARN | DISPATCH_MEMORYPRESSURE_CRITICAL,
                                                      self.workQueue
                                                      );
    
    if (!source) {
        os_log_error(self.logger, "Failed to create memory pressure source");
        return;
    }
    
    __weak __typeof__(self) weakSelf = self;
    dispatch_source_set_event_handler(source, ^{
        __typeof__(self) strongSelf = weakSelf;
        
        if (!strongSelf) return;
        
        unsigned long flags = dispatch_source_get_data(source);
        
        if (flags & DISPATCH_MEMORYPRESSURE_CRITICAL) {
            // Critical memory pressure - reduce batch size aggressively
            NSInteger newBatchSize = MAX(100, strongSelf.batchSize / 4);
            os_log_error(strongSelf.logger,
                         "CRITICAL memory pressure! Reducing batch size: %ld → %ld",
                         (long)strongSelf.batchSize, (long)newBatchSize);
            strongSelf.batchSize = newBatchSize;
            
        } else if (flags & DISPATCH_MEMORYPRESSURE_WARN) {
            // Warning - reduce batch size moderately
            NSInteger newBatchSize = MAX(250, strongSelf.batchSize / 2);
            os_log_error(strongSelf.logger,
                         "Memory pressure warning. Reducing batch size: %ld → %ld",
                         (long)strongSelf.batchSize, (long)newBatchSize);
            strongSelf.batchSize = newBatchSize;
        }
    });
    
    dispatch_source_set_cancel_handler(source, ^{
        // Cleanup
    });
    
    self.memoryPressureSource = source;
    dispatch_resume(source);
}

- (void)startWithURL:(NSURL *)url
          completion:(SliceDownloadCompletion)completion {
    // Store completion on work queue and initialize decoder there
    dispatch_async(self.workQueue, ^{
        // Store callbacks synchronously on work queue
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
        self.transactionStarted = NO;  // Reset transaction state
        self.currentTableName = nil; self.currentColumnsArray = nil;
        self.currentColumnsSignature = nil;
        self.dbFailed = NO;
        self.bytesDownloaded = 0;
        self.totalExpectedBytes = 0;  // Will be set from Content-Length header
        self.batchSize = self.initialBatchSize;  // Reset to initial value (may have been reduced by memory pressure)
        self.totalRowsInserted = 0;
        self.pendingRowCount = 0;  // Reset fast counter
        [self.pendingBatch removeAllObjects];
        
        // Store URL for download start after transaction begins
        NSURL *downloadURL = url;
        
        // Begin import transaction (on db.methodQueue)
        // Download will start only after BEGIN IMMEDIATE succeeds
        [self beginImportTransactionWithURL:downloadURL];
    });
}

#pragma mark - NSURLSessionDataDelegate

- (void)URLSession:(NSURLSession *)session
          dataTask:(NSURLSessionDataTask *)dataTask
didReceiveResponse:(NSURLResponse *)response
 completionHandler:(void (^)(NSURLSessionResponseDisposition))completionHandler {
    dispatch_async(self.workQueue, ^{
        // Get expected file size from Content-Length header
        if ([response isKindOfClass:[NSHTTPURLResponse class]]) {
            NSHTTPURLResponse *httpResponse = (NSHTTPURLResponse *)response;
            self.totalExpectedBytes = httpResponse.expectedContentLength;
            
            os_log_info(self.logger, "Starting download: %lld bytes expected", self.totalExpectedBytes);
        }
    });
    
    completionHandler(NSURLSessionResponseAllow);
}

- (void)URLSession:(NSURLSession *)session
          dataTask:(NSURLSessionDataTask *)dataTask
    didReceiveData:(NSData *)data {
    dispatch_async(self.workQueue, ^{
        // Early exit if DB failed
        if (self.dbFailed) {
            return;
        }
        
        self.totalBytesReceived += data.length;
        self.bytesDownloaded += data.length;
        
        if (!self.decoder) {
            os_log_error(self.logger, "Decoder not initialized");
            [self failWithError:@"Decoder not initialized"];
            return;
        }
        
        // Feed compressed data to decoder
        if (!self.decoder->feedCompressedData((const uint8_t *)data.bytes, data.length)) {
            os_log_error(self.logger, "Failed to decompress data: %{public}s",
                         self.decoder->getError().c_str());
            [self failWithError:[NSString stringWithFormat:@"Decompression failed: %s",
                                 self.decoder->getError().c_str()]];
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

- (void)failWithError:(NSString *)errorMessage {
    // Execute on workQueue (touches activeTask, completion, decoder)
    // If already on workQueue, execute inline (avoids queueing delay)
    void (^block)(void) = ^{
        // Guard: only fail once
        if (self.hasCalledCompletion) {
            return;
        }
        
        os_log_error(self.logger, "%{public}@", errorMessage);
        
        self.dbFailed = YES;
        self.didCancelTask = YES;
        [self.activeTask cancel];
        self.activeTask = nil;
        
        // If a transaction is active, roll it back before completing
        DatabaseBridge *strongDB = self.db;
        if (strongDB && self.transactionStarted) {
            os_log_info(self.logger, "Failure occurred with active transaction, rolling back");
            
            // Do NOT barrier-wait here - would deadlock if called from db.methodQueue
            // Safety: db.methodQueue is serial, so rollback won't race with in-flight work
            dispatch_async(strongDB.methodQueue, ^{
                sqlite3 *db = (sqlite3 *)[strongDB getRawConnectionWithConnectionTag:self.connectionTag];
                if (db) {
                    [self rollbackImportOnDB:db reason:errorMessage];
                }
                
                // Complete after rollback
                dispatch_async(self.workQueue, ^{
                    [self completeWithError:[NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                                                code:-1
                                                            userInfo:@{NSLocalizedDescriptionKey: errorMessage}]];
                });
            });
        } else {
            // No transaction, complete immediately
            [self completeWithError:[NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                                        code:-1
                                                    userInfo:@{NSLocalizedDescriptionKey: errorMessage}]];
        }
        
        // Clean up decoder
        if (self.decoder) {
            delete self.decoder;
            self.decoder = nullptr;
        }
    };
    
    // Check if already on workQueue
    if (dispatch_get_specific(kSliceImporterQueueKey)) {
        block();  // Already on workQueue, execute inline
    } else {
        dispatch_async(self.workQueue, block);  // Dispatch to workQueue
    }
}

- (void)completeWithError:(NSError * _Nullable)error {
    // Execute on workQueue (touches hasCalledCompletion, completion)
    // If already on workQueue, execute inline
    void (^block)(void) = ^{
        // Ensure we only call completion once
        if (self.hasCalledCompletion) {
            return;
        }
        
        self.hasCalledCompletion = YES;
        
        // Cleanup memory pressure monitoring
        if (self.memoryPressureSource) {
            dispatch_source_cancel(self.memoryPressureSource);
            self.memoryPressureSource = nil;
        }
        
        if (self.completion) {
            self.completion(error);
            self.completion = nil;
        }
    };
    
    // Check if already on workQueue
    if (dispatch_get_specific(kSliceImporterQueueKey)) {
        block();  // Already on workQueue, execute inline
    } else {
        dispatch_async(self.workQueue, block);  // Dispatch to workQueue
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
                                     self.decoder->getError().c_str()]];
                break;
                
            default:
                os_log_error(self.logger, "Unexpected parse status for slice header");
                [self failWithError:@"Unexpected parse status"];
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
            self.currentTableName = nil; self.currentColumnsArray = nil;
            self.currentColumnsSignature = nil;
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
                self.currentTableName = [NSString stringWithUTF8String:tableHeader.tableName.c_str()];  // Cache to avoid per-row conversion
                self.currentColumnsArray = [self convertColumnsToArray:tableHeader.columns];
                self.currentColumnsSignature = [self.currentColumnsArray componentsJoinedByString:@","];  // Cache signature
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
                    self.currentTableName = nil; self.currentColumnsArray = nil;
                    self.currentColumnsSignature = nil;
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
                                     self.decoder->getError().c_str()]];
                return;
                
            default:
                os_log_error(self.logger, "Unexpected parse status for table header");
                [self failWithError:@"Unexpected parse status"];
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
                    [self failWithError:@"Internal parser error: no progress"];
                    return ParseStatus::Error;
                }
                
                // Accumulate row into batch (improved structure: avoid per-row dicts)
                // Use cached tableName to avoid per-row UTF8 conversion
                NSMutableDictionary *tableBatch = self.pendingBatch[self.currentTableName];
                
                if (!tableBatch) {
                    // First row for this table in this batch
                    tableBatch = [@{
                        @"columns": self.currentColumnsArray,  // Cached array
                        @"columnsSignature": self.currentColumnsSignature,  // Cached signature
                        @"valuesRows": [NSMutableArray array]
                    } mutableCopy];
                    self.pendingBatch[self.currentTableName] = tableBatch;
                }
                
                NSArray *rowValues = [self convertRowToArray:row columns:tableHeader.columns];
                [tableBatch[@"valuesRows"] addObject:rowValues];
                
                rowCount++;
                self.pendingRowCount++;  // Fast O(1) counter
                
                // Flush batch if full (use fast counter, avoid O(#tables) scan)
                if (self.pendingRowCount >= self.batchSize) {
                    [self flushBatchToDatabase:task];
                    
                    // Check if DB failed during flush
                    if (self.dbFailed) {
                        return ParseStatus::Error;
                    }
                }
                
                if (rowCount % 1000 == 0) {
                    os_log_info(self.logger, "Parsed %lu rows from %{public}s",
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
                                     self.decoder->getError().c_str()]];
                return ParseStatus::Error;
                
            default:
                os_log_error(self.logger, "Unexpected parse status for row");
                [self failWithError:@"Unexpected parse status"];
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
            case FieldValue::Type::BLOB_VALUE: {
                // Reduce blob copies: use malloc + transfer ownership
                size_t blobSize = value.blobValue.size();
                if (blobSize > 0) {
                    void *buf = malloc(blobSize);
                    memcpy(buf, value.blobValue.data(), blobSize);
                    NSData *blobData = [NSData dataWithBytesNoCopy:buf
                                                            length:blobSize
                                                      freeWhenDone:YES];
                    [array addObject:blobData];
                } else {
                    [array addObject:[NSData data]];  // Empty blob
                }
                break;
            }
        }
    }
    
    return array;
}

- (BOOL)flushBatchToDatabase:(NSURLSessionTask *)task {
    if (self.pendingBatch.count == 0 || self.dbFailed) {
        return NO;  // No work dispatched
    }
    
    // Deep-freeze batch: copy valuesRows arrays to prevent cross-queue mutation
    NSMutableDictionary *frozen = [NSMutableDictionary dictionaryWithCapacity:self.pendingBatch.count];
    [self.pendingBatch enumerateKeysAndObjectsUsingBlock:^(NSString *tableName, NSDictionary *tableBatch, BOOL *stop) {
        frozen[tableName] = @{
            @"columns": tableBatch[@"columns"],
            @"columnsSignature": tableBatch[@"columnsSignature"],
            @"valuesRows": [tableBatch[@"valuesRows"] copy]  // Freeze mutable array to immutable
        };
    }];
    
    NSDictionary<NSString *, NSDictionary *> *batchCopy = [frozen copy];
    NSInteger batchRowCount = self.pendingRowCount;  // Capture count before reset
    
    [self.pendingBatch removeAllObjects];
    self.pendingRowCount = 0;  // Reset fast counter
    
    if (batchRowCount == 0) {
        return NO;  // No work dispatched
    }
    
    os_log_info(self.logger, "Flushing %ld rows across %lu tables",
                (long)batchRowCount, (unsigned long)batchCopy.count);
    
    // Backpressure: wait for available slot (inflight=1)
    dispatch_semaphore_wait(self.inflightSemaphore, DISPATCH_TIME_FOREVER);
    
    // Check if failed while waiting
    if (self.dbFailed) {
        dispatch_semaphore_signal(self.inflightSemaphore);
        return NO;  // No work dispatched
    }
    
    // Strong-capture db to ensure methodQueue is valid
    DatabaseBridge *strongDB = self.db;
    if (!strongDB) {
        dispatch_semaphore_signal(self.inflightSemaphore);
        self.dbFailed = YES;
        [self failWithError:@"DatabaseBridge deallocated"];
        return NO;  // No work dispatched
    }
    
    // All SQLite work happens on db.methodQueue
    dispatch_async(strongDB.methodQueue, ^{
        sqlite3 *db = (sqlite3 *)[strongDB getRawConnectionWithConnectionTag:self.connectionTag];
        
        if (!db) {
            os_log_error(self.logger, "Lost database connection");
            dispatch_semaphore_signal(self.inflightSemaphore);
            
            // failWithError handles dbFailed + workQueue dispatch
            [self failWithError:@"Lost database connection"];
            return;
        }
        
        // NO per-flush BEGIN/COMMIT - we have one long transaction!
        // Insert all rows using multi-row INSERT statements for speed
        // Use sorted table keys for deterministic ordering (FK safety if enabled)
        BOOL success = YES;
        NSArray<NSString *> *tableNames = [[batchCopy allKeys] sortedArrayUsingSelector:@selector(compare:)];
        
        for (NSString *tableName in tableNames) {
            NSDictionary *tableBatch = batchCopy[tableName];
            NSArray<NSString *> *columns = tableBatch[@"columns"];
            NSString *columnsSignature = tableBatch[@"columnsSignature"];
            NSArray<NSArray *> *valuesRows = tableBatch[@"valuesRows"];
            
            // Use multi-row inserts (chunk to respect SQLite param limit)
            if (![self insertRowsMulti:valuesRows
                             tableName:tableName
                               columns:columns
                      columnsSignature:columnsSignature
                                    db:db]) {
                success = NO;
                break;
            }
        }
        
        if (!success) {
            // Hard abort: rollback entire transaction using centralized helper
            [self rollbackImportOnDB:db reason:@"Database insert failed"];
            
            dispatch_semaphore_signal(self.inflightSemaphore);
            
            // failWithError handles dbFailed + workQueue dispatch
            [self failWithError:@"Database insert failed"];
            return;
        }
        
        // Update counters and check savepoint (per-flush, not per-row for speed)
        self.totalRowsInserted += batchRowCount;
        self.rowsSinceSavepoint += batchRowCount;
        
        // Handle multiple savepoint thresholds in one flush (if batchRowCount > 10k)
        // Best-effort: savepoint cycling is an optimization, not critical for correctness
        while (self.rowsSinceSavepoint >= 10000) {
            char *errMsg = nullptr;
            int rc = sqlite3_exec(db, "RELEASE SAVEPOINT sp;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                if (errMsg) {
                    os_log_debug(self.logger, "Savepoint release failed (non-fatal): %{public}s", errMsg);
                    sqlite3_free(errMsg);
                }
                errMsg = nullptr;
            } else {
                // Only recreate if release succeeded
                rc = sqlite3_exec(db, "SAVEPOINT sp;", nullptr, nullptr, &errMsg);
                if (rc != SQLITE_OK) {
                    if (errMsg) {
                        os_log_debug(self.logger, "Savepoint create failed (non-fatal): %{public}s", errMsg);
                        sqlite3_free(errMsg);
                    }
                } else {
                    os_log_info(self.logger, "Savepoint cycled at %ld rows", (long)self.totalRowsInserted);
                }
            }
            
            self.rowsSinceSavepoint -= 10000;  // Subtract, not modulo (handles multiple thresholds)
        }
        
        os_log_debug(self.logger, "Flushed %ld rows (total: %ld)", (long)batchRowCount, (long)self.totalRowsInserted);
        dispatch_semaphore_signal(self.inflightSemaphore);
    });
    
    return YES;  // Work was dispatched
}

#pragma mark - NSURLSessionTaskDelegate

- (void)URLSession:(NSURLSession *)session
              task:(NSURLSessionTask *)task
didCompleteWithError:(nullable NSError *)error {
    dispatch_async(self.workQueue, ^{
        // Clear active task reference (happens on both success and failure)
        self.activeTask = nil;
        
        if (error) {
            // Handle cancellation: skip if we already handled completion
            if (error.code == NSURLErrorCancelled) {
                if (self.didCancelTask || self.dbFailed || self.hasCalledCompletion) {
                    return;
                }
            }
            
            os_log_error(self.logger, "Network error: %{public}@", error);
            
            // Set failure flags immediately to block new work
            self.didCancelTask = YES;
            self.dbFailed = YES;
            
            // Always wait for in-flight DB work (inflight=1, so single wait is clean barrier)
            os_log_debug(self.logger, "Waiting for in-flight DB work before rollback");
            dispatch_semaphore_wait(self.inflightSemaphore, DISPATCH_TIME_FOREVER);
            dispatch_semaphore_signal(self.inflightSemaphore);  // Restore permit
            
            // Rollback transaction if we started one, then complete
            DatabaseBridge *strongDB = self.db;
            if (strongDB && self.transactionStarted) {
                dispatch_async(strongDB.methodQueue, ^{
                    sqlite3 *db = (sqlite3 *)[strongDB getRawConnectionWithConnectionTag:self.connectionTag];
                    if (db) {
                        // Use centralized rollback helper
                        [self rollbackImportOnDB:db reason:@"Network error"];
                    }
                    
                    // Complete after rollback finishes
                    dispatch_async(self.workQueue, ^{
                        [self completeWithError:error];
                    });
                });
            } else {
                // No transaction to rollback, complete immediately
                [self completeWithError:error];
            }
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
        BOOL didFlush = [self flushBatchToDatabase:task];
        
        // Wait for the flush to complete if we dispatched one (inflight=1, so single wait)
        if (didFlush) {
            os_log_debug(self.logger, "Waiting for final flush to complete");
            dispatch_semaphore_wait(self.inflightSemaphore, DISPATCH_TIME_FOREVER);
            dispatch_semaphore_signal(self.inflightSemaphore);  // Restore permit
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
        
        // Commit the import transaction
        [self commitImportTransaction:^(NSError *error) {
            if (error) {
                dispatch_async(self.workQueue, ^{
                    self.dbFailed = YES;
                    [self completeWithError:error];
                });
            } else {
                dispatch_async(self.workQueue, ^{
                    os_log_info(self.logger, "Import completed successfully");
                    [self completeWithError:nil];
                });
            }
        }];
    });
}

- (void)dealloc {
    if (self.memoryPressureSource) {
        dispatch_source_cancel(self.memoryPressureSource);
        self.memoryPressureSource = nil;
    }
    
    [self.session invalidateAndCancel];
    
    if (self.decoder) {
        delete self.decoder;
        self.decoder = nullptr;
    }
    
    // Note: Statement cache finalized in commitImportTransaction on db.methodQueue
    // Do NOT finalize here (wrong thread, potential race)
}

#pragma mark - Transaction Management (Called on db.methodQueue)

/**
 Centralized rollback + cleanup helper.
 MUST be called on db.methodQueue with a valid db handle.
 Safe to call multiple times or even if transaction was never started.
 */
- (void)rollbackImportOnDB:(sqlite3 *)db reason:(NSString *)reason {
    os_log_error(self.logger, "Rolling back import: %{public}@", reason);
    
    // Best-effort rollback (savepoint might not exist if BEGIN failed mid-way)
    // Ignore return codes - these are cleanup operations
    sqlite3_exec(db, "ROLLBACK TO SAVEPOINT sp;", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "RELEASE SAVEPOINT sp;", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
    
    // Finalize all cached statements
    for (NSValue *stmtValue in self.statementCache.allValues) {
        sqlite3_stmt *stmt = (sqlite3_stmt *)[stmtValue pointerValue];
        if (stmt) {
            sqlite3_finalize(stmt);
        }
    }
    [self.statementCache removeAllObjects];
    
    // Clear transaction flag
    self.transactionStarted = NO;
    
    os_log_info(self.logger, "Transaction rolled back and statements finalized");
}

- (void)beginImportTransactionWithURL:(NSURL *)url {
    DatabaseBridge *strongDB = self.db;
    if (!strongDB) {
        [self failWithError:@"DatabaseBridge deallocated before transaction start"];
        return;
    }
    
    dispatch_async(strongDB.methodQueue, ^{
        sqlite3 *db = (sqlite3 *)[strongDB getRawConnectionWithConnectionTag:self.connectionTag];
        if (!db) {
            [self failWithError:@"Lost database connection during transaction start"];
            return;
        }
        
        // Set fast import PRAGMAs
        sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);  // Ensure WAL mode (idempotent)
        sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);
        sqlite3_exec(db, "PRAGMA temp_store=MEMORY;", nullptr, nullptr, nullptr);
        sqlite3_exec(db, "PRAGMA cache_size=-20000;", nullptr, nullptr, nullptr);  // 20MB cache
        sqlite3_exec(db, "PRAGMA wal_autocheckpoint=10000;", nullptr, nullptr, nullptr);  // Large checkpoint threshold (default 1000)
        
        // Begin transaction
        char *errMsg = nullptr;
        int result = sqlite3_exec(db, "BEGIN IMMEDIATE;", nullptr, nullptr, &errMsg);
        if (result == SQLITE_OK) {
            // Create first savepoint
            sqlite3_exec(db, "SAVEPOINT sp;", nullptr, nullptr, nullptr);
            self.rowsSinceSavepoint = 0;
            self.transactionStarted = YES;  // Mark transaction as started
            os_log_info(self.logger, "Started import transaction with fast PRAGMAs");
            
            // NOW start the download (transaction is live)
            dispatch_async(dispatch_get_main_queue(), ^{
                // Guard: don't start if already failed/completed
                if (self.dbFailed || self.hasCalledCompletion) {
                    return;
                }
                
                self.activeTask = [self.session dataTaskWithURL:url];
                [self.activeTask resume];
                os_log_debug(self.logger, "Download started after transaction ready");
            });
        } else {
            if (errMsg) {
                os_log_error(self.logger, "Failed to begin import transaction: %{public}s", errMsg);
                sqlite3_free(errMsg);
            }
            // Transaction failed - abort import (failWithError handles workQueue dispatch)
            [self failWithError:@"Failed to begin import transaction"];
        }
    });
}

- (void)commitImportTransaction:(void (^)(NSError *error))completion {
    DatabaseBridge *strongDB = self.db;
    if (!strongDB) {
        completion([NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                       code:-1
                                   userInfo:@{NSLocalizedDescriptionKey: @"DB deallocated"}]);
        return;
    }
    
    dispatch_async(strongDB.methodQueue, ^{
        sqlite3 *db = (sqlite3 *)[strongDB getRawConnectionWithConnectionTag:self.connectionTag];
        if (!db) {
            completion([NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                           code:-1
                                       userInfo:@{NSLocalizedDescriptionKey: @"Lost connection"}]);
            return;
        }
        
        // Release final savepoint
        sqlite3_exec(db, "RELEASE SAVEPOINT sp;", nullptr, nullptr, nullptr);
        
        // Commit transaction
        char *errMsg = nullptr;
        int result = sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &errMsg);
        
        NSError *error = nil;
        if (result != SQLITE_OK) {
            NSString *errorString = errMsg ? [NSString stringWithUTF8String:errMsg] : @"Unknown error";
            if (errMsg) sqlite3_free(errMsg);
            os_log_error(self.logger, "Failed to commit import transaction: %{public}@", errorString);
            error = [NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                        code:result
                                    userInfo:@{NSLocalizedDescriptionKey: errorString}];
            
            // Use centralized rollback helper (handles savepoint + statement cleanup)
            [self rollbackImportOnDB:db reason:@"COMMIT failed"];
            
            completion(error);
            return;
        }
        
        os_log_info(self.logger, "Committed import transaction (%ld rows)", (long)self.totalRowsInserted);
        
        // Explicit WAL checkpoint after commit (truncate WAL for predictable finish time)
        // Use sqlite3_wal_checkpoint_v2 for structured result
        int logFrames = 0, ckptFrames = 0;
        int checkpointResult = sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_TRUNCATE, &logFrames, &ckptFrames);
        if (checkpointResult == SQLITE_OK) {
            os_log_info(self.logger, "WAL checkpoint completed: %d log frames, %d checkpointed", logFrames, ckptFrames);
        } else {
            os_log_error(self.logger, "WAL checkpoint failed: rc=%d (non-fatal)", checkpointResult);
        }
        
        // Clean up statement cache (MUST happen on db.methodQueue)
        for (NSValue *stmtValue in self.statementCache.allValues) {
            sqlite3_stmt *stmt = (sqlite3_stmt *)[stmtValue pointerValue];
            if (stmt) {
                sqlite3_finalize(stmt);
            }
        }
        [self.statementCache removeAllObjects];
        
        // Clear transaction flag
        self.transactionStarted = NO;
        
        // Restore normal PRAGMAs (restore to NORMAL, not FULL - better for WAL mode)
        sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);
        sqlite3_exec(db, "PRAGMA wal_autocheckpoint=1000;", nullptr, nullptr, nullptr);  // Restore auto-checkpoint
        
        completion(error);
    });
}

#pragma mark - Multi-Row Insert (Accessed only on db.methodQueue)

- (BOOL)insertRowsMulti:(NSArray<NSArray *> *)valuesRows
              tableName:(NSString *)tableName
                columns:(NSArray<NSString *> *)columns
       columnsSignature:(NSString *)columnsSignature
                     db:(sqlite3 *)db {
    if (valuesRows.count == 0) {
        return YES;  // Nothing to insert
    }
    
    NSInteger columnCount = columns.count;
    if (columnCount == 0) {
        return YES;  // No columns (shouldn't happen)
    }
    
    // Calculate max rows per statement (leave headroom under SQLite limit ~999)
    // Each row binds columnCount params; _status is a literal (not bound)
    NSInteger maxRowsPerStmt = (NSInteger)floor(900.0 / (double)columnCount);
    if (maxRowsPerStmt < 1) {
        maxRowsPerStmt = 1;  // At least one row
    }
    
    // Process rows in chunks
    NSInteger totalRows = valuesRows.count;
    NSInteger offset = 0;
    
    while (offset < totalRows) {
        NSInteger chunkSize = MIN(maxRowsPerStmt, totalRows - offset);
        NSArray<NSArray *> *chunk = [valuesRows subarrayWithRange:NSMakeRange(offset, chunkSize)];
        
        // Only cache full-size chunks; prepare one-off statements for partial chunks
        // (prevents statement cache pollution from varied partial chunk sizes)
        BOOL shouldCache = (chunkSize == maxRowsPerStmt);
        
        // Get or create statement for this chunk size
        sqlite3_stmt *stmt = [self getCachedMultiRowStatementForTable:tableName
                                                              columns:columns
                                                     columnsSignature:columnsSignature
                                                          rowsInChunk:chunkSize
                                                          shouldCache:shouldCache
                                                                   db:db];
        if (!stmt) {
            os_log_error(self.logger, "Failed to prepare multi-row statement for %@", tableName);
            return NO;
        }
        
        // Reset and bind for this chunk
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        
        // Bind all values: row0_col0, row0_col1, ..., row1_col0, row1_col1, ...
        NSInteger paramIndex = 1;
        for (NSArray *rowValues in chunk) {
            for (NSInteger colIdx = 0; colIdx < columnCount; colIdx++) {
                // Bounds check: handle short rows gracefully (schema mismatch safety)
                id value = (colIdx < rowValues.count) ? rowValues[colIdx] : [NSNull null];
                int bindResult = SQLITE_OK;
                
                if ([value isKindOfClass:[NSNull class]]) {
                    bindResult = sqlite3_bind_null(stmt, (int)paramIndex);
                } else if ([value isKindOfClass:[NSNumber class]]) {
                    NSNumber *num = (NSNumber *)value;
                    const char *objCType = [num objCType];
                    if (strcmp(objCType, @encode(double)) == 0 || strcmp(objCType, @encode(float)) == 0) {
                        bindResult = sqlite3_bind_double(stmt, (int)paramIndex, [num doubleValue]);
                    } else {
                        bindResult = sqlite3_bind_int64(stmt, (int)paramIndex, [num longLongValue]);
                    }
                } else if ([value isKindOfClass:[NSString class]]) {
                    bindResult = sqlite3_bind_text(stmt, (int)paramIndex, [(NSString *)value UTF8String], -1, SQLITE_TRANSIENT);
                } else if ([value isKindOfClass:[NSData class]]) {
                    NSData *data = (NSData *)value;
                    // SQLITE_TRANSIENT is safer default: SQLite copies blob data
                    bindResult = sqlite3_bind_blob(stmt, (int)paramIndex, [data bytes], (int)[data length], SQLITE_TRANSIENT);
                } else {
                    // Unsupported type - fail loudly to prevent silent bugs
                    os_log_error(self.logger, "Unsupported value type at %@[%ld]: %{public}@",
                                 tableName, (long)colIdx, NSStringFromClass([value class]));
                    if (!shouldCache && stmt) {
                        sqlite3_finalize(stmt);
                    }
                    return NO;
                }
                
                if (bindResult != SQLITE_OK) {
                    NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
                    os_log_error(self.logger, "Failed to bind parameter %ld for %@: %{public}@",
                                 (long)paramIndex, tableName, errorString);
                    if (!shouldCache && stmt) {
                        sqlite3_finalize(stmt);
                    }
                    return NO;
                }
                
                paramIndex++;
            }
        }
        
        // Execute multi-row insert
        int result = sqlite3_step(stmt);
        if (result != SQLITE_DONE) {
            NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
            os_log_error(self.logger, "Failed to execute multi-row insert for %@ (chunk size %ld): %{public}@",
                         tableName, (long)chunkSize, errorString);
            
            // Finalize uncached statement if needed
            if (!shouldCache && stmt) {
                sqlite3_finalize(stmt);
            }
            return NO;
        }
        
        // Finalize uncached partial chunk statements immediately (not in cache)
        if (!shouldCache && stmt) {
            sqlite3_finalize(stmt);
        }
        
        offset += chunkSize;
    }
    
    return YES;
}

#pragma mark - Statement Cache (Accessed only on db.methodQueue)

- (sqlite3_stmt *)getCachedMultiRowStatementForTable:(NSString *)tableName
                                             columns:(NSArray<NSString *> *)columns
                                    columnsSignature:(NSString *)columnsSignature
                                         rowsInChunk:(NSInteger)rowsInChunk
                                         shouldCache:(BOOL)shouldCache
                                                  db:(sqlite3 *)db {
    // Cache key: table + columns + chunk size
    NSString *cacheKey = [NSString stringWithFormat:@"%@|%@|%ld", tableName, columnsSignature, (long)rowsInChunk];
    
    // Check cache (only if we would cache this size)
    if (shouldCache) {
        NSValue *stmtValue = self.statementCache[cacheKey];
        if (stmtValue) {
            return (sqlite3_stmt *)[stmtValue pointerValue];
        }
    }
    
    // Build multi-row INSERT OR IGNORE statement
    // ASSUMPTION: All tables have _status column (Watermelon schema requirement)
    NSMutableString *columnNames = [NSMutableString string];
    for (NSInteger i = 0; i < columns.count; i++) {
        if (i > 0) [columnNames appendString:@", "];
        [columnNames appendFormat:@"\"%@\"", columns[i]];
    }
    
    // Build VALUES clause: (?,?,...,'synced'),(?,?,...,'synced'),...
    NSMutableString *valuesClause = [NSMutableString string];
    for (NSInteger rowIdx = 0; rowIdx < rowsInChunk; rowIdx++) {
        if (rowIdx > 0) [valuesClause appendString:@", "];
        
        [valuesClause appendString:@"("];
        for (NSInteger colIdx = 0; colIdx < columns.count; colIdx++) {
            if (colIdx > 0) [valuesClause appendString:@", "];
            [valuesClause appendString:@"?"];
        }
        [valuesClause appendString:@", 'synced')"];  // _status column literal
    }
    
    NSString *sql = [NSString stringWithFormat:@"INSERT OR IGNORE INTO \"%@\" (%@, \"_status\") VALUES %@",
                     tableName, columnNames, valuesClause];
    
    // Prepare statement
    sqlite3_stmt *stmt = nullptr;
    int result = sqlite3_prepare_v2(db, [sql UTF8String], -1, &stmt, nullptr);
    if (result != SQLITE_OK) {
        NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
        os_log_error(self.logger, "Failed to prepare multi-row statement for %@ (chunk=%ld): %{public}@",
                     tableName, (long)rowsInChunk, errorString);
        return nullptr;
    }
    
    // Cache it only if shouldCache (prevents cache pollution from partial chunks)
    if (shouldCache) {
        self.statementCache[cacheKey] = [NSValue valueWithPointer:stmt];
        os_log_debug(self.logger, "Cached multi-row prepared statement for: %@ (chunk=%ld)", tableName, (long)rowsInChunk);
    } else {
        // Partial chunk - will be finalized immediately after use
        os_log_debug(self.logger, "Prepared one-off multi-row statement for: %@ (partial chunk=%ld)", tableName, (long)rowsInChunk);
    }
    
    return stmt;
}

- (sqlite3_stmt *)getCachedStatementForTable:(NSString *)tableName
                                     columns:(NSArray<NSString *> *)columns
                            columnsSignature:(NSString *)columnsSignature
                                          db:(sqlite3 *)db {
    // Cache key includes table + columns signature (protects against schema changes)
    // Use pre-computed signature to avoid repeated string joining
    NSString *cacheKey = [NSString stringWithFormat:@"%@|%@", tableName, columnsSignature];
    
    // Check cache
    NSValue *stmtValue = self.statementCache[cacheKey];
    if (stmtValue) {
        return (sqlite3_stmt *)[stmtValue pointerValue];
    }
    
    // Build INSERT OR IGNORE statement
    // ASSUMPTION: All tables have _status column (Watermelon schema requirement)
    // If any table lacks _status, prepare will fail and trigger hard rollback
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
    
    NSString *sql = [NSString stringWithFormat:@"INSERT OR IGNORE INTO \"%@\" (%@, \"_status\") VALUES (%@, 'synced')",
                     tableName, columnNames, placeholders];
    
    // Prepare statement
    sqlite3_stmt *stmt = nullptr;
    int result = sqlite3_prepare_v2(db, [sql UTF8String], -1, &stmt, nullptr);
    if (result != SQLITE_OK) {
        NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
        os_log_error(self.logger, "Failed to prepare statement for %@: %{public}@", tableName, errorString);
        return nullptr;
    }
    
    // Cache it
    self.statementCache[cacheKey] = [NSValue valueWithPointer:stmt];
    os_log_debug(self.logger, "Cached prepared statement for: %@", cacheKey);
    
    return stmt;
}

- (BOOL)insertRowWithCachedStmt:(NSArray *)values
                      tableName:(NSString *)tableName
                        columns:(NSArray<NSString *> *)columns
               columnsSignature:(NSString *)columnsSignature
                             db:(sqlite3 *)db {
    sqlite3_stmt *stmt = [self getCachedStatementForTable:tableName
                                                  columns:columns
                                         columnsSignature:columnsSignature
                                                       db:db];
    if (!stmt) {
        return NO;
    }
    
    // Reset and clear bindings
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
    
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
            // SQLITE_TRANSIENT is safer default: SQLite copies blob data
            // Could use SQLITE_STATIC (no copy) since we call sqlite3_step synchronously
            // and NSData outlives the step, but TRANSIENT is safer for maintenance
            // (blobs are occasional/small, so copy cost is acceptable)
            bindResult = sqlite3_bind_blob(stmt, (int)(i + 1), [data bytes], (int)[data length], SQLITE_TRANSIENT);
        }
        
        if (bindResult != SQLITE_OK) {
            NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
            os_log_error(self.logger, "Failed to bind parameter %ld: %{public}@",
                         (long)(i + 1), errorString);
            return NO;
        }
    }
    
    // Execute
    int result = sqlite3_step(stmt);
    
    if (result != SQLITE_DONE) {
        NSString *errorString = [NSString stringWithUTF8String:sqlite3_errmsg(db)];
        os_log_error(self.logger, "Failed to execute for %@: %{public}@", tableName, errorString);
        return NO;
    }
    
    return YES;
}

@end
