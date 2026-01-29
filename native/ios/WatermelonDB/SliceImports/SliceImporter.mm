#import "SliceImporter.h"

#include "SliceImportEngine.h"
#include "SlicePlatform.h"

#import "../SliceImportDatabaseAdapter.h"

// This is required for importing WatermelonDB-Swift.h
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
@property (nonatomic, copy, nullable) SliceDownloadCompletion completion;
@end

@implementation SliceImporter {
    std::shared_ptr<DatabaseInterface> _dbInterface;
    std::shared_ptr<SliceImportEngine> _engine;
    BOOL _hasCompleted;
}

- (instancetype)initWithDatabaseBridge:(DatabaseBridge *)db connectionTag:(NSNumber *)tag {
    self = [super init];
    if (self) {
        _db = db;
        _connectionTag = tag;
        _hasCompleted = NO;
    }
    return self;
}

- (void)dealloc {
    if (_engine && _engine->isImporting()) {
        _engine->cancel();
    }
    _engine.reset();
    _dbInterface.reset();
}

- (void)startWithURL:(NSURL *)url
          completion:(SliceDownloadCompletion)completion {
    if (!url) {
        if (completion) {
            completion([NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                           code:-1
                                       userInfo:@{NSLocalizedDescriptionKey: @"Invalid URL"}]);
        }
        return;
    }
    
    self.completion = completion;
    _hasCompleted = NO;
    
    _dbInterface = createIOSDatabaseInterface(self.db, self.connectionTag);
    _engine = std::make_shared<SliceImportEngine>(_dbInterface);
    
    const char *urlCString = url.absoluteString.UTF8String;
    std::string urlString = urlCString ? urlCString : "";
    if (urlString.empty()) {
        [self completeWithErrorMessage:@"Invalid URL"];
        return;
    }
    
    __weak __typeof__(self) weakSelf = self;
    _engine->startImport(urlString, [weakSelf](const std::string &errorMessage) {
        __typeof__(self) strongSelf = weakSelf;
        if (!strongSelf) {
            return;
        }
        if (errorMessage.empty()) {
            [strongSelf completeWithError:nil];
        } else {
            NSString *message = [NSString stringWithUTF8String:errorMessage.c_str()];
            [strongSelf completeWithError:[NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                                              code:-1
                                                          userInfo:@{NSLocalizedDescriptionKey: message ?: @"Import failed"}]];
        }
    });
}

- (void)completeWithErrorMessage:(NSString *)message {
    NSError *error = [NSError errorWithDomain:@"com.buildops.watermelon.slice"
                                         code:-1
                                     userInfo:@{NSLocalizedDescriptionKey: message ?: @"Import failed"}];
    [self completeWithError:error];
}

- (void)completeWithError:(NSError *)error {
    if (_hasCompleted) {
        return;
    }
    _hasCompleted = YES;
    
    SliceDownloadCompletion completion = self.completion;
    self.completion = nil;
    
    _engine.reset();
    _dbInterface.reset();
    
    if (completion) {
        completion(error);
    }
}

@end
