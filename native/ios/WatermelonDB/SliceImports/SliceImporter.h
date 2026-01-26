#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

@class DatabaseBridge;

typedef void (^SliceDownloadCompletion)(NSError * _Nullable error);

@interface SliceImporter : NSObject <NSURLSessionDataDelegate>

- (instancetype)initWithDatabaseBridge:(DatabaseBridge *) db connectionTag:(NSNumber *)tag;

- (void)startWithURL:(NSURL *)url
          completion:(SliceDownloadCompletion)completion;

@end

NS_ASSUME_NONNULL_END
