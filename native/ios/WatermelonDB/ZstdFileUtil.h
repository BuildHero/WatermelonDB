#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

@interface ZstdFileUtil : NSObject

+ (void)decompressZstdWithSrc:(NSString *)src
                         dest:(NSString *)dest
                        error:(NSError **)error;

@end

NS_ASSUME_NONNULL_END
