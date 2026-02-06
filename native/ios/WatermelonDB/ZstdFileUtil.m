#import "ZstdFileUtil.h"
#import <libzstd/zstd.h>

static const size_t kBufferSize = 1024 * 1024;

@implementation ZstdFileUtil

+ (void)decompressZstdWithSrc:(NSString *)src
                         dest:(NSString *)dest
                        error:(NSError **)error {

    if (![[NSFileManager defaultManager] fileExistsAtPath:src]) {
        if (error) {
            *error = [NSError errorWithDomain:@"ZstdFileUtil"
                                         code:1
                                     userInfo:@{NSLocalizedDescriptionKey: @"Source file not found"}];
        }
        return;
    }

    NSInputStream *inputStream = [NSInputStream inputStreamWithFileAtPath:src];
    [inputStream open];

    if ([[NSFileManager defaultManager] fileExistsAtPath:dest]) {
        [[NSFileManager defaultManager] removeItemAtPath:dest error:nil];
    }

    NSOutputStream *outputStream = [NSOutputStream outputStreamToFileAtPath:dest append:NO];
    [outputStream open];

    ZSTD_DCtx *dctx = ZSTD_createDCtx();
    if (!dctx) {
        if (error) {
            *error = [NSError errorWithDomain:@"ZstdFileUtil"
                                         code:2
                                     userInfo:@{NSLocalizedDescriptionKey: @"Failed to create decompression context"}];
        }
        [inputStream close];
        [outputStream close];
        return;
    }

    uint8_t *inputBuffer = malloc(kBufferSize);
    uint8_t *outputBuffer = malloc(kBufferSize);

    NSInteger inputSize = [inputStream read:inputBuffer maxLength:kBufferSize];

    while (inputSize > 0) {
        ZSTD_inBuffer input = { inputBuffer, (size_t)inputSize, 0 };

        while (input.pos < input.size) {
            ZSTD_outBuffer output = { outputBuffer, kBufferSize, 0 };

            size_t result = ZSTD_decompressStream(dctx, &output, &input);

            if (ZSTD_isError(result)) {
                if (error) {
                    NSString *errorName = [NSString stringWithUTF8String:ZSTD_getErrorName(result)];
                    *error = [NSError errorWithDomain:@"ZstdFileUtil"
                                                 code:3
                                             userInfo:@{NSLocalizedDescriptionKey: [NSString stringWithFormat:@"Decompression failed: %@", errorName]}];
                }
                free(inputBuffer);
                free(outputBuffer);
                ZSTD_freeDCtx(dctx);
                [inputStream close];
                [outputStream close];
                return;
            }

            if (output.pos > 0) {
                NSInteger bytesWritten = [outputStream write:outputBuffer maxLength:output.pos];
                if (bytesWritten < 0) {
                    if (error) {
                        *error = [NSError errorWithDomain:@"ZstdFileUtil"
                                                     code:4
                                                 userInfo:@{NSLocalizedDescriptionKey: @"Failed to write decompressed data"}];
                    }
                    free(inputBuffer);
                    free(outputBuffer);
                    ZSTD_freeDCtx(dctx);
                    [inputStream close];
                    [outputStream close];
                    return;
                }
            }
        }

        inputSize = [inputStream read:inputBuffer maxLength:kBufferSize];
    }

    free(inputBuffer);
    free(outputBuffer);
    ZSTD_freeDCtx(dctx);
    [inputStream close];
    [outputStream close];
}

@end
