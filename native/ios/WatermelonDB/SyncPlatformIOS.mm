#include "SyncPlatform.h"

#import <Foundation/Foundation.h>

namespace watermelondb {
namespace platform {

void httpRequest(const HttpRequest& request,
                 std::function<void(const HttpResponse&)> onComplete) {
    @autoreleasepool {
        NSURL *url = [NSURL URLWithString:[NSString stringWithUTF8String:request.url.c_str()]];
        if (!url) {
            HttpResponse resp;
            resp.errorMessage = "Invalid URL";
            onComplete(resp);
            return;
        }

        NSMutableURLRequest *urlRequest = [NSMutableURLRequest requestWithURL:url];
        urlRequest.HTTPMethod = [NSString stringWithUTF8String:request.method.c_str()];
        urlRequest.timeoutInterval = request.timeoutMs / 1000.0;

        for (const auto &entry : request.headers) {
            NSString *key = [NSString stringWithUTF8String:entry.first.c_str()];
            NSString *value = [NSString stringWithUTF8String:entry.second.c_str()];
            [urlRequest setValue:value forHTTPHeaderField:key];
        }

        if (!request.body.empty()) {
            NSData *body = [NSData dataWithBytes:request.body.data() length:request.body.size()];
            urlRequest.HTTPBody = body;
        }

        NSURLSessionConfiguration *config = [NSURLSessionConfiguration defaultSessionConfiguration];
        NSURLSession *session = [NSURLSession sessionWithConfiguration:config];

        NSURLSessionDataTask *task = [session dataTaskWithRequest:urlRequest
                                                completionHandler:^(NSData *data, NSURLResponse *response, NSError *error) {
            HttpResponse resp;
            if (error) {
                resp.errorMessage = [[error localizedDescription] UTF8String];
                onComplete(resp);
                return;
            }

            NSHTTPURLResponse *httpResponse = (NSHTTPURLResponse *)response;
            resp.statusCode = (int)httpResponse.statusCode;
            if (data) {
                NSString *body = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
                if (body) {
                    resp.body = [body UTF8String];
                }
            }
            onComplete(resp);
        }];
        [task resume];
    }
}

} // namespace platform
} // namespace watermelondb
