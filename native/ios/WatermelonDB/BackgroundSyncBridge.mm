#import "BackgroundSyncBridge.h"
#include "SyncEngine.h"
#include <memory>
#include <mutex>
#include <string>

#import <UIKit/UIKit.h>

// Static state
static std::mutex sBridgeMutex;
static std::shared_ptr<watermelondb::SyncEngine> sSyncEngine;

@implementation BackgroundSyncBridge

+ (void)configureSyncEnginePtr:(void *)enginePtr {
    {
        std::lock_guard<std::mutex> lock(sBridgeMutex);
        // The enginePtr is a raw pointer to a shared_ptr<SyncEngine> stored by JSISwiftWrapperModule.
        // We reconstruct the shared_ptr from the raw pointer.
        // NOTE: The caller must ensure the shared_ptr outlives this reference.
        if (enginePtr) {
            auto *sharedPtr = static_cast<std::shared_ptr<watermelondb::SyncEngine> *>(enginePtr);
            sSyncEngine = *sharedPtr;
        } else {
            sSyncEngine = nullptr;
        }
    }

    // Register a foreground observer (once) to cancel any in-flight background sync
    // the moment the app returns to foreground — before the JS runtime processes events.
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        [[NSNotificationCenter defaultCenter]
            addObserverForName:UIApplicationWillEnterForegroundNotification
                        object:nil
                         queue:[NSOperationQueue mainQueue]
                    usingBlock:^(NSNotification * _Nonnull note) {
            std::shared_ptr<watermelondb::SyncEngine> engine;
            {
                std::lock_guard<std::mutex> lock(sBridgeMutex);
                engine = sSyncEngine;
            }
            if (engine) {
                NSLog(@"[WatermelonDB][BackgroundSync] App entering foreground — cancelling background sync");
                engine->cancelSync();
            }
        }];
    });
}

+ (void)performSync:(void (^)(BOOL success, NSString * _Nullable errorMessage))completion {
    std::shared_ptr<watermelondb::SyncEngine> engine;
    {
        std::lock_guard<std::mutex> lock(sBridgeMutex);
        engine = sSyncEngine;
    }

    if (!engine) {
        NSLog(@"[WatermelonDB][BackgroundSync] No sync engine configured");
        if (completion) {
            completion(NO, @"No sync engine configured");
        }
        return;
    }

    // Save the existing push callback and set no-op for pull-only background sync
    auto savedPushCallback = engine->getPushChangesCallback();
    engine->setPushChangesCallback([](std::function<void(bool, const std::string&)> pushCompletion) {
        // No-op: skip push in background, just signal success
        if (pushCompletion) {
            pushCompletion(true, "");
        }
    });

    // Start pull-only sync. The SyncEngine will use its normal auth flow:
    // if authToken_ is empty it calls authTokenRequestCallback_ which reaches
    // the JS auth provider (works when JS runtime is still alive in background).
    engine->startWithCompletion("background_task",
        [completion, engine, savedPushCallback](bool success, const std::string& errorMessage) {
            // Restore the original push callback so foreground sync can push
            if (savedPushCallback) {
                engine->setPushChangesCallback(savedPushCallback);
            }
            NSString *error = nil;
            if (!errorMessage.empty()) {
                error = [NSString stringWithUTF8String:errorMessage.c_str()];
            }
            if (completion) {
                completion(success ? YES : NO, error);
            }
        }
    );
}

@end
