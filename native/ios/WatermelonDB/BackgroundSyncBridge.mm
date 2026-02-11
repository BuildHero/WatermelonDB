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

    // Start sync (pull + push). The JS runtime is alive during background tasks,
    // so the existing pushChangesProvider callback works normally. If the OS
    // expires the task, cancelSync() invalidates in-flight operations and
    // remaining mutations flush on next foreground sync.
    engine->startWithCompletion("background_task",
        [completion](bool success, const std::string& errorMessage) {
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

+ (void)cancelSync {
    std::shared_ptr<watermelondb::SyncEngine> engine;
    {
        std::lock_guard<std::mutex> lock(sBridgeMutex);
        engine = sSyncEngine;
    }
    if (engine) {
        engine->cancelSync();
    }
}

@end
