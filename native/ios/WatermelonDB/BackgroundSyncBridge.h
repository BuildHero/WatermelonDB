#pragma once

#import <Foundation/Foundation.h>

/// ObjC++ bridge between Swift (WatermelonDBBackgroundSync) and C++ (SyncEngine).
/// Swift cannot call C++ directly, so this class mediates.
@interface BackgroundSyncBridge : NSObject

/// Store a reference to the C++ SyncEngine for background execution.
/// Called from JSISwiftWrapperModule::configureBackgroundSync.
+ (void)configureSyncEnginePtr:(void *)enginePtr;

/// Execute a pull-only background sync.
/// Completion is called on an arbitrary thread with (success, errorMessage).
+ (void)performSync:(void (^)(BOOL success, NSString * _Nullable errorMessage))completion;

@end
