#include "JSISwiftWrapperModule.h"
#include "JSIWrapperUtils.h"

#include <ReactCommon/TurboModuleUtils.h>

#import <React/RCTBridge+Private.h>
#import <React/RCTBridgeModule.h>

#import <WatermelonDB-Swift.h>
#import <SliceImporter.h>

namespace facebook::react {

static NSMutableSet<SliceImporter *> *activeSliceImporters() {
    static NSMutableSet<SliceImporter *> *set = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        set = [NSMutableSet set];
    });
    return set;
}

static void retainSliceImporter(SliceImporter *importer) {
    @synchronized (activeSliceImporters()) {
        [activeSliceImporters() addObject:importer];
    }
}

static void releaseSliceImporter(SliceImporter *importer) {
    @synchronized (activeSliceImporters()) {
        [activeSliceImporters() removeObject:importer];
    }
}

JSISwiftWrapperModule::JSISwiftWrapperModule(std::shared_ptr<CallInvoker> jsInvoker)
: NativeWatermelonDBModuleCxxSpec(std::move(jsInvoker)) {}

jsi::Array JSISwiftWrapperModule::query(jsi::Runtime &rt, double tag, jsi::String table, jsi::String query) {
    RCTBridge *bridge = [RCTBridge currentBridge];
    DatabaseBridge *db = [bridge moduleForClass: DatabaseBridge.class];
    
    const std::lock_guard<std::mutex> lock(mutex_);
    
    // Convert double tag to jsi::Value
    jsi::Value tagValue = jsi::Value(tag);
    
    jsi::Value result = watermelondb::query(db, rt, tagValue, table, query);
    
    return result.asObject(rt).asArray(rt);
}

jsi::Array JSISwiftWrapperModule::execSqlQuery(jsi::Runtime &rt, double tag, jsi::String sql, jsi::Array args) {
    RCTBridge *bridge = [RCTBridge currentBridge];
    DatabaseBridge *db = [bridge moduleForClass: DatabaseBridge.class];
    
    const std::lock_guard<std::mutex> lock(mutex_);
    
    // Convert double tag to jsi::Value
    jsi::Value tagValue = jsi::Value(tag);
    
    jsi::Value result = watermelondb::execSqlQuery(db, rt, tagValue, sql, args);
    
    return result.asObject(rt).asArray(rt);
}

jsi::Value JSISwiftWrapperModule::importRemoteSlice(
                                                    jsi::Runtime &rt,
                                                    double tag,
                                                    jsi::String sliceUrl
                                                    ) {
    const double tagCopy = tag;
    const std::string sliceUrlUtf8 = sliceUrl.utf8(rt);
    
    RCTBridge *bridge = [RCTBridge currentBridge];
    DatabaseBridge *db = [bridge moduleForClass: DatabaseBridge.class];
    
    auto jsInvoker = jsInvoker_;
    
    return createPromiseAsJSIValue(rt, [db, tagCopy, sliceUrlUtf8, jsInvoker](jsi::Runtime &rt2, std::shared_ptr<Promise> promise) {
        
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
            @autoreleasepool {
                auto tagNumber = [[NSNumber alloc] initWithDouble:tagCopy];
                
                SliceImporter *importer = [[SliceImporter alloc] initWithDatabaseBridge:db connectionTag:tagNumber];
                retainSliceImporter(importer);
                
                [importer startWithURL:[NSURL URLWithString:[NSString stringWithUTF8String:sliceUrlUtf8.c_str()]]
                            completion:^(NSError * _Nullable error) {
                    releaseSliceImporter(importer);
                    jsInvoker->invokeAsync([promise, error]() mutable {
                        if (error) {
                            promise->reject([[error localizedDescription] UTF8String]);
                        } else {
                            promise->resolve(jsi::Value::undefined());
                        }
                    });
                }];
            }
        });
    }
                                   );
}

} // namespace facebook::react
