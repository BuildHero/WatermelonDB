#include "JSISwiftWrapperModule.h"
#include "JSIWrapperUtils.h"
#include "JsonUtils.h"

#include <ReactCommon/TurboModuleUtils.h>

#import <React/RCTBridge+Private.h>
#import <React/RCTBridgeModule.h>

#import <WatermelonDB-Swift.h>
#import <SliceImporter.h>
#import <sqlite3.h>
#include "SyncApplyEngine.h"

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
: NativeWatermelonDBModuleCxxSpec(std::move(jsInvoker)) {
    syncEventState_ = std::make_shared<SyncEventState>();
    syncEventState_->jsInvoker = jsInvoker_;
    syncEngine_ = std::make_shared<watermelondb::SyncEngine>();
    syncEngine_->setEventCallback([this](const std::string &eventJson) {
        emitSyncEventLocked(eventJson);
    });
    syncEngine_->setApplyCallback([this](const std::string &payload, std::string &errorMessage) {
        @autoreleasepool {
            RCTBridge *bridge = [RCTBridge currentBridge];
            DatabaseBridge *db = [bridge moduleForClass: DatabaseBridge.class];
            if (!db) {
                errorMessage = "DatabaseBridge not available";
                return false;
            }
            if (syncConnectionTag_ <= 0) {
                errorMessage = "Missing connectionTag in sync config";
                return false;
            }
            NSNumber *tagNumber = @(syncConnectionTag_);
            sqlite3 *sqlite = (sqlite3 *)[db getRawConnectionWithConnectionTag:tagNumber];
            if (!sqlite) {
                errorMessage = "Failed to get SQLite connection";
                return false;
            }
            return watermelondb::applySyncPayload(sqlite, payload, errorMessage);
        }
    });

    socketStatusObserver_ = [[NSNotificationCenter defaultCenter]
        addObserverForName:SyncSocketClient.statusNotificationName
                    object:nil
                     queue:nil
                usingBlock:^(NSNotification *note) {
        NSString *status = note.userInfo[@"status"];
        NSString *error = note.userInfo[@"error"];
        std::string statusStr = status ? [status UTF8String] : "";
        std::string errorStr = (error && ![error isKindOfClass:[NSNull class]]) ? [error UTF8String] : "";

        std::string eventJson = std::string("{\"status\":\"") +
                                watermelondb::json_utils::escapeJsonString(statusStr) +
                                "\"";
        if (!errorStr.empty()) {
            eventJson += std::string(",\"data\":\"") + watermelondb::json_utils::escapeJsonString(errorStr) + "\"";
        }
        eventJson += "}";

        auto state = syncEventState_;
        if (!state) {
            return;
        }
        const std::lock_guard<std::mutex> lock(state->mutex);
        emitSyncEventLocked(eventJson);
    }];

    socketCdcObserver_ = [[NSNotificationCenter defaultCenter]
        addObserverForName:SyncSocketClient.cdcNotificationName
                    object:nil
                     queue:nil
                usingBlock:^(NSNotification *note) {
        auto state = syncEventState_;
        if (!state) {
            return;
        }
        const std::lock_guard<std::mutex> lock(state->mutex);
        emitSyncEventLocked("{\"status\":\"cdc\"}");
    }];
}

JSISwiftWrapperModule::~JSISwiftWrapperModule() {
    if (syncEngine_) {
        syncEngine_->shutdown();
    }
    if (socketStatusObserver_) {
        [[NSNotificationCenter defaultCenter] removeObserver:(__bridge id)socketStatusObserver_];
        socketStatusObserver_ = nullptr;
    }
    if (socketCdcObserver_) {
        [[NSNotificationCenter defaultCenter] removeObserver:(__bridge id)socketCdcObserver_];
        socketCdcObserver_ = nullptr;
    }
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->alive = false;
        state->runtime = nullptr;
        state->listeners.clear();
    }
}

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

void JSISwiftWrapperModule::configureSync(jsi::Runtime &rt, jsi::String configJson) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    @autoreleasepool {
        std::string config = configJson.utf8(rt);
        NSData *data = [NSData dataWithBytes:config.data() length:config.size()];
        if (data) {
            NSError *err = nil;
            id json = [NSJSONSerialization JSONObjectWithData:data options:0 error:&err];
            if (!err && [json isKindOfClass:[NSDictionary class]]) {
                id tagValue = [(NSDictionary *)json objectForKey:@"connectionTag"];
                if ([tagValue isKindOfClass:[NSNumber class]]) {
                    syncConnectionTag_ = [(NSNumber *)tagValue longLongValue];
                }
            }
        }
    }
    if (syncEngine_) {
        syncEngine_->configure(configJson.utf8(rt));
    }
}

void JSISwiftWrapperModule::startSync(jsi::Runtime &rt, jsi::String reason) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        syncEngine_->start(reason.utf8(rt));
    }
}

jsi::String JSISwiftWrapperModule::getSyncStateJson(jsi::Runtime &rt) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        return jsi::String::createFromUtf8(rt, syncEngine_->stateJson());
    }
    return jsi::String::createFromUtf8(rt, "{\"state\":\"idle\"}");
}

double JSISwiftWrapperModule::addSyncListener(jsi::Runtime &rt, jsi::Function listener) {
    auto state = syncEventState_;
    if (!state) {
        return 0;
    }
    const std::lock_guard<std::mutex> lock(state->mutex);
    state->runtime = &rt;
    const int64_t id = nextSyncListenerId_++;
    state->listeners.emplace(id, std::move(listener));
    return static_cast<double>(id);
}

void JSISwiftWrapperModule::removeSyncListener(jsi::Runtime &rt, double listenerId) {
    auto state = syncEventState_;
    if (!state) {
        return;
    }
    const std::lock_guard<std::mutex> lock(state->mutex);
    state->runtime = &rt;
    state->listeners.erase(static_cast<int64_t>(listenerId));
}

void JSISwiftWrapperModule::notifyQueueDrained(jsi::Runtime &rt) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        syncEngine_->notifyQueueDrained();
    }
}

void JSISwiftWrapperModule::setAuthToken(jsi::Runtime &rt, jsi::String token) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        syncEngine_->setAuthToken(token.utf8(rt));
    }
}

void JSISwiftWrapperModule::clearAuthToken(jsi::Runtime &rt) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        syncEngine_->clearAuthToken();
    }
}

void JSISwiftWrapperModule::initSyncSocket(jsi::Runtime &rt, jsi::String socketUrl) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    NSString *url = [NSString stringWithUTF8String:socketUrl.utf8(rt).c_str()];
#if DEBUG
    [SyncSocketClient.shared initializeWithSocketUrl:url debug:YES];
#else
    [SyncSocketClient.shared initializeWithSocketUrl:url debug:NO];
#endif
    [SyncSocketClient.shared connect];
}

void JSISwiftWrapperModule::syncSocketAuthenticate(jsi::Runtime &rt, jsi::String token) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    NSString *tokenStr = [NSString stringWithUTF8String:token.utf8(rt).c_str()];
    [SyncSocketClient.shared authenticate:tokenStr];
}

void JSISwiftWrapperModule::syncSocketDisconnect(jsi::Runtime &rt) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    [SyncSocketClient.shared disconnect];
}

void JSISwiftWrapperModule::emitSyncEventLocked(const std::string &eventJson) {
    auto state = syncEventState_;
    if (!state || !state->jsInvoker) {
        return;
    }
    auto jsInvoker = state->jsInvoker;
    jsInvoker->invokeAsync([state, eventJson]() {
        const std::lock_guard<std::mutex> lock(state->mutex);
        if (!state->alive || !state->runtime || state->listeners.empty()) {
            return;
        }
        jsi::Runtime &rt = *state->runtime;
        for (auto &entry : state->listeners) {
            entry.second.call(rt, jsi::String::createFromUtf8(rt, eventJson));
        }
    });
}

} // namespace facebook::react
