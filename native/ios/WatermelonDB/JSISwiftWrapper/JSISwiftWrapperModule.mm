#include "JSISwiftWrapperModule.h"
#include "JSIWrapperUtils.h"
#include "JsonUtils.h"

#include <ReactCommon/TurboModuleUtils.h>

#import <Foundation/Foundation.h>
#import <React/RCTBridge+Private.h>
#import <React/RCTBridgeModule.h>

#import <WatermelonDB-Swift.h>
#import <SliceImporter.h>
#import <sqlite3.h>
#include "SyncApplyEngine.h"

#include <exception>

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
    syncEngine_->setAuthTokenRequestCallback([this]() {
        requestAuthTokenFromJs();
    });
    syncEngine_->setPushChangesCallback([this](std::function<void(bool success, const std::string& errorMessage)> completion) {
        requestPushChangesFromJs(std::move(completion));
    });

    socketStatusObserver_ = (__bridge_retained void*)[[NSNotificationCenter defaultCenter]
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

    socketCdcObserver_ = (__bridge_retained void*)[[NSNotificationCenter defaultCenter]
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
        CFRelease(socketStatusObserver_);
        socketStatusObserver_ = nullptr;
    }
    if (socketCdcObserver_) {
        [[NSNotificationCenter defaultCenter] removeObserver:(__bridge id)socketCdcObserver_];
        CFRelease(socketCdcObserver_);
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

void JSISwiftWrapperModule::setSyncPullUrl(jsi::Runtime &rt, jsi::String pullEndpointUrl) {
    if (syncEngine_) {
        syncEngine_->setPullEndpointUrl(pullEndpointUrl.utf8(rt));
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

void JSISwiftWrapperModule::setAuthTokenProvider(jsi::Runtime &rt, jsi::Function provider) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
        authTokenProvider_ = std::make_shared<jsi::Function>(std::move(provider));
    }
    requestAuthTokenFromJs();
}

void JSISwiftWrapperModule::setPushChangesProvider(jsi::Runtime &rt, jsi::Function provider) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
        pushChangesProvider_ = std::make_shared<jsi::Function>(std::move(provider));
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

void JSISwiftWrapperModule::requestAuthTokenFromJs() {
    auto state = syncEventState_;
    if (!state || !state->jsInvoker) {
        return;
    }
    auto engineWeak = std::weak_ptr<watermelondb::SyncEngine>(syncEngine_);
    state->jsInvoker->invokeAsync([state, engineWeak, this]() {
        jsi::Runtime* runtime = nullptr;
        std::shared_ptr<jsi::Function> provider;
        {
            const std::lock_guard<std::mutex> lock(state->mutex);
            if (!state->alive || !state->runtime) {
                return;
            }
            runtime = state->runtime;
            provider = authTokenProvider_;
        }
        if (!runtime || !provider) {
            return;
        }
        jsi::Runtime& rt = *runtime;
        jsi::Value result;
        try {
            result = provider->call(rt);
        } catch (...) {
            if (auto engine = engineWeak.lock()) {
                engine->clearAuthToken();
            }
            return;
        }
        bool usedPromiseResolve = false;
        jsi::Value promiseValue = jsi::Value::undefined();
        if (rt.global().hasProperty(rt, "Promise")) {
            jsi::Value promiseCtorValue = rt.global().getProperty(rt, "Promise");
            if (promiseCtorValue.isObject()) {
                jsi::Object promiseCtor = promiseCtorValue.asObject(rt);
                jsi::Value resolveValue = promiseCtor.getProperty(rt, "resolve");
                if (resolveValue.isObject() && resolveValue.asObject(rt).isFunction(rt)) {
                    jsi::Function resolveFunc = resolveValue.asObject(rt).asFunction(rt);
                    promiseValue = resolveFunc.call(rt, promiseCtor, result);
                    usedPromiseResolve = true;
                }
            }
        }
        if (!usedPromiseResolve && result.isObject()) {
            // jsi::Value is move-only; keep object/thenable alive for inspection below
            promiseValue = std::move(result);
        }

        if (promiseValue.isObject()) {
            jsi::Object promiseObj = promiseValue.asObject(rt);
            jsi::Value thenValue = promiseObj.getProperty(rt, "then");
            if (thenValue.isObject() && thenValue.asObject(rt).isFunction(rt)) {
                jsi::Function thenFunc = thenValue.asObject(rt).asFunction(rt);
                auto resolve = jsi::Function::createFromHostFunction(
                    rt,
                    jsi::PropNameID::forUtf8(rt, "resolve"),
                    1,
                    [engineWeak](jsi::Runtime& rt2, const jsi::Value&, const jsi::Value* args, size_t count) -> jsi::Value {
                        if (count > 0 && args[0].isString()) {
                            if (auto engine = engineWeak.lock()) {
                                engine->setAuthToken(args[0].asString(rt2).utf8(rt2));
                            }
                            return jsi::Value::undefined();
                        }
                        if (auto engine = engineWeak.lock()) {
                            engine->clearAuthToken();
                        }
                        return jsi::Value::undefined();
                    });
                auto reject = jsi::Function::createFromHostFunction(
                    rt,
                    jsi::PropNameID::forUtf8(rt, "reject"),
                    1,
                    [engineWeak](jsi::Runtime&, const jsi::Value&, const jsi::Value*, size_t) -> jsi::Value {
                        if (auto engine = engineWeak.lock()) {
                            engine->clearAuthToken();
                        }
                        return jsi::Value::undefined();
                    });
                bool handled = false;
                jsi::Value functionCtorValue = rt.global().getProperty(rt, "Function");
                if (functionCtorValue.isObject() && functionCtorValue.asObject(rt).isFunction(rt)) {
                    jsi::Function functionCtor = functionCtorValue.asObject(rt).asFunction(rt);
                    jsi::Value helperValue = functionCtor.call(
                        rt,
                        jsi::String::createFromUtf8(rt, "p"),
                        jsi::String::createFromUtf8(rt, "r"),
                        jsi::String::createFromUtf8(rt, "j"),
                        jsi::String::createFromUtf8(rt, "return p.then(r,j);")
                    );
                    if (helperValue.isObject() && helperValue.asObject(rt).isFunction(rt)) {
                        jsi::Function helper = helperValue.asObject(rt).asFunction(rt);
                        helper.call(rt, promiseObj, resolve, reject);
                        handled = true;
                    }
                }
                if (!handled) {
                    thenFunc.call(rt, promiseObj, resolve, reject);
                }
                return;
            }
        }

        if (!usedPromiseResolve && result.isString()) {
            if (auto engine = engineWeak.lock()) {
                engine->setAuthToken(result.asString(rt).utf8(rt));
            }
            return;
        }

        if (auto engine = engineWeak.lock()) {
            engine->clearAuthToken();
        }
    });
}

void JSISwiftWrapperModule::requestPushChangesFromJs(
    std::function<void(bool success, const std::string& errorMessage)> completion) {
    auto state = syncEventState_;
    if (!state || !state->jsInvoker) {
        completion(false, "Missing JS runtime for pushChanges");
        return;
    }
    auto completionPtr = std::make_shared<std::function<void(bool, const std::string&)>>(std::move(completion));
    state->jsInvoker->invokeAsync([state, completionPtr, this]() {
        jsi::Runtime* runtime = nullptr;
        std::shared_ptr<jsi::Function> provider;
        {
            const std::lock_guard<std::mutex> lock(state->mutex);
            if (!state->alive || !state->runtime) {
                return;
            }
            runtime = state->runtime;
            provider = pushChangesProvider_;
        }
        if (!runtime || !provider) {
            (*completionPtr)(false, "Missing pushChanges provider");
            return;
        }
        jsi::Runtime& rt = *runtime;
        jsi::Value result;
        try {
            result = provider->call(rt);
        } catch (const jsi::JSError& e) {
            (*completionPtr)(false, e.what());
            return;
        } catch (const std::exception& e) {
            (*completionPtr)(false, e.what());
            return;
        } catch (...) {
            (*completionPtr)(false, "pushChangesProvider threw");
            return;
        }
        if (result.isObject()) {
            try {
                jsi::Object resultObj = result.asObject(rt);
                jsi::Value thenValue = resultObj.getProperty(rt, "then");
                if (thenValue.isObject() && thenValue.asObject(rt).isFunction(rt)) {
                    jsi::Function thenFunc = thenValue.asObject(rt).asFunction(rt);
                    auto resolve = jsi::Function::createFromHostFunction(
                        rt,
                        jsi::PropNameID::forUtf8(rt, "resolve"),
                        1,
                        [completionPtr](jsi::Runtime&, const jsi::Value&, const jsi::Value*, size_t) -> jsi::Value {
                            (*completionPtr)(true, "");
                            return jsi::Value::undefined();
                        });
                    auto reject = jsi::Function::createFromHostFunction(
                        rt,
                        jsi::PropNameID::forUtf8(rt, "reject"),
                        1,
                        [completionPtr](jsi::Runtime& rt2, const jsi::Value&, const jsi::Value* args, size_t count) -> jsi::Value {
                            std::string message = "pushChanges rejected";
                            if (count > 0 && args[0].isString()) {
                                message = args[0].asString(rt2).utf8(rt2);
                            }
                            (*completionPtr)(false, message);
                            return jsi::Value::undefined();
                        });
                    thenFunc.call(rt, resultObj, resolve, reject);
                    return;
                }
            } catch (const jsi::JSError& e) {
                (*completionPtr)(false, e.what());
                return;
            } catch (const std::exception& e) {
                (*completionPtr)(false, e.what());
                return;
            } catch (...) {
                (*completionPtr)(false, "pushChangesProvider promise handling threw");
                return;
            }
        }
        (*completionPtr)(true, "");
    });
}

} // namespace facebook::react
