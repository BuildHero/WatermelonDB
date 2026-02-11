#pragma once

#include <WatermelonDBSpecJSI.h>

#include <memory>
#include <string>
#include <mutex>
#include <unordered_map>
#include "SyncEngine.h"

#import <jsi/jsi.h>

namespace facebook::react {

class JSISwiftWrapperModule : public NativeWatermelonDBModuleCxxSpec<JSISwiftWrapperModule> {
public:
    JSISwiftWrapperModule(std::shared_ptr<CallInvoker> jsInvoker);
    ~JSISwiftWrapperModule();
    
    jsi::Array query(jsi::Runtime &rt, double tag, jsi::String table, jsi::String query);
    jsi::Array execSqlQuery(jsi::Runtime &rt, double tag, jsi::String sql, jsi::Array args);
    jsi::Value importRemoteSlice(
                                 jsi::Runtime &rt, 
                                 double tag, 
                                 jsi::String sliceUrl
                                 );
    void configureSync(jsi::Runtime &rt, jsi::String configJson);
    void startSync(jsi::Runtime &rt, jsi::String reason);
    jsi::Value syncDatabaseAsync(jsi::Runtime &rt, jsi::String reason);
    void setSyncPullUrl(jsi::Runtime &rt, jsi::String pullEndpointUrl);
    jsi::String getSyncStateJson(jsi::Runtime &rt);
    double addSyncListener(jsi::Runtime &rt, jsi::Function listener);
    void removeSyncListener(jsi::Runtime &rt, double listenerId);
    void setAuthToken(jsi::Runtime &rt, jsi::String token);
    void clearAuthToken(jsi::Runtime &rt);
    void setAuthTokenProvider(jsi::Runtime &rt, jsi::Function provider);
    void setPushChangesProvider(jsi::Runtime &rt, jsi::Function provider);
    void initSyncSocket(jsi::Runtime &rt, jsi::String socketUrl);
    void syncSocketAuthenticate(jsi::Runtime &rt, jsi::String token);
    void syncSocketDisconnect(jsi::Runtime &rt);
    void cancelSync(jsi::Runtime &rt);
    void configureBackgroundSync(jsi::Runtime &rt, jsi::String configJson);
    void enableBackgroundSync(jsi::Runtime &rt);
    void disableBackgroundSync(jsi::Runtime &rt);
    jsi::Value decompressZstd(jsi::Runtime &rt, jsi::String src, jsi::String dest);

private:
    struct SyncEventState {
        std::mutex mutex;
        std::unordered_map<int64_t, jsi::Function> listeners;
        jsi::Runtime* runtime = nullptr;
        std::shared_ptr<CallInvoker> jsInvoker;
        bool alive = true;
    };

    std::mutex mutex_;  
    int64_t nextSyncListenerId_ = 1;
    std::shared_ptr<watermelondb::SyncEngine> syncEngine_;
    std::shared_ptr<SyncEventState> syncEventState_;
    std::shared_ptr<jsi::Function> authTokenProvider_;
    std::shared_ptr<jsi::Function> pushChangesProvider_;
    int64_t syncConnectionTag_ = 0;
    void* socketStatusObserver_ = nullptr;
    void* socketCdcObserver_ = nullptr;
    
    void emitSyncEventLocked(const std::string &eventJson);
    void requestAuthTokenFromJs();
    void requestPushChangesFromJs(std::function<void(bool success, const std::string& errorMessage)> completion);
};

} // namespace facebook::react
