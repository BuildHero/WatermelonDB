#include "JSIWrapperUtils.h"
#include "DatabaseUtils.h"
#include <string>

namespace watermelondb {

jsi::Value execSqlQuery(DatabaseBridge *databaseBridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &sql, const jsi::Array &args) {
   auto tagNumber = [[NSNumber alloc] initWithDouble:tag.asNumber()];
    
    auto db = [databaseBridge getRawConnectionWithConnectionTag:tagNumber];
    
    
    auto stmt = getStmt(rt, static_cast<sqlite3*>(db), sql.utf8(rt), args);
    
    std::vector<jsi::Value> records = {};
    
    while (true) {
        if (getNextRowOrTrue(rt, stmt)) {
            break;
        }
        
        jsi::Object record = resultDictionary(rt, stmt);
        
        records.push_back(std::move(record));
    }
    
    finalizeStmt(stmt);
    
    return arrayFromStd(rt, records);
}

jsi::Value query(DatabaseBridge *databaseBridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &table, const jsi::String &query) {
    auto tagNumber = [[NSNumber alloc] initWithDouble:tag.asNumber()];
    auto tableStr = [NSString stringWithUTF8String:table.utf8(rt).c_str()];
    
    auto db = [databaseBridge getRawConnectionWithConnectionTag:tagNumber];
        
    auto stmt = getStmt(rt, static_cast<sqlite3*>(db), query.utf8(rt), jsi::Array(rt, 0));
    
    std::vector<jsi::Value> records = {};
    
    while (true) {
        if (getNextRowOrTrue(rt, stmt)) {
            break;
        }
        
        assert(std::string(sqlite3_column_name(stmt, 0)) == "id");
        
        const char *id = (const char *)sqlite3_column_text(stmt, 0);
        
        if (!id) {
            throw jsi::JSError(rt, "Failed to get ID of a record");
        }
        
        auto idStr = [NSString stringWithUTF8String:id];
                
        bool isCached = [databaseBridge isCachedWithConnectionTag:tagNumber table:tableStr id:idStr];
        
        if (isCached) {
            jsi::String jsiId = jsi::String::createFromAscii(rt, id);
            records.push_back(std::move(jsiId));
        } else {
            [databaseBridge markAsCachedWithConnectionTag:tagNumber table:tableStr id:idStr];
            jsi::Object record = resultDictionary(rt, stmt);
            records.push_back(std::move(record));
        }
    }
    
    finalizeStmt(stmt);
    
    return arrayFromStd(rt, records);
}

} // namespace watermelondb

