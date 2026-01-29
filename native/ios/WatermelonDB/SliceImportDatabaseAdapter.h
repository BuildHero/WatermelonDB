#pragma once

#include <memory>

#ifdef __OBJC__
@class DatabaseBridge;
@class NSNumber;
#endif

namespace watermelondb {
class DatabaseInterface;
}

std::shared_ptr<watermelondb::DatabaseInterface> createIOSDatabaseInterface(DatabaseBridge *db, NSNumber *connectionTag);
