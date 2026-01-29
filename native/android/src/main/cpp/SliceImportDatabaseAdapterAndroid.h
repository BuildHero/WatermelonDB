#pragma once

#include <memory>
#include <jni.h>

namespace watermelondb {
class DatabaseInterface;
}

std::shared_ptr<watermelondb::DatabaseInterface> createAndroidDatabaseInterface(jobject bridge, jint connectionTag);
