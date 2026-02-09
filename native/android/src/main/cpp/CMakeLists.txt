
# For more information about using CMake with Android Studio, read the
# documentation: https://d.android.com/studio/projects/add-native-code.html.
# For more examples on how to use CMake, see https://github.com/android/ndk-samples.

project(watermelon-jsi-android-bridge)

# Sets the minimum CMake version required for this project.
cmake_minimum_required(VERSION 3.22.1)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(REACT_NATIVE_NEW_ARCH_ENABLED 1)

add_compile_options(
    -fexceptions
    -frtti
    -std=c++20
    -DLOG_TAG=\"ReactNative\"
    -DFOLLY_NO_CONFIG=1
)

include_directories(
    .
    ../../../../shared
)

# Resolve simdjson headers — walk up from this file to find node_modules/@nozbe/simdjson
set(SIMDJSON_DIR "")
set(_SIMDJSON_SEARCH_DIR "${CMAKE_CURRENT_LIST_DIR}")
foreach(_i RANGE 12)
  set(_CANDIDATE "${_SIMDJSON_SEARCH_DIR}/node_modules/@nozbe/simdjson")
  if (EXISTS "${_CANDIDATE}")
    set(SIMDJSON_DIR "${_CANDIDATE}")
    break()
  endif ()
  get_filename_component(_SIMDJSON_SEARCH_DIR "${_SIMDJSON_SEARCH_DIR}" DIRECTORY)
endforeach()

if (SIMDJSON_DIR STREQUAL "")
  message(FATAL_ERROR "simdjson headers not found. Please add @nozbe/simdjson to your dependencies.")
endif ()

set(SIMDJSON_INCLUDE_DIR "${SIMDJSON_DIR}/include")
set(SIMDJSON_SRC_DIR "${SIMDJSON_DIR}/src")
if (EXISTS "${SIMDJSON_DIR}/simdjson.h")
  include_directories("${SIMDJSON_DIR}")
  message(STATUS "Using simdjson headers: ${SIMDJSON_DIR}")
elseif (EXISTS "${SIMDJSON_INCLUDE_DIR}/simdjson.h")
  include_directories("${SIMDJSON_INCLUDE_DIR}")
  message(STATUS "Using simdjson headers: ${SIMDJSON_INCLUDE_DIR}")
elseif (EXISTS "${SIMDJSON_SRC_DIR}/simdjson.h")
  include_directories("${SIMDJSON_SRC_DIR}")
  message(STATUS "Using simdjson headers: ${SIMDJSON_SRC_DIR}")
else ()
  message(FATAL_ERROR "simdjson package found at ${SIMDJSON_DIR} but no headers. Check @nozbe/simdjson installation.")
endif ()

# Configure sqlite-android manually using AAR extraction
# Note: This replaces find_package(sqlite-android REQUIRED CONFIG) to work around
# missing Prefab metadata in the AAR when using custom sqlite-android builds

# Use direct path construction instead of find_file to avoid CMake search issues
set(SQLITE_ANDROID_AAR "${CMAKE_CURRENT_LIST_DIR}/../../../libs/sqlite-android.aar")

find_package(ReactAndroid REQUIRED CONFIG)
find_package(fbjni REQUIRED CONFIG)

# Verify the file exists
if (NOT EXISTS "${SQLITE_ANDROID_AAR}")
  # Try alternative locations
  set(SQLITE_ANDROID_AAR_ALT "${CMAKE_CURRENT_LIST_DIR}/../../../../../../../node_modules/@BuildHero/watermelondb/native/android/libs/sqlite-android.aar")
  if (EXISTS "${SQLITE_ANDROID_AAR_ALT}")
    set(SQLITE_ANDROID_AAR "${SQLITE_ANDROID_AAR_ALT}")
  endif ()
endif ()

message(STATUS "Using sqlite-android AAR: ${SQLITE_ANDROID_AAR}")

if (EXISTS "${SQLITE_ANDROID_AAR}")
  # Extract AAR if not already extracted
  get_filename_component(SQLITE_AAR_DIR "${SQLITE_ANDROID_AAR}" DIRECTORY)
  set(SQLITE_ANDROID_EXTRACTED_DIR "${SQLITE_AAR_DIR}/sqlite-android-extracted")

  if (NOT EXISTS "${SQLITE_ANDROID_EXTRACTED_DIR}")
    file(MAKE_DIRECTORY "${SQLITE_ANDROID_EXTRACTED_DIR}")
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E tar xf "${SQLITE_ANDROID_AAR}"
        WORKING_DIRECTORY "${SQLITE_ANDROID_EXTRACTED_DIR}"
        RESULT_VARIABLE EXTRACT_RESULT
    )
    if (NOT EXTRACT_RESULT EQUAL 0)
      message(FATAL_ERROR "Failed to extract sqlite-android AAR")
    endif ()
  endif ()

  # Create the sqlite-android target manually
  add_library(sqlite-android::sqlite3x SHARED IMPORTED)

  # Set the library file based on the Android architecture
  if (CMAKE_ANDROID_ARCH_ABI STREQUAL "arm64-v8a")
    set_target_properties(sqlite-android::sqlite3x PROPERTIES
        IMPORTED_LOCATION "${SQLITE_ANDROID_EXTRACTED_DIR}/jni/arm64-v8a/libsqlite3x.so")
  elseif (CMAKE_ANDROID_ARCH_ABI STREQUAL "armeabi-v7a")
    set_target_properties(sqlite-android::sqlite3x PROPERTIES
        IMPORTED_LOCATION "${SQLITE_ANDROID_EXTRACTED_DIR}/jni/armeabi-v7a/libsqlite3x.so")
  elseif (CMAKE_ANDROID_ARCH_ABI STREQUAL "x86")
    set_target_properties(sqlite-android::sqlite3x PROPERTIES
        IMPORTED_LOCATION "${SQLITE_ANDROID_EXTRACTED_DIR}/jni/x86/libsqlite3x.so")
  elseif (CMAKE_ANDROID_ARCH_ABI STREQUAL "x86_64")
    set_target_properties(sqlite-android::sqlite3x PROPERTIES
        IMPORTED_LOCATION "${SQLITE_ANDROID_EXTRACTED_DIR}/jni/x86_64/libsqlite3x.so")
  endif ()

  # Use direct path construction for headers too
  set(SQLITE_HEADERS_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../libs/sqlite-headers")

  # Verify headers exist
  if (NOT EXISTS "${SQLITE_HEADERS_DIR}/sqlite3.h")
    # Try alternative locations
    set(SQLITE_HEADERS_ALT "${SQLITE_AAR_DIR}/sqlite-headers")
    if (EXISTS "${SQLITE_HEADERS_ALT}/sqlite3.h")
      set(SQLITE_HEADERS_DIR "${SQLITE_HEADERS_ALT}")
    else ()
      # Check extracted AAR location
      if (EXISTS "${SQLITE_ANDROID_EXTRACTED_DIR}/../sqlite-headers/sqlite3.h")
        set(SQLITE_HEADERS_DIR "${SQLITE_ANDROID_EXTRACTED_DIR}/../sqlite-headers")
      endif ()
    endif ()
  endif ()

  message(STATUS "Using sqlite headers: ${SQLITE_HEADERS_DIR}")

  if (EXISTS "${SQLITE_HEADERS_DIR}/sqlite3.h")
    set_target_properties(sqlite-android::sqlite3x PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${SQLITE_HEADERS_DIR}")
    message(STATUS "Configured sqlite-android from AAR: ${SQLITE_ANDROID_AAR}")
    message(STATUS "Using headers from: ${SQLITE_HEADERS_DIR}")
  else ()
    message(FATAL_ERROR "sqlite3.h header not found. Please ensure your sqlite-android fork includes headers.")
  endif ()
else ()
  message(FATAL_ERROR "sqlite-android.aar not found. Please build your sqlite-android fork first.")
endif ()

# Configure zstd-jni manually (similar to sqlite-android)
set(ZSTD_JNI_AAR "${CMAKE_CURRENT_LIST_DIR}/../../../libs/zstd-jni.aar")

if (EXISTS "${ZSTD_JNI_AAR}")
  message(STATUS "Using zstd-jni AAR: ${ZSTD_JNI_AAR}")

  get_filename_component(ZSTD_AAR_DIR "${ZSTD_JNI_AAR}" DIRECTORY)
  set(ZSTD_JNI_EXTRACTED_DIR "${ZSTD_AAR_DIR}/zstd-jni-extracted")

  if (NOT EXISTS "${ZSTD_JNI_EXTRACTED_DIR}")
    file(MAKE_DIRECTORY "${ZSTD_JNI_EXTRACTED_DIR}")
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E tar xf "${ZSTD_JNI_AAR}"
        WORKING_DIRECTORY "${ZSTD_JNI_EXTRACTED_DIR}"
        RESULT_VARIABLE ZSTD_EXTRACT_RESULT
    )
    if (NOT ZSTD_EXTRACT_RESULT EQUAL 0)
      message(FATAL_ERROR "Failed to extract zstd-jni AAR")
    endif ()
  endif ()

  add_library(zstd-jni::zstdjni SHARED IMPORTED)

  if (CMAKE_ANDROID_ARCH_ABI STREQUAL "arm64-v8a")
    set_target_properties(zstd-jni::zstdjni PROPERTIES
        IMPORTED_LOCATION "${ZSTD_JNI_EXTRACTED_DIR}/jni/arm64-v8a/libzstd-jni-1.5.7-6.so")
  elseif (CMAKE_ANDROID_ARCH_ABI STREQUAL "armeabi-v7a")
    set_target_properties(zstd-jni::zstdjni PROPERTIES
        IMPORTED_LOCATION "${ZSTD_JNI_EXTRACTED_DIR}/jni/armeabi-v7a/libzstd-jni-1.5.7-6.so")
  elseif (CMAKE_ANDROID_ARCH_ABI STREQUAL "x86")
    set_target_properties(zstd-jni::zstdjni PROPERTIES
        IMPORTED_LOCATION "${ZSTD_JNI_EXTRACTED_DIR}/jni/x86/libzstd-jni-1.5.7-6.so")
  elseif (CMAKE_ANDROID_ARCH_ABI STREQUAL "x86_64")
    set_target_properties(zstd-jni::zstdjni PROPERTIES
        IMPORTED_LOCATION "${ZSTD_JNI_EXTRACTED_DIR}/jni/x86_64/libzstd-jni-1.5.7-6.so")
  endif ()
else ()
  message(FATAL_ERROR "zstd-jni AAR not found at ${ZSTD_JNI_AAR}. Please vendor it in native/android/libs.")
endif ()

# zstd headers (needed by shared SliceDecoder)
set(ZSTD_HEADERS_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../libs/zstd-headers")
if (EXISTS "${ZSTD_HEADERS_DIR}/libzstd/zstd.h")
  include_directories("${ZSTD_HEADERS_DIR}")
  message(STATUS "Using zstd headers: ${ZSTD_HEADERS_DIR}")
else ()
  message(FATAL_ERROR "zstd headers not found. Expected ${ZSTD_HEADERS_DIR}/libzstd/zstd.h")
endif ()

# Check if new architecture is enabled to determine library type and source files
set(LIB_TYPE STATIC)

set(SOURCE_FILES
    ../../../../shared/Sqlite.cpp
    ../../../../shared/DatabaseUtils.cpp
    ../../../../shared/SliceDecoder.cpp
    ../../../../shared/SliceImportEngine.cpp
    ../../../../shared/SyncEngine.cpp
    ../../../../shared/SimdjsonImpl.cpp
    ../../../../shared/SyncApplyEngine.cpp
    ../../../../shared/SqliteInsertHelper.cpp
    # List C/C++ source files with relative paths to this CMakeLists.txt.
    JSIAndroidUtils.cpp
    JSIAndroidBridgeWrapper.cpp
    JSIAndroidBridgeModule.cpp
    JSIAndroidBridgeInstaller.cpp
    SlicePlatformAndroid.cpp
    SyncPlatformAndroid.cpp
    SliceImportDatabaseAdapterAndroid.cpp)

set(LINKED_LIBRARIES
    sqlite-android::sqlite3x
    zstd-jni::zstdjni
    android
    log
    ReactAndroid::reactnative
    ReactAndroid::jsi
    fbjni::fbjni
    android
    react_codegen_WatermelonDBSpec)

add_library(watermelon-jsi-android-bridge
    ${LIB_TYPE}
    ${SOURCE_FILES})

# Expose the include directory for autolinking - ALWAYS needed regardless of architecture
target_include_directories(watermelon-jsi-android-bridge
    PUBLIC
    .
    ../../../../shared
    INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR})

# Specifies libraries CMake should link to your target library. You
# can link libraries from various origins, such as libraries defined in this
# build script, prebuilt third-party libraries, or Android system libraries.

target_link_libraries(watermelon-jsi-android-bridge
    ${LINKED_LIBRARIES})

# --- Optional: Include codegen-generated JNI if it exists (RN 0.80+ compatibility) ---
set(WATERMELON_CODEGEN_JNI_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/build/generated/source/codegen/jni")

if (EXISTS "${WATERMELON_CODEGEN_JNI_DIR}/CMakeLists.txt")
  message(STATUS "Including WatermelonDB codegen JNI from ${WATERMELON_CODEGEN_JNI_DIR}")
  add_subdirectory("${WATERMELON_CODEGEN_JNI_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/watermelondb_codegen")
else ()
  message(STATUS "No WatermelonDB codegen JNI directory found, skipping")
endif ()
