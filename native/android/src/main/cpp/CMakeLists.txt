
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

# Check if new architecture is enabled to determine library type and source files
set(LIB_TYPE STATIC)

set(SOURCE_FILES
    ../../../../shared/Sqlite.cpp
    ../../../../shared/DatabaseUtils.cpp
    ../../../../../../../react-native/ReactCommon/jsi/jsi/jsi.cpp
    # List C/C++ source files with relative paths to this CMakeLists.txt.
    JSIAndroidUtils.cpp
    JSIAndroidBridgeWrapper.cpp
    JSIAndroidBridgeModule.cpp
    JSIAndroidBridgeInstaller.cpp)

set(LINKED_LIBRARIES
    sqlite-android::sqlite3x
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