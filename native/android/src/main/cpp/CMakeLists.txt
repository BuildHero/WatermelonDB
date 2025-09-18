
# For more information about using CMake with Android Studio, read the
# documentation: https://d.android.com/studio/projects/add-native-code.html.
# For more examples on how to use CMake, see https://github.com/android/ndk-samples.

project(watermelon-jsi-android-bridge)

# Sets the minimum CMake version required for this project.
cmake_minimum_required(VERSION 3.22.1)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT DEFINED REACT_NATIVE_NEW_ARCH_ENABLED)
    set(REACT_NATIVE_NEW_ARCH_ENABLED 0)
endif()

add_compile_options(-fexceptions -frtti -std=c++20)

include_directories(
        .
        ../../../../shared
        ../../../../../../../react-native/React
        ../../../../../../../react-native/React/Base
        ../../../../../../../react-native/ReactCommon
        ../../../../../../../react-native/ReactCommon/jsi
)

# Configure sqlite-android manually using AAR extraction
# Note: This replaces find_package(sqlite-android REQUIRED CONFIG) to work around
# missing Prefab metadata in the AAR when using custom sqlite-android builds

# Check if we can find sqlite-android AAR in the expected location
find_file(SQLITE_ANDROID_AAR
    NAMES sqlite-android.aar sqlite-android-release.aar sqlite-android-debug.aar
    PATHS 
        "${CMAKE_CURRENT_LIST_DIR}/../../libs"
        "${CMAKE_CURRENT_LIST_DIR}/../../../../../../../node_modules/@BuildHero/watermelondb/native/android/libs"
        # Also check build output directories
        "${CMAKE_CURRENT_LIST_DIR}/../../../../../../../sqlite-android/sqlite-android/build/outputs/aar"
        "${CMAKE_CURRENT_LIST_DIR}/../../../../../../sqlite-android/sqlite-android/build/outputs/aar"
    NO_DEFAULT_PATH
)

if(SQLITE_ANDROID_AAR)
    # Extract AAR if not already extracted
    get_filename_component(SQLITE_AAR_DIR "${SQLITE_ANDROID_AAR}" DIRECTORY)
    set(SQLITE_ANDROID_EXTRACTED_DIR "${SQLITE_AAR_DIR}/sqlite-android-extracted")
    
    if(NOT EXISTS "${SQLITE_ANDROID_EXTRACTED_DIR}")
        file(MAKE_DIRECTORY "${SQLITE_ANDROID_EXTRACTED_DIR}")
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E tar xf "${SQLITE_ANDROID_AAR}"
            WORKING_DIRECTORY "${SQLITE_ANDROID_EXTRACTED_DIR}"
            RESULT_VARIABLE EXTRACT_RESULT
        )
        if(NOT EXTRACT_RESULT EQUAL 0)
            message(FATAL_ERROR "Failed to extract sqlite-android AAR")
        endif()
    endif()
    
    # Create the sqlite-android target manually
    add_library(sqlite-android::sqlite3x SHARED IMPORTED)
    
    # Set the library file based on the Android architecture
    if(CMAKE_ANDROID_ARCH_ABI STREQUAL "arm64-v8a")
        set_target_properties(sqlite-android::sqlite3x PROPERTIES
            IMPORTED_LOCATION "${SQLITE_ANDROID_EXTRACTED_DIR}/jni/arm64-v8a/libsqlite3x.so")
    elseif(CMAKE_ANDROID_ARCH_ABI STREQUAL "armeabi-v7a")
        set_target_properties(sqlite-android::sqlite3x PROPERTIES
            IMPORTED_LOCATION "${SQLITE_ANDROID_EXTRACTED_DIR}/jni/armeabi-v7a/libsqlite3x.so")
    elseif(CMAKE_ANDROID_ARCH_ABI STREQUAL "x86")
        set_target_properties(sqlite-android::sqlite3x PROPERTIES
            IMPORTED_LOCATION "${SQLITE_ANDROID_EXTRACTED_DIR}/jni/x86/libsqlite3x.so")
    elseif(CMAKE_ANDROID_ARCH_ABI STREQUAL "x86_64")
        set_target_properties(sqlite-android::sqlite3x PROPERTIES
            IMPORTED_LOCATION "${SQLITE_ANDROID_EXTRACTED_DIR}/jni/x86_64/libsqlite3x.so")
    endif()
    
    # Look for headers in common locations
    find_path(SQLITE_HEADERS_DIR
        NAMES sqlite3.h
        PATHS
            "${SQLITE_AAR_DIR}/sqlite-headers"
            "${CMAKE_CURRENT_LIST_DIR}/../../libs/sqlite-headers"
            # Also check sqlite-android source directories
            "${CMAKE_CURRENT_LIST_DIR}/../../../../../../../sqlite-android/sqlite-android/src/main/include"
            "${CMAKE_CURRENT_LIST_DIR}/../../../../../../sqlite-android/sqlite-android/src/main/include"
        NO_DEFAULT_PATH
    )
    
    if(SQLITE_HEADERS_DIR)
        set_target_properties(sqlite-android::sqlite3x PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${SQLITE_HEADERS_DIR}")
        message(STATUS "Configured sqlite-android from AAR: ${SQLITE_ANDROID_AAR}")
        message(STATUS "Using headers from: ${SQLITE_HEADERS_DIR}")
    else()
        message(FATAL_ERROR "sqlite3.h header not found. Please ensure your sqlite-android fork includes headers.")
    endif()
else()
    message(FATAL_ERROR "sqlite-android.aar not found. Please build your sqlite-android fork first.")
endif()

# Check if new architecture is enabled to determine library type and source files
if(REACT_NATIVE_NEW_ARCH_ENABLED)
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
        react_codegen_WatermelonDBSpec)
else()
    set(LIB_TYPE SHARED)
    set(SOURCE_FILES
        ../../../../shared/Sqlite.cpp
        ../../../../shared/DatabaseUtils.cpp
        ../../../../../../../react-native/ReactCommon/jsi/jsi/jsi.cpp
        # List C/C++ source files with relative paths to this CMakeLists.txt.
        # JSIAndroidBridgeModule.cpp is excluded for old architecture
        JSIAndroidUtils.cpp
        JSIAndroidBridgeWrapper.cpp
        JSIAndroidBridgeInstaller.cpp)

    set(LINKED_LIBRARIES
        sqlite-android::sqlite3x
        # List libraries link to the target library
        android
        log)
endif()

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

if(EXISTS "${WATERMELON_CODEGEN_JNI_DIR}/CMakeLists.txt")
  message(STATUS "Including WatermelonDB codegen JNI from ${WATERMELON_CODEGEN_JNI_DIR}")
  add_subdirectory("${WATERMELON_CODEGEN_JNI_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/watermelondb_codegen")
else()
  message(STATUS "No WatermelonDB codegen JNI directory found, skipping")
endif()