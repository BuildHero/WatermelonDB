
# For more information about using CMake with Android Studio, read the
# documentation: https://d.android.com/studio/projects/add-native-code.html.
# For more examples on how to use CMake, see https://github.com/android/ndk-samples.

project(watermelon-jsi-android-bridge)

# Sets the minimum CMake version required for this project.
cmake_minimum_required(VERSION 3.22.1)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT DEFINED REACT_NATIVE_NEW_ARCH_ENABLED)
    set(REACT_NATIVE_NEW_ARCH_ENABLED 0)
endif()

add_compile_options(-fexceptions -frtti -std=c++17)

include_directories(
        .
        ../../../../shared
        ../../../../../../../react-native/React
        ../../../../../../../react-native/React/Base
        ../../../../../../../react-native/ReactCommon
        ../../../../../../../react-native/ReactCommon/jsi
)

find_package(sqlite-android REQUIRED CONFIG)

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


# Expose the include directory for autolinking
if(REACT_NATIVE_NEW_ARCH_ENABLED)
target_include_directories(watermelon-jsi-android-bridge
        PUBLIC
        .
        ../../../../shared
        INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# Specifies libraries CMake should link to your target library. You
# can link libraries from various origins, such as libraries defined in this
# build script, prebuilt third-party libraries, or Android system libraries.

target_link_libraries(watermelon-jsi-android-bridge
        ${LINKED_LIBRARIES})