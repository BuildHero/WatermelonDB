cmake_minimum_required(VERSION 3.16)
project(WatermelonDBSharedTests CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT SIMDJSON_INCLUDE_DIR)
  message(FATAL_ERROR "SIMDJSON_INCLUDE_DIR is required (e.g. <repo>/node_modules/@nozbe/simdjson/src)")
endif()
get_filename_component(SIMDJSON_INCLUDE_DIR_ABS "${SIMDJSON_INCLUDE_DIR}" REALPATH BASE_DIR "${CMAKE_CURRENT_LIST_DIR}")

find_package(SQLite3 REQUIRED)
find_library(ZSTD_LIBRARY zstd)
find_path(ZSTD_INCLUDE_DIR libzstd/zstd.h)
if (NOT ZSTD_INCLUDE_DIR)
  set(ZSTD_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../native/android/libs/zstd-headers")
endif()
if (NOT EXISTS "${ZSTD_INCLUDE_DIR}/libzstd/zstd.h")
  message(FATAL_ERROR "zstd headers not found (expected libzstd/zstd.h). Set ZSTD_INCLUDE_DIR or install zstd headers.")
endif()

find_path(JSI_INCLUDE_DIR jsi/jsi.h)
if (NOT JSI_INCLUDE_DIR AND EXISTS "${CMAKE_CURRENT_LIST_DIR}/../../../node_modules/react-native/ReactCommon/jsi/jsi/jsi.h")
  set(JSI_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../node_modules/react-native/ReactCommon/jsi")
elseif (NOT JSI_INCLUDE_DIR)
  set(JSI_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../node_modules/react-native/ReactCommon")
endif()

set(HERMES_ROOT_ENGINE "${CMAKE_CURRENT_LIST_DIR}/../../../node_modules/react-native/sdks/hermes-engine/destroot")
set(HERMES_ROOT_SOURCE "${CMAKE_CURRENT_LIST_DIR}/../../../node_modules/react-native/sdks/hermes/destroot")
set(HERMES_SRC_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../../node_modules/react-native/sdks/hermes")
if (EXISTS "${HERMES_ROOT_ENGINE}")
  set(HERMES_ROOT "${HERMES_ROOT_ENGINE}")
elseif (EXISTS "${HERMES_ROOT_SOURCE}")
  set(HERMES_ROOT "${HERMES_ROOT_SOURCE}")
else()
  set(HERMES_ROOT "${HERMES_ROOT_ENGINE}")
endif()
set(HERMES_INCLUDE_FALLBACK "${HERMES_ROOT}/include")
set(HERMES_API_FALLBACK "${HERMES_SRC_ROOT}/API")
set(HERMES_FRAMEWORK_FALLBACK "${HERMES_ROOT}/Library/Frameworks/macosx/hermes.framework/hermes")
set(HERMES_FRAMEWORK_BUILD_FALLBACK "${HERMES_SRC_ROOT}/build_macosx/API/hermes/hermes.framework/hermes")

find_path(HERMES_INCLUDE_DIR hermes/hermes.h)
if (NOT HERMES_INCLUDE_DIR AND EXISTS "${HERMES_INCLUDE_FALLBACK}/hermes/hermes.h")
  set(HERMES_INCLUDE_DIR "${HERMES_INCLUDE_FALLBACK}")
elseif (NOT HERMES_INCLUDE_DIR AND EXISTS "${HERMES_API_FALLBACK}/hermes/hermes.h")
  set(HERMES_INCLUDE_DIR "${HERMES_API_FALLBACK}")
endif()

if (EXISTS "${HERMES_FRAMEWORK_FALLBACK}")
  set(HERMES_LIB "${HERMES_FRAMEWORK_FALLBACK}")
elseif (EXISTS "${HERMES_FRAMEWORK_BUILD_FALLBACK}")
  set(HERMES_LIB "${HERMES_FRAMEWORK_BUILD_FALLBACK}")
else()
  find_library(HERMES_LIB hermes)
endif()

add_executable(sync_engine_tests
  SyncEngineTests.cpp
  ${SIMDJSON_INCLUDE_DIR_ABS}/simdjson.cpp
)
target_include_directories(sync_engine_tests PRIVATE ${CMAKE_CURRENT_LIST_DIR}/.. ${SIMDJSON_INCLUDE_DIR})
target_include_directories(sync_engine_tests PRIVATE ${SIMDJSON_INCLUDE_DIR_ABS})

add_executable(sync_apply_engine_tests
  SyncApplyEngineTests.cpp
  ../SyncApplyEngine.cpp
  ${SIMDJSON_INCLUDE_DIR_ABS}/simdjson.cpp
)
target_include_directories(sync_apply_engine_tests PRIVATE ${CMAKE_CURRENT_LIST_DIR}/.. ${SIMDJSON_INCLUDE_DIR})
target_include_directories(sync_apply_engine_tests PRIVATE ${SIMDJSON_INCLUDE_DIR_ABS})
target_link_libraries(sync_apply_engine_tests PRIVATE SQLite::SQLite3)

add_executable(slice_decoder_tests
  SliceDecoderTests.cpp
  ../SliceDecoder.cpp
)
target_include_directories(slice_decoder_tests PRIVATE ${CMAKE_CURRENT_LIST_DIR}/..)
if (ZSTD_INCLUDE_DIR)
  target_include_directories(slice_decoder_tests PRIVATE ${ZSTD_INCLUDE_DIR})
endif()
if (ZSTD_LIBRARY)
  target_link_libraries(slice_decoder_tests PRIVATE ${ZSTD_LIBRARY})
endif()

add_executable(slice_import_engine_tests
  SliceImportEngineTests.cpp
  ../SliceImportEngine.cpp
  ../SliceDecoder.cpp
)
target_include_directories(slice_import_engine_tests PRIVATE ${CMAKE_CURRENT_LIST_DIR}/..)
if (ZSTD_INCLUDE_DIR)
  target_include_directories(slice_import_engine_tests PRIVATE ${ZSTD_INCLUDE_DIR})
endif()
if (ZSTD_LIBRARY)
  target_link_libraries(slice_import_engine_tests PRIVATE ${ZSTD_LIBRARY})
endif()

add_executable(sqlite_insert_helper_tests
  SqliteInsertHelperTests.cpp
  ../SqliteInsertHelper.cpp
)
target_include_directories(sqlite_insert_helper_tests PRIVATE ${CMAKE_CURRENT_LIST_DIR}/..)
if (ZSTD_INCLUDE_DIR)
  target_include_directories(sqlite_insert_helper_tests PRIVATE ${ZSTD_INCLUDE_DIR})
endif()
target_link_libraries(sqlite_insert_helper_tests PRIVATE SQLite::SQLite3)

set(HERMES_HEADER_OK FALSE)
if (EXISTS "${HERMES_INCLUDE_DIR}/hermes/hermes.h")
  set(HERMES_HEADER_OK TRUE)
elseif (EXISTS "${HERMES_INCLUDE_DIR}/hermes.h")
  set(HERMES_HEADER_OK TRUE)
endif()

if (HERMES_HEADER_OK AND EXISTS "${JSI_INCLUDE_DIR}/jsi/jsi.h" AND HERMES_LIB)
  message(STATUS "Hermes include: ${HERMES_INCLUDE_DIR}")
  message(STATUS "Hermes lib: ${HERMES_LIB}")
  add_executable(database_utils_tests
    DatabaseUtilsTests.cpp
    ../DatabaseUtils.cpp
    ../Sqlite.cpp
    PlatformStubs.cpp
  )
  target_include_directories(database_utils_tests PRIVATE ${CMAKE_CURRENT_LIST_DIR}/.. ${JSI_INCLUDE_DIR} ${HERMES_INCLUDE_DIR})
  target_link_libraries(database_utils_tests PRIVATE SQLite::SQLite3 ${HERMES_LIB})
else()
  message(STATUS "Hermes/JSI not found; database_utils_tests will be skipped")
endif()
