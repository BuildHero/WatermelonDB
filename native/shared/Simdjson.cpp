// This file is required for simdjson to work properly.
//
// simdjson is a header-only library, but it requires exactly ONE compilation unit
// to define SIMDJSON_IMPLEMENTATION before including the header. This generates
// the actual function implementations that all other files will link against.
//
// Without this file, you would get linker errors (undefined references to simdjson functions).
//
// DO NOT REMOVE THIS FILE - it's the compilation unit that makes simdjson work.
// Other files should just #include <simdjson.h> without defining SIMDJSON_IMPLEMENTATION.

#if __has_include(<simdjson.cpp>)
#include <simdjson.cpp>
#elif __has_include(<simdjson.h>)
#define SIMDJSON_IMPLEMENTATION
#include <simdjson.h>
#elif __has_include("simdjson.h")
#define SIMDJSON_IMPLEMENTATION
#include "simdjson.h"
#endif
