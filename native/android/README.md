# Android Native Notes

## zstd dependency
- We vendor `native/android/libs/zstd-jni.aar` to make native builds deterministic for the C++ linker.
- Gradle uses it as `compileOnly` to avoid shipping duplicate Java classes when apps already depend on zstd‑jni (e.g. `react-native-zstd`).
- CMake links against the AAR directly and includes headers from `native/android/libs/zstd-headers/libzstd`.
- If you update zstd, replace the AAR and headers together.
