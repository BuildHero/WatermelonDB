module.exports = {
  dependency: {
    platforms: {
      /**
       * @type {import('@react-native-community/cli-types').AndroidDependencyParams}
       */
      android: {
        sourceDir: './native/android',
        cxxModuleCMakeListsModuleName: 'watermelon-jsi-android-bridge',
        cxxModuleCMakeListsPath: `src/main/cpp/CMakeLists.txt`,
        cxxModuleHeaderName: 'JSIAndroidBridgeModule',
      },
    },
  },
}
