# WatermelonDB shared native implementation

- you can easily add WatermelonDB to any native platform!
- the platform (OS and JavaScript engine) has to support latest version of React Native with TurboModules
- what you have to do:
  - compile the files in this folder
  - link sqlite3
  - provide implementation for DatabasePlatform.h
  - provide implementation for JSLockPerfHack.h (just add a stub function that calls the passed block)
  - create a TurboModule that implements NativeWatermelonDBModuleCxxSpec
- check ios/ and android/ for implementation examples
