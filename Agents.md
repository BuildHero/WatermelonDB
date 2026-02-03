# WatermelonDB - Repository Overview

## What is WatermelonDB?

WatermelonDB is a **reactive database framework** for React and React Native applications. It's designed to build powerful apps that scale from hundreds to tens of thousands of records while remaining fast and performant.

### Key Features

- ⚡️ **Fast launch times** - Lazy loading ensures apps launch instantly regardless of data size
- 📈 **Highly scalable** - Handles hundreds to tens of thousands of records efficiently
- 🔄 **Offline-first** - Built-in sync capabilities with custom backends
- 📱 **Multiplatform** - Supports iOS, Android, and web
- ⚛️ **React integration** - Seamless integration with React components
- 🔗 **Relational** - Built on SQLite foundation with full relational capabilities
- ✨ **Reactive** - Optional RxJS API for reactive programming
- ⚠️ **Type-safe** - Full TypeScript and Flow support

## Repository Structure

### Core Source Code (`src/`)

- **`Database/`** - Core database implementation, action queue, and collection management
- **`Model/`** - Model base class and model-related utilities
- **`Collection/`** - Collection implementation for querying and managing records
- **`Query/`** - Query building and execution logic
- **`Schema/`** - Schema definition and migration system
- **`adapters/`** - Database adapters:
  - `sqlite/` - SQLite adapter (primary adapter for React Native)
  - `lokijs/` - LokiJS adapter (for web/IndexedDB)
- **`sync/`** - Synchronization engine for syncing with remote backends
- **`decorators/`** - Model decorators (`@field`, `@relation`, `@children`, `@date`, `@json`, etc.)
- **`hooks/`** - React hooks for database access
- **`DatabaseProvider/`** - React context provider for database access
- **`observation/`** - Observable query subscriptions and reactive updates

### Native Code (`native/`)

- **`shared/`** - **Shared C++ code** used by both iOS and Android platforms (preferred for performance-critical cross-platform logic)
- **`ios/`** - iOS native bridge implementation (Swift/Objective-C)
- **`android/`** - Android native bridge implementation (Kotlin/Java/C++)
- **`specs/`** - Native module specifications for code generation

**Note**: The `native/shared/` directory contains C++ code that runs on both platforms and is the preferred location for new performance-critical features.

### Documentation (`docs/`)

The documentation is built using [mdBook](https://rust-lang.github.io/mdBook/) and includes:

- Installation guides
- Core concepts (Models, Queries, Relations, Schema)
- Advanced topics (Migrations, Sync, Performance)
- Implementation details (Architecture, Adapters)
- API reference

**Documentation source files** are typically located in the `docs/` directory and may include markdown files that get compiled to HTML.

### Testing

- **`src/__tests__/`** - JavaScript/TypeScript unit tests (Jest)
- **`native/shared/tests/`** - C++ unit tests
- **`native/iosTest/`** - iOS integration tests
- **`native/androidTest/`** - Android integration tests

## Important: Documentation Updates

### ⚠️ CRITICAL: Always Update Documentation

**When adding or changing features, you MUST update the documentation.**

This repository maintains comprehensive documentation to help developers understand and use WatermelonDB effectively. Keeping documentation up-to-date is essential for:

1. **Developer experience** - Users need accurate information about features and APIs
2. **Onboarding** - New contributors need clear guidance
3. **Maintenance** - Future maintainers need to understand changes
4. **Adoption** - Incomplete or outdated docs hinder adoption

### What to Update

When making changes, update:

1. **Feature Documentation** (`docs/` folder)
   - Add new sections for new features
   - Update existing sections when modifying behavior
   - Include code examples and usage patterns
   - Update API reference if methods/signatures change

2. **CHANGELOG.md**
   - Add entries for new features, bug fixes, and breaking changes
   - Follow [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format
   - See CONTRIBUTING.md for details

3. **README.md**
   - Update if adding major features or changing core behavior
   - Update examples if API changes

4. **Type Definitions**
   - Update TypeScript/Flow types if APIs change
   - Ensure exported types match implementation

5. **Code Comments**
   - Add JSDoc comments for new public APIs
   - Update existing comments if behavior changes

### Documentation Workflow

1. **Before starting work**: Review existing documentation to understand current patterns
2. **During development**: Write documentation alongside code changes
3. **Before PR**: Verify all documentation is updated and accurate
4. **In PR description**: Note what documentation was updated

### Running Documentation Locally

To preview documentation changes:

```bash
yarn docs
```

This will build and serve the documentation locally so you can verify your changes.

## Development Workflow

### Setup

```bash
git clone https://github.com/BuildHero/WatermelonDB.git
cd WatermelonDB
yarn
```

### Development Commands

- `yarn dev` - Build development version with watch mode
- `yarn test` - Run Jest tests
- `yarn test:cpp` - Run C++ tests
- `yarn test:ios` - Run iOS integration tests
- `yarn test:android` - Run Android integration tests
- `yarn eslint` - Run ESLint
- `yarn typecheck` - Run TypeScript type checking
- `yarn prettier` - Format code
- `yarn ci:check` - Run all checks (tests, lint, typecheck)
- `yarn docs` - Build and serve documentation

### Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed contribution guidelines.

Key points:
- Add tests for new/changed functionality
- Format code with `yarn prettier`
- Update CHANGELOG.md
- **Update documentation** (see above)

## Coding Guidelines

### Code Readability

**Leverage whitespace for readability**

- Use blank lines to separate logical sections of code
- Group related code together with consistent spacing
- Use indentation consistently to show code structure
- Add spacing around operators and after commas for clarity
- Break long lines appropriately to improve readability

Well-placed whitespace makes code easier to scan and understand without needing comments to explain structure.

### Code Comments

**Refrain from making unnecessary code comments**

- **Write self-documenting code** - Use clear variable names, function names, and structure
- **Comments should explain "why", not "what"** - If code needs a comment to explain what it does, consider refactoring for clarity
- **Avoid obvious comments** - Don't restate what the code already clearly shows
- **Use comments for**:
  - Complex algorithms or non-obvious business logic
  - Workarounds or temporary solutions with context
  - Public API documentation (JSDoc/TSDoc)
  - Performance considerations or optimizations
  - TODO/FIXME items with context

**Example of unnecessary comment:**
```javascript
// Increment the counter
counter++;
```

**Example of helpful comment:**
```javascript
// Use native implementation for performance on large datasets
// JS implementation is 10x slower for arrays > 10k items
return nativeProcessLargeArray(data);
```

The code itself should be readable enough that most comments become redundant. When in doubt, prefer improving code clarity over adding comments.

## Architecture Highlights

### Architectural Direction

**Performance-first approach: Native-first, JavaScript-light**

The architectural direction going forward emphasizes:

- **Native code priority** - Performance-critical operations should be implemented in native code (iOS/Android)
- **Shared C++ focus** - Leverage `native/shared/` C++ code for cross-platform performance-critical logic
- **Lightweight JavaScript layer** - Keep the JavaScript/TypeScript layer (`src/`) as thin as possible, focusing on:
  - API surface and developer experience
  - React integration and reactivity
  - Type safety and developer ergonomics
- **Performance optimization** - Move computationally expensive operations (parsing, encoding, sync logic, etc.) to native code

When adding new features or optimizing existing ones:
1. **Consider native implementation first** - Can this be done more efficiently in native code?
2. **Prefer shared C++** - Use `native/shared/` for code shared between iOS and Android
3. **Minimize JavaScript overhead** - Keep JS layer focused on orchestration and developer-facing APIs
4. **Benchmark and measure** - Verify performance improvements when moving code to native

This approach ensures WatermelonDB maintains its performance characteristics as it scales, especially on mobile devices where JavaScript execution can be a bottleneck.

### Adapter Pattern

WatermelonDB uses an adapter pattern to support multiple storage backends:
- **SQLite** - Primary adapter for React Native (native performance)
- **LokiJS** - Adapter for web/IndexedDB environments

### Sync Engine

The sync system (`src/sync/` and `native/shared/SyncEngine`) handles:
- Pulling changes from remote servers
- Pushing local changes
- Conflict resolution
- Incremental sync

### Reactive System

WatermelonDB uses observables to automatically update React components when data changes. The observation system (`src/observation/`) tracks queries and notifies subscribers when relevant data changes.

## Key Technologies

- **TypeScript/JavaScript** - Core implementation
- **SQLite** - Primary database engine
- **React Native** - Mobile platform support
- **C++** - Shared native code for performance-critical operations
- **Swift/Kotlin** - Platform-specific native bridges
- **RxJS** - Optional reactive programming support
- **mdBook** - Documentation generation

## Resources

- **Documentation**: https://nozbe.github.io/WatermelonDB/
- **GitHub**: https://github.com/BuildHero/WatermelonDB
- **Issues**: https://github.com/BuildHero/WatermelonDB/issues
- **Contributing Guide**: [CONTRIBUTING.md](./CONTRIBUTING.md)

---

**Remember**: Documentation is as important as code. Always update docs when adding or changing features!
