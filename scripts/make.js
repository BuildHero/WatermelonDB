#!/usr/bin/env node

const {
  pipe,
  filter,
  map,
  mapAsync,
  endsWith,
  both,
  prop,
  replace,
  omit,
  merge,
  forEach,
} = require('rambdax')

const babel = require('@babel/core')
const klaw = require('klaw-sync')
const mkdirp = require('mkdirp')
const path = require('path')
const fs = require('fs-extra')
const glob = require('glob')
const prettyJson = require('json-stringify-pretty-compact')
const chokidar = require('chokidar')
const anymatch = require('anymatch')
const rimraf = require('rimraf')
const { execSync } = require('child_process')

const pkg = require('../package.json')

const resolvePath = (...paths) => path.resolve(__dirname, '..', ...paths)
const isDevelopment = process.env.NODE_ENV === 'development'

const SRC_MODULES = 'src'
const CJS_MODULES = 'cjs'

const SOURCE_PATH = resolvePath('src')
const DIST_PATH = resolvePath('dist')
const DEV_PATH = process.env.DEV_PATH || resolvePath('dev')

const DIR_PATH = isDevelopment ? DEV_PATH : DIST_PATH

const DO_NOT_BUILD_PATHS = [
  /__tests__/,
  /adapters\/__tests__/,
  /test\.(js|ts|tsx)$/,
  /integrationTest/,
  /__mocks__/,
  /\.DS_Store/,
  /package\.json/,
]

const isNotIncludedInBuildPaths = value => !anymatch(DO_NOT_BUILD_PATHS, value)

const cleanFolder = dir => rimraf.sync(dir)

const takeFiles = pipe(
  prop('path'),
  both(file => file.match(/\.(js|jsx|ts|tsx)$/), isNotIncludedInBuildPaths),
)

const takeModules = pipe(
  filter(takeFiles),
  map(prop('path')),
)

const removeSourcePath = replace(SOURCE_PATH, '')

const createModulePath = format => {
  const formatPathSegment = format === CJS_MODULES ? [] : [format]
  const modulePath = resolvePath(DIR_PATH, ...formatPathSegment)
  return replace(SOURCE_PATH, modulePath)
}

const createFolder = dir => mkdirp.sync(resolvePath(dir))

const getBabelConfig = file => {
  const isTypeScript = file.match(/\.(ts|tsx)$/)
  return {
    presets: [...(isTypeScript ? ['@babel/preset-typescript'] : [])],
    filename: file, // This helps Babel detect the correct file type
  }
}

const babelTransform = (format, file) => {
  if (format === SRC_MODULES) {
    // no transform, just return source
    return fs.readFileSync(file)
  }

  const { code } = babel.transformFileSync(file, getBabelConfig(file))
  return code
}

const compileTypeScript = () => {
  // Only run tsc if we have TypeScript files
  const hasTypeScriptFiles = glob.sync(`${SOURCE_PATH}/**/*.{ts,tsx}`).length > 0
  if (hasTypeScriptFiles) {
    try {
      execSync('tsc --emitDeclarationOnly', { stdio: 'inherit' })
    } catch (error) {
      console.error('TypeScript compilation failed:', error)
      process.exit(1)
    }
  }
}

const paths = klaw(SOURCE_PATH)
const modules = takeModules(paths)

const buildModule = format => file => {
  const modulePath = createModulePath(format)
  const code = babelTransform(format, file)
  const filename = modulePath(file).replace(/\.(ts|tsx)$/, '.js')

  createFolder(path.dirname(filename))
  fs.writeFileSync(filename, code)
}

const prepareJson = pipe(
  omit(['scripts']),
  merge({
    main: './index.js',
    types: './index.d.ts',
    sideEffects: false,
  }),
  obj => prettyJson(obj),
)

const createPackageJson = (dir, obj) => {
  const json = prepareJson(obj)
  fs.writeFileSync(resolvePath(dir, 'package.json'), json)
}

const copyFiles = (dir, files, rm = resolvePath()) =>
  forEach(file => {
    fs.copySync(file, path.join(dir, replace(rm, '', file)))
  }, files)

const copyNonJavaScriptFiles = buildPath => {
  createPackageJson(buildPath, pkg)
  copyFiles(buildPath, [
    'LICENSE',
    'README.md',
    'yarn.lock',
    'WatermelonDB.podspec',
    'docs',
    'native/shared',
    'native/ios',
    'native/android',
    'native/android-jsi',
  ])
  cleanFolder(`${buildPath}/native/android/build`)
  cleanFolder(`${buildPath}/native/android/bin/build`)
  cleanFolder(`${buildPath}/native/android-jsi/build`)
  cleanFolder(`${buildPath}/native/android-jsi/bin/build`)
}

if (isDevelopment) {
  const buildCjsModule = buildModule(CJS_MODULES)
  const buildSrcModule = buildModule(SRC_MODULES)

  const buildFile = file => {
    if (file.match(/\.(js|jsx|ts|tsx)$/)) {
      buildSrcModule(file)
      buildCjsModule(file)
      if (file.match(/\.(ts|tsx)$/)) {
        compileTypeScript()
      }
    } else {
      // native files
      fs.copySync(file, path.join(DEV_PATH, replace(resolvePath(), '', file)))
    }
  }

  cleanFolder(DEV_PATH)
  createFolder(DEV_PATH)
  copyNonJavaScriptFiles(DEV_PATH)

  chokidar
    .watch(
      [
        resolvePath('src'),
        resolvePath('native/ios/WatermelonDB'),
        resolvePath('native/shared'),
        resolvePath('native/android/src/main'),
        resolvePath('native/android-jsi/src/main'),
      ],
      {
        ignored: DO_NOT_BUILD_PATHS,
      },
    )
    .on('all', (event, fileOrDir) => {
      // eslint-disable-next-line
      switch (event) {
        case 'add':
        case 'change':
          // eslint-disable-next-line
          console.log(`✓ ${removeSourcePath(fileOrDir)}`)
          buildFile(fileOrDir)
          break
        default:
          break
      }
    })
} else {
  const buildModules = format => mapAsync(buildModule(format))
  const buildCjsModules = buildModules(CJS_MODULES)
  const buildSrcModules = buildModules(SRC_MODULES)

  cleanFolder(DIST_PATH)
  createFolder(DIST_PATH)
  copyNonJavaScriptFiles(DIST_PATH)

  buildSrcModules(modules)
  buildCjsModules(modules)
  compileTypeScript()

  // copy typescript definitions
  glob(`${SOURCE_PATH}/**/*.d.ts`, {}, (err, files) => {
    if (err) {
      console.error('Error copying TypeScript definitions:', err)
      return
    }
    copyFiles(DIST_PATH, files, SOURCE_PATH)
  })
}
