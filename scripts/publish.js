#!/usr/bin/env node
/* eslint-disable no-console */

// inspired by `np` – https://github.com/sindresorhus/np

const Listr = require('listr')
const listrInput = require('listr-input')
const execa = require('execa')
const timeout = require('p-timeout')
const inquirer = require('inquirer')
const semver = require('semver')

const { when, includes, flip, both, add } = require('rambdax')

const pkg = require('../package.json')

const flippedIncludes = flip(includes)
const increments = ['patch', 'minor', 'major', 'prepatch', 'preminor', 'premajor', 'prerelease']
// const prerelease = ['prepatch', 'preminor', 'premajor', 'prerelease']

const belongsToIncrements = flippedIncludes(increments)
const isValidVersion = input => Boolean(semver.valid(input))
const isVersionGreater = input => semver.gt(input, pkg.version)
const getNewVersion = input => semver.inc(pkg.version, input)
const isValidAndGreaterVersion = both(isValidVersion, isVersionGreater)

const throwError = str => info => {
  throw new Error(str, JSON.stringify(info))
}

const questions = [
  {
    type: 'list',
    name: 'version',
    message: `Specify new version (current version: ${pkg.version}):`,
    pageSize: add(increments.length, 4),
    choices: increments
      .map(inc => ({
        name: `${inc} 	${semver.inc(pkg.version, inc)}`,
        value: inc,
      }))
      .concat([
        new inquirer.Separator(),
        {
          name: 'Other (specify)',
          value: null,
        },
      ]),
    filter: input => (belongsToIncrements(input) ? getNewVersion(input) : input),
  },
  {
    type: 'input',
    name: 'version',
    message: 'Version:',
    when: answers => !answers.version,
    validate: input => isValidAndGreaterVersion(input),
  },
]

const buildTasks = options => {
  const { version } = options

  const isPrerelease = includes('-', version)
  const tag = isPrerelease ? 'next' : 'latest'

  return [
    ...(isPrerelease
      ? [
          {
            title: 'WARN: Skipping git checks',
            task: () => {},
          },
        ]
      : []),
    {
      title: 'check tests',
      task: () => execa('yarn', ['test']),
    },
    {
      title: 'check TypeScript types',
      task: () => execa('yarn', ['typecheck']),
    },
    {
      title: 'bump version',
      task: () => execa('yarn', ['version', '--new-version', version]),
    },
    {
      title: 'build package',
      task: () => execa('yarn', ['build']),
    },
    {
      title: 'publish package',
      task: () =>
        execa('yarn', ['publish', '--tag', tag, '--network-timeout', 600000], { cwd: './dist' }),
    },
    {
      title: 'push and tag',
      task: () => execa('git', ['push', 'origin', `v${version}`]),
    },
  ]
}

inquirer.prompt(questions).then(options => {
  const tasks = buildTasks(options)
  const listr = new Listr(tasks)
  listr.run()
})
