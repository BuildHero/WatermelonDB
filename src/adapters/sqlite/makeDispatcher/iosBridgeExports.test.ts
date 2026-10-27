import fs from 'fs'
import path from 'path'

const iosDir = path.join(__dirname, '../../../../native/ios/WatermelonDB')

const MACROS = [
  'WMELON_BRIDGE_METHOD',
  'RCT_EXTERN_METHOD',
  'RCT_EXTERN__BLOCKING_SYNCHRONOUS_METHOD',
]

const invocations = (source: string): Array<[string, string]> => {
  const found: Array<[string, string]> = []
  const pattern = new RegExp(`\\b(${MACROS.join('|')})\\(`, 'g')
  for (const match of source.matchAll(pattern)) {
    let depth = 1
    let end = match.index! + match[0].length
    while (depth > 0) {
      if (source[end] === '(') depth += 1
      if (source[end] === ')') depth -= 1
      end += 1
    }
    found.push([match[1], source.slice(match.index! + match[0].length, end - 1)])
  }
  return found
}

const labels = (args: string) => Array.from(args.matchAll(/(\w+):\s*\(/g), (m) => `${m[1]}:`)

// The selectors DatabaseBridge.m hands React Native; a Swift @objc method missing here is undefined in JS.
const exportedSelectors = (): Set<string> => {
  const source = fs
    .readFileSync(path.join(iosDir, 'DatabaseBridge.m'), 'utf8')
    .replace(/#define(?:[^\n]*\\\n)*[^\n]*/g, '')
    .replace(/WMELON_CONCAT\((\w+), (\w+)\)/g, '$1$2')
  const selectors = new Set<string>()
  for (const [macro, body] of invocations(source)) {
    if (macro === 'WMELON_BRIDGE_METHOD') {
      const [name, args] = [body.slice(0, body.indexOf(',')), body.slice(body.indexOf(',') + 1)]
      const argLabels = labels(args).join('')
      selectors.add(`${name}:${argLabels}resolve:reject:`)
      selectors.add(`${name}Synchronous:${argLabels}`)
    } else {
      selectors.add(labels(body).join(''))
    }
  }
  return selectors
}

const swiftSelectors = (): Set<string> => {
  const source = fs.readFileSync(path.join(iosDir, 'DatabaseBridge.swift'), 'utf8')
  return new Set(Array.from(source.matchAll(/@objc\((\w+:[\w:]*)\)/g), (m) => m[1]))
}

describe('iOS DatabaseBridge exports', () => {
  it('exports every selector the Swift bridge implements', () => {
    const exported = exportedSelectors()
    expect(Array.from(swiftSelectors()).filter((s) => !exported.has(s))).toEqual([])
  })

  it('implements every selector the bridge exports', () => {
    const implemented = swiftSelectors()
    expect(Array.from(exportedSelectors()).filter((s) => !implemented.has(s))).toEqual([])
  })
})
