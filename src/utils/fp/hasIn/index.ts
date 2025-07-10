// inspired by ramda and rambda
/* eslint-disable */

export default function hasIn<T extends object>(prop: keyof any): (obj: T) => boolean
export default function hasIn<T extends object>(prop: keyof any, obj: T): boolean
export default function hasIn<T extends object>(prop: keyof any, obj?: T): any {
  if (obj === undefined) {
    return function(obj: T): boolean {
      return hasIn(prop, obj)
    }
  }

  return prop in obj
}
