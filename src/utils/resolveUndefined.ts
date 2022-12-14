export function resolveUndefined<T>(val: T | undefined, defaultValue?: T) : T {
  if (val == undefined) {
    if (defaultValue == undefined) {
      throw new Error('value undefined');
    }
    else {
      return defaultValue;
    }
  }
  return val;
}
