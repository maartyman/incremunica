import {IDataSource} from "@comunica/types";

export function arrayEquality(array1: [IDataSource, ...IDataSource[]], array2: [IDataSource, ...IDataSource[]]): boolean {
  for (const element1 of array1) {
    let equal = false;
    for (const element2 of array2) {
      if (element1 === element2) {
        equal = true;
        break;
      }
    }
    if (!equal) {
      return false;
    }
  }
  for (const element2 of array2) {
    let equal = false;
    for (const element1 of array1) {
      if (element1 === element2) {
        equal = true;
        break;
      }
    }
    if (!equal) {
      return false;
    }
  }
  return true;
}
