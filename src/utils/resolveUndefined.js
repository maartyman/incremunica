"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.resolveUndefined = void 0;
function resolveUndefined(val, defaultValue) {
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
exports.resolveUndefined = resolveUndefined;
