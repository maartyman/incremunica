"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.listenUntil = void 0;
function listenUntil(eventEmitter, eventName, message, cb) {
    eventEmitter.once(eventName, (value) => {
        if (value === message) {
            cb();
        }
        else {
            listenUntil(eventEmitter, eventName, message, cb);
        }
    });
}
exports.listenUntil = listenUntil;
