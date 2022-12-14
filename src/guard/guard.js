"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Guard = void 0;
const actor_1 = require("../utils/actor-factory/actor");
const tslog_1 = require("tslog");
const loggerSettings_1 = require("../../utils/loggerSettings");
class Guard extends actor_1.Actor {
    constructor(key) {
        super(key);
        this.logger = new tslog_1.Logger(loggerSettings_1.loggerSettings);
        this.guardActive = new Map();
        this.setMaxListeners(Infinity);
    }
    setGuardActive(resource, value) {
        this.emit("guardActive", resource, value);
        this.guardActive.set(resource, value);
    }
    isGuardActive(resource) {
        return this.guardActive.get(resource);
    }
    dataChanged(resource) {
        this.logger.debug("data has changed in resource: " + resource);
        this.emit("ResourceChanged", resource);
    }
}
exports.Guard = Guard;
