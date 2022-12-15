"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.GuardFactory = void 0;
const factory_1 = require("../utils/actor-factory/factory");
const tslog_1 = require("tslog");
const loggerSettings_1 = require("../utils/loggerSettings");
const guardPolling_1 = require("./guardPolling");
const guardWebSockets_1 = require("./guardWebSockets");
const guard_1 = require("./guard");
const websocket_1 = require("websocket");
class GuardFactory extends factory_1.Factory {
    constructor() {
        super(guard_1.Guard);
        this.guardWebSockets = new Map();
    }
    getOrCreate(key, actor, headers) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!headers) {
                throw new Error("the field headers was not specified (this shouldn't happen)");
            }
            //check if there is already a guard, if so return it
            let guard = this.map.get(key);
            if (guard) {
                return guard;
            }
            //check if it has an updates-via field in the header, if not return a new polling guard
            let webSocketHost = headers["updates-via"];
            new tslog_1.Logger(loggerSettings_1.loggerSettings).debug(webSocketHost);
            if (!webSocketHost) {
                new tslog_1.Logger(loggerSettings_1.loggerSettings).debug("Polling guard");
                guard = new guardPolling_1.GuardPolling(key);
                this.map.set(key, guard);
                return guard;
            }
            //return a websocket guard
            guard = this.guardWebSockets.get(webSocketHost);
            if (!guard) {
                new tslog_1.Logger(loggerSettings_1.loggerSettings).debug("Websocket host doesn't exist yet: ", webSocketHost);
                guard = new guardWebSockets_1.GuardWebSockets(webSocketHost);
                this.guardWebSockets.set(webSocketHost, guard);
            }
            new tslog_1.Logger(loggerSettings_1.loggerSettings).debug("Evaluating resource:", key);
            guard.evaluateResource(key);
            this.map.set(key, guard);
            return guard;
        });
    }
    /*
    public async addGuard(resource: Resource): Promise<Guard> {
      const url = new URL(resource.resourceName);
  
      let guard = this.guards.get(resource.resourceName);
  
      if (!guard) {
        if (await this.checkWebSocketAvailability(url.host)) {
          guard = new GuardWebSockets(resource.resourceName);
        } else {
          guard =new GuardPolling();
        }
        this.guards.set(url.host, guard);
      }
  
      guard.evaluateResource(resourceName, aggregatorResource);
      return guard;
    }
  
     */
    changeGuardType(currentGuard, guardType) {
        return __awaiter(this, void 0, void 0, function* () {
            if (typeof currentGuard === guardType) {
                return;
            }
            if (guardType === typeof guardPolling_1.GuardPolling) {
                const newGuard = new guardPolling_1.GuardPolling(currentGuard.key);
                //TODO if ready link it to the resource and delete the polling resource
            }
            else if (guardType === typeof guardWebSockets_1.GuardWebSockets) {
                const newGuard = new guardWebSockets_1.GuardWebSockets(currentGuard.key);
                //TODO if ready link it to the resource and delete the polling resource
            }
            else {
                new tslog_1.Logger(loggerSettings_1.loggerSettings).warn("Guardtype: " + guardType + " doesn't exist");
            }
        });
    }
    checkWebSocketAvailability(webSocketHost) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve) => {
                const ws = new websocket_1.client();
                ws.on('connect', (connection) => {
                    new tslog_1.Logger(loggerSettings_1.loggerSettings).debug("Checking pod availability: connection succeeded");
                    ws.abort();
                    resolve(webSocketHost);
                    return;
                });
                ws.on("connectFailed", () => {
                    new tslog_1.Logger(loggerSettings_1.loggerSettings).debug("Checking pod availability: connection failed");
                    ws.abort();
                    resolve(undefined);
                    return;
                });
                setTimeout(() => {
                    ws.abort();
                    resolve(undefined);
                    return;
                }, 30000);
                ws.connect(webSocketHost, 'solid-0.1');
            });
        });
    }
}
exports.GuardFactory = GuardFactory;
