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
const loggerSettings_1 = require("../../utils/loggerSettings");
const guardPolling_1 = require("./guardPolling");
const guardWebSockets_1 = require("./guardWebSockets");
const guard_1 = require("./guard");
class GuardFactory extends factory_1.Factory {
    constructor() {
        super(guard_1.Guard);
    }
    getOrCreate(key, actor) {
        return __awaiter(this, void 0, void 0, function* () {
            const url = new URL(key);
            const guardPolling = this.map.get(key);
            if (guardPolling) {
                return guardPolling;
            }
            const guardWebSocket = this.map.get(url.host);
            if (guardWebSocket) {
                guardWebSocket.evaluateResource(key);
                return guardWebSocket;
            }
            let guard;
            if (yield this.checkWebSocketAvailability(url.host)) {
                const guardWebSocket = new guardWebSockets_1.GuardWebSockets(url.host);
                guardWebSocket.evaluateResource(key);
                guard = guardWebSocket;
            }
            else {
                guard = new guardPolling_1.GuardPolling(key);
            }
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
    checkWebSocketAvailability(host) {
        return __awaiter(this, void 0, void 0, function* () {
            //TODO is this the best solution?
            new tslog_1.Logger(loggerSettings_1.loggerSettings).debug("Checking pod availability");
            const WebSocketClient = require('websocket').client;
            const ws = new WebSocketClient();
            const promise = new Promise(resolve => {
                ws.on('connect', (connection) => {
                    new tslog_1.Logger(loggerSettings_1.loggerSettings).debug("Checking pod availability: connection succeeded");
                    ws.abort();
                    resolve(true);
                });
                ws.on("connectFailed", () => {
                    new tslog_1.Logger(loggerSettings_1.loggerSettings).debug("Checking pod availability: connection failed");
                    ws.abort();
                    resolve(false);
                });
                setTimeout(() => {
                    ws.abort();
                    resolve(false);
                }, 30000);
            });
            ws.connect("ws://" + host, 'solid-0.1');
            return promise;
        });
    }
}
exports.GuardFactory = GuardFactory;
