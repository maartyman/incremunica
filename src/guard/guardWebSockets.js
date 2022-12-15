"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.GuardWebSockets = void 0;
const guard_1 = require("./guard");
const websocket_1 = require("websocket");
class GuardWebSockets extends guard_1.Guard {
    constructor(host) {
        super(host);
        this.pubRegEx = new RegExp(/pub (https?:\/\/\S+)/);
        this.ackRegEx = new RegExp(/ack (https?:\/\/\S+)/);
        this.ws = new websocket_1.client();
        this.ws.setMaxListeners(Infinity);
        this.ws.on('connect', (connection) => {
            this.logger.debug("ws connected to pod");
            this.connection = connection;
            connection.on('error', (error) => {
                throw new Error("Guarding web socket failed: " + error.toString());
            });
            connection.on('close', () => {
                throw new Error("Guarding web socket closed");
            });
            connection.on('message', (message) => {
                if (message.type === 'utf8') {
                    const resources = this.pubRegEx.exec(message.utf8Data);
                    if (resources && resources[1]) {
                        this.dataChanged(resources[1].toString());
                        return;
                    }
                    const ack = this.ackRegEx.exec(message.utf8Data);
                    if (ack && ack[1]) {
                        this.setGuardActive(ack[1].toString(), true);
                    }
                }
            });
        });
        this.ws.connect(host, 'solid-0.1');
    }
    evaluateResource(resource) {
        this.logger.debug("evaluateResource: " + resource);
        if (this.connection) {
            this.connection.sendUTF('sub ' + resource);
        }
        else {
            this.ws.on('connect', (connection) => {
                connection.sendUTF('sub ' + resource);
            });
        }
    }
}
exports.GuardWebSockets = GuardWebSockets;
