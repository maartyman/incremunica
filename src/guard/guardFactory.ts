import {Factory} from "../utils/actor-factory/factory";
import {Logger} from "tslog";
import {loggerSettings} from "../utils/loggerSettings";
import {GuardPolling} from "./guardPolling";
import {GuardWebSockets} from "./guardWebSockets";
import {Guard} from "./guard";
import {connection} from "websocket";

export class GuardFactory extends Factory<string, Guard> {
  constructor() {
    super(Guard);
  }

  public async getOrCreate(key: string, actor?: new (key: string) => Guard) {
    const url = new URL(key);

    const guardPolling = this.map.get(key);

    if (guardPolling) {
      return guardPolling;
    }

    const guardWebSocket = this.map.get(url.host) as GuardWebSockets;

    if (guardWebSocket) {
      guardWebSocket.evaluateResource(key);
      return guardWebSocket;
    }

    let guard: Guard;

    if (await this.checkWebSocketAvailability(url.host)) {
      const guardWebSocket = new GuardWebSockets(url.host);
      guardWebSocket.evaluateResource(key);
      guard = guardWebSocket;
    } else {
      guard = new GuardPolling(key);
    }

    return guard;
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

  public async changeGuardType(currentGuard: Guard, guardType: string) {
    if (typeof currentGuard === guardType) {
      return;
    }
    if (guardType === typeof GuardPolling) {
      const newGuard = new GuardPolling(currentGuard.key);
      //TODO if ready link it to the resource and delete the polling resource
    }
    else if (guardType === typeof GuardWebSockets) {
      const newGuard = new GuardWebSockets(currentGuard.key);
      //TODO if ready link it to the resource and delete the polling resource
    }
    else {
      new Logger(loggerSettings).warn("Guardtype: " + guardType + " doesn't exist");
    }
  }

  private async checkWebSocketAvailability(host: string): Promise<boolean> {
    //TODO is this the best solution?
    new Logger(loggerSettings).debug("Checking pod availability");
    const WebSocketClient = require('websocket').client;
    const ws = new WebSocketClient();

    const promise = new Promise<boolean>( resolve => {
      ws.on('connect', (connection: connection) => {
        new Logger(loggerSettings).debug("Checking pod availability: connection succeeded");
        ws.abort();
        resolve(true);
      });

      ws.on("connectFailed", () => {
        new Logger(loggerSettings).debug("Checking pod availability: connection failed");
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
  }
}
