import {Factory} from "../utils/actor-factory/factory";
import {Logger} from "tslog";
import {loggerSettings} from "../utils/loggerSettings";
import {GuardPolling} from "./guardPolling";
import {GuardWebSockets} from "./guardWebSockets";
import {Guard} from "./guard";
import {connection, client} from "websocket";

export class GuardFactory extends Factory<string, Guard> {
  private guardWebSockets = new Map<string, GuardWebSockets>();

  constructor() {
    super(Guard);
  }

  public async getOrCreate(key: string, actor?: new (key: string) => Guard, headers?: any) {
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

    new Logger(loggerSettings).debug(webSocketHost);

    if (!webSocketHost) {
      new Logger(loggerSettings).debug("Polling guard");
      guard = new GuardPolling(key);
      this.map.set(key, guard);
      return guard;
    }

    //return a websocket guard
    guard = this.guardWebSockets.get(webSocketHost);

    if (!guard) {
      new Logger(loggerSettings).debug("Websocket host doesn't exist yet: ", webSocketHost);
      guard = new GuardWebSockets(webSocketHost);
      this.guardWebSockets.set(webSocketHost, (guard as GuardWebSockets));
    }

    new Logger(loggerSettings).debug("Evaluating resource:", key);
    (guard as GuardWebSockets).evaluateResource(key);

    this.map.set(key, guard);

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

  private async checkWebSocketAvailability(webSocketHost: string): Promise<string | undefined> {
    return new Promise<string | undefined>((resolve) => {
      const ws = new client();

      ws.on('connect', (connection: connection) => {
        new Logger(loggerSettings).debug("Checking pod availability: connection succeeded");
        ws.abort();
        resolve(webSocketHost);
        return;
      });

      ws.on("connectFailed", () => {
        new Logger(loggerSettings).debug("Checking pod availability: connection failed");
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
    })
  }
}

