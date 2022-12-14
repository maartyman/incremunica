import {Actor} from "../utils/actor-factory/actor";
import {Logger} from "tslog";
import {loggerSettings} from "../utils/loggerSettings";

export class Guard extends Actor<string> {
  protected readonly logger = new Logger(loggerSettings);

  private guardActive = new Map<string, boolean>();

  constructor(key: string) {
    super(key);
    this.setMaxListeners(Infinity);
  }

  protected setGuardActive(resource: string, value: boolean) {
    this.emit("guardActive", resource , value);
    this.guardActive.set(resource, value);
  }

  public isGuardActive(resource: string) {
    return this.guardActive.get(resource);
  }

  protected dataChanged(resource: string) {
    this.logger.debug("data has changed in resource: " + resource);
    this.emit("ResourceChanged", resource);
  }
}
