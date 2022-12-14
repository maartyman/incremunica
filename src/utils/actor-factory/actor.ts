/**
 * @class
 * The Actor class hold links to the parent and child classes and ensures the instances are removed when delete is called.
 */
import {EventEmitter} from "events";
import {Factory} from "./factory";

export class Actor<KeyType> extends EventEmitter {
  static factory = new Factory<any, any>(Actor);

  /**
   * The unique key for the instance.
   */
  public readonly key: KeyType;

  /**
   * The constructor to make the actor class.
   *
   * @param key - The unique key.
   */
  constructor(key: KeyType, ...args: any) {
    super();
    this.key = key;
  }

  /**
   * Deletes an actor from memory. This method will fully delete the instance for all other actors.
   */
  public delete() {
    this.removeAllListeners();
    (<typeof Actor>this.constructor).factory.removeActorFromFactory(this.key);
  }
}
