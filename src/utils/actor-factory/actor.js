"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Actor = void 0;
/**
 * @class
 * The Actor class hold links to the parent and child classes and ensures the instances are removed when delete is called.
 */
const events_1 = require("events");
const factory_1 = require("./factory");
class Actor extends events_1.EventEmitter {
    /**
     * The constructor to make the actor class.
     *
     * @param key - The unique key.
     */
    constructor(key, ...args) {
        super();
        this.key = key;
    }
    /**
     * Deletes an actor from memory. This method will fully delete the instance for all other actors.
     */
    delete() {
        this.removeAllListeners();
        this.constructor.factory.removeActorFromFactory(this.key);
    }
}
exports.Actor = Actor;
Actor.factory = new factory_1.Factory(Actor);
