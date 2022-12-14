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
exports.Factory = void 0;
class Factory {
    /**
     * The constructor of the Factory class.
     *
     * @param actor - The class of the actor function
     */
    constructor(actor) {
        this.map = new Map();
        this.actor = actor;
    }
    /**
     * The getKeyValuePairs() method executes a provided function once per each key/value pair in the Map object, in insertion order.
     *
     * @param cb - Function to execute for each entry in the map. It takes the following arguments: value (Value), key (Key), map (Map<Key, Value>)
     */
    getKeyValuePairs(cb) {
        this.map.forEach(cb);
    }
    /**
     * The getOrCreate() method returns a class based on the given Key. If no class with the given key exists the method will return a new class, otherwise it will return an existing class.
     *
     * @param key - The key.
     * @param actor? - Another initializer that extends the actor of this Factory.
     * @param params - possible additional params for the actor class.
     */
    getOrCreate(key, actor, ...params) {
        return __awaiter(this, void 0, void 0, function* () {
            let element = this.map.get(key);
            if (!element) {
                element = new this.actor(key, ...params);
                this.map.set(key, element);
            }
            return element;
        });
    }
    ;
    /**
     * The get() method returns an actor class if exists.
     *
     * @param key - The key of the actor class.
     */
    get(key) {
        return this.map.get(key);
    }
    /**
     *  The deleteActor() method safely deletes an actor from the factory and also deletes its instance.
     *
     *  @param key - The key of the actor class.
     */
    deleteActor(key) {
        const v = this.map.get(key);
        if (v) {
            v.delete();
        }
    }
    /**
     *  The removeActorFromFactory() method removes the actor from the factory but doesn't delete the instance. This method can cause memory overflow if not used correctly.
     *
     *  @param key - The key of the actor class.
     */
    removeActorFromFactory(key) {
        const v = this.map.get(key);
        if (v) {
            this.map.delete(key);
        }
    }
}
exports.Factory = Factory;
