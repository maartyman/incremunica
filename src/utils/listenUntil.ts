import * as events from "events";

export function listenUntil(eventEmitter: events.EventEmitter, eventName: string, message: string, cb: ()=>void) {
  eventEmitter.once(eventName, (value: string) => {
    if (value === message) {
      cb()
    } else {
      listenUntil(eventEmitter, eventName, message, cb);
    }
  });
}
