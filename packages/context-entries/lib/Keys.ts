import { ActionContextKey } from '@comunica/core';
import type { ISourceWatchEventEmitter } from '@incremunica/bus-source-watch';
import type { IDetermineChangesEvents } from '@incremunica/types';

/**
 * When adding entries to this file, also add a shortcut for them in the contextKeyShortcuts TSDoc comment in
 * ActorIniQueryBase in @comunica/actor-init-query if it makes sense to use this entry externally.
 * Also, add this shortcut to IQueryContextCommon in @comunica/types.
 */

export const KeysDetermineChanges = {
  /**
   * Events sent by the determine-changes actor.
   */
  events: new ActionContextKey<IDetermineChangesEvents>('@incremunica/determine-changes:events'),
};

export const KeysStreamingSource = {
  matchOptions: new ActionContextKey<({ closeStream: () => void })[]>('@incremunica/streaming-source:matchOptions'),
};

export const KeysBindings = {
  isAddition: new ActionContextKey<boolean>('@incremunica/bindings:isAddition'),
};

export const KeysSourceWatch = {
  pollingPeriod: new ActionContextKey<number>('@incremunica/source-watch:pollingPeriod'),
  deferredEvaluationEventEmitter:
    new ActionContextKey<ISourceWatchEventEmitter>('@incremunica/resource-watch:deferredEvaluationEventEmitter'),
};
