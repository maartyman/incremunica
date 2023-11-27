import { ActionContextKey } from '@comunica/core';
import type { IGuardEvents } from '@incremunica/incremental-types';

/**
 * When adding entries to this file, also add a shortcut for them in the contextKeyShortcuts TSDoc comment in
 * ActorIniQueryBase in @comunica/actor-init-query if it makes sense to use this entry externally.
 * Also, add this shortcut to IQueryContextCommon in @comunica/types.
 */

export const KeysGuard = {
  /**
   * Events sent by the guard.
   */
  events: new ActionContextKey<IGuardEvents>('@incremunica/guard:events'),
};

export const KeysStreamingSource = {
  matchOptions: new ActionContextKey<({ stopMatch: () => void })[]>('@incremunica/streaming-source:matchOptions'),
};

export const KeysDeltaQueryJoin = {
  fromDeltaQuery: new ActionContextKey('@incremunica/delta-query-join:fromDeltaQuery'),
};
