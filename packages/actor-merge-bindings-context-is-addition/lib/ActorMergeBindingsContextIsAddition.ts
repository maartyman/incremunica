import type { IActorTest } from '@comunica/core';
import {
  ActorMergeBindingsContext,
  IActorMergeBindingsContextOutput,
  IActorMergeBindingsContextArgs, IActionMergeBindingsContext
} from '@comunica/bus-merge-bindings-context';
import type { IActionContextKey } from '@comunica/types';

/**
 * A incremunica actor for the creation of merge handlers for binding context keys.
 */
export class ActorMergeBindingsContextIsAddition extends ActorMergeBindingsContext {

  public constructor(args: IActorMergeBindingsContextArgs) {
    super(args);
  }

  public async test(_action: IActionMergeBindingsContext): Promise<IActorTest> {
    return true;
  }

  public async run(_action: IActionMergeBindingsContext): Promise<IActorMergeBindingsContextOutput> {
    //TODO change to boolean[] => boolean when comuncia V4
    const handlerFunc: (...args: any[]) => any = (...args: boolean[]): boolean => args.reduce((acc, cur) => acc && cur);
    return {
      mergeHandlers: {
        "isAddition": {
          run: handlerFunc
        }
      }
    };
  }
}

export class ActionContextKeyIsAddition implements IActionContextKey<boolean> {
  readonly name = 'isAddition';
}
