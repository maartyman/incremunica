import type {
  IActorMergeBindingsContextOutput,
  IActorMergeBindingsContextArgs,
  IActionMergeBindingsContext,
} from '@comunica/bus-merge-bindings-context';
import {
  ActorMergeBindingsContext,
} from '@comunica/bus-merge-bindings-context';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';

/**
 * A incremunica actor for the creation of merge handlers for binding context keys.
 */
export class ActorMergeBindingsContextIsAddition extends ActorMergeBindingsContext {
  public constructor(args: IActorMergeBindingsContextArgs) {
    super(args);
  }

  public async test(_action: IActionMergeBindingsContext): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(_action: IActionMergeBindingsContext): Promise<IActorMergeBindingsContextOutput> {
    const handlerFunc = (...args: boolean[]): boolean => args.reduce((acc, cur) => acc && cur);
    return {
      mergeHandlers: {
        '@incremunica/bindings:isAddition': {
          run: handlerFunc,
        },
      },
    };
  }
}
