import {ActorGuard, IActionGuard, IActorGuardOutput, IActorGuardArgs} from '@comunica/bus-guard';
import { IActorTest } from '@comunica/core';
import {MediatorHttp} from "@comunica/bus-http";
import {MediatorDereference} from "@comunica/bus-dereference";

/**
 * A comunica Polling Guard Actor.
 */
export class ActorGuardPolling extends ActorGuard {
  public readonly mediatorHttp: MediatorHttp;
  public readonly mediatorDereference: MediatorDereference;

  public constructor(args: IActorGuardPollingArgs) {
    super(args);
  }

  public async test(action: IActionGuard): Promise<IActorTest> {
    return { filterFactor: 1 }; // TODO implement
  }

  public async run(action: IActionGuard): Promise<IActorGuardOutput> {
    let regex = new RegExp(/max-age=(\d+)/, "g");

    let maxAgeArray = regex.exec(action.streamSource.source.metadata["cache-control"]);

    let maxAge: number;
    if (maxAgeArray) {
      maxAge = Number.parseInt(maxAgeArray[1]);
    }
    else {
      maxAge = 10;
    }
    maxAge = 1;

    let age = Number.parseInt(action.streamSource.source.metadata["age"]);
    if (age) {
      await new Promise<void>(resolve => {
        setTimeout(
          () => {
            resolve();
            this.checkForChanges(action)
          },
          (maxAge - age)*1000
        );
      });
    }
    setInterval(
      this.checkForChanges,
      maxAge*1000,
      action
    );

    /*
    const dereferenceRdfOutput: IActorDereferenceRdfOutput = await this.mediatorDereferenceRdf
      .mediate({ context, url });
    url = dereferenceRdfOutput.url;
     */
    //test:
    /*
    setTimeout(() => {
      action.streamSource.store.attachStream(streamifyArray([quad("<http://test.com/s_laat>", "<http://test.com/p_laat>", "<http://test.com/o_laat>")]))
    }, 10000);
     */

    return {};
  }

  private async checkForChanges(action: IActionGuard) {
    let response = await this.mediatorHttp.mediate(
      {
        context: action.context,
        input: action.streamSource.source.url,
        init: {
          method: "HEAD"
        }
      }
    );

    console.log("old: ", action.streamSource.source.metadata["etag"]);
    console.log("new: ", response.headers.get("etag"));
  }
}

export interface IActorGuardPollingArgs extends IActorGuardArgs {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
  /**
   * The Dereference mediator
   */
  mediatorDereference: MediatorDereference;
}
