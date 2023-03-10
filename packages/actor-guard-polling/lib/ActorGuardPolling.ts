import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IActionGuard, IActorGuardOutput, IActorGuardArgs } from '@comunica/bus-guard';
import { ActorGuard } from '@comunica/bus-guard';
import type { MediatorHttp } from '@comunica/bus-http';
import type { IActorTest } from '@comunica/core';
import type { Quad } from '@comunica/types/lib/Quad';
import { Transform } from 'readable-stream';

/**
 * A comunica Polling Guard Actor.
 */
export class ActorGuardPolling extends ActorGuard {
  public readonly mediatorHttp: MediatorHttp;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;

  public constructor(args: IActorGuardPollingArgs) {
    super(args);
  }

  public async test(action: IActionGuard): Promise<IActorTest> {
    // TODO implement
    return { filterFactor: 1 };
  }

  public async run(action: IActionGuard): Promise<IActorGuardOutput> {
    const regex = /max-age=(\d+)/gu;

    const maxAgeArray = regex.exec(action.streamSource.source.metadata['cache-control']);

    let maxAge: number;
    if (maxAgeArray) {
      maxAge = Number.parseInt(maxAgeArray[1], 10);
    } else {
      maxAge = 10;
    }
    maxAge = 5;

    const age = Number.parseInt(action.streamSource.source.metadata.age, 10);
    if (age) {
      await new Promise<void>(resolve => {
        setTimeout(
          () => {
            resolve();
            this.checkForChanges(action);
          },
          (maxAge - age) * 1_000,
        );
      });
    }
    setInterval(
      this.checkForChanges.bind(this),
      maxAge * 1_000,
      action,
    );

    //
    // const dereferenceRdfOutput: IActorDereferenceRdfOutput = await this.mediatorDereferenceRdf
    // .mediate({ context, url });
    // url = dereferenceRdfOutput.url;
    //
    // test:
    //
    // setTimeout(() => {
    // action.streamSource.store.attachStream(streamifyArray([quad("<http://test.com/s_laat>", "<http://test.com/p_laat>", "<http://test.com/o_laat>")]))
    // }, 10000);
    //

    return {};
  }

  private async checkForChanges(action: IActionGuard) {
    // TODO: is it better to do get instead of head
    const response = await this.mediatorHttp.mediate(
      {
        context: action.context,
        input: action.streamSource.source.url,
        init: {
          method: 'HEAD',
        },
      },
    );

    if (response.headers.get('etag') !== action.streamSource.source.metadata.etag) {
      const store = action.streamSource.store.copyOfStore();
      const matchStream = new Transform({
        transform(quad: Quad, encoding: BufferEncoding, callback: (error?: (Error | null), data?: any) => void) {
          if (quad.diff != undefined) {
            callback(null, quad);
            return;
          }
          if (store.has(quad)) {
            store.delete(quad);
            callback(null, null);
          } else {
            quad.diff = true;
            callback(null, quad);
          }
        },
        objectMode: true,
      });

      const response = await this.mediatorDereferenceRdf.mediate({
        context: action.context,
        url: action.streamSource.source.url,
      });

      response.data.on('end', () => {
        for (const quad of store) {
          (<Quad>quad).diff = false;
          matchStream.write(quad);
        }
        matchStream.end();
      });

      action.streamSource.store.attachStream(response.data.pipe(matchStream, { end: false }));

      response.headers?.forEach((value, key) => {
        action.streamSource.source.metadata[key] = value;
      });
    }
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
  mediatorDereferenceRdf: MediatorDereferenceRdf;
}
