import type { IGuard, IActionGuard } from '@comunica/bus-guard';
import type { Quad } from '@comunica/incremental-types';
import { Transform } from 'readable-stream';
import type { ActorGuardPollingDiff } from './ActorGuardPollingDiff';

export class PollingDiffGuard implements IGuard {
  private static readonly regex = /max-age=(\d+)/u;

  private readonly action: IActionGuard;
  private readonly actorGuardPolling: ActorGuardPollingDiff;

  private flaggedForDeletion = false;
  private promiseId?: NodeJS.Timeout = undefined;

  public pollingFrequency = 0;

  public constructor(action: IActionGuard, actorGuardPolling: ActorGuardPollingDiff) {
    this.action = action;
    this.actorGuardPolling = actorGuardPolling;

    const maxAgeArray = PollingDiffGuard.regex.exec(action.metadata['cache-control']);

    if (maxAgeArray) {
      this.pollingFrequency = Number.parseInt(maxAgeArray[1], 10);
    } else {
      this.pollingFrequency = actorGuardPolling.pollingFrequency;
    }

    const age = Number.parseInt(action.metadata.age, 10);
    if (age) {
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      new Promise<void>(resolve => {
        this.promiseId = setTimeout(
          async() => {
            await this.checkForChanges();
            resolve();
          },
          (this.pollingFrequency - age) * 1_000,
        );
      }).then(() => {
        this.startCheckLoop(this.pollingFrequency);
      });
    } else {
      this.startCheckLoop(this.pollingFrequency);
    }
  }

  private startCheckLoop(maxAge: number): void {
    if (!this.flaggedForDeletion) {
      this.promiseId = setInterval(
        this.checkForChanges.bind(this),
        maxAge * 1_000,
      );
    }
  }

  private async checkForChanges(): Promise<void> {
    // TODO: is it better to do get instead of head
    const responseHead = await this.actorGuardPolling.mediatorHttp.mediate(
      {
        context: this.action.context,
        input: this.action.url,
        init: {
          method: 'HEAD',
        },
      },
    );

    if (responseHead.headers.get('etag') !== this.action.metadata.etag) {
      const store = this.action.streamingSource.store.copyOfStore();
      const matchStream = new Transform({
        transform(quad: Quad, _encoding, callback: (error?: (Error | null), data?: any) => void) {
          if (quad.diff !== undefined) {
            return callback(null, quad);
          }
          if (store.has(quad)) {
            store.delete(quad);
            return callback(null, null);
          }
          quad.diff = true;
          return callback(null, quad);
        },
        objectMode: true,
      });

      const responseGet = await this.actorGuardPolling.mediatorDereferenceRdf.mediate({
        context: this.action.context,
        url: this.action.url,
      });

      responseGet.data.on('end', () => {
        for (const quad of store) {
          (<Quad>quad).diff = false;
          matchStream.write(quad);
        }
        matchStream.end();
      });

      this.action.streamingSource.store.import(responseGet.data.pipe(matchStream, { end: false }));

      // TODO only add useful headers maybe use the extractor mediator
      responseGet.headers?.forEach((value, key) => {
        this.action.metadata[key] = value;
      });
    }
  }

  public delete(url: string): void {
    this.flaggedForDeletion = true;
    clearInterval(this.promiseId);
    // TODO delete source
  }
}
