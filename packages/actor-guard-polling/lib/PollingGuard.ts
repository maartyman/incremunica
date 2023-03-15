import {Guard, IActionGuard} from "@comunica/bus-guard";
import {Transform} from "readable-stream";
import {Quad} from "@comunica/types/lib/Quad";
import {ActorGuardPolling} from "./ActorGuardPolling";

export class PollingGuard implements Guard{
  private static regex = /max-age=(\d+)/gu;

  private readonly action: IActionGuard;
  private readonly actorGuardPolling: ActorGuardPolling;

  private flaggedForDeletion = false;
  private promiseId?: NodeJS.Timeout = undefined;

  constructor(action: IActionGuard, actorGuardPolling: ActorGuardPolling) {
    this.action = action;
    this.actorGuardPolling = actorGuardPolling;

    const maxAgeArray = PollingGuard.regex.exec(action.streamSource.source.metadata['cache-control']);

    let maxAge: number;
    if (maxAgeArray) {
      maxAge = Number.parseInt(maxAgeArray[1], 10);
    } else {
      maxAge = actorGuardPolling.pollingFrequency;
    }

    const age = Number.parseInt(action.streamSource.source.metadata.age, 10);
    if (age) {
      new Promise<void>(resolve => {
        this.promiseId = setTimeout(
          () => {
            resolve();
            this.checkForChanges();
          },
          (maxAge - age) * 1_000,
        );
      }).then(() => {
        this.startCheckLoop(maxAge);
      })
    }
    else {
      this.startCheckLoop(maxAge);
    }

  }

  private startCheckLoop(maxAge: number) {
    if (!this.flaggedForDeletion) {
      this.promiseId = setInterval(
        this.checkForChanges.bind(this),
        maxAge * 1_000
      );
    }
  }

  private async checkForChanges() {
    // TODO: is it better to do get instead of head
    const response = await this.actorGuardPolling.mediatorHttp.mediate(
      {
        context: this.action.context,
        input: this.action.streamSource.source.url,
        init: {
          method: 'HEAD',
        },
      },
    );

    if (response.headers.get('etag') !== this.action.streamSource.source.metadata.etag) {
      const store = this.action.streamSource.store.copyOfStore();
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

      const response = await this.actorGuardPolling.mediatorDereferenceRdf.mediate({
        context: this.action.context,
        url: this.action.streamSource.source.url,
      });

      response.data.on('end', () => {
        for (const quad of store) {
          (<Quad>quad).diff = false;
          matchStream.write(quad);
        }
        matchStream.end();
      });

      this.action.streamSource.store.attachStream(response.data.pipe(matchStream, { end: false }));

      response.headers?.forEach((value, key) => {
        this.action.streamSource.source.metadata[key] = value;
      });
    }
  }

  delete(url: string): void {
    this.flaggedForDeletion = true;
    clearInterval(this.promiseId);
    //TODO delete source
  }
}
