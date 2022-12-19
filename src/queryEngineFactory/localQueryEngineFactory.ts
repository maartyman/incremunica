import {Worker} from "node:worker_threads";
import {listenUntil} from "../utils/listenUntil";

export class LocalQueryEngineFactory {
  private static engines = new Map<string, {count: number, worker:Promise<Worker>}>();

  static async getOrCreate(comunicaVersion: string, comunicaContext: string): Promise<Worker> {
    let tempWorker = this.engines.get(comunicaVersion + comunicaContext);

    if (tempWorker != undefined) {
      tempWorker.count++;
      return tempWorker.worker;
    }

    const worker = new Worker(
      __dirname + "/queryEngineWorker.js",
      {
        workerData: [
          comunicaVersion,
          comunicaContext
        ]
      }
    );

    const workerPromise = new Promise<Worker>((resolve) => {
      listenUntil(worker, "message", "done", () => {
        resolve(worker);
      });
    });

    this.engines.set(comunicaVersion + comunicaContext, {count: 1,worker: workerPromise});

    return workerPromise
  }

  static deleteWorker(comunicaVersion: string, comunicaContext: string): void {
    let tempWorker = this.engines.get(comunicaVersion + comunicaContext);

    if (tempWorker) {
      tempWorker.count--;
      if (tempWorker.count == 0) {
        tempWorker.worker.then((worker) => {
          worker.terminate();
        })
        this.engines.delete(comunicaVersion + comunicaContext);
      }
    }
  }
}
