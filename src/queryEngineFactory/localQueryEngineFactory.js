"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LocalQueryEngineFactory = void 0;
const node_worker_threads_1 = require("node:worker_threads");
const listenUntil_1 = require("../utils/listenUntil");
class LocalQueryEngineFactory {
    static getOrCreate(comunicaVersion, comunicaContext) {
        return __awaiter(this, void 0, void 0, function* () {
            let tempWorker = this.engines.get(comunicaVersion + comunicaContext);
            if (tempWorker != undefined) {
                tempWorker.count++;
                return tempWorker.worker;
            }
            const worker = new node_worker_threads_1.Worker(__dirname + "/queryEngineWorker.js", {
                workerData: [
                    comunicaVersion,
                    comunicaContext
                ]
            });
            const workerPromise = new Promise((resolve) => {
                (0, listenUntil_1.listenUntil)(worker, "message", "done", () => {
                    resolve(worker);
                });
            });
            this.engines.set(comunicaVersion + comunicaContext, { count: 1, worker: workerPromise });
            return workerPromise;
        });
    }
    static deleteWorker(comunicaVersion, comunicaContext) {
        let tempWorker = this.engines.get(comunicaVersion + comunicaContext);
        if (tempWorker) {
            tempWorker.count--;
            if (tempWorker.count == 0) {
                tempWorker.worker.then((worker) => {
                    worker.terminate();
                });
                this.engines.delete(comunicaVersion + comunicaContext);
            }
        }
    }
}
exports.LocalQueryEngineFactory = LocalQueryEngineFactory;
LocalQueryEngineFactory.engines = new Map();
