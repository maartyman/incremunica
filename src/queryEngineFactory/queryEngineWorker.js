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
const { workerData, parentPort } = require('node:worker_threads');
const comunicaVersion = workerData[0];
const comunicaContext = workerData[1];
const queryEngineFactory = require(comunicaVersion).QueryEngineFactory;
const queryEnginePromise = new queryEngineFactory().create({
    configPath: comunicaContext,
});
let queryEngine = undefined;
queryEnginePromise.then((queryEngineArg) => {
    queryEngine = queryEngineArg;
    if (queryEngine == undefined) {
        throw new Error("this shouldn't happen");
    }
    parentPort.postMessage("done");
});
parentPort.on("message", (value) => __awaiter(void 0, void 0, void 0, function* () {
    if (queryEngine == undefined) {
        throw new Error("queryEngine undefined, this shouldn't happen!");
    }
    if (value === "invalidateHttpCache") {
        yield queryEngine.invalidateHttpCache();
    }
    else {
        const port = value[0];
        let queryContext = value[2];
        const extra = {
            fetch: customFetch.bind(port)
        };
        queryContext = Object.assign(Object.assign({}, queryContext), extra);
        const bindingsStream = yield queryEngine.queryBindings(value[1], queryContext);
        bindingsStream.on('data', (binding) => {
            port.postMessage({ messageType: "data", message: JSON.stringify(binding) });
        });
        bindingsStream.on('end', () => {
            port.postMessage({ messageType: "end", message: "" });
            port.close();
        });
        bindingsStream.on('error', (error) => {
            port.postMessage({ messageType: "error", message: error });
            port.close();
        });
    }
}));
function customFetch(input, init) {
    return __awaiter(this, void 0, void 0, function* () {
        //TODO check used resources: delete the ones that aren't used add the new ones
        //TODO possibly wait until the resource is actively guarded
        this.postMessage({ messageType: "fetch", message: input });
        return fetch(input, init);
    });
}
