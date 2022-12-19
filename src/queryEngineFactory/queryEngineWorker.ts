import {QueryEngine, QueryEngineFactory} from "@comunica/query-sparql";
import {QueryStringContext} from "@comunica/types";
import {Bindings} from "@comunica/bindings-factory";
import {fetch} from "cross-fetch";

const {workerData, parentPort} = require('node:worker_threads');

const comunicaVersion = workerData[0];
const comunicaContext = workerData[1];

const queryEngineFactory = require(comunicaVersion).QueryEngineFactory;

const queryEnginePromise = (new queryEngineFactory() as QueryEngineFactory).create({
  configPath: comunicaContext,
});

let queryEngine: QueryEngine | undefined = undefined;
queryEnginePromise.then((queryEngineArg) => {
  queryEngine = queryEngineArg;
  if (queryEngine == undefined) {
    throw new Error("this shouldn't happen");
  }
  parentPort.postMessage("done");
})


parentPort.on("message", async (value: ["invalidateHttpCache", [string]] | [MessagePort, string, QueryStringContext]) => {
  if(queryEngine == undefined) {
    throw new Error("queryEngine undefined, this shouldn't happen!");
  }

  if(value[0] === "invalidateHttpCache"){
    let parallelPromise = new Array<Promise<any>>();
    for (const resource of value[1]) {
      parallelPromise.push(queryEngine.invalidateHttpCache(resource));
    }
    await Promise.all(parallelPromise);
    parentPort.postMessage("Cache ready");
  }
  else {
    const port = value[0] as MessagePort;

    let queryContext = value[2] as QueryStringContext;

    const extra = {
      fetch: customFetch.bind(port)
    }

    queryContext = {...queryContext, ...extra};

    const bindingsStream = await queryEngine.queryBindings(
      value[1],
      queryContext
    );

    bindingsStream.on('data', (binding: Bindings) => {
      port.postMessage({messageType: "data", message: JSON.stringify(binding)});
    });

    bindingsStream.on('end', () => {
      port.postMessage({messageType: "end", message: ""});
      port.close();
    });

    bindingsStream.on('error', (error: any) => {
      port.postMessage({messageType: "error", message: error});
      port.close();
    });
  }
});

async function customFetch(this: MessagePort, input: RequestInfo | URL, init?: RequestInit | undefined): Promise<Response> {
  return fetch(input, init).then((res) => {
    let headers: any = {};
    res.headers.forEach((val, key) => {
      headers[key] = val;
    });
    this.postMessage({messageType: "fetch", message: {input: input, headers: headers}});
    return res;
  });
}
