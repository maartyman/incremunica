import {QueryEngine, QueryEngineFactory} from "@comunica/query-sparql";
import {QueryStringContext} from "@comunica/types";
import {Bindings} from "@comunica/bindings-factory";

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


parentPort.on("message", async (value: string | [MessagePort, string, QueryStringContext]) => {
  if(queryEngine == undefined) {
    throw new Error("queryEngine undefined, this shouldn't happen!");
  }

  if(value === "invalidateHttpCache"){
    await queryEngine.invalidateHttpCache();
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
  //TODO check used resources: delete the ones that aren't used add the new ones
  //TODO possibly wait until the resource is actively guarded

  this.postMessage({messageType: "fetch", message: input});

  return fetch(input, init);
}
