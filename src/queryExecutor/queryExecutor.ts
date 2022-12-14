import {Logger} from "tslog";
import {Bindings} from "@comunica/bindings-factory";
import {QueryExplanation} from "./queryExplanation";
import {loggerSettings} from "../utils/loggerSettings";
import {KeysRdfReason} from "@comunica/reasoning-context-entries";
import {QueryExecutorFactory} from "./queryExecutorFactory";
import {Actor} from "../utils/actor-factory/actor";
import {Guard} from "../guard/guard";
import {GuardPolling} from "../guard/guardPolling";
import {LocalQueryEngineFactory} from "../queryEngineFactory/localQueryEngineFactory";
import {Worker,MessageChannel} from "node:worker_threads";
import {jsonStringToBindings} from "../utils/jsonToBindings";

export class QueryExecutor extends Actor<string> {
  static factory = new QueryExecutorFactory();
  private readonly logger = new Logger(loggerSettings);
  private queryEngine: Worker | undefined;
  private results = new Map<string, { bindings: Bindings, used: boolean }>();
  private queryEngineBuild = false;
  private queryFinished = false;
  private initializationFinished = false;
  private changedResources = new Array<string>();

  public queryExplanation: QueryExplanation;
  private guards = new Map<string, {guard: Guard, used: boolean}>;
  public guardingEnabled = false;

  constructor(UUID:string, queryExplanation: QueryExplanation, guardingEnabled: boolean) {
    super(UUID);
    this.queryExplanation = queryExplanation;
    this.guardingEnabled = guardingEnabled;


    this.logger.debug("comunicaVersion = " + queryExplanation.comunicaVersion.toString());
    this.logger.debug("comunica context path = " + queryExplanation.comunicaContext.toString());

    LocalQueryEngineFactory.getOrCreate(
      queryExplanation.comunicaVersion.toString(),
      queryExplanation.comunicaContext.toString()
    ).then((queryEngine: Worker) => {
      this.queryEngine = queryEngine;
      this.logger.debug(`Comunica engine build`);
      this.queryEngineBuild = true;
      this.emit("queryEngineEvent", "build");
      this.executeQuery();
    });
  }

  private async executeQuery() {
    this.queryFinished = false;

    this.guards.forEach((value) => {
      value.used = false;
    });
    this.results.forEach((value) => {
      value.used = false;
    });


    this.logger.debug(`Starting comunica query, with query: \n${ this.queryExplanation.queryString.toString() }`);

    if (this.queryEngine == undefined) {
      throw new TypeError("queryEngine is undefined");
    }

    this.logger.debug(`Starting comunica query, with reasoningRules: \n${ this.queryExplanation.reasoningRules }`);

    /*
    TODO temporarily turning this off as it doesn't work => query explanation will give the used resources (I think)
    let parallelPromise = new Array<Promise<any>>();
    for (const resource of this.changedResources) {
      parallelPromise.push(this.queryEngine.invalidateHttpCache(resource));
    }
    await Promise.all(parallelPromise);

     */
    this.queryEngine.postMessage("invalidateHttpCache");
    this.changedResources.splice(0);

    /*
    this.queryEngine.postMessage(
      this.queryExplanation.queryString.toString(), {
      sources: this.queryExplanation.sources,
      [KeysRdfReason.implicitDatasetFactory.name]: () => new Store(),
      [KeysRdfReason.rules.name]: this.queryExplanation.reasoningRules,
      fetch: this.customFetch.bind(this),
      lenient: this.queryExplanation.lenient
    });
    */

    const messageChannel = new MessageChannel();
    const bindingsStream = messageChannel.port1;
    const messageChannelIn = messageChannel.port2;

    this.queryEngine.postMessage(
      [
        messageChannelIn,
        this.queryExplanation.queryString.toString(),
        {
          sources: this.queryExplanation.sources,
          [KeysRdfReason.rules.name]: this.queryExplanation.reasoningRules,
          lenient: this.queryExplanation.lenient
        }
      ],
      [
        messageChannelIn
      ]
    );

    bindingsStream.on('message', (value: {messageType: string, message: any}) => {
      switch (value.messageType) {
        case "data":
          const binding = jsonStringToBindings(value.message);
          this.logger.debug(`on data: ${ binding.toString() }`);
          const result = this.results.get(binding.toString());

          if (!result) {
            this.results.set(binding.toString(), {bindings: binding, used: true});
            this.emit("binding", binding, true);
          }
          else {
            result.used = true;
          }
          break;
        case "end":
          this.logger.debug("query end");
          this.afterQueryCleanup();
          break;
        case "error":
          //TODO solve error
          this.logger.error(value.message);
          LocalQueryEngineFactory.deleteWorker(
            this.queryExplanation.comunicaVersion.toString(),
            this.queryExplanation.comunicaContext.toString()
          );
          LocalQueryEngineFactory.getOrCreate(
            this.queryExplanation.comunicaVersion.toString(),
            this.queryExplanation.comunicaContext.toString()
          ).then((queryEngine: Worker) => {
            this.queryEngine = queryEngine;
            this.logger.debug(`Comunica engine build`);
            this.queryEngineBuild = true;
            this.emit("queryEngineEvent", "build");
            this.executeQuery();
          });
          break;
        case "fetch":
          this.customFetch(value.message);
          break;
      }
    });
  }

  private async customFetch(input: RequestInfo | URL) {
    //TODO check used resources: delete the ones that aren't used add the new ones
    //TODO possibly wait until the resource is actively guarded

    if (this.guardingEnabled) {
      const originalInput = input.toString();

      input = new URL(input.toString());
      input = input.origin + input.pathname;

      let guardObject = this.guards.get(input);

      if (!guardObject) {
        guardObject = {guard: await GuardPolling.factory.getOrCreate(input), used: true};

        if (!guardObject) {
          throw new Error("guard couldn't be instantiated;");
        }

        guardObject.guard.on("ResourceChanged", this.resourceChanged.bind(this, originalInput));
      }

      if (!guardObject.guard.isGuardActive(input)) {
        await guardActive(guardObject.guard, input);
      }

      this.guards.set(input, guardObject);
    }
  }

  public async getData() : Promise<Bindings[]> {
    if (!this.guardingEnabled){
      this.executeQuery();
      await new Promise<void>((resolve) => {
        this.on("queryEvent", (arg: string) => {
          if (arg === "done") {
            resolve();
          }
        })
      });
    }
    const bindings: Bindings[] = [];
    this.results.forEach((value) => {
      if (value.used) {
        bindings.push(value.bindings);
      }
    });
    return bindings;
  }

  public isQueryEngineBuild (): boolean {
    return this.queryEngineBuild;
  }

  public isQueryFinished (): boolean {
    return this.queryFinished;
  }

  public isInitializationFinished (): boolean {
    return this.initializationFinished;
  }

  private resourceChanged(resource: string, input: string) {
    this.logger.debug("data has changed with resource: " + resource + " and input: " + input);
    const inputURL = new URL(input);
    if (inputURL.origin + inputURL.pathname === resource) {
      this.changedResources.push(input);
      this.logger.debug("resource added to array: " + this.changedResources[0]);

      if (this.queryFinished) {
        this.executeQuery();
      }
    }
  }

  private afterQueryCleanup() {
    if (!this.initializationFinished) {
      this.emit("queryEvent", "initialized");
      this.initializationFinished = true;
    }
    this.guards.forEach((value, key) => {
      this.logger.debug("Resource: " + key + " is used: " + value.used);
      if (!value.used) {
        value.guard.removeListener("ResourceChanged", this.resourceChanged);
        if (value.guard.listenerCount("ResourceChanged") == 0) {
          value.guard.delete();
        }
        this.guards.delete(key);
      }
    });
    this.results.forEach((value, key) => {
      if (!value.used){
        this.emit("binding", value.bindings, false);
        this.results.delete(key);
      }
    });
    //printMap(this.results);
    this.queryFinished = true;
    this.logger.debug(`Comunica query finished`);
    this.emit("queryEvent", "done");
    if (this.changedResources.length > 0) {
      this.executeQuery();
    }
  }

  delete() {
    this.guards.forEach((value) => {
      value.guard.delete();
    });
    super.delete();
    LocalQueryEngineFactory.deleteWorker(
      this.queryExplanation.comunicaVersion.toString(),
      this.queryExplanation.comunicaContext.toString()
    );
  }
}

async function guardActive(guard: Guard, input: string) {
  guard.on("guardActive", (resource: string, value: boolean) => {
    if (resource === input && value) {
      return;
    }
  });
}

function printMap(map: Map<string, { bindings: Bindings, used: boolean }>) {
  let text = "Map content: \n";
  map.forEach((value: {bindings: Bindings, used: boolean}, key) => {
    text += "bindings: \n";
    value.bindings.forEach((value, key) => {
      text += "\t" + key.value + ": " + value.value + "\n";
    });
  });
  new Logger(loggerSettings).debug(text);
}
