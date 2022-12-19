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
import {listenUntil} from "../utils/listenUntil";

export declare interface QueryExecutor {
  on(event: "binding", listener: (bindings: Bindings, newBinding: boolean) => void): this;
  on(event: "queryEvent", listener: (arg: string) => void): this;
}


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

    this.setMaxListeners(Infinity);

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
    if (!this.isQueryEngineBuild()) {
      return;
    }

    this.queryFinished = false;

    this.guards.forEach((value) => {
      value.used = false;
    });

    this.results.forEach((value) => {
      value.used = false;
    });

    if (this.queryEngine == undefined) {
      throw new TypeError("queryEngine is undefined");
    }


    const cacheInvalidated = new Promise<boolean>((resolve) => {
      if (!this.queryEngine) {
        throw new Error("QueryEngine is undefined");
      }
      listenUntil(this.queryEngine, "message", "Cache ready", () => {
        this.logger.debug("Cache ready!");
        resolve(true);
      });
    });

    this.queryEngine.postMessage(["invalidateHttpCache", this.changedResources]);
    this.changedResources.splice(0);

    this.logger.debug("Making worker message channels");
    const messageChannel = new MessageChannel();
    const bindingsStream = messageChannel.port1;
    const messageChannelIn = messageChannel.port2;

    await cacheInvalidated;

    this.logger.debug(`Starting comunica query, with query: \n${ this.queryExplanation.queryString.toString() }`);

    if (this.queryExplanation.reasoningRules !== "") {
      this.logger.debug(`Starting comunica query, with reasoningRules: \n${ this.queryExplanation.reasoningRules }`);
    }

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
          this.makeGuard(value.message.input, value.message.headers);
          break;
      }
    });
  }

  private async makeGuard(input: RequestInfo | URL, headers: any) {
    if (this.guardingEnabled) {
      const originalInput = input.toString();

      input = new URL(input.toString());
      input = input.origin + input.pathname;

      let guardObject = this.guards.get(input);

      if (!guardObject) {
        guardObject = {guard: await GuardPolling.factory.getOrCreate(input, Guard, headers), used: true};

        if (!guardObject) {
          throw new Error("Guard couldn't be instantiated;");
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
      this.initializationFinished = true;
      this.emit("queryEvent", "initialized");
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
      if (!value.used) {
        this.emit("binding", value.bindings, false);
        this.results.delete(key);
      }
    });

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
