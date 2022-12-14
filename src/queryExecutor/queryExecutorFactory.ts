import {QueryExecutor} from "./queryExecutor";
import {QueryExplanation} from "./queryExplanation";
import { v4 as uuidv4 } from 'uuid';
import {arrayEquality} from "../utils/arrayEquality";
import {Logger} from "tslog";
import {loggerSettings} from "../utils/loggerSettings";
import {Factory} from "../utils/actor-factory/factory";

export class QueryExecutorFactory extends Factory<string, QueryExecutor> {
  private readonly logger = new Logger(loggerSettings);

  constructor() {
    super(QueryExecutor);
  }

  public queryExplanationToUUID(queryExplanation: QueryExplanation): string {
    //TODO this probably can be done in a better way
    // standardize the query explanation and then use a hashmap to look for a uuid

    let queryExecutor: QueryExecutor | undefined;
    this.getKeyValuePairs((value) => {
      if (!(value.queryExplanation.queryString === queryExplanation.queryString)) {
        this.logger.debug("queryString");
        return;
      }
      else if (!arrayEquality(value.queryExplanation.sources, queryExplanation.sources)) {
        this.logger.debug("sources");
        return;
      }
      else if (!(value.queryExplanation.comunicaContext === queryExplanation.comunicaContext)) {
        this.logger.debug("context");
        return;
      }
      else if (!(value.queryExplanation.reasoningRules === queryExplanation.reasoningRules)) {
        this.logger.debug("reasoningRules");
        return;
      }
      else if (!(value.queryExplanation.comunicaVersion === queryExplanation.comunicaVersion)) {
        this.logger.debug("comunicaVersion");
        return;
      }
      else if (value.queryExplanation.lenient != queryExplanation.lenient) {
        this.logger.debug("lenient");
        return;
      }
      queryExecutor = value;
      return;
    });
    if (queryExecutor) {
      this.logger.info("query: \n"+ JSON.stringify(queryExplanation) +" \nalready exists!");
      return queryExecutor.key;
    }
    else {
      return uuidv4();
    }
  }
}
