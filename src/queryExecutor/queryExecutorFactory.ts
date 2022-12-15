import {QueryExecutor} from "./queryExecutor";
import {QueryExplanation} from "./queryExplanation";
import { v4 as uuidv4 } from 'uuid';
import {arrayEquality} from "../utils/arrayEquality";
import {Logger} from "tslog";
import {loggerSettings} from "../utils/loggerSettings";
import {Factory} from "../utils/actor-factory/factory";

export class QueryExecutorFactory extends Factory<string, QueryExecutor> {

  constructor() {
    super(QueryExecutor);
  }

  public queryExplanationToUUID(queryExplanation: QueryExplanation): string {
    //TODO this probably can be done in a better way
    // standardize the query explanation and then use a hashmap to look for a uuid

    let logger = new Logger(loggerSettings);

    let queryExecutor: QueryExecutor | undefined;
    this.getKeyValuePairs((value) => {
      if (!(value.queryExplanation.queryString === queryExplanation.queryString)) {
        logger.debug("queryString");
        return;
      }
      else if (!arrayEquality(value.queryExplanation.sources, queryExplanation.sources)) {
        logger.debug("sources");
        return;
      }
      else if (!(value.queryExplanation.comunicaContext === queryExplanation.comunicaContext)) {
        logger.debug("context");
        return;
      }
      else if (!(value.queryExplanation.reasoningRules === queryExplanation.reasoningRules)) {
        logger.debug("reasoningRules");
        return;
      }
      else if (!(value.queryExplanation.comunicaVersion === queryExplanation.comunicaVersion)) {
        logger.debug("comunicaVersion");
        return;
      }
      else if (value.queryExplanation.lenient != queryExplanation.lenient) {
        logger.debug("lenient");
        return;
      }
      queryExecutor = value;
      return;
    });
    if (queryExecutor) {
      logger.info("query: \n"+ JSON.stringify(queryExplanation) +" \nalready exists!");
      return queryExecutor.key;
    }
    else {
      return uuidv4();
    }
  }
}
