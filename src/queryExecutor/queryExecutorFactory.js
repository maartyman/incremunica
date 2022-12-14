"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueryExecutorFactory = void 0;
const queryExecutor_1 = require("./queryExecutor");
const uuid_1 = require("uuid");
const arrayEquality_1 = require("../utils/arrayEquality");
const tslog_1 = require("tslog");
const loggerSettings_1 = require("../utils/loggerSettings");
const factory_1 = require("../utils/actor-factory/factory");
class QueryExecutorFactory extends factory_1.Factory {
    constructor() {
        super(queryExecutor_1.QueryExecutor);
    }
    queryExplanationToUUID(queryExplanation) {
        //TODO this probably can be done in a better way
        // standardize the query explanation and then use a hashmap to look for a uuid
        let logger = new tslog_1.Logger(loggerSettings_1.loggerSettings);
        let queryExecutor;
        this.getKeyValuePairs((value) => {
            if (!(value.queryExplanation.queryString === queryExplanation.queryString)) {
                logger.debug("queryString");
                return;
            }
            else if (!(0, arrayEquality_1.arrayEquality)(value.queryExplanation.sources, queryExplanation.sources)) {
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
            logger.info("query: \n" + JSON.stringify(queryExplanation) + " \nalready exists!");
            return queryExecutor.key;
        }
        else {
            return (0, uuid_1.v4)();
        }
    }
}
exports.QueryExecutorFactory = QueryExecutorFactory;
