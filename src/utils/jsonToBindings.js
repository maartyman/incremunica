"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.jsonObjectToBindings = exports.jsonStringToBindings = void 0;
const bindings_factory_1 = require("@comunica/bindings-factory");
const n3_1 = require("n3");
const immutable_1 = require("immutable");
function jsonStringToBindings(jsonString) {
    return new bindings_factory_1.Bindings(n3_1.DataFactory, (0, immutable_1.Map)(Object.entries(JSON.parse(jsonString).entries)));
}
exports.jsonStringToBindings = jsonStringToBindings;
function jsonObjectToBindings(json) {
    return new bindings_factory_1.Bindings(n3_1.DataFactory, (0, immutable_1.Map)(Object.entries(json.entries)));
}
exports.jsonObjectToBindings = jsonObjectToBindings;
