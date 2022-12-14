"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueryExplanation = void 0;
const resolveUndefined_1 = require("../utils/resolveUndefined");
const path = __importStar(require("path"));
class QueryExplanation {
    constructor(queryString, sources, comunicaVersion, context, reasoningRules, lenient) {
        this.queryString = queryString;
        this.sources = sources;
        switch (comunicaVersion) {
            case "default":
                this.comunicaVersion = "@comunica/query-sparql";
                break;
            case "reasoning":
                this.comunicaVersion = "@comunica/query-sparql-reasoning";
                break;
            case "link-traversal":
                this.comunicaVersion = "@comunica/query-sparql-link-traversal";
                break;
            case "link-traversal-solid":
                this.comunicaVersion = "@comunica/query-sparql-link-traversal-solid";
                break;
            case "solid":
                this.comunicaVersion = "@comunica/query-sparql-solid";
                break;
            default:
                this.comunicaVersion = "@comunica/query-sparql";
                break;
        }
        switch (context) {
            case "default":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql/config-default.json");
                break;
            case "reasoning-default":
                this.comunicaContext = path.resolve(__dirname, "../config/config-reasoning/config-default.json");
                break;
            case "link-traversal-default":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-default.json");
                break;
            case "link-traversal-follow-all":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-follow-all.json");
                break;
            case "link-traversal-follow-content-policies-conditional":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-follow-content-policies-conditional.json");
                break;
            case "link-traversal-follow-content-policies-restrictive":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-follow-content-policies-restrictive.json");
                break;
            case "link-traversal-follow-match-pattern-bound":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-follow-match-pattern-bound.json");
                break;
            case "link-traversal-follow-match-query":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-follow-match-query.json");
                break;
            case "link-traversal-solid-default":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-solid-default.json");
                break;
            case "link-traversal-solid-prov-sources":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-solid-prov-sources.json");
                break;
            case "link-traversal-solid-shapetrees":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-solid-shapetrees.json");
                break;
            case "link-traversal-solid-single-pod":
                this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-solid-single-pod.json");
                break;
            case "solid-default":
                this.comunicaContext = path.resolve(__dirname, "../config/config-query-sparql-solid/config-default.json");
                break;
            default:
                switch (comunicaVersion) {
                    //TODO add custom configs
                    case "default":
                        this.comunicaContext = path.resolve(__dirname, "../config/query-sparql/config-default.json");
                        break;
                    case "reasoning":
                        this.comunicaContext = path.resolve(__dirname, "../config/config-reasoning/config-default.json");
                        break;
                    case "link-traversal":
                        this.comunicaContext = path.resolve(__dirname, "../config/query-sparql-link-traversal/config-default.json");
                        break;
                    case "solid":
                        this.comunicaContext = path.resolve(__dirname, "../config/config-query-sparql-solid/config-default.json");
                        break;
                    default:
                        this.comunicaContext = path.resolve(__dirname, "../config/query-sparql/config-default.json");
                        break;
                }
                break;
        }
        this.reasoningRules = (0, resolveUndefined_1.resolveUndefined)(reasoningRules, "");
        this.lenient = (0, resolveUndefined_1.resolveUndefined)(lenient, false);
    }
}
exports.QueryExplanation = QueryExplanation;
