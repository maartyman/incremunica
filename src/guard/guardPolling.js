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
exports.GuardPolling = void 0;
const http = __importStar(require("http"));
const guardingConfig_1 = require("./guardingConfig");
const guard_1 = require("./guard");
const guardFactory_1 = require("./guardFactory");
class GuardPolling extends guard_1.Guard {
    constructor(resource) {
        super(resource);
        this.getHead((res) => {
            const lastModifiedServer = res.headers["last-modified"];
            const ETagServer = res.headers.etag;
            if (ETagServer) {
                this.ETag = ETagServer;
            }
            else if (lastModifiedServer) {
                this.lastModified = new Date(lastModifiedServer).valueOf();
            }
            else {
                this.logger.error("Can't guard resource: '" + resource + "'. Server doesn't support Web Sockets nor does it support 'last-modified' or 'eTag' headers.");
            }
            this.setGuardActive(resource, true);
            this.polHeadResource();
        });
    }
    polHeadResource() {
        this.getHead((res) => {
            if (this.ETag) {
                if (res.headers.etag !== this.ETag) {
                    this.ETag = res.headers.etag;
                    this.dataChanged(this.key);
                }
            }
            else if (this.lastModified) {
                const lastModifiedServer = res.headers["last-modified"];
                if (lastModifiedServer) {
                    const lastModifiedDateServer = new Date(lastModifiedServer).valueOf();
                    if (lastModifiedDateServer != this.lastModified) {
                        this.lastModified = lastModifiedDateServer;
                        this.dataChanged(this.key);
                    }
                }
                else {
                    this.logger.error("Last modified tag isn't set by the server for resource: '" + this.key + "'");
                }
            }
        });
        setTimeout(this.polHeadResource.bind(this), guardingConfig_1.GuardingConfig.pollingInterval);
    }
    getHead(callback) {
        const req = http.request(this.key, {
            method: "HEAD"
        }, callback);
        req.end();
    }
}
exports.GuardPolling = GuardPolling;
GuardPolling.factory = new guardFactory_1.GuardFactory();
