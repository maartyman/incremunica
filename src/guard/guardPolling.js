"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.GuardPolling = void 0;
const guardingConfig_1 = require("./guardingConfig");
const guard_1 = require("./guard");
const guardFactory_1 = require("./guardFactory");
const cross_fetch_1 = require("cross-fetch");
class GuardPolling extends guard_1.Guard {
    constructor(resource) {
        super(resource);
        this.getHead((res) => {
            const lastModifiedServer = res.headers.get("last-modified");
            const ETagServer = res.headers.get("etag");
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
                let tempEtag = res.headers.get("etag");
                if (tempEtag !== this.ETag) {
                    this.ETag = (tempEtag == null) ? undefined : tempEtag;
                    this.dataChanged(this.key);
                }
            }
            else if (this.lastModified) {
                const lastModifiedServer = res.headers.get("last-modified");
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
        (0, cross_fetch_1.fetch)(this.key, {
            method: "HEAD"
        }).then((res) => callback(res));
    }
}
exports.GuardPolling = GuardPolling;
GuardPolling.factory = new guardFactory_1.GuardFactory();
