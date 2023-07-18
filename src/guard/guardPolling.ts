import * as http from "http";
import {GuardingConfig} from "./guardingConfig";
import {Guard} from "./guard";
import {GuardFactory} from "./guardFactory";
import {fetch} from "cross-fetch";

export class GuardPolling extends Guard {
  private ETag?: string;
  private lastModified?: number;
  static factory = new GuardFactory();

  constructor(resource: string) {
    super(resource);

    this.getHead((res: Response) => {
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

  private polHeadResource() {
    this.getHead((res:Response) => {
      if (this.ETag) {
        let tempEtag = res.headers.get("etag");
        if (tempEtag !== this.ETag) {
          this.ETag = (tempEtag == null)? undefined : tempEtag;
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
    setTimeout(this.polHeadResource.bind(this), GuardingConfig.pollingInterval);
  }

  private getHead(callback: (res: Response) => void) {
    fetch(this.key, {
      method: "HEAD"
    }).then((res) => callback(res));
  }
}
