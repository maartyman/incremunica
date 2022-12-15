import {connection, Message} from "websocket";
import {Guard} from "./guard";
import {client} from "websocket";

export class GuardWebSockets extends Guard {
  private readonly ws;
  private connection?: connection;
  private pubRegEx = new RegExp(/pub (https?:\/\/\S+)/);
  private ackRegEx = new RegExp(/ack (https?:\/\/\S+)/);

  constructor(host: string) {
    super(host);
    this.ws = new client();
    this.ws.setMaxListeners(Infinity);

    this.ws.on('connect', (connection: connection) => {
      this.logger.debug("ws connected to pod");
      this.connection = connection;
      connection.on('error', (error) => {
        throw new Error("Guarding web socket failed: " + error.toString());
      });
      connection.on('close', () => {
        throw new Error("Guarding web socket closed");
      });
      connection.on('message', (message: Message) => {
        if (message.type === 'utf8') {
          const resources = this.pubRegEx.exec(message.utf8Data);
          if (resources && resources[1]) {
            this.dataChanged(resources[1].toString());
            return;
          }
          const ack = this.ackRegEx.exec(message.utf8Data);
          if (ack && ack[1]) {
            this.setGuardActive(ack[1].toString(), true);
          }
        }
      });
    });

    this.ws.connect(host, 'solid-0.1');
  }

  evaluateResource(resource: string): void {
    this.logger.debug("evaluateResource: " + resource);
    if (this.connection) {
      this.connection.sendUTF('sub ' + resource);
    }
    else {
      this.ws.on('connect', (connection: connection) => {
        connection.sendUTF('sub ' + resource);
      });
    }
  }
}
