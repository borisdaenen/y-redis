import * as array from "lib0/array";
import * as decoding from "lib0/decoding";
import * as encoding from "lib0/encoding";
import * as logging from "lib0/logging";
import * as promise from "lib0/promise";
import type {
  HttpRequest,
  RecognizedString,
  TemplatedApp,
} from "uWebSockets.js";
import * as uws from "uWebSockets.js";
import * as Y from "yjs";
import {
  Api,
  computeRedisRoomStreamName,
  createApiClient,
  isSmallerRedisId,
} from "./api.js";
import * as protocol from "./protocol.js";
import type { AbstractStorage } from "./storage.js";
import { Subscriber, createSubscriber } from "./subscriber.js";

const log = logging.createModuleLogger("@y/redis/ws");

// Small helper to preview binary payloads without flooding logs
const previewBuffer = (buf: Uint8Array, maxBytes = 16): string =>
  Array.from(buf.slice(0, Math.min(maxBytes, buf.byteLength)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join(" ");

/**
 * how to sync
 *   receive sync-step 1
 *   // @todo y-websocket should only accept updates after receiving sync-step 2
 *   redisId = ws.sub(conn)
 *   {doc,redisDocLastId} = api.getdoc()
 *   compute sync-step 2
 *   if (redisId > redisDocLastId) {
 *     subscriber.ensureId(redisDocLastId)
 *   }
 */

class YWebsocketServer {
  app: TemplatedApp;
  subscriber: Subscriber;
  client: Api;
  /**
   * @param {uws.TemplatedApp} app
   * @param {api.Api} client
   * @param {import('./subscriber.js').Subscriber} subscriber
   */
  constructor(app: TemplatedApp, client: Api, subscriber: Subscriber) {
    this.app = app;
    this.subscriber = subscriber;
    this.client = client;
  }

  async destroy() {
    this.subscriber.destroy();
    await this.client.destroy();
  }
}

let _idCnt = 0;

class User {
  room: string;
  hasWriteAccess: boolean;
  initialRedisSubId: string;
  subs: Set<string>;
  id: number;
  userid: string;
  awarenessId: number | null;
  awarenessLastClock: number;
  isClosed: boolean;
  /**
   * @param {string} room
   * @param {boolean} hasWriteAccess
   * @param {string} userid identifies the user globally.
   */
  constructor(room: string, hasWriteAccess: boolean, userid: string) {
    this.room = room;
    this.hasWriteAccess = hasWriteAccess;
    /**
     * @type {string}
     */
    this.initialRedisSubId = "0";
    this.subs = new Set();
    /**
     * This is just an identifier to keep track of the user for logging purposes.
     */
    this.id = _idCnt++;
    /**
     * Identifies the User globally.
     * Note that several clients can have the same userid (e.g. if a user opened several browser
     * windows)
     */
    this.userid = userid;
    /**
     * @type {number|null}
     */
    this.awarenessId = null;
    this.awarenessLastClock = 0;
    this.isClosed = false;
  }
}

/**
 * @param {uws.TemplatedApp} app
 * @param {uws.RecognizedString} pattern
 * @param {import('./storage.js').AbstractStorage} store
 * @param {function(uws.HttpRequest): Promise<{ hasWriteAccess: boolean, room: string, userid: string }>} checkAuth
 * @param {Object} conf
 * @param {string} [conf.redisPrefix]
 * @param {(room:string,docname:string,client:api.Api)=>void} [conf.initDocCallback] - this is called when a doc is
 * accessed, but it doesn't exist. You could populate the doc here. However, this function could be
 * called several times, until some content exists. So you need to handle concurrent calls.
 */
export const registerYWebsocketServer = async (
  app: TemplatedApp,
  pattern: RecognizedString,
  store: AbstractStorage,
  checkAuth: (
    req: HttpRequest
  ) => Promise<{ hasWriteAccess: boolean; room: string; userid: string }>,
  {
    redisPrefix = "y",
    initDocCallback = () => {},
  }: {
    redisPrefix?: string;
    initDocCallback?: (room: string, docname: string, client: Api) => void;
  } = {}
) => {
  const [client, subscriber] = await promise.all([
    createApiClient(store, redisPrefix),
    createSubscriber(store, redisPrefix),
  ]);
  /**
   * @param {string} stream
   * @param {Array<Uint8Array>} messages
   */
  const redisMessageSubscriber = (
    stream: string,
    messages: Array<Uint8Array>
  ) => {
    if (app.numSubscribers(stream) === 0) {
      subscriber.unsubscribe(stream, redisMessageSubscriber);
    }
    console.debug("[ws] redisMessageSubscriber", {
      stream,
      numMessages: messages.length,
      subscribers: app.numSubscribers(stream),
    });
    if (messages.length === 0) {
      return;
    }
    const message =
      messages.length === 1
        ? messages[0]
        : encoding.encode((encoder) =>
            messages.forEach((message: Uint8Array) => {
              encoding.writeUint8Array(encoder, message);
            })
          );
    console.debug("[ws] publish to topic", {
      stream,
      payloadBytes: message.byteLength,
    });
    app.publish(stream, message, true, false);
  };
  app.ws<User>(
    pattern,
    /** @type {uws.WebSocketBehavior<User>} */ {
      compression: uws.SHARED_COMPRESSOR,
      maxPayloadLength: 100 * 1024 * 1024,
      idleTimeout: 60,
      sendPingsAutomatically: true,
      upgrade: async (res, req, context) => {
        const url = req.getUrl();
        const headerWsKey = req.getHeader("sec-websocket-key");
        const headerWsProtocol = req.getHeader("sec-websocket-protocol");
        const headerWsExtensions = req.getHeader("sec-websocket-extensions");
        let aborted = false;
        res.onAborted(() => {
          console.log("Upgrading client aborted", { url });
          aborted = true;
        });
        try {
          const { hasWriteAccess, room, userid } = await checkAuth(req);
          if (aborted) return;
          res.cork(() => {
            res.upgrade(
              new User(room, hasWriteAccess, userid),
              headerWsKey,
              headerWsProtocol,
              headerWsExtensions,
              context
            );
          });
        } catch (err) {
          console.log(`Failed to auth to endpoint ${url}`, err);
          if (aborted) return;
          res.cork(() => {
            res.writeStatus("401 Unauthorized").end("Unauthorized");
          });
        }
      },
      open: async (ws) => {
        const user = ws.getUserData();
        log(() => [
          "client connected (uid=",
          user.id,
          ", ip=",
          Buffer.from(ws.getRemoteAddressAsText()).toString(),
          ")",
        ]);
        console.debug("[ws] open", {
          uid: user.id,
          room: user.room,
          hasWriteAccess: user.hasWriteAccess,
        });
        const stream = computeRedisRoomStreamName(
          user.room,
          "index",
          redisPrefix
        );
        user.subs.add(stream);
        console.debug("[ws] subscribe", { uid: user.id, stream });
        ws.subscribe(stream);
        user.initialRedisSubId = subscriber.subscribe(
          stream,
          redisMessageSubscriber
        ).redisId;
        console.debug("[ws] initial redis sub id", {
          uid: user.id,
          stream,
          redisId: user.initialRedisSubId,
        });
        const indexDoc = await client.getDoc(user.room, "index");
        if (indexDoc.ydoc.store.clients.size === 0) {
          initDocCallback(user.room, "index", client);
        }
        if (user.isClosed) return;
        ws.cork(() => {
          const stateVector = Y.encodeStateVector(indexDoc.ydoc);
          const step1 = protocol.encodeSyncStep1(stateVector);
          const stateUpdate = Y.encodeStateAsUpdate(indexDoc.ydoc);
          const step2 = protocol.encodeSyncStep2(stateUpdate);
          console.debug("[ws] sending initial sync", {
            uid: user.id,
            step1Bytes: step1.byteLength,
            step2Bytes: step2.byteLength,
            awarenessStates: indexDoc.awareness.states.size,
          });
          ws.send(step1, true, false);
          ws.send(step2, true, true);
          if (indexDoc.awareness.states.size > 0) {
            const awMsg = protocol.encodeAwarenessUpdate(
              indexDoc.awareness,
              array.from(indexDoc.awareness.states.keys())
            );
            console.debug("[ws] sending initial awareness", {
              uid: user.id,
              bytes: awMsg.byteLength,
            });
            ws.send(awMsg, true, true);
          }
        });
        if (isSmallerRedisId(indexDoc.redisLastId, user.initialRedisSubId)) {
          // our subscription is newer than the content that we received from the api
          // need to renew subscription id and make sure that we catch the latest content.
          console.debug("[ws] ensureSubId", {
            stream,
            from: user.initialRedisSubId,
            to: indexDoc.redisLastId,
          });
          subscriber.ensureSubId(stream, indexDoc.redisLastId);
        }
      },
      message: (ws, messageBuffer) => {
        const user = ws.getUserData();
        // don't read any messages from users without write access
        if (!user.hasWriteAccess) return;
        // It is important to copy the data here
        const message = Buffer.from(
          messageBuffer.slice(0, messageBuffer.byteLength)
        );
        console.debug("[ws] rx client message", {
          uid: user.id,
          len: message.length,
          type0: message[0],
          type1: message[1],
          preview: previewBuffer(message),
        });
        if (
          // filter out messages that we simply want to propagate to all clients
          // sync update or sync step 2
          (message[0] === protocol.messageSync &&
            (message[1] === protocol.messageSyncUpdate ||
              message[1] === protocol.messageSyncStep2)) ||
          // awareness update
          message[0] === protocol.messageAwareness
        ) {
          if (message[0] === protocol.messageAwareness) {
            const decoder = decoding.createDecoder(message);
            decoding.readVarUint(decoder); // read message type
            decoding.readVarUint(decoder); // read length of awareness update
            const alen = decoding.readVarUint(decoder); // number of awareness updates
            const awId = decoding.readVarUint(decoder);
            console.debug("[ws] awareness update", {
              uid: user.id,
              updatesCount: alen,
              awarenessId: awId,
            });
            if (
              alen === 1 &&
              (user.awarenessId === null || user.awarenessId === awId)
            ) {
              // only update awareness if len=1
              user.awarenessId = awId;
              user.awarenessLastClock = decoding.readVarUint(decoder);
              console.debug("[ws] awareness set", {
                uid: user.id,
                awarenessId: user.awarenessId,
                clock: user.awarenessLastClock,
              });
            }
          }
          client.addMessage(user.room, "index", message);
        } else if (
          message[0] === protocol.messageSync &&
          message[1] === protocol.messageSyncStep1
        ) {
          // sync step 1
          // can be safely ignored because we send the full initial state at the beginning
          console.debug("[ws] ignore sync step 1", { uid: user.id });
        } else {
          console.error("Unexpected message type", message);
        }
      },
      close: (ws, code, message) => {
        const user = ws.getUserData();
        console.debug("[ws] close", {
          uid: user.id,
          code,
          message: Buffer.from(message).toString(),
        });
        user.awarenessId &&
          client.addMessage(
            user.room,
            "index",
            Buffer.from(
              protocol.encodeAwarenessUserDisconnected(
                user.awarenessId,
                user.awarenessLastClock
              )
            )
          );
        user.isClosed = true;
        log(() => [
          "client connection closed (uid=",
          user.id,
          ", code=",
          code,
          ', message="',
          Buffer.from(message).toString(),
          '")',
        ]);
        user.subs.forEach((topic) => {
          console.debug("[ws] unsubscribe check", {
            topic,
            remainingSubscribers: app.numSubscribers(topic),
          });
          if (app.numSubscribers(topic) === 0) {
            subscriber.unsubscribe(topic, redisMessageSubscriber);
          }
        });
      },
    }
  );
  return new YWebsocketServer(app, client, subscriber);
};
