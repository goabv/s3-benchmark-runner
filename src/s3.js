import { Agent as HttpsAgent } from 'node:https';
import { Agent as HttpAgent } from 'node:http';
import dns from 'node:dns';
import { createRequire } from 'node:module';
import { S3Client } from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@smithy/node-http-handler';

const require = createRequire(import.meta.url);

/**
 * Build an undici-backed request handler (@smithy/undici-http-handler). Loaded
 * lazily so the default node handler path doesn't pull in undici. A custom
 * connector preserves the same hooks as the node path: per-socket IP capture
 * (onConnect) for connection/throughput logging, and the DNS-spreading lookup.
 */
function makeUndiciHandler({ maxSockets, onConnect, lookup, connectionTimeout, stampConn, ciphers, onTls }) {
  const { UndiciHttpHandler } = require('@smithy/undici-http-handler');
  const { buildConnector } = require('undici');
  const base = buildConnector({ timeout: connectionTimeout || 10_000, ...(ciphers ? { ciphers } : {}) });

  let connect;
  if (onConnect || lookup || stampConn || onTls) {
    const finish = (o, cb) =>
      base(o, (err, socket) => {
        // undici's connector returns an already-connected socket, so remoteAddress
        // is available now (the connect/secureConnect events have already fired).
        if (!err && socket) {
          if (stampConn) stampConn(socket);
          if (onTls && typeof socket.getProtocol === 'function') {
            try {
              onTls({ protocol: socket.getProtocol(), cipher: socket.getCipher?.()?.name ?? null });
            } catch {
              /* ignore */
            }
          }
          if (onConnect) {
            if (socket.remoteAddress) {
              onConnect(socket.remoteAddress, socket);
            } else {
              socket.once('connect', () => socket.remoteAddress && onConnect(socket.remoteAddress, socket));
            }
          }
        }
        cb(err, socket);
      });

    connect = (opts, cb) => {
      // undici ignores a per-call `lookup`, so spread connections by resolving the
      // hostname ourselves (round-robin over S3 IPs) and connecting to the chosen
      // IP, keeping SNI/cert validation on the original hostname.
      if (lookup && opts.hostname) {
        const original = opts.hostname;
        lookup(original, {}, (err, address) => {
          if (err || !address) return finish(opts, cb);
          finish({ ...opts, hostname: address, host: address, servername: opts.servername || original }, cb);
        });
      } else {
        finish(opts, cb);
      }
    };
  }

  const dispatcher = {
    connections: maxSockets, // max connections per origin
    pipelining: 1,
    headersTimeout: 0,
    bodyTimeout: 0,
  };
  if (connect) dispatcher.connect = connect;
  return new UndiciHttpHandler({ dispatcher });
}

/**
 * Connection-spreading DNS lookup.
 *
 * Node's default dns.lookup() returns a single address, so every socket to an S3
 * endpoint concentrates on one front-end IP and throughput is capped at that IP's
 * share of bandwidth. This custom lookup resolves ALL of the endpoint's A-records
 * and round-robins connections across them, so N concurrent connections fan out
 * over many S3 front-ends. Results are cached with a short TTL and refreshed so
 * the set of IPs rotates over the run (S3 returns different IPs across queries).
 */
const DNS_TTL_MS = 1000;
const dnsCache = new Map(); // hostname -> { ips: string[], ts: number, idx: number }

function spreadLookup(hostname, options, callback) {
  // Fall back to the default resolver for IPv6 requests or odd option shapes.
  if (options && options.family === 6) {
    return dns.lookup(hostname, options, callback);
  }

  const deliver = (entry) => {
    const ip = entry.ips[entry.idx % entry.ips.length];
    entry.idx += 1;
    if (options && options.all) callback(null, [{ address: ip, family: 4 }]);
    else callback(null, ip, 4);
  };

  const cached = dnsCache.get(hostname);
  if (cached && cached.ips.length && Date.now() - cached.ts < DNS_TTL_MS) {
    return deliver(cached);
  }

  dns.resolve4(hostname, (err, ips) => {
    if (err || !ips || !ips.length) {
      // Resolution failed (or non-resolvable host) — use the default path.
      return dns.lookup(hostname, options, callback);
    }
    const entry = { ips, ts: Date.now(), idx: cached ? cached.idx : 0 };
    dnsCache.set(hostname, entry);
    deliver(entry);
  });
}

/**
 * Create an S3 client tuned for high-throughput parallel range GETs.
 *
 * Each worker thread must create its own client — S3Client instances (and the
 * underlying sockets) should not be shared across threads.
 *
 * @param {object} opts
 * @param {string} [opts.region]        AWS region (falls back to env/instance metadata).
 * @param {number} [opts.maxSockets]    Max concurrent sockets per origin. Set this
 *                                      >= the concurrency you intend to drive.
 * @param {number} [opts.connectionTimeout] ms
 * @param {number} [opts.requestTimeout]    ms (0 = no timeout)
 * @returns {S3Client}
 */
/**
 * Build an agent that reports the remote IP of each new socket it opens (once per
 * connection). Lets the benchmark see how connections spread across S3 front-end
 * IPs — if they concentrate on a few, that caps throughput regardless of concurrency.
 */
function makeAgent(Base, opts, { onConnect, lookup, stampConn, onTls } = {}) {
  if (!onConnect && !lookup && !stampConn && !onTls) return new Base(opts);
  class CustomAgent extends Base {
    createConnection(options, cb) {
      // Inject the connection-spreading resolver for this socket.
      const connOpts = lookup ? { ...options, lookup } : options;
      const socket = super.createConnection(connOpts, cb);
      if (stampConn) stampConn(socket);
      if (onTls) {
        // getCipher()/getProtocol() are only meaningful after the TLS handshake.
        socket.once('secureConnect', () => {
          try {
            onTls({ protocol: socket.getProtocol?.() ?? null, cipher: socket.getCipher?.()?.name ?? null });
          } catch {
            /* ignore */
          }
        });
      }
      if (onConnect) {
        let recorded = false;
        const record = () => {
          if (!recorded && socket.remoteAddress) {
            recorded = true;
            onConnect(socket.remoteAddress, socket);
          }
        };
        socket.once('connect', record); // TCP (http)
        socket.once('secureConnect', record); // TLS (https)
      }
      return socket;
    }
  }
  return new CustomAgent(opts);
}

export function makeClient({
  region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
  maxSockets = 256,
  connectionTimeout = 5_000,
  requestTimeout = 0,
  validateChecksum = true,
  onConnect = null, // (remoteAddress) => void, called once per new socket
  spreadConnections = false, // fan connections out across all resolved S3 IPs
  tls = true, // false -> use S3's HTTP endpoint (no TLS) to measure TLS overhead
  httpHandler = 'node', // 'node' (@smithy/node-http-handler) | 'undici'
  captureSocket = false, // tag each socket + record its IP/id per request (part-times)
  connIdPrefix = '', // makes connection ids unique across worker threads
  ciphers = null, // OpenSSL cipher string to pin the TLS suite (null = defaults)
  onTls = null, // ({ protocol, cipher }) => void, fired once per new TLS socket
} = {}) {
  const agentOpts = { keepAlive: true, maxSockets, ...(ciphers ? { ciphers } : {}) };
  const lookup = spreadConnections ? spreadLookup : undefined;

  // Stamp a stable, unique id on each socket once. Keep-alive reuses the socket
  // object across parts, so the id reveals which parts shared a connection.
  let connCounter = 0;
  const stampConn = captureSocket
    ? (socket) => {
        if (socket && socket.__benchConnId === undefined) {
          socket.__benchConnId = `${connIdPrefix}c${++connCounter}`;
        }
      }
    : null;

  const requestHandler =
    httpHandler === 'undici'
      ? makeUndiciHandler({ maxSockets, onConnect, lookup, connectionTimeout, stampConn, ciphers, onTls })
      : new NodeHttpHandler({
          httpsAgent: makeAgent(HttpsAgent, agentOpts, { onConnect, lookup, stampConn, onTls }),
          httpAgent: makeAgent(HttpAgent, agentOpts, { onConnect, lookup, stampConn }),
          connectionTimeout,
          requestTimeout,
        });

  const config = {
    region,
    // WHEN_SUPPORTED: validate response checksums whenever S3 returns one (for a
    // PartNumber GET with ChecksumMode: ENABLED, that's the part's stored CRC32C,
    // validated against the streamed bytes as the body is read).
    // WHEN_REQUIRED: skip validation unless the operation mandates it — used to
    // measure raw download throughput without the checksum-verification cost.
    responseChecksumValidation: validateChecksum ? 'WHEN_SUPPORTED' : 'WHEN_REQUIRED',
    // Retries add latency variance to a throughput benchmark. Keep them low but
    // non-zero so a single transient error does not abort a whole run.
    maxAttempts: 3,
    requestHandler,
  };

  // Force the plaintext HTTP endpoint to measure the cost of TLS. The SDK keeps
  // virtual-hosted addressing, so this becomes http://<bucket>.s3.<region>.amazonaws.com.
  if (!tls) {
    config.endpoint = region ? `http://s3.${region}.amazonaws.com` : 'http://s3.amazonaws.com';
  }

  const client = new S3Client(config);

  // Capture the serving socket's remote IP + connection id per request. Runs at
  // the deserialize step, where the raw HTTP response body is still the node
  // socket-backed stream (before the checksum wrapper hides it). Result is stashed
  // on output.$benchConn. (undici doesn't expose a socket here, so it stays null.)
  if (captureSocket) {
    // Inner (deserialize) mw: response.body is still the raw socket-backed stream
    // here, so grab the socket and stash its IP/id on the shared response object.
    client.middlewareStack.add(
      (next) => async (args) => {
        const result = await next(args);
        try {
          const sock = result?.response?.body?.socket;
          if (sock) {
            stampConn(sock);
            result.response.$benchSocket = {
              vip: sock.remoteAddress ?? null,
              connId: sock.__benchConnId ?? null,
            };
          }
        } catch {
          /* best-effort */
        }
        return result;
      },
      { step: 'deserialize', priority: 'low', name: 'benchSocketGrab' },
    );
    // Outer (build) mw: output is now fully deserialized; copy the stashed socket
    // info onto it so the caller can read res.$benchConn.
    client.middlewareStack.add(
      (next) => async (args) => {
        const result = await next(args);
        if (result?.response?.$benchSocket && result.output) {
          result.output.$benchConn = result.response.$benchSocket;
        }
        return result;
      },
      { step: 'build', priority: 'low', name: 'benchSocketAttach' },
    );
  }

  return client;
}
