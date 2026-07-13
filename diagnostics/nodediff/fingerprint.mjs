// Print the version fingerprint of the running node — the things that change
// between releases and can explain a download-throughput regression.
const v = process.versions;
const out = {
  node: v.node,
  v8: v.v8,
  openssl: v.openssl,   // bulk TLS crypto + handshake live here
  uv: v.uv,             // libuv (event loop, threadpool)
  undici: v.undici,     // bundled undici (used by global fetch)
  modules: v.modules,
  llhttp: v.llhttp,     // HTTP parser
  defaultCiphers: (process.binding ? undefined : undefined),
};
console.log(JSON.stringify(out));
