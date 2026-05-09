import { PassThrough, Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { setDefaultResultOrder } from "node:dns";

export const config = {
  runtime: "nodejs",
  api: {
    bodyParser: false,
    responseLimit: false,
  },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");
const UPSTREAM_DNS_ORDER = (
  process.env.UPSTREAM_DNS_ORDER || "ipv4first"
)
  .trim()
  .toLowerCase();

const RELAY_PATH = normalizeRelayPath(
  process.env.RELAY_PATH || "/api"
);

const PUBLIC_RELAY_PATH = normalizeRelayPath(
  process.env.PUBLIC_RELAY_PATH || "/api"
);

const RELAY_KEY = (process.env.RELAY_KEY || "").trim();

const UPSTREAM_TIMEOUT_MS = parsePositiveInt(
  process.env.UPSTREAM_TIMEOUT_MS,
  30000,
  1000
);

const MAX_INFLIGHT = parsePositiveInt(
  process.env.MAX_INFLIGHT,
  256,
  1
);

const MAX_UP_BPS = parseNonNegativeInt(
  process.env.MAX_UP_BPS,
  0
);

const MAX_DOWN_BPS = parseNonNegativeInt(
  process.env.MAX_DOWN_BPS,
  0
);

const GLOBAL_UPLOAD_LIMITER = createGlobalLimiter(MAX_UP_BPS);
const GLOBAL_DOWNLOAD_LIMITER = createGlobalLimiter(MAX_DOWN_BPS);

applyDnsPreference();

const ALLOWED_METHODS = new Set([
  "GET",
  "HEAD",
  "POST",
  "PUT",
  "OPTIONS",
]);

const STRIP_HEADERS = new Set([
  "connection",
  "proxy-connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
]);

let inFlight = 0;

export default async function handler(req, res) {
  const requestId =
    `${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 8)}`;

  const startedAt = Date.now();

  let slotAcquired = false;

  try {
    if (!TARGET_BASE) {
      res.statusCode = 500;
      return res.end("TARGET_DOMAIN missing");
    }

    const host = req.headers.host || "localhost";

    const url = new URL(
      req.url || "/",
      `https://${host}`
    );

    const normalizedPath = normalizeIncomingPath(
      url.pathname
    );

    if (
      !isAllowedRelayPath(
        normalizedPath,
        PUBLIC_RELAY_PATH
      )
    ) {
      res.statusCode = 404;
      return res.end();
    }

    const upstreamPath = mapPublicPathToRelayPath(
      normalizedPath,
      PUBLIC_RELAY_PATH,
      RELAY_PATH
    );

    if (!ALLOWED_METHODS.has(req.method)) {
      res.statusCode = 405;
      return res.end();
    }

    if (RELAY_KEY) {
      const token = String(
        req.headers["x-relay-key"] || ""
      );

      if (token !== RELAY_KEY) {
        res.statusCode = 403;
        return res.end();
      }
    }

    if (!tryAcquireSlot()) {
      res.statusCode = 503;
      return res.end();
    }

    slotAcquired = true;

    const targetUrl =
      `${TARGET_BASE}${upstreamPath}${url.search || ""}`;

    const headers = {};

    for (const key of Object.keys(req.headers)) {
      const lower = key.toLowerCase();

      if (STRIP_HEADERS.has(lower)) continue;

      const value = req.headers[key];

      if (!value) continue;

      headers[lower] = Array.isArray(value)
        ? value.join(", ")
        : String(value);
    }

    // VERY IMPORTANT FOR XHTTP
    headers["host"] = new URL(TARGET_BASE).host;

    // preserve real ip
    if (req.headers["x-forwarded-for"]) {
      headers["x-forwarded-for"] = String(
        req.headers["x-forwarded-for"]
      );
    }

    const abortCtrl = new AbortController();

    const timeoutRef = setTimeout(() => {
      abortCtrl.abort("upstream_timeout");
    }, UPSTREAM_TIMEOUT_MS);

    try {
      const fetchOpts = {
        method: req.method,
        headers,
        redirect: "manual",
        signal: abortCtrl.signal,
      };

      const hasBody =
        req.method !== "GET" &&
        req.method !== "HEAD";

      if (hasBody) {
        let uploadStream = req;

        if (GLOBAL_UPLOAD_LIMITER) {
          uploadStream = req.pipe(
            createThrottleTransform(
              GLOBAL_UPLOAD_LIMITER
            )
          );
        }

        fetchOpts.body =
          Readable.toWeb(uploadStream);

        // IMPORTANT:
        // removed duplex: "half"
      }

      const upstream = await fetch(
        targetUrl,
        fetchOpts
      );

      res.statusCode = upstream.status;

      upstream.headers.forEach((value, key) => {
        const lower = key.toLowerCase();

        if (
          lower === "transfer-encoding" ||
          lower === "connection"
        ) {
          return;
        }

        try {
          res.setHeader(key, value);
        } catch {}
      });

      if (!upstream.body) {
        res.end();
      } else {
        const upstreamNode =
          Readable.fromWeb(upstream.body);

        let output = upstreamNode;

        if (GLOBAL_DOWNLOAD_LIMITER) {
          output = upstreamNode.pipe(
            createThrottleTransform(
              GLOBAL_DOWNLOAD_LIMITER
            )
          );
        }

        await pipeline(output, res);
      }

      const durationMs = Date.now() - startedAt;

      console.log("relay", {
        requestId,
        path: normalizedPath,
        upstreamPath,
        status: upstream.status,
        durationMs,
      });
    } catch (err) {
      if (
        err?.name === "AbortError" ||
        err === "upstream_timeout"
      ) {
        console.error("upstream timeout", {
          requestId,
        });

        if (!res.headersSent) {
          res.statusCode = 504;
          return res.end();
        }

        return;
      }

      console.error("relay error", {
        requestId,
        error: String(err),
      });

      if (!res.headersSent) {
        res.statusCode = 502;
        return res.end();
      }
    } finally {
      clearTimeout(timeoutRef);
    }
  } catch (err) {
    console.error("fatal relay error", err);

    if (!res.headersSent) {
      res.statusCode = 500;
      res.end();
    }
  } finally {
    if (slotAcquired) {
      releaseSlot();
    }
  }
}

function applyDnsPreference() {
  try {
    if (
      UPSTREAM_DNS_ORDER === "ipv4first" ||
      UPSTREAM_DNS_ORDER === "verbatim"
    ) {
      setDefaultResultOrder(
        UPSTREAM_DNS_ORDER
      );
    }
  } catch {}
}

function normalizeRelayPath(rawPath) {
  if (!rawPath) return "";

  let path = rawPath;

  if (!path.startsWith("/")) {
    path = `/${path}`;
  }

  if (
    path.length > 1 &&
    path.endsWith("/")
  ) {
    path = path.slice(0, -1);
  }

  return path;
}

function normalizeIncomingPath(pathname) {
  if (!pathname) return "/";

  let normalized = String(pathname)
    .replace(/\/{2,}/g, "/");

  if (!normalized.startsWith("/")) {
    normalized = `/${normalized}`;
  }

  if (
    normalized.length > 1 &&
    normalized.endsWith("/")
  ) {
    normalized = normalized.slice(0, -1);
  }

  return normalized;
}

function isAllowedRelayPath(
  pathname,
  publicPath
) {
  return (
    pathname === publicPath ||
    pathname.startsWith(`${publicPath}/`)
  );
}

function mapPublicPathToRelayPath(
  pathname,
  publicPath,
  relayPath
) {
  if (pathname === publicPath) {
    return relayPath;
  }

  const suffix = pathname.slice(
    publicPath.length
  );

  return `${relayPath}${suffix}`;
}

function parsePositiveInt(
  rawValue,
  fallbackValue,
  minValue
) {
  const value = Number(rawValue);

  if (!Number.isFinite(value)) {
    return fallbackValue;
  }

  if (value < minValue) {
    return fallbackValue;
  }

  return Math.trunc(value);
}

function parseNonNegativeInt(
  rawValue,
  fallbackValue
) {
  const value = Number(rawValue);

  if (!Number.isFinite(value)) {
    return fallbackValue;
  }

  if (value < 0) {
    return fallbackValue;
  }

  return Math.trunc(value);
}

function tryAcquireSlot() {
  if (inFlight >= MAX_INFLIGHT) {
    return false;
  }

  inFlight += 1;

  return true;
}

function releaseSlot() {
  inFlight = Math.max(
    0,
    inFlight - 1
  );
}

function createGlobalLimiter(
  bytesPerSecond
) {
  if (
    !Number.isFinite(bytesPerSecond) ||
    bytesPerSecond <= 0
  ) {
    return null;
  }

  const burstCap = Math.max(
    bytesPerSecond,
    262144
  );

  let tokens = burstCap;
  let lastRefill = Date.now();

  const queue = [];

  let timer = null;

  function refill() {
    const now = Date.now();

    const elapsed = now - lastRefill;

    if (elapsed <= 0) return;

    const refillAmount =
      (elapsed * bytesPerSecond) / 1000;

    tokens = Math.min(
      burstCap,
      tokens + refillAmount
    );

    lastRefill = now;
  }

  function drain() {
    refill();

    while (
      queue.length &&
      tokens >= 1
    ) {
      const item = queue[0];

      const grant = Math.min(
        item.maxBytes,
        Math.max(1, Math.floor(tokens))
      );

      tokens -= grant;

      queue.shift();

      item.resolve(grant);
    }
  }

  function schedule() {
    if (timer) return;

    timer = setTimeout(() => {
      timer = null;

      drain();

      if (queue.length) {
        schedule();
      }
    }, 5);
  }

  return {
    acquire(maxBytes) {
      return new Promise((resolve) => {
        queue.push({
          maxBytes,
          resolve,
        });

        drain();

        if (queue.length) {
          schedule();
        }
      });
    },
  };
}

function createThrottleTransform(
  limiter
) {
  if (!limiter) {
    return new PassThrough();
  }

  return new Transform({
    transform(chunk, _enc, cb) {
      (async () => {
        let offset = 0;

        while (
          offset < chunk.length
        ) {
          const grant =
            await limiter.acquire(
              chunk.length - offset
            );

          const piece = chunk.subarray(
            offset,
            offset + grant
          );

          offset += grant;

          this.push(piece);
        }
      })()
        .then(() => cb())
        .catch((err) => cb(err));
    },
  });
}
