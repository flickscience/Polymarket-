
import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import http from "node:http";

const app = express();
const server = http.createServer(app);
const browserWss = new WebSocketServer({ noServer: true });

const PORT = Number(process.env.PORT || 3000);
const GAMMA = "https://gamma-api.polymarket.com";
const CLOB = "https://clob.polymarket.com";
const MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

app.use(express.static("public"));

const arr = value => {
  if (Array.isArray(value)) return value;
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

async function fetchJson(url, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: { accept: "application/json" }
    });

    if (!response.ok) {
      throw new Error(`API HTTP ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error("Polymarket API timed out. Try refreshing.");
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeMarket(m) {
  const outcomes = arr(m.outcomes);
  const tokens = arr(m.clobTokenIds ?? m.clob_token_ids);
  const prices = arr(m.outcomePrices);

  return {
    id: m.id,
    question: m.question || m.groupItemTitle || m.slug || "Market",
    slug: m.slug,
    active: m.active,
    closed: m.closed,
    outcomes: outcomes.map((name, i) => ({
      name: String(name),
      tokenId: tokens[i] ? String(tokens[i]) : null,
      gammaPrice: prices[i] != null ? Number(prices[i]) : null
    }))
  };
}

function normalizeEvent(e) {
  const markets = Array.isArray(e.markets) ? e.markets : [];
  const tags = Array.isArray(e.tags) ? e.tags : [];

  const title = e.title || e.name || e.slug || "Event";
  const tagNames = tags.map(t =>
    typeof t === "string" ? t : (t.label || t.slug || "")
  );

  const text = [
    title,
    e.slug,
    e.sport,
    e.category,
    e.seriesSlug,
    ...tagNames
  ].filter(Boolean).join(" ").toLowerCase();

  let sport = "Other";
  if (/cricket/.test(text)) sport = "Cricket";
  else if (/tennis/.test(text)) sport = "Tennis";
  else if (/soccer|football|epl|premier-league|la-liga|champions-league/.test(text)) {
    sport = "Soccer";
  }

  const explicitLive =
    e.live === true ||
    e.isLive === true ||
    /^(live|in play|in-play|in_progress|in progress)$/i.test(
      String(e.status || e.gameStatus || e.state || "")
    );

  const marketsNormalized = markets.map(normalizeMarket);

  return {
    id: e.id,
    title,
    slug: e.slug || "",
    sport,
    live: explicitLive,
    status: e.status || e.gameStatus || e.state || "",
    startDate: e.startDate || e.startTime || null,
    endDate: e.endDate || null,
    markets: marketsNormalized
  };
}

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

/*
 * Discover active, open events, then retain only events whose metadata
 * explicitly identifies them as live. "active" alone does not mean live.
 */
app.get("/api/events", async (_req, res) => {
  try {
    const url = new URL("/events", GAMMA);
    url.searchParams.set("active", "true");
    url.searchParams.set("closed", "false");
    url.searchParams.set("limit", "100");
    url.searchParams.set("order", "volume24hr");
    url.searchParams.set("ascending", "false");

    const data = await fetchJson(url.toString());
    if (!Array.isArray(data)) {
      throw new Error("Unexpected event API response");
    }

    const normalized = data.map(normalizeEvent);
    const liveEvents = normalized.filter(
      e => e.live && ["Soccer", "Cricket", "Tennis"].includes(e.sport)
    );

    res.set("Cache-Control", "no-store");
    res.json({
      events: liveEvents,
      count: liveEvents.length,
      fetchedAt: new Date().toISOString()
    });
  } catch (error) {
    console.error("Event discovery failed:", error.message);
    res.status(502).json({
      error: "Event discovery failed",
      detail: error.message
    });
  }
});

app.get("/api/event/:slug", async (req, res) => {
  try {
    const url = new URL("/events", GAMMA);
    url.searchParams.set("slug", req.params.slug);

    const data = await fetchJson(url.toString());
    const event = Array.isArray(data) ? data[0] : data;

    if (!event) {
      return res.status(404).json({ error: "Event not found" });
    }

    const normalized = normalizeEvent(event);
    res.json({ event: normalized, fetchedAt: new Date().toISOString() });
  } catch (error) {
    res.status(502).json({
      error: "Could not fetch event metadata",
      detail: error.message
    });
  }
});

app.get("/api/book/:tokenId", async (req, res) => {
  try {
    const url = new URL("/book", CLOB);
    url.searchParams.set("token_id", req.params.tokenId);

    res.json(await fetchJson(url.toString()));
  } catch (error) {
    res.status(502).json({
      error: "Could not fetch book",
      detail: error.message
    });
  }
});

const clients = new Set();
const subscribedIds = new Set();

let marketSocket;
let reconnectTimer;
let heartbeatTimer;

function broadcast(message) {
  const payload = JSON.stringify(message);

  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
}

function sendSubscriptions(ids) {
  if (!marketSocket || marketSocket.readyState !== WebSocket.OPEN || !ids.length) {
    return;
  }

  marketSocket.send(JSON.stringify({
    assets_ids: ids,
    type: "market",
    initial_dump: true
  }));
}

function connectMarketSocket() {
  clearTimeout(reconnectTimer);
  clearInterval(heartbeatTimer);

  marketSocket = new WebSocket(MARKET_WS);

  marketSocket.on("open", () => {
    console.log("Polymarket market feed connected");
    broadcast({ type: "connection", status: "connected" });

    sendSubscriptions([...subscribedIds]);

    heartbeatTimer = setInterval(() => {
      if (marketSocket?.readyState === WebSocket.OPEN) {
        marketSocket.send("PING");
      }
    }, 10000);
  });

  marketSocket.on("message", raw => {
    const message = raw.toString();
    if (message === "PONG") return;

    try {
      broadcast({
        type: "market_data",
        data: JSON.parse(message),
        receivedAt: Date.now()
      });
    } catch {
      broadcast({ type: "market_raw", data: message });
    }
  });

  marketSocket.on("close", () => {
    clearInterval(heartbeatTimer);
    broadcast({ type: "connection", status: "reconnecting" });
    reconnectTimer = setTimeout(connectMarketSocket, 3000);
  });

  marketSocket.on("error", error => {
    console.error("Market WebSocket error:", error.message);
    try {
      marketSocket.close();
    } catch {}
  });
}

browserWss.on("connection", client => {
  clients.add(client);

  client.send(JSON.stringify({
    type: "connection",
    status: marketSocket?.readyState === WebSocket.OPEN
      ? "connected"
      : "connecting"
  }));

  client.on("message", raw => {
    try {
      const message = JSON.parse(raw.toString());

      // Matches the current HTML: { type: "subscribe", tokenIds: [...] }
      if (message.type === "subscribe" && Array.isArray(message.tokenIds)) {
        const fresh = message.tokenIds
          .map(String)
          .filter(id => id && !subscribedIds.has(id));

        for (const id of fresh) subscribedIds.add(id);
        sendSubscriptions(fresh);
      }
    } catch (error) {
      console.error("Invalid browser message:", error.message);
    }
  });

  client.on("close", () => clients.delete(client));
});

server.on("upgrade", (request, socket, head) => {
  if (request.url === "/ws") {
    browserWss.handleUpgrade(request, socket, head, client => {
      browserWss.emit("connection", client, request);
    });
  } else {
    socket.destroy();
  }
});

connectMarketSocket();

server.listen(PORT, () => {
  console.log(`Dashboard listening on port ${PORT}`);
});
