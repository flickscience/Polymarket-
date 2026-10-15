import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import http from "node:http";

const app = express(), server = http.createServer(app);
const browserWss = new WebSocketServer({ noServer: true });
const PORT = Number(process.env.PORT || 3000);
const GAMMA = "https://gamma-api.polymarket.com";
const CLOB = "https://clob.polymarket.com";
const MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
app.use(express.static("public"));
const arr = v => { if (Array.isArray(v)) return v; try { const x=JSON.parse(v); return Array.isArray(x)?x:[]; } catch { return []; } };

function normalizeMarket(m) {
  const outcomes=arr(m.outcomes), tokens=arr(m.clobTokenIds ?? m.clob_token_ids), prices=arr(m.outcomePrices);
  return { id:m.id, question:m.question||m.groupItemTitle||m.slug||"Market", slug:m.slug,
    marketType:m.sportsMarketType||m.marketType||m.groupItemTitle||"", description:m.description||"",
    active:m.active, closed:m.closed, volume:Number(m.volumeNum??m.volume??0), liquidity:Number(m.liquidityNum??m.liquidity??0),
    outcomes:outcomes.map((name,i)=>({name:String(name),tokenId:tokens[i]?String(tokens[i]):null,gammaPrice:prices[i]!=null?Number(prices[i]):null})),
    raw:{groupItemTitle:m.groupItemTitle||null,sportsMarketType:m.sportsMarketType||null,eventSlug:m.eventSlug||null} };
}
app.get("/api/event/:slug", async (req,res)=>{
  try {
    const u=new URL("/events",GAMMA); u.searchParams.set("slug",req.params.slug);
    const r=await fetch(u,{headers:{accept:"application/json"}}); if(!r.ok) throw Error(`Gamma HTTP ${r.status}`);
    const d=await r.json(), e=Array.isArray(d)?d[0]:d; if(!e) return res.status(404).json({error:"Event not found"});
    let markets=Array.isArray(e.markets)?e.markets:[];
    if(!markets.length && e.id){const mu=new URL("/markets",GAMMA);mu.searchParams.set("event_id",String(e.id));mu.searchParams.set("limit","100");const mr=await fetch(mu);if(mr.ok)markets=await mr.json();}
    res.json({event:{id:e.id,title:e.title||e.name||req.params.slug,slug:e.slug||req.params.slug,startDate:e.startDate||e.startTime||null,endDate:e.endDate||null,active:e.active,closed:e.closed},markets:markets.map(normalizeMarket),fetchedAt:new Date().toISOString()});
  } catch(err){res.status(502).json({error:"Could not fetch event metadata",detail:err.message});}
});
app.get("/api/book/:tokenId",async(req,res)=>{try{const u=new URL("/book",CLOB);u.searchParams.set("token_id",req.params.tokenId);const r=await fetch(u);if(!r.ok)throw Error(`CLOB HTTP ${r.status}`);res.json(await r.json());}catch(err){res.status(502).json({error:"Could not fetch book",detail:err.message});}});
app.get("/api/health",(_req,res)=>res.json({ok:true,time:new Date().toISOString()}));

const clients=new Set();let marketSocket,reconnectTimer;const subscribedIds=new Set();
function broadcast(obj){const s=JSON.stringify(obj);for(const c of clients)if(c.readyState===WebSocket.OPEN)c.send(s);}
function connectMarketSocket(){
  clearTimeout(reconnectTimer); marketSocket=new WebSocket(MARKET_WS);
  marketSocket.on("open",()=>{broadcast({type:"connection",status:"connected"});if(subscribedIds.size)marketSocket.send(JSON.stringify({assets_ids:[...subscribedIds],type:"market",initial_dump:true}));marketSocket.heartbeat=setInterval(()=>{if(marketSocket?.readyState===WebSocket.OPEN)marketSocket.send("PING");},10000);});
  marketSocket.on("message",d=>{const s=d.toString();if(s==="PONG")return;try{broadcast({type:"market_data",data:JSON.parse(s),receivedAt:Date.now()});}catch{broadcast({type:"market_raw",data:s});}});
  marketSocket.on("close",()=>{clearInterval(marketSocket?.heartbeat);broadcast({type:"connection",status:"reconnecting"});reconnectTimer=setTimeout(connectMarketSocket,1500);});
  marketSocket.on("error",()=>{try{marketSocket.close();}catch{}});
}
browserWss.on("connection",ws=>{clients.add(ws);ws.send(JSON.stringify({type:"connection",status:marketSocket?.readyState===WebSocket.OPEN?"connected":"connecting"}));ws.on("message",raw=>{try{const m=JSON.parse(raw.toString());if(m.type==="subscribe"&&Array.isArray(m.tokenIds)){const fresh=m.tokenIds.map(String).filter(id=>id&&!subscribedIds.has(id));fresh.forEach(id=>subscribedIds.add(id));if(fresh.length&&marketSocket?.readyState===WebSocket.OPEN)marketSocket.send(JSON.stringify({assets_ids:fresh,type:"market",initial_dump:true}));}}catch{}});ws.on("close",()=>clients.delete(ws));});
server.on("upgrade",(req,socket,head)=>{if(req.url==="/ws")browserWss.handleUpgrade(req,socket,head,ws=>browserWss.emit("connection",ws,req));else socket.destroy();});
connectMarketSocket();server.listen(PORT,()=>console.log(`Dashboard running at http://localhost:${PORT}`));
