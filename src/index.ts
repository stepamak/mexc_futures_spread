import WebSocket from 'ws';
import https from 'https';
import fs from 'fs';
import path from 'path';
import protobuf from 'protobufjs';

type PriceMap = Map<string, number>;
type Ts = number;

const SPOT_WS_URL = process.env.SPOT_WS_URL || 'wss://wbs-api.mexc.com/ws';
const FUT_WS_URL  = process.env.FUT_WS_URL  || 'wss://contract.mexc.com/edge';
const PROTO_DIR   = path.resolve(process.cwd(), 'proto');

const RAW_BASE = 'https://raw.githubusercontent.com/mexcdevelop/websocket-proto/main/';
const CDN_BASE = 'https://cdn.jsdelivr.net/gh/mexcdevelop/websocket-proto@main/';

const LOG_SPREAD_THRESHOLD_PCT = Number(process.env.SPREAD_PCT || 0.25); // %
const MAX_SUBS_PER_SOCKET = Number(process.env.MAX_SUBS_PER_SOCKET || 25);
const RESUBSCRIBE_DELAY_MS = 300;

// ---------- utils ----------
const uA = { 'User-Agent':'curl/8.4', 'Accept':'*/*', 'Connection':'close' };

function delay(ms: number) { return new Promise(res => setTimeout(res, ms)); }

function toSpotFromFutSymbol(contract: string): string {
  // "BTC_USDT" -> "BTCUSDT", "TON_USDC" -> "TONUSDC"
  return contract.replace('_', '');
}

function computeSpread(spot?: number, fut?: number) {
  if (!spot || !fut) return;
  const diff = fut - spot;
  const pct = diff / spot * 100;
  return { diff, pct };
}

async function fetchText(url: string): Promise<string|null> {
  return await new Promise(resolve => {
    const req = https.get(url, { headers: uA }, (res) => {
      if (res.statusCode !== 200) { res.resume(); return resolve(null); }
      let data=''; res.setEncoding('utf8');
      res.on('data', c=>data+=c); res.on('end', ()=>resolve(data));
    });
    req.on('error', ()=>resolve(null));
    req.setTimeout(15000, ()=>{ req.destroy(); resolve(null); });
  });
}

function parseImports(protoText: string): string[] {
  const out: string[] = [];
  const re = /import\s+"([^"]+)"/g; let m;
  while ((m = re.exec(protoText))) out.push(m[1]);
  return Array.from(new Set(out));
}

async function downloadFile(name: string): Promise<string> {
  if (!fs.existsSync(PROTO_DIR)) fs.mkdirSync(PROTO_DIR, { recursive: true });
  const target = path.join(PROTO_DIR, name);
  if (fs.existsSync(target)) return target;

  const tryUrls = [`${RAW_BASE}${name}`, `${CDN_BASE}${name}`];
  for (const url of tryUrls) {
    const ok = await new Promise<boolean>((resolve) => {
      const req = https.get(url, { headers: uA }, (res) => {
        if (res.statusCode !== 200) { res.resume(); return resolve(false); }
        const file = fs.createWriteStream(target);
        res.pipe(file);
        file.on('finish', () => file.close(()=>resolve(true)));
      });
      req.on('error', ()=>resolve(false));
      req.setTimeout(15000, ()=>{ req.destroy(); resolve(false); });
    });
    if (ok) return target;
  }
  throw new Error(`Не удалось скачать ${name}`);
}

async function prepareProtos(): Promise<{ files: string[], Wrapper: protobuf.Type }> {
  const wrapperName = 'PushDataV3ApiWrapper.proto';
  const wrapperText = await fetchText(`${RAW_BASE}${wrapperName}`) || await fetchText(`${CDN_BASE}${wrapperName}`);
  if (!wrapperText) throw new Error(`Не удалось скачать ${wrapperName}`);

  const imports = parseImports(wrapperText);
  const files: string[] = [];
  for (const name of imports) files.push(await downloadFile(name));
  files.push(await downloadFile(wrapperName));

  const root = await protobuf.load(files);
  const Wrapper = root.lookupType('PushDataV3ApiWrapper') as protobuf.Type;
  return { files, Wrapper };
}

// ---------- Spot shard manager (bookTicker@10ms) ----------
class SpotShardManager {
  private sockets: WebSocket[] = [];
  private subsPerSocket: number[] = [];
  private symbolToSocketIdx: Map<string, number> = new Map();
  private readonly maxPerSocket: number;
  private readonly onMidPrice: (symbol: string, mid: number, ts: Ts) => void;
  private readonly Wrapper: protobuf.Type;

  constructor(opts: { maxPerSocket: number; Wrapper: protobuf.Type;
                     onMidPrice: (sym:string, mid:number, ts:Ts)=>void }) {
    this.maxPerSocket = opts.maxPerSocket;
    this.onMidPrice   = opts.onMidPrice;
    this.Wrapper      = opts.Wrapper;
  }

  private ensureSocket(idx: number) {
    if (this.sockets[idx] && this.sockets[idx].readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(SPOT_WS_URL);
    this.sockets[idx] = ws;
    this.subsPerSocket[idx] = 0;

    ws.on('open', () => {
      // resubscribe assigned symbols
      const assigned = [...this.symbolToSocketIdx.entries()]
        .filter(([, i]) => i === idx)
        .map(([s]) => s);
      let i = 0;
      const flush = () => {
        if (i >= assigned.length) return;
        const sym = assigned[i++];
        this.sendSub(ws, sym);
        setTimeout(flush, RESUBSCRIBE_DELAY_MS);
      };
      flush();

      setInterval(()=> ws.send(JSON.stringify({ method: 'PING' })), 30000);
    });

    ws.on('message', (data, isBinary) => {
      if (!isBinary) return;
      try {
        const buf = Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
        const msg: any = this.Wrapper.decode(buf);

        // В wrapper бывают разные поля. Нас интересует bookTicker (aggregated).
        // Ищем по нескольким вариантам, т.к. схемы у MEXC могут отличаться по имени:
        const b =
          msg?.publicAggreBookTicker
          || msg?.publicBookTicker
          || msg?.publicBookTickerBatch
          || msg?.publicAggreBookTickerV3Api
          || msg?.publicBookTickerV3Api;

        if (!b) return;

        const processItem = (it: any) => {
          const symbol: string = String(it.symbol || it.symbolName || it.s || '');
          const bid = Number(it.bidPrice ?? it.bestBidPrice ?? it.bp ?? it.bid);
          const ask = Number(it.askPrice ?? it.bestAskPrice ?? it.ap ?? it.ask);
          if (!symbol || !Number.isFinite(bid) || !Number.isFinite(ask)) return;
          const mid = (bid + ask) / 2;
          this.onMidPrice(symbol, mid, Date.now());
        };

        if (Array.isArray(b.items)) b.items.forEach(processItem);
        else processItem(b);
      } catch {}
    });

    ws.on('close', () => {
      // Пересоздаём при закрытии; подписки восстановим в on('open')
      setTimeout(()=> this.ensureSocket(idx), 1000);
    });

    ws.on('error', () => {/* no-op, reconnect in close */});
  }

  private sendSub(ws: WebSocket, symbol: string) {
    const channel = `spot@public.aggre.bookTicker.v3.api.pb@10ms@${symbol}`;
    ws.send(JSON.stringify({ method: 'SUBSCRIPTION', params: [channel] }));
  }

  async subscribe(symbol: string) {
    if (this.symbolToSocketIdx.has(symbol)) return; // уже подписаны
    // найдем сокет с местом
    let idx = this.subsPerSocket.findIndex(n => n < this.maxPerSocket);
    if (idx === -1) {
      idx = this.sockets.length;
      this.ensureSocket(idx);
    } else {
      this.ensureSocket(idx);
    }
    const ws = this.sockets[idx];
    if (ws && ws.readyState === WebSocket.OPEN) {
      this.sendSub(ws, symbol);
      this.subsPerSocket[idx]++;
      this.symbolToSocketIdx.set(symbol, idx);
    } else {
      // сокет еще не открыт — просто закрепим; подпишемся в on('open')
      this.subsPerSocket[idx]++;
      this.symbolToSocketIdx.set(symbol, idx);
    }
  }

  async subscribeBatch(symbols: string[]) {
    for (const s of symbols) await this.subscribe(s);
  }
}

// ---------- main ----------
async function main() {
  // prices + timestamps, чтобы отсечь мусор
  const spotMid: Map<string, { p: number; t: Ts }> = new Map();
  const futLast: Map<string, { p: number; t: Ts }>  = new Map();

  // 1) Прото
  const { Wrapper } = await prepareProtos();

  // 2) Spot shard manager (10ms bookTicker)
  const spotMgr = new SpotShardManager({
    maxPerSocket: MAX_SUBS_PER_SOCKET,
    Wrapper,
    onMidPrice: (sym, mid, ts) => {
      spotMid.set(sym, { p: mid, t: ts });
      // если есть фьючи — сразу считаем спред
      const fut = futLast.get(sym)?.p;
      const s = computeSpread(mid, fut);
      if (s && Math.abs(s.pct) >= LOG_SPREAD_THRESHOLD_PCT) {
        console.log(`${sym}: spot=${mid} fut=${fut} diff=${s.diff.toFixed(6)} (${s.pct.toFixed(4)}%)`);
      }
    }
  });

  // 3) Futures (tickers → список контрактов и lastPrice/fairPrice)
  const futWs = new WebSocket(FUT_WS_URL);
  // набор тех спот-символов, на которые уже подписались в spot
  const subscribedSpot: Set<string> = new Set();

  futWs.on('open', () => {
    console.log('FUTURES WS открыт');
    futWs.send(JSON.stringify({ method: 'sub.tickers', param: {}, gzip: false }));

    setInterval(() => futWs.send(JSON.stringify({ method: 'ping' })), 15000);
  });

  futWs.on('message', async (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.channel === 'push.tickers' && Array.isArray(msg.data)) {
        const now = Date.now();

        // 3.1 обновим цены фьючей
        for (const t of msg.data) {
          const contr = String(t.symbol);            // "BTC_USDT"
          const spotKey = toSpotFromFutSymbol(contr); // "BTCUSDT"
          const price = Number(t.fairPrice ?? t.lastPrice ?? t.lastPricePx ?? t.px);
          if (!Number.isFinite(price)) continue;
          futLast.set(spotKey, { p: price, t: now });

          // если ещё не подписаны на соответствующий spot — подписываемся
          if (!subscribedSpot.has(spotKey)) {
            subscribedSpot.add(spotKey);
            // небольшая пауза между подписками, чтобы не схлопотать rate limit
            await spotMgr.subscribe(spotKey);
            await delay(RESUBSCRIBE_DELAY_MS);
          }

          // мгновенная проверка спреда, если есть свежий спот
          const sm = spotMid.get(spotKey)?.p;
          const s = computeSpread(sm, price);
          if (s && Math.abs(s.pct) >= LOG_SPREAD_THRESHOLD_PCT) {
            console.log(`${spotKey}: spot=${sm} fut=${price} diff=${s.diff.toFixed(6)} (${s.pct.toFixed(4)}%)`);
          }
        }
      }
    } catch {}
  });

  futWs.on('close', () => console.log('FUTURES WS закрыт'));
  futWs.on('error', (e) => console.error('FUTURES WS ошибка', e));

  // 4) house-keeping: фильтруем очевидные артефакты
  setInterval(() => {
    const now = Date.now();
    // вычистим слишком старые значения (например, > 10 сек)
    for (const [k, v] of spotMid) if (now - v.t > 10000) spotMid.delete(k);
    for (const [k, v] of futLast) if (now - v.t > 10000) futLast.delete(k);
  }, 5000);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
