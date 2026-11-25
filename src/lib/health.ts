import { getDb } from "./db";
import { horizonHost, type Network } from "./horizon";

export interface Check {
  name: string;
  state: "ok" | "degraded" | "down";
  ms?: number;
  detail?: string;
}

async function timed(fn: () => Promise<unknown>) {
  const t0 = Date.now();
  await fn();
  return Date.now() - t0;
}

/** Live checks of Horizon, the price source, the database and indexer lag. */
/**
 * `fresh` (default) bypasses every cache, for /api/health monitors. The cached status page passes
 * false: an uncached fetch would make Next turn the cached page dynamic and fail at runtime.
 */
export async function healthChecks(net: Network, fresh = true): Promise<Check[]> {
  const opts = (ms: number): RequestInit => (fresh ? { cache: "no-store", signal: AbortSignal.timeout(ms) } : { next: { revalidate: 30 }, signal: AbortSignal.timeout(ms) });
  const out: Check[] = [];
  try {
    const ms = await timed(() => fetch(`${horizonHost(net)}/`, opts(8000)).then((r) => { if (!r.ok) throw new Error(String(r.status)); }));
    out.push({ name: "Pi Horizon API", state: ms > 2500 ? "degraded" : "ok", ms });
  } catch (e) {
    out.push({ name: "Pi Horizon API", state: "down", detail: String((e as Error).message) });
  }
  try {
    const ms = await timed(() => fetch("https://www.okx.com/api/v5/market/ticker?instId=PI-USDT", opts(6000)).then((r) => { if (!r.ok) throw new Error(String(r.status)); }));
    out.push({ name: "OKX price", state: ms > 2500 ? "degraded" : "ok", ms });
  } catch (e) {
    out.push({ name: "OKX price", state: "down", detail: String((e as Error).message) });
  }
  const sql = getDb();
  if (sql) {
    try {
      let rows: { stream: string; cursor: string; age: number }[] = [];
      const ms = await timed(async () => {
        rows = await sql<{ stream: string; cursor: string; age: number }[]>`
          select stream, cursor, extract(epoch from now() - updated_at)::int as age from sync_state where net = ${net}`;
      });
      out.push({ name: "Database", state: ms > 2000 ? "degraded" : "ok", ms });
      for (const s of ["ledgers", "operations"]) {
        const r = rows.find((x) => x.stream === s);
        out.push({
          name: `Indexer: ${s}`,
          state: !r ? "down" : r.age > 1800 ? "down" : r.age > 900 ? "degraded" : "ok",
          detail: r ? `${Math.round(r.age / 60)} min` : "not started",
        });
      }
      for (const s of ["ledgers_bf", "operations_bf"]) {
        const r = rows.find((x) => x.stream === s);
        const end = rows.find((x) => x.stream === `${s}_end`);
        let detail = r ? "running" : "not started";
        if (r && end && r.cursor && end.cursor) {
          try {
            detail = BigInt(r.cursor) >= BigInt(end.cursor) ? "done" : "running";
          } catch {
            /* claimable cursors are not numbers */
          }
        }
        out.push({ name: `Backfill: ${s}`, state: r ? "ok" : "degraded", detail });
      }
      // Browser errors reported by visitors in the last 24 hours (our own error monitoring).
      const [e24] = await sql<{ n: number; last: string | null }[]>`
        select count(*)::int as n, max(message) filter (where at = (select max(at) from client_errors)) as last
        from client_errors where at > now() - interval '24 hours'`.catch(() => [{ n: 0, last: null }]);
      out.push({ name: "Browser errors (24h)", state: e24.n > 50 ? "degraded" : "ok", detail: e24.n ? `${e24.n}${e24.last ? ` · ${e24.last.slice(0, 60)}` : ""}` : "0" });
    } catch (e) {
      out.push({ name: "Database", state: "down", detail: String((e as Error).message).slice(0, 80) });
    }
  } else {
    out.push({ name: "Database", state: "degraded", detail: "not configured" });
  }
  return out;
}
