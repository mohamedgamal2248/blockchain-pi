/**
 * Statistics indexer. Reads Pi's Horizon API in ascending pages and keeps
 * small aggregate tables in Postgres up to date. Each page and its cursor are
 * written in one transaction, so a crash never double-counts.
 *
 * Storage is kept small on purpose (the database may share a free-tier quota):
 * only lockups that are still locked, daily aggregates, large transfers and the
 * largest holders are stored — never every operation.
 *
 * Imports are relative so the same code runs from Next routes and from `tsx scripts/ingest.ts`.
 */
import type postgres from "postgres";
import { knownWallet, knownWallets } from "../data/wallets";
import type { Sql } from "./db";
import { horizonHost, type Ledger, type Network, type Operation } from "./horizon";

const PAGE = 200;
// The lockup ("claimable") stream is retired: per-balance rows grew past the free database size,
// so only a small summary is kept (lockup_days / lockup_top) and nothing fetches lockups any more.
export const STREAMS = ["ledgers", "operations", "ledgers_bf", "operations_bf"] as const;
export type Stream = (typeof STREAMS)[number];
type Base = "ledgers" | "operations";
const baseOf = (s: Stream): Base => (s.startsWith("ledgers") ? "ledgers" : "operations");

export function largeThreshold() {
  const n = Number(process.env.LARGE_PAYMENT_MIN || 10000);
  return Number.isFinite(n) && n > 0 ? n : 10000;
}
/** Accounts that move at least this much Pi are tracked for the rich list. */
const HOLDER_MIN = Number(process.env.HOLDER_MIN || 1000);
const HOLDERS_KEEP = 5000;
const BACKFILL_DAYS = Number(process.env.BACKFILL_DAYS || 90);
const LEDGERS_PER_DAY = 17280; // ~5 s per ledger

async function fetchJson<T>(net: Network, path: string, params: Record<string, string> = {}): Promise<T> {
  const url = new URL(path, horizonHost(net));
  for (const [k, v] of Object.entries(params)) if (v) url.searchParams.set(k, v);
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(url, { cache: "no-store", headers: { Accept: "application/json" }, signal: AbortSignal.timeout(15_000) });
    if (res.ok) return (await res.json()) as T;
    if ((res.status === 429 || res.status >= 500) && attempt < 3) {
      await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
      continue;
    }
    const err = new Error(`Horizon ${res.status} for ${url.pathname}`) as Error & { status?: number };
    err.status = res.status;
    throw err;
  }
}

const fetchRecords = async <T,>(net: Network, path: string, params: Record<string, string>) =>
  (await fetchJson<{ _embedded: { records: T[] } }>(net, path, params))._embedded.records;

const PATHS: Record<Base, { path: string; params: Record<string, string> }> = {
  ledgers: { path: "/ledgers", params: {} },
  operations: { path: "/operations", params: {} },
};

async function getState(sql: Sql, net: Network, stream: string) {
  const rows = await sql<{ cursor: string }[]>`select cursor from sync_state where net = ${net} and stream = ${stream}`;
  return rows[0]?.cursor;
}
async function setState(sql: Sql | postgres.TransactionSql, net: Network, stream: string, cursor: string) {
  await sql`
    insert into sync_state (net, stream, cursor, updated_at) values (${net}, ${stream}, ${cursor}, now())
    on conflict (net, stream) do update set cursor = excluded.cursor, updated_at = now()`;
}

/**
 * Where a stream starts the first time. Live streams start at the newest record; lockups at the
 * very beginning; backfills BACKFILL_DAYS ago and stop where the live stream started.
 */
async function loadCursor(sql: Sql, net: Network, stream: Stream): Promise<string> {
  const have = await getState(sql, net, stream);
  if (have !== undefined) return have;
  let cursor = "";
  if (stream === "ledgers" || stream === "operations") {
    const latest = await fetchRecords<{ paging_token: string }>(net, PATHS[stream].path, { order: "desc", limit: "1" });
    cursor = latest[0]?.paging_token ?? "";
  } else if (stream === "ledgers_bf" || stream === "operations_bf") {
    const live = stream === "ledgers_bf" ? "ledgers" : "operations";
    const end = (await getState(sql, net, `${stream}_end`)) ?? (await loadCursor(sql, net, live));
    await setState(sql, net, `${stream}_end`, end);
    const latest = await fetchRecords<Ledger>(net, "/ledgers", { order: "desc", limit: "1" });
    const startSeq = Math.max(1, (latest[0]?.sequence ?? 1) - BACKFILL_DAYS * LEDGERS_PER_DAY);
    cursor = (BigInt(startSeq) << 32n).toString();
  }
  await sql`insert into sync_state (net, stream, cursor) values (${net}, ${stream}, ${cursor}) on conflict do nothing`;
  return cursor;
}

type DayAgg = {
  ledgers: number; txs: number; failed_txs: number; ops: number; payments: number;
  payment_volume: number; new_accounts: number; locks_created: number; locked_amount: number; claims: number;
};
const emptyDay = (): DayAgg => ({
  ledgers: 0, txs: 0, failed_txs: 0, ops: 0, payments: 0, payment_volume: 0, new_accounts: 0, locks_created: 0, locked_amount: 0, claims: 0,
});

async function addDays(tx: postgres.TransactionSql, net: Network, days: Map<string, DayAgg>) {
  for (const [day, d] of days) {
    await tx`
      insert into daily_stats ${tx({ net, day, ...d, payment_volume: d.payment_volume.toFixed(7), locked_amount: d.locked_amount.toFixed(7) })}
      on conflict (net, day) do update set
        ledgers = daily_stats.ledgers + excluded.ledgers,
        txs = daily_stats.txs + excluded.txs,
        failed_txs = daily_stats.failed_txs + excluded.failed_txs,
        ops = daily_stats.ops + excluded.ops,
        payments = daily_stats.payments + excluded.payments,
        payment_volume = daily_stats.payment_volume + excluded.payment_volume,
        new_accounts = daily_stats.new_accounts + excluded.new_accounts,
        locks_created = daily_stats.locks_created + excluded.locks_created,
        locked_amount = daily_stats.locked_amount + excluded.locked_amount,
        claims = daily_stats.claims + excluded.claims`;
  }
}

type FlowAgg = { inflow: number; outflow: number; n_in: number; n_out: number };

const day = (iso: string) => iso.slice(0, 10);
const num = (v: unknown) => Number(typeof v === "string" ? v : 0) || 0;

/** Applies one page of a stream inside a transaction. Exported for tests. */
export async function applyPage(sql: Sql, net: Network, stream: Stream, records: unknown[], threshold = largeThreshold()) {
  const base = baseOf(stream);
  const days = new Map<string, DayAgg>();
  const bump = (iso: string) => {
    const k = day(iso);
    let d = days.get(k);
    if (!d) days.set(k, (d = emptyDay()));
    return d;
  };
  const flows = new Map<string, FlowAgg>();
  const flow = (address: string, iso: string) => {
    const k = `${address}|${day(iso)}`;
    let f = flows.get(k);
    if (!f) flows.set(k, (f = { inflow: 0, outflow: 0, n_in: 0, n_out: 0 }));
    return f;
  };
  const holders = new Set<string>();
  const hourly = new Map<string, { exch_in: number; exch_out: number; found_out: number; found_to_exch: number; large_in_n: number }>();
  const hour = (iso: string) => {
    const k = `${iso.slice(0, 13)}:00:00Z`;
    let h = hourly.get(k);
    if (!h) hourly.set(k, (h = { exch_in: 0, exch_out: 0, found_out: 0, found_to_exch: 0, large_in_n: 0 }));
    return h;
  };
  const alerts: Record<string, unknown>[] = [];
  const large: Record<string, unknown>[] = [];
  const now = Date.now();

  const transfer = (from: string | undefined, to: string | undefined, amount: number, at: string, op?: Operation) => {
    const kf = knownWallet(from);
    const kt = knownWallet(to);
    // Category-level hourly totals for the pressure index (transfers between two exchanges are ignored).
    if (kt?.category === "exchange" && kf?.category !== "exchange") {
      const h = hour(at);
      h.exch_in += amount;
      if (amount >= threshold) h.large_in_n += 1;
    }
    if (kf?.category === "exchange" && kt?.category !== "exchange") hour(at).exch_out += amount;
    if (kf?.category === "foundation" && kt?.category !== "foundation") {
      const h = hour(at);
      h.found_out += amount;
      if (kt?.category === "exchange") h.found_to_exch += amount;
    }
    // Foundation radar: any foundation wallet sending at least the large threshold.
    if (op && kf?.category === "foundation" && amount >= threshold) {
      alerts.push({
        net, id: `f:${op.id}`, kind: "foundation", at, amount: amount.toFixed(7), from_acc: from, to_acc: to ?? null, tx_hash: op.transaction_hash,
        meta: { dest: kt ? kt.category : "unknown" },
      });
    }
    if (from && knownWallet(from)) {
      const f = flow(from, at);
      f.outflow += amount;
      f.n_out += 1;
    }
    if (to && knownWallet(to)) {
      const f = flow(to, at);
      f.inflow += amount;
      f.n_in += 1;
    }
    if (amount >= HOLDER_MIN) {
      if (from) holders.add(from);
      if (to) holders.add(to);
    }
  };

  if (base === "ledgers") {
    for (const l of records as Ledger[]) {
      const d = bump(l.closed_at);
      d.ledgers += 1;
      d.txs += l.successful_transaction_count;
      d.failed_txs += l.failed_transaction_count;
      d.ops += l.operation_count;
    }
  } else if (base === "operations") {
    for (const op of records as Operation[]) {
      if (op.transaction_successful === false) continue;
      const d = bump(op.created_at);
      if (op.type === "payment" && op.asset_type === "native") {
        const amount = num(op.amount);
        d.payments += 1;
        d.payment_volume += amount;
        transfer(op.from as string, op.to as string, amount, op.created_at, op);
        if (amount >= threshold) {
          large.push({ net, op_id: op.id, tx_hash: op.transaction_hash, ledger_time: op.created_at, from_acc: op.from as string, to_acc: op.to as string, amount: String(op.amount), kind: "payment" });
        }
      } else if (op.type === "create_account") {
        const amount = num(op.starting_balance);
        d.new_accounts += 1;
        d.payment_volume += amount;
        transfer(op.funder as string, op.account as string, amount, op.created_at, op);
        if (amount >= threshold) {
          large.push({ net, op_id: op.id, tx_hash: op.transaction_hash, ledger_time: op.created_at, from_acc: op.funder as string, to_acc: op.account as string, amount: String(op.starting_balance), kind: "create_account" });
        }
      } else if (op.type === "create_claimable_balance" && op.asset === "native") {
        d.locks_created += 1;
        d.locked_amount += num(op.amount);
      } else if (op.type === "claim_claimable_balance") {
        d.claims += 1;
        if (typeof op.claimant === "string") holders.add(op.claimant);
      }
    }
  }

  const last = (records[records.length - 1] as { paging_token: string } | undefined)?.paging_token;

  await sql.begin(async (tx) => {
    await addDays(tx, net, days);
    for (const [k, f] of flows) {
      const [address, d] = k.split("|");
      await tx`
        insert into wallet_flows_daily ${tx({ net, address, day: d, inflow: f.inflow.toFixed(7), outflow: f.outflow.toFixed(7), n_in: f.n_in, n_out: f.n_out })}
        on conflict (net, address, day) do update set
          inflow = wallet_flows_daily.inflow + excluded.inflow,
          outflow = wallet_flows_daily.outflow + excluded.outflow,
          n_in = wallet_flows_daily.n_in + excluded.n_in,
          n_out = wallet_flows_daily.n_out + excluded.n_out`;
    }
    for (const [h, v] of hourly) {
      await tx`
        insert into flows_hourly ${tx({ net, hour: h, exch_in: v.exch_in.toFixed(7), exch_out: v.exch_out.toFixed(7), found_out: v.found_out.toFixed(7), found_to_exch: v.found_to_exch.toFixed(7), large_in_n: v.large_in_n })}
        on conflict (net, hour) do update set
          exch_in = flows_hourly.exch_in + excluded.exch_in,
          exch_out = flows_hourly.exch_out + excluded.exch_out,
          found_out = flows_hourly.found_out + excluded.found_out,
          found_to_exch = flows_hourly.found_to_exch + excluded.found_to_exch,
          large_in_n = flows_hourly.large_in_n + excluded.large_in_n`;
    }
    if (alerts.length) {
      // Backfilled history is recorded as already pushed, so nobody gets notified about old events.
      const live = stream === "operations";
      await tx`insert into alerts ${tx(alerts.map((a) => ({ ...a, meta: tx.json(a.meta as never), pushed: !live, tg: !live })))} on conflict do nothing`;
    }
    if (holders.size) {
      const rows = [...holders].filter((a) => /^G[A-Z2-7]{55}$/.test(a)).map((account) => ({ net, account, dirty: true }));
      if (rows.length) await tx`insert into holders ${tx(rows)} on conflict (net, account) do update set dirty = true`;
    }
    if (large.length) await tx`insert into large_payments ${tx(large)} on conflict do nothing`;
    if (last) await setState(tx, net, stream, last);
  });
  return { applied: records.length, cursor: last };
}

export interface IngestResult {
  net: Network;
  stream: string;
  pages: number;
  records: number;
  caughtUp: boolean;
  cursor: string;
}

/** Runs one stream until it is caught up or the deadline passes. Backfills stop at their end cursor. */
export async function ingestStream(sql: Sql, net: Network, stream: Stream, deadline: number, maxPages = Infinity): Promise<IngestResult> {
  let cursor = await loadCursor(sql, net, stream);
  const end = stream.endsWith("_bf") ? await getState(sql, net, `${stream}_end`) : undefined;
  const base = baseOf(stream);
  let pages = 0;
  let total = 0;
  let caughtUp = false;
  while (Date.now() < deadline && pages < maxPages) {
    if (end && BigInt(cursor || "0") >= BigInt(end)) {
      caughtUp = true;
      break;
    }
    let records = await fetchRecords<{ paging_token: string }>(net, PATHS[base].path, {
      ...PATHS[base].params,
      order: "asc",
      limit: String(PAGE),
      cursor,
    });
    const full = records.length === PAGE;
    if (end) records = records.filter((r) => BigInt(r.paging_token) <= BigInt(end));
    if (!records.length) {
      if (end) await setState(sql, net, stream, end);
      caughtUp = true;
      break;
    }
    const r = await applyPage(sql, net, stream, records);
    cursor = r.cursor ?? cursor;
    pages += 1;
    total += records.length;
    if (!full) {
      caughtUp = true;
      break;
    }
  }
  return { net, stream, pages, records: total, caughtUp, cursor };
}

/* ---------- Maintenance steps run after the streams ---------- */

/** Housekeeping: old browser error reports go after 14 days (the table may be missing in old test databases). */
export async function pruneLockups(sql: Sql) {
  const r = await sql`delete from client_errors where at < now() - interval '14 days'`.catch(() => null);
  return r?.count ?? 0;
}

async function nativeBalance(net: Network, account: string): Promise<string | null> {
  try {
    const a = await fetchJson<{ balances: { asset_type: string; balance: string }[] }>(net, `/accounts/${account}`);
    return a.balances.find((b) => b.asset_type === "native")?.balance ?? "0";
  } catch (e) {
    if ((e as { status?: number }).status === 404) return "0";
    return null;
  }
}

/** One balance per known wallet per UTC day, for the wallet history charts. */
export async function snapshotWallets(sql: Sql, net: Network) {
  if (net !== "mainnet") return 0;
  const today = new Date().toISOString().slice(0, 10);
  const have = await sql<{ address: string }[]>`select address from wallet_balances where net = ${net} and day = ${today}`;
  const done = new Set(have.map((r) => r.address));
  let n = 0;
  for (const w of knownWallets) {
    if (done.has(w.address)) continue;
    const bal = await nativeBalance(net, w.address);
    if (bal === null) continue;
    await sql`insert into wallet_balances (net, address, day, balance) values (${net}, ${w.address}, ${today}, ${bal}) on conflict do nothing`;
    n++;
  }
  return n;
}

/** Refreshes balances of accounts that moved large amounts, and keeps only the biggest HOLDERS_KEEP. */
export async function refreshHolders(sql: Sql, net: Network, max = 150) {
  const due = await sql<{ account: string }[]>`
    select account from holders where net = ${net} and dirty order by updated_at nulls first limit ${max}`;
  let n = 0;
  for (let i = 0; i < due.length; i += 10) {
    const batch = due.slice(i, i + 10);
    const balances = await Promise.all(batch.map((h) => nativeBalance(net, h.account)));
    for (let j = 0; j < batch.length; j++) {
      const bal = balances[j];
      if (bal === null) continue;
      await sql`update holders set balance = ${bal}, dirty = false, updated_at = now() where net = ${net} and account = ${batch[j].account}`;
      n++;
    }
  }
  await sql`
    delete from holders where net = ${net} and not dirty and balance < coalesce(
      (select balance from holders where net = ${net} and not dirty order by balance desc offset ${HOLDERS_KEEP} limit 1), -1)`;
  return n;
}

export type MaintenanceHook = (sql: Sql, net: Network, deadline: number) => Promise<unknown>;

const BACKFILL_PAGES = { operations_bf: 6, ledgers_bf: 3 };

/**
 * Live streams first (they are quick), then the heavy backfills share what is left, then
 * maintenance with a few seconds kept in reserve.
 */
export async function ingestAll(sql: Sql, nets: Network[], budgetMs: number, extra?: MaintenanceHook) {
  const start = Date.now();
  const hardDeadline = start + budgetMs;
  const reserve = Math.min(10_000, budgetMs * 0.2);
  const results: IngestResult[] = [];
  const maintenance: Record<string, unknown> = {};

  for (const net of nets) {
    for (const stream of ["ledgers", "operations"] as const) {
      results.push(await ingestStream(sql, net, stream, hardDeadline - reserve));
    }
  }
  /*
   * History backfills are capped per run (pages of 200 records) so they trickle in steadily
   * instead of burning ~6 s of CPU on every run: CPU time is the scarce resource on the free plan.
   */
  const heavy: [Stream, number, number][] = [["operations_bf", 0.6, BACKFILL_PAGES.operations_bf], ["ledgers_bf", 0.4, BACKFILL_PAGES.ledgers_bf]];
  let backlog = false;
  for (const net of nets) {
    const left = hardDeadline - reserve - Date.now();
    for (const [stream, share, maxPages] of heavy) {
      if (left <= 0) {
        backlog = true;
        break;
      }
      const r = await ingestStream(sql, net, stream, Math.min(hardDeadline - reserve, Date.now() + left * share), maxPages);
      results.push(r);
      if (!r.caughtUp) backlog = true;
    }
  }
  for (const net of nets) {
    maintenance[`${net}:pruned_errors`] = await pruneLockups(sql);
    maintenance[`${net}:wallet_snapshots`] = await snapshotWallets(sql, net);
    // While history is still being filled the holder queue is long; afterwards a small batch keeps up.
    const holderBatch = Date.now() > hardDeadline - 5000 ? 10 : 40;
    maintenance[`${net}:holders_refreshed`] = await refreshHolders(sql, net, holderBatch);
    maintenance[`${net}:signals`] = await hourlySignals(sql, net).catch((e) => String(e).slice(0, 120));
    if (extra) maintenance[`${net}:extra`] = await extra(sql, net, hardDeadline);
  }
  for (const net of nets) {
    await setState(sql, net, "backlog", backlog ? "1" : "0");
    await setState(sql, net, "last_run", new Date().toISOString());
  }
  return { results, maintenance, backlog };
}

/**
 * The scheduler calls every 10 minutes. A call that lands within `minGapSec` of the previous
 * finished run (e.g. a manual or duplicate trigger) is skipped at the cost of one small query.
 */
export async function shouldSkip(sql: Sql, net: Network, minGapSec = 270) {
  const rows = await sql<{ stream: string; cursor: string; age: number }[]>`
    select stream, cursor, extract(epoch from now() - updated_at)::int as age
    from sync_state where net = ${net} and stream in ('backlog', 'last_run')`;
  const backlog = rows.find((r) => r.stream === "backlog");
  const last = rows.find((r) => r.stream === "last_run");
  return !!backlog && backlog.cursor === "0" && !!last && last.age < minGapSec;
}

/**
 * Once per hour: stores the market snapshot, the top-100 holder total and the pressure index,
 * and raises an alert when the last full hour's exchange inflow was far above normal.
 */
export async function hourlySignals(sql: Sql, net: Network) {
  if (net !== "mainnet") return null;
  const hourIso = `${new Date().toISOString().slice(0, 13)}:00:00Z`;
  const have = await sql`select 1 from signals_hourly where net = ${net} and hour = ${hourIso}`;
  if (have.length) return null;

  const { computeIndex } = await import("./pindex");
  const { getBooks, combinedImbalance, getVolumePi } = await import("./market");
  const [idx, books, volumePi, top, ticker] = await Promise.all([
    computeIndex(sql, net),
    getBooks(60),
    getVolumePi(300),
    sql<{ s: string }[]>`select coalesce(sum(balance), 0)::text as s from (select balance from holders where net = ${net} and balance is not null order by balance desc limit 100) t`,
    fetchJsonSafe<{ data?: { last: string }[] }>("https://www.okx.com/api/v5/market/ticker?instId=PI-USDT"),
  ]);
  const ob = combinedImbalance(books);
  const price = Number(ticker?.data?.[0]?.last);
  await sql`
    insert into signals_hourly ${sql({
      net,
      hour: hourIso,
      price: Number.isFinite(price) ? price : null,
      volume_pi: volumePi,
      book_bid: ob.bid || null,
      book_ask: ob.ask || null,
      top_sum: top[0]?.s ?? null,
      score: idx.score,
      components: sql.json(idx.components as never),
    })}
    on conflict do nothing`;

  // Unusual inflow: last full hour vs the hourly history of the last 30 days.
  const prevHour = new Date(Date.parse(hourIso) - 3_600_000).toISOString();
  const hours = await sql<{ hour: string; exch_in: string }[]>`
    select to_char(hour at time zone 'utc', 'YYYY-MM-DD"T"HH24:00:00"Z"') as hour, exch_in::text
    from flows_hourly where net = ${net} and hour >= now() - interval '30 days'`;
  const last = hours.find((h) => Date.parse(h.hour) === Date.parse(prevHour));
  const rest = hours.filter((h) => h !== last).map((h) => Number(h.exch_in));
  if (last && rest.length >= 48) {
    const mean = rest.reduce((a, b) => a + b, 0) / rest.length;
    const sd = Math.sqrt(rest.reduce((a, b) => a + (b - mean) ** 2, 0) / rest.length) || 1;
    const v = Number(last.exch_in);
    const z = (v - mean) / sd;
    if (z >= 2.5 && v >= 200_000) {
      await sql`
        insert into alerts (net, id, kind, at, amount, meta)
        values (${net}, ${`spike:${prevHour}`}, 'inflow_spike', ${prevHour}, ${v.toFixed(7)}, ${sql.json({ z: Math.round(z * 10) / 10, normal: Math.round(mean) })})
        on conflict do nothing`;
    }
  }
  return { hour: hourIso, score: idx.score };
}

async function fetchJsonSafe<T>(url: string): Promise<T | null> {
  try {
    const r = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(5000) });
    return r.ok ? ((await r.json()) as T) : null;
  } catch {
    return null;
  }
}
