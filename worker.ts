// worker.ts — high-precision Telegram dispatch worker
// v6 — fix Railway: usa WSS (WebSocket/443) ao invés de TCPFull/80, que o Railway derruba
import { createClient } from "@supabase/supabase-js";
import { TelegramClient, Api } from "telegram";
import { StringSession } from "telegram/sessions";
import bigInt from "big-integer";
import http from "http";

/* ─── Supabase ─── */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─── Constantes ─── */
const SEND_TIMEOUT_MS        = 15_000;
const RETRY_BUDGET_MS        = 50_000;
const RELOAD_INTERVAL_MS     = 30_000;
const LOOKAHEAD_MS           = 2 * 60 * 1000;
const KEEPALIVE_INTERVAL_MS  = 45_000;

// Monitoramento de posição
const MONITOR_DELAY_CLOSED_MS = 6_000;
const MONITOR_MAX_OPEN_MS     = 5 * 60_000;
const MONITOR_POLL_MS         = 5_000;
const MONITOR_HISTORY_LIMIT   = 150;

/* ─── Tipos ─── */
interface Account {
  id: string;
  name: string;
  phone_number: string;
  api_id: string;
  api_hash: string;
  session_string: string;
  is_active: boolean;
}
interface GroupMember {
  id: string;
  message_text: string | null;
  position: number;
  is_active: boolean;
  accounts: Account | null;
}
interface Group {
  id: string;
  telegram_chat_id: string | null;
  group_type: "open" | "closed";
  group_members: GroupMember[];
}
interface Schedule {
  id: string;
  cron_expression: string;
  user_id: string;
  group_id: string;
  next_run_at: string;
  retry_window_seconds: number;
  retry_interval_seconds: number;
  retry_interval_max_seconds: number;
  retry_count: number;
  retry_until: string | null;
  last_attempt_at: string | null;
  groups: Group;
}
interface MemberResult {
  member_id: string;
  account_id: string;
  status: "sent" | "failed" | "skipped";
  retryable: boolean;
  error?: string;
}

/* ─── Peer cache ─── */
const peerCache    = new Map<string, unknown>();
const accountCache = new Map<string, Account>();

/* ─── Resolve peer com múltiplos fallbacks ─── */
async function getOrResolvePeer(
  client: TelegramClient,
  telegramChatId: string
): Promise<unknown> {
  if (peerCache.has(telegramChatId)) return peerCache.get(telegramChatId)!;

  const chatIdNum = parseInt(telegramChatId, 10);
  if (isNaN(chatIdNum)) throw new Error(`telegram_chat_id inválido: "${telegramChatId}"`);

  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(telegramChatId, peer);
    console.log(`[peer] ✓ getInputEntity: ${telegramChatId}`);
    return peer;
  } catch (e1: any) {
    console.warn(`[peer] getInputEntity falhou para ${telegramChatId}: ${e1.message}`);
  }

  const absId     = Math.abs(chatIdNum);
  const channelId = absId > 1_000_000_000_000 ? absId - 1_000_000_000_000 : absId;

  try {
    const result = await client.invoke(
      new Api.channels.GetChannels({
        id: [
          new Api.InputChannel({
            channelId: bigInt(channelId),
            accessHash: bigInt(0),
          }),
        ],
      })
    ) as any;

    const chat = result?.chats?.[0];
    if (chat?.accessHash != null) {
      const peer = new Api.InputPeerChannel({
        channelId: chat.id,
        accessHash: chat.accessHash,
      });
      peerCache.set(telegramChatId, peer);
      console.log(`[peer] ✓ GetChannels MTProto: ${telegramChatId}`);
      return peer;
    }
  } catch (e2: any) {
    console.warn(`[peer] GetChannels falhou para ${telegramChatId}: ${e2.message}`);
  }

  try {
    console.warn(`[peer] Sincronizando dialogs para resolver ${telegramChatId}...`);
    await client.getDialogs({ limit: 200 });
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(telegramChatId, peer);
    console.log(`[peer] ✓ Resolvido após GetDialogs: ${telegramChatId}`);
    return peer;
  } catch (e3: any) {
    throw new Error(
      `PEER_UNRESOLVABLE ${telegramChatId}: conta não é membro ou sessão inválida. ` +
      `Último erro: ${e3.message}`
    );
  }
}

/* ─── Pool de conexões Telegram persistente ─── */
class TelegramClientPool {
  private clients            = new Map<string, TelegramClient>();
  private sessions           = new Map<string, string>();
  private keepaliveTimers    = new Map<string, ReturnType<typeof setInterval>>();
  private connectingPromises = new Map<string, Promise<TelegramClient>>();

  private startKeepalive(accountId: string, client: TelegramClient): void {
    const existing = this.keepaliveTimers.get(accountId);
    if (existing) clearInterval(existing);

    const interval = setInterval(async () => {
      if (!client.connected) {
        console.warn(`[keepalive] ${accountId} desconectado — removendo do pool`);
        this._evict(accountId, interval);
        return;
      }
      try {
        await Promise.race([
          client.getMe(),
          new Promise<never>((_, r) =>
            setTimeout(() => r(new Error("keepalive timeout")), 10_000)
          ),
        ]);
      } catch (err: any) {
        console.warn(`[keepalive] Ping falhou para ${accountId}: ${err.message} — removendo do pool`);
        try { await client.disconnect(); } catch {}
        this._evict(accountId, interval);

        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED") ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          console.warn(`[keepalive] Sessão morta para ${accountId} — desativando no banco.`);
          supabase.from("accounts").update({ is_active: false }).eq("id", accountId)
            .then(({ error }) => {
              if (error) console.error(`[keepalive] Falha ao desativar conta ${accountId}:`, error.message);
              else console.log(`[keepalive] Conta ${accountId} desativada.`);
            });
        }
      }
    }, KEEPALIVE_INTERVAL_MS);

    this.keepaliveTimers.set(accountId, interval);
  }

  private _evict(accountId: string, interval?: ReturnType<typeof setInterval>): void {
    if (interval) clearInterval(interval);
    this.keepaliveTimers.delete(accountId);
    this.clients.delete(accountId);
  }

  async get(account: Account): Promise<TelegramClient> {
    const existing       = this.clients.get(account.id);
    const sessionInUse   = new Map(this.sessions).get(account.id);
    const sessionChanged = sessionInUse !== account.session_string;

    if (existing?.connected && !sessionChanged) return existing;

    const inflight = this.connectingPromises.get(account.id);
    if (inflight) return inflight;

    const connectPromise = (async () => {
      if (existing) {
        try { await existing.disconnect(); } catch {}
        this._evict(account.id);
      }

      if (sessionChanged && sessionInUse) {
        console.log(`[pool] Session mudou para ${account.phone_number} — reconectando...`);
      }

      const client = new TelegramClient(
        new StringSession(account.session_string),
        parseInt(account.api_id),
        account.api_hash,
        {
          connectionRetries: 5,
          retryDelay: 1_000,
          autoReconnect: true,
          floodSleepThreshold: 60,
          requestRetries: 3,
          // FIX v6: Railway derruba TCPFull na porta 80 (trata como HTTP inválido).
          // useWSS=true usa WebSocket sobre TLS/443, que o Railway suporta perfeitamente.
          useWSS: true,
        }
      );

      await client.connect();

      try {
        await client.getDialogs({ limit: 100 });
        console.log(`[pool] ✓ Dialogs sincronizados: ${account.phone_number}`);
      } catch (err: any) {
        console.warn(`[pool] getDialogs falhou no warm-up de ${account.phone_number}: ${err.message}`);
      }

      this.clients.set(account.id, client);
      this.sessions.set(account.id, account.session_string);
      this.startKeepalive(account.id, client);
      console.log(`[pool] Conectado: ${account.phone_number}`);
      return client;
    })();

    this.connectingPromises.set(account.id, connectPromise);
    try {
      return await connectPromise;
    } finally {
      this.connectingPromises.delete(account.id);
    }
  }

  async prewarm(accounts: Account[]): Promise<void> {
    console.log(`[pool] Pre-warming ${accounts.length} conta(s)...`);
    await Promise.allSettled(accounts.map((a) => this.get(a)));
    console.log(`[pool] Pre-warm concluído.`);
  }

  async disconnectAll(): Promise<void> {
    for (const timer of this.keepaliveTimers.values()) clearInterval(timer);
    this.keepaliveTimers.clear();
    await Promise.all(
      [...this.clients.entries()].map(async ([id, client]) => {
        try { await client.disconnect(); } catch {}
        console.log(`[pool] Desconectado: ${id}`);
      })
    );
    this.clients.clear();
  }
}

const clientPool = new TelegramClientPool();

/* ─── Graceful shutdown ─── */
async function shutdown() {
  console.log("[worker] Encerrando...");
  httpServer.close();
  await clientPool.disconnectAll();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);

/* ─── Helpers ─── */
function isRetryableError(msg: string): boolean {
  const upper = msg.toUpperCase();
  if (upper.includes("AUTH_KEY_UNREGISTERED")) return false;
  if (upper.includes("USER_DEACTIVATED"))      return false;
  if (upper.includes("SESSION_REVOKED"))       return false;
  return true;
}

function nextWeeklyOccurrence(cronExpression: string): string {
  const parts = cronExpression.trim().split(/\s+/);
  const mi    = parseInt(parts[0], 10);
  const h     = parseInt(parts[1], 10);
  const dow   = parseInt(parts[4], 10);

  if (
    parts.length < 5 ||
    isNaN(mi) || isNaN(h) || isNaN(dow) ||
    mi < 0 || mi > 59 || h < 0 || h > 23 || dow < 0 || dow > 6
  ) throw new Error(`cron_expression inválida: "${cronExpression}"`);

  const now = new Date();
  let daysUntil = (dow - now.getUTCDay() + 7) % 7;
  if (daysUntil === 0) {
    const nowMins  = now.getUTCHours() * 60 + now.getUTCMinutes();
    const targMins = h * 60 + mi;
    if (targMins <= nowMins) daysUntil = 7;
  }
  const next = new Date(now);
  next.setUTCDate(next.getUTCDate() + daysUntil);
  next.setUTCHours(h, mi, 0, 0);
  return next.toISOString();
}

function calcRetryInterval(count: number, base: number, max: number): number {
  return Math.min(base * Math.pow(2, count), max);
}

function isRetryDue(schedule: Schedule, now: Date): boolean {
  if (!schedule.last_attempt_at) return true;
  const last     = new Date(schedule.last_attempt_at);
  const interval = calcRetryInterval(
    schedule.retry_count,
    schedule.retry_interval_seconds,
    schedule.retry_interval_max_seconds
  );
  return now >= new Date(last.getTime() + interval * 1000);
}

/* ─── Deduplicação ─── */
async function getAlreadySentAccountIds(schedule: Schedule): Promise<Set<string>> {
  const cycleStart = schedule.retry_until
    ? new Date(
        new Date(schedule.retry_until).getTime() - schedule.retry_window_seconds * 1000
      ).toISOString()
    : schedule.next_run_at;

  const { data, error } = await supabase
    .from("dispatch_logs")
    .select("account_id")
    .eq("schedule_id", schedule.id)
    .eq("status", "sent")
    .gte("sent_at", cycleStart);

  if (error) {
    console.warn(`[dedup] Falha ao buscar enviados do schedule ${schedule.id}:`, error.message);
    return new Set();
  }

  return new Set((data ?? []).map((r) => r.account_id as string));
}

/* ─── Envio agressivo com retry interno ─── */
async function sendAggressively(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string
): Promise<void> {
  const budgetEnd = Date.now() + RETRY_BUDGET_MS;
  let attempt     = 0;

  while (Date.now() < budgetEnd) {
    attempt++;
    const timeLeft = budgetEnd - Date.now();
    if (timeLeft < 500) break;

    const attemptTimeout = Math.min(SEND_TIMEOUT_MS, timeLeft - 100);

    try {
      await Promise.race([
        (async () => {
          const peer = await getOrResolvePeer(client, telegramChatId);
          try {
            await client.sendMessage(peer as any, { message: messageText });
          } catch (err: any) {
            const errMsg = String(err?.message ?? "");

            if (
              errMsg.includes("PEER_ID_INVALID") ||
              errMsg.includes("CHANNEL_INVALID") ||
              errMsg.includes("CHANNEL_PRIVATE")
            ) {
              console.warn(`[peer] Cache inválido para ${telegramChatId} — limpando`);
              peerCache.delete(telegramChatId);
            }

            const isFlood =
              err?.seconds != null ||
              err?.constructor?.name === "FloodWaitError" ||
              /flood/i.test(errMsg);

            if (isFlood) {
              const waitSecs: number =
                typeof err.seconds === "number"
                  ? err.seconds
                  : parseInt(errMsg.match(/(\d+)/)?.[1] ?? "30", 10);
              const waitMs = waitSecs * 1000;
              console.warn(`[retry] FloodWait ${waitSecs}s — ${account.phone_number}`);

              if (waitMs < budgetEnd - Date.now() - 500) {
                await new Promise((r) => setTimeout(r, waitMs));
                peerCache.delete(telegramChatId);
                const freshPeer = await getOrResolvePeer(client, telegramChatId);
                await client.sendMessage(freshPeer as any, { message: messageText });
                return;
              }
              throw new Error(`FLOOD_WAIT_${waitSecs}_EXCEEDS_BUDGET`);
            }

            throw err;
          }
        })(),
        new Promise<never>((_, r) =>
          setTimeout(() => r(new Error(`TIMEOUT tentativa ${attempt}`)), attemptTimeout)
        ),
      ]);

      if (attempt > 1) {
        console.log(`[retry] ✓ ${account.phone_number} — enviou na tentativa ${attempt}`);
      }
      return;

    } catch (err: unknown) {
      const errMsg    = (err as any)?.message ?? String(err);
      const remaining = budgetEnd - Date.now();
      if (remaining > 500) {
        console.warn(`[retry] tentativa ${attempt} falhou (${Math.round(remaining / 1000)}s restantes): ${errMsg}`);
      }
    }
  }

  throw new Error(`BUDGET_EXCEEDED após ${attempt} tentativa(s) em ${RETRY_BUDGET_MS / 1000}s`);
}

/* ─── Tenta enviar um membro ─── */
async function trySendMember(
  member: GroupMember,
  account: Account,
  group: Group,
  schedule: Schedule,
  alreadySent: Set<string>
): Promise<MemberResult> {
  if (alreadySent.has(account.id)) {
    console.log(`[worker] ↷ ${member.id} (${account.phone_number}) — já enviou neste ciclo [mem]`);
    return { member_id: member.id, account_id: account.id, status: "skipped", retryable: false };
  }

  let logStatus: "sent" | "failed" = "failed";
  let errorMsg: string | undefined;
  let retryable = false;

  try {
    const client = await clientPool.get(account);
    await sendAggressively(client, account, group.telegram_chat_id!, member.message_text ?? "");
    logStatus = "sent";
    alreadySent.add(account.id);
    console.log(`[worker] ✓ ${member.id} (${account.phone_number})`);
  } catch (err) {
    errorMsg  = err instanceof Error ? err.message : String(err);
    retryable = isRetryableError(errorMsg);
    console.error(
      `[worker] ✗ ${member.id} [${retryable ? "retryável" : "permanente"}] ` +
      `(${account.phone_number}): ${errorMsg}`
    );
  }

  await supabase.from("dispatch_logs").insert({
    user_id:       schedule.user_id,
    group_id:      group.id,
    account_id:    account.id,
    schedule_id:   schedule.id,
    status:        logStatus,
    message_text:  member.message_text,
    sent_at:       logStatus === "sent" ? new Date().toISOString() : null,
    error_message: errorMsg ?? null,
  });

  return { member_id: member.id, account_id: account.id, status: logStatus, retryable, error: errorMsg };
}

/* ─── Processa membros sequencialmente (evita duplicatas em grupo fechado) ─── */
async function processMembersOf(
  schedule: Schedule,
  alreadySent: Set<string>
): Promise<MemberResult[]> {
  const group = schedule.groups;

  const members = (group.group_members ?? [])
    .filter((m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);

  const results: MemberResult[] = [];

  if (group.group_type === "closed") {
    for (const member of members) {
      const result = await trySendMember(member, member.accounts!, group, schedule, alreadySent);
      results.push(result);
    }
  } else {
    const parallel = await Promise.all(
      members.map((member) => trySendMember(member, member.accounts!, group, schedule, alreadySent))
    );
    results.push(...parallel);
  }

  return results;
}

async function waitForAdminSignal(
  client: TelegramClient,
  telegramChatId: string,
  timeoutMs: number = 30 * 60_000
): Promise<boolean> {
  const deadline  = Date.now() + timeoutMs;
  const startUnix = Math.floor((Date.now() - 5_000) / 1000);

  console.log(`[admin-signal] Aguardando OK do admin em ${telegramChatId}...`);

  while (Date.now() < deadline) {
    try {
      const peer = await getOrResolvePeer(client, telegramChatId);

      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer:       peer as any,
          limit:      20,
          offsetDate: 0,
          offsetId:   0,
          maxId:      0,
          minId:      0,
          hash:       bigInt(0),
          addOffset:  0,
        })
      ) as any;

      const recentMsgs: any[] = (result.messages ?? [])
        .filter((m: any) => m._ === "message" && m.date >= startUnix);

      const signal = recentMsgs.some((m: any) => {
        const isOk    = typeof m.message === "string" && m.message.trim().toLowerCase() === "ok";
        const isMedia = m.media && m.media._ !== "messageMediaEmpty" && m.media._ !== undefined;
        return isOk || isMedia;
      });

      if (signal) {
        console.log(`[admin-signal] ✓ Sinal do admin detectado em ${telegramChatId}`);
        return true;
      }
    } catch (err: any) {
      console.warn(`[admin-signal] Erro ao buscar histórico: ${err.message}`);
    }

    await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
  }

  console.warn(`[admin-signal] Timeout — nenhum sinal do admin em ${telegramChatId}`);
  return false;
}

async function monitorPositions(
  telegramChatId: string,
  sentMembers: Array<{ account_id: string; message_text: string }>,
  scheduleId: string,
  dispatchedAt: Date,
  groupType: "open" | "closed"
): Promise<void> {
  if (sentMembers.length === 0) return;

  const account = accountCache.get(sentMembers[0].account_id);
  if (!account) {
    console.warn("[monitor] Conta não encontrada no cache — monitoramento de posição ignorado");
    return;
  }

  const client = await clientPool.get(account).catch(() => null);
  if (!client) {
    console.warn("[monitor] Não foi possível obter client — monitoramento de posição ignorado");
    return;
  }

  const windowStartUnix = Math.floor((dispatchedAt.getTime() - 15_000) / 1000);
  const deadline        = Date.now() + (groupType === "closed" ? MONITOR_DELAY_CLOSED_MS + 10_000 : MONITOR_MAX_OPEN_MS);
  const ourTexts        = new Set(sentMembers.map((m) => m.message_text).filter(Boolean));

  if (groupType === "closed") {
    await new Promise((r) => setTimeout(r, MONITOR_DELAY_CLOSED_MS));
  }

  console.log(`[monitor] Iniciando para schedule ${scheduleId} (${groupType})`);

  while (Date.now() < deadline) {
    try {
      const peer = await getOrResolvePeer(client, telegramChatId);

      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer:       peer as any,
          limit:      MONITOR_HISTORY_LIMIT,
          offsetDate: 0,
          offsetId:   0,
          maxId:      0,
          minId:      0,
          hash:       bigInt(0),
          addOffset:  0,
        })
      ) as any;

      const allMsgs: any[] = result.messages ?? [];

      const windowMsgs = allMsgs
        .filter((m: any) => m._ === "message" && m.date >= windowStartUnix)
        .reverse();

      if (windowMsgs.length === 0) {
        if (groupType === "closed") {
          console.warn(`[monitor] Nenhuma mensagem na janela (grupo fechado) — abortando`);
          return;
        }
        await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      const ourMessagesVisible = windowMsgs.some((m: any) => ourTexts.has(m.message));

      if (groupType === "open" && !ourMessagesVisible) {
        await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      const updates: Promise<unknown>[] = [];
      const cutoff = new Date(dispatchedAt.getTime() - 60_000).toISOString();

      for (const sm of sentMembers) {
        if (!sm.message_text) continue;
        const idx = windowMsgs.findIndex((m: any) => m.message === sm.message_text);
        if (idx < 0) continue;

        const rank = idx + 1;
        console.log(`[monitor] ${sm.account_id}: #${rank} em ${telegramChatId}`);

        updates.push(
          Promise.resolve(
            supabase.from("dispatch_logs")
              .update({ position_rank: rank })
              .eq("schedule_id", scheduleId)
              .eq("account_id",  sm.account_id)
              .eq("status",      "sent")
              .gte("sent_at",    cutoff)
          )
        );
      }

      await Promise.allSettled(updates);
      console.log(`[monitor] ✓ Posições salvas para schedule ${scheduleId}`);
      return;

    } catch (err: any) {
      console.warn(`[monitor] Erro ao buscar histórico: ${err.message}`);
      if (groupType === "closed") return;
      await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
    }
  }

  console.warn(`[monitor] Timeout — posições não registradas para schedule ${scheduleId}`);
}

/* ─── Dispara um schedule ─── */
async function fireSchedule(scheduleId: string): Promise<void> {
  const now    = new Date();
  const nowISO = now.toISOString();

  const { data: rows, error } = await supabase
    .from("schedules")
    .select(`
      id, cron_expression, user_id, group_id, next_run_at,
      retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
      retry_count, retry_until, last_attempt_at,
      groups(id, telegram_chat_id, group_type,
        group_members(id, message_text, position, is_active,
          accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))
    `)
    .eq("id", scheduleId)
    .eq("is_active", true)
    .single();

  if (error || !rows) {
    console.warn(`[timer] Schedule ${scheduleId} não encontrado ou inativo.`);
    return;
  }

  const schedule = rows as unknown as Schedule;
  const group    = schedule.groups;

  if (!group?.telegram_chat_id) {
    console.warn(`[timer] Schedule ${scheduleId}: sem telegram_chat_id, pulando.`);
    return;
  }

  if (group.group_members) {
    group.group_members = group.group_members.map((m) => ({
      ...m,
      accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
    }));
  }

  console.log(`[timer] ⚡ Disparando schedule ${scheduleId} às ${nowISO}`);

  if (group.group_type === "open") {
    const firstAccount = (group.group_members ?? [])
      .find((m) => m.is_active && m.accounts?.is_active)?.accounts;

    if (firstAccount) {
      const client = await clientPool.get(firstAccount).catch(() => null);
      if (client) {
        const gotSignal = await waitForAdminSignal(client, group.telegram_chat_id!);
        if (!gotSignal) {
          console.warn(`[timer] Schedule ${scheduleId}: timeout aguardando ok do admin — abortando ciclo.`);
          return;
        }
      }
    }
  }

  const alreadySent = await getAlreadySentAccountIds(schedule);
  if (alreadySent.size > 0) {
    console.log(`[dedup] ${alreadySent.size} account(s) já enviaram neste ciclo — serão pulados.`);
  }

  const results = await processMembersOf(schedule, alreadySent);

  const sentForMonitor = results
    .filter((r) => r.status === "sent")
    .map((r) => {
      const member = (group.group_members ?? []).find((m) => m.accounts?.id === r.account_id);
      return { account_id: r.account_id, message_text: member?.message_text ?? "" };
    })
    .filter((r) => r.message_text);

  if (sentForMonitor.length > 0) {
    monitorPositions(
      group.telegram_chat_id,
      sentForMonitor,
      scheduleId,
      now,
      group.group_type ?? "closed"
    ).catch((err) => console.error("[monitor] Erro não capturado:", err.message));
  }

  const sentCount         = results.filter((r) => r.status === "sent").length;
  const skippedCount      = results.filter((r) => r.status === "skipped").length;
  const retryableFailures = results.filter((r) => r.status === "failed" && r.retryable);
  const permanentFailures = results.filter((r) => r.status === "failed" && !r.retryable);
  const totalDelivered    = sentCount + skippedCount;

  const hasActiveMembers = (group.group_members ?? []).some(
    (m) => m.is_active && m.accounts?.is_active
  );
  const allSucceeded =
    hasActiveMembers &&
    retryableFailures.length === 0 &&
    permanentFailures.length === 0 &&
    totalDelivered > 0;

  if (allSucceeded) {
    let nextRun: string;
    try {
      nextRun = nextWeeklyOccurrence(schedule.cron_expression);
    } catch (err) {
      console.error(`[timer] cron inválido no schedule ${scheduleId}, desativando:`, err);
      await supabase.from("schedules").update({ is_active: false }).eq("id", scheduleId);
      return;
    }

    await supabase.from("schedules").update({
      next_run_at:         nextRun,
      last_run_at:         nowISO,
      retry_until:         null,
      retry_count:         0,
      last_attempt_at:     nowISO,
      last_attempt_status: "sent",
      last_attempt_error:  null,
    }).eq("id", scheduleId);

    console.log(`[timer] ✓ Schedule ${scheduleId} OK. Próxima: ${nextRun}`);
    scheduleTimer(scheduleId, nextRun);

  } else {
    const newRetryCount  = schedule.retry_count + 1;
    const isFirstFailure = !schedule.retry_until;
    const retryUntil     = isFirstFailure
      ? new Date(now.getTime() + schedule.retry_window_seconds * 1000).toISOString()
      : schedule.retry_until!;

    const failedErrors = results
      .filter((r) => r.status === "failed" && r.error)
      .map((r) => `[${r.account_id}] ${r.error}`)
      .join("; ");

    await supabase.from("schedules").update({
      retry_until:         retryUntil,
      retry_count:         newRetryCount,
      last_attempt_at:     nowISO,
      last_attempt_status: "retrying",
      last_attempt_error:  failedErrors || null,
    }).eq("id", scheduleId);

    const intervalNext = calcRetryInterval(
      newRetryCount,
      schedule.retry_interval_seconds,
      schedule.retry_interval_max_seconds
    );

    console.warn(
      `[timer] ⚠ Schedule ${scheduleId}: ${retryableFailures.length} falha(s) retryável(eis), ` +
      `${permanentFailures.length} permanente(s). ` +
      `Retry #${newRetryCount} em ~${intervalNext}s (até ${retryUntil})`
    );

    const retryAt = new Date(now.getTime() + intervalNext * 1000);
    if (retryAt < new Date(retryUntil)) {
      scheduleTimer(scheduleId, retryAt.toISOString());
    }
  }
}

/* ─── Precision timers ─── */
const scheduledTimers = new Map<string, ReturnType<typeof setTimeout>>();

function scheduleTimer(scheduleId: string, nextRunAt: string): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  if (delay < -5_000) {
    console.warn(`[timer] Schedule ${scheduleId} ignorado — next_run_at muito no passado (${nextRunAt})`);
    return;
  }

  const existing = scheduledTimers.get(scheduleId);
  if (existing) clearTimeout(existing);

  const effectiveDelay = Math.max(0, delay);

  const timer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    try {
      await fireSchedule(scheduleId);
    } catch (err) {
      console.error(`[timer] Erro inesperado ao disparar schedule ${scheduleId}:`, err);
    }
  }, effectiveDelay);

  scheduledTimers.set(scheduleId, timer);

  const fireAt = new Date(Date.now() + effectiveDelay).toISOString();
  console.log(`[timer] ⏰ Schedule ${scheduleId} — dispara em ${Math.round(effectiveDelay / 1000)}s (${fireAt})`);
}

/* ─── Reload periódico ─── */
async function reloadSchedules(): Promise<void> {
  const now          = new Date();
  const nowISO       = now.toISOString();
  const lookaheadISO = new Date(now.getTime() + LOOKAHEAD_MS).toISOString();

  const [
    { data: futureSchedules },
    { data: retrySchedules  },
    { data: expiredRetries  },
    { data: missedSchedules }, // ← v5: schedules que ficaram presos no passado
  ] = await Promise.all([
    // 1. Schedules normais dentro da janela de lookahead
    supabase
      .from("schedules")
      .select("id, next_run_at")
      .eq("is_active", true)
      .is("retry_until", null)
      .lte("next_run_at", lookaheadISO),

    // 2. Schedules em retry ainda dentro do prazo
    supabase
      .from("schedules")
      .select(`
        id, cron_expression, user_id, group_id, next_run_at,
        retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
        retry_count, retry_until, last_attempt_at,
        groups(id, telegram_chat_id, group_type,
          group_members(id, message_text, position, is_active,
            accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))
      `)
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .gt("retry_until", nowISO),

    // 3. Schedules em retry expirados — avança para próxima semana
    supabase
      .from("schedules")
      .select("id, cron_expression")
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .lte("retry_until", nowISO),

    // 4. ← FIX v5: Schedules perdidos (next_run_at no passado, sem retry ativo)
    //    Ocorre quando o worker reiniciou depois do horário agendado.
    //    Fora da janela de lookahead e sem retry_until definido = nunca seriam pegos.
    supabase
      .from("schedules")
      .select("id, next_run_at, cron_expression")
      .eq("is_active", true)
      .is("retry_until", null)
      .lt("next_run_at", nowISO),
  ]);

  // Processa retries expirados
  await Promise.all(
    (expiredRetries ?? []).map(async (expired) => {
      console.warn(`[reload] Schedule ${expired.id}: retry expirou. Avançando para próxima semana.`);
      let nextRun: string;
      try {
        nextRun = nextWeeklyOccurrence(expired.cron_expression);
      } catch {
        await supabase.from("schedules").update({ is_active: false }).eq("id", expired.id);
        return;
      }
      await supabase.from("schedules").update({
        next_run_at:         nextRun,
        last_run_at:         nowISO,
        retry_until:         null,
        retry_count:         0,
        last_attempt_at:     nowISO,
        last_attempt_status: "failed",
        last_attempt_error:  "Retry expirou sem sucesso total",
      }).eq("id", expired.id);
      scheduleTimer(expired.id, nextRun);
    })
  );

  // Agenda schedules futuros normais
  for (const s of futureSchedules ?? []) {
    if (!scheduledTimers.has(s.id)) {
      scheduleTimer(s.id, s.next_run_at);
    }
  }

  // Dispara retries que estão devidos
  for (const s of retrySchedules ?? []) {
    const schedule = s as unknown as Schedule;
    if (isRetryDue(schedule, now) && !scheduledTimers.has(schedule.id)) {
      console.log(`[reload] Schedule ${schedule.id} em retry — disparando agora.`);
      fireSchedule(schedule.id).catch((err) =>
        console.error(`[reload] Erro no retry do schedule ${schedule.id}:`, err)
      );
    }
  }

  // ← FIX v5: Dispara schedules perdidos que estavam presos no passado
  for (const s of missedSchedules ?? []) {
    // Ignora se já tem um timer pendente ou se já foi pego pelo bloco de futuros
    if (scheduledTimers.has(s.id)) continue;

    const lagMs = now.getTime() - new Date(s.next_run_at).getTime();
    console.warn(
      `[reload] ⚠ Schedule ${s.id} estava preso no passado ` +
      `(next_run_at: ${s.next_run_at}, atraso: ${Math.round(lagMs / 1000)}s) — disparando agora.`
    );

    fireSchedule(s.id).catch((err) =>
      console.error(`[reload] Erro ao disparar schedule perdido ${s.id}:`, err)
    );
  }
}

/* ─── Pre-warm ─── */
let prewarmRunning = false;
async function prewarmAccounts(): Promise<void> {
  if (prewarmRunning) return;
  prewarmRunning = true;
  try {
    const { data, error } = await supabase
      .from("accounts")
      .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("is_active", true);

    if (error) {
      console.warn("[prewarm] Falha ao buscar contas:", error.message);
      return;
    }

    const accounts = (data ?? []) as Account[];
    for (const account of accounts) accountCache.set(account.id, account);

    await Promise.allSettled(
      accounts.map(async (account) => {
        try {
          await clientPool.get(account);
        } catch (err: any) {
          const authDead =
            err.message?.includes("AUTH_KEY_UNREGISTERED") ||
            err.message?.includes("USER_DEACTIVATED") ||
            err.message?.includes("SESSION_REVOKED");
          if (authDead) {
            console.warn(`[prewarm] Sessão morta para ${account.phone_number} — desativando no banco.`);
            await supabase.from("accounts").update({ is_active: false }).eq("id", account.id);
          }
        }
      })
    );
  } finally {
    prewarmRunning = false;
  }
}

/* ─── HTTP server interno ────────────────────────────────────────────────────
   Expõe endpoints para que o Next.js busque dados usando a conexão
   já estabelecida pelo worker, evitando AUTH_KEY_DUPLICATED.

   Rotas:
     GET /accounts/:id/chats
     GET /accounts/:id/chat-members?chat_id=XXXX

   Protegido por WORKER_SECRET (header x-worker-secret).
   ─────────────────────────────────────────────────────────────────────────── */
const WORKER_PORT   = parseInt(process.env.PORT ?? "3001", 10);
const WORKER_SECRET = process.env.WORKER_SECRET ?? "";

function jsonResponse(res: http.ServerResponse, status: number, body: unknown) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(payload);
}

const httpServer = http.createServer(async (req, res) => {
  // Autenticação
  if (WORKER_SECRET && req.headers["x-worker-secret"] !== WORKER_SECRET) {
    return jsonResponse(res, 401, { error: "Unauthorized" });
  }

  const url = new URL(req.url ?? "/", `http://localhost:${WORKER_PORT}`);

  // GET /accounts/:id/chats
  const chatsMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chats$/);
  if (req.method === "GET" && chatsMatch) {
    const accountId = chatsMatch[1];
    const account   = accountCache.get(accountId);

    if (!account) {
      return jsonResponse(res, 404, { error: "Conta não encontrada no cache do worker" });
    }

    try {
      const client  = await clientPool.get(account);
      const dialogs = await client.getDialogs({ limit: 200 });
      const chats   = dialogs
        .filter((d) => d.isGroup || d.isChannel)
        .map((d) => ({
          id:          String(d.id),
          name:        d.title ?? d.name ?? "Sem nome",
          type:        d.isChannel ? "channel" : "group",
          accessHash:  null,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));

      return jsonResponse(res, 200, chats);
    } catch (err: any) {
      console.error("[http] /chats erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // GET /accounts/:id/chat-members?chat_id=XXXX
  const membersMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-members$/);
  if (req.method === "GET" && membersMatch) {
    const accountId = membersMatch[1];
    const chatId    = url.searchParams.get("chat_id");
    const account   = accountCache.get(accountId);

    if (!chatId)    return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account)   return jsonResponse(res, 404, { error: "Conta não encontrada no cache do worker" });

    type MemberOut = { id: string; name: string | null; username: string | null; phone: string | null };

    try {
      const client    = await clientPool.get(account);
      const rawId     = chatId.replace(/^-/, "");
      const isSupergroup = chatId.startsWith("-100");
      let members: MemberOut[] = [];

      if (isSupergroup) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const dialog  = dialogs.find((d) => {
            const dId = String(d.id);
            return dId === rawId || dId === chatId || dId === rawId.replace(/^100/, "");
          });
          const entity = dialog?.entity;
          if (entity && (entity.className === "Channel" || entity.className === "Chat")) {
            const result = await client.invoke(
              new Api.channels.GetParticipants({
                channel: entity as Api.Channel,
                filter:  new Api.ChannelParticipantsRecent(),
                offset:  0,
                limit:   200,
                hash:    bigInt(0),
              })
            );
            if (result.className === "channels.ChannelParticipants") {
              members = result.users
                .filter((u): u is Api.User => u.className === "User" && !u.bot)
                .map((u) => ({
                  id:       String(u.id),
                  name:     [u.firstName, u.lastName].filter(Boolean).join(" ") || null,
                  username: u.username ? `@${u.username}` : null,
                  phone:    u.phone ? `+${u.phone}` : null,
                }));
            }
          }
        } catch { /* tenta estratégia 2 */ }
      }

      if (members.length === 0) {
        try {
          const numericId = bigInt(rawId);
          const full      = await client.invoke(new Api.messages.GetFullChat({ chatId: numericId }));
          const chatFull  = full.fullChat as Api.ChatFull;
          const parts     = chatFull.participants;
          if (parts && parts.className === "ChatParticipants") {
            const userMap = new Map<string, Api.User>();
            for (const u of full.users) {
              if (u.className === "User") userMap.set(String(u.id), u as Api.User);
            }
            members = parts.participants
              .map((p) => {
                const u = userMap.get(String((p as any).userId));
                if (!u || u.bot) return null;
                return {
                  id:       String(u.id),
                  name:     [u.firstName, u.lastName].filter(Boolean).join(" ") || null,
                  username: u.username ? `@${u.username}` : null,
                  phone:    u.phone ? `+${u.phone}` : null,
                };
              })
              .filter((m): m is MemberOut => m !== null);
          }
        } catch { /* silencia */ }
      }

      members.sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));
      return jsonResponse(res, 200, members);
    } catch (err: any) {
      console.error("[http] /chat-members erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // GET /accounts/:id/dialogs
  const dialogsMatch = url.pathname.match(/^\/accounts\/([^/]+)\/dialogs$/);
  if (req.method === "GET" && dialogsMatch) {
    const accountId = dialogsMatch[1];
    const account   = accountCache.get(accountId);

    if (!account) {
      return jsonResponse(res, 404, { error: "Conta não encontrada no cache do worker" });
    }

    try {
      const client  = await clientPool.get(account);
      const dialogs = await client.getDialogs({ limit: 200 });
      const result  = dialogs
        .filter((d) => d.isGroup || d.isChannel)
        .map((d) => ({
          id:         String(d.id),
          name:       d.title ?? d.name ?? "Sem nome",
          type:       d.isChannel ? "channel" : "group",
          accessHash: null,
          members:    (d.entity as any)?.participantsCount ?? null,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));

      return jsonResponse(res, 200, result);
    } catch (err: any) {
      console.error("[http] /dialogs erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // GET /accounts/:id/chat-count?chat_id=XXXX
  const chatCountMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-count$/);
  if (req.method === "GET" && chatCountMatch) {
    const accountId = chatCountMatch[1];
    const chatId    = url.searchParams.get("chat_id");
    const account   = accountCache.get(accountId);

    if (!chatId)  return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada no cache do worker" });

    try {
      const client = await clientPool.get(account);
      const rawId  = chatId.replace(/^-/, "");
      let count: number | null = null;

      const dialogs = await client.getDialogs({ limit: 500 });
      const dialog  = dialogs.find((d) => {
        const dId = String(d.id);
        return dId === rawId || dId === chatId || `-${dId}` === chatId || `-100${dId}` === chatId;
      });

      if (dialog?.entity) {
        const entity = dialog.entity as any;
        if (entity.participantsCount != null) {
          count = entity.participantsCount;
        } else if (entity.className === "Channel") {
          const full = await client.invoke(
            new Api.channels.GetFullChannel({ channel: entity })
          ) as any;
          count = full.fullChat?.participantsCount ?? null;
        } else if (entity.className === "Chat") {
          const full = await client.invoke(
            new Api.messages.GetFullChat({ chatId: entity.id })
          ) as any;
          count = full.fullChat?.participants?.participants?.length ?? null;
        }
      }

      return jsonResponse(res, 200, { count });
    } catch (err: any) {
      console.error("[http] /chat-count erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  jsonResponse(res, 404, { error: "Not found" });
});

httpServer.listen(WORKER_PORT, () => {
  console.log(`[worker] HTTP interno escutando na porta ${WORKER_PORT}`);
});

/* ─── Inicialização ─── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando...");
  await prewarmAccounts();
  await reloadSchedules();

  setInterval(async () => {
    try {
      await Promise.allSettled([reloadSchedules(), prewarmAccounts()]);
    } catch (err) {
      console.error("[reload] Erro no reload periódico:", err);
    }
  }, RELOAD_INTERVAL_MS);

  console.log("[worker] Pronto.");
}

init().catch((err) => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});
