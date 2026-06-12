// worker-flat.ts — dispatch worker Telegram, sem camadas de abstração
//
// [histórico de fixes v1–v13 omitido por brevidade — ver git]
//
// Fix v14 (2025-06) — listener de grupo aberto: latência ~0ms via updates MTProto:
//
//   PROBLEMA — 18s de delay entre sinal da admin e envio das mensagens:
//     O listener anterior usava GetHistory em polling com LISTEN_POLL_MS=1ms.
//     Apesar do intervalo de 1ms, cada iteração faz uma round-trip MTProto
//     (~100-200ms). Logo o detector real era ~100-200ms de ciclo — melhor caso.
//     Além disso, ao detectar o sinal, dispatchToGroup chamava getClient()
//     para cada conta no momento do disparo, e sendMessage tinha backoff de
//     1s no primeiro retry. Qualquer falha transitória somava segundos.
//
//   SOLUÇÃO — três mudanças cirúrgicas:
//
//   [A] Updates MTProto nativo em vez de GetHistory polling:
//     TelegramClient.addEventHandler com NewMessage captura updates em tempo
//     real via long-poll MTProto. Latência de detecção: praticamente zero
//     (igual ao Telegram Desktop). O handler é adicionado/removido de forma
//     segura via AbortController.
//     Fallback: GetHistory polling de 500ms continua rodando em paralelo para
//     cobrir casos edge (updates perdidos, grupos grandes onde o bot não recebe
//     o update diretamente). Se o handler disparar primeiro, cancela o polling.
//
//   [B] Pre-conexão + pre-resolve de peers de TODAS as contas do grupo
//     no início do listener (não só a conta de detecção):
//     Antes: ao detectar o sinal, getClient() podia precisar reconectar cada
//     conta, adicionando centenas de ms ou segundos ao caminho crítico.
//     Agora: todas as contas ficam conectadas e com peerCache aquecido
//     durante o período de espera, prontas para disparar imediatamente.
//
//   [C] dispatchOpenGroup(): variante ultra-rápida do dispatch para grupos abertos:
//     - Recebe os clients e peers já resolvidos como parâmetro (sem getClient())
//     - Dispara todas as contas em Promise.allSettled() simultâneo
//     - Sem backoff na primeira tentativa — falha imediata vai direto para retry
//     - Timeout por conta: OPEN_SEND_TIMEOUT_MS=5s (mais agressivo que sendMessage)
//     - Se alguma conta falhar, tenta uma vez mais com getClient() fresco
//     Resultado esperado: do sinal ao envio em <500ms.

import { createClient } from "@supabase/supabase-js";
import { TelegramClient, Api } from "telegram";
import { NewMessage, NewMessageEvent } from "telegram/events";
import { StringSession } from "telegram/sessions";
import bigInt from "big-integer";
import http from "http";

/* ─────────────────────────────────────────────────────────────────────────────
   SUPABASE
   ───────────────────────────────────────────────────────────────────────────── */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─────────────────────────────────────────────────────────────────────────────
   CONSTANTES
   ───────────────────────────────────────────────────────────────────────────── */

const SEND_TIMEOUT_MS               = 15_000;
const RETRY_BUDGET_MS               = 50_000;
const RELOAD_INTERVAL_MS            = 30_000;
const LOOKAHEAD_MS                  = 2 * 60 * 1000;
const KEEPALIVE_INTERVAL_MS         = 45_000;
const KEEPALIVE_JITTER_MAX_MS       = 10_000;
const PREFETCH_BEFORE_MS            = 800;
const MONITOR_MAX_OPEN_MS           = 5 * 60_000;
const MONITOR_POLL_MS               = 5_000;
const OPEN_GROUP_LISTEN_TIMEOUT_MS  = 2 * 60 * 60_000;
const SEND_RETRY_BACKOFF_MAX_MS     = 8_000;
const SNIPER_BEFORE_MS              = 45;
const SNIPER_SEND_TIMEOUT_MS        = 800;
const SNIPER_ATTEMPT_INTERVAL_MS    = 1;
const SNIPER_PAUSE_EVERY_N          = 10;
const SNIPER_PAUSE_MS               = 5;
const SNIPER_BUDGET_MS              = RETRY_BUDGET_MS;
const SNIPER_DONE_BLOCK_TTL_MS      = 500;

// v14: timeout por conta no dispatch rápido de grupos abertos
const OPEN_SEND_TIMEOUT_MS          = 5_000;
// v14: intervalo do polling de fallback (em paralelo com o event handler)
const OPEN_FALLBACK_POLL_MS         = 500;

// Monitor: aguarda N segundos após disparo para capturar histórico completo do leilão
const MONITOR_POST_DISPATCH_WAIT_MS = 10_000;
const MONITOR_HISTORY_FETCH_LIMIT   = 200;
const MONITOR_WINDOW_BEFORE_MS      = 5_000;

const WORKER_PORT   = parseInt(process.env.PORT ?? "3001", 10);
const WORKER_SECRET = process.env.WORKER_SECRET ?? "";

/* ─────────────────────────────────────────────────────────────────────────────
   TIPOS
   ───────────────────────────────────────────────────────────────────────────── */
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
  name: string;
  telegram_chat_id: string | null;
  telegram_chat_name: string | null;
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

interface DispatchResult {
  account_id: string;
  message_text: string | null;
  status: "sent" | "failed" | "skipped";
  retryable: boolean;
  error?: string;
}

/* ─────────────────────────────────────────────────────────────────────────────
   ESTADO GLOBAL
   ───────────────────────────────────────────────────────────────────────────── */

const clients               = new Map<string, TelegramClient>();
const sessions              = new Map<string, string>();
const keepaliveTimers       = new Map<string, ReturnType<typeof setInterval>>();
const peerCache             = new Map<string, unknown>();
const accountCache          = new Map<string, Account>();
const scheduledTimers       = new Map<string, ReturnType<typeof setTimeout>>();
const prefetchTimers        = new Map<string, ReturnType<typeof setTimeout>>();
const sniperTimers          = new Map<string, ReturnType<typeof setTimeout>>();
const schedulePrefetchCache = new Map<string, Schedule>();
const listenMap             = new Map<string, AbortController>();
const firingNow             = new Set<string>();
const sniperFiringNow       = new Set<string>();
const connectingPromises    = new Map<string, Promise<TelegramClient>>();

/* ─────────────────────────────────────────────────────────────────────────────
   QUERY REUTILIZADA
   ───────────────────────────────────────────────────────────────────────────── */
const SCHEDULE_SELECT = `
  id, cron_expression, user_id, group_id, next_run_at,
  retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
  retry_count, retry_until, last_attempt_at,
  groups(
    id, name, telegram_chat_id, telegram_chat_name, group_type,
    group_members(
      id, message_text, position, is_active,
      accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)
    )
  )
`.trim();

/* ─────────────────────────────────────────────────────────────────────────────
   HELPERS PUROS
   ───────────────────────────────────────────────────────────────────────────── */

function isRetryableError(msg: string): boolean {
  const u = msg.toUpperCase();
  return !u.includes("AUTH_KEY_UNREGISTERED") &&
         !u.includes("AUTH_KEY_DUPLICATED") &&
         !u.includes("USER_DEACTIVATED") &&
         !u.includes("SESSION_REVOKED");
}

function nextWeeklyOccurrence(cron: string): string {
  const parts = cron.trim().split(/\s+/);
  const mi    = parseInt(parts[0], 10);
  const h     = parseInt(parts[1], 10);
  const dow   = parseInt(parts[4], 10);

  if (
    parts.length < 5 ||
    isNaN(mi) || isNaN(h) || isNaN(dow) ||
    mi < 0 || mi > 59 || h < 0 || h > 23 || dow < 0 || dow > 6
  ) {
    throw new Error(`cron_expression inválida: "${cron}"`);
  }

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

function makeRandomId(): bigInt.BigInteger {
  const hi = Math.floor(Math.random() * 0xFFFFFFFF);
  const lo = Math.floor(Math.random() * 0xFFFFFFFF);
  return bigInt(hi).shiftLeft(32).add(bigInt(lo));
}

/* ─────────────────────────────────────────────────────────────────────────────
   HELPER: detecção de sinal universalizada (v13)
   ───────────────────────────────────────────────────────────────────────────── */
function hasAnyContent(m: any): boolean {
  if (!m) return false;

  const cls = m.className ?? "";
  const _   = m._ ?? "";

  if (cls === "MessageService" || _ === "messageService") return true;

  const isMsg = cls === "Message" || _ === "message";
  if (!isMsg) return false;

  if (typeof m.message === "string" && m.message.trim().length > 0) return true;

  if (m.media != null) {
    const mc      = m.media.className ?? "";
    const m_      = m.media._ ?? "";
    const isEmpty = mc === "MessageMediaEmpty" || m_ === "messageMediaEmpty";
    if (!isEmpty) return true;
  }

  return false;
}

/* ─────────────────────────────────────────────────────────────────────────────
   HELPERS DE PEER CACHE
   ───────────────────────────────────────────────────────────────────────────── */

function evictPeersForAccount(accountId: string): void {
  for (const key of peerCache.keys()) {
    if (key.startsWith(`${accountId}:`)) {
      peerCache.delete(key);
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   GERENCIAMENTO DE CONEXÕES TELEGRAM
   ───────────────────────────────────────────────────────────────────────────── */

async function getClient(account: Account): Promise<TelegramClient> {
  const existing       = clients.get(account.id);
  const sessionInUse   = sessions.get(account.id);
  const sessionChanged = sessionInUse !== account.session_string;

  if (existing?.connected && !sessionChanged) return existing;

  const inflight = connectingPromises.get(account.id);
  if (inflight) return inflight;

  const connectPromise = (async () => {
    if (existing) {
      try { await existing.disconnect(); } catch {}
      clients.delete(account.id);
      evictPeersForAccount(account.id);
      const t = keepaliveTimers.get(account.id);
      if (t) { clearInterval(t); keepaliveTimers.delete(account.id); }
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
      }
    );

    (client as any)._loopStarted = true;

    await client.connect();

    clients.set(account.id, client);
    sessions.set(account.id, account.session_string);

    const jitter   = Math.floor(Math.random() * KEEPALIVE_JITTER_MAX_MS);
    const interval = setInterval(async () => {
      if (!client.connected) {
        console.warn(`[keepalive] ${account.phone_number} desconectou — removendo do pool`);
        clients.delete(account.id);
        evictPeersForAccount(account.id);
        keepaliveTimers.delete(account.id);
        clearInterval(interval);
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
        console.warn(`[keepalive] Ping falhou para ${account.phone_number}: ${err.message}`);
        try { await client.disconnect(); } catch {}
        clients.delete(account.id);
        evictPeersForAccount(account.id);
        keepaliveTimers.delete(account.id);
        clearInterval(interval);

        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED") ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          console.warn(`[keepalive] Sessão morta: ${account.phone_number} — desativando no banco`);
          supabase.from("accounts").update({ is_active: false }).eq("id", account.id).then(({ error: e }) => {
            if (e) console.error(`[keepalive] Falha ao desativar ${account.id}:`, e.message);
          });
        }
      }
    }, KEEPALIVE_INTERVAL_MS + jitter);

    keepaliveTimers.set(account.id, interval);
    console.log(`[client] ✓ Conectado: ${account.phone_number}`);
    return client;
  })();

  connectingPromises.set(account.id, connectPromise);
  try {
    return await connectPromise;
  } finally {
    connectingPromises.delete(account.id);
  }
}

async function reloadClient(account: Account): Promise<TelegramClient> {
  const inflight = connectingPromises.get(account.id);
  if (inflight) {
    try { await inflight; } catch {}
  }
  connectingPromises.delete(account.id);

  const existing = clients.get(account.id);
  if (existing) {
    try { await existing.disconnect(); } catch {}
    clients.delete(account.id);
    evictPeersForAccount(account.id);
    const t = keepaliveTimers.get(account.id);
    if (t) { clearInterval(t); keepaliveTimers.delete(account.id); }
  }

  return getClient(account);
}

/* ─────────────────────────────────────────────────────────────────────────────
   RESOLUÇÃO DE PEER TELEGRAM
   ───────────────────────────────────────────────────────────────────────────── */
async function resolvePeer(
  client: TelegramClient,
  telegramChatId: string,
  accountId: string
): Promise<unknown> {
  const key = `${accountId}:${telegramChatId}`;
  if (peerCache.has(key)) return peerCache.get(key)!;

  const chatIdNum = parseInt(telegramChatId, 10);
  if (isNaN(chatIdNum)) throw new Error(`telegram_chat_id inválido: "${telegramChatId}"`);

  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(key, peer);
    return peer;
  } catch {}

  const absId     = Math.abs(chatIdNum);
  const channelId = absId > 1_000_000_000_000 ? absId - 1_000_000_000_000 : absId;
  try {
    const result = await client.invoke(
      new Api.channels.GetChannels({
        id: [new Api.InputChannel({ channelId: bigInt(channelId), accessHash: bigInt(0) })],
      })
    ) as any;
    const chat = result?.chats?.[0];
    if (chat?.accessHash != null) {
      const peer = new Api.InputPeerChannel({ channelId: chat.id, accessHash: chat.accessHash });
      peerCache.set(key, peer);
      return peer;
    }
  } catch {}

  await client.getDialogs({ limit: 200 });
  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(key, peer);
    return peer;
  } catch (e: any) {
    throw new Error(
      `PEER_UNRESOLVABLE ${telegramChatId}: conta não é membro ou sessão inválida. ` +
      `Último erro: ${e.message}`
    );
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   ENVIO COM RETRY INTERNO (grupos fechados / fallback)
   ───────────────────────────────────────────────────────────────────────────── */
async function sendMessage(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string
): Promise<void> {
  const budgetEnd      = Date.now() + RETRY_BUDGET_MS;
  const stableRandomId = makeRandomId();
  let attempt = 0;

  while (Date.now() < budgetEnd) {
    attempt++;
    const timeLeft = budgetEnd - Date.now();
    if (timeLeft < 500) break;

    try {
      await Promise.race([
        (async () => {
          const peer = await resolvePeer(client, telegramChatId, account.id);

          try {
            await client.invoke(new Api.messages.SendMessage({
              peer:      peer as any,
              message:   messageText,
              randomId:  stableRandomId,
              noWebpage: true,
            }));
          } catch (err: any) {
            const errMsg = String(err?.message ?? "");

            if (
              errMsg.includes("PEER_ID_INVALID") ||
              errMsg.includes("CHANNEL_INVALID") ||
              errMsg.includes("CHANNEL_PRIVATE")
            ) {
              peerCache.delete(`${account.id}:${telegramChatId}`);
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
              console.warn(`[send] FloodWait ${waitSecs}s — ${account.phone_number}`);

              if (waitMs < budgetEnd - Date.now() - 500) {
                await new Promise(r => setTimeout(r, waitMs));
                peerCache.delete(`${account.id}:${telegramChatId}`);
                const freshPeer = await resolvePeer(client, telegramChatId, account.id);
                await client.invoke(new Api.messages.SendMessage({
                  peer:      freshPeer as any,
                  message:   messageText,
                  randomId:  stableRandomId,
                  noWebpage: true,
                }));
                return;
              }
              throw new Error(`FLOOD_WAIT_${waitSecs}_EXCEEDS_BUDGET`);
            }

            throw err;
          }
        })(),
        new Promise<never>((_, r) =>
          setTimeout(
            () => r(new Error(`TIMEOUT tentativa ${attempt}`)),
            Math.min(SEND_TIMEOUT_MS, timeLeft - 100)
          )
        ),
      ]);

      if (attempt > 1) console.log(`[send] ✓ ${account.phone_number} — enviou na tentativa ${attempt}`);
      return;

    } catch (err: any) {
      const remaining = budgetEnd - Date.now();
      if (remaining > 500) {
        const backoffMs   = Math.min(1_000 * Math.pow(2, attempt - 1), SEND_RETRY_BACKOFF_MAX_MS);
        const safeBackoff = Math.min(backoffMs, remaining - 500);
        console.warn(
          `[send] tentativa ${attempt} falhou — aguardando ${safeBackoff}ms ` +
          `(${Math.round(remaining / 1000)}s restantes): ${err.message}`
        );
        if (safeBackoff > 0) {
          await new Promise(r => setTimeout(r, safeBackoff));
        }
      }
    }
  }

  throw new Error(`BUDGET_EXCEEDED após ${attempt} tentativa(s) em ${RETRY_BUDGET_MS / 1000}s`);
}

/* ─────────────────────────────────────────────────────────────────────────────
   SNIPER — ENVIO ÚNICO COM TIMEOUT CURTO
   ───────────────────────────────────────────────────────────────────────────── */
async function sniperSendOnce(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string,
  randomId: bigInt.BigInteger
): Promise<void> {
  const peer = await resolvePeer(client, telegramChatId, account.id);

  await Promise.race([
    client.invoke(new Api.messages.SendMessage({
      peer:      peer as any,
      message:   messageText,
      randomId,
      noWebpage: true,
    })),
    new Promise<never>((_, r) =>
      setTimeout(() => r(new Error("SNIPER_TIMEOUT")), SNIPER_SEND_TIMEOUT_MS)
    ),
  ]);
}

/* ─────────────────────────────────────────────────────────────────────────────
   DISPATCH RÁPIDO — GRUPOS ABERTOS (v14)
   Recebe clients e peers já aquecidos — sem getClient() no caminho crítico.
   Dispara todas as contas em paralelo com timeout agressivo.
   ───────────────────────────────────────────────────────────────────────────── */
async function dispatchOpenGroup(
  schedule: Schedule,
  group: Group,
  warmClients: Map<string, TelegramClient>,   // accountId → client já conectado
  warmPeers:   Map<string, unknown>,           // accountId → peer já resolvido
): Promise<DispatchResult[]> {
  const members = (group.group_members ?? [])
    .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);

  const chatId = group.telegram_chat_id!;

  const results = await Promise.allSettled(
    members.map(async (member, i): Promise<DispatchResult> => {
      const account      = member.accounts!;
      const positionRank = i + 1;
      const randomId     = makeRandomId();

      // Usa client e peer pré-aquecidos; cai para getClient se não tiver
      let client = warmClients.get(account.id) ?? null;
      let peer   = warmPeers.get(account.id)   ?? null;

      const tryOnce = async (cl: TelegramClient, pr: unknown): Promise<void> => {
        await Promise.race([
          cl.invoke(new Api.messages.SendMessage({
            peer:      pr as any,
            message:   member.message_text ?? "",
            randomId,
            noWebpage: true,
          })),
          new Promise<never>((_, r) =>
            setTimeout(() => r(new Error("OPEN_SEND_TIMEOUT")), OPEN_SEND_TIMEOUT_MS)
          ),
        ]);
      };

      let status: "sent" | "failed" = "failed";
      let error: string | undefined;

      try {
        if (!client || !client.connected) {
          client = await getClient(account);
        }
        if (!peer) {
          peer = await resolvePeer(client, chatId, account.id);
        }

        await tryOnce(client, peer);
        status = "sent";
        console.log(`[open-dispatch] ✓ ${account.phone_number} (pos ${positionRank})`);

      } catch (err: any) {
        error = String(err?.message ?? "");

        // Tenta uma vez mais com client e peer frescos
        try {
          console.warn(`[open-dispatch] ⚠ ${account.phone_number} erro (${error}) — retry com client fresco`);
          const freshClient = await getClient(account);
          peerCache.delete(`${account.id}:${chatId}`);
          const freshPeer   = await resolvePeer(freshClient, chatId, account.id);
          await tryOnce(freshClient, freshPeer);
          status = "sent";
          error  = undefined;
          console.log(`[open-dispatch] ✓ ${account.phone_number} recuperou no retry`);
        } catch (err2: any) {
          error = String(err2?.message ?? "");
          console.error(`[open-dispatch] ✗ ${account.phone_number}: ${error}`);
        }
      }

      // Log de dispatch em background
      supabase.from("dispatch_logs").insert({
        user_id:             schedule.user_id,
        group_id:            group.id,
        account_id:          account.id,
        schedule_id:         schedule.id,
        status,
        message_text:        member.message_text,
        position_rank:       positionRank,
        group_name_snapshot: group.name,
        chat_name_snapshot:  group.telegram_chat_name,
        sent_at:             status === "sent" ? new Date().toISOString() : null,
        error_message:       error ?? null,
      }).then(({ error: e }) => {
        if (e) console.error(`[open-dispatch][log] Falha para ${account.id}:`, e.message);
      });

      return {
        account_id:   account.id,
        message_text: member.message_text,
        status,
        retryable:    status === "failed" ? isRetryableError(error ?? "") : false,
        error,
      };
    })
  );

  return results.map(r =>
    r.status === "fulfilled"
      ? r.value
      : {
          account_id:   "",
          message_text: null,
          status:       "failed" as const,
          retryable:    false,
          error:        String((r as PromiseRejectedResult).reason),
        }
  );
}

/* ─────────────────────────────────────────────────────────────────────────────
   SNIPER LOOP — GRUPOS FECHADOS (v11: loop unificado, todas as contas em paralelo)
   ───────────────────────────────────────────────────────────────────────────── */
async function sniperFireClosed(scheduleId: string): Promise<void> {
  if (sniperFiringNow.has(scheduleId)) {
    console.warn(`[sniper] Schedule ${scheduleId} já em execução — ignorando disparo duplicado`);
    return;
  }
  sniperFiringNow.add(scheduleId);

  const sniperEnteredAt = Date.now();

  try {
    const now = new Date();

    let schedule = schedulePrefetchCache.get(scheduleId);
    if (schedule) {
      schedulePrefetchCache.delete(scheduleId);
      console.log(`[sniper] ⚡ Schedule ${scheduleId} servido do pre-fetch cache`);
    } else {
      const { data, error } = await supabase
        .from("schedules")
        .select(SCHEDULE_SELECT)
        .eq("id", scheduleId)
        .eq("is_active", true)
        .single();
      if (error || !data) {
        console.warn(`[sniper] Schedule ${scheduleId} não encontrado ou inativo.`);
        return;
      }
      schedule = data as unknown as Schedule;
    }

    const scheduledAt     = new Date(schedule.next_run_at).getTime();
    const plannedSniperAt = scheduledAt - SNIPER_BEFORE_MS;
    const timerLagMs      = sniperEnteredAt - plannedSniperAt;
    console.log(`[sniper][timing] timer lag: ${timerLagMs}ms (SNIPER_BEFORE_MS=${SNIPER_BEFORE_MS}, ideal=0)`);

    const group = schedule.groups;

    if (!group?.telegram_chat_id) {
      console.warn(`[sniper] Schedule ${scheduleId}: sem telegram_chat_id — pulando.`);
      return;
    }

    if (group.group_members) {
      group.group_members = group.group_members.map(m => ({
        ...m,
        accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
      }));
    }

    const members = (group.group_members ?? [])
      .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
      .sort((a, b) => a.position - b.position);

    if (members.length === 0) {
      console.warn(`[sniper] Nenhuma conta ativa no schedule ${scheduleId} — abortando.`);
      return;
    }

    const chatId    = group.telegram_chat_id;
    const budgetEnd = Date.now() + SNIPER_BUDGET_MS;

    console.log(`[sniper] 🎯 Loop unificado para schedule ${scheduleId} — ${members.length} conta(s) em paralelo`);

    type AccountState = "pending" | "sent" | "fatal";

    interface SlotState {
      member:       typeof members[number];
      account:      Account;
      state:        AccountState;
      randomId:     bigInt.BigInteger;
      client:       TelegramClient | null;
      sentAt:       Date | null;
      error:        string | undefined;
      attempts:     number;
      floodUntil:   number;
    }

    const slots: SlotState[] = await Promise.all(
      members.map(async (member) => {
        const account = member.accounts!;
        let client: TelegramClient | null = null;
        let state: AccountState = "pending";
        let error: string | undefined;

        try {
          client = await getClient(account);
        } catch (err: any) {
          error = String(err?.message ?? "");
          state = "fatal";
          console.error(`[sniper] Falha ao conectar ${account.phone_number}: ${error}`);
        }

        return {
          member,
          account,
          state,
          randomId:   makeRandomId(),
          client,
          sentAt:     null,
          error,
          attempts:   0,
          floodUntil: 0,
        } satisfies SlotState;
      })
    );

    let globalAttempt = 0;

    while (Date.now() < budgetEnd) {
      const pendingSlots = slots.filter(s => s.state === "pending");
      if (pendingSlots.length === 0) break;

      globalAttempt++;

      const msUntilScheduled = scheduledAt - Date.now();
      if (msUntilScheduled > 0) {
        if (globalAttempt === 1) {
          console.log(`[sniper] ⏳ Aguardando scheduledAt (faltam ${msUntilScheduled}ms)`);
        }
        await new Promise(r => setTimeout(r, msUntilScheduled));
        if (Date.now() >= budgetEnd) break;
      }

      const now2 = Date.now();

      await Promise.allSettled(
        pendingSlots.map(async (slot) => {
          if (slot.floodUntil > now2) return;

          slot.attempts++;

          try {
            await sniperSendOnce(slot.client!, slot.account, chatId, slot.member.message_text ?? "", slot.randomId);

            slot.sentAt = new Date();
            slot.state  = "sent";

            const invokeRttMs = slot.sentAt.getTime() - sniperEnteredAt;
            const vsHorarioMs = slot.sentAt.getTime() - scheduledAt;
            console.log(
              `[sniper][timing] ${slot.account.phone_number} — ` +
              `invoke RTT: ${invokeRttMs}ms | ` +
              `vs horário: ${vsHorarioMs > 0 ? "+" : ""}${vsHorarioMs}ms | ` +
              `tentativa: ${slot.attempts}`
            );
            console.log(`[sniper] ✓ ${slot.account.phone_number} enviou`);

          } catch (err: any) {
            const errMsg = String(err?.message ?? "");

            const isFatal =
              errMsg.includes("AUTH_KEY_UNREGISTERED") ||
              errMsg.includes("AUTH_KEY_DUPLICATED")   ||
              errMsg.includes("USER_DEACTIVATED")      ||
              errMsg.includes("SESSION_REVOKED");

            if (isFatal) {
              slot.state = "fatal";
              slot.error = errMsg;
              console.error(`[sniper] ✗ Fatal ${slot.account.phone_number}: ${errMsg}`);
              return;
            }

            const isFlood =
              err?.seconds != null ||
              err?.constructor?.name === "FloodWaitError" ||
              /flood/i.test(errMsg);

            if (isFlood) {
              const waitSecs = typeof err.seconds === "number"
                ? err.seconds
                : parseInt(errMsg.match(/(\d+)/)?.[1] ?? "5", 10);
              const waitMs = waitSecs * 1000;

              if (now2 + waitMs < budgetEnd - 500) {
                slot.floodUntil = now2 + waitMs;
                console.warn(`[sniper] FloodWait ${waitSecs}s — ${slot.account.phone_number} pausará individualmente`);
              } else {
                slot.state = "fatal";
                slot.error = `FLOOD_WAIT_${waitSecs}_EXCEEDS_BUDGET`;
                console.warn(`[sniper] FloodWait ${waitSecs}s excede budget — ${slot.account.phone_number} marcada fatal`);
              }
              return;
            }

            if (
              errMsg.includes("PEER_ID_INVALID") ||
              errMsg.includes("CHANNEL_INVALID") ||
              errMsg.includes("CHANNEL_PRIVATE")
            ) {
              peerCache.delete(`${slot.account.id}:${chatId}`);
            }
          }
        })
      );

      if (slots.some(s => s.state === "pending")) {
        if (globalAttempt % SNIPER_PAUSE_EVERY_N === 0) {
          await new Promise(r => setTimeout(r, SNIPER_PAUSE_MS));
        } else {
          await new Promise(r => setTimeout(r, SNIPER_ATTEMPT_INTERVAL_MS));
        }
      }
    }

    const results: DispatchResult[] = slots.map(slot => {
      if (slot.state === "sent") {
        return { account_id: slot.account.id, message_text: slot.member.message_text, status: "sent", retryable: false };
      }

      const timedOut = slot.state === "pending";
      const errMsg   = timedOut
        ? `SNIPER_BUDGET_EXCEEDED após ${slot.attempts} tentativas`
        : (slot.error ?? "FATAL_ERROR");

      if (timedOut) {
        console.warn(`[sniper] Budget esgotado para ${slot.account.phone_number} após ${slot.attempts} tentativas`);
      }

      const anySent   = slots.some(s => s.state === "sent");
      const retryable = anySent ? false : (!timedOut ? isRetryableError(errMsg) : true);

      return { account_id: slot.account.id, message_text: slot.member.message_text, status: "failed", retryable, error: errMsg };
    });

    for (const [i, slot] of slots.entries()) {
      const result = results[i];
      supabase.from("dispatch_logs").insert({
        user_id:             schedule.user_id,
        group_id:            group.id,
        account_id:          slot.account.id,
        schedule_id:         schedule.id,
        status:              result.status,
        message_text:        slot.member.message_text,
        position_rank:       i + 1,
        group_name_snapshot: group.name,
        chat_name_snapshot:  group.telegram_chat_name,
        sent_at:             slot.sentAt ? slot.sentAt.toISOString() : null,
        error_message:       result.error ?? null,
      }).then(({ error: e }) => {
        if (e) console.error(`[sniper][log] Falha ao inserir log para ${slot.account.id}:`, e.message);
      });
    }

    const sentSlots   = slots.filter(s => s.state === "sent");
    const firstSentAt = sentSlots.length > 0
      ? sentSlots.reduce((min, s) => s.sentAt! < min ? s.sentAt! : min, sentSlots[0].sentAt!)
      : null;

    if (sentSlots.length > 0 && firstSentAt) {
      const sentForMonitor = sentSlots.map(s => ({
        account_id:   s.account.id,
        message_text: s.member.message_text ?? "",
      })).filter(s => s.message_text);

      if (sentForMonitor.length > 0) {
        monitorPositions(chatId, sentForMonitor, scheduleId, firstSentAt, "closed")
          .catch(err => console.error("[sniper][monitor] Erro:", err.message));
      }
    }

    const dispatchNow = firstSentAt ?? now;
    await updateScheduleAfterDispatch(schedule, results, dispatchNow, "closed");

  } finally {
    sniperFiringNow.delete(scheduleId);
    firingNow.add(scheduleId);
    setTimeout(() => firingNow.delete(scheduleId), SNIPER_DONE_BLOCK_TTL_MS);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   DEDUPLICAÇÃO
   ───────────────────────────────────────────────────────────────────────────── */
async function getAlreadySentIds(schedule: Schedule): Promise<Set<string>> {
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
  return new Set((data ?? []).map(r => r.account_id as string));
}

/* ─────────────────────────────────────────────────────────────────────────────
   MONITOR — HELPER INTERNO
   ───────────────────────────────────────────────────────────────────────────── */
async function _savePositions(
  sentMembers: Array<{ account_id: string; message_text: string }>,
  windowMsgs: any[],
  scheduleId: string,
  dispatchedAt: Date,
): Promise<void> {
  const cutoff = new Date(dispatchedAt.getTime() - 60_000).toISOString();

  await Promise.allSettled(sentMembers.map(sm => {
    if (!sm.message_text) return;

    const myMsgIndex = windowMsgs.findIndex((m: any) => m.message === sm.message_text);
    if (myMsgIndex < 0) {
      console.warn(`[monitor] Mensagem não encontrada no histórico para conta ${sm.account_id}`);
      return;
    }

    const myMsgId  = windowMsgs[myMsgIndex].id as number;
    const position = windowMsgs.filter((m: any) => (m.id as number) < myMsgId).length + 1;

    console.log(
      `[monitor] ${sm.account_id}: posição #${position} ` +
      `(msg_id=${myMsgId}, total na janela=${windowMsgs.length})`
    );

    return supabase.from("dispatch_logs")
      .update({ position_rank: position })
      .eq("schedule_id", scheduleId)
      .eq("account_id", sm.account_id)
      .eq("status", "sent")
      .gte("sent_at", cutoff);
  }));

  console.log(`[monitor] ✓ Posições salvas para schedule ${scheduleId}`);
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
  if (!account) { console.warn("[monitor] Conta não encontrada no cache — ignorando"); return; }

  const client = await getClient(account).catch(() => null);
  if (!client) { console.warn("[monitor] Sem client — ignorando monitoramento"); return; }

  const windowStartUnix = Math.floor(
    (dispatchedAt.getTime() - MONITOR_WINDOW_BEFORE_MS) / 1000
  );

  if (groupType === "closed") {
    console.log(`[monitor] ⏳ Aguardando ${MONITOR_POST_DISPATCH_WAIT_MS / 1000}s (schedule ${scheduleId})`);
    await new Promise(r => setTimeout(r, MONITOR_POST_DISPATCH_WAIT_MS));

    try {
      const peer   = await resolvePeer(client, telegramChatId, account.id);
      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer: peer as any, limit: MONITOR_HISTORY_FETCH_LIMIT,
          offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
          hash: bigInt(0), addOffset: 0,
        })
      ) as any;

      const windowMsgs: any[] = (result.messages ?? [])
        .filter((m: any) => m._ === "message" && (m.date as number) >= windowStartUnix)
        .sort((a: any, b: any) => (a.id as number) - (b.id as number));

      if (windowMsgs.length === 0) {
        console.warn(`[monitor] Nenhuma mensagem na janela (closed) — schedule ${scheduleId}`);
        return;
      }

      console.log(`[monitor] ${windowMsgs.length} msg(s) na janela para schedule ${scheduleId} (closed)`);
      await _savePositions(sentMembers, windowMsgs, scheduleId, dispatchedAt);
    } catch (err: any) {
      console.warn(`[monitor] Erro ao buscar histórico (closed): ${err.message}`);
    }
    return;
  }

  const ourTexts = new Set(sentMembers.map(m => m.message_text).filter(Boolean));
  const deadline = Date.now() + MONITOR_MAX_OPEN_MS;

  console.log(`[monitor] Iniciando polling para schedule ${scheduleId} (open)`);

  while (Date.now() < deadline) {
    try {
      const peer   = await resolvePeer(client, telegramChatId, account.id);
      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer: peer as any, limit: MONITOR_HISTORY_FETCH_LIMIT,
          offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
          hash: bigInt(0), addOffset: 0,
        })
      ) as any;

      const windowMsgs: any[] = (result.messages ?? [])
        .filter((m: any) => m._ === "message" && (m.date as number) >= windowStartUnix)
        .sort((a: any, b: any) => (a.id as number) - (b.id as number));

      if (windowMsgs.length === 0 || !windowMsgs.some((m: any) => ourTexts.has(m.message))) {
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      await _savePositions(sentMembers, windowMsgs, scheduleId, dispatchedAt);
      return;
    } catch (err: any) {
      console.warn(`[monitor] Erro ao buscar histórico (open): ${err.message}`);
      await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
    }
  }

  console.warn(`[monitor] Timeout — posições não registradas para schedule ${scheduleId}`);
}

/* ─────────────────────────────────────────────────────────────────────────────
   LISTENER DE GRUPO ABERTO — v14
   Usa NewMessage event handler (MTProto updates nativos) para detecção
   em tempo real, com polling de fallback a cada 500ms como segurança.
   Pré-aquece clients e peers de TODAS as contas durante a espera.
   ───────────────────────────────────────────────────────────────────────────── */
function startGroupListener(schedule: Schedule, group: Group, account: Account): void {
  const existing = listenMap.get(group.id);
  if (existing) existing.abort();

  const ctrl = new AbortController();
  listenMap.set(group.id, ctrl);

  const chatId  = group.telegram_chat_id!;
  const chatNum = parseInt(chatId, 10);

  console.log(`[listen] 👂 Aguardando sinal em ${chatId} para schedule ${schedule.id}`);

  (async () => {
    try {
      // ── 1. Conecta a conta de detecção ────────────────────────────────
      let detectClient = await getClient(account).catch(() => null);
      if (!detectClient) {
        console.warn(`[listen] Sem client de detecção — abortando listener para ${schedule.id}`);
        listenMap.delete(group.id);
        return;
      }
      try { await resolvePeer(detectClient, chatId, account.id); } catch {}

      // ── 2. Pré-aquece TODAS as contas do grupo em background ──────────
      const members = (group.group_members ?? [])
        .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
        .sort((a, b) => a.position - b.position);

      const warmClients = new Map<string, TelegramClient>();
      const warmPeers   = new Map<string, unknown>();

      // Não bloqueia o listener — aquece em paralelo
      Promise.allSettled(members.map(async m => {
        const acc = m.accounts!;
        try {
          const cl = await getClient(acc);
          warmClients.set(acc.id, cl);
          const peer = await resolvePeer(cl, chatId, acc.id);
          warmPeers.set(acc.id, peer);
          console.log(`[listen] ✓ Pre-warm conta ${acc.phone_number}`);
        } catch (e: any) {
          console.warn(`[listen] Pre-warm falhou para ${acc.phone_number}: ${e.message}`);
        }
      })).catch(() => {});

      // ── 3. Flag de disparo — evita double-fire entre handler e fallback
      let fired = false;

      const doFire = async (source: string) => {
        if (fired || ctrl.signal.aborted) return;
        fired = true;
        listenMap.delete(group.id);

        const signalAt   = Date.now();
        const detectLag  = source === "event" ? "~0ms" : `~${OPEN_FALLBACK_POLL_MS}ms`;
        console.log(`[listen] ✓ Sinal detectado via ${source} (lag ${detectLag}) — disparando schedule ${schedule.id}`);

        await supabase.from("groups").update({ listener_session_id: null }).eq("id", group.id);

        const dispatchedAt = new Date();
        const results      = await dispatchOpenGroup(schedule, group, warmClients, warmPeers);

        const elapsed = Date.now() - signalAt;
        const sent    = results.filter(r => r.status === "sent").length;
        console.log(`[listen] ✓ ${sent}/${members.length} mensagens enviadas em ${elapsed}ms`);

        const sentForMonitor = results
          .filter(r => r.status === "sent")
          .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
          .filter(r => r.message_text);
        if (sentForMonitor.length > 0) {
          monitorPositions(chatId, sentForMonitor, schedule.id, dispatchedAt, "open")
            .catch(err => console.error("[listen] Erro no monitoramento:", err.message));
        }

        await updateScheduleAfterDispatch(schedule, results, dispatchedAt, "open");
      };

      // ── 4. Event handler MTProto — detecção em tempo real ─────────────
      const startUnix = Math.floor(Date.now() / 1000) - 5; // 5s de margem para atraso de entrega

      const handler = async (event: NewMessageEvent) => {
        if (fired || ctrl.signal.aborted) return;

        const msg = event.message;
        if (!msg) return;

        // Filtra pelo chat correto
        const msgChatId = msg.chatId?.toJSNumber?.() ?? msg.peerId?.channelId?.toJSNumber?.() ?? 0;
        const absChat   = Math.abs(chatNum);
        const absMsg    = Math.abs(msgChatId);
        if (absMsg !== absChat && absMsg !== absChat - 1_000_000_000_000) return;

        // Ignora mensagens antigas (antes do listener começar)
        if ((msg as any).date < startUnix) return;

        await doFire("event");
      };

      detectClient.addEventHandler(handler, new NewMessage({}));

      // ── 5. Polling de fallback — cobre updates perdidos ───────────────
      const deadline    = Date.now() + OPEN_GROUP_LISTEN_TIMEOUT_MS;
      let lastSeenMsgId = 0;

      const fallbackLoop = async () => {
        while (Date.now() < deadline && !ctrl.signal.aborted && !fired) {
          try {
            if (!detectClient!.connected) {
              console.warn(`[listen] Client desconectou — reconectando para ${schedule.id}`);
              detectClient = await getClient(account);
              detectClient!.addEventHandler(handler, new NewMessage({}));
              try { await resolvePeer(detectClient!, chatId, account.id); } catch {}
            }

            const peer   = await resolvePeer(detectClient!, chatId, account.id);
            const result = await detectClient!.invoke(
              new Api.messages.GetHistory({
                peer: peer as any, limit: 5,
                offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
                hash: bigInt(0), addOffset: 0,
              })
            ) as any;

            const recentMsgs = (result.messages ?? []).filter(
              (m: any) => m.date >= startUnix && m.id > lastSeenMsgId
            );
            if (recentMsgs.length > 0) {
              lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
            }

            if (!fired && recentMsgs.some((m: any) => hasAnyContent(m))) {
              await doFire("fallback-poll");
              break;
            }
          } catch (err: any) {
            if (!ctrl.signal.aborted && !fired) {
              console.warn(`[listen] Erro no fallback poll (${schedule.id}): ${err.message}`);
              await new Promise(r => setTimeout(r, 2_000));
            }
          }

          if (!fired && !ctrl.signal.aborted) {
            await new Promise(r => setTimeout(r, OPEN_FALLBACK_POLL_MS));
          }
        }

        // Cleanup do handler quando o loop termina
        try { detectClient?.removeEventHandler(handler, new NewMessage({})); } catch {}

        if (fired || ctrl.signal.aborted) return;

        // Timeout de 2h sem sinal
        listenMap.delete(group.id);
        console.warn(`[listen] ⏰ Timeout 2h — nenhum sinal para schedule ${schedule.id}`);
        const nowISO = new Date().toISOString();
        let nextRun: string;
        try { nextRun = nextWeeklyOccurrence(schedule.cron_expression); }
        catch {
          await supabase.from("schedules").update({ is_active: false }).eq("id", schedule.id);
          return;
        }
        await supabase.from("schedules").update({
          next_run_at:         nextRun,
          retry_until:         null,
          retry_count:         0,
          last_attempt_at:     nowISO,
          last_attempt_status: "timeout",
          last_attempt_error:  "Timeout aguardando sinal do admin",
        }).eq("id", schedule.id);
        scheduleTimer(schedule.id, nextRun, "open");
      };

      fallbackLoop().catch(err => {
        console.error(`[listen] Erro inesperado no fallback loop para ${schedule.id}:`, err.message);
        listenMap.delete(group.id);
      });

    } catch (err: any) {
      console.error(`[listen] Erro inesperado para schedule ${schedule.id}:`, err.message);
      listenMap.delete(group.id);
    }
  })();
}

/* ─────────────────────────────────────────────────────────────────────────────
   DESPACHO PARA O GRUPO (grupos fechados / fallback geral)
   ───────────────────────────────────────────────────────────────────────────── */
async function dispatchToGroup(
  schedule: Schedule,
  group: Group,
  alreadySent: Set<string>
): Promise<DispatchResult[]> {
  const members = (group.group_members ?? [])
    .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);

  return Promise.all(members.map(async (member, i) => {
    const account      = member.accounts!;
    const positionRank = i + 1;

    if (alreadySent.has(account.id)) {
      console.log(`[dispatch] ↷ ${account.phone_number} — já enviou neste ciclo`);
      return { account_id: account.id, message_text: member.message_text, status: "skipped" as const, retryable: false };
    }

    let status: "sent" | "failed" = "failed";
    let error: string | undefined;
    let retryable = false;

    try {
      const client = await getClient(account);
      await sendMessage(client, account, group.telegram_chat_id!, member.message_text ?? "");
      status = "sent";
      alreadySent.add(account.id);
      console.log(`[dispatch] ✓ ${account.phone_number}`);
    } catch (err) {
      error     = err instanceof Error ? err.message : String(err);
      retryable = isRetryableError(error);
      console.error(`[dispatch] ✗ ${account.phone_number} [${retryable ? "retryável" : "permanente"}]: ${error}`);
    }

    supabase.from("dispatch_logs").insert({
      user_id:             schedule.user_id,
      group_id:            group.id,
      account_id:          account.id,
      schedule_id:         schedule.id,
      status,
      message_text:        member.message_text,
      position_rank:       positionRank,
      group_name_snapshot: group.name,
      chat_name_snapshot:  group.telegram_chat_name,
      sent_at:             status === "sent" ? new Date().toISOString() : null,
      error_message:       error ?? null,
    }).then(({ error: e }) => {
      if (e) console.error(`[log] Falha ao inserir dispatch_log para ${account.id}:`, e.message);
    });

    return { account_id: account.id, message_text: member.message_text, status, retryable, error };
  }));
}

/* ─────────────────────────────────────────────────────────────────────────────
   ATUALIZAÇÃO DO SCHEDULE APÓS DISPARO
   ───────────────────────────────────────────────────────────────────────────── */
async function updateScheduleAfterDispatch(
  schedule: Schedule,
  results: DispatchResult[],
  now: Date,
  groupType?: "open" | "closed"
): Promise<void> {
  const nowISO = now.toISOString();

  const sentCount      = results.filter(r => r.status === "sent").length;
  const skippedCount   = results.filter(r => r.status === "skipped").length;
  const retryableFails = results.filter(r => r.status === "failed" && r.retryable);
  const permanentFails = results.filter(r => r.status === "failed" && !r.retryable);

  const hasActiveMembers = results.length > 0;
  const allOk =
    hasActiveMembers &&
    retryableFails.length === 0 &&
    permanentFails.length === 0 &&
    (sentCount + skippedCount) > 0;

  if (allOk) {
    let nextRun: string;
    try {
      nextRun = nextWeeklyOccurrence(schedule.cron_expression);
    } catch (err) {
      console.error(`[schedule] cron inválido em ${schedule.id}, desativando:`, err);
      await supabase.from("schedules").update({ is_active: false }).eq("id", schedule.id);
      return;
    }

    supabase.from("schedules").update({
      next_run_at:         nextRun,
      last_run_at:         nowISO,
      retry_until:         null,
      retry_count:         0,
      last_attempt_at:     nowISO,
      last_attempt_status: "sent",
      last_attempt_error:  null,
    }).eq("id", schedule.id).then(({ error: e }) => {
      if (e) console.error(`[schedule] Falha ao atualizar ${schedule.id}:`, e.message);
    });

    console.log(`[schedule] ✓ Schedule ${schedule.id} OK. Próxima: ${nextRun}`);
    scheduleTimer(schedule.id, nextRun, groupType ?? schedule.groups?.group_type);

  } else {
    const newRetryCount = schedule.retry_count + 1;
    const retryUntil    = schedule.retry_until ??
      new Date(now.getTime() + schedule.retry_window_seconds * 1000).toISOString();
    const interval      = calcRetryInterval(
      newRetryCount,
      schedule.retry_interval_seconds,
      schedule.retry_interval_max_seconds
    );
    const failErrors = results
      .filter(r => r.error)
      .map(r => `[${r.account_id}] ${r.error}`)
      .join("; ");

    console.warn(
      `[schedule] ⚠ ${schedule.id}: ${retryableFails.length} falha(s) retryável(eis), ` +
      `${permanentFails.length} permanente(s). Retry #${newRetryCount} em ~${interval}s`
    );

    await supabase.from("schedules").update({
      retry_until:         retryUntil,
      retry_count:         newRetryCount,
      last_attempt_at:     nowISO,
      last_attempt_status: "retrying",
      last_attempt_error:  failErrors || null,
    }).eq("id", schedule.id);

    const retryAt = new Date(now.getTime() + interval * 1000);
    if (retryAt < new Date(retryUntil)) {
      scheduleTimer(schedule.id, retryAt.toISOString(), groupType ?? schedule.groups?.group_type);
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   DISPARO DE SCHEDULE (grupos abertos + fallback para fechados sem sniper)
   ───────────────────────────────────────────────────────────────────────────── */
async function fireSchedule(scheduleId: string): Promise<void> {
  if (sniperFiringNow.has(scheduleId)) {
    console.warn(`[fire] Schedule ${scheduleId} em execução no sniper — ignorando fireSchedule`);
    return;
  }

  if (firingNow.has(scheduleId)) {
    console.warn(`[fire] Schedule ${scheduleId} já em execução — ignorando disparo duplicado`);
    return;
  }
  firingNow.add(scheduleId);

  try {
    const now = new Date();

    let schedule = schedulePrefetchCache.get(scheduleId);
    if (schedule) {
      schedulePrefetchCache.delete(scheduleId);
      console.log(`[fire] ⚡ Schedule ${scheduleId} servido do pre-fetch cache`);
    } else {
      const { data, error } = await supabase
        .from("schedules")
        .select(SCHEDULE_SELECT)
        .eq("id", scheduleId)
        .eq("is_active", true)
        .single();

      if (error || !data) {
        console.warn(`[fire] Schedule ${scheduleId} não encontrado ou inativo.`);
        return;
      }
      schedule = data as unknown as Schedule;
    }

    const group = schedule.groups;

    if (!group?.telegram_chat_id) {
      console.warn(`[fire] Schedule ${scheduleId}: sem telegram_chat_id — pulando.`);
      return;
    }

    if (group.group_members) {
      group.group_members = group.group_members.map(m => ({
        ...m,
        accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
      }));
    }

    console.log(`[fire] ⚡ Disparando schedule ${scheduleId} às ${now.toISOString()}`);

    if (group.group_type === "open") {
      if (listenMap.has(group.id)) {
        console.log(`[fire] Listener já ativo para grupo ${group.id} — ignorando`);
        return;
      }

      const firstAccount = (group.group_members ?? [])
        .filter(m => m.is_active && m.accounts?.is_active)
        .sort((a, b) => a.position - b.position)[0]?.accounts ?? null;

      if (!firstAccount) {
        console.warn(`[fire] Nenhuma conta ativa no grupo — abortando.`);
        return;
      }

      startGroupListener(schedule, group, firstAccount as Account);

      await supabase.from("schedules").update({
        retry_until:         new Date(now.getTime() + OPEN_GROUP_LISTEN_TIMEOUT_MS).toISOString(),
        last_attempt_at:     now.toISOString(),
        last_attempt_status: "waiting_admin",
        last_attempt_error:  null,
      }).eq("id", scheduleId);

      supabase.from("groups")
        .update({ listener_session_id: `schedule-${scheduleId}-${Date.now()}` })
        .eq("id", group.id)
        .then(({ error: e }) => {
          if (e) console.warn(`[fire] Falha ao atualizar listener_session_id:`, e.message);
        });

      return;
    }

    // Grupo fechado fallback
    const alreadySent = schedule.retry_until
      ? await getAlreadySentIds(schedule)
      : new Set<string>();

    if (alreadySent.size > 0) {
      console.log(`[dedup] ${alreadySent.size} account(s) já enviaram neste ciclo — pulando.`);
    }

    const results = await dispatchToGroup(schedule, group, alreadySent);

    const sentForMonitor = results
      .filter(r => r.status === "sent")
      .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
      .filter(r => r.message_text);
    if (sentForMonitor.length > 0) {
      monitorPositions(group.telegram_chat_id, sentForMonitor, scheduleId, now, group.group_type ?? "closed")
        .catch(err => console.error("[monitor] Erro não capturado:", err.message));
    }

    await updateScheduleAfterDispatch(schedule, results, now, group.group_type);

  } finally {
    firingNow.delete(scheduleId);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   TIMER DE PRECISÃO + PRE-FETCH + SNIPER
   ───────────────────────────────────────────────────────────────────────────── */
function scheduleTimer(scheduleId: string, nextRunAt: string, groupType?: "open" | "closed"): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  if (delay < -5_000) {
    console.warn(`[timer] Schedule ${scheduleId} ignorado — muito no passado (${nextRunAt})`);
    return;
  }

  const prev = scheduledTimers.get(scheduleId);
  if (prev) clearTimeout(prev);
  const prevPrefetch = prefetchTimers.get(scheduleId);
  if (prevPrefetch) { clearTimeout(prevPrefetch); prefetchTimers.delete(scheduleId); }
  const prevSniper = sniperTimers.get(scheduleId);
  if (prevSniper) { clearTimeout(prevSniper); sniperTimers.delete(scheduleId); }

  const effectiveDelay = Math.max(0, delay);

  if (effectiveDelay > PREFETCH_BEFORE_MS) {
    const prefetchDelay = effectiveDelay - PREFETCH_BEFORE_MS;
    const pt = setTimeout(async () => {
      prefetchTimers.delete(scheduleId);
      try {
        const { data, error } = await supabase
          .from("schedules")
          .select(SCHEDULE_SELECT)
          .eq("id", scheduleId)
          .eq("is_active", true)
          .single();

        if (error || !data) {
          console.warn(`[prefetch] Schedule ${scheduleId} inativo ou removido — ignorando`);
          return;
        }

        const s = data as unknown as Schedule;
        if (s.groups?.group_members) {
          s.groups.group_members = s.groups.group_members.map(m => ({
            ...m,
            accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
          }));
        }

        schedulePrefetchCache.set(scheduleId, s);
        console.log(`[prefetch] ✅ Schedule ${scheduleId} pré-carregado (${PREFETCH_BEFORE_MS}ms antes do fire)`);
      } catch (err: any) {
        console.warn(`[prefetch] Falha ao pré-carregar schedule ${scheduleId}: ${err.message}`);
      }
    }, prefetchDelay);
    prefetchTimers.set(scheduleId, pt);
  }

  if (groupType === "closed" && effectiveDelay > SNIPER_BEFORE_MS) {
    const sniperDelay = effectiveDelay - SNIPER_BEFORE_MS;
    const st = setTimeout(async () => {
      sniperTimers.delete(scheduleId);
      const cached = schedulePrefetchCache.get(scheduleId);
      if (cached && cached.groups?.group_type !== "closed") {
        console.log(`[sniper] Schedule ${scheduleId} não é closed — pulando sniper`);
        return;
      }
      try {
        await sniperFireClosed(scheduleId);
      } catch (err) {
        console.error(`[sniper] Erro inesperado ao disparar ${scheduleId}:`, err);
      }
    }, sniperDelay);
    sniperTimers.set(scheduleId, st);
    console.log(`[sniper] ⏰ Sniper agendado para schedule ${scheduleId} em ${Math.round(sniperDelay / 1000)}s`);
  }

  const timer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    try {
      await fireSchedule(scheduleId);
    } catch (err) {
      console.error(`[timer] Erro inesperado ao disparar ${scheduleId}:`, err);
    }
  }, effectiveDelay);

  scheduledTimers.set(scheduleId, timer);

  const fireAt = new Date(Date.now() + effectiveDelay).toISOString();
  console.log(`[timer] ⏰ Schedule ${scheduleId} — dispara em ${Math.round(effectiveDelay / 1000)}s (${fireAt})`);
}

/* ─────────────────────────────────────────────────────────────────────────────
   RELOAD PERIÓDICO
   ───────────────────────────────────────────────────────────────────────────── */
async function reloadSchedules(): Promise<void> {
  const now          = new Date();
  const nowISO       = now.toISOString();
  const lookaheadISO = new Date(now.getTime() + LOOKAHEAD_MS).toISOString();

  const [
    { data: futureSchedules },
    { data: retrySchedules },
    { data: expiredRetries },
  ] = await Promise.all([
    supabase.from("schedules")
      .select("id, next_run_at, groups(group_type)")
      .eq("is_active", true)
      .is("retry_until", null)
      .lte("next_run_at", lookaheadISO),

    supabase.from("schedules")
      .select(SCHEDULE_SELECT)
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .gt("retry_until", nowISO),

    supabase.from("schedules")
      .select("id, cron_expression, group_id")
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .lte("retry_until", nowISO),
  ]);

  await Promise.all((expiredRetries ?? []).map(async expired => {
    console.warn(`[reload] Schedule ${expired.id}: retry expirou sem sucesso. Avançando.`);

    const expGroupId = (expired as any).group_id as string | undefined;
    if (expGroupId) {
      const ctrl = listenMap.get(expGroupId);
      if (ctrl) {
        ctrl.abort();
        listenMap.delete(expGroupId);
        console.log(`[reload] Listener cancelado para grupo ${expGroupId}`);
      }
    }

    let nextRun: string;
    try { nextRun = nextWeeklyOccurrence(expired.cron_expression); }
    catch {
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
  }));

  for (const s of futureSchedules ?? []) {
    if (!scheduledTimers.has(s.id)) {
      const gType = (s as any).groups?.group_type as "open" | "closed" | undefined;
      scheduleTimer(s.id, s.next_run_at, gType);
    }
  }

  for (const s of retrySchedules ?? []) {
    const schedule = s as unknown as Schedule;

    if (listenMap.has(schedule.group_id)) continue;

    if (
      isRetryDue(schedule, now) &&
      !scheduledTimers.has(schedule.id) &&
      !firingNow.has(schedule.id) &&
      !sniperFiringNow.has(schedule.id)
    ) {
      console.log(`[reload] Schedule ${schedule.id} em retry — disparando agora.`);
      fireSchedule(schedule.id).catch(err =>
        console.error(`[reload] Erro no retry do schedule ${schedule.id}:`, err)
      );
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   RECONEXÃO LEVE
   ───────────────────────────────────────────────────────────────────────────── */
async function reconnectDeadClients(): Promise<void> {
  const reconnectPromises: Promise<void>[] = [];

  for (const [accountId, client] of clients.entries()) {
    if (!client.connected) {
      const account = accountCache.get(accountId);
      if (!account) continue;
      console.warn(`[reconnect] ${account.phone_number} offline — reconectando`);
      reconnectPromises.push(
        getClient(account)
          .then(() => console.log(`[reconnect] ✓ ${account.phone_number} reconectado`))
          .catch(err => console.warn(`[reconnect] Falha ao reconectar ${account.phone_number}: ${err.message}`))
      );
    }
  }

  if (reconnectPromises.length > 0) {
    await Promise.allSettled(reconnectPromises);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   PRE-WARM DE CONTAS — roda apenas no boot
   ───────────────────────────────────────────────────────────────────────────── */
let prewarmRunning = false;
async function prewarmAccounts(): Promise<void> {
  if (prewarmRunning) return;
  prewarmRunning = true;
  try {
    const { data, error } = await supabase
      .from("accounts")
      .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("is_active", true);

    if (error) { console.warn("[prewarm] Falha ao buscar contas:", error.message); return; }

    const accounts = (data ?? []) as Account[];
    for (const account of accounts) accountCache.set(account.id, account);

    await Promise.allSettled(accounts.map(async account => {
      try {
        const client = await getClient(account);
        await client.getDialogs({ limit: 100 });
        console.log(`[prewarm] ✓ Dialogs prontos: ${account.phone_number}`);
      } catch (err: any) {
        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED") ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          console.warn(`[prewarm] Sessão morta: ${account.phone_number} — desativando.`);
          await supabase.from("accounts").update({ is_active: false }).eq("id", account.id);
        } else {
          console.warn(`[prewarm] Falha ao conectar ${account.phone_number}: ${err.message}`);
        }
      }
    }));

    try {
      const { data: groups } = await supabase
        .from("groups")
        .select("telegram_chat_id, group_members(accounts(id))")
        .not("telegram_chat_id", "is", null)
        .eq("group_members.is_active", true);

      const resolvePromises: Promise<unknown>[] = [];
      for (const group of groups ?? []) {
        if (!group.telegram_chat_id) continue;
        const chatId = String(group.telegram_chat_id);

        for (const member of (group as any).group_members ?? []) {
          const accountId = member?.accounts?.id ?? member?.accounts?.[0]?.id;
          if (!accountId) continue;
          const acc = accountCache.get(accountId);
          if (!acc) continue;
          const cl = clients.get(acc.id);
          if (!cl?.connected) continue;

          resolvePromises.push(
            resolvePeer(cl, chatId, acc.id)
              .then(() => console.log(`[prewarm] ✓ Peer pré-resolvido: ${chatId} via ${acc.phone_number}`))
              .catch(() => {})
          );
        }
      }

      await Promise.allSettled(resolvePromises);
      console.log(`[prewarm] ✓ Pre-resolve de peers concluído (${resolvePromises.length} entradas)`);
    } catch (err: any) {
      console.warn(`[prewarm] Falha no pre-resolve de peers: ${err.message}`);
    }

  } finally {
    prewarmRunning = false;
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   HTTP SERVER
   ───────────────────────────────────────────────────────────────────────────── */
function jsonResponse(res: http.ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

const httpServer = http.createServer(async (req, res) => {
  if (WORKER_SECRET && req.headers["x-worker-secret"] !== WORKER_SECRET) {
    return jsonResponse(res, 401, { error: "Unauthorized" });
  }

  const url = new URL(req.url ?? "/", `http://localhost:${WORKER_PORT}`);

  const chatsMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chats$/);
  if (req.method === "GET" && chatsMatch) {
    const account = accountCache.get(chatsMatch[1]);
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada no cache" });
    try {
      const client  = await getClient(account);
      const dialogs = await client.getDialogs({ limit: 200 });
      const chats   = dialogs
        .filter(d => d.isGroup || d.isChannel)
        .map(d => ({
          id:         String(d.id),
          name:       d.title ?? d.name ?? "Sem nome",
          type:       d.isChannel ? "channel" : "group",
          accessHash: null,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));
      return jsonResponse(res, 200, chats);
    } catch (err: any) {
      console.error("[http] /chats erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const chatCountMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-count$/);
  if (req.method === "GET" && chatCountMatch) {
    const chatId  = url.searchParams.get("chat_id");
    const account = accountCache.get(chatCountMatch[1]);
    if (!chatId)  return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada no cache" });

    try {
      const client = await getClient(account);
      const rawId  = chatId.replace(/^-100/, "").replace(/^-/, "");
      let count: number | null = null;

      try {
        const result = await client.invoke(
          new Api.channels.GetFullChannel({
            channel: new Api.InputChannel({ channelId: bigInt(rawId), accessHash: bigInt(0) }),
          })
        ) as any;
        if (typeof result?.fullChat?.participantsCount === "number") {
          count = result.fullChat.participantsCount;
        }
      } catch {}

      if (count === null) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const absRaw  = rawId.replace(/^100/, "");
          const dialog  = dialogs.find(d => {
            const s = String(d.id).replace(/^-/, "");
            return s === rawId || s === absRaw ||
                   String(d.id) === chatId ||
                   `-100${s}` === chatId ||
                   `-${s}` === chatId;
          });
          if (dialog?.entity) {
            const ent = dialog.entity as any;
            count = typeof ent.participantsCount === "number" ? ent.participantsCount : null;
          }
          if (count === null && dialog) {
            const p = (dialog as any).participantsCount;
            if (typeof p === "number") count = p;
          }
        } catch {}
      }

      if (count === null) {
        try {
          const full = await client.invoke(
            new Api.messages.GetFullChat({ chatId: bigInt(rawId.replace(/^100/, "")) })
          ) as any;
          if (typeof full?.fullChat?.participantsCount === "number") {
            count = full.fullChat.participantsCount;
          } else if (full?.fullChat?.participants?.participants) {
            count = full.fullChat.participants.participants.length;
          }
        } catch {}
      }

      return jsonResponse(res, 200, { count });
    } catch (err: any) {
      console.error("[http] /chat-count erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const membersMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-members$/);
  if (req.method === "GET" && membersMatch) {
    const chatId  = url.searchParams.get("chat_id");
    const account = accountCache.get(membersMatch[1]);
    if (!chatId)  return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada no cache" });

    type MemberOut = { id: string; name: string | null; username: string | null; phone: string | null };

    try {
      const client       = await getClient(account);
      const rawId        = chatId.replace(/^-/, "");
      const isSupergroup = chatId.startsWith("-100");
      let members: MemberOut[] = [];

      if (isSupergroup) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const dialog  = dialogs.find(d => {
            const dId = String(d.id);
            return dId === rawId || dId === chatId || dId === rawId.replace(/^100/, "");
          });
          const entity = dialog?.entity;
          if (entity && (entity.className === "Channel" || entity.className === "Chat")) {
            const result = await client.invoke(
              new Api.channels.GetParticipants({
                channel: entity as Api.Channel,
                filter:  new Api.ChannelParticipantsRecent(),
                offset: 0, limit: 200, hash: bigInt(0),
              })
            );
            if (result.className === "channels.ChannelParticipants") {
              members = result.users
                .filter((u): u is Api.User => u.className === "User" && !u.bot)
                .map(u => ({
                  id:       String(u.id),
                  name:     [u.firstName, u.lastName].filter(Boolean).join(" ") || null,
                  username: u.username ? `@${u.username}` : null,
                  phone:    u.phone ? `+${u.phone}` : null,
                }));
            }
          }
        } catch {}
      }

      if (members.length === 0) {
        try {
          const full     = await client.invoke(new Api.messages.GetFullChat({ chatId: bigInt(rawId) }));
          const chatFull = full.fullChat as Api.ChatFull;
          const parts    = chatFull.participants;
          if (parts && parts.className === "ChatParticipants") {
            const userMap = new Map<string, Api.User>();
            for (const u of full.users) {
              if (u.className === "User") userMap.set(String(u.id), u as Api.User);
            }
            members = parts.participants
              .map(p => {
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
        } catch {}
      }

      members.sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));
      return jsonResponse(res, 200, members);
    } catch (err: any) {
      console.error("[http] /chat-members erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const reloadMatch = url.pathname.match(/^\/accounts\/([^/]+)\/reload$/);
  if (req.method === "POST" && reloadMatch) {
    const accountId = reloadMatch[1];
    const { data: row, error } = await supabase
      .from("accounts")
      .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("id", accountId)
      .single();

    if (error || !row) return jsonResponse(res, 404, { error: "Conta não encontrada" });

    const account = row as Account;
    accountCache.set(accountId, account);

    if (!account.is_active || !account.session_string) {
      return jsonResponse(res, 200, { ok: true, skipped: true, reason: "conta inativa ou sem sessão" });
    }

    try {
      const client = await reloadClient(account);
      await client.getDialogs({ limit: 100 });
      console.log(`[http] /reload ✓ ${account.phone_number} recarregada`);
      return jsonResponse(res, 200, { ok: true });
    } catch (err: any) {
      console.error("[http] /reload erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const listenMatch = url.pathname.match(/^\/groups\/([^/]+)\/listen$/);
  if (listenMatch) {
    const groupId = listenMatch[1];

    if (req.method === "DELETE") {
      const ctrl = listenMap.get(groupId);
      if (ctrl) { ctrl.abort(); listenMap.delete(groupId); }
      return jsonResponse(res, 200, { ok: true });
    }

    if (req.method === "POST") {
      const existing = listenMap.get(groupId);
      if (existing) existing.abort();

      const ctrl = new AbortController();
      listenMap.set(groupId, ctrl);

      (async () => {
        try {
          const { data: grpRow } = await supabase
            .from("groups")
            .select(`
              id, telegram_chat_id, group_type,
              group_members(id, message_text, position, is_active,
                accounts(id, name, phone_number, api_id, api_hash, session_string, is_active))
            `)
            .eq("id", groupId)
            .single();

          if (!grpRow) { console.warn(`[listen-manual] Grupo ${groupId} não encontrado`); return; }

          const chatId  = String(grpRow.telegram_chat_id);
          const chatNum = parseInt(chatId, 10);
          const members: GroupMember[] = (grpRow.group_members ?? []).map((m: any) => ({
            ...m,
            accounts: Array.isArray(m.accounts) ? (m.accounts[0] ?? null) : (m.accounts ?? null),
          }));

          const firstMember = members.find(m => m.is_active && m.accounts?.is_active);
          if (!firstMember?.accounts) {
            console.warn(`[listen-manual] Sem conta ativa em ${groupId}`);
            return;
          }

          const account = accountCache.get(firstMember.accounts.id) ?? firstMember.accounts as unknown as Account;
          let detectClient = await getClient(account);

          // Pre-aquece todas as contas
          const warmClients = new Map<string, TelegramClient>();
          const warmPeers   = new Map<string, unknown>();
          Promise.allSettled(members.map(async m => {
            if (!m.accounts?.is_active) return;
            const acc = accountCache.get(m.accounts.id) ?? m.accounts as unknown as Account;
            try {
              const cl = await getClient(acc);
              warmClients.set(acc.id, cl);
              const peer = await resolvePeer(cl, chatId, acc.id);
              warmPeers.set(acc.id, peer);
            } catch {}
          })).catch(() => {});

          const startUnix   = Math.floor(Date.now() / 1000) - 5;
          const deadline    = Date.now() + 2 * 60 * 60_000;
          let lastSeenMsgId = 0;
          let fired         = false;

          try { await resolvePeer(detectClient, chatId, account.id); } catch {}
          console.log(`[listen-manual] 👂 Aguardando OK em ${chatId} (grupo ${groupId})`);

          const doFire = async (source: string) => {
            if (fired || ctrl.signal.aborted) return;
            fired = true;
            listenMap.delete(groupId);
            await supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId);

            const { data: grpFull } = await supabase
              .from("groups")
              .select("name, telegram_chat_name, user_id")
              .eq("id", groupId)
              .single();

            const scheduleStub = {
              id:                         `manual-${groupId}-${Date.now()}`,
              user_id:                    grpFull?.user_id ?? "",
              group_id:                   groupId,
              cron_expression:            "0 0 * * 0",
              next_run_at:                new Date().toISOString(),
              retry_window_seconds:       60,
              retry_interval_seconds:     5,
              retry_interval_max_seconds: 30,
              retry_count:                0,
              retry_until:                null,
              last_attempt_at:            null,
              groups: {
                id:                 groupId,
                name:               grpFull?.name ?? groupId,
                telegram_chat_id:   chatId,
                telegram_chat_name: grpFull?.telegram_chat_name ?? null,
                group_type:         "open" as const,
                group_members:      members,
              },
            };

            const dispatchedAt = new Date();
            const results      = await dispatchOpenGroup(scheduleStub as any, scheduleStub.groups, warmClients, warmPeers);

            const sentForMonitor = results
              .filter(r => r.status === "sent")
              .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
              .filter(r => r.message_text);
            if (sentForMonitor.length > 0) {
              monitorPositions(chatId, sentForMonitor, scheduleStub.id, dispatchedAt, "open").catch(() => {});
            }

            const sent = results.filter(r => r.status === "sent").length;
            console.log(`[listen-manual] ✓ ${sent} mensagem(ns) enviada(s) via ${source} para grupo ${groupId}`);
          };

          // Event handler
          const handler = async (event: NewMessageEvent) => {
            if (fired || ctrl.signal.aborted) return;
            const msg = event.message;
            if (!msg || (msg as any).date < startUnix) return;
            const msgChatId = msg.chatId?.toJSNumber?.() ?? msg.peerId?.channelId?.toJSNumber?.() ?? 0;
            const absChat   = Math.abs(chatNum);
            const absMsg    = Math.abs(msgChatId);
            if (absMsg !== absChat && absMsg !== absChat - 1_000_000_000_000) return;
            await doFire("event");
          };

          detectClient.addEventHandler(handler, new NewMessage({}));

          // Fallback poll
          while (Date.now() < deadline && !ctrl.signal.aborted && !fired) {
            try {
              if (!detectClient.connected) {
                detectClient = await getClient(account);
                detectClient.addEventHandler(handler, new NewMessage({}));
                try { await resolvePeer(detectClient, chatId, account.id); } catch {}
              }

              const peer   = await resolvePeer(detectClient, chatId, account.id);
              const result = await detectClient.invoke(
                new Api.messages.GetHistory({
                  peer: peer as any, limit: 5,
                  offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
                  hash: bigInt(0), addOffset: 0,
                })
              ) as any;

              const recentMsgs = (result.messages ?? []).filter(
                (m: any) => m.date >= startUnix && m.id > lastSeenMsgId
              );
              if (recentMsgs.length > 0) {
                lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
              }

              if (!fired && recentMsgs.some((m: any) => hasAnyContent(m))) {
                await doFire("fallback-poll");
                break;
              }
            } catch (err: any) {
              if (!ctrl.signal.aborted && !fired) await new Promise(r => setTimeout(r, 2_000));
            }

            if (!fired && !ctrl.signal.aborted) {
              await new Promise(r => setTimeout(r, OPEN_FALLBACK_POLL_MS));
            }
          }

          try { detectClient.removeEventHandler(handler, new NewMessage({})); } catch {}

          if (!ctrl.signal.aborted) {
            await supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId);
          }
          listenMap.delete(groupId);

        } catch (err: any) {
          console.error(`[listen-manual] Erro inesperado para grupo ${groupId}:`, err.message);
          listenMap.delete(groupId);
        }
      })();

      return jsonResponse(res, 200, { ok: true });
    }
  }

  const dispatchMatch = url.pathname.match(/^\/groups\/([^/]+)\/dispatch$/);
  if (req.method === "POST" && dispatchMatch) {
    const groupId = dispatchMatch[1];

    let body: { user_id?: string; send_to_self?: boolean } = {};
    try {
      const raw = await new Promise<string>((resolve, reject) => {
        let data = "";
        req.on("data", chunk => { data += chunk; });
        req.on("end",  () => resolve(data));
        req.on("error", reject);
      });
      if (raw) body = JSON.parse(raw);
    } catch {}

    const sendToSelf = !!body.send_to_self;

    try {
      const { data, error } = await supabase
        .from("groups")
        .select(`
          id, name, telegram_chat_id, telegram_chat_name, group_type,
          group_members(
            id, message_text, position, is_active,
            accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)
          )
        `)
        .eq("id", groupId)
        .single();

      if (error || !data) {
        return jsonResponse(res, 404, { error: "Grupo não encontrado" });
      }

      const group = data as unknown as Group;
      const members = (group.group_members ?? [])
        .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
        .sort((a, b) => a.position - b.position);

      if (members.length === 0) {
        return jsonResponse(res, 200, { ok: true, sent: 0, failed: 0, results: [] });
      }

      let sent = 0, failed = 0;
      const results: Array<{ account_id: string; status: string; error?: string }> = [];

      for (const member of members) {
        const account = member.accounts
          ? (accountCache.get(member.accounts.id) ?? member.accounts as unknown as Account)
          : null;
        if (!account) continue;

        try {
          const client = await getClient(account);
          const text = member.message_text ?? "";

          if (sendToSelf) {
            const stableId = makeRandomId();
            await Promise.race([
              client.invoke(new Api.messages.SendMessage({
                peer:      new Api.InputPeerSelf(),
                message:   text || "[teste de aquecimento]",
                randomId:  stableId,
                noWebpage: true,
              })),
              new Promise<never>((_, r) => setTimeout(() => r(new Error("TIMEOUT")), SEND_TIMEOUT_MS)),
            ]);
          } else {
            if (!group.telegram_chat_id) throw new Error("telegram_chat_id não configurado");
            await sendMessage(client, account, String(group.telegram_chat_id), text);
          }

          sent++;
          results.push({ account_id: account.id, status: "sent" });
          console.log(`[dispatch-http] ✓ ${account.phone_number}${sendToSelf ? " (self)" : ""}`);
        } catch (err: any) {
          failed++;
          results.push({ account_id: account.id, status: "failed", error: err.message });
          console.error(`[dispatch-http] ✗ ${account?.phone_number}: ${err.message}`);
        }
      }

      return jsonResponse(res, 200, { ok: true, sent, failed, results });
    } catch (err: any) {
      console.error("[dispatch-http] Erro inesperado:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  jsonResponse(res, 404, { error: "Not found" });
});

httpServer.listen(WORKER_PORT, () => {
  console.log(`[worker] HTTP interno escutando na porta ${WORKER_PORT}`);
});

/* ─────────────────────────────────────────────────────────────────────────────
   GRACEFUL SHUTDOWN
   ───────────────────────────────────────────────────────────────────────────── */
async function shutdown() {
  console.log("[worker] Encerrando...");

  for (const t of prefetchTimers.values()) clearTimeout(t);
  prefetchTimers.clear();

  for (const t of sniperTimers.values()) clearTimeout(t);
  sniperTimers.clear();

  for (const t of scheduledTimers.values()) clearTimeout(t);
  scheduledTimers.clear();

  for (const t of keepaliveTimers.values()) clearInterval(t);
  keepaliveTimers.clear();

  httpServer.close();

  await Promise.all([...clients.entries()].map(async ([id, client]) => {
    try { await client.disconnect(); } catch {}
    console.log(`[client] Desconectado: ${id}`);
  }));
  clients.clear();

  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);

/* ─────────────────────────────────────────────────────────────────────────────
   INICIALIZAÇÃO
   ───────────────────────────────────────────────────────────────────────────── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando...");
  await prewarmAccounts();
  await reloadSchedules();
  setInterval(async () => {
    try {
      await Promise.allSettled([
        reloadSchedules(),
        reconnectDeadClients(),
      ]);
    } catch (err) {
      console.error("[reload] Erro no reload periódico:", err);
    }
  }, RELOAD_INTERVAL_MS);
  console.log("[worker] Pronto.");
}

init().catch(err => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});
