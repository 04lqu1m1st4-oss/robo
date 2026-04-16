// worker.ts — precision Telegram dispatch worker
//
// Garantias:
//   ✓ Pre-warm progressivo: 60s, 30s, 10s, 5s, 3s, 2s, 1s antes do disparo
//   ✓ Retry infinito até 60s após o horário original — depois descarta
//   ✓ Zero duplicatas via firingLocks + sentSet por (scheduleId, memberId)
//   ✓ Disparo paralelo por membro (contas distintas, mesmo ou múltiplos grupos)
//   ✓ Nenhum erro é considerado permanente dentro da janela de 60s
//   ✓ Supabase Realtime para detectar schedules novos/alterados instantaneamente
//   ✓ isHealthy robusto: detecta reconnect interno do GramJS e evicta corretamente
//   ✓ [FIX] Logger.setLevel("error") — suprime logs internos de INFO/WARN do GramJS
//   ✓ [FIX] pingInterval: 60_000 — keepalive para reduzir drops de conexão ociosa
//   ✓ [FIX] Aguarda 2s antes de evictar — evita race condition com reconnect interno

import { createClient } from "@supabase/supabase-js";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions";
import { Logger } from "telegram/extensions";

/* ─── [FIX] Suprime logs internos de INFO/WARN do GramJS ─── */
/* Elimina os logs de "Connection closed while receiving data",  */
/* "Started reconnecting", "Connecting to ...", etc.             */
Logger.setLevel("error");

/* ─── Supabase ─── */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─── Constantes ─── */
const SEND_TIMEOUT_MS  = 10_000;
const RETRY_BASE_MS    = 300;
const RETRY_MAX_MS     = 3_000;
const DISCARD_AFTER_MS = 60_000;
const LOOKAHEAD_MS     = 10 * 60 * 1000;

const PREWARM_STEPS_MS = [60_000, 30_000, 10_000, 5_000, 3_000, 2_000, 1_000];

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
  group_members: GroupMember[];
}
interface Schedule {
  id: string;
  cron_expression: string;
  user_id: string;
  group_id: string;
  next_run_at: string;
  groups: Group;
}

/* ─── Peer cache ─── */
const peerCache = new Map<string, unknown>();

async function getOrResolvePeer(
  client: TelegramClient,
  telegramChatId: string
): Promise<unknown> {
  if (peerCache.has(telegramChatId)) return peerCache.get(telegramChatId)!;
  const chatIdNum = parseInt(telegramChatId, 10);
  const peer = await client.getInputEntity(
    isNaN(chatIdNum) ? telegramChatId : chatIdNum
  );
  peerCache.set(telegramChatId, peer);
  console.log(`[peer] Resolvido e cacheado: ${telegramChatId}`);
  return peer;
}

/* ─── Pool de conexões Telegram ─── */
class TelegramClientPool {
  private clients    = new Map<string, TelegramClient>();
  private sessions   = new Map<string, string>();
  private connecting = new Map<string, Promise<TelegramClient>>();

  /**
   * Verifica saúde real do client.
   *
   * Problema anterior: durante o reconnect interno do GramJS,
   * `client.connected` retorna `true` mas o sender está em estado
   * de reconexão — causando envios para um client morto.
   *
   * Solução: inspecionamos o estado interno do sender de forma
   * defensiva e consideramos não-saudável qualquer estado ambíguo.
   */
  private isHealthy(client: TelegramClient): boolean {
    // 1. Verificação básica da prop pública
    if (!client.connected) return false;

    try {
      const sender = (client as any)._sender;

      // Sem sender → morto
      if (!sender) return false;

      // Reconectando ativamente → não usar agora
      if (sender._reconnecting === true) return false;

      // Explicitamente desconectado
      if (sender._isConnected === false) return false;
      if (sender._userDisconnected === true) return false;

      // Se há uma promessa de reconexão pendente → instável
      if (sender._reconnect instanceof Promise) return false;

      // Verifica se o loop de receive ainda está rodando
      // GramJS seta _recvLoopRunning = false quando a conexão cai
      if (sender._recvLoopRunning === false) return false;

      // Verifica se o transporte TCP está conectado
      const connection = sender._connection;
      if (connection) {
        if (connection._connected === false) return false;
        if (connection._socket === null || connection._socket === undefined) return false;
        // Checa readyState do socket (0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED)
        const socket = connection._socket;
        if (typeof socket.readyState === "number" && socket.readyState !== 1) return false;
      }
    } catch {
      // Qualquer erro de introspecção → considera não-saudável
      return false;
    }

    return true;
  }

  evict(accountId: string): void {
    const existing = this.clients.get(accountId);
    if (existing) {
      try { existing.disconnect(); } catch {}
      this.clients.delete(accountId);
      this.sessions.delete(accountId);
      console.log(`[pool] Evicted: ${accountId}`);
    }
  }

  /**
   * [FIX] Aguarda o reconnect interno do GramJS terminar antes de evictar.
   * Evita race condition onde evictamos um client que já estava se recuperando
   * sozinho, gerando duas conexões simultâneas.
   */
  async evictIfStillUnhealthy(accountId: string, phone: string): Promise<void> {
    const client = this.clients.get(accountId);
    if (!client) return;

    console.log(`[pool] Aguardando 2s para ver se ${phone} se recupera sozinho...`);
    await sleep(2_000);

    if (!this.isHealthy(client)) {
      this.evict(accountId);
      console.warn(`[pool] Evictado ${phone} após aguardar reconnect interno.`);
    } else {
      console.log(`[pool] Client ${phone} se recuperou sozinho — mantendo conexão.`);
    }
  }

  async get(account: Account): Promise<TelegramClient> {
    const existing     = this.clients.get(account.id);
    const sessionInUse = this.sessions.get(account.id);

    if (existing && sessionInUse === account.session_string && this.isHealthy(existing)) {
      return existing;
    }

    // Evicta se sessão mudou, morreu, ou está em estado ambíguo de reconexão
    if (existing) {
      try { await existing.disconnect(); } catch {}
      this.clients.delete(account.id);
      this.sessions.delete(account.id);
      console.log(`[pool] Client evictado por isHealthy=false: ${account.phone_number}`);
    }

    // Coalescing: evita múltiplas conexões simultâneas para a mesma conta
    const inFlight = this.connecting.get(account.id);
    if (inFlight) return inFlight;

    const promise = (async (): Promise<TelegramClient> => {
      const client = new TelegramClient(
        new StringSession(account.session_string),
        parseInt(account.api_id),
        account.api_hash,
        {
          connectionRetries: 5,
          retryDelay: 1_000,
          // [FIX] Keepalive: envia ping a cada 60s para evitar que o Telegram
          // feche conexões ociosas (~90s timeout), reduzindo drasticamente
          // a frequência dos logs de "Connection closed / reconnecting".
          pingInterval: 60_000,
        }
      );

      try {
        await client.connect();
      } catch (err: any) {
        const msg = String(err?.message ?? err);
        if (msg.includes("AUTH_KEY_DUPLICATED")) {
          console.warn(`[pool] AUTH_KEY_DUPLICATED para ${account.phone_number} — aguardando 4s...`);
          try { await client.disconnect(); } catch {}
          await sleep(4_000);
          await client.connect();
        } else {
          throw err;
        }
      }

      this.clients.set(account.id, client);
      this.sessions.set(account.id, account.session_string);
      console.log(`[pool] Conectado: ${account.phone_number}`);
      return client;
    })();

    this.connecting.set(account.id, promise);
    try {
      return await promise;
    } finally {
      this.connecting.delete(account.id);
    }
  }

  async ensureConnected(account: Account): Promise<void> {
    try {
      await this.get(account);
    } catch (err: any) {
      console.warn(`[pool] Pre-warm falhou para ${account.phone_number}: ${err?.message ?? err}`);
    }
  }

  async disconnectAll(): Promise<void> {
    this.connecting.clear();
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

/* ─── Helpers ─── */
function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function nextWeeklyOccurrence(cronExpression: string): string {
  const parts = cronExpression.trim().split(/\s+/);
  const mi  = parseInt(parts[0], 10);
  const h   = parseInt(parts[1], 10);
  const dow = parseInt(parts[4], 10);

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

/* ─── Estado global de timers ─── */
const scheduledTimers = new Map<string, ReturnType<typeof setTimeout>[]>();
const firingLocks     = new Map<string, number>();
const sentSet         = new Set<string>();

/* ─── Busca schedule completo do banco ─── */
async function fetchSchedule(scheduleId: string): Promise<Schedule | null> {
  const { data, error } = await supabase
    .from("schedules")
    .select(`
      id, cron_expression, user_id, group_id, next_run_at,
      groups(id, telegram_chat_id,
        group_members(id, message_text, position, is_active,
          accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))
    `)
    .eq("id", scheduleId)
    .eq("is_active", true)
    .single();

  if (error || !data) return null;
  return data as unknown as Schedule;
}

/* ─── Envia mensagem com timeout por tentativa ─── */
async function sendMessage(
  client: TelegramClient,
  telegramChatId: string,
  messageText: string
): Promise<void> {
  const timeout = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error("TIMEOUT")), SEND_TIMEOUT_MS)
  );

  const send = (async () => {
    let peer: unknown;
    try {
      peer = await getOrResolvePeer(client, telegramChatId);
    } catch {
      peerCache.delete(telegramChatId);
      const chatIdNum = parseInt(telegramChatId, 10);
      peer = await client.getInputEntity(isNaN(chatIdNum) ? telegramChatId : chatIdNum);
      peerCache.set(telegramChatId, peer);
    }

    try {
      await client.sendMessage(peer as any, { message: messageText });
    } catch (err: any) {
      const msg = String(err?.message ?? "");

      if (msg.includes("PEER_ID_INVALID")) {
        peerCache.delete(telegramChatId);
        const chatIdNum = parseInt(telegramChatId, 10);
        const freshPeer = await client.getInputEntity(
          isNaN(chatIdNum) ? telegramChatId : chatIdNum
        );
        peerCache.set(telegramChatId, freshPeer);
        await client.sendMessage(freshPeer as any, { message: messageText });
        return;
      }

      const isFlood =
        err?.seconds != null ||
        err?.constructor?.name === "FloodWaitError" ||
        /flood/i.test(msg);

      if (isFlood) {
        const waitSecs: number = err.seconds ?? 30;
        console.warn(`[send] FloodWait ${waitSecs}s`);
        await sleep(waitSecs * 1_000);
        const freshPeer = await getOrResolvePeer(client, telegramChatId);
        await client.sendMessage(freshPeer as any, { message: messageText });
        return;
      }

      throw err;
    }
  })();

  await Promise.race([send, timeout]);
}

/* ─── Pre-warm de contas de um schedule ─── */
async function prewarmScheduleAccounts(scheduleId: string): Promise<void> {
  const schedule = await fetchSchedule(scheduleId);
  if (!schedule) return;

  const members = (schedule.groups?.group_members ?? []).filter(
    (m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string
  );

  if (members.length === 0) return;

  console.log(
    `[prewarm] Schedule ${scheduleId}: conectando ${members.length} conta(s)...`
  );

  await Promise.allSettled(
    members.map((m) => clientPool.ensureConnected(m.accounts!))
  );
}

/* ─── Cancela todos os timers de um schedule ─── */
function cancelTimers(scheduleId: string): void {
  const timers = scheduledTimers.get(scheduleId) ?? [];
  for (const t of timers) clearTimeout(t);
  if (timers.length > 0) {
    scheduledTimers.delete(scheduleId);
    console.log(`[timer] ✖ Timers cancelados: ${scheduleId} (${timers.length})`);
  }
}

/* ─── Agenda todos os timers para um schedule (prewarm + disparo) ─── */
function scheduleAll(scheduleId: string, nextRunAt: string): void {
  const fireAt  = new Date(nextRunAt).getTime();
  const now     = Date.now();
  const delay   = fireAt - now;

  if (delay < -DISCARD_AFTER_MS) {
    console.warn(
      `[timer] Schedule ${scheduleId} passou ${Math.round(-delay / 1000)}s atrás — ignorando.`
    );
    return;
  }

  if (scheduledTimers.has(scheduleId)) return;

  const timers: ReturnType<typeof setTimeout>[] = [];

  for (const stepMs of PREWARM_STEPS_MS) {
    const stepDelay = delay - stepMs;
    if (stepDelay <= 0) continue;

    const t = setTimeout(async () => {
      console.log(
        `[prewarm] ⚡ ${stepMs / 1000}s antes do disparo — schedule ${scheduleId}`
      );
      await prewarmScheduleAccounts(scheduleId);
    }, stepDelay);

    timers.push(t);
  }

  const effectiveDelay = Math.max(0, delay);
  const fireTimer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    await fireSchedule(scheduleId, fireAt);
  }, effectiveDelay);
  timers.push(fireTimer);

  scheduledTimers.set(scheduleId, timers);

  const fireISO = new Date(fireAt).toISOString();
  console.log(
    `[timer] ⏰ Schedule ${scheduleId} — dispara em ${Math.round(effectiveDelay / 1000)}s` +
    ` (${fireISO}) | ${timers.length - 1} prewarm step(s) agendado(s)`
  );
}

/* ─── Dispara um schedule ─── */
async function fireSchedule(scheduleId: string, scheduledFireAt: number): Promise<void> {
  if (firingLocks.has(scheduleId)) {
    console.warn(`[fire] Schedule ${scheduleId} já está sendo processado — ignorando.`);
    return;
  }
  firingLocks.set(scheduleId, scheduledFireAt);

  console.log(`[fire] ⚡ Iniciando schedule ${scheduleId}`);

  try {
    const schedule = await fetchSchedule(scheduleId);
    if (!schedule) {
      console.warn(`[fire] Schedule ${scheduleId} não encontrado ou inativo.`);
      return;
    }

    const group = schedule.groups;
    if (!group?.telegram_chat_id) {
      console.warn(`[fire] Schedule ${scheduleId}: sem telegram_chat_id.`);
      return;
    }

    const members = (group.group_members ?? [])
      .filter((m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
      .sort((a, b) => a.position - b.position);

    if (members.length === 0) {
      console.warn(`[fire] Schedule ${scheduleId}: nenhum membro ativo.`);
      return;
    }

    const results = await Promise.allSettled(
      members.map((member) =>
        sendMemberWithRetry(member, group, schedule, scheduledFireAt)
      )
    );

    const anySuccess = results.some((r) => r.status === "fulfilled");
    const allSuccess = results.every((r) => r.status === "fulfilled");

    if (!allSuccess) {
      const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
      console.warn(
        `[fire] Schedule ${scheduleId}: ${failures.length} membro(s) descartado(s) após esgotar janela.`
      );
    }

    const nextRun = nextWeeklyOccurrence(schedule.cron_expression);

    await supabase.from("schedules").update({
      next_run_at:         nextRun,
      last_run_at:         new Date().toISOString(),
      retry_until:         null,
      retry_count:         0,
      last_attempt_at:     new Date().toISOString(),
      last_attempt_status: anySuccess ? (allSuccess ? "sent" : "partial") : "failed",
      last_attempt_error:  anySuccess ? null : "Janela de 60s esgotada sem envio bem-sucedido.",
    }).eq("id", scheduleId);

    console.log(
      anySuccess
        ? `[fire] ✓ Schedule ${scheduleId} completo. Próxima: ${nextRun}`
        : `[fire] ✗ Schedule ${scheduleId}: nenhum membro enviou dentro da janela.`
    );

    scheduleAll(scheduleId, nextRun);

  } finally {
    firingLocks.delete(scheduleId);
    for (const key of [...sentSet]) {
      if (key.startsWith(`${scheduleId}:`)) sentSet.delete(key);
    }
  }
}

/* ─── Envia mensagem de um membro com retry limitado à janela de 60s ─── */
async function sendMemberWithRetry(
  member: GroupMember,
  group: Group,
  schedule: Schedule,
  scheduledFireAt: number
): Promise<void> {
  const account = member.accounts!;
  const sentKey = `${schedule.id}:${member.id}`;
  let attempt   = 0;

  if (sentSet.has(sentKey)) {
    console.warn(`[send] Membro ${member.id} já enviado neste ciclo — ignorando.`);
    return;
  }

  while (true) {
    attempt++;

    const elapsed = Date.now() - scheduledFireAt;
    if (elapsed > DISCARD_AFTER_MS) {
      const msg = `Janela de ${DISCARD_AFTER_MS / 1000}s esgotada após ${attempt - 1} tentativa(s).`;
      console.warn(`[send] ✗ Descartando membro ${member.id} (${account.phone_number}): ${msg}`);

      supabase.from("dispatch_logs").insert({
        user_id:       schedule.user_id,
        group_id:      group.id,
        account_id:    account.id,
        schedule_id:   schedule.id,
        status:        "discarded",
        message_text:  member.message_text,
        sent_at:       null,
        error_message: msg,
      }).then(() => {});

      throw new Error(msg);
    }

    try {
      // Sempre pede o client ao pool — se não estiver saudável, ele reconecta
      const client = await clientPool.get(account);
      await sendMessage(client, group.telegram_chat_id!, member.message_text ?? "");

      sentSet.add(sentKey);
      console.log(
        `[send] ✓ Membro ${member.id} (${account.phone_number}) — tentativa ${attempt} ` +
        `(+${Date.now() - scheduledFireAt}ms)`
      );

      supabase.from("dispatch_logs").insert({
        user_id:       schedule.user_id,
        group_id:      group.id,
        account_id:    account.id,
        schedule_id:   schedule.id,
        status:        "sent",
        message_text:  member.message_text,
        sent_at:       new Date().toISOString(),
        error_message: null,
      }).then(() => {});

      return;

    } catch (err: any) {
      if (err?.message?.includes("esgotada")) throw err;

      const errorMsg = err instanceof Error ? err.message : String(err);

      // Qualquer erro de conexão/transporte → aguarda possível reconnect
      // interno do GramJS antes de evictar, para evitar race condition
      const isConnError =
        /not connected/i.test(errorMsg)         ||
        /connection closed/i.test(errorMsg)     ||
        /Cannot read properties/i.test(errorMsg)||
        /BinaryReader/i.test(errorMsg)          ||
        /TIMEOUT/.test(errorMsg)               ||
        /reconnect/i.test(errorMsg);

      if (isConnError) {
        // [FIX] Aguarda 2s para ver se o GramJS se recupera sozinho antes
        // de evictar. Evita criar uma nova conexão desnecessária enquanto
        // o reconnect interno já estava em andamento.
        await clientPool.evictIfStillUnhealthy(account.id, account.phone_number);
      }

      const backoffMs = Math.min(RETRY_BASE_MS * Math.pow(2, attempt - 1), RETRY_MAX_MS);
      console.warn(
        `[send] ✗ Membro ${member.id} (${account.phone_number}) tentativa ${attempt} ` +
        `— retry em ${backoffMs}ms: ${errorMsg}`
      );

      supabase.from("dispatch_logs").insert({
        user_id:       schedule.user_id,
        group_id:      group.id,
        account_id:    account.id,
        schedule_id:   schedule.id,
        status:        "failed",
        message_text:  member.message_text,
        sent_at:       null,
        error_message: errorMsg,
      }).then(() => {});

      await sleep(backoffMs);
    }
  }
}

/* ─── Carrega schedules ativos no boot ─── */
async function loadSchedules(): Promise<void> {
  const lookaheadISO = new Date(Date.now() + LOOKAHEAD_MS).toISOString();

  const { data, error } = await supabase
    .from("schedules")
    .select("id, next_run_at")
    .eq("is_active", true)
    .lte("next_run_at", lookaheadISO);

  if (error) {
    console.error("[load] Erro ao carregar schedules:", error.message);
    return;
  }

  console.log(`[load] ${(data ?? []).length} schedule(s) carregado(s).`);
  for (const s of data ?? []) {
    scheduleAll(s.id, s.next_run_at);
  }
}

/* ─── Realtime: escuta mudanças na tabela schedules ─── */
function subscribeRealtime(): void {
  supabase
    .channel("schedules-changes")
    .on(
      "postgres_changes",
      { event: "*", schema: "public", table: "schedules" },
      (payload) => {
        const { eventType, new: newRow, old: oldRow } = payload;

        if (eventType === "DELETE") {
          const id = (oldRow as any)?.id;
          if (id) {
            cancelTimers(id);
            console.log(`[realtime] Schedule deletado: ${id}`);
          }
          return;
        }

        const row = newRow as any;
        if (!row?.id) return;

        if (!row.is_active) {
          cancelTimers(row.id);
          console.log(`[realtime] Schedule desativado: ${row.id}`);
          return;
        }

        if (eventType === "INSERT") {
          console.log(`[realtime] Novo schedule: ${row.id} → ${row.next_run_at}`);
          scheduleAll(row.id, row.next_run_at);
          return;
        }

        if (eventType === "UPDATE") {
          const oldNextRun = (oldRow as any)?.next_run_at;
          if (row.next_run_at !== oldNextRun) {
            console.log(`[realtime] Schedule ${row.id} atualizado → ${row.next_run_at}`);
            cancelTimers(row.id);
            scheduleAll(row.id, row.next_run_at);
          }
        }
      }
    )
    .subscribe((status) => {
      console.log(`[realtime] Status: ${status}`);
    });
}

/* ─── Pre-warm global das contas no boot ─── */
async function prewarmAllAccounts(): Promise<void> {
  const { data, error } = await supabase
    .from("accounts")
    .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
    .eq("is_active", true);

  if (error) {
    console.warn("[prewarm] Falha ao buscar contas:", error.message);
    return;
  }

  const accounts = (data ?? []) as Account[];
  console.log(`[prewarm] Conectando ${accounts.length} conta(s)...`);
  await Promise.allSettled(accounts.map((a) => clientPool.ensureConnected(a)));
  console.log(`[prewarm] Pronto.`);
}

/* ─── Graceful shutdown ─── */
async function shutdown(): Promise<void> {
  console.log("[worker] Encerrando...");
  for (const [id, timers] of scheduledTimers) {
    for (const t of timers) clearTimeout(t);
    console.log(`[worker] Timers cancelados: ${id}`);
  }
  await clientPool.disconnectAll();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);

/* ─── Init ─── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando...");

  await Promise.all([
    prewarmAllAccounts(),
    loadSchedules(),
  ]);

  subscribeRealtime();

  console.log("[worker] Pronto. Aguardando disparos...");
}

init().catch((err) => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});
