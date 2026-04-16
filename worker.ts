// worker.ts — precision Telegram dispatch worker
//
// Garantias:
//   ✓ Pre-warm progressivo: 60s, 30s, 10s, 5s, 3s, 2s, 1s antes do disparo
//   ✓ Retry infinito até 60s após o horário original — depois descarta
//   ✓ Zero duplicatas via firingLocks + sentSet por (scheduleId, memberId)
//   ✓ Disparo paralelo por membro (contas distintas, mesmo ou múltiplos grupos)
//   ✓ Nenhum erro é considerado permanente dentro da janela de 60s
//   ✓ Supabase Realtime para detectar schedules novos/alterados instantaneamente

import { createClient } from "@supabase/supabase-js";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions";

/* ─── Supabase ─── */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─── Constantes ─── */
const SEND_TIMEOUT_MS  = 10_000; // timeout por tentativa de envio
const RETRY_BASE_MS    = 300;    // backoff começa em 300ms (queremos velocidade)
const RETRY_MAX_MS     = 3_000;  // máximo 3s entre tentativas (dentro da janela de 60s)
const DISCARD_AFTER_MS = 60_000; // descarta se passou >60s do horário original
const LOOKAHEAD_MS     = 10 * 60 * 1000; // carrega schedules das próximas 10 min no boot

// Milissegundos antes do disparo para cada etapa de pre-warm
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

  private isHealthy(client: TelegramClient): boolean {
    if (!client.connected) return false;
    try {
      const sender = (client as any)._sender;
      if (!sender) return false;
      if (sender._isConnected === false) return false;
      if (sender._userDisconnected) return false;
    } catch {}
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

  async get(account: Account): Promise<TelegramClient> {
    const existing     = this.clients.get(account.id);
    const sessionInUse = this.sessions.get(account.id);

    if (existing && sessionInUse === account.session_string && this.isHealthy(existing)) {
      return existing;
    }

    // Evicta se sessão mudou ou morreu
    if (existing) {
      try { await existing.disconnect(); } catch {}
      this.clients.delete(account.id);
      this.sessions.delete(account.id);
    }

    // Coalescing: evita múltiplas conexões simultâneas para a mesma conta
    const inFlight = this.connecting.get(account.id);
    if (inFlight) return inFlight;

    const promise = (async (): Promise<TelegramClient> => {
      const client = new TelegramClient(
        new StringSession(account.session_string),
        parseInt(account.api_id),
        account.api_hash,
        { connectionRetries: 5 }
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

// Todos os timers ativos por scheduleId (prewarm + disparo)
// Armazena múltiplos timers por schedule (os steps de prewarm)
const scheduledTimers = new Map<string, ReturnType<typeof setTimeout>[]>();

// Lock de disparo: scheduleId → timestamp do início do disparo
const firingLocks = new Map<string, number>();

// Controle de envio já realizado neste ciclo: "scheduleId:memberId"
// Limpa automaticamente após o fireSchedule terminar
const sentSet = new Set<string>();

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

      // FloodWait: espera e retenta dentro da mesma tentativa
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

  // Passou mais de 60s atrás? Ignora — será tratado no próximo ciclo semanal.
  if (delay < -DISCARD_AFTER_MS) {
    console.warn(
      `[timer] Schedule ${scheduleId} passou ${Math.round(-delay / 1000)}s atrás — ignorando.`
    );
    return;
  }

  // Não recria se já existe um conjunto de timers para este schedule
  if (scheduledTimers.has(scheduleId)) return;

  const timers: ReturnType<typeof setTimeout>[] = [];

  // ── Pre-warm steps ──────────────────────────────────────────────────────────
  for (const stepMs of PREWARM_STEPS_MS) {
    const stepDelay = delay - stepMs;
    if (stepDelay <= 0) continue; // já passamos desta janela, pula

    const t = setTimeout(async () => {
      console.log(
        `[prewarm] ⚡ ${stepMs / 1000}s antes do disparo — schedule ${scheduleId}`
      );
      await prewarmScheduleAccounts(scheduleId);
    }, stepDelay);

    timers.push(t);
  }

  // ── Timer principal de disparo ──────────────────────────────────────────────
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
  // ── Lock: nunca double-fire ────────────────────────────────────────────────
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

    // Dispara todos os membros em paralelo, cada um com retry até esgotar janela
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

    if (anySuccess) {
      // Avança para próxima semana
      const nextRun = nextWeeklyOccurrence(schedule.cron_expression);
      await supabase.from("schedules").update({
        next_run_at:         nextRun,
        last_run_at:         new Date().toISOString(),
        retry_until:         null,
        retry_count:         0,
        last_attempt_at:     new Date().toISOString(),
        last_attempt_status: allSuccess ? "sent" : "partial",
        last_attempt_error:  null,
      }).eq("id", scheduleId);

      console.log(`[fire] ✓ Schedule ${scheduleId} completo. Próxima: ${nextRun}`);

      // Agenda próxima ocorrência
      scheduleAll(scheduleId, nextRun);
    } else {
      // Nenhum membro enviou — janela esgotada para todos
      console.error(
        `[fire] ✗ Schedule ${scheduleId}: nenhum membro conseguiu enviar dentro da janela.`
      );
      // Ainda assim avança — não queremos travar o schedule para sempre
      const nextRun = nextWeeklyOccurrence(schedule.cron_expression);
      await supabase.from("schedules").update({
        next_run_at:         nextRun,
        last_run_at:         new Date().toISOString(),
        last_attempt_at:     new Date().toISOString(),
        last_attempt_status: "failed",
        last_attempt_error:  "Janela de 60s esgotada sem envio bem-sucedido.",
      }).eq("id", scheduleId);

      scheduleAll(scheduleId, nextRun);
    }

  } finally {
    // Limpa locks e sentSet deste ciclo
    firingLocks.delete(scheduleId);
    for (const key of sentSet) {
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
  const account    = member.accounts!;
  const sentKey    = `${schedule.id}:${member.id}`;
  let   attempt    = 0;

  // Garante que não enviamos este membro mais de uma vez neste ciclo
  if (sentSet.has(sentKey)) {
    console.warn(`[send] Membro ${member.id} já enviado neste ciclo — ignorando.`);
    return;
  }

  while (true) {
    attempt++;

    // ── Verifica janela de tempo ──────────────────────────────────────────────
    const elapsed = Date.now() - scheduledFireAt;
    if (elapsed > DISCARD_AFTER_MS) {
      const msg = `Janela de ${DISCARD_AFTER_MS / 1000}s esgotada após ${attempt - 1} tentativa(s).`;
      console.warn(`[send] ✗ Descartando membro ${member.id} (${account.phone_number}): ${msg}`);

      await supabase.from("dispatch_logs").insert({
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
      const client = await clientPool.get(account);
      await sendMessage(client, group.telegram_chat_id!, member.message_text ?? "");

      // ── Sucesso ────────────────────────────────────────────────────────────
      sentSet.add(sentKey); // marca como enviado neste ciclo
      console.log(
        `[send] ✓ Membro ${member.id} (${account.phone_number}) — tentativa ${attempt} ` +
        `(+${Date.now() - scheduledFireAt}ms)`
      );

      await supabase.from("dispatch_logs").insert({
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
      // Não propaga se for o erro de "janela esgotada" (já tratado acima)
      if (err?.message?.includes("esgotada")) throw err;

      const errorMsg = err instanceof Error ? err.message : String(err);

      // Erros de conexão → evicta para pegar client fresco
      const isConnError =
        /not connected/i.test(errorMsg)  ||
        /connection closed/i.test(errorMsg) ||
        /TIMEOUT/.test(errorMsg);

      if (isConnError) clientPool.evict(account.id);

      // Backoff exponencial, mas limitado (queremos ser rápidos dentro dos 60s)
      const backoffMs = Math.min(RETRY_BASE_MS * Math.pow(2, attempt - 1), RETRY_MAX_MS);
      console.warn(
        `[send] ✗ Membro ${member.id} (${account.phone_number}) tentativa ${attempt} ` +
        `— retry em ${backoffMs}ms: ${errorMsg}`
      );

      // Log de falha (não-bloqueante)
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
      // Continua tentando...
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
