// worker.ts — high-precision Telegram dispatch worker
//
// Fixes vs versão anterior:
//   1. [Fix Bug 1+2] scheduledFireTimes: novo mapa rastreia o nextRunAt agendado.
//      scheduleTimer só recria o timer se o horário mudou — senão ignora.
//      Resolve reload re-agendando o mesmo schedule a cada 30s.
//   2. [Fix Bug 2] scheduleTimer() removido de dentro do fireSchedule.
//      O reload é a ÚNICA fonte de timers — elimina double-fire após sucesso.
//   3. [Fix Bug 3] Envio sequencial (for await) em vez de Promise.allSettled paralelo.
//      Membros do mesmo grupo enviados um a vez — Telegram não rejeita o segundo send.
//   4. [Fix Bug 4] AUTH_KEY_DUPLICATED: connection coalescing via mapa `connecting`.
//      Concurrent callers em get() aguardam a mesma promise em vez de abrir duas conexões.

import { createClient } from "@supabase/supabase-js";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions";

/* ─── Supabase ─── */
const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─── Constantes ─── */
const SEND_TIMEOUT_MS        = 15_000;
const INLINE_RETRY_BUDGET_MS = 50_000;

const FAST_RETRY_BASE_MS   = 100;
const FAST_RETRY_WINDOW_MS = 30_000;

const RELOAD_INTERVAL_MS = 30_000;
const LOOKAHEAD_MS       = 2 * 60 * 1000; // 2 minutos

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

/* ─── Peer cache global ─── */
const peerCache    = new Map<string, unknown>();
const accountCache = new Map<string, Account>();

async function getOrResolvePeer(client: TelegramClient, telegramChatId: string): Promise<unknown> {
  if (peerCache.has(telegramChatId)) return peerCache.get(telegramChatId)!;
  const chatIdNum = parseInt(telegramChatId, 10);
  const peer = await client.getInputEntity(isNaN(chatIdNum) ? telegramChatId : chatIdNum);
  peerCache.set(telegramChatId, peer);
  console.log(`[peer] Resolvido e cacheado: ${telegramChatId}`);
  return peer;
}

/* ─── Pool de conexões Telegram persistente ─── */
class TelegramClientPool {
  private clients    = new Map<string, TelegramClient>();
  private sessions   = new Map<string, string>();
  private connecting = new Map<string, Promise<TelegramClient>>(); // FIX BUG 4

  async get(account: Account): Promise<TelegramClient> {
    const existing       = this.clients.get(account.id);
    const sessionInUse   = this.sessions.get(account.id);
    const sessionChanged = sessionInUse !== account.session_string;

    if (existing?.connected && !sessionChanged) return existing;

    // FIX BUG 4: se já existe uma conexão em andamento para esta conta,
    // aguarda ela em vez de abrir uma segunda — evita AUTH_KEY_DUPLICATED
    const inFlight = this.connecting.get(account.id);
    if (inFlight) return inFlight;

    if (existing) {
      try { await existing.disconnect(); } catch {}
      this.clients.delete(account.id);
    }

    if (sessionChanged && sessionInUse) {
      console.log(`[pool] Session mudou para ${account.phone_number} — reconectando...`);
    }

    const promise = (async () => {
      const client = new TelegramClient(
        new StringSession(account.session_string),
        parseInt(account.api_id),
        account.api_hash,
        { connectionRetries: 3 }
      );
      await client.connect();
      this.clients.set(account.id, client);
      this.sessions.set(account.id, account.session_string);
      console.log(`[pool] Conectado: ${account.phone_number}`);
      return client;
    })();

    this.connecting.set(account.id, promise);
    try {
      return await promise;
    } finally {
      // FIX BUG 4: sempre limpa — inclusive em caso de erro, para não bloquear reconexões futuras
      this.connecting.delete(account.id);
    }
  }

  async prewarm(accounts: Account[]): Promise<void> {
    console.log(`[pool] Pre-warming ${accounts.length} conta(s)...`);
    await Promise.allSettled(accounts.map((a) => this.get(a)));
    console.log(`[pool] Pre-warm concluído.`);
  }

  async disconnectAll(): Promise<void> {
    this.connecting.clear(); // FIX BUG 4: cancela referências de conexões pendentes
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
  await clientPool.disconnectAll();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);

/* ─── Helpers ─── */
const PERMANENT_ERRORS: string[] = [];

function isRetryableError(msg: string): boolean {
  const upper = msg.toUpperCase();
  if (PERMANENT_ERRORS.some((e) => upper.includes(e))) return false;
  return true;
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

async function getSucceededAccountIds(schedule: Schedule): Promise<string[]> {
  if (!schedule.retry_until) return [];
  const retryStartedAt = new Date(
    new Date(schedule.retry_until).getTime() - schedule.retry_window_seconds * 1000
  ).toISOString();
  const { data, error } = await supabase
    .from("dispatch_logs")
    .select("account_id")
    .eq("schedule_id", schedule.id)
    .eq("status", "sent")
    .gte("sent_at", retryStartedAt);
  if (error) {
    console.warn(`[worker] Falha ao buscar enviados do schedule ${schedule.id}:`, error.message);
    return [];
  }
  return (data ?? []).map((r) => r.account_id as string);
}

/* ─── Envio com fast retry para grupo fechado ─── */
async function sendWithFastRetry(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string
): Promise<void> {
  const windowEnd = Date.now() + FAST_RETRY_WINDOW_MS;
  let attempt     = 0;
  let zigzagDir   = 1;
  let zigzagStep  = 1;

  while (true) {
    const timeout = new Promise<never>((_, r) =>
      setTimeout(() => r(new Error("TIMEOUT: send excedeu 15s")), SEND_TIMEOUT_MS)
    );

    const trySend = (async () => {
      const peer = await getOrResolvePeer(client, telegramChatId);

      try {
        await client.sendMessage(peer as any, { message: messageText });
      } catch (err: any) {
        if (String(err?.message ?? "").includes("PEER_ID_INVALID")) {
          console.warn(`[peer] Cache inválido para ${telegramChatId} — limpando e resolvendo novamente`);
          peerCache.delete(telegramChatId);
        }

        const isFlood =
          err?.seconds != null ||
          err?.constructor?.name === "FloodWaitError" ||
          /flood/i.test(String(err?.message ?? ""));

        if (isFlood) {
          const waitSecs: number = err.seconds ?? parseInt(String(err.message ?? err), 10) ?? 30;
          console.warn(`[pool] FloodWait ${waitSecs}s — ${account.phone_number}`);
          if (waitSecs * 1000 < SEND_TIMEOUT_MS - 2000) {
            await new Promise((r) => setTimeout(r, waitSecs * 1000));
            const freshPeer = await getOrResolvePeer(client, telegramChatId);
            await client.sendMessage(freshPeer as any, { message: messageText });
          } else {
            throw new Error(`FLOOD_WAIT_${waitSecs}`);
          }
          return;
        }

        throw err;
      }
    })();

    try {
      await Promise.race([trySend, timeout]);
      if (attempt > 0) {
        console.log(`[fast-retry] ✓ Enviado na tentativa ${attempt + 1} (${account.phone_number})`);
      }
      return;
    } catch (err: unknown) {
      if (Date.now() < windowEnd) {
        const waitMs   = zigzagStep * FAST_RETRY_BASE_MS;
        const timeLeft = windowEnd - Date.now();

        if (timeLeft > waitMs) {
          console.log(
            `[fast-retry] Falha na tentativa ${attempt + 1} — aguardando ${waitMs}ms ` +
            `(${Math.round(timeLeft / 1000)}s restantes): ${(err as any)?.message ?? err}`
          );
          await new Promise((r) => setTimeout(r, waitMs));
          attempt++;

          zigzagStep += zigzagDir;
          if (zigzagStep >= 7) zigzagDir = -1;
          if (zigzagStep <= 1) zigzagDir =  1;

          continue;
        }
      }

      throw err;
    }
  }
}

/* ─── Tenta enviar um membro ─── */
async function trySendMember(
  member: GroupMember,
  account: Account,
  group: Group,
  schedule: Schedule
): Promise<MemberResult> {
  let logStatus: "sent" | "failed" = "failed";
  let errorMsg: string | undefined;
  let retryable = false;

  try {
    const client = await clientPool.get(account);
    await sendWithFastRetry(client, account, group.telegram_chat_id!, member.message_text ?? "");
    logStatus = "sent";
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

/* ─── Processa membros sequencialmente (FIX BUG 3) ─── */
async function processMembersOf(
  schedule: Schedule,
  skipAccountIds: string[] = []
): Promise<MemberResult[]> {
  const group     = schedule.groups;
  const budgetEnd = Date.now() + INLINE_RETRY_BUDGET_MS;
  const windowEnd = Date.now() + schedule.retry_window_seconds * 1000;
  const inlineEnd = Math.min(budgetEnd, windowEnd);

  const members = (group.group_members ?? [])
    .filter((m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);

  const finalResults = new Map<string, MemberResult>();
  const toProcess: GroupMember[] = [];

  for (const member of members) {
    const account = member.accounts!;
    if (skipAccountIds.includes(account.id)) {
      console.log(`[worker] ↷ ${member.id} pulado — já enviou neste ciclo`);
      finalResults.set(member.id, {
        member_id: member.id,
        account_id: account.id,
        status: "skipped",
        retryable: false,
      });
    } else {
      toProcess.push(member);
    }
  }

  let pending       = [...toProcess];
  let inlineAttempt = 0;

  while (pending.length > 0) {
    if (inlineAttempt > 0) {
      const waitMs   = Math.min(
        schedule.retry_interval_seconds * Math.pow(2, inlineAttempt - 1) * 1000,
        schedule.retry_interval_max_seconds * 1000
      );
      const timeLeft = inlineEnd - Date.now();
      if (timeLeft <= waitMs + SEND_TIMEOUT_MS) {
        console.log(`[worker] ⏱ Budget inline esgotado. ${pending.length} membro(s) → cross-invocation.`);
        break;
      }
      console.log(`[worker] ↻ Retry inline #${inlineAttempt} em ${waitMs}ms...`);
      await new Promise((r) => setTimeout(r, waitMs));
    }

    inlineAttempt++;

    // FIX BUG 3: sequencial em vez de Promise.allSettled paralelo
    const settled: PromiseSettledResult<MemberResult>[] = [];
    for (const m of pending) {
      try {
        const result = await trySendMember(m, m.accounts!, group, schedule);
        settled.push({ status: "fulfilled", value: result });
      } catch (reason) {
        settled.push({ status: "rejected", reason });
      }
    }

    const stillPending: GroupMember[] = [];
    settled.forEach((outcome, i) => {
      const member = pending[i];
      if (outcome.status === "fulfilled") {
        finalResults.set(member.id, outcome.value);
        if (outcome.value.status === "failed" && outcome.value.retryable) stillPending.push(member);
      } else {
        const errorMsg = String((outcome as PromiseRejectedResult).reason);
        finalResults.set(member.id, {
          member_id: member.id,
          account_id: member.accounts!.id,
          status: "failed",
          retryable: true,
          error: errorMsg,
        });
        stillPending.push(member);
      }
    });

    pending = stillPending;
    if (Date.now() >= inlineEnd) break;
  }

  return Array.from(finalResults.values());
}

/* ─── Dispara um schedule específico (chamado pelo precision timer) ─── */
async function fireSchedule(scheduleId: string): Promise<void> {
  const now    = new Date();
  const nowISO = now.toISOString();

  const { data: rows, error } = await supabase
    .from("schedules")
    .select(`
      id, cron_expression, user_id, group_id, next_run_at,
      retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
      retry_count, retry_until, last_attempt_at,
      groups(id, telegram_chat_id,
        group_members(id, message_text, position, is_active,
          accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))
    `)
    .eq("id", scheduleId)
    .eq("is_active", true)
    .single();

  if (error || !rows) {
    console.warn(`[timer] Schedule ${scheduleId} não encontrado ou inativo no momento do disparo.`);
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
      accounts: m.accounts
        ? (accountCache.get(m.accounts.id) ?? m.accounts)
        : null,
    }));
  }

  console.log(`[timer] ⚡ Disparando schedule ${scheduleId} às ${nowISO}`);

  const succeededIds = await getSucceededAccountIds(schedule);
  const results      = await processMembersOf(schedule, succeededIds);

  const sentCount         = results.filter((r) => r.status === "sent").length;
  const skippedCount      = results.filter((r) => r.status === "skipped").length;
  const retryableFailures = results.filter((r) => r.status === "failed" && r.retryable);
  const permanentFailures = results.filter((r) => r.status === "failed" && !r.retryable);
  const totalDelivered    = sentCount + skippedCount;
  const allSucceeded      = retryableFailures.length === 0 && permanentFailures.length === 0 && totalDelivered > 0;

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
      next_run_at: nextRun, last_run_at: nowISO,
      retry_until: null, retry_count: 0,
      last_attempt_at: nowISO, last_attempt_status: "sent", last_attempt_error: null,
    }).eq("id", scheduleId);

    console.log(`[timer] ✓ Schedule ${scheduleId} OK. Próxima: ${nextRun}`);
    // FIX BUG 2: NÃO chama scheduleTimer aqui.
    // O reload periódico (30s) vai encontrar o novo next_run_at no banco e agendar sozinho.
    // Chamar scheduleTimer aqui + reload = double-fire garantido.

  } else {
    const newRetryCount  = schedule.retry_count + 1;
    const isFirstFailure = !schedule.retry_until;
    const retryUntil     = isFirstFailure
      ? new Date(now.getTime() + schedule.retry_window_seconds * 1000).toISOString()
      : schedule.retry_until!;

    const failedErrors = results
      .filter((r) => r.status === "failed" && r.error)
      .map((r) => r.error)
      .join("; ");

    await supabase.from("schedules").update({
      retry_until: retryUntil, retry_count: newRetryCount,
      last_attempt_at: nowISO, last_attempt_status: "retrying",
      last_attempt_error: failedErrors || null,
    }).eq("id", scheduleId);

    const intervalNext = calcRetryInterval(
      newRetryCount,
      schedule.retry_interval_seconds,
      schedule.retry_interval_max_seconds
    );

    console.warn(
      `[timer] ⚠ Schedule ${scheduleId}: ${retryableFailures.length} falha(s), ` +
      `retry cross-invocation #${newRetryCount} em ~${intervalNext}s (até ${retryUntil})`
    );
  }
}

/* ─── Mapa de timers ativos ─── */
const scheduledTimers    = new Map<string, ReturnType<typeof setTimeout>>();
// FIX BUG 1: rastreia qual nextRunAt está agendado por scheduleId
const scheduledFireTimes = new Map<string, string>();

/* ─── Agenda um precision timer para um schedule ─────────────────────────
   FIX BUG 1+2: só cria/recria o timer se o nextRunAt mudou.
   Se já existe um timer para o mesmo scheduleId com o mesmo horário, ignora.
   Isso evita que o reload de 30s duplique disparos.                     ─── */
function scheduleTimer(scheduleId: string, nextRunAt: string): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  if (delay < -5000) {
    // Já passou há mais de 5s — não agenda (retry ou reload vai pegar)
    return;
  }

  const existing       = scheduledTimers.get(scheduleId);
  const existingFireAt = scheduledFireTimes.get(scheduleId);

  // FIX: se já existe timer para o mesmo horário, não recria
  if (existing && existingFireAt === nextRunAt) {
    return;
  }

  // Cancela timer anterior se existir (horário mudou, ex: retry resetou next_run_at)
  if (existing) {
    clearTimeout(existing);
    console.log(`[timer] ♻ Timer anterior cancelado para ${scheduleId} (horário mudou de ${existingFireAt} para ${nextRunAt})`);
  }

  const effectiveDelay = Math.max(0, delay);

  const timer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    scheduledFireTimes.delete(scheduleId);
    try {
      await fireSchedule(scheduleId);
    } catch (err) {
      console.error(`[timer] Erro inesperado ao disparar schedule ${scheduleId}:`, err);
    }
  }, effectiveDelay);

  scheduledTimers.set(scheduleId, timer);
  scheduledFireTimes.set(scheduleId, nextRunAt);

  const fireAt = new Date(Date.now() + effectiveDelay).toISOString();
  console.log(`[timer] ⏰ Schedule ${scheduleId} agendado — dispara em ${Math.round(effectiveDelay / 1000)}s (${fireAt})`);
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
  ] = await Promise.all([
    supabase
      .from("schedules")
      .select("id, next_run_at")
      .eq("is_active", true)
      .is("retry_until", null)
      .lte("next_run_at", lookaheadISO),

    supabase
      .from("schedules")
      .select(`
        id, cron_expression, user_id, group_id, next_run_at,
        retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
        retry_count, retry_until, last_attempt_at,
        groups(id, telegram_chat_id,
          group_members(id, message_text, position, is_active,
            accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))
      `)
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .gt("retry_until", nowISO),

    supabase
      .from("schedules")
      .select("id, cron_expression")
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .lte("retry_until", nowISO),
  ]);

  // Limpa retries expirados
  await Promise.all(
    (expiredRetries ?? []).map(async (expired) => {
      console.warn(`[reload] Schedule ${expired.id}: retry expirou. Avançando.`);
      let nextRun: string;
      try {
        nextRun = nextWeeklyOccurrence(expired.cron_expression);
      } catch {
        await supabase.from("schedules").update({ is_active: false }).eq("id", expired.id);
        return;
      }
      await supabase.from("schedules").update({
        next_run_at: nextRun, last_run_at: nowISO,
        retry_until: null, retry_count: 0,
        last_attempt_at: nowISO, last_attempt_status: "failed",
        last_attempt_error: "Retry expirou sem sucesso total",
      }).eq("id", expired.id);
      scheduleTimer(expired.id, nextRun);
    })
  );

  // Agenda timers para schedules futuros dentro da janela de lookahead
  // FIX BUG 1: scheduleTimer agora é idempotente — ignora se já existe timer para o mesmo horário
  for (const s of futureSchedules ?? []) {
    scheduleTimer(s.id, s.next_run_at);
  }

  // Dispara retries cross-invocation que já são devidos
  for (const s of retrySchedules ?? []) {
    const schedule = s as unknown as Schedule;
    if (isRetryDue(schedule, now) && !scheduledTimers.has(schedule.id)) {
      console.log(`[reload] Schedule ${schedule.id} em retry — disparando agora.`);
      fireSchedule(schedule.id).catch((err) =>
        console.error(`[reload] Erro no retry do schedule ${schedule.id}:`, err)
      );
    }
  }
}

/* ─── Pre-warm ─── */
let prewarmRunning = false;
async function prewarmAccounts(): Promise<void> {
  if (prewarmRunning) return;
  prewarmRunning = true;
  const { data, error } = await supabase
    .from("accounts")
    .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
    .eq("is_active", true);

  if (error) {
    console.warn("[prewarm] Falha ao buscar contas:", error.message);
    prewarmRunning = false;
    return;
  }

  const accounts = (data ?? []) as Account[];

  for (const account of accounts) {
    accountCache.set(account.id, account);
  }

  await clientPool.prewarm(accounts);
  prewarmRunning = false;
}

/* ─── Inicialização ─── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando — precision timers + peer cache + fast retry + envio sequencial");

  await Promise.all([
    prewarmAccounts(),
    reloadSchedules(),
  ]);

  setInterval(async () => {
    try {
      await Promise.all([reloadSchedules(), prewarmAccounts()]);
    } catch (err) {
      console.error("[reload] Erro no reload periódico:", err);
    }
  }, RELOAD_INTERVAL_MS);

  console.log("[worker] Pronto. Precision timers ativos.");
}

init().catch((err) => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});
