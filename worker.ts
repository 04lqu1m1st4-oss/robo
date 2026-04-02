// worker.ts
//
// Processo Node.js persistente rodando no Fly.io (região: ams).
// Substitui o Vercel Cron + process-schedules/route.ts.
//
// Vantagens sobre Vercel serverless:
//   - Sem cold start: processo fica rodando 24/7
//   - Conexões Telegram aquecidas e mantidas entre ciclos
//   - RTT mínimo: região ams é co-located com os servidores do Telegram
//   - Cron via node-cron com precisão de segundos (não minutos fixos)
//
// Lógica de retry mantida igual ao original:
//   - Retry inline: retenta membros que falharam dentro do mesmo ciclo
//   - Retry cross-invocation: salva estado no DB para o próximo tick
//   - Membros já enviados com sucesso no ciclo são ignorados no retry
//   - retry_until expirado → loga falha definitiva e avança next_run_at

import { createClient } from "@supabase/supabase-js";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions";
import cron from "node-cron";

/* ─── Supabase service_role ─── */
const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─── Budgets e timeouts ─── */
const INLINE_RETRY_BUDGET_MS = 50_000; // sem limite de 60s do Vercel — margem generosa
const SEND_TIMEOUT_MS        = 20_000;

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

/* ─── Pool de conexões Telegram persistente ──────────────────────────────
   Diferença chave vs Vercel: o pool vive enquanto o processo viver.
   Conexões abertas num ciclo ficam disponíveis no próximo — zero connect()
   em ciclos subsequentes para o mesmo account_id.                      ─── */
class TelegramClientPool {
  private clients = new Map<string, TelegramClient>();

  async get(account: Account): Promise<TelegramClient> {
    const existing = this.clients.get(account.id);
    if (existing) {
      // Verifica se a conexão ainda está viva
      try {
        if (existing.connected) return existing;
      } catch {
        // conexão morta — remove e reconecta
        this.clients.delete(account.id);
      }
    }

    const client = new TelegramClient(
      new StringSession(account.session_string),
      parseInt(account.api_id),
      account.api_hash,
      { connectionRetries: 3 }
    );

    await client.connect();
    this.clients.set(account.id, client);
    console.log(`[pool] Conexão aberta: ${account.phone_number}`);
    return client;
  }

  async disconnectAll(): Promise<void> {
    await Promise.all(
      [...this.clients.entries()].map(async ([id, client]) => {
        try { await client.disconnect(); } catch {}
        console.log(`[pool] Conexão encerrada: ${id}`);
      })
    );
    this.clients.clear();
  }
}

// Pool global — persiste entre ciclos do cron
const clientPool = new TelegramClientPool();

/* ─── Graceful shutdown ─── */
async function shutdown() {
  console.log("[worker] Encerrando — desconectando clientes Telegram...");
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
  ) {
    throw new Error(`cron_expression inválida: "${cronExpression}"`);
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

/* ─── Envio com timeout e FloodWait ─── */
async function sendMemberMessage(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string
): Promise<void> {
  const timeout = new Promise<never>((_, r) =>
    setTimeout(() => r(new Error("TIMEOUT: send excedeu 20s")), SEND_TIMEOUT_MS)
  );

  const send = (async () => {
    const chatIdNum = parseInt(telegramChatId, 10);
    const peer = await client.getInputEntity(isNaN(chatIdNum) ? telegramChatId : chatIdNum);

    try {
      await client.sendMessage(peer, { message: messageText });
    } catch (err: any) {
      const isFlood =
        err?.seconds != null ||
        err?.constructor?.name === "FloodWaitError" ||
        /flood/i.test(String(err?.message ?? ""));

      if (isFlood) {
        const waitSecs: number = err.seconds ?? parseInt(String(err.message ?? err), 10) ?? 30;
        console.warn(`[pool] FloodWait ${waitSecs}s — conta ${account.phone_number}`);
        if (waitSecs * 1000 < SEND_TIMEOUT_MS - 2000) {
          await new Promise((r) => setTimeout(r, waitSecs * 1000));
          await client.sendMessage(peer, { message: messageText });
        } else {
          throw new Error(`FLOOD_WAIT_${waitSecs}`);
        }
      } else {
        throw err;
      }
    }
  })();

  await Promise.race([send, timeout]);
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
    await sendMemberMessage(client, account, group.telegram_chat_id!, member.message_text ?? "");
    logStatus = "sent";
    console.log(`[worker] ✓ Membro ${member.id} (${account.phone_number})`);
  } catch (err) {
    errorMsg  = err instanceof Error ? err.message : String(err);
    retryable = isRetryableError(errorMsg);
    console.error(
      `[worker] ✗ Membro ${member.id} [${retryable ? "retryável" : "permanente"}] ` +
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

/* ─── Processa membros com retry inline ─── */
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
      console.log(`[worker] ↷ Membro ${member.id} pulado — já enviou neste ciclo`);
      finalResults.set(member.id, {
        member_id: member.id, account_id: account.id,
        status: "skipped", retryable: false,
      });
    } else {
      toProcess.push(member);
    }
  }

  let pending      = [...toProcess];
  let inlineAttempt = 0;

  while (pending.length > 0) {
    if (inlineAttempt > 0) {
      const waitMs  = Math.min(
        schedule.retry_interval_seconds * Math.pow(2, inlineAttempt - 1) * 1000,
        schedule.retry_interval_max_seconds * 1000
      );
      const timeLeft = inlineEnd - Date.now();
      if (timeLeft <= waitMs + SEND_TIMEOUT_MS) {
        console.log(
          `[worker] ⏱ Budget inline esgotado após ${inlineAttempt} tentativa(s). ` +
          `${pending.length} membro(s) → retry cross-invocation.`
        );
        break;
      }
      console.log(`[worker] ↻ Retry inline #${inlineAttempt} em ${waitMs}ms...`);
      await new Promise((r) => setTimeout(r, waitMs));
    }

    inlineAttempt++;

    const settled = await Promise.allSettled(
      pending.map((member) => trySendMember(member, member.accounts!, group, schedule))
    );

    const stillPending: GroupMember[] = [];

    settled.forEach((outcome, i) => {
      const member = pending[i];
      if (outcome.status === "fulfilled") {
        const result = outcome.value;
        finalResults.set(member.id, result);
        if (result.status === "failed" && result.retryable) stillPending.push(member);
      } else {
        const errorMsg = String(outcome.reason);
        finalResults.set(member.id, {
          member_id: member.id, account_id: member.accounts!.id,
          status: "failed", retryable: true, error: errorMsg,
        });
        stillPending.push(member);
      }
    });

    pending = stillPending;
    if (Date.now() >= inlineEnd) break;
  }

  return Array.from(finalResults.values());
}

/* ─── Ciclo principal ─── */
async function runCycle() {
  const now    = new Date();
  const nowISO = now.toISOString();

  const [
    { data: normalSchedules, error: normalError },
    { data: retrySchedules,  error: retryError  },
    { data: expiredRetries                       },
  ] = await Promise.all([
    supabase
      .from("schedules")
      .select(`id, cron_expression, user_id, group_id,
        retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
        retry_count, retry_until, last_attempt_at,
        groups(id, telegram_chat_id,
          group_members(id, message_text, position, is_active,
            accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))`)
      .eq("is_active", true)
      .lte("next_run_at", nowISO)
      .is("retry_until", null),

    supabase
      .from("schedules")
      .select(`id, cron_expression, user_id, group_id,
        retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
        retry_count, retry_until, last_attempt_at,
        groups(id, telegram_chat_id,
          group_members(id, message_text, position, is_active,
            accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))`)
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .gt("retry_until", nowISO),

    supabase
      .from("schedules")
      .select("id, cron_expression, user_id, group_id, retry_count")
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .lte("retry_until", nowISO),
  ]);

  if (normalError) { console.error("[worker] Erro ao buscar schedules:", normalError); return; }
  if (retryError)  { console.error("[worker] Erro ao buscar retries:", retryError); return; }

  // Retries expirados
  await Promise.all(
    (expiredRetries ?? []).map(async (expired) => {
      console.warn(`[worker] Schedule ${expired.id}: retry expirou. Avançando.`);
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
    })
  );

  const allSchedules = [
    ...(normalSchedules ?? []),
    ...(retrySchedules ?? []).filter((s) => isRetryDue(s as unknown as Schedule, now)),
  ] as unknown as Schedule[];

  if (allSchedules.length === 0) return; // nada a fazer neste tick

  console.log(
    `[worker] Processando ${allSchedules.length} schedule(s) ` +
    `(${normalSchedules?.length ?? 0} novos, ${retrySchedules?.length ?? 0} em retry)`
  );

  for (const schedule of allSchedules) {
    const group = schedule.groups;
    if (!group?.telegram_chat_id) {
      console.warn(`[worker] Schedule ${schedule.id}: sem telegram_chat_id, pulando.`);
      continue;
    }

    const succeededIds = await getSucceededAccountIds(schedule);
    const results      = await processMembersOf(schedule, succeededIds);

    const sentCount         = results.filter((r) => r.status === "sent").length;
    const skippedCount      = results.filter((r) => r.status === "skipped").length;
    const retryableFailures = results.filter((r) => r.status === "failed" && r.retryable);
    const permanentFailures = results.filter((r) => r.status === "failed" && !r.retryable);
    const totalDelivered    = sentCount + skippedCount;
    const allSucceeded      =
      retryableFailures.length === 0 && permanentFailures.length === 0 && totalDelivered > 0;

    if (allSucceeded) {
      let nextRun: string;
      try {
        nextRun = nextWeeklyOccurrence(schedule.cron_expression);
      } catch (err) {
        console.error(`[worker] Schedule ${schedule.id}: cron inválido, desativando:`, err);
        await supabase.from("schedules").update({ is_active: false }).eq("id", schedule.id);
        continue;
      }

      await supabase.from("schedules").update({
        next_run_at: nextRun, last_run_at: nowISO,
        retry_until: null, retry_count: 0,
        last_attempt_at: nowISO, last_attempt_status: "sent", last_attempt_error: null,
      }).eq("id", schedule.id);

      console.log(`[worker] ✓ Schedule ${schedule.id} OK. Próxima: ${nextRun}`);
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
      }).eq("id", schedule.id);

      console.warn(
        `[worker] ⚠ Schedule ${schedule.id}: ${retryableFailures.length} falha(s), ` +
        `retry #${newRetryCount} (até ${retryUntil})`
      );
    }
  }
}

// ─── Inicia o cron — roda a cada 30 segundos ────────────────────────────
// node-cron suporta granularidade de segundos com 6 campos.
// Expressão "*/30 * * * * *" = a cada 30 segundos → garante que nenhum
// schedule espera mais de 30s além do horário exato.
console.log("[worker] Iniciando — região ams — pool Telegram persistente");

cron.schedule("*/30 * * * * *", async () => {
  try {
    await runCycle();
  } catch (err) {
    console.error("[worker] Erro inesperado no ciclo:", err);
  }
});
