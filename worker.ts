// worker.ts — precision Telegram dispatch worker
// Arquitetura:
//   - Supabase Realtime: detecta schedules novos/alterados instantaneamente
//   - setTimeout de precisão: dispara exatamente no next_run_at
//   - Retry infinito com backoff até conseguir enviar
//   - Lock por scheduleId: nunca double-send

import { createClient } from "@supabase/supabase-js";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions";

/* ─── Supabase ─── */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─── Constantes ─── */
const SEND_TIMEOUT_MS   = 15_000;
const RETRY_BASE_MS     = 2_000;   // começa em 2s
const RETRY_MAX_MS      = 60_000;  // máximo 60s entre tentativas
const LOOKAHEAD_MS      = 5 * 60 * 1000; // carrega schedules das próximas 5 min no boot

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

async function getOrResolvePeer(client: TelegramClient, telegramChatId: string): Promise<unknown> {
  if (peerCache.has(telegramChatId)) return peerCache.get(telegramChatId)!;
  const chatIdNum = parseInt(telegramChatId, 10);
  const peer = await client.getInputEntity(isNaN(chatIdNum) ? telegramChatId : chatIdNum);
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

    // Coalescing: evita múltiplas conexões simultâneas para mesma conta
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

/* ─── Mapa de timers e locks ativos ─── */
const scheduledTimers = new Map<string, ReturnType<typeof setTimeout>>();
const firingLocks     = new Set<string>(); // schedules em processo de envio

/* ─── Agenda timer de precisão para um schedule ─── */
function scheduleTimer(scheduleId: string, nextRunAt: string): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  // Já passou da hora mas ainda dentro de 60s de tolerância? Dispara agora.
  // Passou mais de 60s? Ignora — será tratado pelo próximo ciclo semanal.
  if (delay < -60_000) {
    console.warn(`[timer] Schedule ${scheduleId} passou ${Math.round(-delay / 1000)}s atrás — ignorando.`);
    return;
  }

  // Já tem timer agendado para o mesmo horário? Não recria.
  if (scheduledTimers.has(scheduleId)) return;

  const effectiveDelay = Math.max(0, delay);

  const timer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    await fireSchedule(scheduleId);
  }, effectiveDelay);

  scheduledTimers.set(scheduleId, timer);

  const fireAt = new Date(Date.now() + effectiveDelay).toISOString();
  console.log(`[timer] ⏰ Schedule ${scheduleId} — dispara em ${Math.round(effectiveDelay / 1000)}s (${fireAt})`);
}

/* ─── Cancela timer de um schedule ─── */
function cancelTimer(scheduleId: string): void {
  const t = scheduledTimers.get(scheduleId);
  if (t) {
    clearTimeout(t);
    scheduledTimers.delete(scheduleId);
    console.log(`[timer] ✖ Timer cancelado: ${scheduleId}`);
  }
}

/* ─── Envia mensagem com timeout ─── */
async function sendMessage(
  client: TelegramClient,
  telegramChatId: string,
  messageText: string
): Promise<void> {
  const timeout = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error("TIMEOUT")), SEND_TIMEOUT_MS)
  );

  const send = (async () => {
    const peer = await getOrResolvePeer(client, telegramChatId);
    try {
      await client.sendMessage(peer as any, { message: messageText });
    } catch (err: any) {
      if (String(err?.message ?? "").includes("PEER_ID_INVALID")) {
        peerCache.delete(telegramChatId);
        const freshPeer = await getOrResolvePeer(client, telegramChatId);
        await client.sendMessage(freshPeer as any, { message: messageText });
        return;
      }

      // FloodWait: espera e retenta
      const isFlood =
        err?.seconds != null ||
        err?.constructor?.name === "FloodWaitError" ||
        /flood/i.test(String(err?.message ?? ""));

      if (isFlood) {
        const waitSecs: number = err.seconds ?? 30;
        console.warn(`[send] FloodWait ${waitSecs}s`);
        await sleep(waitSecs * 1000);
        const freshPeer = await getOrResolvePeer(client, telegramChatId);
        await client.sendMessage(freshPeer as any, { message: messageText });
        return;
      }

      throw err;
    }
  })();

  await Promise.race([send, timeout]);
}

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

/* ─── Dispara um schedule: envia para TODOS os membros, retry infinito por membro ─── */
async function fireSchedule(scheduleId: string): Promise<void> {
  // Lock: evita disparar o mesmo schedule duas vezes em paralelo
  if (firingLocks.has(scheduleId)) {
    console.warn(`[fire] Schedule ${scheduleId} já está sendo processado — ignorando.`);
    return;
  }
  firingLocks.add(scheduleId);

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

    // Dispara todos os membros em paralelo, cada um com retry infinito independente
    await Promise.all(members.map((member) =>
      sendMemberWithInfiniteRetry(member, group, schedule)
    ));

    // Tudo enviado — avança para próxima semana
    const nextRun = nextWeeklyOccurrence(schedule.cron_expression);
    await supabase.from("schedules").update({
      next_run_at:         nextRun,
      last_run_at:         new Date().toISOString(),
      retry_until:         null,
      retry_count:         0,
      last_attempt_at:     new Date().toISOString(),
      last_attempt_status: "sent",
      last_attempt_error:  null,
    }).eq("id", scheduleId);

    console.log(`[fire] ✓ Schedule ${scheduleId} completo. Próxima: ${nextRun}`);

    // Agenda próxima ocorrência imediatamente
    scheduleTimer(scheduleId, nextRun);

  } finally {
    firingLocks.delete(scheduleId);
  }
}

/* ─── Envia mensagem de um membro com retry INFINITO até conseguir ─── */
async function sendMemberWithInfiniteRetry(
  member: GroupMember,
  group: Group,
  schedule: Schedule
): Promise<void> {
  const account = member.accounts!;
  let attempt = 0;

  while (true) {
    attempt++;
    try {
      const client = await clientPool.get(account);
      await sendMessage(client, group.telegram_chat_id!, member.message_text ?? "");

      // Sucesso
      console.log(`[send] ✓ Membro ${member.id} (${account.phone_number}) — tentativa ${attempt}`);

      await supabase.from("dispatch_logs").insert({
        user_id:      schedule.user_id,
        group_id:     group.id,
        account_id:   account.id,
        schedule_id:  schedule.id,
        status:       "sent",
        message_text: member.message_text,
        sent_at:      new Date().toISOString(),
        error_message: null,
      });

      return; // Pronto, sai do loop

    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : String(err);

      // Se perdeu conexão TCP, evicta para próxima tentativa pegar client fresco
      const isConnError =
        /not connected/i.test(errorMsg) ||
        /connection closed/i.test(errorMsg) ||
        /TIMEOUT/.test(errorMsg);

      if (isConnError) {
        clientPool.evict(account.id);
      }

      const backoffMs = Math.min(RETRY_BASE_MS * Math.pow(2, attempt - 1), RETRY_MAX_MS);
      console.warn(
        `[send] ✗ Membro ${member.id} (${account.phone_number}) tentativa ${attempt} — ` +
        `retry em ${backoffMs}ms: ${errorMsg}`
      );

      await supabase.from("dispatch_logs").insert({
        user_id:       schedule.user_id,
        group_id:      group.id,
        account_id:    account.id,
        schedule_id:   schedule.id,
        status:        "failed",
        message_text:  member.message_text,
        sent_at:       null,
        error_message: errorMsg,
      });

      await sleep(backoffMs);
      // Loop infinito — vai tentar de novo
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
    scheduleTimer(s.id, s.next_run_at);
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
            cancelTimer(id);
            console.log(`[realtime] Schedule deletado: ${id}`);
          }
          return;
        }

        const row = newRow as any;
        if (!row?.id) return;

        if (!row.is_active) {
          cancelTimer(row.id);
          console.log(`[realtime] Schedule desativado: ${row.id}`);
          return;
        }

        if (eventType === "INSERT") {
          console.log(`[realtime] Novo schedule detectado: ${row.id} → ${row.next_run_at}`);
          scheduleTimer(row.id, row.next_run_at);
        }

        if (eventType === "UPDATE") {
          // Se o next_run_at mudou, recancela e reagenda
          const oldNextRun = (oldRow as any)?.next_run_at;
          if (row.next_run_at !== oldNextRun) {
            console.log(`[realtime] Schedule ${row.id} atualizado → ${row.next_run_at}`);
            cancelTimer(row.id);
            scheduleTimer(row.id, row.next_run_at);
          }
        }
      }
    )
    .subscribe((status) => {
      console.log(`[realtime] Status: ${status}`);
    });
}

/* ─── Pre-warm das conexões Telegram ─── */
async function prewarmAccounts(): Promise<void> {
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

  await Promise.allSettled(accounts.map((a) => clientPool.get(a)));
  console.log(`[prewarm] Pronto.`);
}

/* ─── Graceful shutdown ─── */
async function shutdown() {
  console.log("[worker] Encerrando...");
  for (const [id, t] of scheduledTimers) {
    clearTimeout(t);
    console.log(`[worker] Timer cancelado: ${id}`);
  }
  await clientPool.disconnectAll();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);

/* ─── Init ─── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando...");

  // 1. Conecta contas Telegram em paralelo com carregamento de schedules
  await Promise.all([
    prewarmAccounts(),
    loadSchedules(),
  ]);

  // 2. Escuta mudanças em tempo real — novos schedules são agendados instantaneamente
  subscribeRealtime();

  console.log("[worker] Pronto. Aguardando disparos...");
}

init().catch((err) => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});
