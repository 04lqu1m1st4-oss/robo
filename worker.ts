// worker-flat.ts — dispatch worker Telegram, sem camadas de abstração
//
// Fix v1: firingNow Set previne duplo disparo quando reloadSchedules
//         roda enquanto fireSchedule ainda está executando (race condition
//         entre RETRY_BUDGET_MS=50s e RELOAD_INTERVAL_MS=30s)
//
// Fix v2 (2025-05):
//   BUG #1 — reconnect loop infinito:
//     _loopStarted=true antes de connect() suprime o updateLoop sem quebrar
//     o fluxo de reconexão (autoReconnect, _handleReconnect não dependem do flag).
//
//   BUG #2 — peerCache stale após reconexão:
//     Ao desconectar/reconectar um account, todos os peers desse account
//     são removidos do cache. accessHash gerado com sessão anterior é inválido.
//
//   BUG #3 — sem delay entre tentativas no sendMessage:
//     Adicionado backoff exponencial (1s → 2s → 4s → 8s) entre tentativas.
//
//   BUG #4 — getDialogs warm-up não era awaited:
//     Warm-up movido para prewarmAccounts() com await + pre-resolve de peers.
//
// Fix v3 (2025-05):
//   BUG #5 — AUTH_KEY_DUPLICATED (406) via race condition em getClient():
//     connectingPromises Map<accountId, Promise<TelegramClient>> serializa
//     conexões concorrentes para o mesmo account.
//
// Otimizações v4 (2025-05) — latência mínima no caminho crítico:
//
//   OPT #1 — invoke direto em vez de client.sendMessage():
//   OPT #2 — pre-fetch do schedule antes do fire
//   OPT #3 — pre-resolve de peers no prewarm
//   OPT #4 — noWebpage: true + randomId único por tentativa
//
// Fix v5 (2025-05) — sniper loop para grupos fechados:
//   OPT #5 — sniperFireClosed(): loop ultra-agressivo para grupos fechados
//
// Fix v6 (2025-05) — correções de bugs críticos:
//
//   BUG #A — duplo disparo sniper + fireSchedule no mesmo ciclo
//   BUG #B — groupType perdido após o 1º ciclo
//   BUG #C — FloodWait no sniper gera ~30.000 chamadas em 30s
//   BUG #D — reloadClient() deleta mutex sem await da promise inflight
//   BUG #E — prewarmAccounts() a cada 30s chama getDialogs em todas as contas
//   BUG #F — makeRandomId com entropia insuficiente
//   BUG #G — keepalive sem jitter
//
// Fix v7 (2025-05) — timing logs + calibração SNIPER_BEFORE_MS
// Fix v8 (2025-05) — randomId estável por ciclo de tentativas
// Fix v9 (2025-05) — 3 fixes cirúrgicos sobre o v8 original
// Fix v10 (2025-05) — fase 2 paralela
// Fix v11 (2025-05) — loop unificado: todas as contas em paralelo desde o início
// Fix v12 (2025-06) — monitorPositions: posição real no histórico do grupo
// Fix v13 (2025-06) — peer pré-resolvido no slot
// Fix v14 (2025-06) — scheduleDate: Telegram segura o envio no horário exato
//
// Fix v15 (2025-06) — sleep calibrado substitui scheduleDate + fix de posições:
//
//   BUG #K — scheduleDate tem granularidade de 1s e entrega em ms aleatório:
//     Medições com 3 testes mostraram conta 065 chegando consistentemente
//     em -3ms a -5ms (antes do horário!) e conta 730 chegando em +1ms a +13ms.
//     O Telegram decide internamente em qual ms do segundo entrega — sem controle
//     nosso. Para leilões onde 200ms = 30 posições, isso é inaceitável.
//
//   SOLUÇÃO — sleep calibrado por conta antes do invoke (sem scheduleDate):
//     Antes de cada invoke no loop paralelo, cada slot dorme até:
//       scheduledAt + TARGET_ARRIVAL_OFFSET_MS - ONE_WAY_RTT_ESTIMATE_MS
//     Com TARGET=10ms e ONE_WAY=23ms, o invoke sai em scheduledAt-13ms
//     e a mensagem chega ao servidor em ~scheduledAt+10ms.
//     Todas as contas dormem até o MESMO timestamp absoluto → disparam juntas.
//     Garante: (1) nunca chega antes do horário, (2) chega ~10ms depois,
//     (3) todas as contas chegam no mesmo instante.
//     Calibrar ONE_WAY_RTT_ESTIMATE_MS com os logs [sniper][timing].
//
//   BUG #L — posição calculada incluía mensagens pré-leilão:
//     windowStartUnix começava MONITOR_WINDOW_BEFORE_MS (5s) antes do
//     scheduledAt, fazendo mensagens enviadas ANTES da abertura do leilão
//     contarem como posições anteriores às nossas.
//
//   SOLUÇÃO — auctionStartUnix separado do windowStartUnix:
//     _savePositions() recebe auctionStartUnix (= Math.floor(scheduledAt/1000)
//     para closed, = windowStartUnix para open). Posição conta apenas
//     mensagens com date >= auctionStartUnix E id < myMsgId.
//     A busca do histórico ainda usa windowStartUnix mais amplo para contexto,
//     mas a contagem de posição é precisa a partir do segundo de abertura.
//
// Fix v16 (2025-06) — fix grupo aberto + remoção do listen-manual:
//
//   BUG #M — gotSignal em startGroupListener só aceitava texto "ok":
//     Emoji (👍, etc.) chegam como m.message com o caractere do emoji —
//     não são mídia — e não passavam pelo isOk (=== "ok"). Agora aceita
//     qualquer texto não vazio OU mídia, igual à lógica da v12.
//
//   REMOÇÃO — endpoint POST /groups/:id/listen (listen-manual) removido:
//     O disparo de grupos abertos é 100% automático via startGroupListener.
//     O endpoint manual era redundante e mantinha lógica duplicada e divergente.
//     O DELETE /groups/:id/listen (abortar listener) foi mantido.
//
// Fix v17 (2025-06) — 3 fixes críticos:
//
//   BUG #N — posição calculada errada quando múltiplas contas mandam o mesmo texto:
//     _savePositions buscava a mensagem só pelo texto (m.message === sm.message_text).
//     Se conta A e conta B mandam o mesmo texto, ambas achavam a MESMA mensagem no
//     histórico e registravam a mesma posição.
//     SOLUÇÃO: _savePositions agora recebe um Map<account_id, message_id> (myMsgIds)
//     resolvido ANTES de chamar a função. O caller (monitorPositions) cruza o histórico
//     com o peer_id/from_id de cada mensagem para achar o msg_id certo por conta.
//     Se não achar pelo from_id (conta anônima/canal), cai de volta no texto como
//     desempate, mas nunca reutiliza o mesmo msg_id para duas contas diferentes.
//
//   BUG #O — monitor falha silenciosamente quando conta não está no accountCache:
//     monitorPositions pegava account = accountCache.get(sentMembers[0].account_id).
//     Se a conta foi adicionada depois do boot (sem prewarm), não está no cache
//     e o monitor aborta sem salvar nenhuma posição.
//     SOLUÇÃO: busca a conta no Supabase como fallback quando não está no cache.
//     Garante que o monitor sempre tenha um client válido para buscar o histórico.
//
//   BUG #P — delay entre disparos no mesmo grupo (grupos abertos, múltiplas contas):
//     dispatchToGroup usava Promise.all mas cada sendMessage tem RETRY_BUDGET_MS=50s
//     de budget interno. Se a 1ª conta demorava, todas as outras esperavam junto.
//     Na prática o Promise.all rodava em paralelo mas dentro de sendMessage havia
//     backoff exponencial (1s→2s→4s→8s) que não tinha timeout externo por slot.
//     SOLUÇÃO: cada slot em dispatchToGroup tem agora um timeout externo de
//     DISPATCH_SLOT_TIMEOUT_MS (padrão 12s). Se uma conta não enviou em 12s,
//     marca como failed sem bloquear as outras. Budget total do grupo mantido
//     pelo Promise.all. Reduz latência de N*backoff para max(12s) em paralelo.
//
// Fix v20 (2025-06) — monitor: filtro de mensagens errado (BUG #R):
//
//   BUG #R — GetHistory retorna objetos com className="Message", não m._="message":
//     O filtro `m._ === "message"` rejeitava 100% das mensagens retornadas pelo
//     GramJS, que usa `className` (string Pascal-case) como identificador de tipo,
//     não o campo `_` da serialização TL raw. Resultado: "Nenhuma mensagem na janela"
//     mesmo quando o leilão tinha dezenas de mensagens visíveis.
//
//   SOLUÇÃO — aceita className OR m._ OR presença de m.message:
//     Filtro robusto: `m.className === "Message" || m._ === "message" || m.message != null`.
//     Cobre GramJS moderno (className), payload raw TL (m._), e fallback pragmático
//     (qualquer objeto com campo message é uma mensagem de texto).
//     windowStartUnix expandido em -30s para absorver skew de clock VPS/Telegram.
//     Log de debug adicionado: mostra total de msgs, na janela, windowStartUnix e
//     date da primeira msg — facilita calibração futura sem precisar de logs extras.
//
// Fix v19 (2025-06) — sleep calibrado movido para fora do Promise.allSettled:
//
//   BUG #Q — contention entre snipers simultâneos (dois schedules no mesmo horário):
//     O sleep calibrado estava DENTRO do Promise.allSettled, em cada slot.
//     Quando dois sniperFireClosed rodam em paralelo no mesmo processo Node.js,
//     o await do sleep interno cede o event loop para o outro sniper.
//     Resultado: o segundo sniper disparava 20~30ms depois do primeiro, mesmo
//     que ambos tivessem contas completamente independentes.
//
//   SOLUÇÃO — sleep único ANTES do Promise.allSettled (globalAttempt === 1 apenas):
//     O sleep é feito uma única vez, antes de entrar no allSettled.
//     Dentro do allSettled, os slots invocam diretamente sem nenhum await de timing.
//     Isso garante que todos os invokes saem no mesmo tick do event loop,
//     e que dois snipers concorrentes não se atrasam mutuamente.
//
// Fix v18 (2025-06) — resolução real de msg_id por from_id + simplifica monitor:
//
//   BUG #N (fix definitivo) — resolveMsgIds da v17 era um stub: o bloco de
//     resolução via from_id estava vazio e caía direto no fallback por texto,
//     que não garante a posição correta quando duas contas mandam o mesmo texto
//     (apenas evita reusar o mesmo msg_id, mas pode atribuir o msg_id errado
//     entre as contas).
//     SOLUÇÃO: telegramUserIdCache (account.id UUID → telegram numeric user id),
//     populado via client.getMe() em getClient() na primeira conexão de cada
//     conta. resolveMsgIds agora casa m.fromId?.userId / m.peerId?.userId do
//     histórico com esse id numérico — identificação correta por conta mesmo
//     com textos idênticos. Fallback por texto mantido apenas para contas
//     anônimas/canais onde from_id não vem preenchido.
//
//   BUG #O (ajuste) — monitorPositions buscava monitorAccount de novo via
//     accountCache.get isoladamente após getMonitorClient, podendo divergir
//     da conta que de fato forneceu o client. getMonitorClient agora retorna
//     { client, account } juntos — fonte única, sem busca redundante.

import { createClient } from "@supabase/supabase-js";
import { TelegramClient, Api } from "telegram";
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
const LISTEN_POLL_MS                = 1000;
const OPEN_GROUP_LISTEN_TIMEOUT_MS  = 2 * 60 * 60_000;
const SEND_RETRY_BACKOFF_MAX_MS     = 8_000;

// v17 BUG #P: timeout externo por slot em dispatchToGroup para grupos abertos.
// Evita que uma conta lenta segure todas as outras no Promise.all.
// Deve ser <= SEND_TIMEOUT_MS (15s) para não mascarar falhas reais.
const DISPATCH_SLOT_TIMEOUT_MS      = 12_000;

// v15: sleep calibrado — invoke sai em scheduledAt - (ONE_WAY - TARGET)
// Mensagem chega ao servidor em scheduledAt + TARGET_ARRIVAL_OFFSET_MS.
// Calibrar ONE_WAY_RTT_ESTIMATE_MS com os logs [sniper][timing] após disparos reais.
// Formula: ONE_WAY ≈ (invoke_RTT_round_trip / 2)
const TARGET_ARRIVAL_OFFSET_MS      = 10;   // chega 10ms depois do horário agendado
const ONE_WAY_RTT_ESTIMATE_MS       = 23;   // RTT one-way medido VPS→Telegram DC

// SNIPER_BEFORE_MS: quanto antes do scheduledAt o setTimeout dispara.
// Deve ser > (ONE_WAY_RTT_ESTIMATE_MS - TARGET_ARRIVAL_OFFSET_MS) + margem.
// Com ONE_WAY=23 e TARGET=10: precisa de >13ms. 45ms dá margem confortável.
const SNIPER_BEFORE_MS              = 45;
const SNIPER_SEND_TIMEOUT_MS        = 800;
const SNIPER_ATTEMPT_INTERVAL_MS    = 1;
const SNIPER_PAUSE_EVERY_N          = 10;
const SNIPER_PAUSE_MS               = 5;
const SNIPER_BUDGET_MS              = RETRY_BUDGET_MS;
const SNIPER_DONE_BLOCK_TTL_MS      = 500;

// Monitor v12/v15: aguarda N segundos após disparo para capturar histórico completo
const MONITOR_POST_DISPATCH_WAIT_MS = 10_000;
const MONITOR_HISTORY_FETCH_LIMIT   = 200;
// Janela de tempo antes do disparo para buscar contexto (fetch mais amplo)
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
// v18: account.id (UUID) -> telegram numeric user id, resolvido via client.getMe()
// na primeira conexão. Necessário para casar mensagens no histórico por from_id.
const telegramUserIdCache   = new Map<string, string>();
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

    // v18: resolve e cacheia o telegram user id numérico desta conta.
    // Usado por resolveMsgIds para casar mensagens no histórico via from_id.
    if (!telegramUserIdCache.has(account.id)) {
      try {
        const me = await client.getMe();
        const meId = (me as any)?.id;
        if (meId != null) {
          telegramUserIdCache.set(account.id, String(meId));
        }
      } catch (err: any) {
        console.warn(`[client] Não foi possível resolver telegram user id de ${account.phone_number}: ${err.message}`);
      }
    }

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
   ENVIO COM RETRY INTERNO
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
   SNIPER — ENVIO ÚNICO COM PEER JÁ RESOLVIDO
   v15: sem scheduleDate — o timing é controlado pelo sleep calibrado no loop.
   peer é passado diretamente — resolvePeer NÃO é chamado aqui.
   ───────────────────────────────────────────────────────────────────────────── */
async function sniperSendOnce(
  client: TelegramClient,
  peer: unknown,
  messageText: string,
  randomId: bigInt.BigInteger,
): Promise<void> {
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
   SNIPER LOOP — GRUPOS FECHADOS
   v15: sleep calibrado substitui scheduleDate.
   Antes de cada invoke, cada slot dorme até:
     scheduledAt + TARGET_ARRIVAL_OFFSET_MS - ONE_WAY_RTT_ESTIMATE_MS
   Todas as contas dormem até o MESMO timestamp absoluto → disparam juntas.
   Resultado esperado: mensagem chega em scheduledAt + ~10ms, nunca antes.
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

    // v15: target de disparo — invoke sai aqui, chega em scheduledAt+TARGET
    const invokeFireAt    = scheduledAt + TARGET_ARRIVAL_OFFSET_MS - ONE_WAY_RTT_ESTIMATE_MS;

    console.log(`[sniper][timing] timer lag: ${timerLagMs}ms (SNIPER_BEFORE_MS=${SNIPER_BEFORE_MS}, ideal=0)`);
    console.log(
      `[sniper][timing] target fire: scheduledAt${invokeFireAt - scheduledAt >= 0 ? "+" : ""}${invokeFireAt - scheduledAt}ms ` +
      `→ chegada esperada: scheduledAt+${TARGET_ARRIVAL_OFFSET_MS}ms`
    );

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

    // ── Estado por conta ─────────────────────────────────────────────────
    type AccountState = "pending" | "sent" | "fatal";

    interface SlotState {
      member:     typeof members[number];
      account:    Account;
      state:      AccountState;
      randomId:   bigInt.BigInteger;
      client:     TelegramClient | null;
      peer:       unknown;
      sentAt:     Date | null;
      error:      string | undefined;
      attempts:   number;
      floodUntil: number;
    }

    // Pré-conecta todos os clientes E resolve todos os peers antes do loop.
    const slots: SlotState[] = await Promise.all(
      members.map(async (member) => {
        const account = member.accounts!;
        let client: TelegramClient | null = null;
        let peer: unknown = null;
        let state: AccountState = "pending";
        let error: string | undefined;

        try {
          client = await getClient(account);
          peer = await resolvePeer(client, chatId, account.id);
          console.log(`[sniper] ✓ Peer pré-resolvido: ${account.phone_number}`);
        } catch (err: any) {
          error = String(err?.message ?? "");
          state = "fatal";
          console.error(`[sniper] Falha ao preparar ${account.phone_number}: ${error}`);
        }

        return {
          member,
          account,
          state,
          randomId:   makeRandomId(),
          client,
          peer,
          sentAt:     null,
          error,
          attempts:   0,
          floodUntil: 0,
        } satisfies SlotState;
      })
    );

    // ── Loop unificado ────────────────────────────────────────────────────
    let globalAttempt = 0;

    while (Date.now() < budgetEnd) {
      const pendingSlots = slots.filter(s => s.state === "pending");
      if (pendingSlots.length === 0) break;

      globalAttempt++;

      const now2 = Date.now();

      // v19: sleep calibrado movido para FORA do Promise.allSettled.
      // Quando dois snipers rodam em paralelo no mesmo processo, o sleep interno
      // cederia o event loop para o outro sniper, causando atraso no segundo disparo.
      // Ao dormir aqui (antes do allSettled), todos os invokes saem no mesmo tick.
      // Apenas na primeira iteração (globalAttempt === 1) o sleep faz sentido;
      // nas retentativas (flood/timeout), disparamos imediatamente.
      if (globalAttempt === 1) {
        const preSleepMs = invokeFireAt - Date.now();
        if (preSleepMs > 0) {
          await new Promise(r => setTimeout(r, preSleepMs));
        }
      }

      await Promise.allSettled(
        pendingSlots.map(async (slot) => {
          if (slot.floodUntil > now2) return;

          if (!slot.peer) {
            try {
              slot.peer = await resolvePeer(slot.client!, chatId, slot.account.id);
            } catch (err: any) {
              slot.state = "fatal";
              slot.error = String(err?.message ?? "");
              console.error(`[sniper] Falha ao re-resolver peer ${slot.account.phone_number}: ${slot.error}`);
              return;
            }
          }

          // Sem sleep aqui — o sleep foi feito antes do allSettled (v19).
          // Todos os slots entram no invoke ao mesmo tempo, no mesmo tick do event loop.

          slot.attempts++;
          const invokeStartMs = Date.now();

          try {
            await sniperSendOnce(
              slot.client!,
              slot.peer,
              slot.member.message_text ?? "",
              slot.randomId,
            );

            slot.sentAt = new Date();
            slot.state  = "sent";

            const ackRttMs    = slot.sentAt.getTime() - invokeStartMs;
            const vsHorarioMs = slot.sentAt.getTime() - scheduledAt;
            console.log(
              `[sniper][timing] ${slot.account.phone_number} — ` +
              `ACK RTT: ${ackRttMs}ms | ` +
              `vs horário ACK: ${vsHorarioMs > 0 ? "+" : ""}${vsHorarioMs}ms | ` +
              `chegada estimada: +${TARGET_ARRIVAL_OFFSET_MS}ms | ` +
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
              slot.peer = null;
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

    // ── Coleta resultados finais ──────────────────────────────────────────
    const results: DispatchResult[] = slots.map(slot => {
      if (slot.state === "sent") {
        return {
          account_id:   slot.account.id,
          message_text: slot.member.message_text,
          status:       "sent",
          retryable:    false,
        };
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

      return {
        account_id:   slot.account.id,
        message_text: slot.member.message_text,
        status:       "failed",
        retryable,
        error:        errMsg,
      };
    });

    // ── Log de dispatch ───────────────────────────────────────────────────
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

    // ── Monitoramento de posições ─────────────────────────────────────────
    const sentSlots   = slots.filter(s => s.state === "sent");
    const dispatchRef = sentSlots.length > 0 ? new Date(scheduledAt) : now;

    if (sentSlots.length > 0) {
      const sentForMonitor = sentSlots.map(s => ({
        account_id:   s.account.id,
        message_text: s.member.message_text ?? "",
      })).filter(s => s.message_text);

      if (sentForMonitor.length > 0) {
        monitorPositions(chatId, sentForMonitor, scheduleId, dispatchRef, "closed")
          .catch(err => console.error("[sniper][monitor] Erro:", err.message));
      }
    }

    // ── Atualiza schedule ─────────────────────────────────────────────────
    await updateScheduleAfterDispatch(schedule, results, dispatchRef, "closed");

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
   v17: recebe myMsgIds Map<account_id, msg_id> para identificar a mensagem
   exata de cada conta no histórico, sem depender só do texto.
   Isso corrige o BUG #N onde duas contas com o mesmo texto pegavam o mesmo msg_id.
   ───────────────────────────────────────────────────────────────────────────── */
async function _savePositions(
  sentMembers: Array<{ account_id: string; message_text: string }>,
  windowMsgs: any[],
  auctionStartUnix: number,
  scheduleId: string,
  dispatchedAt: Date,
  myMsgIds: Map<string, number>,  // v17: account_id → msg_id resolvido pelo caller
): Promise<void> {
  const cutoff = new Date(dispatchedAt.getTime() - 60_000).toISOString();

  await Promise.allSettled(sentMembers.map(sm => {
    if (!sm.message_text) return;

    // v17: usa o msg_id pré-resolvido por conta (sem ambiguidade de texto duplicado)
    const myMsgId = myMsgIds.get(sm.account_id);

    if (myMsgId == null) {
      console.warn(
        `[monitor] msg_id não resolvido para conta ${sm.account_id} ` +
        `(auctionStartUnix=${auctionStartUnix})`
      );
      return;
    }

    const position = windowMsgs.filter(
      (m: any) => (m.id as number) < myMsgId && (m.date as number) >= auctionStartUnix
    ).length + 1;

    console.log(
      `[monitor] ${sm.account_id}: posição #${position} ` +
      `(msg_id=${myMsgId}, msgs na janela do leilão=${
        windowMsgs.filter((m: any) => (m.date as number) >= auctionStartUnix).length
      })`
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

/* ─────────────────────────────────────────────────────────────────────────────
   RESOLVE MSG_IDS POR CONTA NO HISTÓRICO
   v17: cruza o histórico com from_id para identificar a mensagem exata de cada
   conta. Fallback: texto (sem reutilizar o mesmo msg_id para contas diferentes).
   ───────────────────────────────────────────────────────────────────────────── */
function extractFromUserId(m: any): string | null {
  // GramJS: m.fromId pode ser PeerUser { userId }, PeerChannel, PeerChat, ou ausente
  // (mensagens anônimas de admin usam peerId do canal/grupo em vez de fromId).
  const fromId = m.fromId ?? m.from_id;
  if (fromId?.userId != null) return String(fromId.userId);
  if (fromId?.user_id != null) return String(fromId.user_id);

  // Fallback: out === true + peerId do tipo user (DMs ou casos atípicos)
  const peerId = m.peerId ?? m.peer_id;
  if (m.out && (peerId?.userId != null || peerId?.user_id != null)) {
    return String(peerId.userId ?? peerId.user_id);
  }

  return null;
}

function resolveMsgIds(
  sentMembers: Array<{ account_id: string; message_text: string }>,
  windowMsgs: any[],
  auctionStartUnix: number,
): Map<string, number> {
  const result = new Map<string, number>();
  const usedMsgIds = new Set<number>();

  // 1. Resolução primária: casa por telegram_user_id (from_id) da mensagem.
  //    Mais precisa — funciona mesmo com textos idênticos entre contas.
  for (const sm of sentMembers) {
    const telegramUserId = telegramUserIdCache.get(sm.account_id);
    if (!telegramUserId) continue;

    const match = windowMsgs.find(
      (m: any) =>
        (m.date as number) >= auctionStartUnix &&
        !usedMsgIds.has(m.id as number) &&
        extractFromUserId(m) === telegramUserId
    );

    if (match) {
      result.set(sm.account_id, match.id as number);
      usedMsgIds.add(match.id as number);
      console.log(`[monitor] ${sm.account_id}: msg_id=${match.id} resolvido via from_id (telegram_user_id=${telegramUserId})`);
    }
  }

  // 2. Fallback: por texto, para contas sem telegram_user_id cacheado ou cuja
  //    mensagem não trouxe from_id (admin anônimo/canal). Nunca reusa um msg_id
  //    já atribuído na etapa 1 ou nesta etapa.
  for (const sm of sentMembers) {
    if (result.has(sm.account_id)) continue;

    const match = windowMsgs.find(
      (m: any) =>
        m.message === sm.message_text &&
        (m.date as number) >= auctionStartUnix &&
        !usedMsgIds.has(m.id as number)
    );

    if (match) {
      result.set(sm.account_id, match.id as number);
      usedMsgIds.add(match.id as number);
      console.warn(`[monitor] ${sm.account_id}: msg_id=${match.id} resolvido via fallback de texto (from_id indisponível)`);
    }
  }

  return result;
}

/* ─────────────────────────────────────────────────────────────────────────────
   BUSCA CLIENT VÁLIDO PARA MONITOR
   v17 BUG #O: busca conta no Supabase como fallback quando não está no cache.
   Garante que o monitor sempre tenha um client para buscar histórico,
   mesmo para contas adicionadas depois do boot (sem prewarm).
   ───────────────────────────────────────────────────────────────────────────── */
async function getMonitorClient(
  sentMembers: Array<{ account_id: string; message_text: string }>
): Promise<{ client: TelegramClient; account: Account } | null> {
  // v18: retorna { client, account } juntos — elimina busca redundante depois.
  // Tenta cada conta da lista até conseguir um client válido
  for (const sm of sentMembers) {
    // 1. Tenta do cache em memória
    let account = accountCache.get(sm.account_id);

    // 2. Fallback: busca no Supabase (BUG #O fix)
    if (!account) {
      try {
        const { data } = await supabase
          .from("accounts")
          .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
          .eq("id", sm.account_id)
          .eq("is_active", true)
          .single();
        if (data) {
          account = data as Account;
          accountCache.set(account.id, account); // popula cache para próximos usos
        }
      } catch {}
    }

    if (!account) continue;

    try {
      const client = await getClient(account);
      if (client.connected) return { client, account };
    } catch {}
  }

  return null;
}

/* ─────────────────────────────────────────────────────────────────────────────
   MONITORAMENTO DE POSIÇÃO (v12/v15/v17)
   ───────────────────────────────────────────────────────────────────────────── */
async function monitorPositions(
  telegramChatId: string,
  sentMembers: Array<{ account_id: string; message_text: string }>,
  scheduleId: string,
  dispatchedAt: Date,
  groupType: "open" | "closed"
): Promise<void> {
  if (sentMembers.length === 0) return;

  // v17 BUG #O: usa getMonitorClient em vez de só accountCache.get
  // v18: getMonitorClient retorna { client, account } juntos — sem busca redundante
  const monitorResult = await getMonitorClient(sentMembers);
  if (!monitorResult) {
    console.warn("[monitor] Nenhum client disponível — ignorando monitoramento");
    return;
  }
  const { client, account: monitorAccount } = monitorResult;

  const windowStartUnix = Math.floor(
    (dispatchedAt.getTime() - MONITOR_WINDOW_BEFORE_MS) / 1000
  );

  if (groupType === "closed") {
    console.log(
      `[monitor] ⏳ Aguardando ${MONITOR_POST_DISPATCH_WAIT_MS / 1000}s ` +
      `para capturar histórico completo do leilão (schedule ${scheduleId})`
    );
    await new Promise(r => setTimeout(r, MONITOR_POST_DISPATCH_WAIT_MS));

    try {
      const peer   = await resolvePeer(client, telegramChatId, monitorAccount.id);
      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer:       peer as any,
          limit:      MONITOR_HISTORY_FETCH_LIMIT,
          offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
          hash:       bigInt(0), addOffset: 0,
        })
      ) as any;

      // v20: GramJS retorna className ("Message"), não m._ ("message").
      // O filtro anterior rejeitava 100% das mensagens causando "Nenhuma mensagem na janela".
      // Aceita tanto className quanto m._ para compatibilidade com diferentes versões do GramJS.
      // windowStartUnix expandido para 30s antes para absorver skew de clock VPS/Telegram.
      const windowMsgs: any[] = (result.messages ?? [])
        .filter((m: any) => {
          const isMsg = m.className === "Message" || m._ === "message" || m.message != null;
          const inWindow = (m.date as number) >= windowStartUnix - 30;
          return isMsg && inWindow;
        })
        .sort((a: any, b: any) => (a.id as number) - (b.id as number));

      console.log(
        `[monitor][debug] Total msgs no GetHistory: ${result.messages?.length ?? 0}, ` +
        `na janela: ${windowMsgs.length}, ` +
        `windowStartUnix=${windowStartUnix}, ` +
        `primeira msg date=${result.messages?.[0]?.date ?? "N/A"} ` +
        `(schedule ${scheduleId})`
      );

      if (windowMsgs.length === 0) {
        console.warn(`[monitor] Nenhuma mensagem na janela (closed) — schedule ${scheduleId}`);
        return;
      }

      console.log(
        `[monitor] ${windowMsgs.length} msg(s) na janela para schedule ${scheduleId} (closed)`
      );

      const auctionStartUnix = Math.floor(dispatchedAt.getTime() / 1000);

      // v17 BUG #N: resolve msg_id por conta antes de salvar posições
      const myMsgIds = resolveMsgIds(sentMembers, windowMsgs, auctionStartUnix);
      await _savePositions(sentMembers, windowMsgs, auctionStartUnix, scheduleId, dispatchedAt, myMsgIds);

    } catch (err: any) {
      console.warn(`[monitor] Erro ao buscar histórico (closed): ${err.message}`);
    }
    return;
  }

  // ── Grupo aberto: polling ─────────────────────────────────────────────
  const ourTexts = new Set(sentMembers.map(m => m.message_text).filter(Boolean));
  const deadline = Date.now() + MONITOR_MAX_OPEN_MS;
  const auctionStartUnix = windowStartUnix;

  console.log(`[monitor] Iniciando polling para schedule ${scheduleId} (open)`);

  while (Date.now() < deadline) {
    try {
      const peer   = await resolvePeer(client, telegramChatId, monitorAccount.id);
      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer:       peer as any,
          limit:      MONITOR_HISTORY_FETCH_LIMIT,
          offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
          hash:       bigInt(0), addOffset: 0,
        })
      ) as any;

      // v20: mesmo fix do closed — aceita className ou m._ ou presença de m.message
      const windowMsgs: any[] = (result.messages ?? [])
        .filter((m: any) => {
          const isMsg = m.className === "Message" || m._ === "message" || m.message != null;
          const inWindow = (m.date as number) >= windowStartUnix - 30;
          return isMsg && inWindow;
        })
        .sort((a: any, b: any) => (a.id as number) - (b.id as number));

      if (windowMsgs.length === 0 || !windowMsgs.some((m: any) => ourTexts.has(m.message))) {
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      // v17 BUG #N: resolve msg_id por conta antes de salvar posições
      const myMsgIds = resolveMsgIds(sentMembers, windowMsgs, auctionStartUnix);
      await _savePositions(sentMembers, windowMsgs, auctionStartUnix, scheduleId, dispatchedAt, myMsgIds);
      return;

    } catch (err: any) {
      console.warn(`[monitor] Erro ao buscar histórico (open): ${err.message}`);
      await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
    }
  }

  console.warn(`[monitor] Timeout — posições não registradas para schedule ${scheduleId}`);
}

/* ─────────────────────────────────────────────────────────────────────────────
   LISTENER DE GRUPO ABERTO
   v16: gotSignal aceita qualquer texto não vazio OU mídia (emoji, figurinha, etc.)
   ───────────────────────────────────────────────────────────────────────────── */
function startGroupListener(schedule: Schedule, group: Group, account: Account): void {
  const existing = listenMap.get(group.id);
  if (existing) existing.abort();

  const ctrl = new AbortController();
  listenMap.set(group.id, ctrl);

  const deadline    = Date.now() + OPEN_GROUP_LISTEN_TIMEOUT_MS;
  const startUnix   = Math.floor((Date.now() - 10_000) / 1000);
  let lastSeenMsgId = 0;

  console.log(`[listen] 👂 Aguardando sinal do admin em ${group.telegram_chat_id} para schedule ${schedule.id}`);

  (async () => {
    try {
      let client = await getClient(account).catch(() => null);
      if (!client) {
        console.warn(`[listen] Sem client — abortando listener para ${schedule.id}`);
        listenMap.delete(group.id);
        return;
      }

      try { await resolvePeer(client, group.telegram_chat_id!, account.id); } catch {}

      while (Date.now() < deadline && !ctrl.signal.aborted) {
        try {
          if (!client.connected) {
            console.warn(`[listen] Client desconectou — reconectando para ${schedule.id}`);
            client = await getClient(account);
            try { await resolvePeer(client, group.telegram_chat_id!, account.id); } catch {}
          }

          const peer   = await resolvePeer(client, group.telegram_chat_id!, account.id);
          const result = await client.invoke(
            new Api.messages.GetHistory({
              peer: peer as any, limit: 10,
              offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
              hash: bigInt(0), addOffset: 0,
            })
          ) as any;

          const recentMsgs = (result.messages ?? []).filter(
            (m: any) =>
              (m.className === "Message" || m._ === "message") &&
              m.date >= startUnix &&
              m.id > lastSeenMsgId
          );
          if (recentMsgs.length > 0) {
            lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
          }

          // v16: aceita qualquer texto não vazio (inclui emoji) OU qualquer mídia
          // (figurinha, foto, gif, sticker, etc.)
          const gotSignal = recentMsgs.some((m: any) => {
            const hasText = typeof m.message === "string" && m.message.trim().length > 0;
            const isMedia = m.media != null && m.media.className !== "MessageMediaEmpty";
            return hasText || isMedia;
          });

          if (gotSignal && !ctrl.signal.aborted) {
            console.log(`[listen] ✓ Sinal detectado — disparando schedule ${schedule.id}`);
            listenMap.delete(group.id);

            const dispatchedAt = new Date();
            const alreadySent  = await getAlreadySentIds(schedule);
            const results      = await dispatchToGroup(schedule, group, alreadySent);

            const sentForMonitor = results
              .filter(r => r.status === "sent")
              .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
              .filter(r => r.message_text);
            if (sentForMonitor.length > 0) {
              monitorPositions(group.telegram_chat_id!, sentForMonitor, schedule.id, dispatchedAt, "open")
                .catch(err => console.error("[listen] Erro no monitoramento:", err.message));
            }

            await updateScheduleAfterDispatch(schedule, results, dispatchedAt, "open");
            return;
          }

        } catch (err: any) {
          if (!ctrl.signal.aborted) {
            console.warn(`[listen] Erro ao buscar histórico (${schedule.id}): ${err.message}`);
            await new Promise(r => setTimeout(r, 2_000));
          }
        }

        if (!ctrl.signal.aborted) await new Promise(r => setTimeout(r, LISTEN_POLL_MS));
      }

      listenMap.delete(group.id);

      if (ctrl.signal.aborted) {
        console.log(`[listen] ⏹ Listener abortado para schedule ${schedule.id}`);
        return;
      }

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

    } catch (err: any) {
      console.error(`[listen] Erro inesperado para schedule ${schedule.id}:`, err.message);
      listenMap.delete(group.id);
    }
  })();
}

/* ─────────────────────────────────────────────────────────────────────────────
   DESPACHO PARA O GRUPO
   v17 BUG #P: cada slot tem timeout externo de DISPATCH_SLOT_TIMEOUT_MS.
   Uma conta lenta não bloqueia as outras no Promise.all.
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
      return {
        account_id:   account.id,
        message_text: member.message_text,
        status:       "skipped" as const,
        retryable:    false,
      };
    }

    let status: "sent" | "failed" = "failed";
    let error: string | undefined;
    let retryable = false;

    try {
      // v17 BUG #P: wraps o sendMessage com timeout externo por slot.
      // Impede que backoff interno (até 8s) de uma conta atrase as demais.
      const client = await getClient(account);
      await Promise.race([
        sendMessage(client, account, group.telegram_chat_id!, member.message_text ?? ""),
        new Promise<never>((_, r) =>
          setTimeout(
            () => r(new Error(`DISPATCH_SLOT_TIMEOUT após ${DISPATCH_SLOT_TIMEOUT_MS}ms`)),
            DISPATCH_SLOT_TIMEOUT_MS
          )
        ),
      ]);
      status = "sent";
      alreadySent.add(account.id);
      console.log(`[dispatch] ✓ ${account.phone_number}`);
    } catch (err) {
      error     = err instanceof Error ? err.message : String(err);
      retryable = isRetryableError(error);
      console.error(
        `[dispatch] ✗ ${account.phone_number} [${retryable ? "retryável" : "permanente"}]: ${error}`
      );
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

      // Conta listener: a primeira com message_text configurado e ativa
      const listenerAccount = (group.group_members ?? [])
        .filter(m => m.is_active && m.accounts?.is_active && m.message_text)
        .sort((a, b) => a.position - b.position)[0]?.accounts ?? null;

      if (!listenerAccount) {
        console.warn(`[fire] Nenhuma conta com mensagem configurada no grupo — abortando.`);
        return;
      }

      startGroupListener(schedule, group, listenerAccount as Account);

      await supabase.from("schedules").update({
        retry_until:         new Date(now.getTime() + OPEN_GROUP_LISTEN_TIMEOUT_MS).toISOString(),
        last_attempt_at:     now.toISOString(),
        last_attempt_status: "waiting_admin",
        last_attempt_error:  null,
      }).eq("id", scheduleId);
      return;
    }

    // Grupo fechado chegou aqui sem sniper (retry via banco, boot tardio, etc.)
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
      monitorPositions(
        group.telegram_chat_id,
        sentForMonitor,
        scheduleId,
        now,
        group.group_type ?? "closed"
      ).catch(err => console.error("[monitor] Erro não capturado:", err.message));
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
   Endpoints disponíveis:
     GET  /accounts/:id/chats
     GET  /accounts/:id/chat-count?chat_id=...
     GET  /accounts/:id/chat-members?chat_id=...
     POST /accounts/:id/reload
     DELETE /groups/:id/listen   — aborta listener ativo
     POST /groups/:id/dispatch   — disparo manual (send_to_self opcional)
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

  // GET /accounts/:id/chats
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

  // GET /accounts/:id/chat-count
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

  // GET /accounts/:id/chat-members
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

  // POST /accounts/:id/reload
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

  // DELETE /groups/:id/listen — aborta listener ativo de grupo aberto
  const listenMatch = url.pathname.match(/^\/groups\/([^/]+)\/listen$/);
  if (listenMatch && req.method === "DELETE") {
    const groupId = listenMatch[1];
    const ctrl = listenMap.get(groupId);
    if (ctrl) { ctrl.abort(); listenMap.delete(groupId); }
    return jsonResponse(res, 200, { ok: true });
  }

  // POST /groups/:id/dispatch — disparo manual (aquecimento ou envio direto)
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
