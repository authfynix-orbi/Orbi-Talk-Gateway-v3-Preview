import express from "express";
import cors from "cors";
import { createServer as createViteServer } from "vite";
import path from "path";
import { fileURLToPath } from "url";
import admin from "firebase-admin";
import { WebSocketServer, WebSocket } from "ws";
import http from "http";
import crypto from "crypto";
import os from "os";
import nodemailer from "nodemailer";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Track last heartbeat time per device
const lastHeartbeat = new Map<string, number>();
const lastHeartbeatWrite = new Map<string, number>();
const deviceSentCountCache = new Map<string, { totalSent: number; fetchedAt: number }>();
const lastPersistedDeviceState = new Map<string, {
  status: string;
  batteryLevel: number | null;
  model: string | null;
  fcmToken: string | null;
  ownerUid: string | null;
}>();

function envInteger(name: string, fallback: number, minimum: number) {
  const parsed = Number.parseInt(String(process.env[name] || ""), 10);
  return Number.isFinite(parsed) ? Math.max(parsed, minimum) : fallback;
}

function envEnabled(name: string, fallback = true) {
  const value = String(process.env[name] || "").trim().toLowerCase();
  if (!value) return fallback;
  return !["0", "false", "no", "off", "disabled"].includes(value);
}

const HEARTBEAT_TIMEOUT_MS = 90000;    // 90 seconds - tolerate 30s client heartbeat cadence with network jitter
const STALE_CHECK_INTERVAL_MS = 30000; // How often to check for stale devices
const HEARTBEAT_WRITE_INTERVAL_MS = 5 * 60 * 1000;
const HEARTBEAT_BATTERY_WRITE_DELTA = 5;
const DEVICE_SENT_COUNT_CACHE_MS = 60 * 1000;
const STALE_PROCESSING_TIMEOUT_MS = 120000; // Requeue jobs stuck in processing for 2+ minutes
const STALE_PROCESSING_CHECK_INTERVAL_MS = envInteger("ORBI_TALK_STALE_RECOVERY_INTERVAL_MS", 300000, 60000);
const STALE_PROCESSING_BATCH_SIZE = envInteger("ORBI_TALK_STALE_RECOVERY_BATCH_SIZE", 50, 1);
const AUTO_DISPATCH_INTERVAL_MS = envInteger("ORBI_TALK_AUTO_DISPATCH_INTERVAL_MS", 300000, 60000);
const AUTO_DISPATCH_RETRY_COOLDOWN_MS = 60000;
const AUTO_DISPATCH_BATCH_SIZE = envInteger("ORBI_TALK_AUTO_DISPATCH_BATCH_SIZE", 50, 1);
const AUTO_DISPATCH_ENABLED = envEnabled("ORBI_TALK_AUTO_DISPATCH_ENABLED");
const STALE_RECOVERY_ENABLED = envEnabled("ORBI_TALK_STALE_RECOVERY_ENABLED");
const FIRESTORE_QUOTA_BACKOFF_BASE_MS = envInteger("ORBI_TALK_FIRESTORE_QUOTA_BACKOFF_MS", 900000, 60000);
const FIRESTORE_QUOTA_BACKOFF_MAX_MS = 6 * 60 * 60 * 1000;
const FORCE_RESEND_BATCH_SIZE = 250;
const DEVICE_ONLINE_FLUSH_BATCH_SIZE = 50;
const CANONICAL_TALK_GATEWAY_BASE_URL = "https://talk.orbifinancial.com";
const LEGACY_MESSAGING_GATEWAY_HOSTS = new Set([
  "gateway.orbifinancial.com",
  "www.gateway.orbifinancial.com",
]);
const SERVICE_VERSION = process.env.ORBI_TALK_GATEWAY_VERSION?.trim()
  || process.env.ORBI_GATEWAY_VERSION?.trim()
  || process.env.npm_package_version?.trim()
  || "0.0.0";

const PAY_GATEWAY_DELIVERY_CALLBACK_URL = String(process.env.ORBI_PAY_NOTIFICATION_CALLBACK_URL || "").trim();
const PAY_GATEWAY_DELIVERY_CALLBACK_SECRET = String(process.env.ORBI_PAY_NOTIFICATION_CALLBACK_SECRET || "").trim();
const DELIVERY_CALLBACK_INTERVAL_MS = envInteger("ORBI_PAY_NOTIFICATION_CALLBACK_INTERVAL_MS", 30000, 5000);
const callbackSignature = (body: Record<string, unknown>, timestamp: string, nonce: string) => { const stable=(v:any):string=>v==null?"":typeof v!=="object"?JSON.stringify(v):Array.isArray(v)?`[${v.map(stable).join(",")}]`:`{${Object.entries(v).filter(([,x])=>x!==undefined).sort(([a],[b])=>a.localeCompare(b)).map(([k,x])=>`${JSON.stringify(k)}:${stable(x)}`).join(",")}}`;const digest=crypto.createHash("sha256").update(stable(body)).digest("hex");return crypto.createHmac("sha256",PAY_GATEWAY_DELIVERY_CALLBACK_SECRET).update(`${timestamp}.${nonce}.${digest}`).digest("hex");};
async function deliverPayGatewayCallback(payload:Record<string,unknown>){const timestamp=new Date().toISOString(),nonce=crypto.randomUUID();const response=await fetch(PAY_GATEWAY_DELIVERY_CALLBACK_URL,{method:"POST",headers:{"content-type":"application/json","x-orbi-talk-timestamp":timestamp,"x-orbi-talk-nonce":nonce,"x-orbi-talk-signature":`sha256=${callbackSignature(payload,timestamp,nonce)}`,"x-orbi-environment":String(process.env.ORBI_TALK_ENVIRONMENT||"live")},body:JSON.stringify(payload)});if(!response.ok)throw Error(`PAY_GATEWAY_CALLBACK_HTTP_${response.status}`);}
async function enqueuePayGatewayDeliveryCallback(messageId:string,status:string,reason?:string){if(!PAY_GATEWAY_DELIVERY_CALLBACK_URL||PAY_GATEWAY_DELIVERY_CALLBACK_SECRET.length<32)return;const normalized=status==="delivered"?"delivered":status==="failed"?"failed":"";if(!normalized)return;const eventId=`talk_delivery_${crypto.createHash("sha256").update(`${messageId}:${normalized}`).digest("hex").slice(0,32)}`,payload={eventId,providerMessageId:messageId,status:normalized,...(reason?{reason:String(reason).slice(0,500)}:{})},adminApp=getFirebaseAdmin();if(!adminApp)return deliverPayGatewayCallback(payload);const ref=adminApp.firestore().collection("pay_gateway_delivery_callback_outbox").doc(eventId);try{await ref.create({payload,status:"pending",attempts:0,nextAttemptAt:admin.firestore.Timestamp.now(),createdAt:admin.firestore.FieldValue.serverTimestamp(),updatedAt:admin.firestore.FieldValue.serverTimestamp()});}catch(e:any){if(Number(e?.code)!==6&&String(e?.code)!=="already-exists")throw e;}}
async function flushPayGatewayDeliveryCallbacks(){const adminApp=getFirebaseAdmin();if(!adminApp||!PAY_GATEWAY_DELIVERY_CALLBACK_URL)return;const snapshot=await adminApp.firestore().collection("pay_gateway_delivery_callback_outbox").where("status","==","pending").limit(50).get();for(const doc of snapshot.docs){const d=doc.data(),attempts=Number(d.attempts||0);if(d.nextAttemptAt?.toMillis?.()>Date.now())continue;try{await deliverPayGatewayCallback(d.payload||{});await doc.ref.update({status:"delivered",deliveredAt:admin.firestore.FieldValue.serverTimestamp(),updatedAt:admin.firestore.FieldValue.serverTimestamp()});}catch(e:any){await doc.ref.update({attempts:attempts+1,lastError:String(e?.message||e).slice(0,300),nextAttemptAt:admin.firestore.Timestamp.fromMillis(Date.now()+Math.min(3600,2**(attempts+1)*30)*1000),updatedAt:admin.firestore.FieldValue.serverTimestamp()});}}}

let firestoreQuotaBackoffUntil = 0;
let firestoreQuotaFailureCount = 0;

function isFirestoreQuotaError(error: any) {
  return error?.code === 8
    || String(error?.code || "").toUpperCase() === "RESOURCE_EXHAUSTED"
    || /RESOURCE_EXHAUSTED|quota exceeded/i.test(String(error?.message || error?.details || ""));
}

function registerFirestoreQuotaBackoff(scope: string, error: any) {
  if (!isFirestoreQuotaError(error)) {
    return false;
  }
  firestoreQuotaFailureCount += 1;
  const backoffMs = Math.min(
    FIRESTORE_QUOTA_BACKOFF_BASE_MS * (2 ** Math.min(firestoreQuotaFailureCount - 1, 5)),
    FIRESTORE_QUOTA_BACKOFF_MAX_MS,
  );
  firestoreQuotaBackoffUntil = Date.now() + backoffMs;
  console.warn(
    `[${scope}] Firestore quota exhausted; background reads paused for ${Math.ceil(backoffMs / 60000)} minute(s).`,
  );
  return true;
}

function clearFirestoreQuotaBackoff() {
  firestoreQuotaBackoffUntil = 0;
  firestoreQuotaFailureCount = 0;
}

function normalizeTalkGatewayBaseUrl(rawUrl: string | undefined | null) {
  const value = rawUrl?.trim();
  if (!value) {
    return "";
  }

  try {
    const parsed = new URL(value);
    if (LEGACY_MESSAGING_GATEWAY_HOSTS.has(parsed.hostname.toLowerCase())) {
      return "";
    }
    parsed.pathname = "/";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString().replace(/\/$/, "");
  } catch {
    return "";
  }
}

function resolveTalkGatewayBaseUrl(fallbackBaseUrl = "") {
  return (
    normalizeTalkGatewayBaseUrl(process.env.ORBI_TALK_GATEWAY_URL)
    || normalizeTalkGatewayBaseUrl(process.env.APP_URL)
    || normalizeTalkGatewayBaseUrl(process.env.VITE_APP_URL)
    || normalizeTalkGatewayBaseUrl(process.env.PUBLIC_GATEWAY_BASE_URL)
    || normalizeTalkGatewayBaseUrl(fallbackBaseUrl)
    || CANONICAL_TALK_GATEWAY_BASE_URL
  );
}

function firestoreTimestampToMillis(value: any) {
  if (!value) {
    return 0;
  }
  if (typeof value.toMillis === "function") {
    return value.toMillis();
  }
  if (typeof value.seconds === "number") {
    return value.seconds * 1000;
  }
  if (typeof value === "number") {
    return value;
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

// Lazy initialization for Firebase Admin
let firebaseAdminApp: admin.app.App | null = null;

function getFirebaseAdmin() {
  if (!firebaseAdminApp) {
    const serviceAccountVar = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
    if (serviceAccountVar) {
      try {
        const serviceAccount = JSON.parse(serviceAccountVar);
        firebaseAdminApp = admin.initializeApp({
          credential: admin.credential.cert(serviceAccount),
        });
        // Enable ignoreUndefinedProperties
        firebaseAdminApp.firestore().settings({ ignoreUndefinedProperties: true });
        console.log("Firebase Admin initialized successfully.");
      } catch (error) {
        console.error("Failed to parse FIREBASE_SERVICE_ACCOUNT_KEY:", error);
      }
    } else {
      console.warn("FIREBASE_SERVICE_ACCOUNT_KEY not found. Server-side Firebase features will be limited.");
    }
  }
  return firebaseAdminApp;
}

// Activity Logging Helper
async function logActivity(type: string, details: string, deviceId: string | null = null, ownerUid: string | null = null) {
  const payload = {
    ts: new Date().toISOString(),
    scope: "activity",
    type,
    details,
    deviceId,
    ownerUid,
  };
  console.log(`[ORBI_ACTIVITY] ${JSON.stringify(payload)}`);
}

function logTrace(stage: string, metadata: Record<string, unknown> = {}) {
  const payload = {
    ts: new Date().toISOString(),
    scope: "trace",
    stage,
    ...metadata,
  };
  console.log(`[ORBI_TRACE] ${JSON.stringify(payload)}`);
}

function logErrorTrace(stage: string, error: unknown, metadata: Record<string, unknown> = {}) {
  const normalizedError = error instanceof Error
    ? {
        name: error.name,
        message: error.message,
        stack: error.stack,
      }
    : { message: String(error) };
  const payload = {
    ts: new Date().toISOString(),
    scope: "trace_error",
    stage,
    ...metadata,
    error: normalizedError,
  };
  console.error(`[ORBI_TRACE_ERROR] ${JSON.stringify(payload)}`);
}

function normalizeOptionalString(value: unknown) {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim();
  return normalized ? normalized : null;
}

function normalizeBatteryLevel(value: unknown) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

type DevicePresenceState = {
  status: string;
  batteryLevel: number | null;
  model: string | null;
  fcmToken: string | null;
  ownerUid: string | null;
};

function buildDevicePresenceState(input: {
  status?: unknown;
  batteryLevel?: unknown;
  model?: unknown;
  fcmToken?: unknown;
  ownerUid?: unknown;
}): DevicePresenceState {
  return {
    status: normalizeOptionalString(input.status) || "online",
    batteryLevel: normalizeBatteryLevel(input.batteryLevel),
    model: normalizeOptionalString(input.model),
    fcmToken: normalizeOptionalString(input.fcmToken),
    ownerUid: normalizeOptionalString(input.ownerUid),
  };
}

function shouldPersistDevicePresence(deviceId: string, nextState: DevicePresenceState, force = false) {
  if (force) {
    return true;
  }

  const now = Date.now();
  const previousState = lastPersistedDeviceState.get(deviceId);
  const lastWriteAt = lastHeartbeatWrite.get(deviceId) || 0;

  if (!previousState) {
    return true;
  }

  if (previousState.status !== nextState.status) {
    return true;
  }

  if (previousState.model !== nextState.model) {
    return true;
  }

  if (previousState.fcmToken !== nextState.fcmToken) {
    return true;
  }

  if (previousState.ownerUid !== nextState.ownerUid) {
    return true;
  }

  if (
    nextState.batteryLevel != null &&
    (
      previousState.batteryLevel == null ||
      Math.abs(previousState.batteryLevel - nextState.batteryLevel) >= HEARTBEAT_BATTERY_WRITE_DELTA
    )
  ) {
    return true;
  }

  return now - lastWriteAt >= HEARTBEAT_WRITE_INTERVAL_MS;
}

function markDevicePresencePersisted(deviceId: string, state: DevicePresenceState) {
  lastHeartbeatWrite.set(deviceId, Date.now());
  lastPersistedDeviceState.set(deviceId, state);
}

function buildLiveDeviceStatus(deviceId: string, isSocketConnected: boolean) {
  const lastBeat = lastHeartbeat.get(deviceId) || 0;
  const lastWriteAt = lastHeartbeatWrite.get(deviceId) || 0;
  const lastPersistedState = lastPersistedDeviceState.get(deviceId);
  const sentCountCache = deviceSentCountCache.get(deviceId);
  const now = Date.now();
  const heartbeatAgeMs = lastBeat > 0 ? now - lastBeat : null;
  const isAlive = heartbeatAgeMs != null && heartbeatAgeMs <= HEARTBEAT_TIMEOUT_MS;

  return {
    deviceId,
    status: isAlive ? (lastPersistedState?.status || "online") : "offline",
    liveConnected: isSocketConnected,
    heartbeatAlive: isAlive,
    lastHeartbeatAt: lastBeat > 0 ? new Date(lastBeat).toISOString() : null,
    heartbeatAgeMs,
    heartbeatAgeSeconds: heartbeatAgeMs == null ? null : Math.round(heartbeatAgeMs / 1000),
    lastPersistedAt: lastWriteAt > 0 ? new Date(lastWriteAt).toISOString() : null,
    batteryLevel: lastPersistedState?.batteryLevel ?? null,
    model: lastPersistedState?.model ?? null,
    ownerUid: lastPersistedState?.ownerUid ?? null,
    totalMessagesSent: sentCountCache?.totalSent ?? null,
  };
}

function renderTemplateContent(template: any, data: Record<string, unknown> | undefined) {
  let parsedBody = typeof template?.body === "string" ? template.body : "";
  const components = Array.isArray(template?.components) ? template.components : [];

  if (!parsedBody && components.length > 0) {
    parsedBody = components
      .map((component: any) => (typeof component?.text === "string" ? component.text : ""))
      .filter(Boolean)
      .join("\n\n");
  }

  const replaceTokens = (input: string) => {
    let output = input;
    if (data && typeof data === "object") {
      for (const [key, value] of Object.entries(data)) {
        output = output.replace(new RegExp(`{{${key}}}`, "g"), String(value));
      }
    }
    return output;
  };

  const renderedComponents = components.map((component: any) => ({
    ...component,
    text: typeof component?.text === "string" ? replaceTokens(component.text) : component?.text,
  }));

  return {
    subject: typeof template?.subject === "string" ? replaceTokens(template.subject) : "",
    body: replaceTokens(parsedBody),
    components: renderedComponents,
    senderName: firstEmailDisplayName(
      typeof template?.senderName === "string" ? replaceTokens(template.senderName) : undefined,
      typeof template?.fromName === "string" ? replaceTokens(template.fromName) : undefined,
      typeof template?.brandName === "string" ? replaceTokens(template.brandName) : undefined,
    ),
    logoUrl: typeof template?.logoUrl === "string" ? replaceTokens(template.logoUrl) : "",
    imageUrls: Array.isArray(template?.imageUrls)
      ? template.imageUrls.map((url: unknown) => (
          typeof url === "string" ? replaceTokens(url) : ""
        )).filter(Boolean)
      : [],
  };
}

function escapeHtml(input: string) {
  return input
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function renderPlainTextAsHtml(input: string) {
  return `<div style="font-family:Arial,sans-serif;line-height:1.6;color:#0f172a;white-space:pre-wrap">${escapeHtml(input)}</div>`;
}

function getAllowedEmailImageHosts() {
  const configured = String(
    process.env.ORBI_TALK_EMAIL_IMAGE_ALLOWED_HOSTS
    || "media-stock.orbifinancial.com,media-store.orbifinancial.com,orbishop-storage.orbifinancial.com",
  )
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
  return new Set(configured);
}

function isAllowedEmailImageHost(hostname: string, allowedHosts: Set<string>) {
  const normalized = hostname.toLowerCase();
  return Array.from(allowedHosts).some((allowed) => (
    allowed.startsWith("*.")
      ? normalized.endsWith(allowed.slice(1))
      : normalized === allowed
  ));
}

function normalizeEmailImageUrl(value: unknown) {
  if (typeof value !== "string" || !value.trim()) {
    return undefined;
  }
  try {
    const parsed = new URL(value.trim());
    if (
      parsed.protocol !== "https:"
      || parsed.username
      || parsed.password
      || !isAllowedEmailImageHost(parsed.hostname, getAllowedEmailImageHosts())
    ) {
      throw new Error("EMAIL_IMAGE_URL_NOT_ALLOWED");
    }
    parsed.hash = "";
    return parsed.toString();
  } catch (error: any) {
    if (error?.message === "EMAIL_IMAGE_URL_NOT_ALLOWED") throw error;
    throw new Error("EMAIL_IMAGE_URL_INVALID");
  }
}

function normalizeEmailImageUrls(...values: unknown[]) {
  const flattened = values.flatMap((value) => {
    if (Array.isArray(value)) return value;
    if (typeof value === "string" && value.includes(",")) return value.split(",");
    return value == null ? [] : [value];
  });
  return Array.from(new Set(flattened.map(normalizeEmailImageUrl).filter(Boolean))).slice(0, 6) as string[];
}

function renderEmailHtml(input: {
  text: string;
  html?: string;
  senderName?: string;
  logoUrl?: string;
  imageUrls?: string[];
}) {
  const content = input.html || renderPlainTextAsHtml(input.text);
  const logo = input.logoUrl
    ? `<img src="${escapeHtml(input.logoUrl)}" alt="${escapeHtml(input.senderName || "Brand")}" width="120" style="display:block;max-width:120px;max-height:72px;width:auto;height:auto;margin:0 auto 20px;border:0;" />`
    : "";
  const images = (input.imageUrls || [])
    .map((url) => `<img src="${escapeHtml(url)}" alt="" style="display:block;width:100%;max-width:640px;height:auto;margin:20px auto 0;border:0;border-radius:12px;" />`)
    .join("");
  return `<div style="margin:0;padding:24px;background:#f8fafc;"><div style="max-width:680px;margin:0 auto;padding:28px;background:#ffffff;border:1px solid #e2e8f0;border-radius:16px;">${logo}${content}${images}</div></div>`;
}

function normalizeEmailRecipient(input: unknown) {
  const value = typeof input === "string" ? input.trim() : "";
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value) ? value : "";
}

function extractEmailAddress(input: string) {
  const bracketMatch = input.match(/<([^>]+)>/);
  return (bracketMatch?.[1] || input).trim().toLowerCase();
}

function getAllowedEmailSenders() {
  const configured = (process.env.ORBI_TALK_EMAIL_ALLOWED_FROM || process.env.ORBI_EMAIL_ALLOWED_FROM || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  const defaults = [
    process.env.ORBI_TALK_EMAIL_FROM?.trim(),
    "ORBI Financial <no-reply@orbifinancial.com>",
    "ORBI Financial <notifications@orbifinancial.com>",
    "ORBI Support <support@orbifinancial.com>",
    "ORBI Sales <sales@orbifinancial.com>",
    "ORBI Security <security@orbifinancial.com>",
    "ORBI Admin <admin@orbifinancial.com>",
    "ORBI Info <info@orbifinancial.com>",
  ].filter((entry): entry is string => Boolean(entry));

  const byAddress = new Map<string, string>();
  for (const sender of [...defaults, ...configured]) {
    const address = extractEmailAddress(sender);
    if (address) {
      byAddress.set(address, sender);
    }
  }
  return byAddress;
}

function sanitizeEmailDisplayName(value?: unknown) {
  if (typeof value !== "string") {
    return undefined;
  }
  const sanitized = value
    .replace(/[\r\n<>"\\]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 120);
  return sanitized || undefined;
}

function firstEmailDisplayName(...values: unknown[]) {
  for (const value of values) {
    const displayName = sanitizeEmailDisplayName(value);
    if (displayName) {
      return displayName;
    }
  }
  return undefined;
}

function firstEmailAddress(...values: unknown[]) {
  for (const value of values) {
    if (typeof value !== "string" || !value.trim()) {
      continue;
    }
    const address = normalizeEmailRecipient(extractEmailAddress(value.trim()));
    if (address) {
      return address;
    }
  }
  return undefined;
}

function isGenericMerchantDisplayName(value?: string) {
  if (!value) {
    return true;
  }
  return [
    "merchant",
    "merchant desk",
    "merchant account",
    "business",
    "seller",
    "unknown merchant",
  ].includes(value.trim().toLowerCase());
}

function extractConfiguredDisplayName(sender: string) {
  const bracketIndex = sender.lastIndexOf("<");
  if (bracketIndex <= 0) {
    return undefined;
  }
  return sanitizeEmailDisplayName(sender.slice(0, bracketIndex));
}

function resolveAllowedEmailSender(requestedFrom?: unknown, requestedDisplayName?: unknown) {
  const allowed = getAllowedEmailSenders();
  const fallback =
    process.env.ORBI_TALK_EMAIL_FROM?.trim()
    || process.env.ORBI_EMAIL_FROM?.trim()
    || "ORBI Financial <no-reply@orbifinancial.com>";
  const requested = typeof requestedFrom === "string" ? requestedFrom.trim() : "";
  if (!requested) {
    const fallbackAddress = extractEmailAddress(fallback);
    const allowedSender = allowed.get(fallbackAddress) || fallback;
    const displayName = sanitizeEmailDisplayName(requestedDisplayName);
    return displayName ? `${displayName} <${fallbackAddress}>` : allowedSender;
  }

  const requestedAddress = extractEmailAddress(requested);
  const allowedSender = allowed.get(requestedAddress);
  if (!allowedSender) {
    throw new Error(`EMAIL_FROM_NOT_ALLOWED:${requestedAddress || "unknown"}`);
  }
  const displayName =
    sanitizeEmailDisplayName(requestedDisplayName)
    || extractConfiguredDisplayName(requested)
    || extractConfiguredDisplayName(allowedSender);
  return displayName ? `${displayName} <${requestedAddress}>` : requestedAddress;
}

function resolveAllowedReplyTo(requestedReplyTo?: unknown) {
  const requested = typeof requestedReplyTo === "string" ? requestedReplyTo.trim() : "";
  if (!requested) {
    return undefined;
  }
  const normalized = normalizeEmailRecipient(extractEmailAddress(requested));
  if (!normalized) {
    throw new Error("EMAIL_REPLY_TO_INVALID");
  }
  return requested;
}

function buildEmailDeliveryHealth() {
  const configuredProvider = process.env.ORBI_TALK_EMAIL_PROVIDER?.trim().toLowerCase() || "";
  const hasResendKey = Boolean(process.env.ORBI_TALK_RESEND_API_KEY?.trim() || process.env.RESEND_API_KEY?.trim());
  const hasSmtpHost = Boolean(process.env.ORBI_TALK_SMTP_HOST?.trim() || process.env.SMTP_HOST?.trim());
  const provider = configuredProvider || (hasResendKey ? "resend" : hasSmtpHost ? "smtp" : "");
  const allowedSenders = Array.from(getAllowedEmailSenders().entries()).map(([email, sender]) => ({
    email,
    sender,
  }));
  const defaultFrom = resolveAllowedEmailSender();
  const missing: string[] = [];

  if (!provider) {
    missing.push("ORBI_TALK_EMAIL_PROVIDER");
  } else if (!["resend", "smtp"].includes(provider)) {
    missing.push("ORBI_TALK_EMAIL_PROVIDER_UNSUPPORTED");
  }
  if (provider === "resend" && !hasResendKey) {
    missing.push("ORBI_TALK_RESEND_API_KEY");
  }
  if (provider === "smtp") {
    if (!hasSmtpHost) missing.push("ORBI_TALK_SMTP_HOST");
    if (!(process.env.ORBI_TALK_SMTP_USER?.trim() || process.env.SMTP_USER?.trim())) {
      missing.push("ORBI_TALK_SMTP_USER");
    }
    if (!(process.env.ORBI_TALK_SMTP_PASS || process.env.SMTP_PASS)) {
      missing.push("ORBI_TALK_SMTP_PASS");
    }
  }

  return {
    configured: missing.length === 0 && Boolean(provider),
    provider: provider || null,
    defaultFrom,
    replyRouting: process.env.ORBI_TALK_EMAIL_REPLY_TO?.trim()
      ? "explicit_reply_to"
      : "cloudflare_email_routing_by_from_alias",
    allowedSenders,
    checks: {
      resendApiKeyConfigured: hasResendKey,
      smtpHostConfigured: hasSmtpHost,
      allowedSenderCount: allowedSenders.length,
    },
    missing,
  };
}

type EmailDeliveryInput = {
  to: string;
  from?: string;
  fromName?: string;
  subject: string;
  text: string;
  html?: string;
  messageId?: string;
  replyTo?: string;
  logoUrl?: string;
  imageUrls?: string[];
};

async function sendEmailDelivery(input: EmailDeliveryInput) {
  const to = normalizeEmailRecipient(input.to);
  if (!to) {
    throw new Error("EMAIL_RECIPIENT_INVALID");
  }

  const from =
    resolveAllowedEmailSender(input.from, input.fromName);
  if (!from) {
    throw new Error("EMAIL_FROM_NOT_CONFIGURED");
  }

  const subject = input.subject?.trim() || "ORBI Notification";
  const text = input.text || "";
  const html = renderEmailHtml({
    text,
    html: input.html,
    senderName: input.fromName,
    logoUrl: input.logoUrl,
    imageUrls: input.imageUrls,
  });
  const replyTo =
    resolveAllowedReplyTo(input.replyTo) || undefined;
  const provider =
    process.env.ORBI_TALK_EMAIL_PROVIDER?.trim().toLowerCase()
    || (process.env.ORBI_TALK_RESEND_API_KEY?.trim() || process.env.RESEND_API_KEY?.trim() ? "resend" : "")
    || (process.env.ORBI_TALK_SMTP_HOST?.trim() || process.env.SMTP_HOST?.trim() ? "smtp" : "");

  if (provider === "resend") {
    const apiKey = process.env.ORBI_TALK_RESEND_API_KEY?.trim() || process.env.RESEND_API_KEY?.trim() || "";
    if (!apiKey) {
      throw new Error("RESEND_API_KEY_NOT_CONFIGURED");
    }

    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        from,
        to: [to],
        subject,
        html,
        text,
        ...(replyTo ? { reply_to: replyTo } : {}),
        ...(input.messageId ? { headers: { "X-ORBI-Message-ID": input.messageId } } : {}),
      }),
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload?.message || payload?.error || `RESEND_EMAIL_FAILED_${response.status}`);
    }

    return {
      provider: "resend",
      providerMessageId: typeof payload?.id === "string" ? payload.id : null,
    };
  }

  if (provider === "smtp") {
    const host = process.env.ORBI_TALK_SMTP_HOST?.trim() || process.env.SMTP_HOST?.trim() || "";
    const port = Number.parseInt(process.env.ORBI_TALK_SMTP_PORT || process.env.SMTP_PORT || "587", 10);
    const secureValue = process.env.ORBI_TALK_SMTP_SECURE || process.env.SMTP_SECURE || "";
    const user = process.env.ORBI_TALK_SMTP_USER?.trim() || process.env.SMTP_USER?.trim() || "";
    const pass = process.env.ORBI_TALK_SMTP_PASS || process.env.SMTP_PASS || "";

    if (!host) {
      throw new Error("SMTP_HOST_NOT_CONFIGURED");
    }

    const transporter = nodemailer.createTransport({
      host,
      port: Number.isFinite(port) ? port : 587,
      secure: secureValue === "true" || secureValue === "1",
      ...(user || pass ? { auth: { user, pass } } : {}),
    });

    const result = await transporter.sendMail({
      from,
      to,
      subject,
      text,
      html,
      ...(replyTo ? { replyTo } : {}),
      ...(input.messageId ? { headers: { "X-ORBI-Message-ID": input.messageId } } : {}),
    });

    return {
      provider: "smtp",
      providerMessageId: typeof result.messageId === "string" ? result.messageId : null,
    };
  }

  throw new Error("EMAIL_PROVIDER_NOT_CONFIGURED");
}

function normalizeImportedTemplateComponents(components: unknown) {
  if (!Array.isArray(components)) {
    return [];
  }

  return components
    .filter((component) => component && typeof component === "object")
    .map((component: any) => ({
      type: typeof component?.type === "string" ? component.type.trim() : "",
      text: typeof component?.text === "string" ? component.text.trim() : "",
    }))
    .filter((component) => component.type);
}

function normalizeImportedTemplate(input: any) {
  const components = normalizeImportedTemplateComponents(input?.components);
  const derivedBody = components
    .map((component) => component.text)
    .filter(Boolean)
    .join("\n\n")
    .trim();
  const body = typeof input?.body === "string" ? input.body.trim() : "";
  const normalizedBody = body || derivedBody;
  const normalizedChannel = typeof input?.channel === "string" ? input.channel.trim() : "";
  const normalizedMessageType = input?.messageType === "promotional" ? "promotional" : "transactional";
  const normalizedTemplate: Record<string, unknown> = {
    name: typeof input?.name === "string" ? input.name.trim() : "",
    language: typeof input?.language === "string" && input.language.trim() ? input.language.trim() : "en",
    subject: typeof input?.subject === "string" ? input.subject.trim() : "",
    senderName: typeof input?.senderName === "string" ? input.senderName.trim() : "",
    fromEmail: typeof input?.fromEmail === "string" ? input.fromEmail.trim() : "",
    replyTo: typeof input?.replyTo === "string" ? input.replyTo.trim() : "",
    logoUrl: typeof input?.logoUrl === "string" ? input.logoUrl.trim() : "",
    imageUrls: Array.isArray(input?.imageUrls)
      ? input.imageUrls.filter((url: unknown): url is string => typeof url === "string").map((url: string) => url.trim()).filter(Boolean).slice(0, 6)
      : [],
    body: normalizedBody,
    channel: normalizedChannel,
    messageType: normalizedMessageType,
  };

  if (normalizedChannel === "whatsapp") {
    const nextComponents = components.length > 0
      ? components.map((component) => (
          component.type === "body"
            ? { ...component, text: normalizedBody }
            : component
        ))
      : [{ type: "body", text: normalizedBody }];

    if (!nextComponents.some((component) => component.type === "body")) {
      nextComponents.unshift({ type: "body", text: normalizedBody });
    }

    normalizedTemplate.components = nextComponents;
  }

  return normalizedTemplate;
}

function extractTemplateVariables(template: Record<string, unknown>) {
  const values: string[] = [];
  const capture = (input: unknown) => {
    if (typeof input !== "string") return;
    const matches = input.match(/\{\{(.*?)\}\}/g) || [];
    for (const match of matches) {
      const normalized = match.replace(/[{}]/g, "").trim();
      if (normalized) {
        values.push(normalized);
      }
    }
  };

  capture(template.subject);
  capture(template.senderName);
  capture(template.logoUrl);
  capture(template.body);
  if (Array.isArray(template.imageUrls)) {
    for (const imageUrl of template.imageUrls) capture(imageUrl);
  }
  if (Array.isArray(template.components)) {
    for (const component of template.components) {
      capture((component as any)?.text);
    }
  }

  return Array.from(new Set(values)).sort();
}

function validateImportedTemplate(template: Record<string, unknown>) {
  if (typeof template.name !== "string" || !template.name || template.name.length >= 255) {
    return "Template name is required";
  }

  if (typeof template.channel !== "string" || !["sms", "whatsapp", "email", "push"].includes(template.channel)) {
    return "Template channel must be one of sms, whatsapp, email, or push";
  }

  if (typeof template.language !== "string" || !template.language || template.language.length >= 10) {
    return "Template language is required";
  }

  if (typeof template.body !== "string" || !template.body || template.body.length >= 1000) {
    return "Template body is required";
  }

  if (template.subject != null && (typeof template.subject !== "string" || template.subject.length >= 255)) {
    return "Template subject is invalid";
  }

  if (template.senderName != null && (typeof template.senderName !== "string" || template.senderName.length >= 121)) {
    return "Template senderName is invalid";
  }

  if (template.fromEmail != null && (typeof template.fromEmail !== "string" || template.fromEmail.length >= 255)) {
    return "Template fromEmail is invalid";
  }

  if (template.replyTo != null && (typeof template.replyTo !== "string" || template.replyTo.length >= 255)) {
    return "Template replyTo is invalid";
  }

  if (template.logoUrl != null && (typeof template.logoUrl !== "string" || template.logoUrl.length >= 2048)) {
    return "Template logoUrl is invalid";
  }

  if (template.imageUrls != null) {
    if (!Array.isArray(template.imageUrls) || template.imageUrls.length > 6) {
      return "Template imageUrls are invalid";
    }
    if (template.imageUrls.some((url) => typeof url !== "string" || !url || url.length >= 2048)) {
      return "Template imageUrls contain an invalid URL";
    }
  }

  if (template.messageType !== "transactional" && template.messageType !== "promotional") {
    return "Template messageType must be transactional or promotional";
  }

  if (template.components != null) {
    if (!Array.isArray(template.components) || template.components.length === 0 || template.components.length >= 20) {
      return "Template components are invalid";
    }

    for (const component of template.components) {
      if (!component || typeof component !== "object") {
        return "Template components are invalid";
      }
      const componentType = typeof (component as any).type === "string" ? (component as any).type : "";
      const componentText = (component as any).text;
      if (!componentType || componentType.length >= 50) {
        return "Template component type is invalid";
      }
      if (componentText != null && (typeof componentText !== "string" || componentText.length >= 1000)) {
        return "Template component text is invalid";
      }
    }
  }

  return null;
}

async function persistDevicePresence(
  db: FirebaseFirestore.Firestore,
  deviceId: string,
  input: {
    status?: unknown;
    batteryLevel?: unknown;
    model?: unknown;
    fcmToken?: unknown;
    ownerUid?: unknown;
  },
  options: {
    force?: boolean;
  } = {},
) {
  const nextState = buildDevicePresenceState(input);
  const previousState = lastPersistedDeviceState.get(deviceId);

  if (!shouldPersistDevicePresence(deviceId, nextState, options.force === true)) {
    return false;
  }

  const updateData: Record<string, unknown> = {
    lastSeen: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    status: nextState.status,
  };

  if (previousState?.status !== nextState.status) {
    updateData.last_status_change = admin.firestore.FieldValue.serverTimestamp();
  }

  if (nextState.status === "offline") {
    updateData.offline_reason = "Heartbeat timeout - no recent gateway activity";
  } else {
    updateData.offline_reason = admin.firestore.FieldValue.delete();
  }

  if (nextState.batteryLevel != null) {
    updateData.batteryLevel = nextState.batteryLevel;
  }

  if (nextState.model) {
    updateData.model = nextState.model;
  }

  if (nextState.fcmToken) {
    updateData.fcmToken = nextState.fcmToken;
  }

  if (nextState.ownerUid) {
    updateData.ownerUid = nextState.ownerUid;
  }

  await db.collection("devices").doc(deviceId).set(updateData, { merge: true });
  markDevicePresencePersisted(deviceId, nextState);
  return true;
}

async function getDeviceSentCount(
  db: FirebaseFirestore.Firestore,
  deviceId: string,
) {
  const cached = deviceSentCountCache.get(deviceId);
  const now = Date.now();
  if (cached && now - cached.fetchedAt < DEVICE_SENT_COUNT_CACHE_MS) {
    return cached.totalSent;
  }

  const countSnapshot = await db.collection("message_logs")
    .where("deviceId", "==", deviceId)
    .where("status", "in", ["sent", "delivered"])
    .count()
    .get();
  const totalSent = countSnapshot.data().count || 0;
  deviceSentCountCache.set(deviceId, { totalSent, fetchedAt: now });
  return totalSent;
}

async function resolveOwnerContext(
  db: any,
  {
    ownerUid,
    ownerEmail,
    deviceId,
  }: {
    ownerUid?: string;
    ownerEmail?: string;
    deviceId?: string;
  },
) {
  let resolvedOwnerUid = ownerUid?.trim() || null;
  let resolvedOwnerEmail = ownerEmail?.trim().toLowerCase() || null;
  let resolvedDeviceOwnerUid: string | null = null;

  if (deviceId?.trim()) {
    const deviceDoc = await db.collection("devices").doc(deviceId.trim()).get();
    if (deviceDoc.exists) {
      resolvedDeviceOwnerUid = deviceDoc.data()?.ownerUid || null;
    }
  }

  if (!resolvedOwnerUid && resolvedOwnerEmail) {
    const usersSnapshot = await db.collection("users")
      .where("email", "==", resolvedOwnerEmail)
      .limit(1)
      .get();
    if (!usersSnapshot.empty) {
      resolvedOwnerUid = usersSnapshot.docs[0].id;
    }
  }

  if (!resolvedOwnerUid && resolvedDeviceOwnerUid) {
    resolvedOwnerUid = resolvedDeviceOwnerUid;
  }

  if (resolvedOwnerUid && !resolvedOwnerEmail) {
    const userDoc = await db.collection("users").doc(resolvedOwnerUid).get();
    if (userDoc.exists) {
      const email = userDoc.data()?.email;
      if (typeof email === "string" && email.trim()) {
        resolvedOwnerEmail = email.trim().toLowerCase();
      }
    }
  }

  if (resolvedOwnerUid && resolvedDeviceOwnerUid && resolvedOwnerUid !== resolvedDeviceOwnerUid) {
    throw new Error("deviceId does not belong to the specified owner");
  }

  return {
    ownerUid: resolvedOwnerUid,
    ownerEmail: resolvedOwnerEmail,
  };
}

async function findExistingMessageByRequestId(
  db: any,
  {
    ownerUid,
    requestId,
  }: {
    ownerUid?: string | null;
    requestId?: string | null;
  },
) {
  const normalizedRequestId = requestId?.trim();
  const normalizedOwnerUid = ownerUid?.trim();
  if (!normalizedRequestId || !normalizedOwnerUid) {
    return null;
  }

  const snapshot = await db.collection("message_logs")
    .where("createdBy", "==", normalizedOwnerUid)
    .where("requestId", "==", normalizedRequestId)
    .limit(1)
    .get();

  if (snapshot.empty) {
    return null;
  }

  const existingDoc = snapshot.docs[0];
  return {
    id: existingDoc.id,
    data: existingDoc.data(),
  };
}

function buildIdempotencyDocId(ownerUid: string, requestId: string) {
  return crypto
    .createHash("sha256")
    .update(`${ownerUid.trim()}:${requestId.trim()}`)
    .digest("hex");
}

async function getUserRole(db: any, uid: string) {
  const userDoc = await db.collection("users").doc(uid).get();
  if (!userDoc.exists) {
    return "user";
  }
  return userDoc.data()?.role || "user";
}

async function deleteCollectionInBatches(
  db: FirebaseFirestore.Firestore,
  collectionName: string,
  batchSize = 200,
) {
  let deleted = 0;

  while (true) {
    const snapshot = await db.collection(collectionName).limit(batchSize).get();
    if (snapshot.empty) {
      return deleted;
    }

    const batch = db.batch();
    for (const doc of snapshot.docs) {
      batch.delete(doc.ref);
    }
    await batch.commit();
    deleted += snapshot.size;

    if (snapshot.size < batchSize) {
      return deleted;
    }
  }
}

function hashApiKey(rawKey: string) {
  return crypto.createHash("sha256").update(rawKey).digest("hex");
}

function buildApiKeyPrefix(rawKey: string) {
  return rawKey.length <= 16 ? rawKey : rawKey.slice(0, 16);
}

function generateExternalApiKey() {
  const randomPart = crypto.randomBytes(24).toString("base64url");
  return `orbi_live_${randomPart}`;
}

type ExternalCredential = {
  authType: "user_api_key";
  credentialId: string;
  ownerUid: string | null;
  ownerEmail: string | null;
  scopes: string[];
  name: string | null;
};

const API_CREDENTIAL_CACHE_MS = envInteger("ORBI_TALK_API_CREDENTIAL_CACHE_MS", 300000, 60000);
const externalCredentialCache = new Map<string, { credential: ExternalCredential; expiresAt: number }>();

function apiKeysMatch(left: string, right: string) {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return leftBuffer.length === rightBuffer.length && crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function loadStaticApiCredentials() {
  const credentials: Array<{ apiKey: string; credential: ExternalCredential }> = [];
  const rawConfig = process.env.ORBI_TALK_STATIC_API_CREDENTIALS?.trim();
  if (rawConfig) {
    try {
      const entries = JSON.parse(rawConfig);
      if (Array.isArray(entries)) {
        for (const [index, entry] of entries.entries()) {
          const apiKey = normalizeOptionalString(entry?.apiKey);
          if (!apiKey) continue;
          credentials.push({
            apiKey,
            credential: {
              authType: "user_api_key",
              credentialId: `static_${index + 1}`,
              ownerUid: normalizeOptionalString(entry?.ownerUid),
              ownerEmail: normalizeOptionalString(entry?.ownerEmail)?.toLowerCase() || null,
              scopes: Array.isArray(entry?.scopes)
                ? entry.scopes.filter((scope: unknown): scope is string => typeof scope === "string")
                : ["send_template", "send_sms", "send_email"],
              name: normalizeOptionalString(entry?.name) || "Static Product Integration",
            },
          });
        }
      }
    } catch (error) {
      console.error("ORBI_TALK_STATIC_API_CREDENTIALS is not valid JSON:", error);
    }
  }

  const shopApiKey = normalizeOptionalString(process.env.ORBI_SHOP_TALK_API_KEY);
  if (shopApiKey) {
    credentials.push({
      apiKey: shopApiKey,
      credential: {
        authType: "user_api_key",
        credentialId: "static_orbi_shop",
        ownerUid: normalizeOptionalString(process.env.ORBI_SHOP_TALK_OWNER_UID),
        ownerEmail: normalizeOptionalString(process.env.ORBI_SHOP_TALK_OWNER_EMAIL)?.toLowerCase() || null,
        scopes: ["send_template", "send_sms", "send_email", "templates_read"],
        name: "ORBI Shop",
      },
    });
  }
  return credentials;
}

const staticApiCredentials = loadStaticApiCredentials();

function generatePairingCode() {
  const randomPart = crypto.randomBytes(18).toString("base64url");
  return `pair_${randomPart}`;
}

async function startServer() {
  const app = express();
  const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
  const connectedDevices = new Map<string, WebSocket>();

  // Enable CORS for all origins (configure this later for production security)
  app.use(cors({
    origin: '*', // Allow all origins for now. Change to specific domains in production.
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'x-api-key']
  }));
  app.use(express.json());

  const requireFirebaseUser = async (req: any, res: any, next: any) => {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith("Bearer ")) {
        return res.status(401).json({ error: "Missing Firebase bearer token" });
      }

      const idToken = authHeader.slice("Bearer ".length).trim();
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const decoded = await adminApp.auth().verifyIdToken(idToken);
      req.firebaseUser = decoded;
      next();
    } catch (error) {
      console.error("Firebase auth verification failed:", error);
      return res.status(401).json({ error: "Invalid Firebase bearer token" });
    }
  };

  const authenticateFirebaseUserIfPresent = async (req: any, res: any, next: any) => {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith("Bearer ")) {
        req.firebaseUser = null;
        return next();
      }

      const idToken = authHeader.slice("Bearer ".length).trim();
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const decoded = await adminApp.auth().verifyIdToken(idToken);
      req.firebaseUser = decoded;
      next();
    } catch (error) {
      console.error("Optional Firebase auth verification failed:", error);
      return res.status(401).json({ error: "Invalid Firebase bearer token" });
    }
  };

  const authenticateExternalRequest = async (req: any, res: any, next: any) => {
    try {
      const rawApiKey = req.header("x-api-key")?.trim() || "";
      if (!rawApiKey) {
        req.externalAuth = null;
        return next();
      }

      const trustedGatewayApiKey =
        process.env.ORBI_TALK_GATEWAY_API_KEY?.trim()
        || process.env.ORBI_GATEWAY_API_KEY?.trim()
        || "";
      if (trustedGatewayApiKey && rawApiKey === trustedGatewayApiKey) {
        req.externalAuth = {
          authType: "trusted_system",
          credentialId: "trusted_system",
          ownerUid: null,
          ownerEmail: null,
          scopes: ["send_template", "send_sms", "send_email", "admin"],
          name: "ORBI Trusted Infrastructure",
        };
        return next();
      }

      const staticCredential = staticApiCredentials.find((entry) => apiKeysMatch(rawApiKey, entry.apiKey));
      if (staticCredential) {
        req.externalAuth = staticCredential.credential;
        return next();
      }

      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const keyHash = hashApiKey(rawApiKey);
      const cachedCredential = externalCredentialCache.get(keyHash);
      if (cachedCredential && cachedCredential.expiresAt > Date.now()) {
        req.externalAuth = cachedCredential.credential;
        return next();
      }
      const credentialDoc = await db.collection("api_credentials").doc(keyHash).get();
      if (!credentialDoc.exists) {
        return res.status(401).json({ error: "Invalid API key" });
      }

      const credentialData = credentialDoc.data();
      if (!credentialData || credentialData.status !== "active") {
        return res.status(403).json({ error: "API key is inactive" });
      }

      const resolvedCredential: ExternalCredential = {
        authType: "user_api_key",
        credentialId: credentialDoc.id,
        ownerUid: credentialData.ownerUid || null,
        ownerEmail: credentialData.ownerEmail || null,
        scopes: Array.isArray(credentialData.scopes) ? credentialData.scopes : [],
        name: credentialData.name || null,
      };
      externalCredentialCache.set(keyHash, {
        credential: resolvedCredential,
        expiresAt: Date.now() + API_CREDENTIAL_CACHE_MS,
      });
      req.externalAuth = resolvedCredential;
      credentialDoc.ref.set(
        { lastUsedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true },
      ).catch((error) => {
        if (!registerFirestoreQuotaBackoff("API Credential Usage", error)) {
          console.warn("Failed to update API credential lastUsedAt:", error);
        }
      });
      next();
    } catch (error) {
      if (registerFirestoreQuotaBackoff("API Authentication", error)) {
        return res.status(503).json({
          error: "API_CREDENTIAL_STORE_UNAVAILABLE",
          message: "Scoped API key verification is temporarily unavailable because Firestore quota is exhausted.",
          retryAfterSeconds: Math.max(Math.ceil((firestoreQuotaBackoffUntil - Date.now()) / 1000), 60),
        });
      }
      console.error("External API key auth failed:", error);
      return res.status(500).json({ error: "Failed to authenticate API key" });
    }
  };

  const requireCredentialScope = (scopes: string[]) => {
    return (req: any, res: any, next: any) => {
      const externalAuth = req.externalAuth;
      if (!externalAuth) {
        return next();
      }
      if (externalAuth.authType === "trusted_system") {
        return next();
      }
      const grantedScopes = Array.isArray(externalAuth.scopes) ? externalAuth.scopes : [];
      const hasScope = scopes.some((scope) => grantedScopes.includes(scope));
      if (!hasScope) {
        return res.status(403).json({ error: "API key does not have the required scope" });
      }
      next();
    };
  };

  const requireAuthenticatedSender = (req: any, res: any, next: any) => {
    if (req.externalAuth || req.firebaseUser) {
      return next();
    }
    return res.status(401).json({
      error: "Missing authentication. Provide x-api-key or Firebase bearer token.",
    });
  };

  const buildHealthPayload = () => ({
    status: "NOMINAL",
    node: os.hostname(),
    version: SERVICE_VERSION,
    uptime: process.uptime(),
    circuits: [] as string[],
    ledger: "VERIFIED",
    ts: Date.now(),
  });

  // Health check endpoint for the platform
  app.get("/health", (req, res) => {
    res.json(buildHealthPayload());
  });

  // Device status endpoint for debugging
  app.get('/api/devices/status/:deviceId', async (req, res) => {
    const { deviceId } = req.params;
    const lastBeat = lastHeartbeat.get(deviceId);
    
    if (!lastBeat) {
      return res.json({ status: "unknown", last_heartbeat: null, time_since_heartbeat_ms: null });
    }
    
    const timeSinceLastBeat = Date.now() - lastBeat;
    const isAlive = timeSinceLastBeat <= HEARTBEAT_TIMEOUT_MS;
    
    res.json({
      status: isAlive ? "online" : "offline",
      last_heartbeat: new Date(lastBeat).toISOString(),
      time_since_heartbeat_ms: timeSinceLastBeat,
      time_since_heartbeat_seconds: Math.round(timeSinceLastBeat / 1000),
      timeout_in_seconds: Math.max(0, Math.round((HEARTBEAT_TIMEOUT_MS - timeSinceLastBeat) / 1000)),
    });
  });

  app.get("/api/devices/live-status", requireFirebaseUser, async (req: any, res) => {
    try {
      const requestedIds = String(req.query.deviceIds || "")
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean);

      if (requestedIds.length === 0) {
        return res.json({ devices: [] });
      }

      const uniqueIds = Array.from(new Set(requestedIds)).slice(0, 250);
      const adminApp = getFirebaseAdmin();
      const db = adminApp?.firestore();
      const devices = await Promise.all(
        uniqueIds.map(async (deviceId) => {
          if (db) {
            try {
              await getDeviceSentCount(db, deviceId);
            } catch (error) {
              console.error(`Failed to refresh sent count for ${deviceId}:`, error);
            }
          }
          return buildLiveDeviceStatus(deviceId, connectedDevices.has(deviceId));
        }),
      );

      return res.json({
        devices,
        generatedAt: new Date().toISOString(),
        heartbeatTimeoutMs: HEARTBEAT_TIMEOUT_MS,
      });
    } catch (error) {
      console.error("Failed to load live device status:", error);
      return res.status(500).json({ error: "Failed to load live device status" });
    }
  });

  // API routes for ORBI Talk Gateway
  app.get("/api/status", (req, res) => {
    res.json({ 
      gateway: "ORBI Talk Gateway",
      version: SERVICE_VERSION,
      status: "active",
      uptime: process.uptime()
    });
  });

  app.get(
    "/api/email/health",
    authenticateFirebaseUserIfPresent,
    authenticateExternalRequest,
    requireAuthenticatedSender,
    requireCredentialScope(["send_email", "send_template", "admin"]),
    (req: any, res) => {
      const health = buildEmailDeliveryHealth();
      res.status(health.configured ? 200 : 503).json({
        success: health.configured,
        email: health,
        authType: req.externalAuth?.authType || (req.firebaseUser ? "firebase_user" : "unknown"),
        generatedAt: new Date().toISOString(),
      });
    },
  );

  app.get(
    "/api/queue-health",
    authenticateFirebaseUserIfPresent,
    authenticateExternalRequest,
    async (req: any, res) => {
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const ownerUid =
        req.externalAuth?.authType === "trusted_system"
          ? null
          : (req.externalAuth?.ownerUid || req.firebaseUser?.uid || null);
      const statuses = ["pending", "queued", "processing", "sent", "delivered", "failed"];
      const counts: Record<string, number> = {};

      for (const status of statuses) {
        let query: any = db.collection("message_logs").where("status", "==", status);
        if (ownerUid) {
          query = query.where("createdBy", "==", ownerUid);
        }
        const snapshot = await query.count().get();
        counts[status] = snapshot.data().count || 0;
      }

      const staleThreshold = admin.firestore.Timestamp.fromMillis(
        Date.now() - STALE_PROCESSING_TIMEOUT_MS,
      );
      let staleQuery: any = db.collection("message_logs")
        .where("status", "==", "processing")
        .where("updatedAt", "<=", staleThreshold);
      if (ownerUid) {
        staleQuery = staleQuery.where("createdBy", "==", ownerUid);
      }
      const staleSnapshot = await staleQuery.count().get();
      clearFirestoreQuotaBackoff();

      return res.json({
        ownerUid: ownerUid || null,
        generatedAt: new Date().toISOString(),
        staleProcessingTimeoutSeconds: Math.round(STALE_PROCESSING_TIMEOUT_MS / 1000),
        counts,
        staleProcessingCount: staleSnapshot.data().count || 0,
      });
    } catch (error) {
      if (registerFirestoreQuotaBackoff("Queue Health", error)) {
        return res.status(429).json({
          error: "FIRESTORE_QUOTA_EXHAUSTED",
          retryAfterSeconds: Math.max(Math.ceil((firestoreQuotaBackoffUntil - Date.now()) / 1000), 60),
        });
      }
      console.error("Failed to fetch queue health:", error);
      return res.status(500).json({ error: "Failed to fetch queue health" });
    }
    },
  );

  app.get("/api/api-credentials", requireFirebaseUser, async (req: any, res) => {
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const requesterUid = req.firebaseUser.uid;
      const role = await getUserRole(db, requesterUid);
      const snapshot = role === "admin"
        ? await db.collection("api_credentials").orderBy("createdAt", "desc").limit(100).get()
        : await db.collection("api_credentials")
            .where("ownerUid", "==", requesterUid)
            .limit(50)
            .get();

      const credentials = snapshot.docs.map((credentialDoc) => {
        const data = credentialDoc.data();
        return {
          id: credentialDoc.id,
          ownerUid: data.ownerUid,
          ownerEmail: data.ownerEmail || null,
          name: data.name || "External Integration",
          keyPrefix: data.keyPrefix || null,
          scopes: Array.isArray(data.scopes) ? data.scopes : [],
          status: data.status || "inactive",
          createdAt: data.createdAt || null,
          lastUsedAt: data.lastUsedAt || null,
          revokedAt: data.revokedAt || null,
        };
      }).sort((a, b) => {
        const aMillis = a.createdAt?.toMillis?.() || 0;
        const bMillis = b.createdAt?.toMillis?.() || 0;
        return bMillis - aMillis;
      });

      return res.json({ credentials });
    } catch (error) {
      console.error("Failed to list API credentials:", error);
      return res.status(500).json({ error: "Failed to list API credentials" });
    }
  });

  app.post("/api/api-credentials", requireFirebaseUser, async (req: any, res) => {
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const requesterUid = req.firebaseUser.uid;
      const userDoc = await db.collection("users").doc(requesterUid).get();
      const ownerEmail = req.firebaseUser.email || userDoc.data()?.email || null;
      const name = typeof req.body?.name === "string" && req.body.name.trim()
        ? req.body.name.trim()
        : "External Integration";
      const requestedScopes = Array.isArray(req.body?.scopes) ? req.body.scopes : [];
      const scopes = requestedScopes.length > 0
        ? requestedScopes.filter((scope: any) => typeof scope === "string")
        : ["send_template", "send_sms", "send_email"];
      const rawApiKey = generateExternalApiKey();
      const keyHash = hashApiKey(rawApiKey);

      await db.collection("api_credentials").doc(keyHash).set({
        ownerUid: requesterUid,
        ownerEmail,
        name,
        keyPrefix: buildApiKeyPrefix(rawApiKey),
        keyHash,
        status: "active",
        scopes,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        lastUsedAt: null,
      });

      return res.status(201).json({
        id: keyHash,
        apiKey: rawApiKey,
        ownerUid: requesterUid,
        ownerEmail,
        name,
        scopes,
        keyPrefix: buildApiKeyPrefix(rawApiKey),
        message: "API key created. Store it now; it will not be shown again.",
      });
    } catch (error) {
      console.error("Failed to create API credential:", error);
      return res.status(500).json({ error: "Failed to create API credential" });
    }
  });

  app.post("/api/api-credentials/:credentialId/revoke", requireFirebaseUser, async (req: any, res) => {
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const requesterUid = req.firebaseUser.uid;
      const role = await getUserRole(db, requesterUid);
      const credentialRef = db.collection("api_credentials").doc(req.params.credentialId);
      const credentialDoc = await credentialRef.get();
      if (!credentialDoc.exists) {
        return res.status(404).json({ error: "API credential not found" });
      }

      const credentialData = credentialDoc.data();
      const isOwner = credentialData?.ownerUid === requesterUid;
      if (!isOwner && role !== "admin") {
        return res.status(403).json({ error: "Not authorized to revoke this API credential" });
      }

      await credentialRef.set(
        {
          status: "revoked",
          revokedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true },
      );

      return res.json({ success: true, message: "API credential revoked" });
    } catch (error) {
      console.error("Failed to revoke API credential:", error);
      return res.status(500).json({ error: "Failed to revoke API credential" });
    }
  });

  app.post("/api/admin/reset", requireFirebaseUser, async (req: any, res) => {
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const requesterUid = req.firebaseUser.uid;
      const role = await getUserRole(db, requesterUid);
      if (role !== "admin") {
        return res.status(403).json({ error: "Only admins can reset the ORBI Talk Gateway server" });
      }

      const collectionsToDelete = [
        "message_logs",
        "devices",
        "message_templates",
        "api_credentials",
        "device_pairings",
        "message_request_index",
      ] as const;
      const deletedCounts: Record<string, number> = {};

      logTrace("admin.reset.start", {
        requesterUid,
        collections: collectionsToDelete,
      });

      for (const collectionName of collectionsToDelete) {
        const deleted = await deleteCollectionInBatches(db, collectionName);
        deletedCounts[collectionName] = deleted;
      }

      for (const [connectedDeviceId, socket] of connectedDevices.entries()) {
        try {
          socket.close(1012, "Gateway reset");
        } catch (_) {
          // Ignore close failures during reset.
        }
        connectedDevices.delete(connectedDeviceId);
      }
      lastHeartbeat.clear();
      lastHeartbeatWrite.clear();
      deviceSentCountCache.clear();
      lastPersistedDeviceState.clear();

      await db.collection("system_meta").doc("gateway_reset_state").set({
        lastResetAt: admin.firestore.FieldValue.serverTimestamp(),
        lastResetBy: requesterUid,
        serviceVersion: SERVICE_VERSION,
        deletedCounts,
      });

      const totalDeleted = Object.values(deletedCounts).reduce((sum, value) => sum + value, 0);
      logTrace("admin.reset.complete", {
        requesterUid,
        totalDeleted,
        deletedCounts,
      });

      return res.json({
        success: true,
        totalDeleted,
        deletedCounts,
        recreated: ["system_meta/gateway_reset_state"],
        message: "ORBI Talk Gateway reset completed successfully",
      });
    } catch (error) {
      logErrorTrace("admin.reset.failed", error, {
        requesterUid: req.firebaseUser?.uid || null,
      });
      return res.status(500).json({ error: "Failed to reset ORBI Talk Gateway server" });
    }
  });

  app.post("/api/templates/import", requireFirebaseUser, async (req: any, res) => {
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const templates = Array.isArray(req.body?.templates) ? req.body.templates : [];
      if (templates.length === 0) {
        return res.status(400).json({ error: "At least one template is required" });
      }
      if (templates.length > 500) {
        return res.status(400).json({ error: "Too many templates in one import request" });
      }

      const db = adminApp.firestore();
      const batch = db.batch();
      let importedCount = 0;

      for (let index = 0; index < templates.length; index += 1) {
        const normalizedTemplate = normalizeImportedTemplate(templates[index]);
        const validationError = validateImportedTemplate(normalizedTemplate);
        if (validationError) {
          return res.status(400).json({
            error: `Template ${index + 1} failed validation: ${validationError}`,
          });
        }

        const docRef = db.collection("message_templates").doc();
        batch.set(docRef, {
          ...normalizedTemplate,
          createdBy: req.firebaseUser.uid,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        });
        importedCount += 1;
      }

      await batch.commit();
      return res.status(201).json({
        success: true,
        importedCount,
      });
    } catch (error) {
      console.error("Failed to import templates:", error);
      return res.status(500).json({ error: "Failed to import templates" });
    }
  });

  app.get("/api/templates/catalog", authenticateFirebaseUserIfPresent, authenticateExternalRequest, requireAuthenticatedSender, requireCredentialScope(["send_template"]), async (req: any, res) => {
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(503).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const search = typeof req.query.search === "string" ? req.query.search.trim().toLowerCase() : "";
      const channel = typeof req.query.channel === "string" ? req.query.channel.trim() : "";
      const language = typeof req.query.language === "string" ? req.query.language.trim() : "";
      const messageType = typeof req.query.messageType === "string" ? req.query.messageType.trim() : "";
      const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 200);

      let query: any = db.collection("message_templates").limit(limit);
      const trustedCatalogOwnerUid =
        normalizeOptionalString(req.query.ownerUid)
        || normalizeOptionalString(process.env.ORBI_TALK_GATEWAY_USER_ID)
        || normalizeOptionalString(process.env.ORBI_COMMUNICATIONS_GATEWAY_USER_ID)
        || normalizeOptionalString(process.env.ORBI_GATEWAY_USER_ID)
        || normalizeOptionalString(process.env.OBI_GATEWAY_USER_ID);
      const ownerUid =
        req.externalAuth?.authType === "trusted_system"
          ? trustedCatalogOwnerUid
          : (req.externalAuth?.ownerUid || req.firebaseUser?.uid || null);

      if (!ownerUid) {
        return res.status(400).json({
          error: "TEMPLATE_OWNER_REQUIRED",
          message: "Template catalogs must be scoped to an authenticated owner.",
        });
      }
      query = query.where("createdBy", "==", ownerUid);
      if (channel) {
        query = query.where("channel", "==", channel);
      }
      if (language) {
        query = query.where("language", "==", language);
      }
      if (messageType) {
        query = query.where("messageType", "==", messageType);
      }

      const snapshot = await query.get();
      let data = snapshot.docs.map((doc) => {
        const entry = doc.data() || {};
        return {
          id: doc.id,
          name: entry.name,
          channel: entry.channel,
          language: entry.language || "en",
          messageType: entry.messageType || "transactional",
          subject: entry.subject || "",
          senderName: entry.senderName || "",
          fromEmail: entry.fromEmail || "",
          replyTo: entry.replyTo || "",
          logoUrl: entry.logoUrl || "",
          imageUrls: Array.isArray(entry.imageUrls) ? entry.imageUrls : [],
          body: entry.body || "",
          variables: extractTemplateVariables(entry),
        };
      });

      if (search) {
        data = data.filter((item) => String(item.name || "").toLowerCase().includes(search));
      }

      data.sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
      return res.json({ success: true, data });
    } catch (error) {
      logErrorTrace("templates.catalog.failed", error, {
        authType: req.externalAuth?.authType || (req.firebaseUser ? "firebase_user" : "anonymous"),
      });
      return res.status(500).json({ error: "Failed to fetch template catalog" });
    }
  });

  app.get("/api/pairing-config", requireFirebaseUser, async (req: any, res) => {
    try {
      const protocolHeader = (req.headers["x-forwarded-proto"] as string | undefined)?.split(",")[0]?.trim();
      const hostHeader = (req.headers["x-forwarded-host"] as string | undefined)?.split(",")[0]?.trim()
        || req.get("host");
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }
      const db = adminApp.firestore();

      const protocol = protocolHeader || req.protocol || "https";
      const requestBaseUrl = hostHeader ? `${protocol}://${hostHeader}` : "";
      const baseUrl = resolveTalkGatewayBaseUrl(requestBaseUrl);

      const parsed = new URL(baseUrl);
      parsed.protocol = parsed.protocol === "http:" ? "ws:" : "wss:";
      parsed.pathname = "/";
      parsed.search = "";
      parsed.hash = "";
      const pairingCode = generatePairingCode();
      await db.collection("device_pairings").doc(pairingCode).set({
        ownerUid: req.firebaseUser.uid,
        ownerEmail: req.firebaseUser.email || null,
        status: "pending",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        expiresAt: admin.firestore.Timestamp.fromMillis(Date.now() + 10 * 60 * 1000),
      });

      return res.json({
        ownerUid: req.firebaseUser.uid,
        ownerEmail: req.firebaseUser.email || null,
        gatewayUrl: parsed.toString(),
        pairingCode,
      });
    } catch (error) {
      console.error("Failed to generate pairing config:", error);
      return res.status(500).json({ error: "Failed to generate pairing config" });
    }
  });

  app.delete("/api/messages/:messageId", requireFirebaseUser, async (req: any, res) => {
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const requesterUid = req.firebaseUser.uid;
      const role = await getUserRole(db, requesterUid);
      const messageRef = db.collection("message_logs").doc(req.params.messageId);
      const messageDoc = await messageRef.get();
      if (!messageDoc.exists) {
        return res.status(404).json({ error: "Message not found" });
      }

      const messageData = messageDoc.data();
      const isOwner = messageData?.createdBy === requesterUid;
      if (!isOwner && role !== "admin") {
        return res.status(403).json({ error: "Not authorized to delete this message" });
      }

      await messageRef.delete();
      return res.json({ success: true, messageId: req.params.messageId });
    } catch (error) {
      console.error("Failed to delete message:", error);
      return res.status(500).json({ error: "Failed to delete message" });
    }
  });

  app.post("/api/messages/bulk-delete", requireFirebaseUser, async (req: any, res) => {
    try {
      const messageIds: string[] = Array.isArray(req.body?.messageIds)
        ? (req.body.messageIds as unknown[])
            .filter((id: unknown): id is string => typeof id === "string" && id.trim().length > 0)
            .map((id) => id.trim())
        : [];
      if (messageIds.length === 0) {
        return res.status(400).json({ error: "messageIds is required" });
      }

      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.status(500).json({ error: "Firebase Admin is not configured" });
      }

      const db = adminApp.firestore();
      const requesterUid = req.firebaseUser.uid;
      const role = await getUserRole(db, requesterUid);
      const uniqueIds = Array.from(new Set(messageIds.map((id: string) => id.trim()))).slice(0, 500);
      const deletedIds: string[] = [];
      const forbiddenIds: string[] = [];

      for (const messageId of uniqueIds) {
        const messageRef = db.collection("message_logs").doc(messageId);
        const messageDoc = await messageRef.get();
        if (!messageDoc.exists) {
          continue;
        }
        const messageData = messageDoc.data();
        const isOwner = messageData?.createdBy === requesterUid;
        if (!isOwner && role !== "admin") {
          forbiddenIds.push(messageId);
          continue;
        }
        await messageRef.delete();
        deletedIds.push(messageId);
      }

      return res.json({
        success: true,
        deletedCount: deletedIds.length,
        deletedIds,
        forbiddenIds,
      });
    } catch (error) {
      console.error("Failed to bulk delete messages:", error);
      return res.status(500).json({ error: "Failed to bulk delete messages" });
    }
  });

  // Device Registration
  app.post("/api/devices/register", authenticateFirebaseUserIfPresent, authenticateExternalRequest, async (req: any, res) => {
    const { deviceId, model, androidVersion, batteryLevel, name, ownerUid, pairingCode } = req.body;
    if (!deviceId) return res.status(400).json({ error: "deviceId is required" });

    try {
      const adminApp = getFirebaseAdmin();
      if (adminApp) {
        const db = adminApp.firestore();
        const existingDeviceDoc = await db.collection("devices").doc(deviceId).get();
        const existingOwnerUid = existingDeviceDoc.exists ? existingDeviceDoc.data()?.ownerUid || null : null;
        const normalizedPairingCode = typeof pairingCode === "string" && pairingCode.trim() ? pairingCode.trim() : null;
        const trustedOwnerUid = req.externalAuth?.authType === "user_api_key"
          ? req.externalAuth.ownerUid
          : req.firebaseUser?.uid || null;
        const requestedOwnerUid = typeof ownerUid === "string" && ownerUid.trim() ? ownerUid.trim() : null;
        let pairedOwnerUid: string | null = null;

        if (normalizedPairingCode) {
          const pairingRef = db.collection("device_pairings").doc(normalizedPairingCode);
          const pairingDoc = await pairingRef.get();
          if (!pairingDoc.exists) {
            return res.status(401).json({ error: "Invalid pairing code" });
          }
          const pairingData = pairingDoc.data();
          const expiresAtMillis = pairingData?.expiresAt?.toMillis?.() || 0;
          if (pairingData?.status !== "pending" || expiresAtMillis <= Date.now()) {
            return res.status(401).json({ error: "Pairing code expired or already used" });
          }
          pairedOwnerUid = pairingData?.ownerUid || null;
        }

        const claimedOwnerUid = trustedOwnerUid || pairedOwnerUid || requestedOwnerUid;

        if (!existingOwnerUid && !trustedOwnerUid && !pairedOwnerUid && req.externalAuth?.authType !== "trusted_system") {
          return res.status(401).json({
            error: "First-time device claiming requires Firebase authentication, a valid pairing code, or the trusted gateway API key",
          });
        }

        if (req.externalAuth?.authType === "trusted_system" && !requestedOwnerUid && !pairedOwnerUid && !existingOwnerUid) {
          return res.status(400).json({ error: "Trusted system registration requires ownerUid or pairingCode for first-time pairing" });
        }

        if (trustedOwnerUid && requestedOwnerUid && trustedOwnerUid !== requestedOwnerUid) {
          return res.status(403).json({ error: "Authenticated owner does not match the requested ownerUid" });
        }

        if (pairedOwnerUid && requestedOwnerUid && pairedOwnerUid !== requestedOwnerUid) {
          return res.status(403).json({ error: "Pairing code owner does not match the requested ownerUid" });
        }

        if (existingOwnerUid && claimedOwnerUid && existingOwnerUid !== claimedOwnerUid) {
          return res.status(409).json({ error: "Device is already claimed by a different owner" });
        }
        
        const deviceData: any = {
          name: name || model || "Unknown Device",
          model: model || "Unknown",
          androidVersion: androidVersion || "Unknown",
          batteryLevel: batteryLevel || 100,
          lastSeen: admin.firestore.FieldValue.serverTimestamp(),
          status: "online"
        };

        if (existingOwnerUid || claimedOwnerUid) {
          deviceData.ownerUid = existingOwnerUid || claimedOwnerUid;
        }

        await db.collection("devices").doc(deviceId).set(deviceData, { merge: true });

        if (normalizedPairingCode && pairedOwnerUid) {
          await db.collection("device_pairings").doc(normalizedPairingCode).set({
            status: "used",
            deviceId,
            usedAt: admin.firestore.FieldValue.serverTimestamp(),
          }, { merge: true });
        }
        
        console.log(`Device registered via Admin SDK: ${deviceId} (${model})`);
      } else {
        console.log(`Device registration (mock): ${deviceId} (${model})`);
      }
      
      res.json({ success: true, message: "Device registered successfully" });
    } catch (error) {
      console.error("Registration error:", error);
      res.status(500).json({ error: "Failed to register device" });
    }
  });

  // Device Heartbeat
  app.post("/api/devices/heartbeat", async (req, res) => {
    const { deviceId, status, batteryLevel, model, fcmToken, ownerUid } = req.body;
    
    if (!deviceId) return res.status(400).json({ error: "deviceId is required" });

    try {
      lastHeartbeat.set(deviceId, Date.now());
      const adminApp = getFirebaseAdmin();
      if (adminApp) {
        const db = adminApp.firestore();
        await persistDevicePresence(db, deviceId, {
          status,
          batteryLevel,
          model,
          fcmToken,
          ownerUid,
        });
      }
      
      res.json({ success: true, nextCheckIn: 300 });
    } catch (error) {
      console.error(`Heartbeat error for ${deviceId}:`, error);
      res.status(500).json({ error: "Failed to process heartbeat" });
    }
  });

  // Fetch Pending Messages (for devices) - Enterprise Grade Transactional Locking
  app.get("/api/messages/pending/:deviceId", async (req, res) => {
    const { deviceId } = req.params;
    lastHeartbeat.set(deviceId, Date.now());
    const requestedBatchSize = Number.parseInt(String(req.query.batchSize || ""), 10);
    const batchSize = Number.isFinite(requestedBatchSize)
      ? Math.min(Math.max(requestedBatchSize, 1), 100)
      : 50;
    logTrace("pending.fetch.start", { deviceId, batchSize });
    
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        logTrace("pending.fetch.mock", { deviceId, batchSize });
        return res.json({ messages: [], count: 0 });
      }
      
      const db = adminApp.firestore();
      const messages: any[] = [];
      
      // Get device info to know the owner
      const deviceDoc = await db.collection("devices").doc(deviceId).get();
      const ownerUid = deviceDoc.exists ? deviceDoc.data()?.ownerUid : null;
      logTrace("pending.fetch.device_context", { deviceId, ownerUid, batchSize });
      
      // Use a transaction to prevent race conditions if multiple devices poll simultaneously
      await db.runTransaction(async (transaction) => {
        // 1. Query for messages specifically assigned to this device
        const assignedQuery = db.collection("message_logs")
          .where("status", "in", ["pending", "queued"])
          .where("channel", "==", "sms")
          .where("deviceId", "==", deviceId)
          .limit(batchSize);
          
        const assignedSnapshot = await transaction.get(assignedQuery);
        
        // 2. Query for unassigned messages for this owner
        let unassignedDocs: any[] = [];
        if (ownerUid) {
          const unassignedQuery = db.collection("message_logs")
            .where("status", "in", ["pending", "queued"])
            .where("channel", "==", "sms")
            .where("createdBy", "==", ownerUid)
            .limit(batchSize * 2); // Fetch a bit more to filter in memory
            
          const tempSnapshot = await transaction.get(unassignedQuery);
          unassignedDocs = tempSnapshot.docs.filter(d => !d.data().deviceId);
        }

        if (assignedSnapshot.empty && unassignedDocs.length === 0) return;

        logTrace("pending.fetch.candidates", {
          deviceId,
          ownerUid,
          assignedCount: assignedSnapshot.size,
          unassignedCount: unassignedDocs.length,
          batchSize,
        });

        const docsToProcess = [...assignedSnapshot.docs, ...unassignedDocs].slice(0, batchSize);

        // 3. Lock them and prepare response
        for (const doc of docsToProcess) {
          const msgData = doc.data();
          // Double check status in case it changed
          if (msgData.status === "pending" || msgData.status === "queued") {
            logTrace("pending.fetch.lock_message", {
              deviceId,
              ownerUid,
              messageId: doc.id,
              previousStatus: msgData.status,
              recipient: msgData.recipient || null,
              assignedToDevice: msgData.deviceId || null,
            });
            messages.push({
              id: doc.id,
              messageId: doc.id, // Android app might expect messageId
              phone: msgData.recipient,
              message: msgData.body,
              recipient: msgData.recipient,
              body: msgData.body,
              task: "SEND_SMS", // Hint for Android app
              templateName: msgData.templateName || "direct"
            });

            // Lock the message by marking it as 'processing' and assigning it to this device
            transaction.update(doc.ref, {
              status: "processing",
              deviceId: deviceId,
              updatedAt: admin.firestore.FieldValue.serverTimestamp()
            });
          }
        }
      });

      res.json({ 
        messages, 
        count: messages.length 
      });
      logTrace("pending.fetch.complete", {
        deviceId,
        ownerUid,
        count: messages.length,
        messageIds: messages.map((entry) => entry.id),
      });
    } catch (error) {
      logErrorTrace("pending.fetch.failed", error, { deviceId, batchSize });
      res.status(500).json({ error: "Failed to fetch pending messages" });
    }
  });

  // Update Message Status
  app.post("/api/messages/update", async (req, res) => {
    const { messageId, status, error } = req.body;
    
    try {
      const adminApp = getFirebaseAdmin();
      if (adminApp) {
        const db = adminApp.firestore();
        await db.collection("message_logs").doc(messageId).update({
          status: status, // e.g., 'sent' or 'failed'
          error: error || null,
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });
      }
      console.log(`Message ${messageId} updated to ${status}${error ? `: ${error}` : ""}`);
      res.json({ success: true });
    } catch (err) {
      console.error("Error updating message status:", err);
      res.status(500).json({ error: "Failed to update message status" });
    }
  });

  // Delivery Report API
  app.post("/api/messages/delivery-report", async (req, res) => {
    const { messageId, status, error, deviceId } = req.body;
    
    if (!messageId || !status) {
      return res.status(400).json({ error: "messageId and status are required" });
    }

    try {
      const adminApp = getFirebaseAdmin();
      if (adminApp) {
        const db = adminApp.firestore();
        const updateData: any = {
          status: status, // 'delivered' or 'failed'
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        };
        
        if (status === 'delivered') {
          updateData.deliveredAt = admin.firestore.FieldValue.serverTimestamp();
        }
        
        if (error) {
          updateData.error = error;
        }

        await db.collection("message_logs").doc(messageId).update(updateData);
        
        // Log activity
        const docRef = await db.collection("message_logs").doc(messageId).get();
        const ownerUid = docRef.exists ? docRef.data()?.createdBy : null;
        logActivity("delivery_report", `Message ${messageId} delivery status: ${status}`, deviceId || null, ownerUid);
      }
      
      console.log(`Delivery report for ${messageId}: ${status}${error ? ` (${error})` : ""}`);
      await enqueuePayGatewayDeliveryCallback(String(messageId), String(status), error ? String(error) : undefined);
      res.json({ success: true });
    } catch (err) {
      console.error("Error processing delivery report:", err);
      res.status(500).json({ error: "Failed to process delivery report" });
    }
  });

  app.post("/api/messages/inbound", async (req, res) => {
    const { eventId, sender, body, deviceId, ownerUid, timestampMs } = req.body;

    if (!eventId || !sender || !body || !deviceId) {
      return res.status(400).json({
        error: "eventId, sender, body, and deviceId are required",
      });
    }

    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.json({ success: true, messageId: eventId, message: "Inbound message accepted (mock)" });
      }

      const db = adminApp.firestore();
      const deviceDoc = await db.collection("devices").doc(deviceId).get();
      const resolvedOwnerUid =
        (deviceDoc.exists ? deviceDoc.data()?.ownerUid || null : null) ||
        (typeof ownerUid === "string" && ownerUid.trim() ? ownerUid.trim() : null);

      const messageRef = db.collection("message_logs").doc(eventId);
      const existingDoc = await messageRef.get();
      if (existingDoc.exists) {
        return res.json({
          success: true,
          duplicate: true,
          messageId: existingDoc.id,
          message: "Inbound message already recorded",
        });
      }

      const inboundLog: any = {
        recipient: sender,
        sender,
        body,
        status: "received",
        channel: "sms",
        direction: "inbound",
        messageType: "inbound",
        source: "device_inbox",
        deviceId,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      if (resolvedOwnerUid) {
        inboundLog.createdBy = resolvedOwnerUid;
        inboundLog.ownerUid = resolvedOwnerUid;
      }
      if (timestampMs != null) {
        const normalizedTs = Number(timestampMs);
        if (Number.isFinite(normalizedTs) && normalizedTs > 0) {
          inboundLog.deviceTimestamp = admin.firestore.Timestamp.fromMillis(normalizedTs);
        }
      }

      await messageRef.set(inboundLog, { merge: false });
      await logActivity(
        "incoming_sms",
        `Inbound SMS received from ${sender} on device ${deviceId}`,
        deviceId,
        resolvedOwnerUid,
      );

      return res.json({
        success: true,
        messageId: eventId,
        ownerUid: resolvedOwnerUid,
      });
    } catch (error) {
      console.error("Error processing inbound SMS:", error);
      return res.status(500).json({ error: "Failed to process inbound SMS" });
    }
  });

  // Send Template API
  app.post("/api/send-template", authenticateFirebaseUserIfPresent, authenticateExternalRequest, requireAuthenticatedSender, requireCredentialScope(["send_template"]), async (req: any, res) => {
    const {
      templateName,
      recipient,
      data,
      channel,
      language,
      messageType,
      ownerUid,
      ownerEmail,
      deviceId,
      requestId,
      brand,
      brandCode,
      senderName,
      fromName,
      sender,
      platformName,
      senderEmail,
      fromEmail,
      replyTo,
      logoUrl,
      imageUrl,
      imageUrls,
    } = req.body;
    const templateData = data && typeof data === "object"
      ? data as Record<string, unknown>
      : {};
    const requestBrand = brand && typeof brand === "object"
      ? brand as Record<string, unknown>
      : {};
    let resolvedSenderName = firstEmailDisplayName(
      templateData.businessName,
      templateData.business_name,
      templateData.merchantName,
      templateData.merchant_name,
      requestBrand.displayName,
      senderName,
      fromName,
      sender,
      platformName,
      templateData.actorLabel,
      templateData.actor_label,
      templateData.brandDisplayName,
      templateData.brand_display_name,
    );
    const resolvedBrandCode = firstEmailDisplayName(
      brandCode,
      requestBrand.code,
      templateData.brandCode,
      templateData.brand_code,
    );
    const merchantBrandRequested =
      String(requestBrand.source || "").toLowerCase() === "merchant"
      || String(resolvedBrandCode || "").toUpperCase().startsWith("MERCHANT_")
      || Boolean(
        templateData.businessName
        || templateData.business_name
        || templateData.merchantName
        || templateData.merchant_name,
      );

    logTrace("send_template.request", {
      templateName,
      recipient,
      channel,
      language: language || null,
      messageType: messageType || null,
      requestedOwnerUid: ownerUid || null,
      requestedOwnerEmail: ownerEmail || null,
      requestedDeviceId: deviceId || null,
      requestId: requestId || null,
      brandCode: resolvedBrandCode || null,
      senderName: resolvedSenderName || null,
      authType: req.externalAuth?.authType || (req.firebaseUser ? "firebase_user" : "anonymous"),
    });

    if (!templateName || !recipient || !channel) {
      return res.status(400).json({ error: "templateName, recipient, and channel are required" });
    }
    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        // Mock behavior if no Firebase Admin
        console.log(`[MOCK] Sending template ${templateName} to ${recipient} via ${channel}`);
        return res.json({ success: true, messageId: "mock-id", message: "Message queued (mock)" });
      }

      const db = adminApp.firestore();
      const isTrustedSystem = req.externalAuth?.authType === "trusted_system";
      if (isTrustedSystem && !ownerUid && !ownerEmail && !deviceId) {
        return res.status(400).json({
          error: "Trusted system requests must include ownerUid, ownerEmail, or deviceId",
        });
      }
      const resolvedOwner = await resolveOwnerContext(db, {
        ownerUid: req.externalAuth?.ownerUid || req.firebaseUser?.uid || ownerUid,
        ownerEmail: req.externalAuth?.ownerEmail || req.firebaseUser?.email || ownerEmail,
        deviceId,
      });
      logTrace("send_template.owner_resolved", {
        templateName,
        recipient,
        channel,
        resolvedOwnerUid: resolvedOwner.ownerUid || null,
        resolvedOwnerEmail: resolvedOwner.ownerEmail || null,
      });
      
      // Find template
      let templateQuery = db.collection("message_templates")
        .where("name", "==", templateName)
        .where("channel", "==", channel);

      if (resolvedOwner.ownerUid) {
        templateQuery = templateQuery.where("createdBy", "==", resolvedOwner.ownerUid);
      }
        
      if (language) {
        templateQuery = templateQuery.where("language", "==", language);
      }

      const snapshot = await templateQuery.limit(1).get();

      if (snapshot.empty) {
        const scope = resolvedOwner.ownerUid ? ` for owner ${resolvedOwner.ownerUid}` : "";
        return res.status(404).json({ error: `Template '${templateName}' not found for channel '${channel}'${scope}` });
      }

      const template = snapshot.docs[0].data();
      const renderedTemplate = renderTemplateContent(
        template,
        templateData,
      );
      resolvedSenderName = resolvedSenderName || renderedTemplate.senderName;
      if (channel === "email" && merchantBrandRequested && isGenericMerchantDisplayName(resolvedSenderName)) {
        return res.status(400).json({
          error: "MERCHANT_NOTIFICATION_BRAND_REQUIRED",
          message: "Merchant email templates require a resolved business name.",
        });
      }
      const parsedBody = renderedTemplate.body;
      let resolvedLogoUrl: string | undefined;
      let resolvedImageUrls: string[] = [];
      if (channel === "email") {
        try {
          resolvedLogoUrl = normalizeEmailImageUrl(
            logoUrl
            || requestBrand.logoUrl
            || templateData.logoUrl
            || templateData.logo_url
            || renderedTemplate.logoUrl,
          );
          resolvedImageUrls = normalizeEmailImageUrls(
            imageUrls,
            imageUrl,
            templateData.imageUrls,
            templateData.image_urls,
            templateData.imageUrl,
            templateData.image_url,
            renderedTemplate.imageUrls,
          );
        } catch (error: any) {
          return res.status(400).json({
            error: error?.message || "EMAIL_IMAGE_URL_INVALID",
            message: "Email images must use HTTPS URLs from an approved image host.",
          });
        }
      }
      const resolvedSenderEmail = firstEmailAddress(
        senderEmail,
        fromEmail,
        requestBrand.senderEmail,
        requestBrand.fromEmail,
        template.fromEmail,
        template.senderEmail,
      );
      const resolvedReplyTo = firstEmailDisplayName(
        replyTo,
        requestBrand.replyTo,
        template.replyTo,
      );

      // Determine device to use
      let targetDeviceId = deviceId;
      const resolvedOwnerUid = resolvedOwner.ownerUid || template.createdBy;
      if (channel === 'sms' && !targetDeviceId && !resolvedOwnerUid) {
        return res.status(400).json({
          error: "ownerUid, ownerEmail, or deviceId is required for SMS template dispatch",
        });
      }
      if (!targetDeviceId && channel === 'sms') {
        if (resolvedOwnerUid) {
          targetDeviceId = await findBestDeviceForOwner(db, resolvedOwnerUid);
        }
      }
      logTrace("send_template.device_selected", {
        templateName,
        recipient,
        channel,
        resolvedOwnerUid: resolvedOwnerUid || null,
        targetDeviceId: targetDeviceId || null,
      });

      if (channel === 'sms' && !targetDeviceId) {
        console.warn(
          `[Dispatch] No live SMS device resolved for template message. ownerUid=${resolvedOwnerUid || 'none'} recipient=${recipient}. Queueing for pending pickup.`,
        );
      }

      const messageSource = req.externalAuth?.authType || (req.firebaseUser ? "firebase_user" : "anonymous");
      let messageId = "";
      let duplicateMessage: { id: string; data: any } | null = null;
      let createdNew = false;

      if (requestId?.trim() && resolvedOwnerUid) {
        const normalizedRequestId = requestId.trim();
        const idempotencyRef = db.collection("message_request_index").doc(
          buildIdempotencyDocId(resolvedOwnerUid, normalizedRequestId),
        );

        await db.runTransaction(async (transaction) => {
          const idempotencyDoc = await transaction.get(idempotencyRef);
          if (idempotencyDoc.exists) {
            const existingMessageId = idempotencyDoc.data()?.messageId;
            if (existingMessageId) {
              const existingRef = db.collection("message_logs").doc(existingMessageId);
              const existingDoc = await transaction.get(existingRef);
              if (existingDoc.exists) {
                duplicateMessage = { id: existingDoc.id, data: existingDoc.data() };
                messageId = existingDoc.id;
                return;
              }
            }
          }

          const docRef = db.collection("message_logs").doc();
          messageId = docRef.id;
          const messageLog: any = {
            templateName,
            recipient,
            body: parsedBody,
            status: "pending",
            channel,
            ...(renderedTemplate.components.length > 0
              ? { components: renderedTemplate.components }
              : {}),
            messageType: messageType || template.messageType || "transactional",
            createdBy: resolvedOwnerUid,
            source: messageSource,
            ...(channel === "email" && resolvedSenderName ? { senderName: resolvedSenderName } : {}),
            ...(channel === "email" && resolvedSenderEmail ? { fromEmail: resolvedSenderEmail } : {}),
            ...(channel === "email" && resolvedReplyTo ? { replyTo: resolvedReplyTo } : {}),
            ...(channel === "email" && resolvedBrandCode ? { brandCode: resolvedBrandCode } : {}),
            ...(channel === "email" && resolvedLogoUrl ? { logoUrl: resolvedLogoUrl } : {}),
            ...(channel === "email" && resolvedImageUrls.length ? { imageUrls: resolvedImageUrls } : {}),
            requestId: normalizedRequestId,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            timestamp: admin.firestore.FieldValue.serverTimestamp(),
          };
          if (targetDeviceId) {
            messageLog.deviceId = targetDeviceId;
          }
          transaction.set(docRef, messageLog);
          transaction.set(idempotencyRef, {
            ownerUid: resolvedOwnerUid,
            requestId: normalizedRequestId,
            messageId,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          createdNew = true;
        });
      } else {
        const docRef = db.collection("message_logs").doc();
        messageId = docRef.id;
        const messageLog: any = {
          templateName,
          recipient,
          body: parsedBody,
          status: "pending",
          channel,
          ...(renderedTemplate.components.length > 0
            ? { components: renderedTemplate.components }
            : {}),
          messageType: messageType || template.messageType || "transactional",
          createdBy: resolvedOwnerUid,
          source: messageSource,
          ...(channel === "email" && resolvedSenderName ? { senderName: resolvedSenderName } : {}),
          ...(channel === "email" && resolvedSenderEmail ? { fromEmail: resolvedSenderEmail } : {}),
          ...(channel === "email" && resolvedReplyTo ? { replyTo: resolvedReplyTo } : {}),
          ...(channel === "email" && resolvedBrandCode ? { brandCode: resolvedBrandCode } : {}),
          ...(channel === "email" && resolvedLogoUrl ? { logoUrl: resolvedLogoUrl } : {}),
          ...(channel === "email" && resolvedImageUrls.length ? { imageUrls: resolvedImageUrls } : {}),
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          timestamp: admin.firestore.FieldValue.serverTimestamp(),
        };
        if (targetDeviceId) {
          messageLog.deviceId = targetDeviceId;
        }
        await docRef.set(messageLog);
        createdNew = true;
      }

      if (duplicateMessage) {
        logTrace("send_template.duplicate", {
          templateName,
          recipient,
          channel,
          messageId: duplicateMessage.id,
          status: duplicateMessage.data.status || null,
          deviceId: duplicateMessage.data.deviceId || null,
        });
        return res.json({
          success: true,
          duplicate: true,
          messageId: duplicateMessage.id,
          ownerUid: resolvedOwnerUid || null,
          status: duplicateMessage.data.status || null,
          deviceId: duplicateMessage.data.deviceId || null,
          message: "Duplicate requestId detected; returning existing message",
        });
      }

      const docRef = db.collection("message_logs").doc(messageId);

      // Real-time push via WebSocket if device is connected
      let pushed = false;
      let emailSent = false;
      let dispatchReason: string | null = null;
      if (createdNew && channel === "sms" && !targetDeviceId) {
        dispatchReason = "No live device resolved; queued for pending pickup";
      }
      logTrace("send_template.message_created", {
        templateName,
        recipient,
        channel,
        messageId,
        createdNew,
        targetDeviceId: targetDeviceId || null,
        dispatchReason,
      });
      if (createdNew && targetDeviceId) {
        const dispatchResult = notifyDevice(targetDeviceId, {
          id: messageId,
          recipient,
          body: parsedBody,
          channel,
          ...(renderedTemplate.components.length > 0
            ? { components: renderedTemplate.components }
            : {}),
        });
        pushed = dispatchResult.pushed;
        dispatchReason = dispatchResult.reason;
        
        if (pushed) {
          await docRef.update({
            status: "queued",
            pushedAt: admin.firestore.FieldValue.serverTimestamp()
          });
          logActivity("message_pushed", `Message ${messageId} pushed to device ${targetDeviceId}`, targetDeviceId, resolvedOwnerUid || null);
        } else {
          await docRef.update({
            dispatchError: dispatchReason,
            lastDispatchAttemptAt: admin.firestore.FieldValue.serverTimestamp(),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
        }
        logTrace("send_template.dispatch_attempt", {
          templateName,
          recipient,
          channel,
          messageId,
          targetDeviceId,
          pushed,
          dispatchReason,
        });
      }

      if (createdNew && channel === "email") {
        try {
          const emailResult = await sendEmailDelivery({
            to: recipient,
            subject: renderedTemplate.subject || template.subject || "ORBI Notification",
            from: resolvedSenderEmail,
            fromName: resolvedSenderName,
            replyTo: resolvedReplyTo,
            text: parsedBody,
            html: renderedTemplate.components.find((component: any) => component?.type === "html")?.text,
            logoUrl: resolvedLogoUrl,
            imageUrls: resolvedImageUrls,
            messageId,
          });
          emailSent = true;
          await docRef.update({
            status: "sent",
            sentAt: admin.firestore.FieldValue.serverTimestamp(),
            provider: emailResult.provider,
            providerMessageId: emailResult.providerMessageId,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            error: admin.firestore.FieldValue.delete(),
            dispatchError: admin.firestore.FieldValue.delete(),
          });
          logActivity("email_sent", `Email ${messageId} sent to ${recipient}`, null, resolvedOwnerUid || null);
        } catch (error: any) {
          dispatchReason = error?.message || "Email delivery failed";
          await docRef.update({
            status: "failed",
            error: dispatchReason,
            dispatchError: dispatchReason,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          throw error;
        }
      }

      res.json({ 
        success: true, 
        messageId,
        pushed,
        emailSent,
        dispatchReason,
        source: messageSource,
        ownerUid: resolvedOwnerUid || null,
        message: emailSent
          ? "Email sent"
          : pushed
          ? "Message pushed to device"
          : (dispatchReason
              ? `Message queued; live dispatch unavailable: ${dispatchReason}`
              : "Message queued successfully")
      });
    } catch (error) {
      logErrorTrace("send_template.failed", error, {
        templateName,
        recipient,
        channel,
        requestId: requestId || null,
      });
      res.status(500).json({ error: "Failed to process template" });
    }
  });

  const handleDirectMessageRequest = async (
    req: any,
    res: any,
    options: {
      channelOverride?: "sms" | "email" | "push" | "whatsapp";
      requireTemplateChannel?: "email" | "push" | "whatsapp";
    } = {},
  ) => {
    const {
      recipient,
      body,
      channel: rawChannel = "sms",
      ownerUid,
      ownerEmail,
      deviceId,
      messageType = "transactional",
      requestId,
      subject,
      fromEmail,
      senderEmail,
      senderName,
      fromName,
      sender,
      platformName,
      brand,
      brandCode,
      replyTo,
      logoUrl,
      imageUrl,
      imageUrls,
    } = req.body;
    const channel = options.channelOverride || rawChannel;
    const requestBrand = brand && typeof brand === "object"
      ? brand as Record<string, unknown>
      : {};
    const resolvedSenderName = firstEmailDisplayName(
      requestBrand.displayName,
      senderName,
      fromName,
      sender,
      platformName,
    );
    const resolvedSenderEmail = firstEmailAddress(
      senderEmail,
      fromEmail,
      requestBrand.senderEmail,
      requestBrand.fromEmail,
    );
    const resolvedBrandCode = firstEmailDisplayName(brandCode, requestBrand.code);
    let resolvedLogoUrl: string | undefined;
    let resolvedImageUrls: string[] = [];
    if (channel === "email") {
      try {
        resolvedLogoUrl = normalizeEmailImageUrl(logoUrl || requestBrand.logoUrl);
        resolvedImageUrls = normalizeEmailImageUrls(imageUrls, imageUrl);
      } catch (error: any) {
        return res.status(400).json({
          error: error?.message || "EMAIL_IMAGE_URL_INVALID",
          message: "Email images must use HTTPS URLs from an approved image host.",
        });
      }
    }
    logTrace("send_message.request", {
      recipient,
      channel,
      bodyLength: typeof body === "string" ? body.length : null,
      requestedOwnerUid: ownerUid || null,
      requestedOwnerEmail: ownerEmail || null,
      requestedDeviceId: deviceId || null,
      messageType,
      requestId: requestId || null,
      subject: subject || null,
      fromEmail: resolvedSenderEmail || null,
      senderName: resolvedSenderName || null,
      brandCode: resolvedBrandCode || null,
      replyTo: replyTo || null,
      logoUrl: resolvedLogoUrl || null,
      imageCount: resolvedImageUrls.length,
      authType: req.externalAuth?.authType || (req.firebaseUser ? "firebase_user" : "anonymous"),
    });

    if (!recipient || !body) {
      return res.status(400).json({ error: "recipient and body are required" });
    }

    if (options.requireTemplateChannel) {
      return res.status(400).json({
        error: `${options.requireTemplateChannel.toUpperCase()}_TEMPLATE_REQUIRED`,
        message: `Direct ${options.requireTemplateChannel} delivery is not supported on this endpoint. Use /api/send-template with channel='${options.requireTemplateChannel}'.`,
      });
    }

    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.json({ success: true, messageId: "mock-id", message: "Message queued (mock)" });
      }

      const db = adminApp.firestore();
      const isTrustedSystem = req.externalAuth?.authType === "trusted_system";
      if (isTrustedSystem && !ownerUid && !ownerEmail && !deviceId) {
        return res.status(400).json({
          error: "Trusted system requests must include ownerUid, ownerEmail, or deviceId",
        });
      }
      const resolvedOwner = await resolveOwnerContext(db, {
        ownerUid: req.externalAuth?.ownerUid || req.firebaseUser?.uid || ownerUid,
        ownerEmail: req.externalAuth?.ownerEmail || req.firebaseUser?.email || ownerEmail,
        deviceId,
      });
      logTrace("send_message.owner_resolved", {
        recipient,
        channel,
        resolvedOwnerUid: resolvedOwner.ownerUid || null,
        resolvedOwnerEmail: resolvedOwner.ownerEmail || null,
      });
      
      // Determine device to use
      let targetDeviceId = deviceId;
      if (channel === 'sms' && !targetDeviceId && !resolvedOwner.ownerUid) {
        return res.status(400).json({
          error: "ownerUid, ownerEmail, or deviceId is required for SMS dispatch",
        });
      }
      if (!targetDeviceId && channel === 'sms' && resolvedOwner.ownerUid) {
        targetDeviceId = await findBestDeviceForOwner(db, resolvedOwner.ownerUid);
      }
      logTrace("send_message.device_selected", {
        recipient,
        channel,
        resolvedOwnerUid: resolvedOwner.ownerUid || null,
        targetDeviceId: targetDeviceId || null,
      });
      if (channel === 'sms' && !targetDeviceId) {
        console.warn(
          `[Dispatch] No live SMS device resolved for ownerUid=${resolvedOwner.ownerUid || 'none'} recipient=${recipient}. Queueing for pending pickup.`,
        );
      }

      const messageSource = req.externalAuth?.authType || (req.firebaseUser ? "firebase_user" : "anonymous");
      let messageId = "";
      let duplicateMessage: { id: string; data: any } | null = null;
      let createdNew = false;

      if (requestId?.trim() && resolvedOwner.ownerUid) {
        const normalizedRequestId = requestId.trim();
        const idempotencyRef = db.collection("message_request_index").doc(
          buildIdempotencyDocId(resolvedOwner.ownerUid, normalizedRequestId),
        );

        await db.runTransaction(async (transaction) => {
          const idempotencyDoc = await transaction.get(idempotencyRef);
          if (idempotencyDoc.exists) {
            const existingMessageId = idempotencyDoc.data()?.messageId;
            if (existingMessageId) {
              const existingRef = db.collection("message_logs").doc(existingMessageId);
              const existingDoc = await transaction.get(existingRef);
              if (existingDoc.exists) {
                duplicateMessage = { id: existingDoc.id, data: existingDoc.data() };
                messageId = existingDoc.id;
                return;
              }
            }
          }

          const docRef = db.collection("message_logs").doc();
          messageId = docRef.id;
          const messageLog: any = {
            recipient,
            body,
            ...(channel === "email" ? { subject: subject || "ORBI Notification" } : {}),
            ...(channel === "email" && resolvedSenderEmail ? { fromEmail: resolvedSenderEmail } : {}),
            ...(channel === "email" && resolvedSenderName ? { senderName: resolvedSenderName } : {}),
            ...(channel === "email" && resolvedBrandCode ? { brandCode: resolvedBrandCode } : {}),
            ...(channel === "email" && replyTo ? { replyTo } : {}),
            ...(channel === "email" && resolvedLogoUrl ? { logoUrl: resolvedLogoUrl } : {}),
            ...(channel === "email" && resolvedImageUrls.length ? { imageUrls: resolvedImageUrls } : {}),
            status: "pending",
            channel,
            messageType,
            createdBy: resolvedOwner.ownerUid || deviceId,
            source: messageSource,
            requestId: normalizedRequestId,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            timestamp: admin.firestore.FieldValue.serverTimestamp(),
          };
          if (targetDeviceId) {
            messageLog.deviceId = targetDeviceId;
          }
          transaction.set(docRef, messageLog);
          transaction.set(idempotencyRef, {
            ownerUid: resolvedOwner.ownerUid,
            requestId: normalizedRequestId,
            messageId,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          createdNew = true;
        });
      } else {
        const docRef = db.collection("message_logs").doc();
        messageId = docRef.id;
        const messageLog: any = {
          recipient,
          body,
          ...(channel === "email" ? { subject: subject || "ORBI Notification" } : {}),
          ...(channel === "email" && resolvedSenderEmail ? { fromEmail: resolvedSenderEmail } : {}),
          ...(channel === "email" && resolvedSenderName ? { senderName: resolvedSenderName } : {}),
          ...(channel === "email" && resolvedBrandCode ? { brandCode: resolvedBrandCode } : {}),
          ...(channel === "email" && replyTo ? { replyTo } : {}),
          ...(channel === "email" && resolvedLogoUrl ? { logoUrl: resolvedLogoUrl } : {}),
          ...(channel === "email" && resolvedImageUrls.length ? { imageUrls: resolvedImageUrls } : {}),
          status: "pending",
          channel,
          messageType,
          createdBy: resolvedOwner.ownerUid || deviceId,
          source: messageSource,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          timestamp: admin.firestore.FieldValue.serverTimestamp(),
        };
        if (requestId?.trim()) {
          messageLog.requestId = requestId.trim();
        }
        if (targetDeviceId) {
          messageLog.deviceId = targetDeviceId;
        }
        await docRef.set(messageLog);
        createdNew = true;
      }

      if (duplicateMessage) {
        logTrace("send_message.duplicate", {
          recipient,
          channel,
          messageId: duplicateMessage.id,
          status: duplicateMessage.data.status || null,
          deviceId: duplicateMessage.data.deviceId || null,
        });
        return res.json({
          success: true,
          duplicate: true,
          messageId: duplicateMessage.id,
          ownerUid: resolvedOwner.ownerUid || null,
          status: duplicateMessage.data.status || null,
          deviceId: duplicateMessage.data.deviceId || null,
          message: "Duplicate requestId detected; returning existing message",
        });
      }

      const docRef = db.collection("message_logs").doc(messageId);

      // Real-time push via WebSocket if device is connected
      let pushed = false;
      let emailSent = false;
      let dispatchReason: string | null = null;
      if (createdNew && channel === "sms" && !targetDeviceId) {
        dispatchReason = "No live device resolved; queued for pending pickup";
      }
      logTrace("send_message.message_created", {
        recipient,
        channel,
        messageId,
        createdNew,
        targetDeviceId: targetDeviceId || null,
        dispatchReason,
      });
      if (createdNew && targetDeviceId) {
        const dispatchResult = notifyDevice(targetDeviceId, {
          id: messageId,
          recipient,
          body,
          channel
        });
        pushed = dispatchResult.pushed;
        dispatchReason = dispatchResult.reason;
        
        if (pushed) {
          await docRef.update({
            status: "queued",
            pushedAt: admin.firestore.FieldValue.serverTimestamp()
          });
        } else {
          await docRef.update({
            dispatchError: dispatchReason,
            lastDispatchAttemptAt: admin.firestore.FieldValue.serverTimestamp(),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
        }
        logTrace("send_message.dispatch_attempt", {
          recipient,
          channel,
          messageId,
          targetDeviceId,
          pushed,
          dispatchReason,
        });
      }

      if (createdNew && channel === "email") {
        try {
          const emailResult = await sendEmailDelivery({
            to: recipient,
            from: resolvedSenderEmail,
            fromName: resolvedSenderName,
            subject: subject || "ORBI Notification",
            text: body,
            replyTo,
            logoUrl: resolvedLogoUrl,
            imageUrls: resolvedImageUrls,
            messageId,
          });
          emailSent = true;
          await docRef.update({
            status: "sent",
            sentAt: admin.firestore.FieldValue.serverTimestamp(),
            provider: emailResult.provider,
            providerMessageId: emailResult.providerMessageId,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            error: admin.firestore.FieldValue.delete(),
            dispatchError: admin.firestore.FieldValue.delete(),
          });
          logActivity("email_sent", `Email ${messageId} sent to ${recipient}`, null, resolvedOwner.ownerUid || null);
        } catch (error: any) {
          dispatchReason = error?.message || "Email delivery failed";
          await docRef.update({
            status: "failed",
            error: dispatchReason,
            dispatchError: dispatchReason,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          throw error;
        }
      }

      res.json({ 
        success: true, 
        messageId,
        pushed,
        emailSent,
        dispatchReason,
        source: messageSource,
        ownerUid: resolvedOwner.ownerUid || null,
        deviceId: targetDeviceId || null,
        message: emailSent
          ? "Email sent"
          : pushed
          ? "Message pushed to device"
          : (dispatchReason
              ? `Message queued; live dispatch unavailable: ${dispatchReason}`
              : "Message queued successfully")
      });
    } catch (error) {
      logErrorTrace("send_message.failed", error, {
        recipient,
        channel,
        requestId: requestId || null,
      });
      res.status(500).json({ error: "Failed to send message" });
    }
  };

  // Direct Send API (Raw SMS without templates)
  app.post(
    "/api/messages/send",
    authenticateFirebaseUserIfPresent,
    authenticateExternalRequest,
    requireAuthenticatedSender,
    requireCredentialScope(["send_sms"]),
    async (req: any, res) => handleDirectMessageRequest(req, res),
  );

  // Compatibility endpoint expected by ORBI Backend for direct SMS.
  app.post(
    "/api/send-sms",
    authenticateFirebaseUserIfPresent,
    authenticateExternalRequest,
    requireAuthenticatedSender,
    requireCredentialScope(["send_sms"]),
    async (req: any, res) =>
      handleDirectMessageRequest(req, res, { channelOverride: "sms" }),
  );

  // Direct email compatibility endpoint. Prefer /api/send-template for customer lifecycle messages.
  app.post(
    "/api/send-email",
    authenticateFirebaseUserIfPresent,
    authenticateExternalRequest,
    requireAuthenticatedSender,
    requireCredentialScope(["send_email"]),
    async (req: any, res) =>
      handleDirectMessageRequest(req, res, { channelOverride: "email" }),
  );

  // Native app push moved into ORBI Backend; keep compatibility endpoint explicit.
  app.post(
    "/api/send-push",
    authenticateFirebaseUserIfPresent,
    authenticateExternalRequest,
    requireAuthenticatedSender,
    requireCredentialScope(["send_push", "send_template"]),
    async (_req: any, res) =>
      res.status(410).json({
        error: "PUSH_DELIVERY_MOVED",
        message:
          "Mobile push delivery is handled directly by ORBI Backend via Firebase Admin. Use /api/send-template only for gateway-managed channels.",
      }),
  );

  // Hard Resend API (Retry failed or stuck queued messages)
  app.post("/api/messages/resend", authenticateFirebaseUserIfPresent, authenticateExternalRequest, requireAuthenticatedSender, async (req: any, res) => {
    const { messageId, deviceId } = req.body;

    if (!messageId) {
      return res.status(400).json({ error: "messageId is required" });
    }

    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.json({ success: true, message: "Message resend queued (mock)" });
      }

      const db = adminApp.firestore();
      const docRef = db.collection("message_logs").doc(messageId);
      
      let pushed = false;
      let dispatchReason: string | null = null;
      let targetDeviceId = deviceId;

      await db.runTransaction(async (transaction) => {
        const doc = await transaction.get(docRef);
        if (!doc.exists) {
          throw new Error("Message not found");
        }

        const msgData = doc.data();
        const requesterOwnerUid = req.externalAuth?.ownerUid || req.firebaseUser?.uid || null;
        const isTrustedSystem = req.externalAuth?.authType === "trusted_system";

        if (!isTrustedSystem && requesterOwnerUid && msgData?.createdBy && msgData.createdBy !== requesterOwnerUid) {
          throw new Error("Not authorized to resend this message");
        }
        
        // Only allow resending if it's not already successfully sent
        if (msgData?.status === "sent") {
          throw new Error("Message is already marked as sent");
        }

        // Determine target device (use provided, fallback to previous, or find a new one)
        targetDeviceId = targetDeviceId || msgData?.deviceId;

        if (!targetDeviceId && msgData?.createdBy) {
          targetDeviceId = await findBestDeviceForOwner(db, msgData.createdBy);
        }

        // Attempt real-time push
        if (targetDeviceId) {
          const dispatchResult = notifyDevice(targetDeviceId, {
            id: messageId,
            recipient: msgData?.recipient,
            body: msgData?.body,
            channel: msgData?.channel || "sms"
          });
          pushed = dispatchResult.pushed;
          dispatchReason = dispatchResult.reason;
        }

        const newStatus = pushed ? "queued" : "pending";
        const retryCount = (msgData?.retryCount || 0) + 1;

        const updateData: any = {
          status: newStatus,
          error: pushed
            ? admin.firestore.FieldValue.delete()
            : (dispatchReason || admin.firestore.FieldValue.delete()),
          retryCount: retryCount,
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        };

        if (targetDeviceId) {
          updateData.deviceId = targetDeviceId;
        }
        
        if (pushed) {
          updateData.pushedAt = admin.firestore.FieldValue.serverTimestamp();
        }
        updateData.lastDispatchAttemptAt = admin.firestore.FieldValue.serverTimestamp();

        transaction.update(docRef, updateData);
      });

      res.json({ 
        success: true, 
        messageId,
        pushed,
        dispatchReason,
        deviceId: targetDeviceId || null,
        message: pushed
          ? "Message successfully pushed to device for retry"
          : (dispatchReason
              ? `Message placed back in pending queue for retry: ${dispatchReason}`
              : "Message placed back in pending queue for retry")
      });

    } catch (error: any) {
      console.error("Error in /api/messages/resend:", error);
      res.status(500).json({ error: error.message || "Failed to resend message" });
    }
  });

  // Force resend all unsent SMS jobs for the authenticated owner or trusted system scope.
  app.post("/api/messages/force-resend-queue", authenticateFirebaseUserIfPresent, authenticateExternalRequest, requireAuthenticatedSender, async (req: any, res) => {
    const requestedLimit = Number.parseInt(String(req.body?.limit || ""), 10);
    const batchLimit = Number.isFinite(requestedLimit)
      ? Math.min(Math.max(requestedLimit, 1), FORCE_RESEND_BATCH_SIZE)
      : FORCE_RESEND_BATCH_SIZE;
    const requestedDeviceId = typeof req.body?.deviceId === "string" ? req.body.deviceId.trim() : "";
    const requestedOwnerUid = typeof req.body?.ownerUid === "string" ? req.body.ownerUid.trim() : "";
    const includeFailed = req.body?.includeFailed !== false;

    try {
      const adminApp = getFirebaseAdmin();
      if (!adminApp) {
        return res.json({
          success: true,
          message: "Queue resend accepted (mock)",
          scanned: 0,
          pushed: 0,
          pending: 0,
          skipped: 0,
        });
      }

      const requesterOwnerUid = req.externalAuth?.ownerUid || req.firebaseUser?.uid || "";
      const isTrustedSystem = req.externalAuth?.authType === "trusted_system";
      const scopedOwnerUid = isTrustedSystem ? (requestedOwnerUid || requesterOwnerUid) : requesterOwnerUid;

      if (!isTrustedSystem && !scopedOwnerUid) {
        return res.status(401).json({ error: "Authenticated owner is required to force resend queue" });
      }

      const dispatchResult = await dispatchUnsentQueue({
        reason: "manual_force_resend",
        ownerUid: scopedOwnerUid || undefined,
        deviceId: requestedDeviceId || undefined,
        limit: batchLimit,
        cooldownMs: 0,
        includeFailed,
        trustedSystem: isTrustedSystem,
      });

      res.json({
        success: true,
        ...dispatchResult,
        message: dispatchResult.pushed > 0
          ? `Force resend scanned ${dispatchResult.scanned} message(s); ${dispatchResult.pushed} pushed to live devices`
          : `Force resend scanned ${dispatchResult.scanned} message(s); waiting for device availability`,
      });
    } catch (error: any) {
      console.error("Error in /api/messages/force-resend-queue:", error);
      res.status(500).json({ error: error.message || "Failed to force resend queue" });
    }
  });

  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*all", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  // Create HTTP server
  const server = http.createServer(app);

  // Initialize WebSocket server
  const wss = new WebSocketServer({ server });

  const findBestDeviceForOwner = async (db: any, ownerUid: string) => {
    const onlineSnapshot = await db.collection("devices")
      .where("ownerUid", "==", ownerUid)
      .where("status", "==", "online")
      .limit(10)
      .get();

    const onlineConnected = onlineSnapshot.docs.find((doc) => {
      const ws = connectedDevices.get(doc.id);
      return ws?.readyState === WebSocket.OPEN;
    });
    if (onlineConnected) {
      return onlineConnected.id;
    }
    if (!onlineSnapshot.empty) {
      return onlineSnapshot.docs[0].id;
    }

    const ownerDevicesSnapshot = await db.collection("devices")
      .where("ownerUid", "==", ownerUid)
      .limit(20)
      .get();
    const connectedFallback = ownerDevicesSnapshot.docs.find((doc) => {
      const ws = connectedDevices.get(doc.id);
      return ws?.readyState === WebSocket.OPEN;
    });
    return connectedFallback?.id;
  };

  wss.on("connection", (ws, req) => {
    let deviceId: string | null = null;
    let model: string = "Unknown Device";
    let ownerUid: string | null = null;
    const adminApp = getFirebaseAdmin();

    console.log(`[WebSocket] New connection established, waiting for identification...`);
    logTrace("ws.connection.open", {
      remoteAddress: req.socket.remoteAddress || null,
      remotePort: req.socket.remotePort || null,
      url: req.url || null,
    });

    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString());
        
        // Handle identification/heartbeat to get deviceId
        if (data.type === "heartbeat" || data.task === "DEVICE_INFO") {
          const newDeviceId = data.deviceId;
          const incomingOwnerUid = normalizeOptionalString(data.ownerUid);
          
          if (!newDeviceId) {
            console.error("[WebSocket] Received message without deviceId");
            return;
          }

          // If this is the first time we see this deviceId, or it changed
          if (deviceId !== newDeviceId) {
            deviceId = newDeviceId;
            model = data.model || model;
            const deviceRef = adminApp?.firestore().collection("devices").doc(newDeviceId);
            const existingDeviceDoc = deviceRef ? await deviceRef.get() : null;
            const storedOwnerUid = existingDeviceDoc?.exists ? existingDeviceDoc.data()?.ownerUid || null : null;
            ownerUid = storedOwnerUid || incomingOwnerUid || ownerUid;
            
            console.log(`[WebSocket] Device identified: ${deviceId} (${model})`);
            connectedDevices.set(deviceId, ws);

            // Update device status in Firestore
            if (adminApp) {
              const db = adminApp.firestore();
              
              const deviceData: any = {
                status: "online",
                lastSeen: admin.firestore.FieldValue.serverTimestamp(),
                model: model
              };
              
              const deviceRef = db.collection("devices").doc(deviceId);
              const existingDeviceDoc = await deviceRef.get();
              const storedOwnerUid = existingDeviceDoc.exists ? existingDeviceDoc.data()?.ownerUid || null : null;
              const resolvedOwnerUid = storedOwnerUid || incomingOwnerUid || ownerUid;
              if (resolvedOwnerUid) {
                ownerUid = resolvedOwnerUid;
                deviceData.ownerUid = resolvedOwnerUid;
              }

              persistDevicePresence(db, deviceId, deviceData, { force: true })
                .catch(err => console.error("Error updating device status:", err));
            }

            dispatchUnsentQueue({
              reason: "device_online",
              ownerUid: ownerUid || incomingOwnerUid || undefined,
              deviceId,
              limit: DEVICE_ONLINE_FLUSH_BATCH_SIZE,
              cooldownMs: 0,
            }).catch(err => console.error("[Queue Auto Dispatch] Device-online flush failed:", err));
          }

          // Continue with existing heartbeat logic
          if (data.type === "heartbeat") {
            lastHeartbeat.set(deviceId, Date.now());
            ws.send(JSON.stringify({ type: "heartbeat_ack" }));
            
            if (adminApp) {
              const heartbeatOwnerUid = incomingOwnerUid || ownerUid;
              if (heartbeatOwnerUid && heartbeatOwnerUid !== ownerUid) {
                ownerUid = heartbeatOwnerUid;
              }
              persistDevicePresence(
                adminApp.firestore(),
                deviceId,
                {
                  status: "online",
                  batteryLevel: data.batteryLevel,
                  model: data.model,
                  fcmToken: data.fcmToken,
                  ownerUid: heartbeatOwnerUid,
                },
              ).catch(err => console.error("Error updating heartbeat:", err));
            }
          }
        } else if (data.type === "ping") {
          if (deviceId) {
            lastHeartbeat.set(deviceId, Date.now());
          }
          ws.send(JSON.stringify({ type: "pong", ts: Date.now() }));
        } else if (data.task === "DEVICE_INFO") {
          // Already handled above
        } else if (data.type === "message_status") {
          // Handle message status updates (sent, failed)
          logTrace("ws.message_status.received", {
            deviceId,
            ownerUid,
            messageId: data.messageId || null,
            status: data.status || null,
            error: data.error || null,
          });
          if (adminApp && data.messageId) {
            const docRef = adminApp.firestore().collection("message_logs").doc(data.messageId);
            docRef.update({
              status: data.status,
              error: data.error || null,
              updatedAt: admin.firestore.FieldValue.serverTimestamp()
            }).catch(err => console.error("Error updating message status:", err));
            
            // Try to get ownerUid from the message log for logging
            docRef.get().then(doc => {
              if (doc.exists) {
                logActivity("message_status", `Message ${data.messageId} status updated to ${data.status}`, deviceId, doc.data()?.createdBy);
              } else {
                logActivity("message_status", `Message ${data.messageId} status updated to ${data.status}`, deviceId, ownerUid);
              }
            }).catch(() => {
              logActivity("message_status", `Message ${data.messageId} status updated to ${data.status}`, deviceId, ownerUid);
            });
          }
        } else if (data.type === "delivery_report") {
          // Handle explicit delivery reports
          logTrace("ws.delivery_report.received", {
            deviceId,
            ownerUid,
            messageId: data.messageId || null,
            status: data.status || null,
            error: data.error || null,
          });
          if (adminApp && data.messageId) {
            const docRef = adminApp.firestore().collection("message_logs").doc(data.messageId);
            const updateData: any = {
              status: data.status, // 'delivered' or 'failed'
              updatedAt: admin.firestore.FieldValue.serverTimestamp()
            };
            
            if (data.status === 'delivered') {
              updateData.deliveredAt = admin.firestore.FieldValue.serverTimestamp();
            }
            if (data.error) {
              updateData.error = data.error;
            }
            
            docRef.update(updateData).catch(err => console.error("Error updating delivery report:", err));
            
            docRef.get().then(doc => {
              if (doc.exists) {
                logActivity("delivery_report", `Message ${data.messageId} delivery status: ${data.status}`, deviceId, doc.data()?.createdBy);
              } else {
                logActivity("delivery_report", `Message ${data.messageId} delivery status: ${data.status}`, deviceId, ownerUid);
              }
            }).catch(() => {});
          }
        } else if (data.type === "incoming_sms") {
          if (adminApp && data.eventId && data.sender && data.body && deviceId) {
            const db = adminApp.firestore();
            const inboundRef = db.collection("message_logs").doc(String(data.eventId));
            const existingDoc = await inboundRef.get();
            if (!existingDoc.exists) {
              const inboundLog: any = {
                recipient: String(data.sender),
                sender: String(data.sender),
                body: String(data.body),
                status: "received",
                channel: "sms",
                direction: "inbound",
                messageType: "inbound",
                source: "device_inbox",
                deviceId,
                createdAt: admin.firestore.FieldValue.serverTimestamp(),
                timestamp: admin.firestore.FieldValue.serverTimestamp(),
                updatedAt: admin.firestore.FieldValue.serverTimestamp(),
              };
              if (ownerUid) {
                inboundLog.createdBy = ownerUid;
                inboundLog.ownerUid = ownerUid;
              }
              const incomingTs = Number(data.timestampMs);
              if (Number.isFinite(incomingTs) && incomingTs > 0) {
                inboundLog.deviceTimestamp = admin.firestore.Timestamp.fromMillis(incomingTs);
              }
              await inboundRef.set(inboundLog);
              await logActivity(
                "incoming_sms",
                `Inbound SMS received from ${data.sender} on device ${deviceId}`,
                deviceId,
                ownerUid,
              );
            }
          }
        }
      } catch (error) {
        logErrorTrace("ws.message.failed", error, {
          deviceId,
          ownerUid,
        });
      }
    });

    ws.on("close", () => {
      console.log(`[WebSocket] Device ${deviceId} closed connection`);
      logTrace("ws.connection.close", {
        deviceId,
        ownerUid,
      });
      
      // Don't immediately mark offline - let the heartbeat timeout mechanism handle it
      // Only clean up the lastHeartbeat entry after a delay
      setTimeout(() => {
        const lastBeat = lastHeartbeat.get(deviceId);
        if (lastBeat && Date.now() - lastBeat > 60000) { // If no heartbeat for 1+ minute
          lastHeartbeat.delete(deviceId);
        }
      }, 60000); // Clean up after 1 minute

      connectedDevices.delete(deviceId);
    });
  });

  // Every 60 seconds, check for stale devices and mark them offline
  setInterval(async () => {
    const now = Date.now();
    const staleDevices = [];
    
    for (const [deviceId, lastTime] of lastHeartbeat.entries()) {
      const timeSinceLastHeartbeat = now - lastTime;
      
      if (timeSinceLastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
        console.log(`[Stale Device Check] Device ${deviceId} timed out after ${Math.round(timeSinceLastHeartbeat / 1000)} seconds`);
        staleDevices.push(deviceId);
        
        // Remove from active tracking
        lastHeartbeat.delete(deviceId);
        
        // Remove from actively connected devices
        if (connectedDevices.has(deviceId)) {
          connectedDevices.delete(deviceId);
        }
        
        // Mark as offline in Firestore
        const adminApp = getFirebaseAdmin();
        if (adminApp) {
          try {
            await persistDevicePresence(
              adminApp.firestore(),
              deviceId,
              {
                status: "offline",
              },
              { force: true },
            )
              .catch(err => {
                console.error(`[Stale Device Check] Error updating device ${deviceId}:`, err);
              });
          } catch (err) {
            console.error(`[Stale Device Check] Failed to update device ${deviceId}:`, err);
          }
        }
      }
    }
    
    if (staleDevices.length > 0) {
      console.log(`[Stale Device Check] Marked ${staleDevices.length} devices as offline: ${staleDevices.join(', ')}`);
    }
  }, STALE_CHECK_INTERVAL_MS);

  let staleRecoveryInFlight = false;
  const recoverStaleProcessing = async () => {
    if (
      !STALE_RECOVERY_ENABLED
      || staleRecoveryInFlight
      || Date.now() < firestoreQuotaBackoffUntil
    ) {
      return;
    }
    staleRecoveryInFlight = true;
    const adminApp = getFirebaseAdmin();
    if (!adminApp) {
      staleRecoveryInFlight = false;
      return;
    }

    try {
      const db = adminApp.firestore();
      const cutoff = admin.firestore.Timestamp.fromMillis(
        Date.now() - STALE_PROCESSING_TIMEOUT_MS,
      );
      const staleSnapshot = await db.collection("message_logs")
        .where("status", "==", "processing")
        .where("updatedAt", "<=", cutoff)
        .limit(STALE_PROCESSING_BATCH_SIZE)
        .get();
      clearFirestoreQuotaBackoff();

      if (staleSnapshot.empty) {
        return;
      }

      let recoveredCount = 0;
      for (const doc of staleSnapshot.docs) {
        const data = doc.data();
        const assignedDeviceId = typeof data.deviceId === "string" ? data.deviceId.trim() : "";
        const isAssignedDeviceOnline =
          assignedDeviceId.length > 0 &&
          connectedDevices.get(assignedDeviceId)?.readyState === WebSocket.OPEN;

        const updateData: Record<string, any> = {
          status: isAssignedDeviceOnline ? "queued" : "pending",
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          recoveredAt: admin.firestore.FieldValue.serverTimestamp(),
          recoveryReason: "stale_processing_timeout",
          recoveryAttempts: admin.firestore.FieldValue.increment(1),
        };

        if (!isAssignedDeviceOnline) {
          updateData.deviceId = admin.firestore.FieldValue.delete();
        }

        await doc.ref.update(updateData);
        recoveredCount += 1;
      }

      if (recoveredCount > 0) {
        console.log(
          `[Queue Recovery] Recovered ${recoveredCount} stale processing messages`,
        );
      }
    } catch (error) {
      if (!registerFirestoreQuotaBackoff("Queue Recovery", error)) {
        console.error("[Queue Recovery] Failed to recover stale processing messages:", error);
      }
    } finally {
      staleRecoveryInFlight = false;
    }
  };
  if (STALE_RECOVERY_ENABLED) {
    setInterval(recoverStaleProcessing, STALE_PROCESSING_CHECK_INTERVAL_MS);
  }

  // Function to notify devices of new messages
  // This could be called from the /api/send-template endpoint
  // or via a Firestore trigger in a real production environment
  type DeviceDispatchResult = {
    pushed: boolean;
    reason: string | null;
  };

  const notifyDevice = (deviceId: string, messageData: any) => {
    const ws = connectedDevices.get(deviceId);
    logTrace("dispatch.notify.attempt", {
      deviceId,
      messageId: messageData.id || null,
      recipient: messageData.recipient || null,
      channel: messageData.channel || "sms",
    });
    if (!ws) {
      const reason = `No active websocket registered for device ${deviceId}`;
      console.warn(`[Dispatch] ${reason}`);
      logTrace("dispatch.notify.miss", {
        deviceId,
        messageId: messageData.id || null,
        reason,
      });
      return { pushed: false, reason };
    }

    if (ws.readyState !== WebSocket.OPEN) {
      const reason = `Websocket for device ${deviceId} is not open (state ${ws.readyState})`;
      console.warn(`[Dispatch] ${reason}`);
      logTrace("dispatch.notify.not_open", {
        deviceId,
        messageId: messageData.id || null,
        readyState: ws.readyState,
        reason,
      });
      return { pushed: false, reason };
    }

    try {
      ws.send(JSON.stringify({
        type: "new_message",
        message: {
          id: messageData.id,
          messageId: messageData.id,
          task: "SEND_SMS",
          phone: messageData.recipient,
          message: messageData.body,
          recipient: messageData.recipient,
          body: messageData.body,
          channel: messageData.channel || "sms",
          ...(messageData.simSlot !== undefined ? { simSlot: messageData.simSlot } : {}),
          ...(messageData.templateName ? { templateName: messageData.templateName } : {}),
        }
      }));
      console.log(`[Dispatch] Message ${messageData.id} pushed to device ${deviceId}`);
      logTrace("dispatch.notify.pushed", {
        deviceId,
        messageId: messageData.id || null,
        recipient: messageData.recipient || null,
      });
      return { pushed: true, reason: null };
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      console.error(`[Dispatch] Failed to push message ${messageData.id} to device ${deviceId}:`, error);
      logErrorTrace("dispatch.notify.failed", error, {
        deviceId,
        messageId: messageData.id || null,
        recipient: messageData.recipient || null,
      });
      return { pushed: false, reason };
    }
  };

  async function dispatchUnsentQueue(options: {
    reason: string;
    ownerUid?: string;
    deviceId?: string;
    limit?: number;
    cooldownMs?: number;
    includeFailed?: boolean;
    trustedSystem?: boolean;
  }) {
    const adminApp = getFirebaseAdmin();
    if (!adminApp) {
      return {
        scanned: 0,
        pushed: 0,
        pending: 0,
        skipped: 0,
        skippedRecent: 0,
        results: [],
      };
    }

    const db = adminApp.firestore();
    const limit = Math.min(Math.max(options.limit || AUTO_DISPATCH_BATCH_SIZE, 1), FORCE_RESEND_BATCH_SIZE);
    const statuses = options.includeFailed === false ? ["pending", "queued"] : ["pending", "queued", "failed"];

    if (options.reason === "interval_sweep") {
      const hasLiveRelay = Array.from(connectedDevices.values())
        .some((socket) => socket.readyState === WebSocket.OPEN);
      if (!hasLiveRelay || Date.now() < firestoreQuotaBackoffUntil) {
        return {
          scanned: 0,
          pushed: 0,
          pending: 0,
          skipped: 0,
          skippedRecent: 0,
          results: [],
        };
      }
    }

    let queue: any = db.collection("message_logs")
      .where("status", "in", statuses)
      .limit(limit);

    if (options.ownerUid) {
      queue = queue.where("createdBy", "==", options.ownerUid);
    }

    const snapshot = await queue.get();
    const docs: any[] = snapshot.docs;
    clearFirestoreQuotaBackoff();

    let pushed = 0;
    let pending = 0;
    let skipped = 0;
    let skippedRecent = 0;
    const results: any[] = [];
    const now = Date.now();
    const cooldownMs = Math.max(options.cooldownMs ?? AUTO_DISPATCH_RETRY_COOLDOWN_MS, 0);
    const forcedDeviceId = options.deviceId?.trim() || "";

    for (const doc of docs.slice(0, limit)) {
      const data = doc.data();
      const status = String(data.status || "");
      const channel = typeof data.channel === "string" ? data.channel.trim().toLowerCase() : "";
      const createdBy = typeof data.createdBy === "string" ? data.createdBy : "";
      const direction = typeof data.direction === "string" ? data.direction.trim().toLowerCase() : "";

      if (!statuses.includes(status)) {
        skipped += 1;
        continue;
      }

      if (direction === "inbound" || status === "received") {
        skipped += 1;
        continue;
      }

      // Legacy bulk records may not have channel populated. Missing channel is treated as SMS
      // only when the record has SMS job fields.
      if (channel && channel !== "sms") {
        skipped += 1;
        continue;
      }

      if (!data.recipient || !data.body) {
        skipped += 1;
        continue;
      }

      if (options.ownerUid && createdBy && createdBy !== options.ownerUid && !options.trustedSystem) {
        skipped += 1;
        continue;
      }

      const lastAttemptMs = Math.max(
        firestoreTimestampToMillis(data.lastDispatchAttemptAt),
        firestoreTimestampToMillis(data.pushedAt),
      );

      if (cooldownMs > 0 && lastAttemptMs > 0 && now - lastAttemptMs < cooldownMs) {
        skippedRecent += 1;
        continue;
      }

      const previousDeviceId = typeof data.deviceId === "string" ? data.deviceId.trim() : "";
      let targetDeviceId = forcedDeviceId;

      if (!targetDeviceId && previousDeviceId && connectedDevices.get(previousDeviceId)?.readyState === WebSocket.OPEN) {
        targetDeviceId = previousDeviceId;
      }

      if (!targetDeviceId && createdBy) {
        targetDeviceId = await findBestDeviceForOwner(db, createdBy);
      }

      if (!targetDeviceId && previousDeviceId) {
        targetDeviceId = previousDeviceId;
      }

      if (!targetDeviceId) {
        pending += 1;
        continue;
      }

      const dispatchResult = notifyDevice(targetDeviceId, {
        id: doc.id,
        recipient: data.recipient,
        body: data.body,
        channel: data.channel || "sms",
        simSlot: data.simSlot,
        templateName: data.templateName,
      });

      const updateData: Record<string, any> = {
        status: dispatchResult.pushed ? "queued" : "pending",
        deviceId: targetDeviceId,
        lastDispatchAttemptAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      if (options.reason === "manual_force_resend") {
        updateData.retryCount = admin.firestore.FieldValue.increment(1);
        updateData.forceResendCount = admin.firestore.FieldValue.increment(1);
        updateData.forceResentAt = admin.firestore.FieldValue.serverTimestamp();
      } else {
        updateData.autoDispatchAttempts = admin.firestore.FieldValue.increment(1);
        updateData.autoDispatchReason = options.reason;
      }

      if (dispatchResult.pushed) {
        updateData.pushedAt = admin.firestore.FieldValue.serverTimestamp();
        updateData.error = admin.firestore.FieldValue.delete();
        updateData.dispatchError = admin.firestore.FieldValue.delete();
        pushed += 1;
      } else {
        updateData.error = dispatchResult.reason || "Queued for retry when a relay device is available";
        pending += 1;
      }

      await doc.ref.update(updateData);
      results.push({
        messageId: doc.id,
        status: updateData.status,
        pushed: dispatchResult.pushed,
        deviceId: targetDeviceId || null,
        reason: dispatchResult.reason,
      });
    }

    return {
      scanned: docs.length,
      pushed,
      pending,
      skipped,
      skippedRecent,
      results,
    };
  }

  let queueDispatchInFlight = false;
  const runAutomaticQueueDispatch = async () => {
    if (!AUTO_DISPATCH_ENABLED || Date.now() < firestoreQuotaBackoffUntil) {
      return;
    }
    if (queueDispatchInFlight) {
      return;
    }

    queueDispatchInFlight = true;
    try {
      const result = await dispatchUnsentQueue({
        reason: "interval_sweep",
        limit: AUTO_DISPATCH_BATCH_SIZE,
        cooldownMs: AUTO_DISPATCH_RETRY_COOLDOWN_MS,
        includeFailed: false,
      });

      if (result.pushed > 0 || result.pending > 0) {
        console.log(
          `[Queue Auto Dispatch] scanned=${result.scanned} pushed=${result.pushed} waiting=${result.pending} skipped=${result.skipped} skipped_recent=${result.skippedRecent}`,
        );
      }
    } catch (error) {
      if (!registerFirestoreQuotaBackoff("Queue Auto Dispatch", error)) {
        console.error("[Queue Auto Dispatch] Failed to dispatch unsent messages:", error);
      }
    } finally {
      queueDispatchInFlight = false;
    }
  };
  if (AUTO_DISPATCH_ENABLED) {
    setInterval(runAutomaticQueueDispatch, AUTO_DISPATCH_INTERVAL_MS);
  }

  if (PAY_GATEWAY_DELIVERY_CALLBACK_URL && PAY_GATEWAY_DELIVERY_CALLBACK_SECRET.length >= 32) {
    setInterval(() => void flushPayGatewayDeliveryCallbacks().catch((error) => console.error("[Pay Gateway Callback] flush failed:", error)), DELIVERY_CALLBACK_INTERVAL_MS);
    void flushPayGatewayDeliveryCallbacks().catch((error) => console.error("[Pay Gateway Callback] startup flush failed:", error));
  }

  const keepAliveBaseUrl = resolveTalkGatewayBaseUrl();
  const keepAliveApiKey =
    process.env.ORBI_TALK_GATEWAY_API_KEY?.trim()
    || process.env.ORBI_GATEWAY_API_KEY?.trim()
    || "";
  const requestedKeepAliveInterval = Number.parseInt(
    String(process.env.SELF_KEEPALIVE_INTERVAL_MS || ""),
    10,
  );
  const keepAliveIntervalMs = Number.isFinite(requestedKeepAliveInterval)
    ? Math.max(requestedKeepAliveInterval, 30_000)
    : 30_000;

  if (keepAliveBaseUrl) {
    setInterval(async () => {
      const normalizedBaseUrl = keepAliveBaseUrl.replace(/\/+$/, "");
      try {
        const headers: Record<string, string> = {};
        if (keepAliveApiKey) {
          headers["x-api-key"] = keepAliveApiKey;
        }
        const response = await fetch(`${normalizedBaseUrl}/health`, {
          headers,
        });
        if (!response.ok) {
          console.warn(`[Self Keepalive] health returned HTTP ${response.status}`);
        }
      } catch (error) {
        console.error("[Self Keepalive] Failed to call health:", error);
      }
    }, keepAliveIntervalMs);
  } else {
    console.warn(
      "[Self Keepalive] Disabled because APP_URL/VITE_APP_URL/PUBLIC_GATEWAY_BASE_URL is missing.",
    );
  }

  server.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://0.0.0.0:${PORT}`);
    console.log(`WebSocket server running on ws://0.0.0.0:${PORT}`);
    logActivity("server_start", "ORBI Talk Gateway server started successfully");
  });
}

startServer().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
