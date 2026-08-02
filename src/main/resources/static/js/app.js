const API = "/api/gsm";
const TOOL = "/api/tool";
const state = { sims: [], smsTab: "INBOX", page: "overview" };
const titles = {
  overview: ["CONTROL CENTER", "Tổng quan vận hành"],
  devices: ["SIM INVENTORY", "Quản lý thiết bị"],
  import: ["SIM DATABASE", "Import danh sách SIM"],
  messages: ["MESSAGE CENTER", "Tin nhắn SMS"],
  calls: ["VOICE OPERATIONS", "Cuộc gọi"],
  settings: ["DELIVERY CONTROL", "Settings"]
};
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

document.addEventListener("DOMContentLoaded", () => {
  bindNavigation();
  bindActions();
  updateClock();
  setInterval(updateClock, 1000);
  connectWebSocket();
  refreshAll();
});

function bindNavigation() {
  $$(".nav-link").forEach(button => button.addEventListener("click", () => showPage(button.dataset.page)));
  $$("[data-go]").forEach(button => button.addEventListener("click", () => showPage(button.dataset.go)));
  $("#mobile-menu").addEventListener("click", () => $(".rail").classList.toggle("open"));
}

function showPage(page) {
  state.page = page;
  $$(".nav-link").forEach(item => item.classList.toggle("active", item.dataset.page === page));
  $$(".page").forEach(item => item.classList.toggle("active", item.id === "page-" + page));
  $("#page-eyebrow").textContent = titles[page][0];
  $("#page-title").textContent = titles[page][1];
  $(".rail").classList.remove("open");
  if (page === "messages") loadMessages();
  if (page === "calls") loadCalls();
  if (page === "settings") loadDeliverySettings();
}

function bindActions() {
  $("#global-refresh").addEventListener("click", refreshAll);
  $("#hero-scan").addEventListener("click", scanSims);
  $("#scan-sims").addEventListener("click", scanSims);
  $("#sim-search").addEventListener("input", renderDevices);
  $("#sim-status-filter").addEventListener("change", renderDevices);
  $("#sms-content").addEventListener("input", e => $("#sms-length").textContent = e.target.value.length);
  $("#sms-form").addEventListener("submit", sendSms);
  $("#call-form").addEventListener("submit", makeCall);
  $("#refresh-calls").addEventListener("click", loadCalls);
  $$("[data-sms-tab]").forEach(button => button.addEventListener("click", () => {
    state.smsTab = button.dataset.smsTab;
    $$("[data-sms-tab]").forEach(item => item.classList.toggle("active", item === button));
    loadMessages();
  }));
  $("#webhook-form").addEventListener("submit", saveWebhook);
  $("#test-webhook").addEventListener("click", testWebhook);
  $("#test-telegram").addEventListener("click", testTelegram);
  $("#excel-import-form").addEventListener("submit", importExcel);
  $("#text-import-form").addEventListener("submit", importText);
  $("#sim-excel-file").addEventListener("change", event => {
    $("#excel-file-label").textContent = event.target.files[0]?.name || "Chọn hoặc kéo file Excel vào đây";
  });
  const dropZone = $("#excel-drop-zone");
  ["dragenter", "dragover"].forEach(name => dropZone.addEventListener(name, event => {
    event.preventDefault();
    dropZone.classList.add("dragging");
  }));
  ["dragleave", "drop"].forEach(name => dropZone.addEventListener(name, event => {
    event.preventDefault();
    dropZone.classList.remove("dragging");
  }));
  dropZone.addEventListener("drop", event => {
    const file = event.dataTransfer.files[0];
    if (!file) return;
    const transfer = new DataTransfer();
    transfer.items.add(file);
    $("#sim-excel-file").files = transfer.files;
    $("#excel-file-label").textContent = file.name;
  });
}

async function refreshAll() {
  await Promise.allSettled([loadSims(), loadStats(), loadMessages(), loadDeliverySettings()]);
  if (state.page === "calls") loadCalls();
}

async function api(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options
  });
  let body = {};
  try { body = await response.json(); } catch (_) {}
  if (!response.ok || body.success === false) {
    throw new Error(body.error || body.message || "Yêu cầu không thành công");
  }
  return Object.prototype.hasOwnProperty.call(body, "data") ? body.data : body;
}

async function loadSims() {
  try {
    const records = await api(TOOL + "/sims") || [];
    state.sims = records.map(normalizeSim);
    renderDevices();
    renderOverviewSims();
    fillPortSelects();
    const online = state.sims.filter(sim => sim.status === "ONLINE").length;
    $("#metric-online").textContent = online;
    $("#metric-total").textContent = state.sims.length + " thiết bị";
    $("#sim-count").textContent = state.sims.length + " SIM";
  } catch (error) {
    toast(error.message, "error");
  }
}

function renderOverviewSims() {
  const rows = state.sims.slice(0, 5);
  $("#overview-sims").innerHTML = rows.length ? rows.map(sim => `
    <div class="sim-row">
      <div class="identity"><span class="sim-avatar">${escapeHtml(shortPort(sim.comPort))}</span><div><b>${escapeHtml(sim.phoneNumber || "Chưa có số")}</b><small>${escapeHtml(sim.comPort || "—")}</small></div></div>
      <span>${escapeHtml(sim.carrier || "Chưa xác định")}</span>
      <span>${Number(sim.todaySms || 0)} SMS hôm nay</span>
      ${statusChip(sim.status)}
    </div>`).join("") : '<div class="empty">Chưa phát hiện SIM nào</div>';
}

function renderDevices() {
  const keyword = ($("#sim-search")?.value || "").toLowerCase();
  const status = $("#sim-status-filter")?.value || "";
  const sims = state.sims.filter(sim => {
    const haystack = [sim.phoneNumber, sim.comPort, sim.iccid, sim.carrier].join(" ").toLowerCase();
    return (!keyword || haystack.includes(keyword)) && (!status || sim.status === status);
  });
  $("#device-grid").innerHTML = sims.length ? sims.map(sim => `
    <article class="device-card">
      <div class="device-top"><span class="device-port">${escapeHtml(shortPort(sim.comPort))}</span>${statusChip(sim.status)}</div>
      <h4>${escapeHtml(sim.phoneNumber || "Đang phát hiện…")}</h4>
      <p>${escapeHtml(sim.carrier || "Nhà mạng chưa xác định")}</p>
      <div class="device-meta"><div><small>CỔNG KẾT NỐI</small><b>${escapeHtml(sim.comPort || "—")}</b></div><div><small>ICCID</small><b title="${escapeHtml(sim.iccid || "")}">${escapeHtml(sim.iccid || "—")}</b></div><div><small>SMS HÔM NAY</small><b>${Number(sim.todaySms || 0)}</b></div><div><small>CALL HÔM NAY</small><b>${Number(sim.todayCall || 0)}</b></div></div>
    </article>`).join("") : '<div class="empty">Không có SIM phù hợp</div>';
}

function fillPortSelects() {
  ["#sms-port", "#call-port"].forEach(selector => {
    const select = $(selector);
    const selected = select.value;
    select.innerHTML = '<option value="">Chọn cổng COM</option>' + state.sims
      .filter(sim => sim.comPort)
      .map(sim =>
      `<option value="${escapeHtml(sim.comPort)}">${escapeHtml(sim.comPort)} — ${escapeHtml(sim.phoneNumber || "Chưa có số")}</option>`
    ).join("");
    select.value = selected;
  });
}

async function scanSims() {
  const buttons = [$("#scan-sims"), $("#hero-scan")].filter(Boolean);
  buttons.forEach(button => { button.disabled = true; button.textContent = "Đang quét…"; });
  try {
    await api(API + "/sim/scan");
    await loadSims();
    toast("Quét SIM hoàn tất", "success");
  } catch (error) { toast(error.message, "error"); }
  finally {
    if ($("#scan-sims")) { $("#scan-sims").disabled = false; $("#scan-sims").textContent = "↻ Quét SIM"; }
    if ($("#hero-scan")) { $("#hero-scan").disabled = false; $("#hero-scan").textContent = "Quét toàn bộ cổng →"; }
  }
}

async function loadStats() {
  try {
    const stats = await api(API + "/stats") || {};
    $("#metric-sms").textContent = stats.todaySms ?? stats.smsSentToday ?? 0;
    $("#metric-calls").textContent = stats.todayCalls ?? stats.callsToday ?? 0;
    const rate = stats.successRate;
    $("#metric-rate").textContent = rate == null ? "—" : Math.round(Number(rate)) + "%";
  } catch (_) {}
  try {
    const unread = await api(API + "/sms/unread-count");
    const count = typeof unread === "number" ? unread : (unread?.count || 0);
    $("#nav-unread").textContent = count;
  } catch (_) {}
}

async function loadMessages() {
  try {
    const direction = state.smsTab === "INBOX" ? "INBOUND" : "OUTBOUND";
    const page = await api(`${TOOL}/sms?direction=${direction}&page=0&size=200`) || {};
    let messages = page.content || [];
    if (state.smsTab === "SENT") {
      messages = messages.filter(item => ["SENT", "SUCCESS", "PENDING"]
        .includes(String(item.status).toUpperCase()));
    }
    if (state.smsTab === "OUTBOX") {
      messages = messages.filter(item => ["FAILED", "ERROR"]
        .includes(String(item.status).toUpperCase()));
    }
    $("#message-list").innerHTML = messages.length ? messages.map(messageCard).join("") : '<div class="empty">Chưa có tin nhắn trong mục này</div>';
    if (state.smsTab === "INBOX") renderRecentSms(messages.slice(0, 8));
  } catch (error) {
    $("#message-list").innerHTML = '<div class="empty">Không thể tải tin nhắn</div>';
  }
}

function messageCard(message) {
  const inbound = (message.direction || "").toUpperCase() === "INBOUND"
    || (message.type || "").toUpperCase() === "INBOX";
  return `<article class="message-item">
    <span class="message-direction ${inbound ? "" : "out"}">${inbound ? "↓" : "↑"}</span>
    <div class="message-copy"><header><h4>${escapeHtml(message.phoneNumber || "Không rõ")}</h4><time>${formatDate(message.createdAt)}</time></header><p>${escapeHtml(message.content || "")}</p><small>${escapeHtml(message.simPhone || "")} · ${escapeHtml(message.comPort || "—")}</small></div>
    ${statusChip(message.status)}
  </article>`;
}

function renderRecentSms(messages) {
  $("#overview-sms").innerHTML = messages.length ? messages.map(message => `<tr>
    <td>${formatDate(message.createdAt)}</td><td>${escapeHtml(message.simPhone || "—")}<br><small>${escapeHtml(message.comPort || "")}</small></td>
    <td>${escapeHtml(message.phoneNumber || "Không rõ")}</td><td class="message-content">${escapeHtml(message.content || "")}</td><td>${statusChip(message.status)}</td>
  </tr>`).join("") : '<tr><td colspan="5" class="empty-cell">Chưa có tin nhắn</td></tr>';
}

async function sendSms(event) {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true; button.textContent = "Đang gửi…";
  try {
    await api(API + "/sms/send", { method: "POST", body: JSON.stringify({
      comPort: $("#sms-port").value, phoneNumber: $("#sms-phone").value.trim(), content: $("#sms-content").value
    })});
    toast("Tin nhắn đã được đưa vào hàng gửi", "success");
    $("#sms-phone").value = ""; $("#sms-content").value = ""; $("#sms-length").textContent = "0";
    setTimeout(loadMessages, 900);
  } catch (error) { toast(error.message, "error"); }
  finally { button.disabled = false; button.textContent = "Gửi qua SIM đã chọn"; }
}

async function importExcel(event) {
  event.preventDefault();
  const file = $("#sim-excel-file").files[0];
  if (!file) return toast("Hãy chọn file Excel", "error");
  const button = event.submitter;
  button.disabled = true;
  button.textContent = "Đang import…";
  try {
    const form = new FormData();
    form.append("file", file);
    const response = await fetch(TOOL + "/sims/import/excel", { method: "POST", body: form });
    const result = await response.json();
    if (!response.ok) throw new Error(result.message || result.error || "Không import được Excel");
    renderImportResult(result);
    toast("Import Excel hoàn tất", "success");
    await loadSims();
  } catch (error) {
    toast(error.message, "error");
  } finally {
    button.disabled = false;
    button.textContent = "Import từ Excel";
  }
}

async function importText(event) {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true;
  button.textContent = "Đang import…";
  try {
    const result = await api(TOOL + "/sims/import/text", {
      method: "POST",
      body: JSON.stringify({ text: $("#sim-import-text").value })
    });
    renderImportResult(result);
    toast("Import nội dung hoàn tất", "success");
    await loadSims();
  } catch (error) {
    toast(error.message, "error");
  } finally {
    button.disabled = false;
    button.textContent = "Import nội dung";
  }
}

function renderImportResult(result) {
  const warnings = result.warnings || [];
  $("#import-result").className = "";
  $("#import-result").innerHTML = `<div class="result-metrics">
    <div><strong>${Number(result.totalRows || 0)}</strong><small>Tổng dòng</small></div>
    <div><strong>${Number(result.created || 0)}</strong><small>Tạo mới</small></div>
    <div><strong>${Number(result.updated || 0)}</strong><small>Cập nhật</small></div>
    <div><strong>${Number(result.fuzzyMatched || 0)}</strong><small>CCID gần đúng</small></div>
    <div><strong>${Number(result.skipped || 0)}</strong><small>Bỏ qua</small></div>
  </div>${warnings.length ? `<ul class="import-warnings">${warnings.map(item => `<li>${escapeHtml(item)}</li>`).join("")}</ul>` : ""}`;
}

async function makeCall(event) {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true; button.textContent = "Đang kết nối…";
  try {
    await api(API + "/call/make", { method: "POST", body: JSON.stringify({
      comPort: $("#call-port").value,
      targetPhone: $("#call-phone").value.trim(),
      callDuration: Number($("#call-duration").value),
      record: $("#call-record").checked
    })});
    toast("Cuộc gọi đã bắt đầu", "success");
    setTimeout(loadCalls, 1000);
  } catch (error) { toast(error.message, "error"); }
  finally { button.disabled = false; button.textContent = "☎ Bắt đầu gọi"; }
}

async function loadCalls() {
  try {
    const page = await api(API + "/call/history?page=0&size=60") || {};
    const calls = page.content || [];
    $("#call-history").innerHTML = calls.length ? calls.map(call => `<tr>
      <td>${formatDate(call.createdAt || call.callStartTime)}</td><td>${escapeHtml(call.comPort || "—")}</td>
      <td>${escapeHtml(call.targetPhone || call.phoneNumber || "—")}</td><td>${Number(call.duration || call.callDuration || 0)}s</td>
      <td>${statusChip(call.status)}</td><td><button class="text-button" onclick="hangup('${escapeJs(call.comPort || "")}')">Kết thúc</button></td>
    </tr>`).join("") : '<tr><td colspan="6" class="empty-cell">Chưa có cuộc gọi</td></tr>';
  } catch (_) {}
}

async function hangup(comPort) {
  if (!comPort) return;
  try { await api(API + "/call/hangup/" + encodeURIComponent(comPort), { method: "POST" }); toast("Đã kết thúc cuộc gọi", "success"); }
  catch (error) { toast(error.message, "error"); }
}

async function loadDeliverySettings() {
  try {
    const [telegram, webhook] = await Promise.all([api(TOOL + "/telegram/status"), api(TOOL + "/settings/webhook")]);
    const telegramOn = !!telegram.configured;
    $("#telegram-chip").textContent = telegramOn ? "Sẵn sàng" : "Chưa cấu hình";
    $("#telegram-chip").className = "status-chip " + (telegramOn ? "configured" : "");
    $("#overview-telegram").textContent = telegramOn ? "Đang hoạt động" : "Chưa cấu hình";
    $("#webhook-enabled").checked = !!webhook.enabled;
    $("#webhook-url").value = webhook.url || "";
    $("#token-state").textContent = webhook.bearerTokenConfigured ? "Đã lưu token • nhập giá trị mới để thay đổi" : "Chưa có token";
    $("#secret-state").textContent = webhook.signingSecretConfigured ? "Đã bật chữ ký HMAC • nhập giá trị mới để thay đổi" : "Chưa có signing secret";
    $("#overview-webhook").textContent = webhook.enabled ? "Đang hoạt động" : "Đang tắt";
  } catch (_) {
    $("#telegram-chip").textContent = "Không kết nối";
  }
}

async function saveWebhook(event) {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true; button.textContent = "Đang lưu…";
  try {
    await api(TOOL + "/settings/webhook", { method: "PUT", body: JSON.stringify({
      enabled: $("#webhook-enabled").checked,
      url: $("#webhook-url").value.trim(),
      bearerToken: $("#webhook-token").value,
      signingSecret: $("#webhook-secret").value,
      clearBearerToken: $("#clear-webhook-token").checked,
      clearSigningSecret: $("#clear-webhook-secret").checked
    })});
    $("#webhook-token").value = ""; $("#webhook-secret").value = "";
    $("#clear-webhook-token").checked = false; $("#clear-webhook-secret").checked = false;
    toast("Đã lưu cấu hình webhook", "success");
    loadDeliverySettings();
  } catch (error) { toast(error.message, "error"); }
  finally { button.disabled = false; button.textContent = "Lưu cấu hình"; }
}

async function testWebhook() {
  try { const result = await api(TOOL + "/settings/webhook/test", { method: "POST" }); toast(result.message || "Webhook phản hồi thành công", "success"); }
  catch (error) { toast(error.message, "error"); }
}
async function testTelegram() {
  try { const result = await api(TOOL + "/telegram/test", { method: "POST" }); toast(result.message || "Đã gửi Telegram", "success"); }
  catch (error) { toast(error.message, "error"); }
}

function connectWebSocket() {
  if (!window.SockJS || !window.Stomp) { setConnection(false); return; }
  try {
    const client = Stomp.over(new SockJS("/ws")); client.debug = null;
    client.connect({}, () => {
      setConnection(true);
      client.subscribe("/topic/sims", message => { try { state.sims = JSON.parse(message.body).map(normalizeSim); renderDevices(); renderOverviewSims(); fillPortSelects(); } catch (_) {} });
      client.subscribe("/topic/sms/new", () => { loadMessages(); loadStats(); toast("Có SMS mới — đã chuyển tiếp tới các kênh", "success"); });
      client.subscribe("/topic/sms/status", () => loadMessages());
      client.subscribe("/topic/call/status", () => loadCalls());
    }, () => { setConnection(false); setTimeout(connectWebSocket, 5000); });
  } catch (_) { setConnection(false); }
}
function setConnection(online) {
  $("#connection-dot").classList.toggle("online", online);
  $("#connection-label").textContent = online ? "Đã kết nối" : "Mất kết nối";
}
function normalizeSim(sim) {
  return {
    ...sim,
    comPort: sim.comPort || sim.comName || "",
    carrier: sim.carrier || sim.simProvider || "",
    iccid: sim.iccid || sim.ccid || "",
    status: sim.status === "ACTIVE" ? "ONLINE" : sim.status
  };
}
function updateClock() {
  $("#live-clock").textContent = new Intl.DateTimeFormat("vi-VN", { weekday: "short", hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date());
}
function statusChip(status) {
  const value = String(status || "UNKNOWN").toUpperCase();
  const css = ["ONLINE","SENT","RECEIVED","SUCCESS","ACTIVE","IN_CALL"].includes(value) ? "online" : ["FAILED","OFFLINE","ERROR","NO_ANSWER"].includes(value) ? "failed" : "";
  return `<span class="status-chip ${css}">${escapeHtml(value.replace("_", " "))}</span>`;
}
function shortPort(port) { return String(port || "SIM").replace(/[^0-9]/g, "").slice(-2) || "SIM"; }
function formatDate(value) {
  if (!value) return "—";
  const date = new Date(value); return Number.isNaN(date.getTime()) ? escapeHtml(String(value)) : new Intl.DateTimeFormat("vi-VN", { day:"2-digit", month:"2-digit", hour:"2-digit", minute:"2-digit" }).format(date);
}
function toast(message, type = "") {
  const element = document.createElement("div"); element.className = "toast " + type; element.textContent = message;
  $("#toast-stack").appendChild(element); setTimeout(() => element.remove(), 4200);
}
function escapeHtml(value) { const div = document.createElement("div"); div.textContent = value == null ? "" : String(value); return div.innerHTML; }
function escapeJs(value) { return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'"); }
