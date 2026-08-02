/**
 * 🌐 Proxy Management Module
 * Quản lý Mobile Proxy - mỗi SIM 1 proxy riêng
 */

const PROXY_API = "/api/proxy";

// Proxy state
let proxyList = [];
let proxyRefreshInterval = null;

// APN presets for Japanese carriers
const APN_PRESETS = {
  "Rakuten": "rakuten.jp",
  "Docomo": "spmode.ne.jp",
  "Softbank": "plus.4g",
  "AU/KDDI": "uno.au-net.ne.jp",
  "Y!mobile": "plus.acs.jp",
  "IIJmio": "iijmio.jp",
  "Default": "internet"
};

// ===== Initialize =====
function initProxy() {
  // Event listeners
  (function(el){ if(el) el.addEventListener })(document.getElementById("proxy-start-all-btn"))("click", proxyStartAll);
  (function(el){ if(el) el.addEventListener })(document.getElementById("proxy-stop-all-btn"))("click", proxyStopAll);
  (function(el){ if(el) el.addEventListener })(document.getElementById("proxy-refresh-btn"))("click", loadProxyList);
  (function(el){ if(el) el.addEventListener })(document.getElementById("proxy-export-btn"))("click", proxyExport);

  // Subscribe WebSocket for live updates
  if (typeof stompClient !== "undefined" && (stompClient ? stompClient.connected : false)) {
    subscribeProxyTopic();
  }

  // Auto-detect APN from carrier
  autoDetectApn();

  console.log("✅ Proxy module initialized");
}

function subscribeProxyTopic() {
  try {
    stompClient.subscribe("/topic/proxy", (message) => {
      const data = JSON.parse(message.body);
      if (Array.isArray(data)) {
        proxyList = data;
        renderProxyGrid(data);
        updateProxyOverview(data);
      }
    });
    console.log("✅ Subscribed to /topic/proxy");
  } catch (e) {
    console.warn("⚠️ Could not subscribe proxy topic:", e);
  }
}

// ===== Auto-detect APN from carrier =====
function autoDetectApn() {
  // Try to detect from simList (global from app.js)
  if (typeof simList !== "undefined" && simList.length > 0) {
    for (const sim of simList) {
      const carrier = (sim.carrier || "").toLowerCase();
      if (carrier.includes("rakuten")) {
        document.getElementById("proxy-apn").value = APN_PRESETS["Rakuten"];
        return;
      }
      if (carrier.includes("docomo")) {
        document.getElementById("proxy-apn").value = APN_PRESETS["Docomo"];
        return;
      }
      if (carrier.includes("softbank")) {
        document.getElementById("proxy-apn").value = APN_PRESETS["Softbank"];
        return;
      }
      if (carrier.includes("au") || carrier.includes("kddi")) {
        document.getElementById("proxy-apn").value = APN_PRESETS["AU/KDDI"];
        return;
      }
    }
  }
}

// ===== Load Proxy List =====
async function loadProxyList() {
  try {
    const response = await fetch(`${PROXY_API}/list`);
    const result = await response.json();

    if (result.success) {
      proxyList = result.data || [];
      renderProxyGrid(proxyList);
      updateProxyOverview(proxyList);
      updateProxyBadge(proxyList);
    }
  } catch (e) {
    console.error("❌ Error loading proxy list:", e);
    showToast("error", "Lỗi tải danh sách proxy: " + e.message);
  }
}

// ===== Render Proxy Grid =====
function renderProxyGrid(proxies) {
  const grid = document.getElementById("proxy-grid");
  if (!grid) return;

  if (!proxies || proxies.length === 0) {
    grid.innerHTML = `
      <div class="empty-state">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <circle cx="12" cy="12" r="10" />
          <line x1="2" y1="12" x2="22" y2="12" />
          <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10" />
        </svg>
        <p>Chưa có proxy nào</p>
        <span>Nhấn "Start All" hoặc scan SIM trước</span>
      </div>`;
    return;
  }

  grid.innerHTML = proxies.map(proxy => renderProxyCard(proxy)).join("");

  // Attach event listeners to buttons
  grid.querySelectorAll("[data-action]").forEach(btn => {
    btn.addEventListener("click", (e) => {
      const action = btn.dataset.action;
      const comPort = btn.dataset.port;
      handleProxyAction(action, comPort);
    });
  });
}

function renderProxyCard(proxy) {
  const statusClass = getStatusClass(proxy.status);
  const statusIcon = getStatusIcon(proxy.status);
  const statusText = getStatusText(proxy.status);
  const isConnected = proxy.status === "CONNECTED";
  const isError = proxy.status === "ERROR";
  const isStopped = proxy.status === "STOPPED" || proxy.status === "AVAILABLE" || proxy.status === "NOT_CONFIGURED";

  // Format uptime
  const uptime = proxy.uptimeSeconds > 0 ? formatUptime(proxy.uptimeSeconds) : "-";
  const traffic = proxy.totalBytes > 0 ? formatBytes(proxy.totalBytes) : "0 B";

  return `
    <div class="proxy-card ${statusClass}" data-port="${proxy.comPort}">
      <div class="proxy-card-header">
        <div class="proxy-port-info">
          <span class="proxy-com-port">${proxy.comPort}</span>
          <span class="proxy-carrier">${proxy.carrier || "Unknown"}</span>
        </div>
        <div class="proxy-status-badge ${statusClass}">
          ${statusIcon} ${statusText}
        </div>
      </div>

      <div class="proxy-card-body">
        <div class="proxy-info-row">
          <span class="proxy-info-label">📱 SIM</span>
          <span class="proxy-info-value">${proxy.phoneNumber || "N/A"}</span>
        </div>
        ${isConnected ? `
        <div class="proxy-info-row highlight">
          <span class="proxy-info-label">🌐 Proxy</span>
          <span class="proxy-info-value proxy-address" onclick="copyToClipboard('${proxy.proxyAddress || ""}')" title="Click để copy">
            ${proxy.proxyAddress || "-"}
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:14px;height:14px;margin-left:4px;opacity:0.6;">
              <rect x="9" y="9" width="13" height="13" rx="2" /><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
            </svg>
          </span>
        </div>
        <div class="proxy-info-row">
          <span class="proxy-info-label">🔑 IP Public</span>
          <span class="proxy-info-value" style="color:var(--success-light)">${proxy.publicIp || "Detecting..."}</span>
        </div>
        <div class="proxy-info-row">
          <span class="proxy-info-label">🔄 Rotated</span>
          <span class="proxy-info-value">${proxy.rotateCount || 0} lần</span>
        </div>
        <div class="proxy-info-row">
          <span class="proxy-info-label">📊 Requests</span>
          <span class="proxy-info-value">${proxy.totalRequests || 0}</span>
        </div>
        <div class="proxy-info-row">
          <span class="proxy-info-label">📦 Traffic</span>
          <span class="proxy-info-value">${traffic}</span>
        </div>
        <div class="proxy-info-row">
          <span class="proxy-info-label">⏱️ Uptime</span>
          <span class="proxy-info-value">${uptime}</span>
        </div>
        ${proxy.authRequired ? `
        <div class="proxy-info-row">
          <span class="proxy-info-label">🔐 Auth</span>
          <span class="proxy-info-value">${proxy.username}:***</span>
        </div>` : ""}
        ` : ""}
        ${isError ? `
        <div class="proxy-info-row error-row">
          <span class="proxy-info-label">❌ Lỗi</span>
          <span class="proxy-info-value" style="color:var(--danger)">${proxy.errorMessage || "Unknown error"}</span>
        </div>` : ""}
      </div>

      <div class="proxy-card-actions">
        ${isStopped ? `
          <button class="btn btn-sm btn-primary proxy-action-btn" data-action="start" data-port="${proxy.comPort}">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3" /></svg>
            Start
          </button>
        ` : ""}
        ${isConnected ? `
          <button class="btn btn-sm btn-warning proxy-action-btn" data-action="rotate" data-port="${proxy.comPort}">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38" /></svg>
            Đổi IP
          </button>
          <button class="btn btn-sm btn-danger proxy-action-btn" data-action="stop" data-port="${proxy.comPort}">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="6" y="6" width="12" height="12" /></svg>
            Stop
          </button>
        ` : ""}
        ${isError ? `
          <button class="btn btn-sm btn-primary proxy-action-btn" data-action="start" data-port="${proxy.comPort}">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38" /></svg>
            Retry
          </button>
          <button class="btn btn-sm btn-outline proxy-action-btn" data-action="stop" data-port="${proxy.comPort}">
            Clear
          </button>
        ` : ""}
        ${proxy.status === "CONNECTING" ? `
          <div class="proxy-connecting">
            <div class="spinner-sm"></div>
            <span>Đang kết nối...</span>
          </div>
        ` : ""}
      </div>
    </div>
  `;
}

// ===== Update Overview Stats =====
function updateProxyOverview(proxies) {
  const running = proxies.filter(p => p.status === "CONNECTED").length;
  const available = proxies.filter(p => p.status === "AVAILABLE" || p.status === "STOPPED").length;
  const totalRequests = proxies.reduce((sum, p) => sum + (p.totalRequests || 0), 0);
  const totalBytes = proxies.reduce((sum, p) => sum + (p.totalBytes || 0), 0);

  document.getElementById("proxy-stat-running").textContent = running;
  document.getElementById("proxy-stat-available").textContent = available;
  document.getElementById("proxy-stat-requests").textContent = totalRequests.toLocaleString();
  document.getElementById("proxy-stat-traffic").textContent = formatBytes(totalBytes);
}

function updateProxyBadge(proxies) {
  const running = proxies.filter(p => p.status === "CONNECTED").length;
  const badge = document.getElementById("proxy-badge");
  if (badge) {
    badge.textContent = running;
    badge.style.display = running > 0 ? "inline-flex" : "none";
    badge.style.background = "var(--success)";
  }
}

// ===== Actions =====
async function handleProxyAction(action, comPort) {
  switch (action) {
    case "start":
      await proxyStart(comPort);
      break;
    case "stop":
      await proxyStop(comPort);
      break;
    case "rotate":
      await proxyRotateIp(comPort);
      break;
  }
}

async function proxyStart(comPort) {
  const apn = (document.getElementById("proxy-apn") ? document.getElementById("proxy-apn").value : undefined) || "rakuten.jp";

  showToast("info", `🚀 Starting proxy cho ${comPort}...`);

  try {
    const response = await fetch(`${PROXY_API}/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        comPort: comPort,
        apn: apn,
        proxyPort: 0  // auto-assign
      })
    });
    const result = await response.json();

    if (result.success) {
      showToast("success", `✅ Proxy ${comPort} started! Port: ${result.data.proxyPort}`);
      await loadProxyList();
    } else {
      showToast("error", `❌ ${result.message || "Lỗi start proxy"}`);
    }
  } catch (e) {
    showToast("error", `❌ Lỗi start proxy: ${e.message}`);
  }
}

async function proxyStop(comPort) {
  showToast("info", `🛑 Stopping proxy ${comPort}...`);

  try {
    const response = await fetch(`${PROXY_API}/stop/${comPort}`, { method: "POST" });
    const result = await response.json();

    if (result.success) {
      showToast("success", `✅ Proxy ${comPort} stopped`);
      await loadProxyList();
    } else {
      showToast("error", `❌ ${result.message || "Lỗi stop proxy"}`);
    }
  } catch (e) {
    showToast("error", `❌ Lỗi stop proxy: ${e.message}`);
  }
}

async function proxyRotateIp(comPort) {
  showToast("info", `🔄 Rotating IP for ${comPort}...`);

  try {
    const response = await fetch(`${PROXY_API}/rotate/${comPort}`, { method: "POST" });
    const result = await response.json();

    if (result.success) {
      showToast("success", `✅ IP rotated! New IP: ${result.data.publicIp}`);
      await loadProxyList();
    } else {
      showToast("error", `❌ ${result.message || "Lỗi rotate IP"}`);
    }
  } catch (e) {
    showToast("error", `❌ Lỗi rotate IP: ${e.message}`);
  }
}

async function proxyStartAll() {
  const apn = (document.getElementById("proxy-apn") ? document.getElementById("proxy-apn").value : undefined) || "rakuten.jp";

  if (!confirm(`Start proxy cho TẤT CẢ SIM với APN: ${apn}?`)) return;

  showToast("info", "🚀 Starting all proxies...");

  try {
    const response = await fetch(`${PROXY_API}/start-all?apn=${encodeURIComponent(apn)}`, {
      method: "POST"
    });
    const result = await response.json();

    if (result.success) {
      const count = (result.data ? result.data.length : undefined) || 0;
      showToast("success", `✅ Started ${count} proxies`);
      await loadProxyList();
    } else {
      showToast("error", `❌ ${result.message || "Lỗi start all"}`);
    }
  } catch (e) {
    showToast("error", `❌ Lỗi: ${e.message}`);
  }
}

async function proxyStopAll() {
  if (!confirm("Dừng TẤT CẢ proxy đang chạy?")) return;

  showToast("info", "🛑 Stopping all proxies...");

  try {
    const response = await fetch(`${PROXY_API}/stop-all`, { method: "POST" });
    const result = await response.json();

    if (result.success) {
      showToast("success", "✅ Tất cả proxy đã dừng");
      await loadProxyList();
    } else {
      showToast("error", `❌ ${result.message}`);
    }
  } catch (e) {
    showToast("error", `❌ Lỗi: ${e.message}`);
  }
}

async function proxyExport() {
  try {
    const response = await fetch(`${PROXY_API}/export`);
    const result = await response.json();

    if (result.success && result.data) {
      // Copy to clipboard
      await navigator.clipboard.writeText(result.data);
      showToast("success", "📋 Đã copy danh sách proxy vào clipboard!");

      // Also show in a modal/alert
      console.log("Proxy list:\n" + result.data);
    } else {
      showToast("warning", "Không có proxy đang chạy để export");
    }
  } catch (e) {
    showToast("error", `❌ Lỗi export: ${e.message}`);
  }
}

// ===== Helpers =====
function getStatusClass(status) {
  switch (status) {
    case "CONNECTED": return "status-connected";
    case "CONNECTING": return "status-connecting";
    case "ERROR": return "status-error";
    case "STOPPED": return "status-stopped";
    case "AVAILABLE": return "status-available";
    default: return "status-stopped";
  }
}

function getStatusIcon(status) {
  switch (status) {
    case "CONNECTED": return "🟢";
    case "CONNECTING": return "🟡";
    case "ERROR": return "🔴";
    case "STOPPED": return "⚫";
    case "AVAILABLE": return "🔵";
    default: return "⚪";
  }
}

function getStatusText(status) {
  switch (status) {
    case "CONNECTED": return "Connected";
    case "CONNECTING": return "Connecting...";
    case "ERROR": return "Error";
    case "STOPPED": return "Stopped";
    case "AVAILABLE": return "Available";
    default: return status || "Unknown";
  }
}

function formatUptime(seconds) {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

function formatBytes(bytes) {
  if (bytes === 0) return "0 B";
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GB";
}

function copyToClipboard(text) {
  if (!text) return;
  navigator.clipboard.writeText(text).then(() => {
    showToast("success", `📋 Đã copy: ${text}`);
  }).catch(() => {
    // Fallback
    const ta = document.createElement("textarea");
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
    showToast("success", `📋 Đã copy: ${text}`);
  });
}
