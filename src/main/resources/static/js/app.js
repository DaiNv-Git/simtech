/**
 * GSM Smart App - Main JavaScript
 */

// ===== Configuration =====
const API_BASE = "/api/gsm";
let stompClient = null;
let currentPage = "dashboard";
let simList = [];

// Pagination and Search State
let smsCurrentPage = 0;
let smsSearchPhone = "";
let callCurrentPage = 0;
let callSearchPhone = "";
let callTotalPages = 1;

// ===== Initialize =====
document.addEventListener("DOMContentLoaded", async () => {
  // Initialize i18n first
  if (typeof i18n !== 'undefined') {
    try {
      const savedLang = localStorage.getItem("appLanguage") || "vi";
      await i18n.loadLanguage(savedLang);
    } catch (e) {
      console.warn("Failed to load saved language, using default");
      try { await i18n.loadLanguage("vi"); } catch(err) {}
    }
  }

  initNavigation();
  initWebSocket();
  initEventListeners();
  initLanguageSelector(); // Initialize language selector early
  initCallSimSearch(); // 🆕 Initialize call SIM search
  await loadSimListFromServer();
  loadDashboard();
  loadUnreadCount(); // Load unread badge
});

// ===== Navigation =====
function initNavigation() {
  document.querySelectorAll(".nav-item").forEach((item) => {
    item.addEventListener("click", (e) => {
      e.preventDefault();
      const page = item.dataset.page;
      navigateTo(page);
    });
  });

  // Also handle btn-link navigation
  document.querySelectorAll("[data-page]").forEach((link) => {
    link.addEventListener("click", (e) => {
      e.preventDefault();
      navigateTo(link.dataset.page);
    });
  });
}

function navigateTo(page) {
  currentPage = page;

  // Update nav items
  document.querySelectorAll(".nav-item").forEach((item) => {
    item.classList.toggle("active", item.dataset.page === page);
  });

  // Show correct page
  document.querySelectorAll(".page").forEach((p) => {
    p.classList.toggle("active", p.id === `page-${page}`);
  });

  // Update header
  const titles = {
    dashboard: { title: "Dashboard", subtitle: "Tổng quan hệ thống GSM" },
    "scan-sim": { title: "Scan SIM", subtitle: "Quét và quản lý SIM cards" },
    messages: {
      title: "Tin nhắn",
      subtitle: "Quản lý SMS - Inbox, Sent, Outbox",
    },
    calls: { title: "Gọi điện", subtitle: "Thực hiện cuộc gọi và xem lịch sử" },
    recordings: { title: "Ghi âm", subtitle: "Quản lý file ghi âm" },
    proxy: { title: "Proxy", subtitle: "Quản lý Mobile Proxy - mỗi SIM 1 IP riêng" },
    "fail-tracking": { title: "Lỗi SIM", subtitle: "Theo dõi lỗi gửi SMS và Blacklist" },

  };

  document.getElementById("page-title").textContent =
    (titles[page] ? titles[page].title : undefined) || page;
  document.getElementById("page-subtitle").textContent =
    (titles[page] ? titles[page].subtitle : undefined) || "";

  // Load page data
  switch (page) {
    case "dashboard":
      loadDashboard();
      // 🔧 FIX: Re-render simList khi quay lại dashboard
      if (simList.length > 0) {
        renderSimList(simList);
        updateComPortSelects(simList);
      }
      break;
    case "scan-sim":
      // 🔧 FIX: Re-render simList khi vào tab scan-sim
      if (simList.length > 0) {
        renderSimList(simList);
        updateComPortSelects(simList);
      }
      break;
    case "messages":
      loadMessages();
      markAllAsRead(); // Đánh dấu tất cả đã đọc khi vào tab messages
      break;
    case "calls":
      loadCallHistory();
      break;
    case "recordings":
      loadRecordings();
      break;
    case "proxy":
      if (typeof initProxy === "function") initProxy();
      if (typeof loadProxyList === "function") loadProxyList();
      break;

    case "fail-tracking":
      loadErrorSims();
      loadFailStats();
      break;
  }
}

// ===== WebSocket =====
function initWebSocket() {
  try {
    const socket = new SockJS("/ws");
    stompClient = Stomp.over(socket);
    stompClient.debug = null; // Disable debug logs

    stompClient.connect(
      {},
      (frame) => {
        updateConnectionStatus(true);

        // 🔧 Subscribe to unified /topic/sims for all SIM updates
        // Handles both single SIM (array with 1 element) and full array updates
        stompClient.subscribe("/topic/sims", (message) => {
          const data = JSON.parse(message.body);
          handleSimUpdate(data);
        });

        stompClient.subscribe("/topic/sms/new", (message) => {
          const sms = JSON.parse(message.body);
          handleNewSms(sms);
        });

        stompClient.subscribe("/topic/sms/inbox", (message) => {
          const smsEvent = JSON.parse(message.body);
          handleSmsInboxUpdate(smsEvent);
        });

        stompClient.subscribe("/topic/sms/status", (message) => {
          const sms = JSON.parse(message.body);
          handleSmsStatus(sms);
        });

        stompClient.subscribe("/topic/call/status", (message) => {
          const call = JSON.parse(message.body);
          handleCallStatus(call);
        });

        // 🆕 Subscribe cho cuộc gọi đến (incoming call)
        stompClient.subscribe("/topic/call/incoming", (message) => {
          const incomingCall = JSON.parse(message.body);
          handleIncomingCallStatus(incomingCall);
        });

        // 🆕 Subscribe để cập nhật badge unread
        stompClient.subscribe("/topic/sms/unread-count", (message) => {
          const count = JSON.parse(message.body);
          updateUnreadBadge(count);
        });

        // 🆕 Subscribe proxy updates
        stompClient.subscribe("/topic/proxy", (message) => {
          const data = JSON.parse(message.body);
          if (typeof proxyList !== "undefined" && Array.isArray(data)) {
            proxyList = data;
            if (typeof renderProxyGrid === "function") renderProxyGrid(data);
            if (typeof updateProxyOverview === "function") updateProxyOverview(data);
            if (typeof updateProxyBadge === "function") updateProxyBadge(data);
          }
        });
      },
      (error) => {
        console.error("WebSocket error:", error);
        updateConnectionStatus(false);
        // Reconnect after 5 seconds
        setTimeout(initWebSocket, 5000);
      }
    );
  } catch (e) {
    console.error("WebSocket init error:", e);
    updateConnectionStatus(false);
  }
}

function updateConnectionStatus(online) {
  const status = document.getElementById("connection-status");
  const dot = status.querySelector(".status-dot");
  const text = status.querySelector(".status-text");

  dot.classList.toggle("online", online);
  text.textContent = online ? "Đã kết nối" : "Đang kết nối...";
}

// ===== Language Selector =====
function initLanguageSelector() {
  const langSelect = document.getElementById("language-select");
  if (!langSelect) return;

  // Set initial value from i18n current language
  langSelect.value = i18n.currentLang || "vi";

  // Add change listener
  langSelect.addEventListener("change", async (e) => {
    const newLang = e.target.value;

    try {
      // Save to localStorage for i18n.js sync
      localStorage.setItem("appLanguage", newLang);

      // Save to backend
      const response = await fetch(`${API_BASE}/settings`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ language: newLang }),
      });
      const result = await response.json();

      if (result.success) {
        // Load new language translations
        await i18n.loadLanguage(newLang);
        showToast("success", i18n.t("toasts.languageChanged"));
      } else {
        showToast("error", "Lỗi lưu ngôn ngữ");
      }
    } catch (err) {
      console.error("Error saving language:", err);
      showToast("error", "Lỗi lưu ngôn ngữ");
    }
  });

  console.log("✅ Language selector initialized");
}

// ===== Event Listeners =====
function initEventListeners() {
  // Refresh button
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("refresh-btn"))("click", () => {
    switch (currentPage) {
      case "dashboard":
        loadDashboard();
        loadSimListFromServer();
        break;
      case "scan-sim":
        loadSimListFromServer();
        break;
      case "messages":
        loadMessages();
        break;
      case "calls":
        loadCallHistory();
        break;
      case "recordings":
        loadRecordings();
        break;
      case "proxy":
        if (typeof loadProxyList === "function") loadProxyList();
        break;
    }
  });

  // Scan SIM buttons
  document.querySelectorAll("#scan-sim-btn, #do-scan-btn").forEach(btn => {
      if(btn) btn.addEventListener("click", scanSims);
  });
  
  // Dashboard Refresh button
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("sys-refresh-btn"))("click", () => {
      loadDashboard();
      loadSimListFromServer();
  });
  
  // Dashboard SIM filters
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("sim-search-input"))("input", () => renderSimList(simList));
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("sim-status-filter"))("change", () => renderSimList(simList));

  // Control Panel SIM filters
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("sim-ctrl-search"))("input", () => {
      if(typeof renderSimControlPanel === "function") renderSimControlPanel(simList);
  });
  document.querySelectorAll(".sim-filter-btn").forEach(btn => {
      btn.addEventListener("click", (e) => {
          document.querySelectorAll(".sim-filter-btn").forEach(b => b.classList.remove("active"));
          e.target.classList.add("active");
          if(typeof renderSimControlPanel === "function") renderSimControlPanel(simList);
      });
  });

  // Message Tabs
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document
        .querySelectorAll(".tab-btn")
        .forEach((b) => b.classList.remove("active"));
      document
        .querySelectorAll(".tab-pane")
        .forEach((p) => p.classList.remove("active"));
      btn.classList.add("active");
      document.getElementById(`tab-${btn.dataset.tab}`).classList.add("active");
    });
  });

  // SMS Search
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("sms-phone-search"))("input", (e) => {
    smsSearchPhone = e.target.value.trim();
    loadMessages();
  });

  // Read SMS button
  const readSmsBtn = document.getElementById("read-sms-btn");
  if (readSmsBtn) readSmsBtn.addEventListener("click", readSmsFromModem);

  // Send SMS button
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("send-sms-btn"))("click", sendSms);
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("send-modal-sms-btn"))("click", sendSms);

  // Modal close
  document
    .querySelectorAll(".modal-close, .modal-cancel, .modal-overlay")
    .forEach((el) => {
      el.addEventListener("click", closeModal);
    });

  // Call form
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("call-form"))("submit", (e) => {
    e.preventDefault();
    makeCall();
  });

  // Call COM port change - auto-fill SIM phone number
  const callComPortBtn = document.getElementById("call-com-port");
  if (callComPortBtn) callComPortBtn.addEventListener("change", async (e) => {
      const comPort = e.target.value;
      const phoneInput = document.getElementById("call-sim-phone");

      // Thử tìm trong simList trước
      const sim = simList.find((s) => s.comPort === comPort);
      if ((sim ? sim.phoneNumber : undefined)) {
        phoneInput.textContent = sim.phoneNumber;
        return;
      }

      // Fallback: fetch từ API nếu simList chưa có
      if (comPort) {
        try {
          phoneInput.textContent = "Đang kiểm tra...";
          const response = await fetch(`/api/gsm/sims/${comPort}`);
          if (response.ok) {
            const simInfo = await response.json();
            phoneInput.textContent = (simInfo ? simInfo.phoneNumber : undefined) || "Không có số";
          } else {
            phoneInput.textContent = "Không có số";
          }
        } catch (err) {
          phoneInput.textContent = "Lỗi kết nối";
        }
      } else {
        phoneInput.textContent = "Đang chờ...";
      }
    });

  // Refresh calls
  const refCallsBtn = document.getElementById("refresh-calls-btn");
  if (refCallsBtn) refCallsBtn.addEventListener("click", loadCallHistory);

  // Recordings
  const refRecBtn = document.getElementById("refresh-recordings-btn");
  if (refRecBtn) refRecBtn.addEventListener("click", loadRecordings);
  const openFolderBtn = document.getElementById("open-folder-btn");
  if (openFolderBtn) openFolderBtn.addEventListener("click", openRecordingFolder);


  // Character count for SMS
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("sms-content"))("input", (e) => {
    document.getElementById("char-current").textContent = e.target.value.length;
  });
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("sms-modal-content"))("input", (e) => {
    document.getElementById("char-modal-current").textContent = e.target.value.length;
  });

  // Audio player
  const audioCloseBtn = document.getElementById("audio-close");
  if (audioCloseBtn) audioCloseBtn.addEventListener("click", closeAudioPlayer);

  // SMS Phone Search
  const smsSearchInput = document.getElementById("sms-phone-search");
  if (smsSearchInput) {
    let smsSearchTimeout;
    smsSearchInput.addEventListener("input", (e) => {
      clearTimeout(smsSearchTimeout);
      smsSearchTimeout = setTimeout(() => {
        smsSearchPhone = e.target.value.trim();
        smsCurrentPage = 0;
        loadMessages();
      }, 500); // Debounce 500ms
    });
  }

  // Call Phone Search
  const callSearchInput = document.getElementById("call-phone-search");
  if (callSearchInput) {
    let callSearchTimeout;
    callSearchInput.addEventListener("input", (e) => {
      clearTimeout(callSearchTimeout);
      callSearchTimeout = setTimeout(() => {
        callSearchPhone = e.target.value.trim();
        callCurrentPage = 0;
        loadCallHistory();
      }, 500); // Debounce 500ms
    });
  }

  // Call Pagination
  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("call-prev-page"))("click", () => {
    if (callCurrentPage > 0) {
      callCurrentPage--;
      loadCallHistory();
    }
  });

  (function(el){ return el ? el.addEventListener.bind(el) : () => {}; })(document.getElementById("call-next-page"))("click", () => {
    if (callCurrentPage < callTotalPages - 1) {
      callCurrentPage++;
      loadCallHistory();
    }
  });
}

// ===== Dashboard =====
async function loadDashboard() {
  try {
    const response = await fetch(`${API_BASE}/stats`);
    const result = await response.json();

    if (result.success) {
      const stats = result.data;

      // 🔧 FIX: Đếm SIM ONLINE và INACTIVE từ simList
      const activeSims = simList.filter((s) => s.status === "ONLINE").length;
      const inactiveSims = simList.filter(
        (s) => s.status === "INACTIVE" || s.status === "ERROR" || s.status === "COM_ERROR" || s.status === "SIM_ERROR" || s.status === "NO_SIM"
      ).length;

      // Ưu tiên dùng simList nếu có, fallback về stats từ DB
      const simsOnlineCount =
        simList.length > 0 ? activeSims : stats.simsOnline || 0;
      const simsInactiveCount =
        simList.length > 0 ? inactiveSims : stats.simsInactive || 0;

      const elOnline = document.getElementById("sys-sim-online");
      const elOffline = document.getElementById("sys-sim-offline");
      const elSms = document.getElementById("sys-sms-sent");
      const elCalls = document.getElementById("sys-calls-made");
      const elSuccess = document.getElementById("sys-success-rate");
      const elDevices = document.getElementById("sys-active-devices");

      if (elOnline) elOnline.textContent = simsOnlineCount;
      if (elOffline) elOffline.textContent = simsInactiveCount;
      if (elSms) elSms.textContent = stats.totalMessages || 0;
      if (elCalls) elCalls.textContent = stats.totalCalls || 0;
      if (elSuccess) {
          const totalAttempts = (stats.sentCount || 0) + (stats.outboxCount || 0);
          if (totalAttempts > 0) {
              const rate = ((stats.sentCount / totalAttempts) * 100).toFixed(1);
              elSuccess.textContent = `${rate}%`;
          } else {
              elSuccess.textContent = "0%";
          }
      }
      if (elDevices) elDevices.textContent = simList.length || 0;

      updateUnreadBadge(stats.unreadMessages || 0);
      
      // Inject real activity data instead of mock
      const feedEl = document.getElementById("sys-activity-feed");
      if (feedEl) {
          try {
              // Fetch latest 5 sent SMS
              const smsResponse = await fetch(`${API_BASE}/sms/sent?page=0&size=5`);
              const smsData = await smsResponse.json();
              if (smsData.success && smsData.data.content && smsData.data.content.length > 0) {
                  const activities = smsData.data.content.map(sms => {
                      const dateObj = new Date(sms.createdAt);
                      const timeStr = dateObj.toLocaleTimeString('vi-VN') + ' ' + dateObj.toLocaleDateString('vi-VN');
                      return {
                          icon: '✉️',
                          msg: `Gửi SMS đến ${sms.phoneNumber}`,
                          time: timeStr,
                          color: 'var(--success)'
                      };
                  });
                  feedEl.innerHTML = activities.map(a => `
                     <div class="sys-timeline-item">
                        <div class="sys-timeline-icon" style="color: ${a.color}">${a.icon}</div>
                        <div class="sys-timeline-content">
                           <p>${a.msg}</p>
                           <span class="sys-timeline-time">${a.time}</span>
                        </div>
                     </div>
                  `).join('');
              } else {
                  feedEl.innerHTML = '<p style="color: var(--text-muted); font-size: 0.85rem; padding: 10px;">Chưa có hoạt động nào gần đây.</p>';
              }
          } catch (e) {
              feedEl.innerHTML = '<p style="text-align:center; color:var(--text-muted); margin-top:20px;">Đang tải...</p>';
          }
      }
      
      // ✅ FIX: Update SMS Analytics bar chart với dữ liệu thực
      // Lấy SMS SENT hôm nay rồi phân nhóm theo 4 buổi: Sáng/Trưa/Chiều/Tối
      try {
          const todaySentResp = await fetch(`${API_BASE}/sms/sent?page=0&size=200`);
          const todaySentData = await todaySentResp.json();
          if (todaySentData.success && todaySentData.data.content) {
              const now = new Date();
              const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
              
              // Chỉ tính SMS hôm nay
              const todayMsgs = todaySentData.data.content.filter(sms => {
                  const d = new Date(sms.createdAt);
                  return d >= todayStart;
              });
              
              // Phân nhóm: Sáng (5-11h), Trưa (11-14h), Chiều (14-18h), Tối (18-5h)
              let sang = 0, trua = 0, chieu = 0, toi = 0;
              todayMsgs.forEach(sms => {
                  const h = new Date(sms.createdAt).getHours();
                  if (h >= 5 && h < 11) sang++;
                  else if (h >= 11 && h < 14) trua++;
                  else if (h >= 14 && h < 18) chieu++;
                  else toi++;
              });
              
              const maxVal = Math.max(sang, trua, chieu, toi, 1); // tránh chia 0
              const bars = document.querySelectorAll('.sys-bar-chart .bar-col');
              const values = [sang, trua, chieu, toi];
              const labels = ['Sáng', 'Trưa', 'Chiều', 'Tối'];
              
              if (bars.length >= 4) {
                  // Xác định buổi hiện tại để highlight
                  const currentHour = now.getHours();
                  let activeIdx = 3; // Tối mặc định
                  if (currentHour >= 5 && currentHour < 11) activeIdx = 0;
                  else if (currentHour >= 11 && currentHour < 14) activeIdx = 1;
                  else if (currentHour >= 14 && currentHour < 18) activeIdx = 2;
                  
                  bars.forEach((col, i) => {
                      const bar = col.querySelector('.bar');
                      const span = col.querySelector('span');
                      if (bar) {
                          const pct = Math.max(5, (values[i] / maxVal) * 100); // min 5% để luôn thấy
                          bar.style.height = values[i] === 0 ? '3%' : pct + '%';
                          bar.classList.toggle('active', i === activeIdx);
                          bar.title = `${values[i]} tin nhắn`;
                      }
                      if (span) span.textContent = labels[i];
                  });
              }
          }
      } catch (chartErr) {
          console.debug('Chart update skipped:', chartErr);
      }
      
      const topSms = document.getElementById("sys-top-sms-sims");
      if (topSms) {
          if (simList && simList.length > 0) {
              const smsSorted = [...simList].filter(s => (s.todaySms || 0) > 0).sort((a, b) => (b.todaySms || 0) - (a.todaySms || 0)).slice(0, 5);
              if (smsSorted.length > 0) {
                  topSms.innerHTML = smsSorted.map(s => `
                      <li style="display: flex; justify-content: space-between; align-items: center; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.05); font-size: 0.9rem;">
                          <div style="display: flex; flex-direction: column;">
                              <span style="font-weight: 500; color: var(--text-primary); margin-bottom: 2px;">${s.phoneNumber || "Unknown ID"}</span>
                              <span style="font-size: 0.75rem; color: var(--text-muted); font-family: monospace;">${s.comPort}</span>
                          </div>
                          <span style="background: rgba(94, 106, 210, 0.15); color: var(--primary-light); padding: 4px 10px; border-radius: 12px; font-weight: 600; font-size: 0.8rem; border: 1px solid rgba(94, 106, 210, 0.2);">
                              ${s.todaySms} tin
                          </span>
                      </li>
                  `).join('');
              } else {
                  topSms.innerHTML = `<li><span style="color:var(--text-muted); font-size: 0.85rem;">Chưa có dữ liệu gửi SMS hôm nay.</span></li>`;
              }
          } else {
              topSms.innerHTML = `<li><span style="color:var(--text-muted); font-size: 0.85rem;">Hệ thống đang tổng hợp dữ liệu...</span></li>`;
          }
      }
      
      const topCall = document.getElementById("sys-top-call-sims");
      if (topCall) {
          if (simList && simList.length > 0) {
              const callSorted = [...simList].filter(s => (s.todayCall || 0) > 0).sort((a, b) => (b.todayCall || 0) - (a.todayCall || 0)).slice(0, 5);
              if (callSorted.length > 0) {
                  topCall.innerHTML = callSorted.map(s => `
                      <li style="display: flex; justify-content: space-between; align-items: center; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.05); font-size: 0.9rem;">
                          <div style="display: flex; flex-direction: column;">
                              <span style="font-weight: 500; color: var(--text-primary); margin-bottom: 2px;">${s.phoneNumber || "Unknown ID"}</span>
                              <span style="font-size: 0.75rem; color: var(--text-muted); font-family: monospace;">${s.comPort}</span>
                          </div>
                          <span style="background: rgba(245, 158, 11, 0.15); color: var(--warning); padding: 4px 10px; border-radius: 12px; font-weight: 600; font-size: 0.8rem; border: 1px solid rgba(245, 158, 11, 0.2);">
                              ${s.todayCall} cuộc
                          </span>
                      </li>
                  `).join('');
              } else {
                  topCall.innerHTML = `<li><span style="color:var(--text-muted); font-size: 0.85rem;">Chưa có dữ liệu gọi hôm nay.</span></li>`;
              }
          } else {
              topCall.innerHTML = `<li><span style="color:var(--text-muted); font-size: 0.85rem;">Hệ thống đang tổng hợp dữ liệu...</span></li>`;
          }
      }
    }
  } catch (e) {
    console.error("Error loading dashboard:", e);
  }
}

// ===== SIM Scanning =====
let scanningInProgress = false;

async function loadSimListFromServer() {
  try {
    const response = await fetch(`${API_BASE}/sim/list`);
    const result = await response.json();

    if (result.success && Array.isArray(result.data)) {
      handleSimUpdate(result.data);

      if (simList.length === 0) {
        renderSimList([]);
      }
    }
  } catch (e) {
    console.error("Error loading SIM list:", e);
  }
}

async function scanSims() {
  if (scanningInProgress) {
    showToast("info", "Đang scan, vui lòng chờ...");
    return;
  }

  scanningInProgress = true;

  // 🆕 PROGRESSIVE LOADING: Clear list nhưng KHÔNG block giao diện
  simList = [];
  renderSimList([]); // Hiển thị trạng thái trống ban đầu
  updateScanStatus(0, "Đang quét COM ports...");

  // Hiện loading nhẹ nhàng
  const scanBtn = document.getElementById("scan-sim-btn");
  const doScanBtn = document.getElementById("do-scan-btn");
  if (scanBtn) scanBtn.disabled = true;
  if (doScanBtn) doScanBtn.disabled = true;

  try {
    // 🆕 Gọi API nhưng KHÔNG chờ - SIM sẽ được push qua WebSocket /topic/sims/found
    fetch(`${API_BASE}/sim/scan`)
      .then((response) => response.json())
      .then((result) => {
        scanningInProgress = false;
        if (scanBtn) scanBtn.disabled = false;
        if (doScanBtn) doScanBtn.disabled = false;

        if (result.success) {
          // Final update - lúc này simList đã được cập nhật qua WebSocket
          updateScanStatus(
            100,
            `Scan hoàn tất! Tìm thấy ${simList.length} SIM`
          );
          showToast("success", `Scan hoàn tất! Tìm thấy ${simList.length} SIM`);
        }
      })
      .catch((e) => {
        scanningInProgress = false;
        if (scanBtn) scanBtn.disabled = false;
        if (doScanBtn) doScanBtn.disabled = false;
        console.error("Error scanning:", e);
        showToast("error", "Lỗi khi scan SIM: " + e.message);
        updateScanStatus(0, "Lỗi scan");
      });
  } catch (e) {
    scanningInProgress = false;
    if (scanBtn) scanBtn.disabled = false;
    if (doScanBtn) doScanBtn.disabled = false;
    console.error("Error scanning:", e);
    showToast("error", "Lỗi khi scan SIM: " + e.message);
    updateScanStatus(0, "Lỗi scan");
  }
}

function renderSimList(sims) {
    const translateStatus = (status) => {
        if (!status) return "N/A";
        switch (status.toUpperCase()) {
            case "ONLINE": return "Sẵn sàng";
            case "INACTIVE": return "Không có số";
            case "COM_ERROR": return "Lỗi cổng COM";
            case "NO_SIM": return "Không nhận SIM";
            case "SIM_ERROR": return "Lỗi SIM";
            default: return status;
        }
    };

  // Search filtering logic
  const simSearchInput = document.getElementById("sim-search-input");
  const simStatusFilter = document.getElementById("sim-status-filter");
  
  let filteredSims = sims;
  
  // Apply Search
  if (simSearchInput && simSearchInput.value) {
      const term = simSearchInput.value.toLowerCase();
      filteredSims = filteredSims.filter(s => 
          (s.phoneNumber && s.phoneNumber.toLowerCase().includes(term)) || 
          (s.comPort && s.comPort.toLowerCase().includes(term)) ||
          (s.carrier && s.carrier.toLowerCase().includes(term))
      );
  }

  // Apply Status Filter
  if (simStatusFilter && simStatusFilter.value !== "ALL") {
      const fs = simStatusFilter.value;
      if (fs === "ACTIVE") {
          filteredSims = filteredSims.filter(s => s.status === "ONLINE");
      } else if (fs === "ERROR") {
          filteredSims = filteredSims.filter(s => s.status !== "ONLINE");
      }
  }

  const generateSimRows = (simsData) => {
    if (simsData.length === 0) {
      return `
        <tr>
          <td colspan="7">
            <div class="empty-state" style="padding: 60px 0;">
                <div class="empty-icon-wrapper" style="width: 80px; height: 80px; background: rgba(94, 106, 210, 0.1); border-radius: 50%; display: flex; align-items: center; justify-content: center; margin: 0 auto 20px;">
                <svg viewBox="0 0 24 24" fill="none" class="w-8 h-8" stroke="var(--primary)" stroke-width="1.5" style="width:40px; height:40px;">
                    <rect x="5" y="2" width="14" height="20" rx="2" />
                    <line x1="12" y1="18" x2="12" y2="18" />
                </svg>
                </div>
                <p style="font-size: 1.1rem; font-weight: 500;">Không tìm thấy SIM/Port</p>
                <span style="color: var(--text-muted); font-size: 0.9rem;">Thử thay đổi bộ lọc tìm kiếm</span>
            </div>
          </td>
        </tr>
      `;
    }
    
    return simsData.map(sim => {
        const isOnline = sim.status === "ONLINE";
        
        let statusIndicator = "";
        let connectionColor = "var(--danger)";
        
        if (isOnline) {
            statusIndicator = `<span style="color:var(--success);font-weight:600;"><span style="display:inline-block; width:8px; height:8px; background:var(--success); border-radius:50%; margin-right:4px;"></span>Sẵn sàng</span>`;
            connectionColor = "var(--success)";
        } else if (sim.status === "INACTIVE") {
            statusIndicator = `<span style="color:var(--warning);font-weight:600;"><span style="display:inline-block; width:8px; height:8px; background:var(--warning); border-radius:50%; margin-right:4px;"></span>Idle</span>`;
            connectionColor = "var(--warning)";
        } else {
            statusIndicator = `<span style="color:var(--danger);font-weight:600;"><span style="display:inline-block; width:8px; height:8px; background:var(--danger); border-radius:50%; margin-right:4px;"></span>${translateStatus(sim.status)}</span>`;
        }
        
        const smsNay = sim.todaySms || 0; 
        const callNay = sim.todayCall || 0;

        return `
            <tr data-port="${sim.comPort}" style="cursor: pointer; transition: background 0.2s ease;">
                <td>
                    <div style="display:flex; flex-direction: column;">
                        <strong style="color: var(--text-primary); font-size: 1.05rem;">${sim.phoneNumber || "Unknown ID"}</strong>
                    </div>
                </td>
                <td><span style="background: rgba(94, 106, 210, 0.1); color: var(--primary); padding: 4px 10px; border-radius: 6px; font-weight: 600; font-family: monospace;">${sim.comPort}</span></td>
                <td><span style="color: ${connectionColor}; font-weight: 600; font-size: 0.85rem; border: 1px solid ${connectionColor}; padding: 2px 8px; border-radius: 12px; opacity: 0.8;">Kết nối OK</span></td>
                <td>${statusIndicator}</td>
                <td><strong style="color: var(--primary-light);">${smsNay}</strong> <span style="font-size:0.8rem; color:var(--text-muted)">tin</span></td>
                <td><strong style="color: var(--warning);">${callNay}</strong> <span style="font-size:0.8rem; color:var(--text-muted)">cuộc</span></td>
                <td>
                    <div style="display: flex; gap: 8px;">
                        <button class="btn btn-sm btn-outline" onclick="restartPort('${sim.comPort}')" title="Khởi động lại Port">⟳</button>
                        <button class="btn btn-sm btn-outline" style="color: var(--danger); border-color: rgba(239, 68, 68, 0.3);" title="Vô hiệu hóa">✕</button>
                    </div>
                </td>
            </tr>
        `;
    }).join("");
  };

  // Dashboard grid
  const tbody = document.getElementById("sim-tbody");
  if (tbody) {
      tbody.innerHTML = generateSimRows(filteredSims);
  }

  // Scan page control panel
  if (typeof renderSimControlPanel === "function") {
      renderSimControlPanel(sims);
  }
}

let activeCtrlFilter = "ALL";

function renderSimControlPanel(sims) {
    const grid = document.getElementById("sim-list-grid");
    if (!grid) return;

    // Mini stats update
    const countTotal = sims.length;
    const countActive = sims.filter(s => s.status === "ONLINE").length;
    const countBusy = sims.filter(s => s.status === "BUSY" || s.status === "CALLING").length;
    const countIdle = sims.filter(s => s.status === "INACTIVE").length;
    const countError = countTotal - countActive - countBusy - countIdle;

    document.getElementById("ctrl-stat-total").textContent = countTotal;
    document.getElementById("ctrl-stat-active").textContent = countActive;
    document.getElementById("ctrl-stat-busy").textContent = countBusy;
    document.getElementById("ctrl-stat-error").textContent = countError;
    document.getElementById("ctrl-stat-idle").textContent = countIdle;

    // Update alert
    const alertBanner = document.getElementById("sim-ctrl-alert");
    const alertCount = document.getElementById("sim-alert-count");
    const alertList = document.getElementById("sim-alert-list");
    
    if (countError > 0) {
        alertBanner.style.display = "flex";
        alertCount.textContent = countError;
        
        // Liệt kê các cổng bị lỗi
        const errorSims = sims.filter(s => s.status !== "ONLINE" && s.status !== "BUSY" && s.status !== "CALLING" && s.status !== "INACTIVE");
        if (alertList) {
            alertList.textContent = "(" + errorSims.map(s => s.comPort).join(", ") + ")";
        }
    } else {
        alertBanner.style.display = "none";
    }

    // Filter Logic
    const searchInput = document.getElementById("sim-ctrl-search");
    let term = searchInput ? searchInput.value.toLowerCase() : "";
    
    // Get active tab filter
    const activeBtn = document.querySelector(".sim-filter-btn.active");
    activeCtrlFilter = activeBtn ? activeBtn.getAttribute("data-filter") : "ALL";

    let filtered = sims;

    if (term) {
        filtered = filtered.filter(s => 
            (s.phoneNumber && s.phoneNumber.toLowerCase().includes(term)) || 
            (s.comPort && s.comPort.toLowerCase().includes(term)) ||
            (s.carrier && s.carrier.toLowerCase().includes(term))
        );
    }

    if (activeCtrlFilter !== "ALL") {
        if (activeCtrlFilter === "ONLINE") {
            filtered = filtered.filter(s => s.status === "ONLINE");
        } else if (activeCtrlFilter === "BUSY") {
            filtered = filtered.filter(s => s.status === "BUSY" || s.status === "CALLING");
        } else if (activeCtrlFilter === "INACTIVE") {
            filtered = filtered.filter(s => s.status === "INACTIVE");
        } else if (activeCtrlFilter === "ERROR") {
            filtered = filtered.filter(s => s.status !== "ONLINE" && s.status !== "BUSY" && s.status !== "CALLING" && s.status !== "INACTIVE");
        }
    }

    if (filtered.length === 0) {
        grid.innerHTML = `
            <div class="empty-state card" style="grid-column: 1 / -1; padding: 80px 20px; border-style: dashed; text-align: center;">
                <div style="background: var(--bg-dark); width: 64px; height: 64px; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin: 0 auto 16px;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="var(--text-muted)" stroke-width="2" style="width: 32px;"><rect x="5" y="2" width="14" height="20" rx="2"></rect><line x1="12" y1="18" x2="12" y2="18"></line></svg>
                </div>
                <h3 style="margin-bottom: 8px;">Không tìm thấy SIM</h3>
                <p style="color: var(--text-muted); font-size: 0.95rem;">Không có SIM nào khớp với bộ lọc và tìm kiếm hiện tại.</p>
            </div>
        `;
        return;
    }

    grid.innerHTML = filtered.map(sim => {
        const isOnline = sim.status === "ONLINE";
        const isBusy = sim.status === "BUSY" || sim.status === "CALLING";
        const isIdle = sim.status === "INACTIVE";
        
        let statusString = "ERROR";
        let badgeString = "LỖI";
        if (isOnline) { statusString = "ONLINE"; badgeString = "🟢 SẴN SÀNG"; }
        else if (isBusy) { statusString = "BUSY"; badgeString = "🟡 BUSY"; }
        else if (isIdle) { statusString = "INACTIVE"; badgeString = "⚪ IDLE"; }
        else { badgeString = "🔴 " + (sim.status || "LỖI"); }

        const smsNay = sim.todaySms || 0; 
        const callNay = sim.todayCall || 0;

        return `
            <div class="sim-grid-card" data-status="${statusString}">
                <div class="sim-grid-card-header">
                    <div>
                        <div class="sim-grid-card-title">${sim.phoneNumber || "Unknown ID"}</div>
                        <div class="sim-grid-card-subtitle">${sim.carrier || "Chưa xác định"} • <strong>${sim.comPort}</strong></div>
                    </div>
                    <div class="sim-grid-card-badge">${badgeString}</div>
                </div>
                <div class="sim-grid-card-stats">
                    <div class="sim-grid-card-stat">
                        <span>SMS (Nay)</span>
                        <strong>${smsNay}</strong>
                    </div>
                    <div class="sim-grid-card-stat">
                        <span>Call (Nay)</span>
                        <strong>${callNay}</strong>
                    </div>
                </div>
                <div class="sim-grid-card-actions">
                    <button class="btn btn-outline" style="border-radius:8px;">Test</button>
                    <button class="btn btn-outline" style="border-radius:8px;" onclick="restartPort('${sim.comPort}')">Restart</button>
                    <button class="btn btn-outline" style="border-radius:8px; color: var(--danger); border-color: rgba(239, 68, 68, 0.3);">Disable</button>
                </div>
            </div>
        `;
    }).join('');
}

function updateScanStatus(progress, text) {
  const progressBar = document.getElementById("scan-progress");
  const scanText = document.getElementById("scan-text");
  if (progressBar) progressBar.style.width = `${progress}%`;
  if (scanText) scanText.textContent = text;
}

// Global variable to store filtered SIMs for call dropdown
let callSimSearchTerm = "";

function updateComPortSelects(sims) {
  const selects = [
    document.getElementById("call-com-port"),
    document.getElementById("sms-com-port"),
    document.getElementById("sms-modal-com-port"),
    document.getElementById("chat-com-port"),
  ];

  selects.forEach((select) => {
    if (!select) return;
    const currentValue = select.value;
    const isCallSelect = select.id === "call-com-port";
    
    select.innerHTML = '<option value="">-- Chọn COM Port --</option>';
    
    sims.forEach((sim) => {
      // 🔧 FIX: Hiển thị cả ONLINE và INACTIVE (có số điện thoại)
      // Chỉ bỏ qua OFFLINE hoặc REPLACED
      const isAvailable =
        sim.status === "ONLINE" ||
        (sim.status === "INACTIVE" && sim.phoneNumber);
      
      // Filter by search term for call dropdown
      if (isCallSelect && callSimSearchTerm) {
        const searchLower = callSimSearchTerm.toLowerCase();
        const phoneMatch = (sim.phoneNumber ? sim.phoneNumber.toLowerCase() : "").includes(searchLower);
        const carrierMatch = (sim.carrier ? sim.carrier.toLowerCase() : "").includes(searchLower);
        if (!phoneMatch && !carrierMatch) {
          return; // Skip this SIM if it doesn't match search
        }
      }
      
      if (isAvailable) {
        const option = document.createElement("option");
        option.value = sim.comPort;
        const statusIcon = sim.status === "ONLINE" ? "🟢" : "🟡";
        
        // 🆕 Hiển thị nhà mạng (Docomo/Rakuten) trong dropdown
        const carrierName = sim.carrier || "Unknown";
        const carrierShort = getCarrierShortName(carrierName);
        
        option.textContent = `${statusIcon} ${sim.comPort} - ${sim.phoneNumber || "N/A"} (${carrierShort})`;
        option.dataset.phone = sim.phoneNumber || "";
        option.dataset.carrier = carrierName;
        
        select.appendChild(option);
      }
    });
    if (currentValue) select.value = currentValue;
    
    // Apply modern custom select UI
    applyCustomSelect(select.id);
  });
}

// 🌟 APPLY MODERN UI TO SELECT ELEMENTS 🌟
function applyCustomSelect(selectId) {
    const select = document.getElementById(selectId);
    if (!select || select.tagName !== 'SELECT') return;
    
    let wrapper = select.nextElementSibling;
    if (!wrapper || !wrapper.classList.contains('custom-select-wrapper')) {
        // Initialize wrapper structure
        wrapper = document.createElement('div');
        wrapper.className = 'custom-select-wrapper';
        select.parentNode.insertBefore(wrapper, select.nextSibling);
        select.style.display = 'none'; // Hide native select entirely
        
        const trigger = document.createElement('div');
        // Sao chép các class từ native select để mượn CSS 
        trigger.className = 'custom-select-trigger select-input';
        // Tắt background arrow gốc của select-input vì custom-trigger đã tự dùng mũi tên CSS
        trigger.style.backgroundImage = 'none'; 
        
        const list = document.createElement('div');
        list.className = 'custom-select-list';
        
        wrapper.appendChild(trigger);
        wrapper.appendChild(list);

        // Toggle open/close
        trigger.addEventListener('click', (e) => {
            e.stopPropagation();
            // Đóng tất cả dropdown khác
            document.querySelectorAll('.custom-select-wrapper').forEach(w => {
                if (w !== wrapper) w.classList.remove('open');
            });
            wrapper.classList.toggle('open');
        });

        // Click ngoài màn hình sẽ đóng
        document.addEventListener('click', () => {
            wrapper.classList.remove('open');
        });
    }
    
    const trigger = wrapper.querySelector('.custom-select-trigger');
    const list = wrapper.querySelector('.custom-select-list');
    list.innerHTML = ''; // Reset option list
    
    // Render current selection text
    const selectedOption = select.options[select.selectedIndex];
    trigger.innerHTML = selectedOption ? selectedOption.textContent : '-- Chọn --';
    
    // Render options
    Array.from(select.options).forEach(opt => {
        const div = document.createElement('div');
        div.className = 'custom-select-option';
        // Nếu value rỗng, làm mờ nó để chỉ định là placeholder
        if (!opt.value) {
           div.style.color = "var(--text-muted)";
           div.style.fontStyle = "italic";
        }
        div.innerHTML = opt.innerHTML;
        if (opt.selected) div.classList.add('selected');
        
        div.addEventListener('click', (e) => {
            select.value = opt.value;
            trigger.innerHTML = opt.innerHTML;
            wrapper.classList.remove('open');
            
            // Xóa highlight cũ
            Array.from(list.children).forEach(c => c.classList.remove('selected'));
            div.classList.add('selected');
            
            // Bắn event change để các fn khác bắt được
            select.dispatchEvent(new Event('change'));
        });
        list.appendChild(div);
    });
}


// Helper function to get short carrier name
function getCarrierShortName(carrier) {
  if (!carrier) return "Unknown";
  const carrierLower = carrier.toLowerCase();
  if (carrierLower.includes("docomo")) return "Docomo";
  if (carrierLower.includes("rakuten")) return "Rakuten";
  if (carrierLower.includes("softbank")) return "Softbank";
  if (carrierLower.includes("au") || carrierLower.includes("kddi")) return "au";
  return carrier;
}

// 🆕 Initialize call SIM search functionality
function initCallSimSearch() {
  const callComSelect = document.getElementById("call-com-port");
  if (!callComSelect) return;
  
  // Create search input container
  const searchContainer = document.createElement("div");
  searchContainer.className = "call-sim-search-container";
  searchContainer.style.cssText = "margin-bottom: 8px; position: relative;";
  
  const searchInput = document.createElement("input");
  searchInput.type = "text";
  searchInput.id = "call-sim-search";
  searchInput.className = "text-input";
  searchInput.placeholder = "🔍 Tìm số điện thoại hoặc nhà mạng...";
  searchInput.style.cssText = "width: 100%; padding: 8px 12px; border-radius: 6px; border: 1px solid var(--border); background: var(--secondary); color: var(--text-primary); font-size: 14px;";
  
  searchContainer.appendChild(searchInput);
  
  // Insert search input before the select
  const formGroup = callComSelect.parentElement;
  formGroup.insertBefore(searchContainer, callComSelect);
  
  // Add search event listener with debounce
  let searchTimeout;
  searchInput.addEventListener("input", (e) => {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => {
      callSimSearchTerm = e.target.value.trim();
      updateComPortSelects(simList);
    }, 300);
  });
  
  console.log("✅ Call SIM search initialized");
}

// ===== SMS Unread Badge =====
async function loadUnreadCount() {
  try {
    const response = await fetch(`${API_BASE}/sms/unread-count`);
    const result = await response.json();
    updateUnreadBadge(result.data || 0);
  } catch (e) {
    console.error("Error loading unread count:", e);
  }
}

function updateUnreadBadge(count) {
  const badge = document.getElementById("unread-badge");
  if (badge) {
    badge.textContent = count;
    badge.style.display = count > 0 ? "inline-flex" : "none";
  }
}

async function markAllAsRead() {
  try {
    await fetch(`${API_BASE}/sms/mark-all-read`, { method: "PUT" });
    updateUnreadBadge(0);
  } catch (e) {
    console.error("Error marking all as read:", e);
  }
}

let allConversations = {};
let currentConversationPhone = null;
const recentSmsRealtimeEvents = new Map();
const SMS_REALTIME_EVENT_TTL_MS = 30000;
let smsRealtimeRefreshTimer = null;

// ===== SMS =====
async function loadMessages() {
  try {
    const params = new URLSearchParams({
      page: smsCurrentPage,
      size: 50
    });

    if (smsSearchPhone) {
      params.append('phoneNumber', smsSearchPhone);
    }

    const [inbox, sent, outbox] = await Promise.all([
      fetch(`${API_BASE}/sms/inbox?${params}`).then((r) => r.json()),
      fetch(`${API_BASE}/sms/sent?${params}`).then((r) => r.json()),
      fetch(`${API_BASE}/sms/outbox?${params}`).then((r) => r.json()),
    ]);

    renderMessages("inbox-list", (inbox.data ? inbox.data.content : undefined) || [], "inbox");
    renderMessages("sent-list", (sent.data ? sent.data.content : undefined) || [], "sent");
    renderMessages("outbox-list", (outbox.data ? outbox.data.content : undefined) || [], "outbox");

    document.getElementById("inbox-count").textContent =
      (inbox.data ? inbox.data.totalElements : undefined) || 0;
    document.getElementById("sent-count").textContent =
      (sent.data ? sent.data.totalElements : undefined) || 0;
    document.getElementById("outbox-count").textContent =
      (outbox.data ? outbox.data.totalElements : undefined) || 0;
  } catch (e) {
    console.error("Error loading messages:", e);
  }
}

function renderMessages(elementId, messages, type) {
  const tbody = document.getElementById(elementId);
  if (!tbody) return;

  if (messages.length === 0) {
    const colspan = type === "outbox" ? 6 : 5;
    tbody.innerHTML = `<tr><td colspan="${colspan}" style="text-align:center; color: var(--text-muted); padding: 32px 0;">Không có tin nhắn</td></tr>`;
    return;
  }

  tbody.innerHTML = messages
    .map((msg) => {
      const date = new Date(msg.createdAt).toLocaleString("vi-VN");
      let actions = "";
      let phone = msg.phoneNumber || "Unknown";

      if (type === "inbox") {
        actions = `
            <button class="btn btn-sm btn-outline" onclick="markAsRead(${msg.id})" ${msg.isRead ? "disabled" : ""}>
                ${msg.isRead ? "Đã đọc" : "Đánh dấu đã đọc"}
            </button>
        `;
      } else if (type === "outbox") {
        actions = `<button class="btn btn-sm btn-primary" onclick="resendSms(${msg.id})">Gửi lại</button>`;
      }

      const fw = type === "inbox" && !msg.isRead ? "700" : "400";
      const color = type === "inbox" && !msg.isRead ? "var(--text-primary)" : "var(--text-secondary)";

      let errorCol = "";
      if (type === "outbox") {
        errorCol = `<td style="color:var(--danger)" title="${msg.errorMessage || ""}">${truncate(msg.errorMessage || "", 20)}</td>`;
      }

      return `
        <tr data-sms-id="${msg.id || ""}" style="font-weight: ${fw}; color: ${color};">
            <td><strong>${phone}</strong></td>
            <td title="${msg.content}">${truncate(msg.content, 60)}</td>
            <td>${date}</td>
            <td><span style="color: var(--primary-light); font-weight: 500;">${msg.comPort}</span></td>
            ${type === "sent" ? '<td><span style="color:var(--success)">✓ Đã gửi</span></td>' : ""}
            ${errorCol}
            ${actions ? `<td>${actions}</td>` : (type==="inbox"?"<td></td>":"")}
        </tr>
      `;
    })
    .join("");
}

function buildInboxRow(msg) {
  const date = msg.createdAt ? new Date(msg.createdAt).toLocaleString("vi-VN") : new Date().toLocaleString("vi-VN");
  const phone = msg.phoneNumber || msg.sender || "Unknown";
  const content = msg.content || "";
  const comPort = msg.comPort || "";
  const isRead = !!msg.isRead;
  const fw = !isRead ? "700" : "400";
  const color = !isRead ? "var(--text-primary)" : "var(--text-secondary)";

  return `
    <tr data-sms-id="${msg.id || ""}" style="font-weight: ${fw}; color: ${color};">
        <td><strong>${phone}</strong></td>
        <td title="${content}">${truncate(content, 60)}</td>
        <td>${date}</td>
        <td><span style="color: var(--primary-light); font-weight: 500;">${comPort}</span></td>
        <td>
          <button class="btn btn-sm btn-outline" onclick="markAsRead(${msg.id})" ${isRead || !msg.id ? "disabled" : ""}>
              ${isRead ? "Đã đọc" : "Đánh dấu đã đọc"}
          </button>
        </td>
    </tr>
  `;
}

function prependRealtimeInboxMessages(payload) {
  const tbody = document.getElementById("inbox-list");
  if (!tbody) return;

  const messages = normalizeSmsRealtimePayload(payload)
    .filter((msg) => msg && (msg.type || "INBOX") === "INBOX");

  if (!messages.length) return;

  let added = 0;
  messages.slice().reverse().forEach((msg) => {
    const phone = msg.phoneNumber || msg.sender || "";
    if (smsSearchPhone && !phone.toLowerCase().includes(smsSearchPhone.toLowerCase())) {
      return;
    }

    if (msg.id && tbody.querySelector(`tr[data-sms-id="${msg.id}"]`)) {
      return;
    }

    const hasEmptyRow = tbody.children.length === 1 && tbody.textContent.includes("Không có tin nhắn");
    if (hasEmptyRow) {
      tbody.innerHTML = "";
    }

    tbody.insertAdjacentHTML("afterbegin", buildInboxRow(msg));
    added++;
  });

  if (added > 0) {
    const inboxCount = document.getElementById("inbox-count");
    if (inboxCount) {
      const current = parseInt(inboxCount.textContent || "0", 10) || 0;
      inboxCount.textContent = current + added;
    }
  }
}

function openComposeModal() {
  document.getElementById("compose-modal").classList.add("active");
  updateComPortSelects(simList);
}

function closeModal() {
  document
    .querySelectorAll(".modal")
    .forEach((m) => m.classList.remove("active"));
}

async function sendSms(e) {
  let isModal = false;
  if (e && e.target) {
    const btn = e.target.closest('button');
    if (btn && btn.id === 'send-modal-sms-btn') {
      isModal = true;
    }
  }

  const prefix = isModal ? "sms-modal-" : "sms-";
  const comPort = document.getElementById(prefix + "com-port").value;
  const phoneNumber = document.getElementById(prefix + "phone").value;
  const content = document.getElementById(prefix + "content").value;

  if (!comPort || !phoneNumber || !content) {
    showToast("error", "Vui lòng điền đầy đủ thông tin");
    return;
  }

  showLoading("Đang gửi tin nhắn...");

  try {
    const response = await fetch(`${API_BASE}/sms/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ comPort, phoneNumber, content }),
    });

    const result = await response.json();

    if (result.success) {
      showToast("success", "Đã gửi tin nhắn!");
      document.getElementById(prefix + "phone").value = "";
      document.getElementById(prefix + "content").value = "";
      
      // Auto switch to outbox/sent tab if needed or just load
      loadMessages();
    } else {
      throw new Error(result.error);
    }
  } catch (e) {
    showToast("error", "Lỗi gửi tin nhắn: " + e.message);
  } finally {
    hideLoading();
  }
}

async function readSmsFromModem() {
  const comPort = (document.getElementById("sms-com-port") ? document.getElementById("sms-com-port").value : undefined);
  if (!comPort) {
    showToast("error", "Vui lòng chọn COM Port ở phần Soạn tin nhắn để đọc");
    return;
  }

  showLoading("Đang đọc SMS từ modem...");

  try {
    const response = await fetch(`${API_BASE}/sms/read/${comPort}`);
    const result = await response.json();

    if (result.success) {
      showToast("success", `Đọc được ${(result.data ? result.data.length : undefined) || 0} tin nhắn`);
      loadMessages();
    } else {
      throw new Error(result.error);
    }
  } catch (e) {
    showToast("error", "Lỗi đọc SMS: " + e.message);
  } finally {
    hideLoading();
  }
}

async function markAsRead(id) {
  try {
    await fetch(`${API_BASE}/sms/${id}/read`, { method: "PUT" });
    loadMessages();
    loadDashboard();
  } catch (e) {
    console.error("Error marking as read:", e);
  }
}

async function resendSms(id) {
  showLoading("Đang gửi lại...");
  try {
    const response = await fetch(`${API_BASE}/sms/${id}/resend`, {
      method: "POST",
    });
    const result = await response.json();
    if (result.success) {
      showToast("success", "Đã gửi lại!");
      loadMessages();
    } else {
      throw new Error(result.error);
    }
  } catch (e) {
    showToast("error", "Lỗi: " + e.message);
  } finally {
    hideLoading();
  }
}

// ===== Calls =====
async function makeCall() {
  const comPort = document.getElementById("call-com-port").value;
  const simPhone = document.getElementById("call-sim-phone").value;
  const targetPhone = document.getElementById("call-target").value;
  const record = document.getElementById("call-record").checked;

  if (!comPort || !targetPhone) {
    showToast("error", "Vui lòng điền đầy đủ thông tin");
    return;
  }

  // Get call duration from dropdown
  const callDurationSelect = document.getElementById("call-duration");
  const callDuration = parseInt((callDurationSelect ? callDurationSelect.value : undefined) || "20", 10);

  showLoading("Đang thực hiện cuộc gọi...");

  try {
    const response = await fetch(`${API_BASE}/call/make`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        comPort,
        simPhone,
        targetPhone,
        callDuration,
        record,
      }),
    });

    const result = await response.json();

    if (result.success) {
      showToast("success", `Đang gọi... Tự động tắt sau ${callDuration} giây`);
      loadCallHistory();
    } else {
      throw new Error(result.error);
    }
  } catch (e) {
    showToast("error", "Lỗi: " + e.message);
  } finally {
    hideLoading();
  }
}

async function loadCallHistory() {
  try {
    // Build query params with optional phone search and pagination
    const params = new URLSearchParams({
      page: callCurrentPage,
      size: 20
    });

    if (callSearchPhone) {
      params.append('phoneNumber', callSearchPhone);
    }

    const response = await fetch(`${API_BASE}/call/history?${params}`);
    const result = await response.json();

    const tbody = document.getElementById("call-tbody");
    if (!tbody) return;

    const calls = (result.data ? result.data.content : undefined) || [];
    callTotalPages = (result.data ? result.data.totalPages : undefined) || 1;

    // Update pagination UI
    updateCallPagination();

    if (calls.length === 0) {
      tbody.innerHTML = `
          <tr>
            <td colspan="6">
              <div class="empty-state" style="padding: 60px 0;">
                <div style="width: 80px; height: 80px; background: var(--bg-dark); border-radius: 50%; display: flex; justify-content: center; align-items: center; margin: 0 auto 20px;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="var(--primary-light)" stroke-width="1.5" style="width: 40px; height: 40px;">
                      <path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72" />
                    </svg>
                </div>
                <h3>${callSearchPhone ? 'Không tìm thấy cuộc gọi' : 'Chưa có cuộc gọi nào'}</h3>
                <p style="color: var(--text-muted); font-size: 0.95rem; margin-top: 8px;">Dữ liệu lịch sử các cuộc gọi sẽ xuất hiện ở đây.</p>
              </div>
            </td>
          </tr>`;
      return;
    }

    tbody.innerHTML = calls
      .map((call) => {
        const date = call.createdAt
          ? new Date(call.createdAt).toLocaleString("vi-VN")
          : "";

        const isSuccess = call.status === "COMPLETED";
        const isFailed = call.status === "FAILED";
        const isCalling = call.status === "CALLING";
        const isMissed = call.status === "MISSED";

        let statusColor = "var(--text-secondary)";
        if (isSuccess) statusColor = "var(--success)";
        else if (isFailed) statusColor = "var(--danger)";
        else if (isCalling) statusColor = "var(--warning)";
        else if (isMissed) statusColor = "var(--danger)";

        const statusBadge = `<span style="color: ${statusColor}; font-weight: 500;">
          ${isSuccess ? '✅ Thành công' : isFailed ? '❌ Thất bại' : isCalling ? '📞 Đang gọi...' : isMissed ? '📵 Nhỡ' : call.status || "N/A"}
        </span>`;

        // Duration
        const duration = call.durationSeconds || call.actualDuration || call.callDuration || 0;
        const durationText = duration > 0 ? formatDuration(duration) : "--";

        // Recording file name from path
        const recordingFileName = call.recordingPath
          ? call.recordingPath.split(/[/\\]/).pop()
          : null;

        // Create action buttons based on recording availability
        let actionButtons = "";
        if (recordingFileName) {
          actionButtons = `
            <div style="display: flex; gap: 8px;">
              <button class="btn btn-sm btn-primary" onclick="playRecordingFromCall('${recordingFileName}')" title="Nghe ghi âm" style="display: flex; align-items: center; gap: 4px; border-radius: 6px;">
                <svg viewBox="0 0 24 24" fill="currentColor" style="width: 14px; height: 14px;"><path d="M8 5v14l11-7z"/></svg> Nghe
              </button>
              <button class="btn btn-sm btn-outline" onclick="openRecordingFolderByFile('${recordingFileName}')" title="Mở thư mục" style="border-radius: 6px;">📁</button>
              <button class="btn btn-sm btn-outline" onclick="downloadRecording('${recordingFileName}')" title="Tải xuống" style="border-radius: 6px;">⬇</button>
            </div>
          `;
        } else {
            actionButtons = `<span style="color: var(--text-muted); font-size: 0.85rem;">Không có</span>`;
        }

        return `
            <tr data-id="${call.id}">
                <td><span style="color: var(--primary-light); font-weight: 500;">${call.simPhone || call.comPort || ""}</span></td>
                <td><strong style="color: var(--text-primary);">${call.toNumber || call.targetPhone || "N/A"}</strong></td>
                <td>${durationText}</td>
                <td>${statusBadge}</td>
                <td style="color: var(--text-secondary);">${date}</td>
                <td>${actionButtons}</td>
            </tr>
        `;
      })
      .join("");
  } catch (e) {
    console.error("Error loading call history:", e);
  }
}

// Update call pagination UI
function updateCallPagination() {
  const prevBtn = document.getElementById("call-prev-page");
  const nextBtn = document.getElementById("call-next-page");
  const pageInfo = document.getElementById("call-page-info");

  if (prevBtn) prevBtn.disabled = callCurrentPage <= 0;
  if (nextBtn) nextBtn.disabled = callCurrentPage >= callTotalPages - 1;
  if (pageInfo) pageInfo.textContent = `Trang ${callCurrentPage + 1}/${callTotalPages}`;
}

// Format duration in mm:ss
function formatDuration(seconds) {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}:${secs.toString().padStart(2, "0")}`;
}

// Open folder containing recording file
async function openRecordingFolderByFile(fileName) {
  try {
    await fetch(
      `${API_BASE}/recording/open-folder/${encodeURIComponent(fileName)}`
    );
  } catch (e) {
    console.error("Error opening folder:", e);
  }
}

// ===== Recordings =====
async function loadRecordings() {
  try {
    const [recordings, config] = await Promise.all([
      fetch(`${API_BASE}/recording/list`).then((r) => r.json()),
      fetch(`${API_BASE}/recording/config`).then((r) => r.json()),
    ]);

    document.getElementById("recording-path").textContent =
      config.data || "./recordings";

    const container = document.getElementById("recordings-grid");
    const files = recordings.data || [];

    if (files.length === 0) {
      container.innerHTML = `
                <div class="empty-state">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                        <path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3z"/>
                        <path d="M19 10v2a7 7 0 0 1-14 0v-2"/>
                    </svg>
                    <p>Chưa có file ghi âm nào</p>
                </div>`;
      return;
    }

    container.innerHTML = files
      .map((file) => {
        const size = formatFileSize(file.size);
        const date = new Date(file.modified).toLocaleString("vi-VN");

        return `
                <div class="recording-card">
                    <div class="recording-info">
                        <div class="recording-name">${file.name}</div>
                        <div class="recording-meta">${size} • ${date}</div>
                    </div>
                    <div class="recording-actions">
                        <button class="btn btn-sm btn-primary" onclick="playRecording('${file.name}')">
                            ▶ Nghe
                        </button>
                        <button class="btn btn-sm btn-outline" onclick="openRecordingFile('${file.name}')">
                            📂 Open
                        </button>
                        <button class="btn btn-sm btn-outline" onclick="downloadRecording('${file.name}')">
                            ⬇ Tải
                        </button>
                    </div>
                </div>
            `;
      })
      .join("");

    document.getElementById("stat-recordings").textContent = files.length;
  } catch (e) {
    console.error("Error loading recordings:", e);
  }
}

// Current audio state
let currentAudioFileName = null;

// Play recording from call history
function playRecordingFromCall(fileName) {
  playRecording(fileName);
}

function playRecording(fileName) {
  const player = document.getElementById("audio-player");
  const audioElement = document.getElementById("audio-element");
  const audioTitle = document.getElementById("audio-title");
  const audioDuration = document.getElementById("audio-duration");
  const progressBar = document.getElementById("audio-progress-bar");
  const playBtn = document.getElementById("audio-play");

  // Store current file name
  currentAudioFileName = fileName;

  // Set audio source
  audioElement.src = `${API_BASE}/recording/download/${encodeURIComponent(fileName)}`;
  audioTitle.textContent = fileName;
  player.style.display = "block";

  // Play audio
  audioElement.play().catch(e => {
    console.error("Error playing audio:", e);
    showToast("error", "Không thể phát audio: " + e.message);
  });

  // Update play button icon
  updatePlayButtonIcon(true);

  // Audio events
  audioElement.onplay = () => updatePlayButtonIcon(true);
  audioElement.onpause = () => updatePlayButtonIcon(false);
  audioElement.onended = () => {
    updatePlayButtonIcon(false);
    progressBar.value = 0;
  };

  audioElement.ontimeupdate = () => {
    if (audioElement.duration) {
      const percent = (audioElement.currentTime / audioElement.duration) * 100;
      progressBar.value = percent;
      audioDuration.textContent = `${formatAudioTime(audioElement.currentTime)} / ${formatAudioTime(audioElement.duration)}`;
    }
  };

  audioElement.onloadedmetadata = () => {
    audioDuration.textContent = `00:00 / ${formatAudioTime(audioElement.duration)}`;
  };

  // Progress bar seek
  progressBar.oninput = (e) => {
    if (audioElement.duration) {
      const percent = e.target.value;
      audioElement.currentTime = (percent / 100) * audioElement.duration;
    }
  };

  // Play/pause button
  playBtn.onclick = () => {
    if (audioElement.paused) {
      audioElement.play();
    } else {
      audioElement.pause();
    }
  };

  // Download button
  document.getElementById("audio-download").onclick = () => {
    downloadRecording(currentAudioFileName);
  };

  // Open folder button
  document.getElementById("audio-folder").onclick = () => {
    openRecordingFolderByFile(currentAudioFileName);
  };

  // Close button
  document.getElementById("audio-close").onclick = () => {
    closeAudioPlayer();
  };
}

function updatePlayButtonIcon(isPlaying) {
  const playIcon = document.getElementById("play-icon");
  if (playIcon) {
    if (isPlaying) {
      // Pause icon
      playIcon.innerHTML = '<rect x="6" y="4" width="4" height="16" fill="currentColor"/><rect x="14" y="4" width="4" height="16" fill="currentColor"/>';
    } else {
      // Play icon
      playIcon.innerHTML = '<path d="M8 5v14l11-7z"/>';
    }
  }
}

function formatAudioTime(seconds) {
  if (!seconds || isNaN(seconds)) return "00:00";
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
}

async function openRecordingFile(fileName) {
  try {
    await fetch(`${API_BASE}/recording/open/${fileName}`);
    showToast("success", "Đã mở file");
  } catch (e) {
    showToast("error", "Không thể mở file");
  }
}

function downloadRecording(fileName) {
  // Create a temporary anchor element to trigger download
  const a = document.createElement('a');
  a.href = `${API_BASE}/recording/download/${encodeURIComponent(fileName)}`;
  a.download = fileName; // This attribute triggers download instead of navigation
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  showToast("success", `Đang tải xuống ${fileName}...`);
}

function closeAudioPlayer() {
  const player = document.getElementById("audio-player");
  const audioElement = document.getElementById("audio-element");

  player.style.display = "none";
  if (audioElement) {
    audioElement.pause();
    audioElement.src = "";
  }
  currentAudioFileName = null;
}

async function openRecordingFolder() {
  try {
    const response = await fetch(`${API_BASE}/recording/open-folder`);
    const result = await response.json();
    if (result.success) {
      showToast("success", "Đã mở thư mục ghi âm");
    } else {
      throw new Error(result.error || "Không thể mở thư mục");
    }
  } catch (e) {
    console.error("Error opening recording folder:", e);
    showToast("error", "Không thể mở thư mục: " + e.message);
  }
}


const callDurations = new Map();

function startCallTimer(callId, startTime) {
  if (callDurations.has(callId)) return;
  if (!startTime) startTime = Date.now();

  const start = new Date(startTime).getTime();

  // Initial update
  updateTimerDisplay(callId, start);

  const intervalId = setInterval(() => {
    updateTimerDisplay(callId, start);
  }, 1000);

  callDurations.set(callId, intervalId);
}

function updateTimerDisplay(callId, start) {
  const now = Date.now();
  const seconds = Math.floor((now - start) / 1000);
  if (seconds < 0) return;

  // Try finding by ID or OrderID
  let callItem = document.querySelector(`.call-item[data-id="${callId}"]`);
  if (!callItem) {
    callItem = document.querySelector(`.call-item[data-order-id="${callId}"]`);
  }

  if (callItem) {
    const durationDiv = callItem.querySelector(".call-duration");
    if (durationDiv) {
      durationDiv.textContent = formatDuration(seconds);
    }
  }
}

function stopCallTimer(callId) {
  if (callDurations.has(callId)) {
    clearInterval(callDurations.get(callId));
    callDurations.delete(callId);
  }
}

function handleSimUpdate(sims) {
  // 🔧 FIX: Không reset simList nếu nhận được array rỗng (tín hiệu bắt đầu scan)
  if (!sims || sims.length === 0) {
    console.log("📡 Received empty sims array - ignoring (scan starting)");
    return; // Giữ nguyên simList hiện tại
  }

  // 🔧 FIX: Chỉ giữ SIM có CCID (có thông tin thực sự)
  const validSims = sims.filter(
    (sim) => sim.iccid && sim.iccid !== "N/A" && sim.iccid !== ""
  );

  if (!validSims.length) {
    return;
  }

  // 🔧 FIX: Merge strategy - always merge to prevent race condition
  // This works for both single SIM updates and full array replacements
  const newSimList = [...simList];
  let hasChanges = false;

  validSims.forEach((sim) => {
    const existingIndex = newSimList.findIndex(
      (s) => s.comPort === sim.comPort || (s.iccid && s.iccid === sim.iccid)
    );

    if (existingIndex >= 0) {
      // Update existing SIM - preserve phone number if new one doesn't have it
      const existing = newSimList[existingIndex];
      if (!sim.phoneNumber && existing.phoneNumber) {
        sim.phoneNumber = existing.phoneNumber;
      }
      // Only update if something changed
      if (JSON.stringify(existing) !== JSON.stringify(sim)) {
        newSimList[existingIndex] = sim;
        hasChanges = true;
      }
    } else {
      // Add new SIM
      newSimList.push(sim);
      hasChanges = true;
    }
  });

    // Only update UI if something changed
    if (hasChanges) {
      // Sort by comPort
      newSimList.sort((a, b) => {
        const portA = parseInt(a.comPort.replace(/\D/g, "")) || 0;
        const portB = parseInt(b.comPort.replace(/\D/g, "")) || 0;
        return portA - portB;
      });
  
      simList = newSimList;
      renderSimList(simList);
      updateComPortSelects(simList);
  
      // Update dashboard stats
      const activeSims = simList.filter((s) => s.status === "ONLINE").length;
      const inactiveSims = simList.filter((s) => s.status === "INACTIVE").length;
      const statSims = document.getElementById("stat-sims");
      const statSimsInactive = document.getElementById("stat-sims-inactive");
      if (statSims) statSims.textContent = activeSims;
      if (statSimsInactive) statSimsInactive.textContent = inactiveSims;
  
      // [NEW] Update scan progress realtime if scanning is in progress
      if (typeof scanningInProgress !== 'undefined' && scanningInProgress) {
        let maxSimsExpected = 32; // Assume standard equipment max is 32/64
        let perc = Math.min((simList.length / maxSimsExpected) * 100, 95);
        updateScanStatus(perc, `Đang xử lý dữ liệu... Đã tìm thấy ${simList.length} SIM ⚡`);
      }
      
      // Update dashboard quietly to reflect new metrics realtime
      if (currentPage === 'dashboard') {
        loadDashboard();
      }
  
      console.log(`📡 SIM list updated: ${simList.length} total (${activeSims} active, ${inactiveSims} inactive)`);
    }
}

function showBrowserNotification(title, options) {
  if (!("Notification" in window)) return;
  if (Notification.permission === "granted") {
    new Notification(title, options);
  } else if (Notification.permission !== "denied") {
    Notification.requestPermission().then(permission => {
      if (permission === "granted") {
        new Notification(title, options);
      }
    });
  }
}

function normalizeSmsRealtimePayload(payload) {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload.sms)) return payload.sms;
  if (payload.sms) return [payload.sms];
  if (Array.isArray(payload.data)) return payload.data;
  return [payload];
}

function getSmsRealtimeKey(sms) {
  if (!sms) return null;
  if (sms.id) return `id:${sms.id}`;
  return [
    sms.comPort || "",
    sms.phoneNumber || sms.sender || "",
    sms.content || "",
    sms.createdAt || sms.timestamp || ""
  ].join("|");
}

function shouldShowSmsNotification(sms) {
  const now = Date.now();
  for (const [key, ts] of recentSmsRealtimeEvents.entries()) {
    if (now - ts > SMS_REALTIME_EVENT_TTL_MS) {
      recentSmsRealtimeEvents.delete(key);
    }
  }

  const key = getSmsRealtimeKey(sms);
  if (!key) return true;
  if (recentSmsRealtimeEvents.has(key)) return false;
  recentSmsRealtimeEvents.set(key, now);
  return true;
}

function refreshSmsViewsFromRealtime(payload) {
  prependRealtimeInboxMessages(payload);

  const messages = normalizeSmsRealtimePayload(payload);
  const messageWithUnreadCount = messages.find((sms) => typeof sms.unreadCount === "number");
  const unreadCount = payload && typeof payload.unreadCount === "number"
    ? payload.unreadCount
    : (messageWithUnreadCount ? messageWithUnreadCount.unreadCount : undefined);

  if (typeof unreadCount === "number") {
    updateUnreadBadge(unreadCount);
  } else {
    loadUnreadCount();
  }

  if (smsRealtimeRefreshTimer) {
    clearTimeout(smsRealtimeRefreshTimer);
  }

  smsRealtimeRefreshTimer = setTimeout(() => {
    smsRealtimeRefreshTimer = null;
    loadMessages();
    loadDashboard();
  }, 150);
}

function handleNewSms(payload) {
  const messages = normalizeSmsRealtimePayload(payload);
  const firstSms = messages[0] || {};

  if (shouldShowSmsNotification(firstSms)) {
    const sender = firstSms.phoneNumber || firstSms.sender || "N/A";
    const content = firstSms.content || "";
    const suffix = messages.length > 1 ? ` (+${messages.length - 1})` : "";
    showToast("info", `Tin nhắn mới từ ${sender}${suffix}`);
    showBrowserNotification("Tin nhắn mới", {
      body: `Từ: ${sender}\n${content}`,
      icon: "/images/logo.png"
    });
    playNotificationSound();
  }

  refreshSmsViewsFromRealtime(payload);
}

function handleSmsInboxUpdate(payload) {
  refreshSmsViewsFromRealtime(payload);
}

function handleSmsStatus(sms) {
  if (sms.status === "FAILED") {
    showToast("error", `Gửi tin nhắn thất bại: ${sms.errorMessage || ""}`);
  }
  loadMessages();
}

function handleCallStatus(call) {
  // Update active call status in real-time
  const callId = call.id || call.orderId;
  const status = call.status;
  const duration = call.duration || 0;
  const message = call.message || "";
  const comPort = call.comPort || "";

  // Show toast for important status changes
  switch (status) {
    case "DIALING":
      showToast("info", `📞 ${comPort}: Đang quay số...`);
      break;
    case "RINGING":
      // Don't spam toasts for ringing
      break;
    case "IN_CALL":
    case "CONNECTED":
      if (duration <= 1) {
        showToast("success", `📞 ${comPort}: Đã kết nối!`);
      }
      break;
    case "RECORDING_START":
      showToast("info", `🎙️ ${comPort}: Đang bật ghi âm...`);
      break;
    case "ENDING":
      showToast("info", `📞 ${comPort}: Đang kết thúc cuộc gọi...`);
      break;
    case "RECORDING_DOWNLOAD":
      // Show progress in status bar instead of toast
      updateRecordingProgress(comPort, duration, message);
      break;
    case "RECORDING_SAVE":
      showToast("info", `💾 ${comPort}: Đang lưu file ghi âm...`);
      break;
    case "RECORDING_CLEANUP":
      // Silent
      break;
    case "RECORDING_COMPLETE":
      showToast("success", `✅ ${comPort}: Hoàn tất ghi âm!`);
      hideRecordingProgress(comPort);
      break;
    case "RECORDING_FAILED":
      showToast("error", `❌ ${comPort}: Lỗi ghi âm - ${message}`);
      hideRecordingProgress(comPort);
      break;
    case "COMPLETED":
    case "ENDED_SUCCESS":
    case "ENDED_EARLY":
      showToast("success", `✅ ${comPort}: Cuộc gọi hoàn tất (${duration}s)`);
      hideRecordingProgress(comPort);
      break;
    case "FAILED":
      showToast("error", `❌ ${comPort}: ${message || "Cuộc gọi thất bại"}`);
      hideRecordingProgress(comPort);
      break;
    case "MISSED":
      showToast("warning", `📵 ${comPort}: Không nhấc máy`);
      break;
  }

  // Update call history list inline (no popup)
  loadCallHistory().then(() => {
    // Handle duration timer
    if (status === "CONNECTED" || status === "IN_CALL") {
      const startTime = call.startTime || Date.now();
      startCallTimer(callId, startTime);
    } else if (["COMPLETED", "FAILED", "MISSED", "ENDED_SUCCESS", "ENDED_EARLY"].includes(status)) {
      stopCallTimer(callId);
    }

    // Update specific item UI if needed (backup for loadCallHistory delay)
    let callItem = document.querySelector(`.call-item[data-id="${callId}"]`);
    if (!callItem)
      callItem = document.querySelector(
        `.call-item[data-order-id="${callId}"]`
      );

    if (callItem) {
      // Update status badge with more states
      const statusDiv = callItem.querySelector(".call-status");
      if (statusDiv) {
        let badge;
        switch (status) {
          case "DIALING":
            badge =
              '<span class="status-badge calling">📞 Đang quay số...</span>';
            break;
          case "RINGING":
            badge = `<span class="status-badge calling">📞 Đang đổ chuông (${duration}s)</span>`;
            break;
          case "IN_CALL":
          case "CONNECTED":
            badge = `<span class="status-badge calling">🟢 Đang gọi ${formatDuration(
              duration
            )}</span>`;
            break;
          case "RECORDING_DOWNLOAD":
            badge = `<span class="status-badge recording">📥 Đang tải ${duration}%</span>`;
            break;
          case "RECORDING_SAVE":
            badge =
              '<span class="status-badge recording">💾 Đang lưu...</span>';
            break;
          case "COMPLETED":
          case "ENDED_SUCCESS":
          case "ENDED_EARLY":
            badge = '<span class="status-badge success">✅ Thành công</span>';
            break;
          case "FAILED":
            badge = '<span class="status-badge failed">❌ Thất bại</span>';
            break;
          case "MISSED":
            badge = '<span class="status-badge missed">📵 Nhỡ</span>';
            break;
          default:
            badge = `<span class="status-badge">${status}</span>`;
        }
        statusDiv.innerHTML = badge;
      }

      // Update status class
      callItem.dataset.status = status;
      const iconDiv = callItem.querySelector(".call-icon");
      if (iconDiv) {
        iconDiv.className =
          "call-icon " +
          (["COMPLETED", "ENDED_SUCCESS", "ENDED_EARLY"].includes(status)
            ? "outgoing"
            : status === "FAILED"
              ? "missed"
              : [
                "CALLING",
                "RINGING",
                "CONNECTED",
                "IN_CALL",
                "RECORDING_DOWNLOAD",
              ].includes(status)
                ? "calling"
                : "incoming");
      }
    }
  });

  console.log(`📞 Call status: ${status}`, call);
}

/**
 * 🆕 Handle incoming call status updates
 * Xử lý các trạng thái cuộc gọi đến: WAITING_CALL, RINGING, ANSWERED, RECORDING, COMPLETED, FAILED, TIMEOUT
 */
function handleIncomingCallStatus(incomingCall) {
  const status = incomingCall.status;
  const comPort = incomingCall.comPort || "";
  const callerNumber = incomingCall.callerNumber || "Không xác định";
  const orderId = incomingCall.orderId;
  const simPhone = incomingCall.simPhone || "";

  console.log(`📞 Incoming call status: ${status}`, incomingCall);

  // Show toast based on status
  switch (status) {
    case "WAITING_CALL":
      showToast("info", `📞 ${comPort}: Đang đợi cuộc gọi đến từ ${callerNumber}...`);
      break;
    case "RINGING":
      showToast("info", `📞 ${comPort}: Cuộc gọi đến từ ${callerNumber}!`);
      playNotificationSound();
      break;
    case "ANSWERED":
    case "IN_CALL":
      showToast("success", `📞 ${comPort}: Đã nhấc máy cuộc gọi từ ${callerNumber}`);
      break;
    case "RECORDING_START":
      showToast("info", `🎙️ ${comPort}: Đang bắt đầu ghi âm...`);
      break;
    case "RECORDING":
      showToast("info", `🎙️ ${comPort}: Đang ghi âm cuộc gọi từ ${callerNumber}...`);
      break;
    case "RECORDING_COMPLETE":
      showToast("success", `✅ ${comPort}: Hoàn tất ghi âm cuộc gọi đến!`);
      break;
    case "COMPLETED":
    case "ENDED_SUCCESS":
      showToast("success", `✅ ${comPort}: Cuộc gọi đến từ ${callerNumber} đã hoàn tất`);
      break;
    case "TIMEOUT":
      showToast("warning", `⏰ ${comPort}: Hết thời gian đợi cuộc gọi đến`);
      break;
    case "NO_CALL":
      showToast("warning", `📵 ${comPort}: Không có cuộc gọi đến`);
      break;
    case "REJECTED":
      showToast("warning", `🚫 ${comPort}: Cuộc gọi từ ${callerNumber} đã bị từ chối (không khớp số)`);
      break;
    case "FAILED":
      const message = incomingCall.message || "Lỗi không xác định";
      showToast("error", `❌ ${comPort}: ${message}`);
      break;
    default:
      console.log(`📞 Unknown incoming call status: ${status}`);
  }

  // Reload call history to show updated status
  loadCallHistory();

  // Update any UI elements if needed
  const callItem = document.querySelector(`.call-item[data-order-id="${orderId}"]`);
  if (callItem) {
    const statusDiv = callItem.querySelector(".call-status");
    if (statusDiv) {
      let badge;
      switch (status) {
        case "WAITING_CALL":
          badge = '<span class="status-badge waiting">⏳ Đang đợi...</span>';
          break;
        case "RINGING":
          badge = '<span class="status-badge calling">📞 Đang đổ chuông</span>';
          break;
        case "ANSWERED":
        case "IN_CALL":
          badge = '<span class="status-badge calling">🟢 Đang gọi</span>';
          break;
        case "RECORDING":
          badge = '<span class="status-badge recording">🎙️ Đang ghi âm</span>';
          break;
        case "COMPLETED":
        case "ENDED_SUCCESS":
          badge = '<span class="status-badge success">✅ Hoàn tất</span>';
          break;
        case "FAILED":
        case "TIMEOUT":
          badge = '<span class="status-badge failed">❌ Thất bại</span>';
          break;
        default:
          badge = `<span class="status-badge">${status}</span>`;
      }
      statusDiv.innerHTML = badge;
    }
  }
}

// Recording progress UI
let recordingProgressElements = new Map();

function updateRecordingProgress(comPort, percent, message) {
  // Find or create progress indicator
  let progressEl = recordingProgressElements.get(comPort);

  if (!progressEl) {
    progressEl = document.createElement("div");
    progressEl.className = "recording-progress-toast";
    progressEl.innerHTML = `
            <div class="progress-content">
                <span class="progress-label">📥 ${comPort}: Đang tải ghi âm...</span>
                <div class="progress-bar-container">
                    <div class="progress-bar-fill" style="width: 0%"></div>
                </div>
                <span class="progress-percent">0%</span>
            </div>
        `;
    document.getElementById("toast-container").prepend(progressEl);
    recordingProgressElements.set(comPort, progressEl);
  }

  // Update progress
  const fill = progressEl.querySelector(".progress-bar-fill");
  const percentText = progressEl.querySelector(".progress-percent");
  if (fill) fill.style.width = percent + "%";
  if (percentText) percentText.textContent = percent + "%";
}

function hideRecordingProgress(comPort) {
  const progressEl = recordingProgressElements.get(comPort);
  if (progressEl) {
    progressEl.remove();
    recordingProgressElements.delete(comPort);
  }
}

// ===== Utilities =====
function showLoading(text = "Đang tải...") {
  document.getElementById("loading-text").textContent = text;
  document.getElementById("loading-overlay").classList.add("active");
}

function hideLoading() {
  document.getElementById("loading-overlay").classList.remove("active");
}

function showFullMessage(content) {
  const modal = document.getElementById("message-detail-modal");
  const contentEl = document.getElementById("full-message-content");
  if (modal && contentEl) {
    contentEl.textContent = content;
    modal.classList.add("active");
  }
}

function showToast(type, message) {
  const container = document.getElementById("toast-container");
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.innerHTML = `
        <span class="toast-message">${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">×</button>
    `;
  container.appendChild(toast);

  setTimeout(() => toast.remove(), 5000);
}

function updateUnreadBadge(count) {
  const badge = document.getElementById("unread-badge");
  const notifCount = document.getElementById("notification-count");

  badge.textContent = count > 0 ? count : "";
  notifCount.textContent = count > 0 ? count : "";
}

function truncate(str, len) {
  if (!str) return "";
  return str.length > len ? str.substring(0, len) + "..." : str;
}

function formatFileSize(bytes) {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

function playNotificationSound() {
  try {
    const audio = document.getElementById("notification-sound");
    if (audio) audio.play();
  } catch (e) { }
}

// ✅ Load SIM lỗi kết nối vào bảng "SIM Lỗi Kết Nối"
function loadErrorSims() {
    const tbody = document.getElementById('error-sims-tbody');
    if (!tbody) return;

    const errorStatuses = ['INACTIVE', 'ERROR', 'COM_ERROR', 'SIM_ERROR', 'NO_SIM'];
    const errorSims = simList.filter(s => errorStatuses.includes(s.status));

    if (errorSims.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--success); padding: 40px 0;">✅ Tất cả SIM đều hoạt động bình thường!</td></tr>';
        return;
    }

    tbody.innerHTML = errorSims.map(s => {
        const statusMap = {
            'INACTIVE': { label: 'Mất kết nối', color: 'var(--warning)', bg: 'rgba(245,158,11,0.15)' },
            'ERROR': { label: 'Lỗi', color: 'var(--danger)', bg: 'rgba(239,68,68,0.15)' },
            'COM_ERROR': { label: 'Lỗi COM Port', color: 'var(--danger)', bg: 'rgba(239,68,68,0.15)' },
            'SIM_ERROR': { label: 'Lỗi SIM', color: 'var(--danger)', bg: 'rgba(239,68,68,0.15)' },
            'NO_SIM': { label: 'Không có SIM', color: 'var(--text-muted)', bg: 'rgba(156,163,175,0.15)' },
        };
        const st = statusMap[s.status] || { label: s.status, color: 'var(--text-muted)', bg: 'rgba(156,163,175,0.1)' };
        const errorMsg = s.errorMessage || s.lastError || 'Không phản hồi / Mất kết nối';

        return `
            <tr>
                <td><strong style="font-family: monospace;">${s.comPort}</strong></td>
                <td>${s.phoneNumber || '<span style="color:var(--text-muted)">N/A</span>'}</td>
                <td>${s.operator || s.network || '<span style="color:var(--text-muted)">—</span>'}</td>
                <td><span style="background:${st.bg}; color:${st.color}; padding:4px 10px; border-radius:6px; font-weight:600; font-size:0.8rem;">${st.label}</span></td>
                <td style="color:var(--danger); font-size:0.85rem; max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${errorMsg}">${errorMsg}</td>
            </tr>
        `;
    }).join('');
}

async function loadFailStats() {
    try {
        const response = await fetch(`${API_BASE}/stats/fail-stats`);
        const result = await response.json();
        
        const tbody = document.getElementById('fail-stats-tbody');
        if (!tbody) return;

        if (result.success && Object.keys(result.data).length > 0) {
            const data = result.data;
            tbody.innerHTML = Object.keys(data).map(comPort => {
                const stat = data[comPort];
                const isBlacklisted = stat.isBlacklisted;
                const statusBadge = isBlacklisted 
                    ? `<span class="sys-badge" style="background:var(--danger);color:white;padding:4px 8px;border-radius:4px;font-size:0.8rem;">Blacklisted</span>`
                    : `<span class="sys-badge" style="background:var(--success);color:white;padding:4px 8px;border-radius:4px;font-size:0.8rem;">Normal</span>`;
                
                const actionBtn = isBlacklisted 
                    ? `<button class="btn btn-sm btn-outline" style="border-color:var(--success);color:var(--success);" onclick="unblacklistSim('${comPort}')">Gỡ Khóa</button>`
                    : `<span style="color:var(--text-muted);font-size:0.85rem;">-</span>`;

                return `
                    <tr>
                        <td><strong>${comPort}</strong></td>
                        <td style="${stat.consecutiveFailCount > 0 ? 'color:var(--warning);font-weight:600;' : ''}">${stat.consecutiveFailCount}</td>
                        <td>${stat.dailyFailCount}</td>
                        <td>${statusBadge}</td>
                        <td>${actionBtn}</td>
                    </tr>
                `;
            }).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-muted); padding: 40px 0;">Không có dữ liệu lỗi. Tất cả SIM đều bình thường!</td></tr>';
        }
    } catch (e) {
        console.error('Error fetching fail stats:', e);
        showToast('error', 'Không thể tải thống kê lỗi');
    }
}

async function unblacklistSim(comPort) {
    if(!confirm(`Bạn chắc chắn muốn gỡ khóa bộ đếm lỗi cho SIM trên cổng ${comPort}?`)) return;
    try {
        const response = await fetch(`${API_BASE}/sim/${comPort}/unblacklist`, { method: 'POST' });
        const result = await response.json();
        if (result.success) {
            showToast('success', result.message || 'Đã gỡ khóa thành công');
            loadFailStats();
        } else {
            showToast('error', result.error || 'Lỗi khi gỡ khóa');
        }
    } catch (e) {
        showToast('error', 'Không thể thực hiện tác vụ');
    }
}
