/**
 * 🏥 SIM Health Check — Frontend Module
 * 
 * Hiển thị trạng thái sức khỏe SIM realtime.
 * - Fetch từ REST API: /api/sim-health
 * - Nhận push WebSocket: /topic/sim-health, /topic/sim-health/alert
 * - Hỗ trợ filter, force recover, badge notification
 */
(function () {
  'use strict';

  // =========================================================================
  // STATE
  // =========================================================================
  let healthData = [];
  let currentFilter = 'ALL';

  // =========================================================================
  // INIT — Chờ DOM + WebSocket ready
  // =========================================================================
  document.addEventListener('DOMContentLoaded', () => {
    // Refresh button
    const refreshBtn = document.getElementById('health-refresh-btn');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', () => fetchHealthData());
    }

    // Filter buttons
    document.querySelectorAll('[data-health-filter]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('[data-health-filter]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentFilter = btn.getAttribute('data-health-filter');
        renderHealthTable(healthData);
      });
    });

    // Auto-load khi vào trang health
    const observer = new MutationObserver(() => {
      const page = document.getElementById('page-sim-health');
      if (page && page.classList.contains('active')) {
        fetchHealthData();
      }
    });
    const pageContainer = document.querySelector('.page-container');
    if (pageContainer) {
      observer.observe(pageContainer, { subtree: true, attributes: true, attributeFilter: ['class'] });
    }

    // WebSocket subscription (hook vào STOMP client nếu có)
    waitForStomp(() => {
      subscribeHealthTopics();
    });

    // Fetch initial data
    setTimeout(() => fetchHealthData(), 2000);
  });

  // =========================================================================
  // FETCH HEALTH DATA
  // =========================================================================
  function fetchHealthData() {
    fetch('/api/sim-health')
      .then(res => {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(data => {
        // data.results là map { comName -> HealthResult }
        const results = data.results || {};
        healthData = Object.values(results);

        // Sort: dead first, then by comName
        healthData.sort((a, b) => {
          if (a.isDead && !b.isDead) return -1;
          if (!a.isDead && b.isDead) return 1;
          return (a.comName || '').localeCompare(b.comName || '');
        });

        updateStats(healthData);
        renderHealthTable(healthData);
        updateDeadBadge(data.totalDead || 0);
      })
      .catch(err => {
        console.warn('⚠️ [Health] Fetch error:', err.message);
      });
  }

  // =========================================================================
  // WEBSOCKET SUBSCRIPTION
  // =========================================================================
  function waitForStomp(callback) {
    // Poll until window.stompClient is available
    const check = setInterval(() => {
      if (window.stompClient && window.stompClient.connected) {
        clearInterval(check);
        callback();
      }
    }, 1000);

    // Stop checking after 30s
    setTimeout(() => clearInterval(check), 30000);
  }

  function subscribeHealthTopics() {
    try {
      // Periodic health results
      window.stompClient.subscribe('/topic/sim-health', (message) => {
        try {
          const results = JSON.parse(message.body);
          if (Array.isArray(results)) {
            healthData = results;
            healthData.sort((a, b) => {
              if (a.isDead && !b.isDead) return -1;
              if (!a.isDead && b.isDead) return 1;
              return (a.comPort || '').localeCompare(b.comPort || '');
            });
            updateStats(healthData);
            renderHealthTable(healthData);

            const deadCount = healthData.filter(r => r.isDead).length;
            updateDeadBadge(deadCount);
          }
        } catch (e) {
          console.warn('⚠️ [Health WS] Parse error:', e);
        }
      });

      // Dead SIM alerts
      window.stompClient.subscribe('/topic/sim-health/alert', (message) => {
        try {
          const alert = JSON.parse(message.body);
          if (alert.type === 'SIM_DEAD') {
            showDeadSimToast(alert);
            fetchHealthData(); // Refresh full data
          }
        } catch (e) {
          console.warn('⚠️ [Health Alert] Parse error:', e);
        }
      });

      console.log('✅ [Health] WebSocket subscribed');
    } catch (e) {
      console.warn('⚠️ [Health] WebSocket subscription failed:', e);
    }
  }

  // =========================================================================
  // UPDATE STATS
  // =========================================================================
  function updateStats(data) {
    let alive = 0, dead = 0, warning = 0;

    data.forEach(r => {
      // Normalize: WebSocket sends comPort, REST sends comName
      const item = normalizeItem(r);
      if (item.isDead) {
        dead++;
      } else if (item.consecutiveCregFails > 0 || item.consecutiveSignalFails > 0 || item.consecutiveSmsFails > 3) {
        warning++;
      } else {
        alive++;
      }
    });

    const elAlive = document.getElementById('health-alive');
    const elDead = document.getElementById('health-dead');
    const elWarning = document.getElementById('health-warning');
    const elTotal = document.getElementById('health-total');

    if (elAlive) elAlive.textContent = alive;
    if (elDead) elDead.textContent = dead;
    if (elWarning) elWarning.textContent = warning;
    if (elTotal) elTotal.textContent = data.length;
  }

  // =========================================================================
  // RENDER TABLE
  // =========================================================================
  function renderHealthTable(data) {
    const tbody = document.getElementById('health-tbody');
    if (!tbody) return;

    if (!data || data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:60px 0;">Đang chờ dữ liệu health check... (tự động mỗi 3 phút)</td></tr>';
      return;
    }

    // Filter
    let filtered = data;
    if (currentFilter === 'DEAD') {
      filtered = data.filter(r => normalizeItem(r).isDead);
    } else if (currentFilter === 'WARNING') {
      filtered = data.filter(r => {
        const n = normalizeItem(r);
        return !n.isDead && (n.consecutiveCregFails > 0 || n.consecutiveSignalFails > 0 || n.consecutiveSmsFails > 3);
      });
    } else if (currentFilter === 'ALIVE') {
      filtered = data.filter(r => {
        const n = normalizeItem(r);
        return !n.isDead && n.consecutiveCregFails === 0 && n.consecutiveSignalFails === 0;
      });
    }

    if (filtered.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:40px 0;">Không có SIM nào trong filter này</td></tr>';
      return;
    }

    tbody.innerHTML = filtered.map(r => {
      const item = normalizeItem(r);
      return buildRow(item);
    }).join('');
  }

  function normalizeItem(r) {
    return {
      comName: r.comPort || r.comName || 'N/A',
      phoneNumber: r.phoneNumber || 'N/A',
      cregOk: r.cregOk || false,
      cregStatus: r.cregStatus != null ? r.cregStatus : -1,
      signalLevel: r.signalLevel != null ? r.signalLevel : -1,
      cpinOk: r.cpinOk || false,
      cpinStatus: r.cpinStatus || 'UNKNOWN',
      operator: r.operatorName || r.operator || 'N/A',
      isDead: r.isDead || false,
      deadReason: r.deadReason || null,
      consecutiveCregFails: r.consecutiveCregFails || 0,
      consecutiveSignalFails: r.consecutiveSignalFails || 0,
      consecutiveSmsFails: r.consecutiveSmsFails || 0,
      smsSuccessCount: r.smsSuccessCount || 0,
      smsFailedTotal: r.smsFailedTotal || 0,
      smsSentTotal: r.smsSentTotal || 0,
      checkedAt: r.checkedAt || null
    };
  }

  function buildRow(item) {
    // CREG status display
    const cregLabels = {
      0: '❌ Not Registered',
      1: '✅ Home',
      2: '🔍 Searching...',
      3: '🚫 Denied',
      4: '❓ Unknown',
      5: '✅ Roaming',
      '-1': '⚠️ Error'
    };
    const cregText = cregLabels[item.cregStatus] || '❓ Unknown';
    const cregClass = item.cregOk ? 'text-success' : 'text-danger';

    // Signal display (0-31 scale, 99=unknown)
    let signalDisplay;
    if (item.signalLevel === 99 || item.signalLevel === -1) {
      signalDisplay = '<span class="text-danger">❌ 99 (Unknown)</span>';
    } else if (item.signalLevel === 0) {
      signalDisplay = '<span class="text-danger">⚠️ 0 (No signal)</span>';
    } else if (item.signalLevel < 10) {
      signalDisplay = `<span class="text-warning">📶 ${item.signalLevel} (Weak)</span>`;
    } else if (item.signalLevel < 20) {
      signalDisplay = `<span class="text-success">📶 ${item.signalLevel} (Good)</span>`;
    } else {
      signalDisplay = `<span class="text-success">📶 ${item.signalLevel} (Strong)</span>`;
    }

    // Signal bar visual
    const signalPercent = item.signalLevel >= 0 && item.signalLevel <= 31
      ? Math.round((item.signalLevel / 31) * 100) : 0;
    const signalColor = signalPercent > 60 ? 'var(--success)' : signalPercent > 30 ? 'var(--warning)' : 'var(--danger)';

    // CPIN display
    const cpinColor = item.cpinOk ? 'text-success' : 'text-danger';
    const cpinIcon = item.cpinOk ? '✅' : '❌';

    // Status badge
    let statusBadge;
    if (item.isDead) {
      statusBadge = `<span style="display:inline-block;padding:4px 12px;border-radius:20px;font-size:0.8rem;font-weight:600;background:rgba(239,68,68,0.15);color:var(--danger);border:1px solid rgba(239,68,68,0.3);">🔴 DEAD</span>`;
    } else if (item.consecutiveCregFails > 0 || item.consecutiveSmsFails > 3) {
      statusBadge = `<span style="display:inline-block;padding:4px 12px;border-radius:20px;font-size:0.8rem;font-weight:600;background:rgba(245,158,11,0.15);color:var(--warning);border:1px solid rgba(245,158,11,0.3);">⚠️ WARNING</span>`;
    } else {
      statusBadge = `<span style="display:inline-block;padding:4px 12px;border-radius:20px;font-size:0.8rem;font-weight:600;background:rgba(16,185,129,0.15);color:var(--success);border:1px solid rgba(16,185,129,0.3);">🟢 ALIVE</span>`;
    }

    // Dead reason tooltip
    const reasonTitle = item.deadReason ? ` title="${item.deadReason}"` : '';

    // Action button
    const actionBtn = item.isDead
      ? `<button class="btn btn-sm btn-outline" style="border-color:var(--success);color:var(--success);border-radius:8px;font-size:0.8rem;" onclick="window.simHealthForceRecover('${item.comName}')">🔓 Recover</button>`
      : `<button class="btn btn-sm btn-outline" style="border-radius:8px;font-size:0.8rem;" onclick="window.simHealthCheckSingle('${item.comName}')">🔍 Check</button>`;

    // SMS stats display
    let smsStatsDisplay;
    if (item.smsSentTotal > 0) {
      const successRate = Math.round((item.smsSuccessCount / item.smsSentTotal) * 100);
      const rateColor = successRate >= 90 ? 'var(--success)' : successRate >= 70 ? 'var(--warning)' : 'var(--danger)';
      smsStatsDisplay = `
        <div style="font-size:0.85rem;">
          <span class="text-success">✅ ${item.smsSuccessCount}</span>
          <span style="color:var(--text-muted);margin:0 2px;">/</span>
          <span class="text-danger">❌ ${item.smsFailedTotal}</span>
          <span style="color:var(--text-muted);margin:0 2px;">/</span>
          <span>${item.smsSentTotal}</span>
        </div>
        <div style="margin-top:4px;height:4px;background:var(--bg-dark);border-radius:2px;overflow:hidden;">
          <div style="height:100%;width:${successRate}%;background:${rateColor};border-radius:2px;transition:width 0.3s;"></div>
        </div>
        <div style="font-size:0.75rem;color:${rateColor};margin-top:2px;">${successRate}% success</div>
      `;
    } else {
      smsStatsDisplay = '<span class="text-muted">Chưa gửi</span>';
    }

    return `
      <tr${reasonTitle} style="${item.isDead ? 'background:rgba(239,68,68,0.03);' : ''}">
        <td><strong>${item.comName}</strong></td>
        <td style="font-family: monospace;">${item.phoneNumber}</td>
        <td><span class="${cregClass}">${cregText}</span></td>
        <td>
          ${signalDisplay}
          <div style="margin-top:4px;height:4px;background:var(--bg-dark);border-radius:2px;overflow:hidden;">
            <div style="height:100%;width:${signalPercent}%;background:${signalColor};border-radius:2px;transition:width 0.3s;"></div>
          </div>
        </td>
        <td><span class="${cpinColor}">${cpinIcon} ${item.cpinStatus}</span></td>
        <td>${item.operator !== 'NO_OPERATOR' && item.operator !== 'UNKNOWN' ? item.operator : '<span class="text-muted">—</span>'}</td>
        <td>${smsStatsDisplay}</td>
        <td>${statusBadge}</td>
        <td>${actionBtn}</td>
      </tr>
    `;
  }

  // =========================================================================
  // ACTIONS
  // =========================================================================
  window.simHealthForceRecover = function (comName) {
    if (!confirm(`Force recover SIM ${comName}? SIM sẽ được đánh dấu ALIVE và cho phép gửi SMS lại.`)) return;

    fetch(`/api/sim-health/${encodeURIComponent(comName)}/recover`, { method: 'POST' })
      .then(res => res.json())
      .then(data => {
        if (data.success) {
          showToast(`✅ SIM ${comName} đã được recover thành công!`, 'success');
          fetchHealthData();
        } else {
          showToast(`❌ Recover thất bại: ${data.message}`, 'error');
        }
      })
      .catch(err => {
        showToast(`❌ Lỗi: ${err.message}`, 'error');
      });
  };

  window.simHealthCheckSingle = function (comName) {
    showToast(`🔍 Đang check SIM ${comName}...`, 'info');
    fetch(`/api/sim-health/${encodeURIComponent(comName)}`)
      .then(res => res.json())
      .then(result => {
        // Update in healthData
        const idx = healthData.findIndex(r => (r.comPort || r.comName) === comName);
        if (idx >= 0) {
          healthData[idx] = result;
        } else {
          healthData.push(result);
        }
        updateStats(healthData);
        renderHealthTable(healthData);

        const status = result.isDead ? '🔴 DEAD' : '🟢 ALIVE';
        showToast(`${status} — ${comName}: CREG=${result.cregStatus}, CSQ=${result.signalLevel}`, result.isDead ? 'error' : 'success');
      })
      .catch(err => {
        showToast(`❌ Check thất bại: ${err.message}`, 'error');
      });
  };

  // =========================================================================
  // BADGE & TOAST
  // =========================================================================
  function updateDeadBadge(count) {
    const badge = document.getElementById('dead-sim-badge');
    if (!badge) return;

    if (count > 0) {
      badge.textContent = count;
      badge.style.display = 'inline-flex';
    } else {
      badge.style.display = 'none';
    }
  }

  function showDeadSimToast(alert) {
    showToast(`🔴 SIM DEAD: ${alert.comPort || alert.comName} (${alert.phoneNumber}) — ${alert.reason}`, 'error');
  }

  function showToast(message, type) {
    // Try to use existing toast system
    if (typeof window.showToastNotification === 'function') {
      window.showToastNotification(message, type);
      return;
    }

    // Fallback: create simple toast
    const container = document.getElementById('toast-container');
    if (!container) {
      console.log(`[Toast ${type}]`, message);
      return;
    }

    const toast = document.createElement('div');
    toast.className = `toast toast-${type || 'info'}`;
    toast.style.cssText = 'padding:12px 20px;margin-bottom:8px;border-radius:10px;background:var(--bg-card);border:1px solid var(--border-color);color:var(--text-primary);font-size:0.9rem;box-shadow:0 4px 12px rgba(0,0,0,0.3);animation:slideInRight 0.3s ease;max-width:400px;';

    if (type === 'error') toast.style.borderColor = 'var(--danger)';
    else if (type === 'success') toast.style.borderColor = 'var(--success)';
    else toast.style.borderColor = 'var(--primary)';

    toast.textContent = message;
    container.appendChild(toast);

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transition = 'opacity 0.3s';
      setTimeout(() => toast.remove(), 300);
    }, 5000);
  }
})();
