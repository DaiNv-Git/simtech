/**
 * i18n Auto-Translator Helper
 * Add this script temporarily to auto-add data-i18n to dynamic content
 */

// Mapping of Vietnamese text to translation keys
const TEXT_TO_KEY_MAP = {
    // Stats
    'SIM Online': 'dashboard.simsOnline',
    'Tin nhắn': 'dashboard.messages',
    'Cuộc gọi': 'dashboard.calls',
    'Ghi âm': 'dashboard.recordings',

    // Buttons
    'Scan SIM': 'buttons.scan',
    'Gửi': 'buttons.send',
    'Gọi': 'buttons.call',
    'Lưu': 'buttons.save',
    'Lưu cài đặt': 'settings.save',
    'Hủy': 'buttons.cancel',
    'Làm mới': 'buttons.refresh',
    'Đóng': 'buttons.close',

    // Empty states
    'Chưa có SIM nào': 'dashboard.noSims',
    'Nhấn "Scan SIM" để quét': 'dashboard.clickScan',
    'Chưa có tin nhắn': 'sms.noMessages',
    'Chưa có cuộc gọi nào': 'calls.noCalls',
    'Chưa có file ghi âm nào': 'recordings.noRecordings',

    // Page titles (will be set by JS navigation)
    'Dashboard': 'dashboard.title',
    'Quét SIM': 'scanSim.title',
    'Thực hiện cuộc gọi': 'calls.makeCall',

    // Settings
    'Cài đặt chung': 'settings.general',
    'Tự động scan SIM khi khởi động': 'settings.autoScan',
    'Tự động quét tất cả COM port khi mở ứng dụng': 'settings.autoScanDesc',
    'Thông báo tin nhắn mới': 'settings.notification',
    'Hiển thị thông báo khi nhận tin nhắn mới': 'settings.notificationDesc',
    'Âm thanh thông báo': 'settings.sound',
    'Phát âm thanh khi có tin nhắn/cuộc gọi mới': 'settings.soundDesc',
    'Ngôn ngữ giao diện': 'settings.language',
    'Tự động ghi âm cuộc gọi': 'settings.autoRecord',
    'Ghi âm tất cả cuộc gọi tự động': 'settings.autoRecordDesc',
    'Thư mục lưu ghi âm': 'settings.recordingPath',
    'Thay đổi': 'settings.change',
    'URL Server': 'settings.serverUrl',
    'Kiểm tra kết nối': 'settings.testConnection',
    'Tắt ứng dụng': 'settings.shutdown',

    // Tabs
    'Hộp thư đến': 'sms.inbox',
    'Đã gửi': 'sms.sent',
    'Thất bại': 'sms.outbox',

    // Loading
    'Đang tải...': 'toasts.loading'
};

// Auto-translate function
function autoTranslate() {
    let count = 0;

    // Find all text nodes and add data-i18n if text matches
    document.querySelectorAll('span, p, h3, button, label').forEach(el => {
        const text = el.textContent.trim();
        const key = TEXT_TO_KEY_MAP[text];

        if (key && !el.hasAttribute('data-i18n')) {
            el.setAttribute('data-i18n', key);
            count++;
            console.log(`Added data-i18n="${key}" to:`, text);
        }
    });

    console.log(`✅ Added data-i18n to ${count} elements`);

    // Re-apply translations
    if (typeof i18n !== 'undefined' && i18n.isLoaded()) {
        i18n.applyTranslations();
        console.log('✅ Applied translations');
    }
}

// Auto-run on load
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', autoTranslate);
} else {
    autoTranslate();
}

// Export for manual use
window.autoTranslate = autoTranslate;
