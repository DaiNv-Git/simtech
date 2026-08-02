/**
 * i18n (Internationalization) System
 * Hỗ trợ: Tiếng Việt (vi), 日本語 (ja), 中文 (zh)
 */

const i18n = {
    currentLang: 'vi',
    translations: {},
    fallbackLang: 'vi',

    /**
     * Load translation file for specified language
     */
    async loadLanguage(lang) {
        try {
            const response = await fetch(`/i18n/${lang}.json`);
            if (!response.ok) {
                console.warn(`Failed to load ${lang}.json, using fallback`);
                if (lang !== this.fallbackLang) {
                    return this.loadLanguage(this.fallbackLang);
                }
                return;
            }

            this.translations = await response.json();
            this.currentLang = lang;
            this.applyTranslations();

            console.log(`✅ Loaded language: ${lang}`);
        } catch (error) {
            console.error('Error loading language:', error);
            if (lang !== this.fallbackLang) {
                this.loadLanguage(this.fallbackLang);
            }
        }
    },

    /**
     * Get translation for key (supports nested keys with dot notation)
     * Example: t('nav.dashboard') => 'Tổng quan'
     */
    t(key) {
        const keys = key.split('.');
        let value = this.translations;

        for (const k of keys) {
            value = (value ? value[k] : undefined);
            if (value === undefined) {
                console.warn(`Translation missing: ${key}`);
                return key;
            }
        }

        return value;
    },

    /**
     * Apply all translations to DOM elements with data-i18n attributes
     */
    applyTranslations() {
        // Translate text content
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            const translation = this.t(key);

            if (translation !== key) {
                el.textContent = translation;
            }
        });

        // Translate placeholders
        document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
            const key = el.getAttribute('data-i18n-placeholder');
            const translation = this.t(key);

            if (translation !== key) {
                el.placeholder = translation;
            }
        });

        // Translate titles/tooltips
        document.querySelectorAll('[data-i18n-title]').forEach(el => {
            const key = el.getAttribute('data-i18n-title');
            const translation = this.t(key);

            if (translation !== key) {
                el.title = translation;
            }
        });

        // Update document title
        const appTitle = this.t('app.title');
        if (appTitle && appTitle !== 'app.title') {
            document.title = appTitle;
        }
    },

    /**
     * Get current language
     */
    getCurrentLanguage() {
        return this.currentLang;
    },

    /**
     * Check if language is loaded
     */
    isLoaded() {
        return Object.keys(this.translations).length > 0;
    }
};

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = i18n;
}

// ✅ AUTO-INIT: Load saved language on page load
(function autoInit() {
    // Get saved language from localStorage (quick load) or use default
    const savedLang = localStorage.getItem('appLanguage') || 'vi';

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            i18n.loadLanguage(savedLang).then(() => {
                // Sync to localStorage after successful load
                localStorage.setItem('appLanguage', i18n.currentLang);
            });
        });
    } else {
        i18n.loadLanguage(savedLang).then(() => {
            // Sync to localStorage after successful load
            localStorage.setItem('appLanguage', i18n.currentLang);
        });
    }
})();
