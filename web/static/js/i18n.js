/**
 * Lightweight i18n (internationalization) library
 * Supports: static text via data-i18n attributes, dynamic text via i18n.t()
 * Language persistence via localStorage
 */
const i18n = {
    currentLang: 'zh',
    translations: {},
    fallbackLang: 'zh',
    onLanguageChange: [],

    /**
     * Initialize i18n with available languages and default language
     */
    async init(defaultLang = 'zh') {
        const saved = localStorage.getItem('lang') || defaultLang;
        await this.setLanguage(saved);
    },

    /**
     * Load a language pack and switch to it
     */
    async setLanguage(lang) {
        try {
            const resp = await fetch(`/static/data/lang/${lang}.json`);
            if (!resp.ok) throw new Error(`Failed to load ${lang}.json`);
            this.translations = await resp.json();
            this.currentLang = lang;
            localStorage.setItem('lang', lang);
            document.documentElement.lang = lang === 'zh' ? 'zh-CN' : 'en';

            // Update all elements with data-i18n attribute
            document.querySelectorAll('[data-i18n]').forEach(el => {
                const key = el.getAttribute('data-i18n');
                if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
                    const phKey = el.getAttribute('data-i18n-placeholder');
                    if (phKey) el.placeholder = this.t(phKey);
                    // For input value placeholder, check data-i18n-placeholder too
                    const titleKey = el.getAttribute('data-i18n-title');
                    if (titleKey) el.title = this.t(titleKey);
                } else {
                    el.innerHTML = this.t(key);
                }
            });

            // Update elements with data-i18n-placeholder
            document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
                const key = el.getAttribute('data-i18n-placeholder');
                el.placeholder = this.t(key);
            });

            // Update elements with data-i18n-title
            document.querySelectorAll('[data-i18n-title]').forEach(el => {
                const key = el.getAttribute('data-i18n-title');
                el.title = this.t(key);
            });

            // Fire callbacks
            this.onLanguageChange.forEach(cb => cb(lang));

            return true;
        } catch (e) {
            console.warn(`i18n: Failed to load language "${lang}":`, e);
            // Fallback to default language
            if (lang !== this.fallbackLang) {
                return this.setLanguage(this.fallbackLang);
            }
            return false;
        }
    },

    /**
     * Translate a key - supports nested keys like "section.key"
     */
    t(key, params = {}) {
        let value = key.split('.').reduce((obj, k) => obj && obj[k], this.translations);
        if (value === undefined) return key;

        // Replace {param} placeholders
        if (params && typeof value === 'string') {
            Object.entries(params).forEach(([k, v]) => {
                value = value.replace(new RegExp(`\\{${k}\\}`, 'g'), v);
            });
        }
        return value;
    },

    /**
     * Get current language
     */
    getLang() {
        return this.currentLang;
    },

    getDocLang() {
        return document.documentElement.lang;
    },

    /**
     * Check if current language is Chinese
     */
    isZh() {
        return this.currentLang === 'zh';
    },

    /**
     * Register callback for language change
     */
    onChange(callback) {
        this.onLanguageChange.push(callback);
    }
};
