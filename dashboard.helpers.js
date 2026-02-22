(function () {
    function formatDate(value) {
        if (!value) return '-';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return value;
        return date.toLocaleDateString('es-CO', {
            timeZone: 'America/Bogota',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        });
    }

    function formatDateTime(value) {
        if (!value) return '-';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return value;
        return date.toLocaleString('es-CO', {
            timeZone: 'America/Bogota',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        });
    }

    function formatTime24(value) {
        if (!value) return '-';
        const asText = String(value);
        if (/^\d{2}:\d{2}(:\d{2})?$/.test(asText)) return asText.slice(0, 5);
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return asText;
        return date.toLocaleTimeString('es-CO', {
            timeZone: 'America/Bogota',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        });
    }

    function escapeHtml(value) {
        const text = String(value ?? '');
        return text
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#39;');
    }

    function normalizeText(value) {
        return String(value || '').replaceAll('\uFEFF', '').trim().toLowerCase();
    }

    function normalizeCode(value) {
        return String(value || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    }

    function normalizeVehicleKey(value) {
        return normalizeCode(String(value || '').trim());
    }

    function isActiveDriverStatus(status) {
        const value = normalizeText(status);
        return value === 'activo' || value === 'active' || value === 'habilitado';
    }

    function parseCSV(text) {
        const rows = [];
        let row = [];
        let cell = '';
        let inQuotes = false;

        for (let i = 0; i < text.length; i += 1) {
            const char = text[i];
            const next = text[i + 1];

            if (char === '"') {
                if (inQuotes && next === '"') {
                    cell += '"';
                    i += 1;
                } else {
                    inQuotes = !inQuotes;
                }
                continue;
            }

            if (char === ',' && !inQuotes) {
                row.push(cell);
                cell = '';
                continue;
            }

            if ((char === '\n' || char === '\r') && !inQuotes) {
                if (char === '\r' && next === '\n') i += 1;
                row.push(cell);
                if (row.some((item) => item.trim() !== '')) rows.push(row);
                row = [];
                cell = '';
                continue;
            }

            cell += char;
        }

        if (cell.length > 0 || row.length > 0) {
            row.push(cell);
            if (row.some((item) => item.trim() !== '')) rows.push(row);
        }

        return rows;
    }

    function getFieldByKeywords(row, keywords) {
        const entries = Object.entries(row || {});
        for (const [key, value] of entries) {
            const keyNorm = normalizeText(key);
            if (keywords.every((k) => keyNorm.includes(k))) return String(value || '').trim();
        }
        return '';
    }

    function isVinculadoStatus(status) {
        const s = normalizeText(status);
        return s.includes('vincul') && !s.includes('desvinc');
    }

    function parseDateFlexible(value) {
        const raw = String(value || '').trim();
        if (!raw) return null;

        const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (iso) return new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`);

        const dmy = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (dmy) return new Date(`${dmy[3]}-${dmy[2]}-${dmy[1]}T00:00:00`);

        const parsed = new Date(raw);
        return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    function getDateStatus(value) {
        const date = parseDateFlexible(value);
        if (!date) return { text: 'Sin fecha', level: 'unknown', days: null };

        const today = new Date();
        today.setHours(0, 0, 0, 0);
        date.setHours(0, 0, 0, 0);
        const diffDays = Math.floor((date.getTime() - today.getTime()) / 86400000);

        if (diffDays < 0) return { text: `Vencido hace ${Math.abs(diffDays)} dia(s)`, level: 'expired', days: diffDays };
        if (diffDays === 0) return { text: 'Vence hoy', level: 'warning', days: diffDays };
        if (diffDays <= 30) return { text: `Vence en ${diffDays} dia(s)`, level: 'warning', days: diffDays };
        return { text: `Vigente (${diffDays} dia(s))`, level: 'ok', days: diffDays };
    }

    function getTodayParts() {
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const day = String(now.getDate()).padStart(2, '0');
        return { year, month, day };
    }

    function formatMinutesDiff(minutes) {
        return `${Math.abs(Math.round(minutes))} minuto(s)`;
    }

    function getDispatchDateKey(dispatch) {
        const value = dispatch.departure_time || dispatch.created_at;
        if (!value) return '';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return '';
        const parts = new Intl.DateTimeFormat('en-CA', {
            timeZone: 'America/Bogota',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        }).formatToParts(date);
        const year = parts.find((p) => p.type === 'year')?.value || '';
        const month = parts.find((p) => p.type === 'month')?.value || '';
        const day = parts.find((p) => p.type === 'day')?.value || '';
        return `${year}-${month}-${day}`;
    }

    function getStoredDateKey(dispatch) {
        const raw = String(dispatch.departure_time || dispatch.created_at || '').trim();
        if (!raw) return '';

        const isoMatch = raw.match(/^(\d{4}-\d{2}-\d{2})/);
        if (isoMatch) return isoMatch[1];

        const dmyMatch = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (dmyMatch) {
            return `${dmyMatch[3]}-${dmyMatch[2]}-${dmyMatch[1]}`;
        }

        return getDispatchDateKey(dispatch);
    }

    window.DashboardHelpers = {
        formatDate,
        formatDateTime,
        formatTime24,
        escapeHtml,
        normalizeText,
        normalizeCode,
        normalizeVehicleKey,
        isActiveDriverStatus,
        parseCSV,
        getFieldByKeywords,
        isVinculadoStatus,
        parseDateFlexible,
        getDateStatus,
        getTodayParts,
        formatMinutesDiff,
        getDispatchDateKey,
        getStoredDateKey
    };
})();
