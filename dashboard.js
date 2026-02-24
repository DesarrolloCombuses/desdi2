const {
    SUPABASE_URL,
    SUPABASE_KEY,
    DRIVERS_CSV_URL,
    VEHICLES_CSV_URL,
    FLEET_CSV_URL,
    SICOV_CSV_URL,
    EXT_SUPABASE_URL,
    EXT_SUPABASE_ANON_KEY,
    EXT_VEHICULO_CONDUCTORES_TABLE,
    APPS_SCRIPT_URL,
    ITINERARIES,
    DISPATCH_PAGE_SIZE,
    MANAGER_SHIFT_PAGE_SIZE
} = window.DashboardConfig || {};
const {
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
    getDateStatus,
    getTodayParts,
    formatMinutesDiff,
    getDispatchDateKey,
    getStoredDateKey
} = window.DashboardHelpers || {};
if (!SUPABASE_URL || !SUPABASE_KEY || typeof formatDate !== 'function') {
    throw new Error('No se pudo cargar la configuracion/helpers del dashboard.');
}

const { createClient } = window.supabase;
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);
const sbExternal = (EXT_SUPABASE_URL && EXT_SUPABASE_ANON_KEY)
    ? createClient(EXT_SUPABASE_URL, EXT_SUPABASE_ANON_KEY, {
        auth: {
            persistSession: false,
            autoRefreshToken: false,
            detectSessionInUrl: false
        }
    })
    : null;
const LOGIN_LOCK_TABLE = 'user_login_locks';
const LOCK_TOKEN_KEY = 'combuses_lock_token';
const LOCK_HEARTBEAT_MS = 60 * 1000;
const DISPATCH_QUEUE_KEY_PREFIX = 'pending_dispatch_queue:';

let currentUser = null;
let driversCatalog = [];
let vehiclesCatalog = [];
let dispatchesCache = [];
let lastNoDataAlertKey = '';
let dispatchPage = 1;
let fleetRows = [];
let fleetHeaders = [];
let fleetRecords = [];
let lastVehicleExpiryAlertKey = '';
let sicovRows = [];
let sicovHeaders = [];
let externalVcRows = [];
let externalVcHeaders = [];
let managerProfile = null;
let activeShift = null;
let managerAuthenticated = false;
let selectedItineraryGroup = '';
let managerShiftRows = [];
let managerShiftPage = 1;
let lastManagerAlertKey = '';
let shiftMapInstances = [];
let suppressManagerAlerts = false;
let shiftSessionMismatch = false;
const SHIFT_TOKEN_STORAGE_PREFIX = 'manager_shift_token:';

const userEmail = document.getElementById('userEmail');
const dispatchForm = document.getElementById('dispatchForm');
const vehicle = document.getElementById('vehicle');
const vehicleInfo = document.getElementById('vehicleInfo');
const quickRecentDispatchesList = document.getElementById('quickRecentDispatchesList');
const vehicleSelectedMeta = document.getElementById('vehicleSelectedMeta');
const vehicleDocsPanel = document.getElementById('vehicleDocsPanel');
const departureDate = document.getElementById('departureDate');
const departureTime = document.getElementById('departureTime');
const liveClock24 = document.getElementById('liveClock24');
const routeInput = document.getElementById('route');
const routeSelectedMeta = document.getElementById('routeSelectedMeta');
const driver = document.getElementById('driver');
const driverSelectedMeta = document.getElementById('driverSelectedMeta');
const manager = document.getElementById('manager');
const managerIdentity = document.getElementById('managerIdentity');
const managerItineraryGroup = document.getElementById('managerItineraryGroup');
const startShiftBtn = document.getElementById('startShiftBtn');
const endShiftBtn = document.getElementById('endShiftBtn');
const managerStatus = document.getElementById('managerStatus');
const managerShiftHistory = document.getElementById('managerShiftHistory');
const shiftPrevBtn = document.getElementById('shiftPrevBtn');
const shiftNextBtn = document.getElementById('shiftNextBtn');
const shiftPageInfo = document.getElementById('shiftPageInfo');
const notes = document.getElementById('notes');
const driverInfo = document.getElementById('driverInfo');
const dispatchFormCard = document.getElementById('dispatchFormCard');
const dispatchListCard = document.getElementById('dispatchListCard');
const dispatchesList = document.getElementById('dispatchesList');
const passengerAlert = document.getElementById('passengerAlert');
const dispatchPrevBtn = document.getElementById('dispatchPrevBtn');
const dispatchNextBtn = document.getElementById('dispatchNextBtn');
const dispatchPageInfo = document.getElementById('dispatchPageInfo');
const missingPassengerList = document.getElementById('missingPassengerList');
const missingItineraryFilter = document.getElementById('missingItineraryFilter');
const salidasList = document.getElementById('salidasList');
const itineraryFilter = document.getElementById('itineraryFilter');
const dispatchDateFilter = document.getElementById('dispatchDateFilter');
const dispatchManagerFilter = document.getElementById('dispatchManagerFilter');
const dispatchItineraryFilter = document.getElementById('dispatchItineraryFilter');
const sessionManagerBtn = document.getElementById('sessionManagerBtn');
const sessionDispatchBtn = document.getElementById('sessionDispatchBtn');
const sessionSalidasBtn = document.getElementById('sessionSalidasBtn');
const sessionMissingBtn = document.getElementById('sessionMissingBtn');
const sessionFleetBtn = document.getElementById('sessionFleetBtn');
const sessionSicovBtn = document.getElementById('sessionSicovBtn');
const sessionExternalVcBtn = document.getElementById('sessionExternalVcBtn');
const sessionUpdatesBtn = document.getElementById('sessionUpdatesBtn');
const managerSession = document.getElementById('managerSession');
const dispatchSession = document.getElementById('dispatchSession');
const salidasSession = document.getElementById('salidasSession');
const missingSession = document.getElementById('missingSession');
const fleetSession = document.getElementById('fleetSession');
const sicovSession = document.getElementById('sicovSession');
const externalVcSession = document.getElementById('externalVcSession');
const updatesSession = document.getElementById('updatesSession');
const fleetStatus = document.getElementById('fleetStatus');
const fleetList = document.getElementById('fleetList');
const sicovStatus = document.getElementById('sicovStatus');
const sicovList = document.getElementById('sicovList');
const sicovFilter = document.getElementById('sicovFilter');
const externalVcStatus = document.getElementById('externalVcStatus');
const externalVcList = document.getElementById('externalVcList');
const externalVcFilter = document.getElementById('externalVcFilter');
const refreshAllCsvBtn = document.getElementById('refreshAllCsvBtn');
const refreshVehiclesCsvBtn = document.getElementById('refreshVehiclesCsvBtn');
const refreshDriversCsvBtn = document.getElementById('refreshDriversCsvBtn');
const refreshFleetCsvBtn = document.getElementById('refreshFleetCsvBtn');
const refreshSicovCsvBtn = document.getElementById('refreshSicovCsvBtn');
const refreshExternalVcBtn = document.getElementById('refreshExternalVcBtn');
const dispatchLockedNotice = document.getElementById('dispatchLockedNotice');
const submitDispatchBtn = document.getElementById('submitDispatchBtn');
const dispatchConfirmModal = document.getElementById('dispatchConfirmModal');
const dispatchConfirmText = document.getElementById('dispatchConfirmText');
const dispatchConfirmCancel = document.getElementById('dispatchConfirmCancel');
const dispatchConfirmOk = document.getElementById('dispatchConfirmOk');
const scanVehicleQrBtn = document.getElementById('scanVehicleQrBtn');
const qrScannerModal = document.getElementById('qrScannerModal');
const qrScannerVideo = document.getElementById('qrScannerVideo');
const qrScannerStatus = document.getElementById('qrScannerStatus');
const qrScannerClose = document.getElementById('qrScannerClose');
const networkStatus = document.getElementById('networkStatus');
const dispatchLockedText = document.getElementById('dispatchLockedText');
let hasInternet = navigator.onLine !== false;
let dispatchEntryMethod = 'manual';
let qrSelectionInProgress = false;
let qrScannerStream = null;
let qrScannerRunning = false;
let qrScannerFrameId = null;
let qrDetector = null;
let qrCanvas = null;
let qrCanvasCtx = null;
let vehicleDriverLookupSeq = 0;
let lastVehicleSelectionKey = '';
let dispatchSubmitInFlight = false;
let managerShiftActionInFlight = false;
let loginLockHeartbeatTimer = null;
let externalVcLoading = false;
let externalVcAutoRefreshTimer = null;
let liveClockTimer = null;

function getDispatchQueueStorageKey() {
    return `${DISPATCH_QUEUE_KEY_PREFIX}${currentUser?.id || 'anon'}`;
}

function readPendingDispatchQueue() {
    try {
        const raw = localStorage.getItem(getDispatchQueueStorageKey()) || '[]';
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
        return [];
    }
}

function writePendingDispatchQueue(items) {
    try {
        localStorage.setItem(getDispatchQueueStorageKey(), JSON.stringify(items));
    } catch (err) {
        // Si localStorage falla, no bloqueamos UX.
    }
}

function enqueuePendingDispatch(entry) {
    const queue = readPendingDispatchQueue();
    queue.push({
        id: `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`,
        queued_at: new Date().toISOString(),
        ...entry
    });
    writePendingDispatchQueue(queue);
    return queue.length;
}

if (
    !userEmail || !dispatchForm || !vehicle || !vehicleInfo || !quickRecentDispatchesList || !vehicleSelectedMeta || !vehicleDocsPanel || !departureDate || !departureTime || !liveClock24 || !routeInput || !routeSelectedMeta ||
    !driver || !driverSelectedMeta || !manager || !managerIdentity || !managerItineraryGroup || !startShiftBtn || !endShiftBtn || !managerStatus || !managerShiftHistory || !shiftPrevBtn || !shiftNextBtn || !shiftPageInfo ||
    !dispatchFormCard || !dispatchListCard || !dispatchLockedNotice ||
    !notes || !driverInfo || !dispatchesList || !passengerAlert || !dispatchPrevBtn || !dispatchNextBtn || !dispatchPageInfo || !missingPassengerList || !missingItineraryFilter || !salidasList || !itineraryFilter ||
    !dispatchDateFilter || !dispatchManagerFilter || !dispatchItineraryFilter ||
    !sessionManagerBtn || !sessionDispatchBtn || !sessionSalidasBtn || !sessionMissingBtn || !sessionFleetBtn || !sessionSicovBtn || !sessionExternalVcBtn || !sessionUpdatesBtn || !managerSession || !dispatchSession || !salidasSession || !missingSession || !fleetSession || !sicovSession || !externalVcSession || !updatesSession || !fleetStatus || !fleetList || !sicovStatus || !sicovList || !sicovFilter || !externalVcStatus || !externalVcList || !externalVcFilter ||
    !refreshAllCsvBtn || !refreshVehiclesCsvBtn || !refreshDriversCsvBtn || !refreshFleetCsvBtn || !refreshSicovCsvBtn || !refreshExternalVcBtn ||
    !dispatchConfirmModal || !dispatchConfirmText || !dispatchConfirmCancel || !dispatchConfirmOk || !scanVehicleQrBtn || !qrScannerModal || !qrScannerVideo || !qrScannerStatus || !qrScannerClose || !submitDispatchBtn ||
    !networkStatus || !dispatchLockedText
) {
    throw new Error('Faltan elementos en el HTML del dashboard.');
}

function renderVehicleDocsStatus() {
    const selected = normalizeCode(vehicle.value);
    if (!selected) {
        vehicleDocsPanel.style.display = 'none';
        vehicleDocsPanel.innerHTML = '';
        return;
    }

    const record = fleetRecords.find((item) => normalizeCode(item.interno) === selected);
    if (!record) {
        vehicleDocsPanel.style.display = 'block';
        vehicleDocsPanel.innerHTML = `<p class="doc-line doc-unknown"><b>Documentos:</b> no se encontro informacion para este interno.</p>`;
        return;
    }

    const soat = getDateStatus(record.soat);
    const tecno = getDateStatus(record.tecnomecanica);
    const oper = getDateStatus(record.operacion);
    const hasExpired = [soat, tecno, oper].some((s) => s.level === 'expired');

    vehicleDocsPanel.style.display = 'block';
    vehicleDocsPanel.innerHTML = `
        <p class="doc-line"><b>Interno:</b> ${escapeHtml(record.interno || '-')} | <b>Estado:</b> ${escapeHtml(record.estado || '-')}</p>
        <p class="doc-line doc-${soat.level}"><b>SOAT:</b> ${escapeHtml(record.soat || '-')} - ${escapeHtml(soat.text)}</p>
        <p class="doc-line doc-${tecno.level}"><b>Tecnomecanica:</b> ${escapeHtml(record.tecnomecanica || '-')} - ${escapeHtml(tecno.text)}</p>
        <p class="doc-line doc-${oper.level}"><b>Operacion:</b> ${escapeHtml(record.operacion || '-')} - ${escapeHtml(oper.text)}</p>
    `;

    if (hasExpired) {
        const key = `${record.interno}|${record.soat}|${record.tecnomecanica}|${record.operacion}`;
        if (key !== lastVehicleExpiryAlertKey) {
            alert(`Alerta: el interno ${record.interno} tiene documentos vencidos.`);
            lastVehicleExpiryAlertKey = key;
        }
    }
}

function buildNoCacheUrl(url) {
    const raw = String(url || '').trim();
    if (!raw) return raw;
    try {
        const u = new URL(raw, window.location.href);
        u.searchParams.set('_cb', String(Date.now()));
        return u.toString();
    } catch (err) {
        const sep = raw.includes('?') ? '&' : '?';
        return `${raw}${sep}_cb=${Date.now()}`;
    }
}


function setAutomaticDate() {
    const { year, month, day } = getTodayParts();
    departureDate.value = `${day}/${month}/${year}`;
}

function getDepartureIsoFromDateAndTime(dateValue, timeValue) {
    const match = String(dateValue || '').trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
    if (!match) return '';
    const day = match[1];
    const month = match[2];
    const year = match[3];
    // Guardamos la hora seleccionada como hora de Colombia (-05:00) para evitar desfases.
    return `${year}-${month}-${day}T${timeValue}:00-05:00`;
}

function getCurrentDateKeyBogota() {
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'America/Bogota',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).formatToParts(new Date());
    const y = parts.find((p) => p.type === 'year')?.value || '';
    const m = parts.find((p) => p.type === 'month')?.value || '';
    const d = parts.find((p) => p.type === 'day')?.value || '';
    return `${y}-${m}-${d}`;
}

function getActiveDispatchDateKey() {
    return String(dispatchDateFilter.value || '').trim() || getCurrentDateKeyBogota();
}

function getDateRangeIsoBogota(dateKey) {
    const dateMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(dateKey || ''));
    if (!dateMatch) return null;
    const y = Number(dateMatch[1]);
    const m = Number(dateMatch[2]);
    const d = Number(dateMatch[3]);
    const start = `${dateMatch[1]}-${dateMatch[2]}-${dateMatch[3]}T00:00:00-05:00`;
    const next = new Date(Date.UTC(y, m - 1, d + 1, 5, 0, 0));
    const yy = next.getUTCFullYear();
    const mm = String(next.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(next.getUTCDate()).padStart(2, '0');
    const end = `${yy}-${mm}-${dd}T00:00:00-05:00`;
    return { start, end };
}

function buildDispatchNotesWithMethod(userText) {
    const modeText = dispatchEntryMethod === 'qr'
        ? 'Despacho generado por lectura QR'
        : 'Despacho generado manualmente';
    const clean = String(userText || '').trim();
    return clean ? `${modeText}. ${clean}` : modeText;
}

function findVehicleByQrRaw(rawText) {
    const code = normalizeCode(rawText);
    if (!code) return null;

    return vehiclesCatalog.find((item) => {
        const placaCode = normalizeCode(item.placa || '');
        const idCode = normalizeCode(item.id || '');
        const descCode = normalizeCode(item.descripcion || '');
        return code === placaCode || code === idCode || code === descCode;
    }) || null;
}

function applyScannedVehicle(rawText) {
    const found = findVehicleByQrRaw(rawText);
    if (!found) {
        alert(`No se encontro un vehiculo para el codigo: ${rawText}`);
        return false;
    }

    const targetValue = found.descripcion || found.placa;
    if (!targetValue) {
        alert('El vehiculo encontrado no tiene un valor seleccionable.');
        return false;
    }

    qrSelectionInProgress = true;
    vehicle.value = targetValue;
    vehicle.dispatchEvent(new Event('change'));
    qrSelectionInProgress = false;
    dispatchEntryMethod = 'qr';
    return true;
}

async function stopQrScanner() {
    qrScannerRunning = false;
    if (qrScannerFrameId) {
        cancelAnimationFrame(qrScannerFrameId);
        qrScannerFrameId = null;
    }
    if (qrScannerStream) {
        qrScannerStream.getTracks().forEach((track) => track.stop());
        qrScannerStream = null;
    }
    qrScannerVideo.srcObject = null;
    qrScannerModal.style.display = 'none';
}

function scheduleQrScanLoop() {
    if (!qrScannerRunning) return;
    qrScannerFrameId = requestAnimationFrame(scanQrFrame);
}

async function scanQrFrame() {
    if (!qrScannerRunning) return;
    if (!qrScannerVideo.videoWidth || !qrScannerVideo.videoHeight) {
        scheduleQrScanLoop();
        return;
    }

    if (!qrCanvas) {
        qrCanvas = document.createElement('canvas');
        qrCanvasCtx = qrCanvas.getContext('2d', { willReadFrequently: true });
    }

    qrCanvas.width = qrScannerVideo.videoWidth;
    qrCanvas.height = qrScannerVideo.videoHeight;
    qrCanvasCtx.drawImage(qrScannerVideo, 0, 0, qrCanvas.width, qrCanvas.height);

    try {
        if (!qrDetector && 'BarcodeDetector' in window && window.isSecureContext) {
            qrDetector = new window.BarcodeDetector({ formats: ['qr_code'] });
        }

        let detectedValue = '';
        let barcodeDetectorFailed = false;

        if (qrDetector) {
            try {
                const results = await qrDetector.detect(qrCanvas);
                if (results && results.length > 0) {
                    detectedValue = String(results[0].rawValue || '').trim();
                }
            } catch (detectorErr) {
                barcodeDetectorFailed = true;
                // En algunos navegadores el servicio interno de BarcodeDetector falla en runtime.
                // Desactivamos ese motor para continuar con jsQR sin interrumpir al usuario.
                qrDetector = null;
            }
        }

        if (!detectedValue && typeof window.jsQR === 'function') {
            const imgData = qrCanvasCtx.getImageData(0, 0, qrCanvas.width, qrCanvas.height);
            const decoded = window.jsQR(imgData.data, imgData.width, imgData.height, {
                inversionAttempts: 'dontInvert'
            });
            if (decoded && decoded.data) {
                detectedValue = String(decoded.data).trim();
            }
            if (barcodeDetectorFailed) {
                qrScannerStatus.textContent = 'Usando modo de escaneo compatible...';
            }
        } else if (!detectedValue && !qrDetector) {
            qrScannerStatus.textContent = 'No hay motor de lectura QR disponible en este navegador.';
            scheduleQrScanLoop();
            return;
        }

        if (detectedValue) {
            const ok = applyScannedVehicle(detectedValue);
            if (ok) {
                qrScannerStatus.textContent = 'QR detectado, seleccionando vehiculo...';
                await stopQrScanner();
                alert(`Vehiculo seleccionado por QR: ${detectedValue}`);
                return;
            }
        }
    } catch (err) {
        qrScannerStatus.textContent = `Error de escaneo: ${err.message}`;
    }

    scheduleQrScanLoop();
}

async function startQrScanner() {
    if (!vehiclesCatalog.length) {
        alert('Primero debes cargar vehiculos.');
        return;
    }

    const hasBarcodeDetector = 'BarcodeDetector' in window && window.isSecureContext;
    const hasJsQr = typeof window.jsQR === 'function';
    if (!hasBarcodeDetector && !hasJsQr) {
        const manual = prompt('No se detecto motor QR. Ingresa el codigo manualmente:');
        if (!manual) return;
        if (applyScannedVehicle(manual)) {
            alert('Vehiculo seleccionado con codigo ingresado manualmente.');
        }
        return;
    }

    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        const manual = prompt('Tu navegador no permite camara. Ingresa codigo QR manualmente:');
        if (!manual) return;
        if (applyScannedVehicle(manual)) {
            alert('Vehiculo seleccionado con codigo ingresado manualmente.');
        }
        return;
    }

    try {
        qrScannerStatus.textContent = 'Solicitando acceso a camara...';
        qrScannerModal.style.display = 'flex';
        qrScannerStream = await navigator.mediaDevices.getUserMedia({
            video: { facingMode: { ideal: 'environment' } },
            audio: false
        });
        qrScannerVideo.srcObject = qrScannerStream;
        qrScannerRunning = true;
        qrScannerStatus.textContent = 'Apunta la camara al codigo QR del vehiculo.';
        scheduleQrScanLoop();
    } catch (err) {
        qrScannerModal.style.display = 'none';
        alert(`No se pudo abrir la camara para escanear QR. (${err.message})`);
    }
}


function openDispatchConfirmModal(message, okLabel = 'Despachar de todos modos') {
    return new Promise((resolve) => {
        dispatchConfirmText.textContent = message;
        dispatchConfirmOk.textContent = okLabel;
        dispatchConfirmModal.style.display = 'flex';

        const onCancel = () => cleanup(false);
        const onOk = () => cleanup(true);
        const onBackdrop = (e) => {
            if (e.target === dispatchConfirmModal) cleanup(false);
        };

        function cleanup(value) {
            dispatchConfirmModal.style.display = 'none';
            dispatchConfirmCancel.removeEventListener('click', onCancel);
            dispatchConfirmOk.removeEventListener('click', onOk);
            dispatchConfirmModal.removeEventListener('click', onBackdrop);
            dispatchConfirmOk.textContent = 'Despachar de todos modos';
            resolve(value);
        }

        dispatchConfirmCancel.addEventListener('click', onCancel);
        dispatchConfirmOk.addEventListener('click', onOk);
        dispatchConfirmModal.addEventListener('click', onBackdrop);
    });
}

async function checkVehicleDispatchWindow(vehicleName, departureIso, departureHour) {
    const targetVehicle = normalizeVehicleKey(vehicleName);
    if (!targetVehicle) return { shouldConfirm: false };

    const targetDateKey = (() => {
        const date = new Date(departureIso);
        if (Number.isNaN(date.getTime())) return '';
        const parts = new Intl.DateTimeFormat('en-CA', {
            timeZone: 'America/Bogota',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        }).formatToParts(date);
        const y = parts.find((p) => p.type === 'year')?.value || '';
        const m = parts.find((p) => p.type === 'month')?.value || '';
        const d = parts.find((p) => p.type === 'day')?.value || '';
        return `${y}-${m}-${d}`;
    })();
    if (!targetDateKey) return { shouldConfirm: false };

    const targetTime = /^\d{2}:\d{2}(:\d{2})?$/.test(String(departureHour || '').trim())
        ? String(departureHour).slice(0, 5)
        : formatTime24(departureIso);
    if (!/^\d{2}:\d{2}$/.test(targetTime)) return { shouldConfirm: false };
    const targetMs = new Date(`${targetDateKey}T${targetTime}:00-05:00`).getTime();
    if (Number.isNaN(targetMs)) return { shouldConfirm: false };

    const { data, error } = await sb
        .from('dispatches')
        .select('id, vehicle, departure_time, hora_salida, route, created_at')
        .eq('user_id', currentUser.id)
        .order('departure_time', { ascending: false })
        .limit(2000);

    if (error) throw error;
    if (!data || data.length === 0) return { shouldConfirm: false };

    let closest = null;
    for (const row of data.filter((r) => normalizeVehicleKey(r.vehicle) === targetVehicle)) {
        const rowDateKey = getStoredDateKey(row);
        const rowHour = String(row.hora_salida || '').trim();
        const rowHourNormalized = /^\d{2}:\d{2}(:\d{2})?$/.test(rowHour) ? rowHour.slice(0, 5) : formatTime24(row.departure_time);
        if (!rowDateKey || !/^\d{2}:\d{2}$/.test(rowHourNormalized)) continue;
        const rowTime = new Date(`${rowDateKey}T${rowHourNormalized}:00-05:00`).getTime();
        if (Number.isNaN(rowTime)) continue;

        const diffMinutes = Math.abs((targetMs - rowTime) / 60000);
        if (diffMinutes <= 30 && (!closest || diffMinutes < closest.diffMinutes)) {
            closest = { row, diffMinutes };
        }
    }

    if (!closest) return { shouldConfirm: false };

    return {
        shouldConfirm: true,
        message: `Ya existe un despacho de este vehiculo hace ${formatMinutesDiff(closest.diffMinutes)} (Ruta: ${closest.row.route || '-'}). ¿Deseas despacharlo de todos modos?`
    };
}

function getCurrentTimeForSql() {
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    const ss = String(now.getSeconds()).padStart(2, '0');
    return `${hh}:${mm}:${ss}`;
}

async function insertDispatchWithCreatedAtFallback(basePayload) {
    const isoValue = new Date().toISOString();
    const timeValue = getCurrentTimeForSql();
    const payloadWithHour = { ...basePayload };
    const payloadWithoutHour = { ...basePayload };
    delete payloadWithoutHour.hora_salida;

    // 1) Intento principal: created_at como timestamptz (ISO), con hora_salida
    let result = await sb.from('dispatches').insert([{ ...payloadWithHour, created_at: isoValue }]);
    if (!result.error) return result;

    let msg = String(result.error.message || '').toLowerCase();

    // Si falla por columna inexistente hora_salida, reintenta sin esa columna.
    if (msg.includes('hora_salida') && (msg.includes('column') || msg.includes('schema cache'))) {
        result = await sb.from('dispatches').insert([{ ...payloadWithoutHour, created_at: isoValue }]);
        if (!result.error) return result;
        msg = String(result.error.message || '').toLowerCase();
    }

    // Solo cambiamos created_at a TIME si el error explícitamente lo requiere.
    const expectsTimeCreatedAt = msg.includes('type time') && msg.includes('created_at');
    if (!expectsTimeCreatedAt) return result;

    // 2) created_at como time, primero con hora_salida y luego sin hora_salida si aplica.
    result = await sb.from('dispatches').insert([{ ...payloadWithHour, created_at: timeValue }]);
    if (!result.error) return result;

    msg = String(result.error.message || '').toLowerCase();
    if (msg.includes('hora_salida') && (msg.includes('column') || msg.includes('schema cache'))) {
        result = await sb.from('dispatches').insert([{ ...payloadWithoutHour, created_at: timeValue }]);
    }

    return result;
}

function setSessionView(view) {
    const isManager = view === 'manager';
    const isDispatch = view === 'dispatch';
    const isSalidas = view === 'salidas';
    const isMissing = view === 'missing';
    const isFleet = view === 'fleet';
    const isSicov = view === 'sicov';
    const isExternalVc = view === 'external-vc';
    const isUpdates = view === 'updates';
    managerSession.classList.toggle('active', isManager);
    dispatchSession.classList.toggle('active', isDispatch);
    salidasSession.classList.toggle('active', isSalidas);
    missingSession.classList.toggle('active', isMissing);
    fleetSession.classList.toggle('active', isFleet);
    sicovSession.classList.toggle('active', isSicov);
    externalVcSession.classList.toggle('active', isExternalVc);
    updatesSession.classList.toggle('active', isUpdates);
    sessionManagerBtn.classList.toggle('active', isManager);
    sessionDispatchBtn.classList.toggle('active', isDispatch);
    sessionSalidasBtn.classList.toggle('active', isSalidas);
    sessionMissingBtn.classList.toggle('active', isMissing);
    sessionFleetBtn.classList.toggle('active', isFleet);
    sessionSicovBtn.classList.toggle('active', isSicov);
    sessionExternalVcBtn.classList.toggle('active', isExternalVc);
    sessionUpdatesBtn.classList.toggle('active', isUpdates);

    if (isExternalVc) {
        loadExternalVehiculoConductores();
    }
}

function setDispatchAvailability() {
    const hasProfile = !!managerProfile;
    const hasActiveShift = !!activeShift;
    const enabled = hasProfile && hasActiveShift && managerAuthenticated && !shiftSessionMismatch;

    const shouldLockByInternet = false;
    const shouldLockByShift = !hasActiveShift;
    const shouldLock = shouldLockByInternet || shouldLockByShift;

    dispatchLockedNotice.style.display = shouldLock ? 'block' : 'none';
    dispatchFormCard.style.display = shouldLock ? 'none' : 'block';
    dispatchListCard.style.display = shouldLock ? 'none' : 'block';
    dispatchLockedText.textContent = 'Debes iniciar turno en la pestaña Control gestor para habilitar esta seccion.';
    manager.value = enabled ? (managerProfile.full_name || '') : '';
    submitDispatchBtn.disabled = !enabled || dispatchSubmitInFlight;

    if (!hasProfile) {
        managerStatus.textContent = 'No se pudo cargar tu identidad de usuario.';
        showManagerAlertOnce('need_identity', 'No se pudo identificar el gestor logueado. Vuelve a iniciar sesion.');
        return;
    }

    if (!managerAuthenticated) {
        managerStatus.textContent = 'Debes iniciar sesion para habilitar operaciones.';
        showManagerAlertOnce('need_validate', 'Debes iniciar sesion para continuar.');
        return;
    }

    if (shiftSessionMismatch) {
        managerStatus.textContent = 'Turno activo detectado en otra sesion. Cierra sesion y vuelve a entrar para recuperar control.';
        showManagerAlertOnce('shift_session_mismatch', 'Este turno activo pertenece a otra sesion. Cierra sesion y vuelve a ingresar.');
        return;
    }

    if (!hasActiveShift) {
        managerStatus.textContent = 'No tienes turno activo. Debes iniciar turno para generar despachos.';
        showManagerAlertOnce('need_shift', 'No has iniciado labores. Debes iniciar turno para generar despachos.');
        return;
    }

    const start = formatDateTime(activeShift.start_time);
    managerStatus.textContent = `Turno activo desde ${start}. Recuerda finalizar turno al terminar.`;
    lastManagerAlertKey = '';
}

function setDispatchSubmitLoading(isLoading) {
    if (!submitDispatchBtn.dataset.defaultText) {
        submitDispatchBtn.dataset.defaultText = submitDispatchBtn.textContent.trim() || 'Realizar Despacho';
    }
    submitDispatchBtn.textContent = isLoading ? 'Procesando...' : submitDispatchBtn.dataset.defaultText;
}

function renderNetworkBadge() {
    networkStatus.textContent = hasInternet ? 'En linea' : 'Sin internet';
    networkStatus.classList.toggle('online', hasInternet);
    networkStatus.classList.toggle('offline', !hasInternet);
}

function renderLiveClock24() {
    const now = new Date();
    const time = now.toLocaleTimeString('es-CO', {
        timeZone: 'America/Bogota',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });
    liveClock24.textContent = `Hora actual (24h): ${time}`;
}

function startLiveClock24() {
    if (liveClockTimer) {
        clearInterval(liveClockTimer);
        liveClockTimer = null;
    }
    renderLiveClock24();
    liveClockTimer = setInterval(renderLiveClock24, 1000);
}

function renderQuickRecentDispatches() {
    if (!dispatchesCache || dispatchesCache.length === 0) {
        quickRecentDispatchesList.innerHTML = '<p class="quick-empty">Sin datos recientes.</p>';
        return;
    }

    const latest = [...dispatchesCache]
        .sort((a, b) => {
            const aTs = getDispatchSortTimestamp(a);
            const bTs = getDispatchSortTimestamp(b);
            if (aTs !== bTs) return bTs - aTs;
            return Number(b.id || 0) - Number(a.id || 0);
        })
        .slice(0, 3);

    quickRecentDispatchesList.innerHTML = latest.map((row) => `
        <article class="quick-item">
            <p><b>${escapeHtml(formatTime24(row.hora_salida || row.departure_time))}</b> | ${escapeHtml(row.vehicle || '-')}</p>
            <p>${escapeHtml(row.route || '-')}</p>
        </article>
    `).join('');
}

function updateNetworkStatus(shouldAlert) {
    hasInternet = navigator.onLine !== false;
    renderNetworkBadge();

    if (!hasInternet && shouldAlert) {
        alert('Sin internet: los despachos nuevos se guardaran en local y se sincronizaran cuando vuelva la conexion.');
    }

    if (hasInternet && shouldAlert) {
        alert('Conexion restablecida. Se intentara sincronizar la cola local de despachos.');
    }

    setDispatchAvailability();
}

async function syncPendingDispatchQueue(showAlert = false) {
    if (!hasInternet || !currentUser) return;

    const queue = readPendingDispatchQueue();
    if (queue.length === 0) return;

    const remaining = [];
    let synced = 0;

    for (const item of queue) {
        const payload = item?.payload || null;
        if (!payload) continue;

        try {
            const { error } = await insertDispatchWithCreatedAtFallback(payload);
            if (error) {
                remaining.push(item);
                continue;
            }

            const appPayload = item?.appsScriptPayload || null;
            if (appPayload && appPayload.mId && appPayload.itinerary && appPayload.drvId) {
                try {
                    await sendDispatchToAppsScript(appPayload);
                } catch (err) {
                    // No bloquea sync de Supabase.
                }
            }

            synced += 1;
        } catch (err) {
            remaining.push(item);
        }
    }

    writePendingDispatchQueue(remaining);

    if (synced > 0) {
        await loadDispatches();
    }

    if (showAlert) {
        if (synced > 0 && remaining.length === 0) {
            alert(`Sincronizacion completada: ${synced} despacho(s) enviados.`);
        } else if (synced > 0 && remaining.length > 0) {
            alert(`Sincronizacion parcial: ${synced} enviados, ${remaining.length} pendientes.`);
        }
    }
}

function showManagerAlertOnce(key, message) {
    if (suppressManagerAlerts) return;
    if (lastManagerAlertKey === key) return;
    lastManagerAlertKey = key;
    alert(message);
}

function formatCoord(value) {
    if (value === null || value === undefined || value === '') return '-';
    const n = Number(value);
    return Number.isNaN(n) ? String(value) : n.toFixed(5);
}

function parseCoordPair(lat, lng) {
    const latNum = Number(lat);
    const lngNum = Number(lng);
    if (Number.isNaN(latNum) || Number.isNaN(lngNum)) return null;
    if (Math.abs(latNum) > 90 || Math.abs(lngNum) > 180) return null;
    return [latNum, lngNum];
}

function formatShiftWorkedTime(startTime, endTime) {
    if (!startTime) return '-';
    const startMs = new Date(startTime).getTime();
    const endMs = new Date(endTime || new Date().toISOString()).getTime();
    if (Number.isNaN(startMs) || Number.isNaN(endMs)) return '-';

    const diffMs = Math.max(0, endMs - startMs);
    const totalMinutes = Math.floor(diffMs / 60000);
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    return `${hours}h ${String(minutes).padStart(2, '0')}m`;
}

function getShiftTokenStorageKey() {
    if (!currentUser?.id) return SHIFT_TOKEN_STORAGE_PREFIX;
    return `${SHIFT_TOKEN_STORAGE_PREFIX}${currentUser.id}`;
}

function getStoredShiftToken() {
    try {
        return localStorage.getItem(getShiftTokenStorageKey()) || '';
    } catch (err) {
        return '';
    }
}

function setStoredShiftToken(token) {
    try {
        if (!token) return;
        localStorage.setItem(getShiftTokenStorageKey(), token);
    } catch (err) {
        // Ignorado: algunos navegadores bloquean localStorage.
    }
}

function clearStoredShiftToken() {
    try {
        localStorage.removeItem(getShiftTokenStorageKey());
    } catch (err) {
        // Ignorado: algunos navegadores bloquean localStorage.
    }
}

function generateSessionToken() {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
        return window.crypto.randomUUID();
    }
    return `sess_${Date.now()}_${Math.random().toString(36).slice(2, 12)}`;
}

function getLoginLockToken() {
    try {
        return localStorage.getItem(LOCK_TOKEN_KEY) || '';
    } catch (err) {
        return '';
    }
}

async function refreshLoginLock() {
    const token = getLoginLockToken();
    if (!currentUser?.id || !token) return;

    await sb
        .from(LOGIN_LOCK_TABLE)
        .update({
            active: true,
            last_seen_at: new Date().toISOString()
        })
        .eq('user_id', currentUser.id)
        .eq('session_token', token);
}

function startLoginLockHeartbeat() {
    if (loginLockHeartbeatTimer) {
        clearInterval(loginLockHeartbeatTimer);
        loginLockHeartbeatTimer = null;
    }

    loginLockHeartbeatTimer = setInterval(() => {
        refreshLoginLock().catch(() => {});
    }, LOCK_HEARTBEAT_MS);
}

async function releaseLoginLock() {
    const token = getLoginLockToken();
    if (!currentUser?.id || !token) return;

    await sb
        .from(LOGIN_LOCK_TABLE)
        .update({
            active: false,
            last_seen_at: new Date().toISOString()
        })
        .eq('user_id', currentUser.id)
        .eq('session_token', token);
}

function clearShiftMaps() {
    shiftMapInstances.forEach((map) => map.remove());
    shiftMapInstances = [];
}

function renderShiftMaps(rows, startIndex) {
    if (typeof L === 'undefined') return;
    clearShiftMaps();

    rows.forEach((row, localIndex) => {
        const mapId = `shiftMap-${startIndex + localIndex}`;
        const startPoint = parseCoordPair(row.start_lat, row.start_lng);
        const endPoint = parseCoordPair(row.end_lat, row.end_lng);
        if (!startPoint && !endPoint) return;

        const mapEl = document.getElementById(mapId);
        if (!mapEl) return;

        const map = L.map(mapEl, {
            zoomControl: false,
            attributionControl: false
        });

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 19
        }).addTo(map);

        const points = [];
        if (startPoint) {
            L.marker(startPoint).addTo(map).bindTooltip('Ingreso', { permanent: false });
            points.push(startPoint);
        }
        if (endPoint) {
            L.marker(endPoint).addTo(map).bindTooltip('Salida', { permanent: false });
            points.push(endPoint);
        }

        if (points.length === 2) {
            L.polyline(points, { color: '#2563eb', weight: 3 }).addTo(map);
            map.fitBounds(points, { padding: [16, 16] });
        } else {
            map.setView(points[0], 14);
        }

        shiftMapInstances.push(map);
    });
}

async function getCurrentPositionSafe() {
    if (!navigator.geolocation) return null;
    return new Promise((resolve) => {
        navigator.geolocation.getCurrentPosition(
            (pos) => resolve({ lat: pos.coords.latitude, lng: pos.coords.longitude }),
            () => resolve(null),
            { enableHighAccuracy: true, timeout: 10000, maximumAge: 300000 }
        );
    });
}

function resolveLoggedManagerProfile() {
    const metadata = currentUser?.user_metadata || {};
    const fullName = String(
        metadata.display_name || metadata.full_name || metadata.name || currentUser?.email || 'Gestor'
    ).trim();

    managerProfile = {
        user_id: currentUser?.id || '',
        full_name: fullName
    };
    managerAuthenticated = true;
    managerIdentity.textContent = `Gestor: ${fullName}`;
}

async function loadActiveShift() {
    const { data, error } = await sb
        .from('manager_shifts')
        .select('*')
        .eq('user_id', currentUser.id)
        .is('end_time', null)
        .order('start_time', { ascending: false })
        .limit(1);

    if (error) throw error;
    activeShift = (data && data[0]) ? data[0] : null;
    shiftSessionMismatch = false;

    if (!activeShift) {
        clearStoredShiftToken();
        return;
    }

    const shiftToken = String(activeShift.session_token || '').trim();
    if (!shiftToken) return;

    const storedToken = getStoredShiftToken();
    if (!storedToken) {
        setStoredShiftToken(shiftToken);
        return;
    }

    if (storedToken !== shiftToken) {
        shiftSessionMismatch = true;
    }
}

async function loadManagerShiftHistory() {
    const { data, error } = await sb
        .from('manager_shifts')
        .select('*')
        .eq('user_id', currentUser.id)
        .order('start_time', { ascending: false })
        .limit(50);

    if (error) throw error;

    managerShiftRows = data || [];
    managerShiftPage = 1;
    renderManagerShiftHistoryPage();
}

function renderManagerShiftHistoryPage() {
    const total = managerShiftRows.length;
    const totalPages = Math.max(1, Math.ceil(total / MANAGER_SHIFT_PAGE_SIZE));
    managerShiftPage = Math.min(totalPages, Math.max(1, managerShiftPage));

    if (total === 0) {
        clearShiftMaps();
        managerShiftHistory.innerHTML = '<div class="no-tasks">No hay registros de labores.</div>';
        shiftPageInfo.textContent = 'Pagina 0 de 0';
        shiftPrevBtn.disabled = true;
        shiftNextBtn.disabled = true;
        return;
    }

    const start = (managerShiftPage - 1) * MANAGER_SHIFT_PAGE_SIZE;
    const end = start + MANAGER_SHIFT_PAGE_SIZE;
    const pageRows = managerShiftRows.slice(start, end);

    managerShiftHistory.innerHTML = pageRows.map((row, idx) => {
        const started = formatDateTime(row.start_time);
        const ended = row.end_time ? formatDateTime(row.end_time) : '-';
        const status = row.end_time ? 'Finalizado' : 'Activo';
        const worked = formatShiftWorkedTime(row.start_time, row.end_time);
        return `
            <article class="shift-item ${row.end_time ? '' : 'shift-active'}">
                <p><b>Gestor:</b> ${escapeHtml(row.manager_name || managerProfile?.full_name || '-')}</p>
                <p><b>Estado:</b> ${escapeHtml(status)}</p>
                <p><b>Inicio:</b> ${escapeHtml(started)}</p>
                <p><b>Fin:</b> ${escapeHtml(ended)}</p>
                <p><b>Horas laboradas:</b> ${escapeHtml(worked)}${row.end_time ? '' : ' (en curso)'}</p>
                <p><b>Geo inicio:</b> ${escapeHtml(formatCoord(row.start_lat))}, ${escapeHtml(formatCoord(row.start_lng))}</p>
                <p><b>Geo fin:</b> ${escapeHtml(formatCoord(row.end_lat))}, ${escapeHtml(formatCoord(row.end_lng))}</p>
                <div class="shift-map-wrap">
                    <div id="shiftMap-${start + idx}" class="shift-map"></div>
                </div>
            </article>
        `;
    }).join('');

    shiftPageInfo.textContent = `Pagina ${managerShiftPage} de ${totalPages}`;
    shiftPrevBtn.disabled = managerShiftPage <= 1;
    shiftNextBtn.disabled = managerShiftPage >= totalPages;
    renderShiftMaps(pageRows, start);
}

async function startManagerShift() {
    if (managerShiftActionInFlight) return;
    managerShiftActionInFlight = true;
    startShiftBtn.disabled = true;
    endShiftBtn.disabled = true;
    try {
    const confirmStart = await openDispatchConfirmModal('¿Deseas iniciar turno ahora?', 'Si, iniciar turno');
    if (!confirmStart) return;

    if (!managerProfile) {
        alert('No se pudo identificar el gestor logueado.');
        return;
    }

    if (!managerAuthenticated) {
        alert('Debes iniciar sesion para iniciar turno.');
        return;
    }

    if (activeShift) {
        alert('Ya tienes un turno activo. Finaliza la salida para iniciar uno nuevo.');
        return;
    }

    const geo = await getCurrentPositionSafe();
    const sessionToken = generateSessionToken();
    let { error } = await sb.from('manager_shifts').insert([
        {
            user_id: currentUser.id,
            manager_name: managerProfile.full_name,
            session_token: sessionToken,
            start_time: new Date().toISOString(),
            start_lat: geo?.lat ?? null,
            start_lng: geo?.lng ?? null
        }
    ]);

    if (error) {
        const msg = String(error.message || '').toLowerCase();
        const missingSecureColumns = (msg.includes('session_token') || msg.includes('manager_profile_id')) &&
            (msg.includes('column') || msg.includes('schema cache'));
        if (missingSecureColumns) {
            const fallback = await sb.from('manager_shifts').insert([
                {
                    user_id: currentUser.id,
                    manager_name: managerProfile.full_name,
                    start_time: new Date().toISOString(),
                    start_lat: geo?.lat ?? null,
                    start_lng: geo?.lng ?? null
                }
            ]);
            error = fallback.error || null;
            if (!error) {
                alert('Turno iniciado en modo compatible.');
            }
        }
    }

    if (error) {
        alert(error.message);
        return;
    }

    await loadActiveShift();
    if (activeShift?.session_token) setStoredShiftToken(String(activeShift.session_token));
    setDispatchAvailability();
    alert('Turno iniciado correctamente.');
    await loadManagerShiftHistory();
    } finally {
        managerShiftActionInFlight = false;
        startShiftBtn.disabled = false;
        endShiftBtn.disabled = false;
    }
}

async function endManagerShift() {
    if (managerShiftActionInFlight) return;
    managerShiftActionInFlight = true;
    startShiftBtn.disabled = true;
    endShiftBtn.disabled = true;
    try {
    const confirmEnd = await openDispatchConfirmModal('¿Deseas finalizar turno ahora?', 'Si, finalizar turno');
    if (!confirmEnd) return;

    if (!activeShift) {
        alert('No tienes un turno activo para finalizar.');
        return;
    }

    const geo = await getCurrentPositionSafe();
    const { error } = await sb
        .from('manager_shifts')
        .update({
            end_time: new Date().toISOString(),
            end_lat: geo?.lat ?? null,
            end_lng: geo?.lng ?? null
        })
        .eq('id', activeShift.id)
        .eq('user_id', currentUser.id);

    if (error) {
        alert(error.message);
        return;
    }

    await loadActiveShift();
    shiftSessionMismatch = false;
    clearStoredShiftToken();
    managerAuthenticated = true;
    setDispatchAvailability();
    alert('Turno finalizado correctamente.');
    await loadManagerShiftHistory();
    } finally {
        managerShiftActionInFlight = false;
        startShiftBtn.disabled = false;
        endShiftBtn.disabled = false;
    }
}

function validateDispatchForm() {
    if (!vehicle.value) return 'Selecciona un vehiculo.';
    if (!driver.value) return 'Selecciona un conductor.';
    if (!/^\d{2}\/\d{2}\/\d{4}$/.test(String(departureDate.value || '').trim())) return 'Ingresa la fecha en formato DD/MM/YYYY.';
    if (!departureTime.value) return 'Selecciona la hora de salida.';
    if (!routeInput.value.trim()) return 'Selecciona un itinerario.';
    return '';
}

function loadItineraries() {
    routeInput.innerHTML = '<option value="">Selecciona itinerario</option>';

    const filtered = selectedItineraryGroup
        ? ITINERARIES.filter((itinerary) => String(itinerary.grupo || '').trim() === selectedItineraryGroup)
        : ITINERARIES;

    filtered.forEach((itinerary) => {
        const label = `${itinerary.id} | ${itinerary.grupo} | ${itinerary.nombre}`;
        const option = document.createElement('option');
        option.value = itinerary.nombre;
        option.textContent = label;
        routeInput.appendChild(option);
    });

    renderRouteSelectedMeta();
}

function loadItineraryGroups() {
    const current = managerItineraryGroup.value;
    const groups = [...new Set(ITINERARIES.map((it) => String(it.grupo || '').trim()).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b));

    managerItineraryGroup.innerHTML = '<option value="">Todos los grupos</option>';
    groups.forEach((group) => {
        const option = document.createElement('option');
        option.value = group;
        option.textContent = group;
        managerItineraryGroup.appendChild(option);
    });

    if (current && groups.includes(current)) {
        managerItineraryGroup.value = current;
    }
}

function loadVehicles(catalog) {
    vehicle.innerHTML = '<option value="">Selecciona vehiculo</option>';

    catalog.forEach((item) => {
        const placa = item.placa || '';
        const id = item.id || '';
        const descripcion = item.descripcion || '';
        const label = `${placa} | ${id} | ${descripcion}`;

        const option = document.createElement('option');
        option.value = descripcion || placa;
        option.textContent = label;
        vehicle.appendChild(option);
    });

    renderVehicleSelectedMeta();
}

function renderVehicleSelectedMeta() {
    const selected = getSelectedVehicleRecord();
    if (!selected) {
        vehicleSelectedMeta.textContent = 'ID vehiculo: -';
        return;
    }
    vehicleSelectedMeta.textContent = `ID vehiculo: ${selected.id || '-'} | Placa: ${selected.placa || '-'} | Interno: ${selected.descripcion || '-'}`;
}

function getSelectedVehicleRecord() {
    return vehiclesCatalog.find((item) => String(item.descripcion || item.placa || '').trim() === String(vehicle.value || '').trim()) || null;
}

function getDispatchSortTime(dispatch) {
    const hora = String(dispatch.hora_salida || '').trim();
    if (/^\d{2}:\d{2}(:\d{2})?$/.test(hora)) return hora.slice(0, 5);
    return formatTime24(dispatch.departure_time);
}

function getDispatchSortTimestamp(dispatch) {
    const dateKey = getStoredDateKey(dispatch);
    const timeText = getDispatchSortTime(dispatch);

    const dateMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(dateKey || ''));
    const timeMatch = /^(\d{2}):(\d{2})$/.exec(String(timeText || ''));

    if (dateMatch && timeMatch) {
        const year = Number(dateMatch[1]);
        const month = Number(dateMatch[2]);
        const day = Number(dateMatch[3]);
        const hour = Number(timeMatch[1]);
        const minute = Number(timeMatch[2]);
        return new Date(year, month - 1, day, hour, minute, 0, 0).getTime();
    }

    const fallback = new Date(dispatch.departure_time || dispatch.created_at || 0).getTime();
    return Number.isNaN(fallback) ? 0 : fallback;
}

function matchesDispatchDate(dispatch, selectedDate) {
    const selected = String(selectedDate || '').trim();
    if (!selected) return true;

    const rawDeparture = String(dispatch.departure_time || '').trim();
    const rawDepartureIso = rawDeparture.match(/^(\d{4}-\d{2}-\d{2})/)?.[1] || '';
    let rawDepartureBogota = '';
    if (rawDeparture) {
        const date = new Date(rawDeparture);
        if (!Number.isNaN(date.getTime())) {
            const parts = new Intl.DateTimeFormat('en-CA', {
                timeZone: 'America/Bogota',
                year: 'numeric',
                month: '2-digit',
                day: '2-digit'
            }).formatToParts(date);
            const y = parts.find((p) => p.type === 'year')?.value || '';
            const m = parts.find((p) => p.type === 'month')?.value || '';
            const d = parts.find((p) => p.type === 'day')?.value || '';
            rawDepartureBogota = `${y}-${m}-${d}`;
        }
    }

    const candidates = new Set([
        rawDepartureIso,
        rawDepartureBogota,
        getStoredDateKey({ departure_time: rawDeparture }),
        getDispatchDateKey({ departure_time: rawDeparture })
    ].filter(Boolean));

    return candidates.has(selected);
}

function canCurrentManagerEditDispatch(dispatch) {
    const currentManager = normalizeText(managerProfile?.full_name || '');
    const dispatchManager = normalizeText(dispatch?.manager || '');
    return !!currentManager && !!dispatchManager && currentManager === dispatchManager;
}


function renderItineraryFilter(data) {
    const currentValue = itineraryFilter.value;
    const itineraries = [...new Set(data.map((item) => String(item.route || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b));

    itineraryFilter.innerHTML = '<option value="">Todos los itinerarios</option>';
    itineraries.forEach((name) => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        itineraryFilter.appendChild(option);
    });

    if (currentValue && itineraries.includes(currentValue)) {
        itineraryFilter.value = currentValue;
    }
}

function renderDispatchFilters(data) {
    const currentItinerary = dispatchItineraryFilter.value;

    const itineraries = [...new Set(data.map((item) => String(item.route || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b));
    dispatchItineraryFilter.innerHTML = '<option value="">Todos los itinerarios</option>';
    itineraries.forEach((name) => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        dispatchItineraryFilter.appendChild(option);
    });
    if (currentItinerary && itineraries.includes(currentItinerary)) {
        dispatchItineraryFilter.value = currentItinerary;
    }
}

function renderMissingItineraryFilter(data) {
    const current = missingItineraryFilter.value;
    const itineraries = [...new Set(data.map((item) => String(item.route || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b));
    missingItineraryFilter.innerHTML = '<option value="">Todos los itinerarios</option>';
    itineraries.forEach((name) => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        missingItineraryFilter.appendChild(option);
    });
    if (current && itineraries.includes(current)) {
        missingItineraryFilter.value = current;
    }
}

function loadDrivers(catalog) {
    driver.innerHTML = '<option value="">Selecciona conductor</option>';

    const enabledCatalog = catalog.filter((item) => {
        const statusText = normalizeText(item.status);
        return statusText !== 'disabled' && statusText !== 'inactivo' && statusText !== 'desvinculado';
    });

    const activeFirst = [
        ...enabledCatalog.filter((item) => isActiveDriverStatus(item.status)),
        ...enabledCatalog.filter((item) => !isActiveDriverStatus(item.status))
    ];

    activeFirst.forEach((item) => {
        const label = `${item.nombre} (${item.cedula || 'sin cedula'})`;
        const option = document.createElement('option');
        option.value = item.nombre;
        option.textContent = label;
        driver.appendChild(option);
    });

    renderDriverSelectedMeta();
}

function renderDriverSelectedMeta() {
    const selected = getSelectedDriverRecord();
    if (!selected) {
        driverSelectedMeta.textContent = 'ID conductor: -';
        return;
    }
    driverSelectedMeta.textContent = `ID conductor: ${selected.dr_id || '-'} | Cedula: ${selected.cedula || '-'} | Nombre: ${selected.nombre || '-'}`;
}

function getSelectedDriverRecord() {
    return driversCatalog.find((item) => normalizeText(item.nombre) === normalizeText(driver.value)) || null;
}

function renderDriverInfo() {
    const selected = driversCatalog.find((item) => normalizeText(item.nombre) === normalizeText(driver.value));

    if (!selected) {
        driverInfo.textContent = 'Selecciona un conductor de la lista.';
        renderDriverSelectedMeta();
        return;
    }

    driverInfo.textContent = `Cedula: ${selected.cedula || '-'} | Fleet: ${selected.fleet || '-'} | Celular: ${selected.celular || '-'} | Email: ${selected.email || '-'} | Status: ${selected.status || '-'}`;
    renderDriverSelectedMeta();

    if (selected.fleet) {
        const fleetValue = normalizeText(selected.fleet);
        const fleetOption = Array.from(vehicle.options).find((opt) => normalizeText(opt.value).includes(fleetValue));
        if (fleetOption) {
            vehicle.value = fleetOption.value;
            renderVehicleDocsStatus();
        }
    }
}

function handleVehicleChange() {
    const currentVehicleKey = normalizeVehicleKey(vehicle.value);
    const vehicleChanged = currentVehicleKey !== lastVehicleSelectionKey;
    lastVehicleSelectionKey = currentVehicleKey;

    if (!qrSelectionInProgress) {
        dispatchEntryMethod = 'manual';
    }

    if (vehicleChanged) {
        driver.value = '';
        driverInfo.textContent = 'Selecciona un conductor de la lista.';
        renderDriverSelectedMeta();
    }

    renderVehicleSelectedMeta();
    renderVehicleDocsStatus();
    autoFillDriverFromExternalByVehicle();
}

function renderRouteSelectedMeta() {
    const selected = getSelectedItineraryRecord();
    if (!selected) {
        routeSelectedMeta.textContent = 'ID itinerario: -';
        return;
    }
    routeSelectedMeta.textContent = `ID itinerario: ${selected.id || '-'} | Grupo: ${selected.grupo || '-'} | Ruta: ${selected.nombre || '-'}`;
}

function getSelectedItineraryRecord() {
    return ITINERARIES.find((item) => String(item.nombre || '').trim() === String(routeInput.value || '').trim()) || null;
}

async function sendDispatchToAppsScript(payload) {
    const url = String(APPS_SCRIPT_URL || '').trim();
    if (!url) return { skipped: true, reason: 'missing_url' };

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain;charset=utf-8' },
            body: JSON.stringify(payload)
        });

        const text = await response.text();
        let parsed = null;
        try {
            parsed = JSON.parse(text);
        } catch (err) {
            parsed = null;
        }

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${text.slice(0, 250)}`);
        }

        return parsed || { success: true, message: 'Respuesta recibida' };
    } catch (err) {
        if (String(err?.message || '').toLowerCase().includes('failed to fetch')) {
            await fetch(url, {
                method: 'POST',
                mode: 'no-cors',
                headers: { 'Content-Type': 'text/plain;charset=utf-8' },
                body: JSON.stringify(payload)
            });
            return { success: true, message: 'Enviado sin confirmacion (no-cors)' };
        }
        throw err;
    }
}

function getSelectedVehicleNumber() {
    const option = vehicle.options[vehicle.selectedIndex] || null;
    const candidates = [String(vehicle.value || ''), String(option?.textContent || '')];
    for (const text of candidates) {
        const matches = text.match(/\d{3,5}/g);
        if (!matches || matches.length === 0) continue;
        const last = Number(matches[matches.length - 1]);
        if (Number.isInteger(last)) return last;
    }
    return null;
}

async function findExternalDriverByVehicle(vehicleNumber) {
    if (!sbExternal || !Number.isInteger(vehicleNumber)) return null;

    const cached = externalVcRows
        .filter((row) => Number(String(row.vehiculo || '').trim()) === vehicleNumber)
        .sort((a, b) => {
            const aTs = new Date(a.created_at || 0).getTime();
            const bTs = new Date(b.created_at || 0).getTime();
            return bTs - aTs;
        })[0] || null;

    if (cached) {
        // Refresco puntual en segundo plano para este vehiculo sin bloquear el flujo.
        refreshExternalVehiculoConductoresByVehicle(vehicleNumber).catch(() => {});
        return cached;
    }

    return refreshExternalVehiculoConductoresByVehicle(vehicleNumber);
}

function mergeExternalVehiculoConductoresRows(rows) {
    if (!rows || rows.length === 0) return;

    const incomingHeaders = Object.keys(rows[0] || {});
    if (externalVcHeaders.length === 0) {
        externalVcHeaders = [...incomingHeaders];
    } else {
        incomingHeaders.forEach((h) => {
            if (!externalVcHeaders.includes(h)) externalVcHeaders.push(h);
        });
    }

    const mapKey = (row) => String(
        row.turno_id ||
        `${row.vehiculo || ''}|${row.cedula || ''}|${row.fecha_ingreso || ''}|${row.hora_ingreso || ''}|${row.created_at || ''}`
    );

    const existingMap = new Map(
        externalVcRows.map((row) => [mapKey(row), row])
    );

    rows.forEach((raw) => {
        const mapped = {};
        externalVcHeaders.forEach((header) => {
            mapped[header] = raw[header] === null || raw[header] === undefined ? '' : String(raw[header]);
        });
        existingMap.set(mapKey(mapped), mapped);
    });

    externalVcRows = Array.from(existingMap.values()).sort((a, b) => {
        const aTs = new Date(a.created_at || 0).getTime();
        const bTs = new Date(b.created_at || 0).getTime();
        return bTs - aTs;
    });

    externalVcStatus.textContent = `${externalVcRows.length} registros disponibles.`;
    if (externalVcSession.classList.contains('active')) {
        renderExternalVcRows();
    }
}

async function refreshExternalVehiculoConductoresByVehicle(vehicleNumber) {
    if (!sbExternal || !Number.isInteger(vehicleNumber)) return null;
    const tableVehiculoConductores = String(EXT_VEHICULO_CONDUCTORES_TABLE || 'vehiculo_conductores').trim();
    const { data, error } = await sbExternal
        .from(tableVehiculoConductores)
        .select('*')
        .eq('vehiculo', vehicleNumber)
        .order('created_at', { ascending: false })
        .limit(5);

    if (error) throw error;
    const rows = data || [];
    if (rows.length === 0) return null;

    mergeExternalVehiculoConductoresRows(rows);

    return rows[0];
}

function selectDriverFromCsvByExternalRow(row) {
    const byCedula = driversCatalog.find((d) => normalizeText(d.cedula) === normalizeText(row.cedula));
    if (byCedula) {
        driver.value = byCedula.nombre;
        renderDriverInfo();
        return true;
    }

    const byName = driversCatalog.find((d) => normalizeText(d.nombre) === normalizeText(row.nombre));
    if (byName) {
        driver.value = byName.nombre;
        renderDriverInfo();
        return true;
    }

    return false;
}

async function autoFillDriverFromExternalByVehicle() {
    const requestId = ++vehicleDriverLookupSeq;

    if (!sbExternal) return;
    const vehicleNumber = getSelectedVehicleNumber();
    if (!Number.isInteger(vehicleNumber)) return;
    const hasDriverSelected = String(driver.value || '').trim() !== '';

    try {
        const externalRow = await findExternalDriverByVehicle(vehicleNumber);
        if (requestId !== vehicleDriverLookupSeq) return;

        if (externalRow) {
            if (hasDriverSelected) return;
            const matched = selectDriverFromCsvByExternalRow(externalRow);
            if (matched) {
                alert(`Conductor autocompletado desde biometrico para vehiculo ${vehicleNumber}.`);
                return;
            }

            alert(`Se encontro conductor en biometrico (${externalRow.nombre}), pero no esta en la lista de conductores.`);
            return;
        }

        if (hasDriverSelected) return;

        const goManual = confirm(
            `No se encontro conductor en biometrico para el vehiculo ${vehicleNumber}. ¿Deseas registrarlo manualmente desde la lista de conductores?`
        );
        if (!goManual) return;
        driver.value = '';
        driver.focus();
        driverInfo.textContent = 'Selecciona manualmente el conductor desde la lista.';
    } catch (err) {
        if (requestId !== vehicleDriverLookupSeq) return;
        alert(`No se pudo consultar biometrico por vehiculo. (${err.message})`);
    }
}

async function loadVehiclesFromSheet() {
    try {
        const response = await fetch(buildNoCacheUrl(VEHICLES_CSV_URL), { cache: 'no-store' });
        if (!response.ok) throw new Error('No se pudo obtener vehiculos.');

        const csvText = await response.text();
        const rows = parseCSV(csvText);
        if (rows.length < 2) throw new Error('No hay datos de vehiculos.');

        const headers = rows[0].map((h) => normalizeText(h));
        const data = rows.slice(1).map((cols) => {
            const item = {};
            headers.forEach((key, idx) => {
                item[key] = String(cols[idx] || '').trim();
            });
            return item;
        });

        vehiclesCatalog = data
            .map((item) => ({
                placa: item.placa || '',
                id: item.id || '',
                descripcion: item.descripcion || item['descripcion'] || item['descripción'] || ''
            }))
            .filter((item) => item.placa || item.id || item.descripcion);

        loadVehicles(vehiclesCatalog);
        vehicle.value = '';
        renderVehicleSelectedMeta();
        renderVehicleDocsStatus();
        vehicleInfo.textContent = `${vehiclesCatalog.length} vehiculos disponibles.`;
    } catch (err) {
        vehiclesCatalog = [];
        loadVehicles([]);
        vehicleInfo.textContent = `No se pudieron cargar vehiculos. (${err.message})`;
    }
}

async function loadDriversFromSheet() {
    try {
        const response = await fetch(buildNoCacheUrl(DRIVERS_CSV_URL), { cache: 'no-store' });
        if (!response.ok) throw new Error('No se pudo obtener conductores.');

        const csvText = await response.text();
        const rows = parseCSV(csvText);
        if (rows.length < 2) throw new Error('No hay datos de conductores.');

        const headers = rows[0].map((h) => normalizeText(h));
        const data = rows.slice(1).map((cols) => {
            const item = {};
            headers.forEach((key, idx) => {
                item[key] = String(cols[idx] || '').trim();
            });
            return item;
        });

        driversCatalog = data.filter((item) => {
            if (!item.nombre) return false;
            const statusText = normalizeText(item.status);
            return statusText !== 'disabled' && statusText !== 'inactivo' && statusText !== 'desvinculado';
        });
        loadDrivers(driversCatalog);
        driverInfo.textContent = `${driversCatalog.length} conductores disponibles.`;
    } catch (err) {
        driversCatalog = [];
        loadDrivers([]);
        driverInfo.textContent = `No se pudieron cargar conductores. (${err.message})`;
    }
}

function renderFleet() {
    if (fleetRows.length === 0 || fleetHeaders.length === 0) {
        fleetList.innerHTML = '<div class="no-tasks">No hay datos de parque automotor.</div>';
        return;
    }

    fleetList.innerHTML = `
        <div class="data-table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        ${fleetHeaders.map((header) => `<th>${escapeHtml(header)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
                    ${fleetRows.map((row) => `
                        <tr>
                            ${fleetHeaders.map((header) => `<td>${escapeHtml(row[header] || '-')}</td>`).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
}

async function loadFleetFromSheet() {
    try {
        const response = await fetch(buildNoCacheUrl(FLEET_CSV_URL), { cache: 'no-store' });
        if (!response.ok) throw new Error('No se pudo obtener el parque automotor.');

        const csvText = await response.text();
        const rows = parseCSV(csvText);
        if (rows.length < 2) throw new Error('No hay datos del parque automotor.');

        fleetHeaders = rows[0].map((h) => String(h || '').replaceAll('\uFEFF', '').trim()).filter(Boolean);
        const allRows = rows.slice(1).map((cols) => {
            const item = {};
            fleetHeaders.forEach((header, idx) => {
                item[header] = String(cols[idx] || '').trim();
            });
            return item;
        }).filter((item) => Object.values(item).some((v) => String(v).trim() !== ''));

        fleetRecords = allRows.map((row) => ({
            raw: row,
            interno: getFieldByKeywords(row, ['interno']),
            estado: getFieldByKeywords(row, ['estado']),
            soat: getFieldByKeywords(row, ['soat']),
            tecnomecanica: getFieldByKeywords(row, ['tecnomecanica']),
            operacion: getFieldByKeywords(row, ['operacion'])
        }));

        const vinculados = fleetRecords.filter((r) => isVinculadoStatus(r.estado));
        fleetRows = vinculados.map((r) => r.raw);
        fleetRecords = vinculados;

        fleetStatus.textContent = `${fleetRows.length} registros vinculados disponibles.`;
        renderFleet();
        renderVehicleDocsStatus();
    } catch (err) {
        fleetHeaders = [];
        fleetRows = [];
        fleetRecords = [];
        fleetStatus.textContent = `No se pudo cargar el parque automotor. (${err.message})`;
        renderFleet();
        renderVehicleDocsStatus();
    }
}

function renderSicovRows() {
    if (sicovRows.length === 0 || sicovHeaders.length === 0) {
        sicovList.innerHTML = '<div class="no-tasks">No hay datos de preoperacionales SICOV.</div>';
        return;
    }

    const query = normalizeText(sicovFilter.value);
    const filtered = query
        ? sicovRows.filter((row) => Object.values(row).some((value) => normalizeText(value).includes(query)))
        : sicovRows;

    if (filtered.length === 0) {
        sicovList.innerHTML = '<div class="no-tasks">No hay coincidencias para ese filtro.</div>';
        return;
    }

    sicovList.innerHTML = `
        <div class="data-table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        ${sicovHeaders.map((header) => `<th>${escapeHtml(header)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
                    ${filtered.map((row) => `
                        <tr>
                            ${sicovHeaders.map((header) => `<td>${escapeHtml(row[header] || '-')}</td>`).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
}

async function loadSicovFromSheet() {
    try {
        const response = await fetch(buildNoCacheUrl(SICOV_CSV_URL), { cache: 'no-store' });
        if (!response.ok) throw new Error('No se pudo obtener SICOV.');

        const csvText = await response.text();
        const rows = parseCSV(csvText);
        if (rows.length < 2) throw new Error('No hay datos de SICOV.');

        sicovHeaders = rows[0].map((h) => String(h || '').replaceAll('\uFEFF', '').trim()).filter(Boolean);
        sicovRows = rows.slice(1).map((cols) => {
            const item = {};
            sicovHeaders.forEach((header, idx) => {
                item[header] = String(cols[idx] || '').trim();
            });
            return item;
        }).filter((item) => Object.values(item).some((v) => String(v).trim() !== ''));

        sicovStatus.textContent = `${sicovRows.length} registros SICOV disponibles.`;
        renderSicovRows();
    } catch (err) {
        sicovHeaders = [];
        sicovRows = [];
        sicovStatus.textContent = `No se pudo cargar SICOV. (${err.message})`;
        renderSicovRows();
    }
}

function renderExternalVcRows() {
    if (externalVcRows.length === 0 || externalVcHeaders.length === 0) {
        externalVcList.innerHTML = '<div class="no-tasks">No hay datos en vehiculo_conductores.</div>';
        return;
    }

    const query = normalizeText(externalVcFilter.value);
    const filtered = query
        ? externalVcRows.filter((row) => Object.values(row).some((value) => normalizeText(value).includes(query)))
        : externalVcRows;

    if (filtered.length === 0) {
        externalVcList.innerHTML = '<div class="no-tasks">No hay coincidencias para ese filtro.</div>';
        return;
    }

    externalVcList.innerHTML = `
        <div class="fleet-grid">
            ${filtered.map((row) => `
                <article class="fleet-item">
                    ${externalVcHeaders.map((header) => `
                        <p><b>${escapeHtml(header)}:</b> ${escapeHtml(row[header] || '-')}</p>
                    `).join('')}
                </article>
            `).join('')}
        </div>
    `;
}

async function loadExternalVehiculoConductores() {
    if (externalVcLoading) return;
    externalVcLoading = true;
    if (!sbExternal) {
        externalVcHeaders = [];
        externalVcRows = [];
        externalVcStatus.textContent = 'No hay configuracion disponible para esta consulta.';
        renderExternalVcRows();
        externalVcLoading = false;
        return;
    }

    try {
        const tableVehiculoConductores = String(EXT_VEHICULO_CONDUCTORES_TABLE || 'vehiculo_conductores').trim();
        const { data, error } = await sbExternal
            .from(tableVehiculoConductores)
            .select('*')
            .order('created_at', { ascending: false });

        if (error) throw error;

        const rows = data || [];
        if (rows.length === 0) {
            externalVcHeaders = [];
            externalVcRows = [];
            externalVcStatus.textContent = 'Sin registros en vehiculo_conductores.';
            renderExternalVcRows();
            return;
        }

        externalVcHeaders = Object.keys(rows[0]);
        externalVcRows = rows.map((row) => {
            const mapped = {};
            externalVcHeaders.forEach((header) => {
                mapped[header] = row[header] === null || row[header] === undefined ? '' : String(row[header]);
            });
            return mapped;
        });

        externalVcStatus.textContent = `${externalVcRows.length} registros disponibles.`;
        renderExternalVcRows();
    } catch (err) {
        externalVcHeaders = [];
        externalVcRows = [];
        externalVcStatus.textContent = `No se pudo cargar vehiculo_conductores. (${err.message})`;
        renderExternalVcRows();
    } finally {
        externalVcLoading = false;
    }
}

function startExternalVcAutoRefresh() {
    if (externalVcAutoRefreshTimer) {
        clearInterval(externalVcAutoRefreshTimer);
        externalVcAutoRefreshTimer = null;
    }

    // Refresco suave de biometrico externo para mantener datos al dia.
    externalVcAutoRefreshTimer = setInterval(() => {
        if (document.hidden) return;
        if (!sbExternal) return;
        loadExternalVehiculoConductores();
    }, 90000);
}

function setCsvButtonsDisabled(disabled) {
    refreshAllCsvBtn.disabled = disabled;
    refreshVehiclesCsvBtn.disabled = disabled;
    refreshDriversCsvBtn.disabled = disabled;
    refreshFleetCsvBtn.disabled = disabled;
    refreshSicovCsvBtn.disabled = disabled;
    refreshExternalVcBtn.disabled = disabled;
}

async function refreshCsv(kind) {
    setCsvButtonsDisabled(true);
    try {
        if (kind === 'all') {
            await Promise.all([
                loadVehiclesFromSheet(),
                loadDriversFromSheet(),
                loadFleetFromSheet(),
                loadSicovFromSheet(),
                loadExternalVehiculoConductores()
            ]);
            alert('Actualizacion finalizada.');
            return;
        }

        if (kind === 'vehicles') {
            await loadVehiclesFromSheet();
            alert('Vehiculos actualizados.');
            return;
        }

        if (kind === 'drivers') {
            await loadDriversFromSheet();
            alert('Conductores actualizados.');
            return;
        }

        if (kind === 'fleet') {
            await loadFleetFromSheet();
            alert('Parque automotor actualizado.');
            return;
        }

        if (kind === 'sicov') {
            await loadSicovFromSheet();
            alert('SICOV actualizado.');
            return;
        }

        if (kind === 'external-vc') {
            await loadExternalVehiculoConductores();
            alert('Vehiculo-Conductores actualizado.');
        }
    } finally {
        setCsvButtonsDisabled(false);
    }
}

async function init() {
    const { data: { user }, error } = await sb.auth.getUser();

    if (error || !user) {
        location.href = 'index.html';
        return;
    }

    currentUser = user;
    userEmail.textContent = user.email;
    renderNetworkBadge();
    await refreshLoginLock();
    startLoginLockHeartbeat();
    startLiveClock24();

    setAutomaticDate();
    setSessionView('dispatch');
    loadItineraryGroups();
    loadItineraries();
    dispatchDateFilter.value = getCurrentDateKeyBogota();
    dispatchManagerFilter.value = '';
    dispatchItineraryFilter.value = '';
    try {
        resolveLoggedManagerProfile();
        await loadActiveShift();
        if (activeShift) {
            managerAuthenticated = !shiftSessionMismatch;
            if (!shiftSessionMismatch && activeShift.session_token) {
                setStoredShiftToken(String(activeShift.session_token));
            }
        }
        suppressManagerAlerts = true;
        await loadManagerShiftHistory();
        setDispatchAvailability();
        suppressManagerAlerts = false;
    } catch (managerErr) {
        suppressManagerAlerts = false;
        managerStatus.textContent = `Configura tablas de gestor en Supabase. (${managerErr.message})`;
        managerShiftRows = [];
        renderManagerShiftHistoryPage();
        managerShiftHistory.innerHTML = '<div class="no-tasks">No se pudo cargar el historial de labores.</div>';
        submitDispatchBtn.disabled = true;
    }
    await loadVehiclesFromSheet();
    await loadDriversFromSheet();
    await loadFleetFromSheet();
    await loadSicovFromSheet();
    await loadExternalVehiculoConductores();
    startExternalVcAutoRefresh();
    await loadDispatches();
    await syncPendingDispatchQueue(false);
}

async function fetchAllDispatchesByDate(dateKey) {
    const range = getDateRangeIsoBogota(dateKey);
    if (!range) return { rows: [], count: 0 };

    const columns = 'id,user_id,vehicle,departure_time,hora_salida,route,driver,passenger_count,manager,notes,is_canceled,cancellation_note,created_at';
    const pageSize = 1000;
    let from = 0;
    const rows = [];
    let totalCount = 0;

    while (true) {
        const to = from + pageSize - 1;
        const { data, error, count } = await sb
            .from('dispatches')
            .select(columns, { count: from === 0 ? 'exact' : null })
            .gte('departure_time', range.start)
            .lt('departure_time', range.end)
            .order('id', { ascending: false })
            .range(from, to);

        if (error) throw error;
        if (from === 0 && typeof count === 'number') totalCount = count;
        if (!data || data.length === 0) break;

        rows.push(...data);
        if (data.length < pageSize) break;
        from += pageSize;
    }

    return { rows, count: totalCount || rows.length };
}

async function loadDispatches() {
    const activeDate = getActiveDispatchDateKey();
    dispatchDateFilter.value = activeDate;

    let data = [];
    try {
        const result = await fetchAllDispatchesByDate(activeDate);
        data = result.rows;
    } catch (err) {
        alert(err.message);
        return;
    }

    dispatchesList.innerHTML = '';
    salidasList.innerHTML = '';

    if (!data || data.length === 0) {
        dispatchesCache = [];
        renderItineraryFilter([]);
        renderDispatchFilters([]);
        renderMissingItineraryFilter([]);
        renderQuickRecentDispatches();
        dispatchesList.innerHTML = '<div class="no-tasks">No hay despachos registrados.</div>';
        salidasList.innerHTML = '<div class="no-tasks">No hay salidas disponibles.</div>';
        missingPassengerList.innerHTML = '<div class="no-tasks">No hay viajes sin pasajeros.</div>';
        return;
    }

    dispatchesCache = [...data];
    dispatchPage = 1;
    renderItineraryFilter(dispatchesCache);
    renderDispatchFilters(dispatchesCache);
    renderMissingItineraryFilter(dispatchesCache);
    renderDispatches();
    renderMissingPassengers();
    renderSalidas();
    renderQuickRecentDispatches();
}

function renderDispatches() {
    if (dispatchesCache.length === 0) {
        dispatchesList.innerHTML = '<div class="no-tasks">No hay despachos registrados.</div>';
        passengerAlert.style.display = 'none';
        missingPassengerList.innerHTML = '<div class="no-tasks">No hay viajes sin pasajeros.</div>';
        dispatchPageInfo.textContent = 'Pagina 0 de 0';
        dispatchPrevBtn.disabled = true;
        dispatchNextBtn.disabled = true;
        return;
    }

    const filtered = dispatchesCache.filter((item) => {
        const byDate = matchesDispatchDate(item, getActiveDispatchDateKey());
        const managerQuery = normalizeText(dispatchManagerFilter.value);
        const managerText = normalizeText(item.manager || item.user_id || '');
        const byManager = managerQuery ? managerText.includes(managerQuery) : true;
        const byItinerary = dispatchItineraryFilter.value ? String(item.route || '').trim() === dispatchItineraryFilter.value : true;
        return byDate && byManager && byItinerary;
    });

    if (filtered.length === 0) {
        dispatchesList.innerHTML = '<div class="no-tasks">No hay despachos con esos filtros.</div>';
        passengerAlert.style.display = 'none';
        dispatchPageInfo.textContent = 'Pagina 0 de 0';
        dispatchPrevBtn.disabled = true;
        dispatchNextBtn.disabled = true;
        const key = `${dispatchDateFilter.value}|${dispatchManagerFilter.value}|${dispatchItineraryFilter.value}`;
        if (key !== lastNoDataAlertKey) {
            alert('No hay datos para la fecha/itinerario seleccionados.');
            lastNoDataAlertKey = key;
        }
        return;
    }

    lastNoDataAlertKey = '';

    const missingPassengers = filtered.filter((d) => !Number.isInteger(Number(d.passenger_count)) || Number(d.passenger_count) <= 0).length;
    if (missingPassengers > 0) {
        passengerAlert.style.display = 'block';
        passengerAlert.textContent = `Alerta: ${missingPassengers} despacho(s) sin pasajeros ingresados.`;
    } else {
        passengerAlert.style.display = 'block';
        passengerAlert.textContent = 'Todos los despachos filtrados tienen pasajeros ingresados.';
    }

    const orderedDispatches = [...filtered].sort((a, b) => {
        const aCreated = new Date(a.created_at || a.departure_time || 0).getTime();
        const bCreated = new Date(b.created_at || b.departure_time || 0).getTime();
        if (!Number.isNaN(aCreated) && !Number.isNaN(bCreated) && aCreated !== bCreated) {
            return bCreated - aCreated;
        }
        return Number(b.id || 0) - Number(a.id || 0);
    });

    const totalPages = Math.max(1, Math.ceil(orderedDispatches.length / DISPATCH_PAGE_SIZE));
    dispatchPage = Math.min(totalPages, Math.max(1, dispatchPage));
    const start = (dispatchPage - 1) * DISPATCH_PAGE_SIZE;
    const end = start + DISPATCH_PAGE_SIZE;
    const pageRows = orderedDispatches.slice(start, end);

    dispatchPageInfo.textContent = `Pagina ${dispatchPage} de ${totalPages}`;
    dispatchPrevBtn.disabled = dispatchPage <= 1;
    dispatchNextBtn.disabled = dispatchPage >= totalPages;
    dispatchesList.innerHTML = '';

    pageRows.forEach((dispatch, index) => {
        const isCanceled = !!dispatch.is_canceled;
        const canEdit = canCurrentManagerEditDispatch(dispatch);
        const card = document.createElement('article');
        card.className = `dispatch-item ${isCanceled ? 'canceled' : ''}`;

        card.innerHTML = `
            <div class="dispatch-head">
                <span class="dispatch-order">#${start + index + 1}</span>
                <strong>${escapeHtml(dispatch.route)}</strong>
                <span class="status ${isCanceled ? 'status-canceled' : 'status-active'}">
                    ${isCanceled ? 'Cancelado' : 'Activo'}
                </span>
            </div>
            <p><b>Hora salida:</b> ${escapeHtml(formatTime24(dispatch.hora_salida || dispatch.departure_time))}</p>
            <p><b>Itinerario:</b> ${escapeHtml(dispatch.route)}</p>
            <p><b>Vehiculo:</b> ${escapeHtml(dispatch.vehicle)}</p>
            <p><b>Conductor:</b> ${escapeHtml(dispatch.driver)}</p>
            <p><b>Pasajeros:</b> ${escapeHtml(dispatch.passenger_count ?? '-')}</p>
            <p><b>Gestor:</b> ${escapeHtml(dispatch.manager)}</p>
            <p><b>Fecha salida:</b> ${escapeHtml(formatDate(dispatch.departure_time))}</p>
            <p><b>Observaciones:</b> ${escapeHtml(dispatch.notes || '-')}</p>
            <p><b>Cancelacion:</b> ${isCanceled ? escapeHtml(dispatch.cancellation_note || 'Sin motivo') : '-'}</p>
            <div class="dispatch-actions">
                <button onclick="editPassengers(${dispatch.id}, ${dispatch.passenger_count})" ${(isCanceled || !canEdit) ? 'disabled' : ''} title="${canEdit ? '' : 'Solo el gestor que registro este despacho puede editar pasajeros.'}">Editar pasajeros</button>
            </div>
        `;

        dispatchesList.appendChild(card);
    });
}

function renderMissingPassengers() {
    if (dispatchesCache.length === 0) {
        missingPassengerList.innerHTML = '<div class="no-tasks">No hay viajes sin pasajeros.</div>';
        return;
    }

    const filtered = dispatchesCache.filter((item) => {
        const byItinerary = missingItineraryFilter.value ? String(item.route || '').trim() === missingItineraryFilter.value : true;
        const missing = !Number.isInteger(Number(item.passenger_count)) || Number(item.passenger_count) <= 0;
        return byItinerary && missing;
    }).sort((a, b) => Number(b.id || 0) - Number(a.id || 0));

    if (filtered.length === 0) {
        missingPassengerList.innerHTML = '<div class="no-tasks">No hay viajes sin pasajeros.</div>';
        return;
    }

    missingPassengerList.innerHTML = `
        <div class="missing-grid">
            ${filtered.map((row) => `
                <article class="missing-item">
                    <p><b>Hora:</b> ${escapeHtml(formatTime24(row.hora_salida || row.departure_time))}</p>
                    <p><b>Itinerario:</b> ${escapeHtml(row.route || '-')}</p>
                    <p><b>Vehiculo:</b> ${escapeHtml(row.vehicle || '-')}</p>
                    <p><b>Gestor:</b> ${escapeHtml(row.manager || '-')}</p>
                    <div class="dispatch-actions">
                        <button onclick="editPassengers(${row.id}, ${row.passenger_count})" ${canCurrentManagerEditDispatch(row) ? '' : 'disabled'} title="${canCurrentManagerEditDispatch(row) ? '' : 'Solo el gestor que registro este despacho puede editar pasajeros.'}">Ingresar pasajeros</button>
                    </div>
                </article>
            `).join('')}
        </div>
    `;
}

function resetDispatchPaginationAndRender() {
    dispatchPage = 1;
    renderDispatches();
}

function renderSalidas() {
    const filtered = itineraryFilter.value
        ? dispatchesCache.filter((item) => String(item.route || '').trim() === itineraryFilter.value)
        : dispatchesCache;

    if (filtered.length === 0) {
        salidasList.innerHTML = '<div class="no-tasks">No hay salidas para ese itinerario.</div>';
        return;
    }

    const orderedDispatches = [...filtered].sort((a, b) => {
        const aTs = getDispatchSortTimestamp(a);
        const bTs = getDispatchSortTimestamp(b);
        if (aTs !== bTs) return bTs - aTs;
        return Number(b.id || 0) - Number(a.id || 0);
    });

    salidasList.innerHTML = '';

    orderedDispatches.forEach((dispatch, index) => {
        const isCanceled = !!dispatch.is_canceled;
        const row = document.createElement('article');
        row.className = `salida-item ${isCanceled ? 'canceled' : ''}`;

        row.innerHTML = `
            <div class="salida-order">#${index + 1}</div>
            <div class="salida-date">${escapeHtml(formatDate(dispatch.departure_time || dispatch.created_at))}</div>
            <div class="salida-time">${escapeHtml(formatTime24(dispatch.hora_salida || dispatch.departure_time))}</div>
            <div class="salida-route">${escapeHtml(dispatch.route || '-')}</div>
            <div class="salida-vehicle">${escapeHtml(dispatch.vehicle || '-')}</div>
            <div class="salida-driver">${escapeHtml(dispatch.driver || '-')}</div>
        `;

        salidasList.appendChild(row);
    });
}

driver.addEventListener('change', renderDriverInfo);
vehicle.addEventListener('change', handleVehicleChange);
vehicle.addEventListener('change', () => {
    if (vehicle.value) driver.focus();
});
driver.addEventListener('change', () => {
    if (driver.value) departureTime.focus();
});
departureTime.addEventListener('change', () => {
    if (departureTime.value) routeInput.focus();
});
routeInput.addEventListener('change', () => {
    renderRouteSelectedMeta();
    if (routeInput.value) notes.focus();
});
scanVehicleQrBtn.addEventListener('click', startQrScanner);
qrScannerClose.addEventListener('click', stopQrScanner);
qrScannerModal.addEventListener('click', (e) => {
    if (e.target === qrScannerModal) stopQrScanner();
});
managerItineraryGroup.addEventListener('change', () => {
    selectedItineraryGroup = managerItineraryGroup.value;
    loadItineraries();
    if (routeInput.value) {
        const exists = Array.from(routeInput.options).some((opt) => opt.value === routeInput.value);
        if (!exists) routeInput.value = '';
    }
    renderRouteSelectedMeta();
});
itineraryFilter.addEventListener('change', renderSalidas);
dispatchDateFilter.addEventListener('change', () => { loadDispatches(); });
dispatchManagerFilter.addEventListener('input', resetDispatchPaginationAndRender);
dispatchItineraryFilter.addEventListener('change', resetDispatchPaginationAndRender);
sessionManagerBtn.addEventListener('click', () => setSessionView('manager'));
sessionDispatchBtn.addEventListener('click', () => setSessionView('dispatch'));
sessionSalidasBtn.addEventListener('click', () => setSessionView('salidas'));
sessionMissingBtn.addEventListener('click', () => setSessionView('missing'));
sessionFleetBtn.addEventListener('click', () => setSessionView('fleet'));
sessionSicovBtn.addEventListener('click', () => setSessionView('sicov'));
sessionExternalVcBtn.addEventListener('click', () => setSessionView('external-vc'));
sessionUpdatesBtn.addEventListener('click', () => setSessionView('updates'));
startShiftBtn.addEventListener('click', startManagerShift);
endShiftBtn.addEventListener('click', endManagerShift);
shiftPrevBtn.addEventListener('click', () => {
    managerShiftPage -= 1;
    renderManagerShiftHistoryPage();
});
shiftNextBtn.addEventListener('click', () => {
    managerShiftPage += 1;
    renderManagerShiftHistoryPage();
});
dispatchPrevBtn.addEventListener('click', () => {
    dispatchPage -= 1;
    renderDispatches();
});
dispatchNextBtn.addEventListener('click', () => {
    dispatchPage += 1;
    renderDispatches();
});
missingItineraryFilter.addEventListener('change', renderMissingPassengers);
sicovFilter.addEventListener('input', renderSicovRows);
externalVcFilter.addEventListener('input', renderExternalVcRows);
refreshAllCsvBtn.addEventListener('click', () => refreshCsv('all'));
refreshVehiclesCsvBtn.addEventListener('click', () => refreshCsv('vehicles'));
refreshDriversCsvBtn.addEventListener('click', () => refreshCsv('drivers'));
refreshFleetCsvBtn.addEventListener('click', () => refreshCsv('fleet'));
refreshSicovCsvBtn.addEventListener('click', () => refreshCsv('sicov'));
refreshExternalVcBtn.addEventListener('click', () => refreshCsv('external-vc'));
window.addEventListener('offline', () => updateNetworkStatus(true));
window.addEventListener('online', async () => {
    updateNetworkStatus(true);
    await syncPendingDispatchQueue(true);
});
window.addEventListener('visibilitychange', () => {
    if (!document.hidden) {
        loadExternalVehiculoConductores();
    }
});
window.addEventListener('beforeunload', () => {
    stopQrScanner();
    if (loginLockHeartbeatTimer) {
        clearInterval(loginLockHeartbeatTimer);
        loginLockHeartbeatTimer = null;
    }
    if (externalVcAutoRefreshTimer) {
        clearInterval(externalVcAutoRefreshTimer);
        externalVcAutoRefreshTimer = null;
    }
    if (liveClockTimer) {
        clearInterval(liveClockTimer);
        liveClockTimer = null;
    }
});

dispatchForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (dispatchSubmitInFlight) return;
    dispatchSubmitInFlight = true;
    setDispatchSubmitLoading(true);
    setDispatchAvailability();

    try {
    hasInternet = navigator.onLine !== false;
    renderNetworkBadge();
    setDispatchAvailability();

    if (!managerProfile || !activeShift) {
        alert('Debes tener un gestor registrado y turno activo para generar despachos.');
        setSessionView('dispatch');
        return;
    }

    const formError = validateDispatchForm();
    if (formError) {
        alert(formError);
        return;
    }

    const departureIso = getDepartureIsoFromDateAndTime(departureDate.value, departureTime.value);
    if (!departureIso) {
        alert('Fecha de salida invalida. Usa formato DD/MM/YYYY.');
        return;
    }
    const departureHour = `${departureTime.value}:00`;
    const selectedVehicle = getSelectedVehicleRecord();
    const selectedDriver = getSelectedDriverRecord();
    const selectedItinerary = getSelectedItineraryRecord();

    const payload = {
        user_id: currentUser.id,
        vehicle: vehicle.value,
        departure_time: departureIso,
        hora_salida: departureHour,
        route: routeInput.value.trim(),
        driver: driver.value.trim(),
        passenger_count: 0,
        manager: managerProfile.full_name,
        notes: buildDispatchNotesWithMethod(notes.value),
        is_canceled: false,
        canceled_at: null
    };

    if (!payload.route || !payload.manager) {
        alert('Completa los campos obligatorios del formulario.');
        return;
    }

    const appsScriptPayload = {
        mId: String(selectedVehicle?.id || '').trim(),
        itinerary: String(selectedItinerary?.id || '').trim(),
        drvId: String(selectedDriver?.dr_id || '').trim()
    };

    const confirmMessage = `Confirma el despacho:\nVehiculo: ${payload.vehicle}\nConductor: ${payload.driver}\nRuta: ${payload.route}\nFecha/Hora: ${departureDate.value} ${departureTime.value}`;
    const confirmDispatch = await openDispatchConfirmModal(confirmMessage, 'Confirmar despacho');
    if (!confirmDispatch) return;

    if (!hasInternet) {
        const pending = enqueuePendingDispatch({
            payload,
            appsScriptPayload
        });
        alert(`Despacho guardado en modo local. Pendientes por sincronizar: ${pending}.`);
    } else {
        try {
            const validation = await checkVehicleDispatchWindow(payload.vehicle, payload.departure_time, payload.hora_salida);
            if (validation.shouldConfirm) {
                const proceed = await openDispatchConfirmModal(validation.message);
                if (!proceed) return;
            }
        } catch (validationErr) {
            alert(`No se pudo validar ventana de 30 minutos. (${validationErr.message})`);
            return;
        }

        const { error } = await insertDispatchWithCreatedAtFallback(payload);

        if (error) {
            alert(error.message);
            return;
        }

        if (!appsScriptPayload.mId || !appsScriptPayload.itinerary || !appsScriptPayload.drvId) {
            alert('Despacho registrado. No se envio al servicio externo porque faltan IDs.');
        } else {
            try {
                const bridgeResult = await sendDispatchToAppsScript(appsScriptPayload);
                if (bridgeResult?.skipped) {
                    alert('Despacho registrado. El enlace de envio no esta configurado.');
                } else if (bridgeResult?.success === false) {
                    alert(`Despacho registrado. El servicio externo reporto error: ${bridgeResult.message || 'sin detalle'}`);
                } else {
                    alert('Despacho registrado y envio externo realizado.');
                }
            } catch (bridgeErr) {
                alert(`Despacho registrado, pero el envio externo fallo: ${bridgeErr.message}`);
            }
        }
    }

    dispatchForm.reset();
    setAutomaticDate();
    loadVehicles(vehiclesCatalog);
    loadDrivers(driversCatalog);
    loadItineraries();
    dispatchEntryMethod = 'manual';
    setDispatchAvailability();
    driverInfo.textContent = `${driversCatalog.length} conductores disponibles.`;
    vehicleInfo.textContent = `${vehiclesCatalog.length} vehiculos disponibles.`;
    vehicle.focus();
    if (hasInternet) {
        await loadDispatches();
        await syncPendingDispatchQueue(false);
    }
    } finally {
        dispatchSubmitInFlight = false;
        setDispatchSubmitLoading(false);
        setDispatchAvailability();
    }
});

window.editPassengers = async function (id, currentPassengers) {
    const { data: dispatchRow, error: dispatchError } = await sb
        .from('dispatches')
        .select('id, manager')
        .eq('id', id)
        .eq('user_id', currentUser.id)
        .limit(1)
        .maybeSingle();

    if (dispatchError) {
        alert(dispatchError.message);
        return;
    }

    if (!dispatchRow) {
        alert('No se encontro el despacho.');
        return;
    }

    if (!canCurrentManagerEditDispatch(dispatchRow)) {
        alert('Solo el gestor que registro este despacho puede editar pasajeros.');
        return;
    }

    const value = prompt('Nueva cantidad de pasajeros:', String(currentPassengers ?? ''));
    if (value === null) return;

    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed <= 0) {
        alert('Ingresa un numero entero mayor a 0.');
        return;
    }

    const confirmUpdate = await openDispatchConfirmModal(
        `¿Confirmas actualizar pasajeros a ${parsed}?`,
        'Si, guardar'
    );
    if (!confirmUpdate) return;

    const { error } = await sb
        .from('dispatches')
        .update({ passenger_count: parsed })
        .eq('id', id)
        .eq('user_id', currentUser.id);

    if (error) {
        alert(error.message);
        return;
    }

    await loadDispatches();
};

window.logout = async function () {
    try {
        await loadActiveShift();
    } catch (err) {
        if (activeShift) {
            alert('No se pudo verificar el turno. Debes finalizar el turno antes de cerrar sesion.');
            setSessionView('manager');
            return;
        }
    }

    if (activeShift) {
        alert('Tienes un turno activo. Debes finalizar turno antes de cerrar sesion.');
        setSessionView('manager');
        return;
    }

    try {
        await releaseLoginLock();
    } catch (err) {
        // Si falla el release igual cerramos sesion local.
    }

    await sb.auth.signOut();
    location.href = 'index.html';
};

init();















