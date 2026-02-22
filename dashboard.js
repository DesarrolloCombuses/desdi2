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
    ? createClient(EXT_SUPABASE_URL, EXT_SUPABASE_ANON_KEY)
    : null;

let currentUser = null;
let driversCatalog = [];
let vehiclesCatalog = [];
let currentStep = 1;
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
const vehicleDocsPanel = document.getElementById('vehicleDocsPanel');
const departureDate = document.getElementById('departureDate');
const departureTime = document.getElementById('departureTime');
const routeInput = document.getElementById('route');
const driver = document.getElementById('driver');
const passengerCount = document.getElementById('passengerCount');
const manager = document.getElementById('manager');
const managerCedula = document.getElementById('managerCedula');
const managerFullName = document.getElementById('managerFullName');
const managerItineraryGroup = document.getElementById('managerItineraryGroup');
const saveManagerBtn = document.getElementById('saveManagerBtn');
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
const prevStepBtn = document.getElementById('prevStepBtn');
const nextStepBtn = document.getElementById('nextStepBtn');
const submitDispatchBtn = document.getElementById('submitDispatchBtn');
const stepIndicator = document.getElementById('stepIndicator');
const formSteps = Array.from(document.querySelectorAll('.form-step'));
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

if (
    !userEmail || !dispatchForm || !vehicle || !vehicleInfo || !vehicleDocsPanel || !departureDate || !departureTime || !routeInput ||
    !driver || !passengerCount || !manager || !managerCedula || !managerFullName || !managerItineraryGroup || !saveManagerBtn || !startShiftBtn || !endShiftBtn || !managerStatus || !managerShiftHistory || !shiftPrevBtn || !shiftNextBtn || !shiftPageInfo ||
    !dispatchFormCard || !dispatchListCard || !dispatchLockedNotice ||
    !notes || !driverInfo || !dispatchesList || !passengerAlert || !dispatchPrevBtn || !dispatchNextBtn || !dispatchPageInfo || !missingPassengerList || !missingItineraryFilter || !salidasList || !itineraryFilter ||
    !dispatchDateFilter || !dispatchItineraryFilter ||
    !sessionManagerBtn || !sessionDispatchBtn || !sessionSalidasBtn || !sessionMissingBtn || !sessionFleetBtn || !sessionSicovBtn || !sessionExternalVcBtn || !sessionUpdatesBtn || !managerSession || !dispatchSession || !salidasSession || !missingSession || !fleetSession || !sicovSession || !externalVcSession || !updatesSession || !fleetStatus || !fleetList || !sicovStatus || !sicovList || !sicovFilter || !externalVcStatus || !externalVcList || !externalVcFilter ||
    !refreshAllCsvBtn || !refreshVehiclesCsvBtn || !refreshDriversCsvBtn || !refreshFleetCsvBtn || !refreshSicovCsvBtn || !refreshExternalVcBtn ||
    !dispatchConfirmModal || !dispatchConfirmText || !dispatchConfirmCancel || !dispatchConfirmOk || !scanVehicleQrBtn || !qrScannerModal || !qrScannerVideo || !qrScannerStatus || !qrScannerClose ||
    !prevStepBtn || !nextStepBtn || !submitDispatchBtn || !stepIndicator || formSteps.length !== 2 ||
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


function setAutomaticDate() {
    const { year, month, day } = getTodayParts();
    departureDate.value = `${day}/${month}/${year}`;
}

function getDepartureIsoFromTodayAndTime(timeValue) {
    const { year, month, day } = getTodayParts();
    // Guardamos la hora seleccionada como hora de Colombia (-05:00) para evitar desfases.
    return `${year}-${month}-${day}T${timeValue}:00-05:00`;
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


function openDispatchConfirmModal(message) {
    return new Promise((resolve) => {
        dispatchConfirmText.textContent = message;
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

function showStep(step) {
    currentStep = Math.min(2, Math.max(1, step));

    formSteps.forEach((section) => {
        section.classList.toggle('active', Number(section.dataset.step) === currentStep);
    });

    stepIndicator.textContent = `Paso ${currentStep} de 2`;
    prevStepBtn.disabled = currentStep === 1;
    nextStepBtn.style.display = currentStep === 1 ? 'inline-block' : 'none';
    submitDispatchBtn.style.display = currentStep === 2 ? 'inline-block' : 'none';
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
}

function setDispatchAvailability() {
    const hasProfile = !!managerProfile;
    const hasActiveShift = !!activeShift;
    const enabled = hasProfile && hasActiveShift && managerAuthenticated && hasInternet && !shiftSessionMismatch;

    const shouldLockByInternet = !hasInternet;
    const shouldLockByShift = !hasActiveShift;
    const shouldLock = shouldLockByInternet || shouldLockByShift;

    dispatchLockedNotice.style.display = shouldLock ? 'block' : 'none';
    dispatchFormCard.style.display = shouldLock ? 'none' : 'block';
    dispatchListCard.style.display = shouldLock ? 'none' : 'block';
    dispatchLockedText.textContent = shouldLockByInternet
        ? 'No tienes internet. Debes reconectarte para generar y consultar despachos.'
        : 'Debes iniciar turno en la pestaña Control gestor para habilitar esta seccion.';
    manager.value = enabled ? (managerProfile.full_name || '') : '';
    nextStepBtn.disabled = !enabled;
    submitDispatchBtn.disabled = !enabled;

    if (!hasProfile) {
        managerStatus.textContent = 'Debes registrar cédula y nombre completo para operar.';
        showManagerAlertOnce('need_register', 'No existe gestor validado. Debes registrarte con cedula y nombre completo.');
        return;
    }

    if (!managerAuthenticated) {
        managerStatus.textContent = 'Valida tus datos de gestor (cedula y nombre) para habilitar operaciones.';
        showManagerAlertOnce('need_validate', 'Debes validar tu cedula y nombre para continuar.');
        return;
    }

    if (shiftSessionMismatch) {
        managerStatus.textContent = 'Turno activo detectado en otra sesion. Debes validar de nuevo con tu cedula.';
        showManagerAlertOnce('shift_session_mismatch', 'Este turno activo pertenece a otra sesion. Valida tus datos para continuar.');
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

function renderNetworkBadge() {
    networkStatus.textContent = hasInternet ? 'En linea' : 'Sin internet';
    networkStatus.classList.toggle('online', hasInternet);
    networkStatus.classList.toggle('offline', !hasInternet);
}

function updateNetworkStatus(shouldAlert) {
    hasInternet = navigator.onLine !== false;
    renderNetworkBadge();

    if (!hasInternet && shouldAlert) {
        alert('No tienes internet. No podras generar despachos hasta reconectarte.');
    }

    if (hasInternet && shouldAlert) {
        alert('Conexion restablecida. Ya puedes generar despachos.');
    }

    setDispatchAvailability();
}

function showManagerAlertOnce(key, message) {
    if (suppressManagerAlerts) return;
    if (lastManagerAlertKey === key) return;
    lastManagerAlertKey = key;
    alert(message);
}

function setManagerNameLocked(locked) {
    managerFullName.readOnly = locked;
    managerFullName.style.background = locked ? '#f3f4f6' : '';
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

async function loadManagerProfile() {
    managerProfile = null;
    managerAuthenticated = false;

    managerCedula.value = '';
    managerFullName.value = '';
    setManagerNameLocked(false);

    const { data, error } = await sb
        .from('manager_profiles')
        .select('*')
        .eq('user_id', currentUser.id)
        .order('updated_at', { ascending: false })
        .limit(200);

    if (error) throw error;

    const profiles = data || [];
    if (profiles.length === 0) return;

    let selected = null;
    if (activeShift?.manager_profile_id) {
        selected = profiles.find((row) => String(row.id) === String(activeShift.manager_profile_id)) || null;
    }

    if (!selected && activeShift?.manager_name) {
        selected = profiles.find((row) => normalizeText(row.full_name) === normalizeText(activeShift.manager_name)) || null;
    }

    if (!selected) selected = profiles[0];

    managerProfile = selected;
    managerCedula.value = selected.cedula || '';
    managerFullName.value = selected.full_name || '';
    setManagerNameLocked(true);
}

async function findManagerByCedula(cedula) {
    const { data, error } = await sb
        .from('manager_profiles')
        .select('*')
        .eq('user_id', currentUser.id)
        .eq('cedula', cedula)
        .limit(1)
        .maybeSingle();

    if (error) throw error;
    return data || null;
}

async function resolveManagerByCedulaInput() {
    const cedula = managerCedula.value.trim();
    if (!cedula) {
        managerAuthenticated = false;
        managerProfile = null;
        managerFullName.value = '';
        setManagerNameLocked(false);
        showManagerAlertOnce('empty_cedula', 'Debes ingresar la cedula del gestor.');
        setDispatchAvailability();
        return null;
    }

    const found = await findManagerByCedula(cedula);
    if (!found) {
        managerAuthenticated = false;
        managerProfile = null;
        managerFullName.value = '';
        setManagerNameLocked(false);
        managerStatus.textContent = 'Cedula no registrada. Debes registrar nombre completo.';
        showManagerAlertOnce('cedula_not_found', 'Cedula no registrada. Debes registrarte con nombre completo.');
        setDispatchAvailability();
        return null;
    }

    if (activeShift?.manager_profile_id && String(found.id) !== String(activeShift.manager_profile_id)) {
        managerAuthenticated = false;
        managerStatus.textContent = 'Existe un turno activo de otro gestor en esta sesion.';
        showManagerAlertOnce('active_shift_other_manager', 'El turno activo pertenece a otro gestor. Debes finalizar ese turno o usar la cedula correcta.');
        setDispatchAvailability();
        return null;
    }

    managerProfile = found;
    managerFullName.value = found.full_name || '';
    setManagerNameLocked(true);
    managerAuthenticated = true;
    shiftSessionMismatch = false;
    if (activeShift?.session_token) setStoredShiftToken(String(activeShift.session_token));
    showManagerAlertOnce('cedula_found', `Cedula validada. Bienvenido ${found.full_name || ''}`.trim());
    setDispatchAvailability();
    return found;
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
        return `
            <article class="shift-item ${row.end_time ? '' : 'shift-active'}">
                <p><b>Gestor:</b> ${escapeHtml(row.manager_name || managerProfile?.full_name || '-')}</p>
                <p><b>Estado:</b> ${escapeHtml(status)}</p>
                <p><b>Inicio:</b> ${escapeHtml(started)}</p>
                <p><b>Fin:</b> ${escapeHtml(ended)}</p>
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

async function saveManagerProfile() {
    const cedula = managerCedula.value.trim();
    const fullName = managerFullName.value.trim();

    if (!cedula) {
        alert('Debes ingresar cedula.');
        return;
    }

    const byCedula = await findManagerByCedula(cedula);
    if (byCedula) {
        if (activeShift?.manager_profile_id && String(byCedula.id) !== String(activeShift.manager_profile_id)) {
            alert('El turno activo pertenece a otro gestor. Debes finalizar ese turno o validar la cedula correcta.');
            return;
        }
        managerProfile = byCedula;
        managerFullName.value = byCedula.full_name || '';
        setManagerNameLocked(true);
        managerAuthenticated = true;
        shiftSessionMismatch = false;
        if (activeShift?.session_token) setStoredShiftToken(String(activeShift.session_token));
        showManagerAlertOnce('login_ok', 'Gestor validado correctamente.');
        setDispatchAvailability();
        await loadManagerShiftHistory();
        return;
    }

    if (!fullName) {
        alert('Cedula no registrada. Debes ingresar nombre completo para registrarte.');
        return;
    }

    // Siempre registramos una nueva persona para este usuario cuando la cédula no existe.
    const { error } = await sb
        .from('manager_profiles')
        .insert([
            {
                user_id: currentUser.id,
                cedula,
                full_name: fullName,
                updated_at: new Date().toISOString()
            }
        ]);

    if (error) {
        alert(error.message);
        return;
    }

    const created = await findManagerByCedula(cedula);
    managerProfile = created;
    managerAuthenticated = !!created;
    shiftSessionMismatch = false;
    setManagerNameLocked(!!created);
    showManagerAlertOnce('registered_ok', 'Gestor registrado correctamente. Ahora puedes iniciar turno.');

    setDispatchAvailability();
    await loadManagerShiftHistory();
}

async function startManagerShift() {
    if (!managerProfile) {
        alert('Primero guarda los datos del gestor.');
        return;
    }

    if (!managerAuthenticated) {
        alert('Debes validar tu cedula y nombre antes de iniciar turno.');
        return;
    }

    if (!managerProfile.id) {
        alert('No se pudo identificar el gestor (falta id de perfil). Guarda/valida nuevamente la cédula.');
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
            manager_profile_id: managerProfile.id,
            manager_name: managerProfile.full_name,
            session_token: sessionToken,
            start_time: new Date().toISOString(),
            start_lat: geo?.lat ?? null,
            start_lng: geo?.lng ?? null
        }
    ]);

    if (error) {
        const msg = String(error.message || '').toLowerCase();
        const missingSecureColumns = (msg.includes('manager_profile_id') || msg.includes('session_token')) &&
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
                alert('Turno iniciado en modo compatible. Falta aplicar migracion de seguridad (manager_profile_id/session_token).');
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
    showManagerAlertOnce('shift_started', 'Turno iniciado correctamente.');
    await loadManagerShiftHistory();
}

async function endManagerShift() {
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
    managerAuthenticated = false;
    shiftSessionMismatch = false;
    clearStoredShiftToken();
    managerCedula.value = '';
    managerFullName.value = '';
    setManagerNameLocked(false);
    setDispatchAvailability();
    showManagerAlertOnce('shift_ended', 'Turno finalizado. Debes registrarte/validarte de nuevo para continuar.');
    await loadManagerShiftHistory();
}

function validateStep1() {
    if (!vehicle.value) return 'Selecciona un vehiculo.';
    if (!driver.value) return 'Selecciona un conductor.';
    if (!departureTime.value) return 'Selecciona la hora de salida.';
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

    const activeFirst = [
        ...catalog.filter((item) => isActiveDriverStatus(item.status)),
        ...catalog.filter((item) => !isActiveDriverStatus(item.status))
    ];

    activeFirst.forEach((item) => {
        const label = `${item.nombre} (${item.cedula || 'sin cedula'})`;
        const option = document.createElement('option');
        option.value = item.nombre;
        option.textContent = label;
        driver.appendChild(option);
    });
}

function renderDriverInfo() {
    const selected = driversCatalog.find((item) => normalizeText(item.nombre) === normalizeText(driver.value));

    if (!selected) {
        driverInfo.textContent = 'Selecciona un conductor de la lista.';
        return;
    }

    driverInfo.textContent = `Cedula: ${selected.cedula || '-'} | Fleet: ${selected.fleet || '-'} | Celular: ${selected.celular || '-'} | Email: ${selected.email || '-'} | Status: ${selected.status || '-'}`;

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
    if (!qrSelectionInProgress) {
        dispatchEntryMethod = 'manual';
    }
    renderVehicleDocsStatus();
    autoFillDriverFromExternalByVehicle();
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
    const tableVehiculoConductores = String(EXT_VEHICULO_CONDUCTORES_TABLE || 'vehiculo_conductores').trim();
    const { data, error } = await sbExternal
        .from(tableVehiculoConductores)
        .select('vehiculo, cedula, nombre, email, created_at, fecha_ingreso, hora_ingreso')
        .eq('vehiculo', vehicleNumber)
        .order('created_at', { ascending: false })
        .limit(1);

    if (error) throw error;
    return data && data[0] ? data[0] : null;
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

    try {
        const externalRow = await findExternalDriverByVehicle(vehicleNumber);
        if (requestId !== vehicleDriverLookupSeq) return;

        if (externalRow) {
            const matched = selectDriverFromCsvByExternalRow(externalRow);
            if (matched) {
                alert(`Conductor autocompletado desde biometrico para vehiculo ${vehicleNumber}.`);
                return;
            }

            alert(`Se encontro conductor en biometrico (${externalRow.nombre}), pero no esta en la lista de conductores.`);
            return;
        }

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
        const response = await fetch(VEHICLES_CSV_URL, { cache: 'no-store' });
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
        vehicleInfo.textContent = `${vehiclesCatalog.length} vehiculos disponibles.`;
    } catch (err) {
        vehiclesCatalog = [];
        loadVehicles([]);
        vehicleInfo.textContent = `No se pudieron cargar vehiculos. (${err.message})`;
    }
}

async function loadDriversFromSheet() {
    try {
        const response = await fetch(DRIVERS_CSV_URL, { cache: 'no-store' });
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

        driversCatalog = data.filter((item) => item.nombre);
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
        <div class="fleet-grid">
            ${fleetRows.map((row) => `
                <article class="fleet-item">
                    ${fleetHeaders.map((header) => `
                        <p><b>${escapeHtml(header)}:</b> ${escapeHtml(row[header] || '-')}</p>
                    `).join('')}
                </article>
            `).join('')}
        </div>
    `;
}

async function loadFleetFromSheet() {
    try {
        const response = await fetch(FLEET_CSV_URL, { cache: 'no-store' });
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
        <div class="fleet-grid">
            ${filtered.map((row) => `
                <article class="fleet-item">
                    ${sicovHeaders.map((header) => `
                        <p><b>${escapeHtml(header)}:</b> ${escapeHtml(row[header] || '-')}</p>
                    `).join('')}
                </article>
            `).join('')}
        </div>
    `;
}

async function loadSicovFromSheet() {
    try {
        const response = await fetch(SICOV_CSV_URL, { cache: 'no-store' });
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
    if (!sbExternal) {
        externalVcHeaders = [];
        externalVcRows = [];
        externalVcStatus.textContent = 'No hay configuracion disponible para esta consulta.';
        renderExternalVcRows();
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
    }
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

    setAutomaticDate();
    showStep(1);
    setSessionView('dispatch');
    loadItineraryGroups();
    loadItineraries();
    try {
        await loadActiveShift();
        await loadManagerProfile();
        if (activeShift) {
            managerAuthenticated = !shiftSessionMismatch;
            if (!managerProfile) {
                managerProfile = {
                    user_id: currentUser.id,
                    cedula: '',
                    full_name: activeShift.manager_name || ''
                };
                managerCedula.value = '';
                managerFullName.value = managerProfile.full_name;
                setManagerNameLocked(true);
            }
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
        nextStepBtn.disabled = true;
        submitDispatchBtn.disabled = true;
    }
    await loadVehiclesFromSheet();
    await loadDriversFromSheet();
    await loadFleetFromSheet();
    await loadSicovFromSheet();
    await loadExternalVehiculoConductores();
    await loadDispatches();
}

async function loadDispatches() {
    const { data, error } = await sb
        .from('dispatches')
        .select('*')
        .eq('user_id', currentUser.id);

    if (error) {
        alert(error.message);
        return;
    }

    dispatchesList.innerHTML = '';
    salidasList.innerHTML = '';

    if (!data || data.length === 0) {
        dispatchesCache = [];
        renderItineraryFilter([]);
        renderDispatchFilters([]);
        dispatchesList.innerHTML = '<div class="no-tasks">No hay despachos registrados.</div>';
        salidasList.innerHTML = '<div class="no-tasks">No hay salidas disponibles.</div>';
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
        const byDate = dispatchDateFilter.value
            ? getStoredDateKey(item) === dispatchDateFilter.value
            : true;
        const byItinerary = dispatchItineraryFilter.value ? String(item.route || '').trim() === dispatchItineraryFilter.value : true;
        return byDate && byItinerary;
    });

    if (filtered.length === 0) {
        dispatchesList.innerHTML = '<div class="no-tasks">No hay despachos con esos filtros.</div>';
        passengerAlert.style.display = 'none';
        dispatchPageInfo.textContent = 'Pagina 0 de 0';
        dispatchPrevBtn.disabled = true;
        dispatchNextBtn.disabled = true;
        const key = `${dispatchDateFilter.value}|${dispatchItineraryFilter.value}`;
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
                <button onclick="editPassengers(${dispatch.id}, ${dispatch.passenger_count})" ${isCanceled ? 'disabled' : ''}>Editar pasajeros</button>
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
                        <button onclick="editPassengers(${row.id}, ${row.passenger_count})">Ingresar pasajeros</button>
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

nextStepBtn.addEventListener('click', () => {
    const err = validateStep1();
    if (err) {
        alert(err);
        return;
    }
    showStep(2);
});

prevStepBtn.addEventListener('click', () => showStep(1));

driver.addEventListener('change', renderDriverInfo);
vehicle.addEventListener('change', handleVehicleChange);
scanVehicleQrBtn.addEventListener('click', startQrScanner);
qrScannerClose.addEventListener('click', stopQrScanner);
qrScannerModal.addEventListener('click', (e) => {
    if (e.target === qrScannerModal) stopQrScanner();
});
managerCedula.addEventListener('blur', async () => {
    try {
        await resolveManagerByCedulaInput();
    } catch (err) {
        managerStatus.textContent = `Error validando cedula. (${err.message})`;
    }
});
managerItineraryGroup.addEventListener('change', () => {
    selectedItineraryGroup = managerItineraryGroup.value;
    loadItineraries();
    if (routeInput.value) {
        const exists = Array.from(routeInput.options).some((opt) => opt.value === routeInput.value);
        if (!exists) routeInput.value = '';
    }
});
itineraryFilter.addEventListener('change', renderSalidas);
dispatchDateFilter.addEventListener('change', resetDispatchPaginationAndRender);
dispatchItineraryFilter.addEventListener('change', resetDispatchPaginationAndRender);
sessionManagerBtn.addEventListener('click', () => setSessionView('manager'));
sessionDispatchBtn.addEventListener('click', () => setSessionView('dispatch'));
sessionSalidasBtn.addEventListener('click', () => setSessionView('salidas'));
sessionMissingBtn.addEventListener('click', () => setSessionView('missing'));
sessionFleetBtn.addEventListener('click', () => setSessionView('fleet'));
sessionSicovBtn.addEventListener('click', () => setSessionView('sicov'));
sessionExternalVcBtn.addEventListener('click', () => setSessionView('external-vc'));
sessionUpdatesBtn.addEventListener('click', () => setSessionView('updates'));
saveManagerBtn.addEventListener('click', saveManagerProfile);
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
window.addEventListener('online', () => updateNetworkStatus(true));
window.addEventListener('beforeunload', () => { stopQrScanner(); });

dispatchForm.addEventListener('submit', async (e) => {
    e.preventDefault();

    if (navigator.onLine === false || !hasInternet) {
        hasInternet = false;
        renderNetworkBadge();
        setDispatchAvailability();
        alert('Sin internet: no es posible guardar el despacho en este momento.');
        return;
    }

    if (!managerProfile || !activeShift) {
        alert('Debes tener un gestor registrado y turno activo para generar despachos.');
        setSessionView('dispatch');
        return;
    }

    const step1Error = validateStep1();
    if (step1Error) {
        showStep(1);
        alert(step1Error);
        return;
    }

    const departureIso = getDepartureIsoFromTodayAndTime(departureTime.value);
    const departureHour = `${departureTime.value}:00`;
    const passengersValue = String(passengerCount.value || '').trim();
    const passengersNumber = passengersValue ? Number(passengersValue) : 0;

    if (passengersValue && (!Number.isInteger(passengersNumber) || passengersNumber < 0)) {
        alert('Si ingresas pasajeros, debe ser un numero entero mayor o igual a 0.');
        return;
    }

    const payload = {
        user_id: currentUser.id,
        vehicle: vehicle.value,
        departure_time: departureIso,
        hora_salida: departureHour,
        route: routeInput.value.trim(),
        driver: driver.value.trim(),
        passenger_count: Number.isInteger(passengersNumber) ? passengersNumber : 0,
        manager: managerProfile.full_name,
        notes: buildDispatchNotesWithMethod(notes.value),
        is_canceled: false,
        canceled_at: null
    };

    if (!payload.route || !payload.manager) {
        alert('Completa los campos obligatorios del paso 2.');
        return;
    }

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

    alert('Despacho registrado exitosamente.');

    dispatchForm.reset();
    setAutomaticDate();
    loadVehicles(vehiclesCatalog);
    loadDrivers(driversCatalog);
    loadItineraries();
    dispatchEntryMethod = 'manual';
    setDispatchAvailability();
    driverInfo.textContent = `${driversCatalog.length} conductores disponibles.`;
    vehicleInfo.textContent = `${vehiclesCatalog.length} vehiculos disponibles.`;
    showStep(1);
    await loadDispatches();
});

window.editPassengers = async function (id, currentPassengers) {
    const value = prompt('Nueva cantidad de pasajeros:', String(currentPassengers ?? ''));
    if (value === null) return;

    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed <= 0) {
        alert('Ingresa un numero entero mayor a 0.');
        return;
    }

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
    await sb.auth.signOut();
    location.href = 'index.html';
};

init();






