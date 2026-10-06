package com.example.network

import com.example.model.CallRecordRequest
import com.example.model.CallerCustomerContext
import com.example.model.PosHubState
import com.example.model.SignalRAuthRequest
import com.example.model.TrustCredentials
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONArray
import org.json.JSONObject
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Robust Central POS Hub Connection Manager for TaloolaPos (/posHub).
 * 
 * Strict Server Contract Adherence:
 * - SignalR protocol: json, version 1
 * - KeepAlive / Ping: Responds immediately to "Ping" event with HeartbeatCallerAssistant(clientId)
 * - Heartbeat Loop: Executes HeartbeatCallerAssistant(clientId) exactly every 10 seconds while in READY state
 * - Server timeout: 30 seconds
 * - Automatic Reconnect delays: [0s, 2s, 5s, 10s, 15s, 30s]
 * - Server events handled:
 *    * "Ping" -> immediate heartbeat response
 *    * "CallerAssistantReplaced" -> clean notification and reconnect/backoff
 *    * "ServerDisconnect" -> records disconnect reason
 *    * "CallerContextReceived" / "CallerCustomerContext" -> live caller data
 * - AuthenticateCallerAssistant result parsing: extracts clientId, sessionId, serverId, capabilities
 */
class PosHubConnectionManager(
    private val okHttpClient: OkHttpClient = OkHttpClient.Builder()
        .readTimeout(0, TimeUnit.MILLISECONDS) // Indefinite read for WebSockets
        .pingInterval(10, TimeUnit.SECONDS)   // Matching 10s keepalive
        .connectTimeout(15, TimeUnit.SECONDS)
        .build()
) {
    companion object {
        private const val TAG = "ALAMER_POSHUB"
        private const val RECORD_SEPARATOR = "\u001e"

        @Volatile
        private var instance: PosHubConnectionManager? = null

        fun getInstance(okHttpClient: OkHttpClient? = null): PosHubConnectionManager {
            return instance ?: synchronized(this) {
                instance ?: PosHubConnectionManager(
                    okHttpClient ?: OkHttpClient.Builder()
                        .readTimeout(0, TimeUnit.MILLISECONDS)
                        .pingInterval(10, TimeUnit.SECONDS)
                        .connectTimeout(15, TimeUnit.SECONDS)
                        .build()
                ).also { instance = it }
            }
        }
    }

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val moshi = Moshi.Builder().addLast(KotlinJsonAdapterFactory()).build()
    private val authAdapter = moshi.adapter(SignalRAuthRequest::class.java)
    private val callRecordAdapter = moshi.adapter(CallRecordRequest::class.java)
    private val customerContextAdapter = moshi.adapter(CallerCustomerContext::class.java)

    // State Machine
    private val _connectionState = MutableStateFlow(PosHubState.DISCONNECTED)
    val connectionState: StateFlow<PosHubState> = _connectionState.asStateFlow()

    // Concurrency controls
    private val connectionMutex = Mutex()
    private val currentGeneration = AtomicInteger(0)
    private val invocationCounter = AtomicInteger(1)

    // Current connection target & credentials
    @Volatile
    private var activeTrust: TrustCredentials? = null
    @Volatile
    private var webSocket: WebSocket? = null
    @Volatile
    private var isManuallyStopped = false

    // Active session info returned by server AuthenticateCallerAssistant
    @Volatile
    private var activeClientId: String? = null
    @Volatile
    private var activeSessionId: String? = null
    @Volatile
    private var activeServerId: String? = null

    // Heartbeat job running every 10 seconds
    private var heartbeatJob: Job? = null
    private val _heartbeatCount = MutableStateFlow(0)
    val heartbeatCount: StateFlow<Int> = _heartbeatCount.asStateFlow()

    // Diagnostic information
    private val _lastError = MutableStateFlow<String?>(null)
    val lastError: StateFlow<String?> = _lastError.asStateFlow()

    private val _retryCount = MutableStateFlow(0)
    val retryCount: StateFlow<Int> = _retryCount.asStateFlow()

    private val _lastConnectAttempt = MutableStateFlow("-")
    val lastConnectAttempt: StateFlow<String> = _lastConnectAttempt.asStateFlow()

    private val _lastSuccessfulConnect = MutableStateFlow("-")
    val lastSuccessfulConnect: StateFlow<String> = _lastSuccessfulConnect.asStateFlow()

    private val _lastDisconnectReason = MutableStateFlow("-")
    val lastDisconnectReason: StateFlow<String> = _lastDisconnectReason.asStateFlow()

    private val _lastAuthResult = MutableStateFlow("-")
    val lastAuthResult: StateFlow<String> = _lastAuthResult.asStateFlow()

    private val _hubUrl = MutableStateFlow("-")
    val hubUrl: StateFlow<String> = _hubUrl.asStateFlow()

    private val _serverUrl = MutableStateFlow("-")
    val serverUrl: StateFlow<String> = _serverUrl.asStateFlow()

    private val _activeClientIdFlow = MutableStateFlow("-")
    val activeClientIdFlow: StateFlow<String> = _activeClientIdFlow.asStateFlow()

    // Incoming caller context events
    private val _customerContextEvents = MutableSharedFlow<CallerCustomerContext>(extraBufferCapacity = 64)
    val customerContextEvents: SharedFlow<CallerCustomerContext> = _customerContextEvents.asSharedFlow()

    // Automatic reconnect policy with exact contract backoffs
    private var reconnectJob: Job? = null
    private var retryIndex = 0
    private val backoffDelaysMs = listOf(0L, 2000L, 5000L, 10000L, 15000L, 30000L)

    /**
     * Start connection to POS Hub using saved trust credentials.
     */
    fun start(trust: TrustCredentials) {
        scope.launch {
            connectionMutex.withLock {
                isManuallyStopped = false
                activeTrust = trust
                _serverUrl.value = trust.serverUrl
                _hubUrl.value = PosHubUrlBuilder.buildPosHubUrl(trust.serverUrl)
                retryIndex = 0
                _retryCount.value = 0
                _lastError.value = null
                connectSingleFlight(trust)
            }
        }
    }

    private suspend fun connectSingleFlight(trust: TrustCredentials) {
        if (isManuallyStopped) return

        if (_connectionState.value == PosHubState.READY || _connectionState.value == PosHubState.AUTHENTICATING) {
            return
        }

        val gen = currentGeneration.incrementAndGet()
        _connectionState.value = PosHubState.CONNECTING
        _lastConnectAttempt.value = formatTimestamp(Date())
        val fullHubUrl = PosHubUrlBuilder.buildPosHubUrl(trust.serverUrl)
        logInfo("POSHUB_CONNECT_START (generation=$gen) URL=$fullHubUrl")

        stopHeartbeat()
        closeCurrentSocket()

        // Step 1: Transport Negotiation
        var connectionToken: String? = null
        try {
            val negotiateUrl = PosHubUrlBuilder.buildNegotiateUrl(trust.serverUrl)
            val negRequest = Request.Builder()
                .url(negotiateUrl)
                .header("User-Agent", "ALAMER-Caller-Assistant/1.0")
                .header("Accept", "application/json")
                .post("{}".toRequestBody("application/json; charset=utf-8".toMediaType()))
                .build()

            okHttpClient.newCall(negRequest).execute().use { response ->
                if (response.isSuccessful) {
                    val body = response.body?.string() ?: ""
                    val json = JSONObject(body)
                    connectionToken = json.optString(
                        "connectionToken",
                        json.optString("connectionId", null)
                    )
                    logInfo("POSHUB_NEGOTIATE_SUCCESS (token=${connectionToken?.take(8)}...)")
                } else {
                    logInfo("POSHUB_NEGOTIATE_STATUS: ${response.code}, continuing to WebSocket")
                }
            }
        } catch (e: Exception) {
            logInfo("POSHUB_NEGOTIATE_NOTE: ${e.message}, proceeding to direct WebSocket")
        }

        // Step 2: Establish WebSocket Connection
        val wsUrl = PosHubUrlBuilder.buildWebSocketUrl(trust.serverUrl, connectionToken)
        val request = Request.Builder()
            .url(wsUrl)
            .header("User-Agent", "ALAMER-Caller-Assistant/1.0")
            .build()

        webSocket = okHttpClient.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(ws: WebSocket, response: Response) {
                if (currentGeneration.get() != gen) return
                logInfo("POSHUB_SOCKET_OPEN -> sending SignalR handshake (protocol=json, version=1)")
                val handshakeJson = "{\"protocol\":\"json\",\"version\":1}$RECORD_SEPARATOR"
                ws.send(handshakeJson)
            }

            override fun onMessage(ws: WebSocket, text: String) {
                if (currentGeneration.get() != gen) return
                handleIncomingFrames(ws, text, gen, trust)
            }

            override fun onClosing(ws: WebSocket, code: Int, reason: String) {
                ws.close(1000, null)
            }

            override fun onClosed(ws: WebSocket, code: Int, reason: String) {
                if (currentGeneration.get() != gen) return
                logInfo("POSHUB_CLOSED (code=$code, reason=$reason)")
                stopHeartbeat()
                _lastDisconnectReason.value = "تم إغلاق الاتصال ($code): ${reason.ifBlank { "طبيعي" }}"
                if (isManuallyStopped) {
                    _connectionState.value = PosHubState.DISCONNECTED
                } else {
                    _connectionState.value = PosHubState.RECONNECTING
                    scheduleReconnect()
                }
            }

            override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
                if (currentGeneration.get() != gen) return
                val errMsg = t.message ?: "فشل في اتصال WebSocket"
                logError("POSHUB_CONNECT_FAILED: $errMsg", t)
                stopHeartbeat()
                _lastDisconnectReason.value = errMsg
                _lastError.value = errMsg
                if (isManuallyStopped) {
                    _connectionState.value = PosHubState.DISCONNECTED
                } else {
                    _connectionState.value = PosHubState.RECONNECTING
                    scheduleReconnect()
                }
            }
        })
    }

    private fun handleIncomingFrames(
        ws: WebSocket,
        text: String,
        generation: Int,
        trust: TrustCredentials
    ) {
        val messages = text.split(RECORD_SEPARATOR).filter { it.isNotBlank() }
        for (msg in messages) {
            try {
                val json = JSONObject(msg)

                // 1. Handshake Response Handling
                if (_connectionState.value == PosHubState.CONNECTING) {
                    if (json.has("error")) {
                        val err = json.optString("error")
                        logError("POSHUB_HANDSHAKE_ERROR: $err")
                        _lastError.value = "فشل بروتوكول SignalR: $err"
                        _connectionState.value = PosHubState.FAILED
                        scheduleReconnect()
                        return
                    }

                    _connectionState.value = PosHubState.CONNECTED
                    _lastSuccessfulConnect.value = formatTimestamp(Date())
                    logInfo("POSHUB_CONNECT_SUCCESS -> Handshake Accepted")

                    // Proceed immediately to AuthenticateCallerAssistant
                    _connectionState.value = PosHubState.AUTHENTICATING
                    logInfo("POSHUB_AUTH_START -> Invoking AuthenticateCallerAssistant")
                    sendAuthentication(ws, trust)
                    return
                }

                val type = json.optInt("type", -1)
                when (type) {
                    1 -> { // Server Invocations & Hub Events
                        val target = json.optString("target", "")
                        val args = json.optJSONArray("arguments")
                        handleServerInvocation(ws, target, args)
                    }
                    3 -> { // Completion (Response to AuthenticateCallerAssistant or other Invocations)
                        val invocationId = json.optString("invocationId", "")
                        if (invocationId == "auth-1") {
                            handleAuthCompletion(json)
                        } else if (invocationId.startsWith("hb-")) {
                            handleHeartbeatCompletion(json)
                        }
                    }
                    6 -> { // SignalR Protocol Ping
                        ws.send("{\"type\":6}$RECORD_SEPARATOR")
                    }
                }
            } catch (e: Exception) {
                logError("POSHUB_FRAME_PARSE_ERROR: ${e.message}", e)
            }
        }
    }

    private fun sendAuthentication(ws: WebSocket, trust: TrustCredentials) {
        val authReq = SignalRAuthRequest(
            deviceId = trust.deviceId,
            deviceName = trust.deviceName,
            installationBinding = trust.installationBinding,
            callerCredential = trust.callerCredential,
            protocolVersion = trust.protocolVersion,
            platform = "Android"
        )
        val authJson = authAdapter.toJson(authReq)
        val invocation = JSONObject().apply {
            put("type", 1)
            put("invocationId", "auth-1")
            put("target", "AuthenticateCallerAssistant")
            put("arguments", JSONArray().put(JSONObject(authJson)))
        }

        val credRedacted = if (trust.callerCredential.length > 4) {
            "***" + trust.callerCredential.takeLast(4)
        } else "[SET]"
        logInfo("POSHUB_SEND_AUTH (deviceId=${trust.deviceId}, cred=$credRedacted)")
        ws.send(invocation.toString() + RECORD_SEPARATOR)
    }

    private fun handleAuthCompletion(json: JSONObject) {
        val error = json.optString("error", "")
        if (error.isNotBlank()) {
            logError("POSHUB_AUTH_FAILED: $error")
            _lastAuthResult.value = "فشل المصادقة: $error"
            _lastError.value = "فشل المصادقة: $error"
            if (error.contains("Unauthorized", ignoreCase = true) ||
                error.contains("Credential", ignoreCase = true) ||
                error.contains("Revoked", ignoreCase = true)
            ) {
                _connectionState.value = PosHubState.FAILED
                return
            }
            _connectionState.value = PosHubState.FAILED
            scheduleReconnect()
            return
        }

        val rawResult = json.opt("result")
        if (rawResult == null) {
            logInfo("POSHUB_AUTH_SUCCESS (empty payload)")
            _lastAuthResult.value = "تمت المصادقة"
            _connectionState.value = PosHubState.READY
            resetRetryPolicy()
            startHeartbeatLoop()
            return
        }

        try {
            val resultObj = when (rawResult) {
                is JSONObject -> rawResult
                else -> JSONObject(rawResult.toString())
            }

            // Extract session identifiers from ClientAuthResult
            activeClientId = resultObj.optString("clientId", resultObj.optString("ClientId", ""))
            activeSessionId = resultObj.optString("sessionId", resultObj.optString("SessionId", ""))
            activeServerId = resultObj.optString("serverId", resultObj.optString("ServerId", ""))

            _activeClientIdFlow.value = activeClientId?.ifBlank { "-" } ?: "-"

            val isSuccess = resultObj.optBoolean("success", resultObj.optBoolean("Success", false))
            val isCallerGranted = resultObj.optBoolean("callerAssistantGranted", resultObj.optBoolean("CallerAssistantGranted", true))
            val message = resultObj.optString("message", resultObj.optString("Message", ""))

            val pairingResult = PairingResultParser.parse(rawResult.toString())
            val isApproved = isSuccess || isCallerGranted || pairingResult.success ||
                    pairingResult.deviceStatus.equals("Approved", ignoreCase = true)

            if (isApproved) {
                logInfo("POSHUB_AUTH_SUCCESS: ClientId=$activeClientId, SessionId=$activeSessionId, Capabilities=${pairingResult.capabilities}")
                _lastAuthResult.value = "تمت المصادقة بنجاح (${pairingResult.deviceStatus ?: "Approved"})"
                _connectionState.value = PosHubState.READY
                resetRetryPolicy()

                // Crucial fix: Start sending HeartbeatCallerAssistant every 10 seconds!
                startHeartbeatLoop()
            } else {
                val msg = if (message.isNotBlank()) message else (pairingResult.message ?: "رفض الخادم المصادقة")
                logError("POSHUB_AUTH_REJECTED: $msg")
                _lastAuthResult.value = "مرفوض: $msg"
                _lastError.value = msg
                _connectionState.value = PosHubState.FAILED
                scheduleReconnect()
            }
        } catch (e: Exception) {
            logError("POSHUB_AUTH_PARSE_FAILED: ${e.message}", e)
            _lastAuthResult.value = "خطأ في قراءة رد المصادقة: ${e.message}"
            _connectionState.value = PosHubState.FAILED
            scheduleReconnect()
        }
    }

    /**
     * Handles Server Invocations & Hub Events:
     * - "Ping" -> Respond immediately with HeartbeatCallerAssistant(clientId)
     * - "CallerAssistantReplaced" -> Handled gracefully
     * - "ServerDisconnect" -> Disconnect reason logged
     * - "CallerContextReceived" / "CallerCustomerContext" -> Dispatches customer context
     */
    private fun handleServerInvocation(ws: WebSocket, target: String, args: JSONArray?) {
        logInfo("POSHUB_SERVER_EVENT: target=$target")

        when {
            target.equals("Ping", ignoreCase = true) -> {
                // Must reply immediately with HeartbeatCallerAssistant(clientId)
                logInfo("POSHUB_EVENT_PING -> Sending immediate heartbeat response")
                sendHeartbeatImmediate()
            }

            target.equals("CallerAssistantReplaced", ignoreCase = true) -> {
                val reason = if (args != null && args.length() > 0) args.optString(0) else "تم فتح جلسة أحدث لهذا الجهاز"
                logError("POSHUB_CALLER_ASSISTANT_REPLACED: $reason")
                _lastDisconnectReason.value = "تم استبدال الاتصال بجلسة أحدث: $reason"
                _lastError.value = "تم فتح اتصال أحدث لنفس الجهاز من الخادم"
                stopHeartbeat()
                // Graceful backoff
                scheduleReconnect()
            }

            target.equals("ServerDisconnect", ignoreCase = true) -> {
                val reason = if (args != null && args.length() > 0) args.optString(0) else "قطع الاتصال من جهة الخادم"
                logError("POSHUB_SERVER_DISCONNECT: $reason")
                _lastDisconnectReason.value = reason
                _lastError.value = "أوقف الخادم الاتصال: $reason"
                stopHeartbeat()
                scheduleReconnect()
            }

            target.equals("CallerCustomerContext", ignoreCase = true) ||
            target.equals("OnCallerCustomerContext", ignoreCase = true) ||
            target.equals("CallerContextReceived", ignoreCase = true) -> {
                if (args != null && args.length() > 0) {
                    val argObj = args.optJSONObject(0)
                    if (argObj != null) {
                        val parsed = customerContextAdapter.fromJson(argObj.toString())
                        if (parsed != null) {
                            scope.launch {
                                _customerContextEvents.emit(parsed)
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Periodically invokes HeartbeatCallerAssistant(clientId) exactly every 10 seconds.
     * Prevents the 20-60 second disconnection timeout diagnosed on the server!
     */
    private fun startHeartbeatLoop() {
        stopHeartbeat()
        heartbeatJob = scope.launch {
            logInfo("POSHUB_HEARTBEAT_LOOP_STARTED (interval=10s, clientId=$activeClientId)")
            while (isActive && _connectionState.value == PosHubState.READY) {
                delay(10000L) // Exactly 10 seconds
                if (_connectionState.value == PosHubState.READY) {
                    sendHeartbeatImmediate()
                }
            }
        }
    }

    private fun stopHeartbeat() {
        heartbeatJob?.cancel()
        heartbeatJob = null
    }

    /**
     * Sends HeartbeatCallerAssistant(clientId: string) to /posHub.
     * If clientId is unknown, passes activeTrust.deviceId as fallback.
     */
    fun sendHeartbeatImmediate(): Boolean {
        val ws = webSocket ?: return false
        if (_connectionState.value != PosHubState.READY) return false

        val cid = activeClientId ?: activeTrust?.deviceId ?: return false
        val invId = "hb-" + invocationCounter.getAndIncrement()

        return try {
            val invocation = JSONObject().apply {
                put("type", 1)
                put("invocationId", invId)
                put("target", "HeartbeatCallerAssistant")
                put("arguments", JSONArray().put(cid))
            }
            ws.send(invocation.toString() + RECORD_SEPARATOR)
            _heartbeatCount.value++
            logInfo("POSHUB_HEARTBEAT_SENT (count=${_heartbeatCount.value}, cid=$cid)")
            true
        } catch (e: Exception) {
            logError("POSHUB_HEARTBEAT_SEND_FAILED: ${e.message}", e)
            false
        }
    }

    private fun handleHeartbeatCompletion(json: JSONObject) {
        val error = json.optString("error", "")
        if (error.isNotBlank()) {
            logError("POSHUB_HEARTBEAT_ERROR: $error")
            if (error.contains("SESSION_EXPIRED", ignoreCase = true) ||
                error.contains("UNAUTHORIZED", ignoreCase = true)
            ) {
                _lastError.value = "انتهت صلاحية جلسة الاتصال بالخادم (SESSION_EXPIRED)"
                _connectionState.value = PosHubState.RECONNECTING
                stopHeartbeat()
                scheduleReconnect()
            }
        }
    }

    /**
     * Invokes RecordCallerCall(CallerCallRecord) -> CallerCallRecord
     */
    fun recordCallerCall(record: CallRecordRequest): Boolean {
        val ws = webSocket ?: return false
        if (_connectionState.value != PosHubState.READY) return false

        return try {
            val recordJson = callRecordAdapter.toJson(record)
            val invId = "call-" + invocationCounter.getAndIncrement()
            val invocation = JSONObject().apply {
                put("type", 1)
                put("invocationId", invId)
                put("target", "RecordCallerCall")
                put("arguments", JSONArray().put(JSONObject(recordJson)))
            }
            ws.send(invocation.toString() + RECORD_SEPARATOR)
            logInfo("POSHUB_RECORD_CALL_SENT (callId=${record.callId}, phone=${record.phone})")
            true
        } catch (e: Exception) {
            logError("POSHUB_RECORD_CALL_ERROR: ${e.message}", e)
            false
        }
    }

    /**
     * Automatic reconnect backoff policy matching the server specification:
     * [0s, 2s, 5s, 10s, 15s, 30s]
     */
    private fun scheduleReconnect() {
        if (isManuallyStopped) return
        val trust = activeTrust ?: return
        reconnectJob?.cancel()

        reconnectJob = scope.launch {
            val delayMs = backoffDelaysMs.getOrElse(retryIndex) { 30000L }
            if (retryIndex < backoffDelaysMs.size - 1) {
                retryIndex++
            }
            _retryCount.value++
            val currentAttempt = _retryCount.value
            logInfo("POSHUB_RECONNECT_SCHEDULED (attempt=$currentAttempt, delay=${delayMs}ms)")

            if (delayMs > 0) {
                delay(delayMs)
            }

            connectionMutex.withLock {
                if (!isManuallyStopped && _connectionState.value != PosHubState.READY) {
                    logInfo("POSHUB_RECONNECT_RUNNING (attempt=$currentAttempt)")
                    connectSingleFlight(trust)
                }
            }
        }
    }

    fun onNetworkAvailable() {
        val trust = activeTrust ?: return
        if (isManuallyStopped) return
        if (_connectionState.value == PosHubState.DISCONNECTED ||
            _connectionState.value == PosHubState.RECONNECTING ||
            _connectionState.value == PosHubState.FAILED
        ) {
            scope.launch {
                connectionMutex.withLock {
                    logInfo("POSHUB_NETWORK_RESTORED -> immediate reconnect attempt")
                    retryIndex = 0
                    connectSingleFlight(trust)
                }
            }
        }
    }

    private fun resetRetryPolicy() {
        retryIndex = 0
        _retryCount.value = 0
        reconnectJob?.cancel()
    }

    fun stop() {
        isManuallyStopped = true
        stopHeartbeat()
        reconnectJob?.cancel()
        _connectionState.value = PosHubState.DISCONNECTED
        closeCurrentSocket()
        logInfo("POSHUB_STOPPED")
    }

    private fun closeCurrentSocket() {
        try {
            webSocket?.close(1000, "App paused or re-initializing")
        } catch (e: Exception) {
            // Ignore
        }
        webSocket = null
    }

    private fun formatTimestamp(date: Date): String {
        return SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(date)
    }

    private fun logInfo(msg: String) {
        try {
            android.util.Log.i(TAG, msg)
        } catch (_: Throwable) {
            println("$TAG: $msg")
        }
    }

    private fun logError(msg: String, tr: Throwable? = null) {
        try {
            android.util.Log.e(TAG, msg, tr)
        } catch (_: Throwable) {
            System.err.println("$TAG: $msg")
            tr?.printStackTrace()
        }
    }
}
