const express = require("express");
const tdl = require("tdl");
const { getTdjson } = require("prebuilt-tdlib");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");
const {
  NTgCalls,
  StreamMode,
  StreamType,
  StreamDevice,
  MediaSource,
  ConnectionState,
  VideoRotation,
  setLogLevel,
  LogLevel,
} = require("ntgcalls");

tdl.configure({
  tdjson: getTdjson(),
});

// NTG_LOG=info (or debug) turns on the call engine's detailed log.
const NTG_LOG_LEVEL =
  process.env.NTG_LOG === "debug"
    ? LogLevel.DEBUG
    : process.env.NTG_LOG === "info"
      ? LogLevel.INFO
      : LogLevel.WARNING;

setLogLevel(NTG_LOG_LEVEL);

const app = express();
app.use(express.json({ limit: "50kb" }));

const PORT = 3000;
const SESSION_ROOT = path.join(__dirname, "telegram-sessions");

// Recorded audio (raw PCM) goes in this folder.
const AUDIO_DIR = path.join(__dirname, "audio");
fs.mkdirSync(AUDIO_DIR, { recursive: true });

// Websites allowed to open the live-audio WebSocket.
// Add or change these if your pages live at other addresses.
const ALLOWED_ORIGINS = [
  "https://paramantra.us",
  "https://mirror.paramantra.com",
  "http://localhost:3000",
  "http://127.0.0.1:3000",
];

// One in-memory entry per Paramantra client_id.
const clients = Object.create(null);

// audio token -> { entry, userKey }  (one secret token per live call)
const audioTokens = Object.create(null);

function getClientId(value) {
  const id = String(value || "");

  // Use only safe directory-name characters.
  if (!/^[a-zA-Z0-9_-]{1,80}$/.test(id)) {
    throw new Error("Invalid client_id.");
  }

  return id;
}

/* ------------------------------------------------------------------ */
/* ntgcalls (media engine) helpers                                     */
/* ------------------------------------------------------------------ */

// Protocol advertised to Telegram comes from the media engine itself.
function buildCallProtocol() {
  const p = NTgCalls.getProtocol();

  // Optional test: CALL_VERSIONS="9.0.0,12.0.0,13.0.0" offers only some of the
  // versions ntgcalls supports. Values ntgcalls does not list are ignored.
  let versions = p.libraryVersions;

  if (process.env.CALL_VERSIONS) {
    const wanted = process.env.CALL_VERSIONS.split(",")
      .map(function (v) {
        return v.trim();
      })
      .filter(function (v) {
        return p.libraryVersions.indexOf(v) !== -1;
      });

    if (wanted.length) {
      versions = wanted;
    }
  }

  return {
    _: "callProtocol",
    udp_p2p: p.udpP2p,
    udp_reflector: p.udpReflector,
    min_layer: p.minLayer,
    max_layer: p.maxLayer,
    library_versions: versions,
  };
}

// Convert TDLib callServer objects into ntgcalls RTCServer objects.
// NOTE: this ntgcalls build wants byte fields as plain arrays, not Buffers.
function mapServers(servers) {
  return (servers || []).map(function (s) {
    const t = s.type || {};
    const isReflector = t._ === "callServerTypeTelegramReflector";

    return {
      id: BigInt(s.id),
      ipv4: s.ip_address || "",
      ipv6: s.ipv6_address || "",
      port: s.port,
      username: t.username || undefined,
      password: t.password || undefined,
      turn: !!t.supports_turn,
      stun: !!t.supports_stun,
      tcp: !!t.is_tcp,
      peerTag:
        isReflector && t.peer_tag
          ? Array.from(Buffer.from(t.peer_tag, "base64"))
          : undefined,
    };
  });
}

function sendToBrowser(info, message) {
  if (info && info.ws && info.ws.readyState === 1) {
    info.ws.send(JSON.stringify(message));
  }
}

function hangUp(entry, info, reason) {
  if (!info || !info.callId) {
    return Promise.resolve();
  }

  info.hangupRequested = reason || "unspecified";
  console.log("BRIDGE HANGING UP the call. Reason: " + info.hangupRequested);

  return entry.client
    .invoke({
      _: "discardCall",
      call_id: info.callId,
      is_disconnected: false,
      duration: 0,
      is_video: false,
      connection_id: "0",
    })
    .catch(function (err) {
      console.error("discardCall failed:", err.message || err);
    });
}

// One ntgcalls instance per Telegram account (client_id), created lazily.
function getNtg(entry) {
  if (entry.ntg) {
    return entry.ntg;
  }

  const ntg = new NTgCalls();

  // Media engine -> Telegram: signalling data produced by ntgcalls.
  ntg.onSignalingData(function (userId, data) {
    const info = entry.calls[String(userId)];

    if (!info || !info.callId) {
      return;
    }

    info.stats.sigOut++;

    entry.client
      .invoke({
        _: "sendCallSignalingData",
        call_id: info.callId,
        data: Buffer.from(data).toString("base64"),
      })
      .catch(function (err) {
        console.error(
          "sendCallSignalingData failed [" + entry.clientId + "]:",
          err.message || err,
        );
      });
  });

  ntg.onConnectionChange(function (userId, state) {
    console.log(
      "NTGCALLS CONNECTION [" + entry.clientId + "] user " + userId + ":",
      JSON.stringify(state),
    );

    const info = entry.calls[String(userId)];

    if (info && state && state.state === ConnectionState.CONNECTED) {
      info.connected = true;

      if (info.externalTone) {
        startTone(entry, String(userId));
      } else if (!info.audioFile) {
        startMicPacer(entry, String(userId));
      }
      sendToBrowser(info, { type: "state", state: "audio_connected" });
    }
  });

  ntg.onRemoteSourceChange(function (userId, state) {
    console.log("REMOTE SOURCE [" + entry.clientId + "]:", JSON.stringify(state));
  });

  ntg.onUpgrade(function (userId, state) {
    console.log("MEDIA STATE [" + entry.clientId + "]:", JSON.stringify(state));
  });

  // Telegram -> browser: audio from the person we called.
  ntg.onFrames(function (userId, mode, device, frames) {
    // Ignore our own outgoing (capture) frames.
    if (mode === StreamMode.CAPTURE) {
      return;
    }

    const info = entry.calls[String(userId)];

    if (!info) {
      return;
    }

    for (let i = 0; i < frames.length; i++) {
      const bytes = Buffer.from(frames[i].data);
      info.stats.remoteFrames++;

      if (!info.gotFrames) {
        info.gotFrames = true;
        console.log(
          "First audio frame from remote: mode=" + mode + " device=" + device +
            " bytes=" + bytes.length,
        );
      }

      if (info.ws && info.ws.readyState === 1) {
        info.ws.send(bytes, { binary: true });
      }
    }
  });

  // Recorded audio finished playing -> hang up.
  ntg.onStreamEnd(function (userId, type) {
    if (type !== StreamType.AUDIO) {
      return;
    }

    const info = entry.calls[String(userId)];

    if (!info || !info.audioFile) {
      return;
    }

    console.log("Audio finished, hanging up call:", info.callId);

    setTimeout(function () {
      hangUp(entry, info, "audio file finished playing");
    }, 1000);
  });

  entry.ntg = ntg;

  return ntg;
}

// Hand a "ready" call over to the media engine.
async function startMedia(entry, call) {
  const state = call.state;
  const info = entry.calls[String(call.user_id)];
  const userId = BigInt(call.user_id);
  const ntg = getNtg(entry);

  // Log the ready state, but never log the encryption key.
  const safeState = Object.assign({}, state, { encryption_key: "[hidden]" });
  console.log("CALL READY STATE [" + entry.clientId + "]:");
  console.log(JSON.stringify(safeState, null, 2));

  info.step = "createP2pCall";
  await ntg.createP2pCall(userId);
  console.log("OK: createP2pCall");

  // TDLib already did the key exchange, so give ntgcalls the final key.
  info.step = "skipExchange";
  await ntg.skipExchange(
    userId,
    Array.from(Buffer.from(state.encryption_key, "base64")),
    !!call.is_outgoing,
  );
  console.log("OK: skipExchange");

  info.step = "setStreamSources";

  if (info.audioFile) {
    // Recorded audio from a raw PCM file.
    await ntg.setStreamSources(userId, StreamMode.CAPTURE, {
      microphone: {
        mediaSource: MediaSource.FILE,
        input: info.audioFile,
        sampleRate: 48000,
        channelCount: 2,
        keepOpen: false,
      },
    });
  } else {
    // Live audio pushed from the browser: mono, 48 kHz, 16-bit PCM.
    await ntg.setStreamSources(userId, StreamMode.CAPTURE, {
      microphone: {
        mediaSource: MediaSource.EXTERNAL,
        input: "",
        sampleRate: 48000,
        channelCount: 1,
        keepOpen: true,
      },
    });
  }

  // Live call: also receive the other person's voice (speaker side).
  if (!info.audioFile) {
    try {
      // Test mode: RECORD_INCOMING=1 writes the other person's audio to a file.
      const recordFile =
        info.recordIncoming || process.env.RECORD_INCOMING === "1"
          ? path.join(AUDIO_DIR, "incoming_" + Date.now() + ".raw")
          : "";

      await ntg.setStreamSources(userId, StreamMode.PLAYBACK, {
        speaker: {
          mediaSource: recordFile ? MediaSource.FILE : MediaSource.EXTERNAL,
          input: recordFile,
          sampleRate: 48000,
          channelCount: 1,
          keepOpen: true,
        },
      });
      console.log(
        "OK: setStreamSources (speaker)" +
          (recordFile ? " RECORDING TO " + recordFile : ""),
      );
    } catch (err) {
      console.error("Speaker setup failed:", err.stack || err);
    }
  }

  console.log("OK: setStreamSources");

  // info.step = "connectP2p";
  // await ntg.connectP2p(
  //   userId,
  //   mapServers(state.servers),
  //   (state.protocol && state.protocol.library_versions) || [],
  //   !!state.allow_p2p,
  //   state.custom_parameters || "{}",
  // );
  // console.log("OK: connectP2p");

  
  // Connect the P2P call first.
  info.step = "connectP2p";

  await ntg.connectP2p(
    userId,
    mapServers(state.servers),
    (state.protocol && state.protocol.library_versions) || [],
    !!state.allow_p2p,
    state.custom_parameters || "{}",
  );

  console.log("OK: connectP2p");

  // Diagnostic test: receive remote audio through PLAYBACK.
  // Keep incoming recording disabled during this test.
  if (!info.audioFile) {
    try {
      info.step = "setIncomingPlayback";

      await ntg.setStreamSources(userId, StreamMode.PLAYBACK, {
        microphone: {
          mediaSource: MediaSource.EXTERNAL,
          input: "",
          sampleRate: 48000,
          channelCount: 1,
          keepOpen: true,
        },
      });

      console.log("OK: setStreamSources (incoming playback test)");
    } catch (err) {
      console.error(
        "Incoming playback setup failed:",
        err.stack || err,
      );
      throw err;
    }
  }

  info.timer = setInterval(function () {
    const s = info.stats;
    console.log(
      "STATS [" + entry.clientId + "]: browserBytes=" + s.browserBytes +
        " framesSentToTelegram=" + s.framesSent +
        " framesFromRemote=" + s.remoteFrames +
        " signalingIn=" + s.sigIn +
        " signalingOut=" + s.sigOut +
        " micPeak=" + s.peak +
        " underruns=" + s.underruns,
    );
    s.peak = 0;
  }, 5000);
}

// Clean up when a call ends.
async function stopMedia(entry, call) {
  const key = String(call.user_id);
  const info = entry.calls[key];

  delete entry.calls[key];

  if (info && info.timer) {
    clearInterval(info.timer);
  }

  if (info && info.toneTimer) {
    clearInterval(info.toneTimer);
  }

  if (info && info.micTimer) {
    clearInterval(info.micTimer);
  }

  if (info && info.token) {
    delete audioTokens[info.token];
  }

  if (info && info.ws) {
    try {
      info.ws.close(1000, "call ended");
    } catch (e) {}
    info.ws = null;
  }

  if (info && info.started && entry.ntg) {
    try {
      await entry.ntg.stop(BigInt(call.user_id));
    } catch (err) {
      console.log("ntgcalls stop:", err.message || err);
    }
  }
}

// Main call lifecycle handler (TDLib updateCall).
async function handleCallUpdate(entry, update) {
  const call = update.call;
  const state = call.state;
  const key = String(call.user_id);
  const info = entry.calls[key];

  if (!info) {
    return;
  }

  info.callId = call.id;
  info.lastState = state._;
  sendToBrowser(info, { type: "state", state: state._ });

  if (state._ === "callStateReady" && !info.started) {
    info.started = true;

    try {
      await startMedia(entry, call);
    } catch (err) {
      console.error(
        "Starting media failed at step '" + info.step + "' [" + entry.clientId + "]:",
        err.stack || err,
      );

      hangUp(entry, info, "starting the audio failed");
    }

    return;
  }

  if (state._ === "callStateDiscarded" || state._ === "callStateError") {
    console.log(
      "CALL ENDED. Telegram reason: " + JSON.stringify(state.reason || state._) +
        (info.hangupRequested
          ? " (the bridge ended it: " + info.hangupRequested + ")"
          : " (the bridge did NOT hang up; B or Telegram ended it)"),
    );

    await stopMedia(entry, call);
  }
}

/* ------------------------------------------------------------------ */
/* Live audio from the browser                                         */
/* ------------------------------------------------------------------ */

// 10 ms of mono 48 kHz 16-bit audio = 480 samples = 960 bytes.
const FRAME_BYTES = 960;

function pushAudio(entry, userKey, data) {
  const info = entry.calls[userKey];

  if (!info || info.audioFile || info.externalTone) {
    return;
  }

  if (!Buffer.isBuffer(data)) {
    data = Buffer.concat(Array.isArray(data) ? data : [Buffer.from(data)]);
  }

  // Track the loudest sample so we can see whether the microphone is silent.
  for (let i = 0; i + 1 < data.length; i += 2) {
    const v = Math.abs(data.readInt16LE(i));

    if (v > info.stats.peak) {
      info.stats.peak = v;
    }
  }

  info.pcm = Buffer.concat([info.pcm, data]);

  // If more than 0.4 s is queued, keep only the newest 0.2 s so the delay stays low.
  if (info.pcm.length > 38400) {
    info.pcm = info.pcm.subarray(info.pcm.length - 19200);
  }

  if (info.connected) {
    startMicPacer(entry, userKey);
  }
}

// Wait for about 100 ms of audio before sending, then send one 10 ms frame at a
// time on a steady clock. If the queue runs dry, pause and refill the buffer
// instead of leaving gaps in the middle of speech.
const PREBUFFER_BYTES = 9600;

function startMicPacer(entry, userKey) {
  const info = entry.calls[userKey];

  if (!info || info.micTimer || !entry.ntg) {
    return;
  }

  let buffering = true;
  let t0 = 0;
  let sent = 0;

  info.micTimer = setInterval(function () {
    if (buffering) {
      if (info.pcm.length < PREBUFFER_BYTES) {
        return;
      }

      buffering = false;
      t0 = Date.now();
      sent = 0;
    }

    const due = Math.floor((Date.now() - t0) / 10);

    while (sent < due) {
      if (info.pcm.length < FRAME_BYTES) {
        info.stats.underruns++;
        buffering = true;
        break;
      }

      const chunk = info.pcm.subarray(0, FRAME_BYTES);
      info.pcm = info.pcm.subarray(FRAME_BYTES);

      sent++;
      info.stats.framesSent++;

      entry.ntg
        .sendExternalFrame(
          BigInt(userKey),
          StreamDevice.MICROPHONE,
          Array.from(chunk),
          {
            absoluteCaptureTimestampMs: BigInt(Date.now()),
            rotation: VideoRotation.VIDEO_ROTATION_0,
            width: 0,
            height: 0,
          },
        )
        .catch(function (err) {
          if (!info.frameErrorLogged) {
            info.frameErrorLogged = true;
            console.error("sendExternalFrame failed:", err.stack || err);
          }
        });
    }
  }, 5);
}

// Test: the bridge makes its own 440 Hz beep and sends it through the SAME
// live path the microphone uses (sendExternalFrame), paced in real time.
function startTone(entry, userKey) {
  const info = entry.calls[userKey];

  if (!info || info.toneTimer || !entry.ntg) {
    return;
  }

  const t0 = Date.now();
  let sent = 0;

  console.log("EXTERNAL TONE test started (6 seconds, 440 Hz)");

  info.toneTimer = setInterval(function () {
    const elapsed = Date.now() - t0;

    if (elapsed > 6000) {
      clearInterval(info.toneTimer);
      console.log("EXTERNAL TONE test finished, frames sent: " + sent);

      setTimeout(function () {
        hangUp(entry, info, "tone test finished");
      }, 500);

      return;
    }

    const due = Math.floor(elapsed / 10);

    while (sent < due) {
      const buf = Buffer.alloc(FRAME_BYTES);

      for (let i = 0; i < 480; i++) {
        const v = Math.round(
          Math.sin((2 * Math.PI * 440 * (sent * 480 + i)) / 48000) * 0.3 * 32767,
        );
        buf.writeInt16LE(v, i * 2);
      }

      sent++;
      info.stats.framesSent++;

      entry.ntg
        .sendExternalFrame(
          BigInt(userKey),
          StreamDevice.MICROPHONE,
          Array.from(buf),
          {
            absoluteCaptureTimestampMs: BigInt(Date.now()),
            rotation: VideoRotation.VIDEO_ROTATION_0,
            width: 0,
            height: 0,
          },
        )
        .catch(function (err) {
          if (!info.frameErrorLogged) {
            info.frameErrorLogged = true;
            console.error("sendExternalFrame failed:", err.stack || err);
          }
        });
    }
  }, 5);
}

function onAudioSocket(ws, ref) {
  const entry = ref.entry;
  const userKey = ref.userKey;
  const info = entry.calls[userKey];

  if (!info || info.ws) {
    ws.close(1008, "not available");
    return;
  }

  info.ws = ws;
  sendToBrowser(info, { type: "state", state: info.lastState || "waiting" });
  sendToBrowser(info, { type: "mode", mode: info.mode });

  if (info.connected) {
    sendToBrowser(info, { type: "state", state: "audio_connected" });
  }

  console.log("Browser audio connected for client " + entry.clientId);

  ws.on("message", function (data, isBinary) {
    if (isBinary) {
      info.stats.browserBytes += data.length;
      pushAudio(entry, userKey, data);
      return;
    }

    try {
      const msg = JSON.parse(data.toString());

      if (msg && msg.type === "hangup") {
        hangUp(entry, info, "Hang up button on the talk page");
      }
    } catch (e) {}
  });

  ws.on("close", function () {
    console.log("Browser audio closed for client " + entry.clientId);

    if (info.ws === ws) {
      info.ws = null;
      // Browser left -> end the call.
      hangUp(entry, info, "talk page closed, reloaded or lost its connection");
    }
  });

  ws.on("error", function () {});
}

/* ------------------------------------------------------------------ */
/* TDLib client management                                             */
/* ------------------------------------------------------------------ */

function getOrCreateClient(data) {
  const clientId = getClientId(data.client_id);
  const apiId = parseInt(data.api_id, 10);
  const apiHash = String(data.api_hash || "");

  if (!apiId || !apiHash) {
    throw new Error("api_id and api_hash are required.");
  }

  let entry = clients[clientId];

  if (entry) {
    // Do not silently reuse a session with different API credentials.
    if (entry.apiId !== apiId || entry.apiHash !== apiHash) {
      throw new Error(
        "API credentials differ from those already loaded for this client_id. " +
          "Use the same credentials for this session.",
      );
    }

    return entry;
  }

  const sessionDir = path.join(SESSION_ROOT, clientId);

  const client = tdl.createClient({
    apiId: apiId,
    apiHash: apiHash,
    databaseDirectory: sessionDir,
    filesDirectory: path.join(sessionDir, "files"),
  });

  entry = {
    clientId: clientId,
    apiId: apiId,
    apiHash: apiHash,
    client: client,
    telegramPhone: "",
    status: "not_logged_in",
    requestedStep: null,
    inputResolver: null,
    loginPromise: null,
    lastError: null,
    ntg: null,
    calls: Object.create(null), // telegram user id -> call info
  };

  client.on("error", function (err) {
    console.error(
      "TDLib error for client " + clientId + ":",
      err.message || err,
    );
  });

  client.on("update", function (update) {
    if (update._ === "updateCall") {
      console.log(
        "CALL STATE [" + clientId + "]:",
        update.call && update.call.state ? update.call.state._ : "?",
        update.call && update.call.state && update.call.state.reason
          ? JSON.stringify(update.call.state.reason)
          : "",
      );

      handleCallUpdate(entry, update).catch(function (err) {
        console.error("handleCallUpdate error:", err.message || err);
      });

      return;
    }

    // Telegram -> media engine: signalling data from the other side.
    if (update._ === "updateNewCallSignalingData") {
      const keys = Object.keys(entry.calls);

      for (let i = 0; i < keys.length; i++) {
        const info = entry.calls[keys[i]];

        if (info.callId === update.call_id && info.started && entry.ntg) {
          info.stats.sigIn++;

          entry.ntg
            .sendSignalingData(
              BigInt(keys[i]),
              Array.from(Buffer.from(update.data, "base64")),
            )
            .catch(function (err) {
              console.error("sendSignalingData failed:", err.message || err);
            });
        }
      }
    }
  });

  clients[clientId] = entry;

  console.log("Created Telegram client for:", clientId);

  return entry;
}

// Wait for a code or password to be submitted through an HTTP endpoint.
function waitForInput(entry, step) {
  return new Promise(function (resolve) {
    entry.requestedStep = step;
    entry.inputResolver = resolve;
    entry.status = "waiting_" + step;
    entry.lastError = null;

    console.log(
      "Client " + entry.clientId + " is waiting for Telegram " + step + ".",
    );
  });
}

// Start the TDLib login flow only when one isn't already running.
function startLogin(entry, telegramPhone) {
  if (entry.loginPromise) {
    return entry.loginPromise;
  }

  if (telegramPhone) {
    entry.telegramPhone = telegramPhone;
  }

  entry.lastError = null;

  const loginDetails = {
    getPhoneNumber: async function () {
      if (!entry.telegramPhone) {
        throw new Error(
          "Telegram account phone is missing. Start login with telegram_phone.",
        );
      }

      return entry.telegramPhone;
    },

    getAuthCode: async function (retry) {
      if (retry) {
        entry.lastError =
          "The previous login code was rejected. Submit a new code.";
      }

      return await waitForInput(entry, "code");
    },

    getPassword: async function (passwordHint, retry) {
      if (retry) {
        entry.lastError =
          "The previous 2FA password was rejected. Submit it again.";
      }

      entry.passwordHint = passwordHint || "";

      return await waitForInput(entry, "password");
    },

    getName: async function () {
      return {
        firstName: "Paramantra",
        lastName: "User",
      };
    },
  };

  entry.status = "logging_in";

  const loginPromise = entry.client
    .login(loginDetails)
    .then(function () {
      entry.status = "authorized";
      entry.requestedStep = null;
      entry.inputResolver = null;
      entry.lastError = null;

      console.log("Telegram login successful for client:", entry.clientId);
    })
    .catch(function (err) {
      entry.status = "login_error";
      entry.requestedStep = null;
      entry.inputResolver = null;
      entry.lastError = err.message || String(err);

      console.error(
        "Telegram login failed for client " + entry.clientId + ":",
        entry.lastError,
      );

      throw err;
    })
    .finally(function () {
      entry.loginPromise = null;
    });

  entry.loginPromise = loginPromise;

  // Handle the rejection here too, because login starts in the background
  // when called from the HTTP endpoint.
  loginPromise.catch(function () {});

  return loginPromise;
}

// Verify that a stored Telegram session is usable.
// If this client has not been authenticated, do not prompt in the terminal.
async function ensureAuthorized(entry) {
  if (entry.status === "authorized") {
    return;
  }

  if (entry.loginPromise) {
    await entry.loginPromise;
  } else {
    const loginDetails = {
      getPhoneNumber: async function () {
        throw new Error(
          "Telegram login required. Call POST /telegram/login first.",
        );
      },
      getAuthCode: async function () {
        throw new Error(
          "Telegram login required. Call POST /telegram/login first.",
        );
      },
      getPassword: async function () {
        throw new Error(
          "Telegram login required. Call POST /telegram/login first.",
        );
      },
    };

    entry.loginPromise = entry.client
      .login(loginDetails)
      .then(function () {
        entry.status = "authorized";
      })
      .catch(function (err) {
        entry.status = "not_logged_in";
        throw err;
      })
      .finally(function () {
        entry.loginPromise = null;
      });

    await entry.loginPromise;
  }

  if (entry.status !== "authorized") {
    throw new Error(
      "Telegram session is not authorized. Complete POST /telegram/login first.",
    );
  }
}

/* ------------------------------------------------------------------ */
/* HTTP endpoints                                                      */
/* ------------------------------------------------------------------ */

// Basic health endpoint for a browser.
app.get("/", function (req, res) {
  res.json({
    status: "ok",
    message: "Telegram Bridge Running",
  });
});

// Talk page: open as /talk?token=... (token comes from POST /calls).
app.get("/talk", function (req, res) {
  res.sendFile(path.join(__dirname, "public", "talk.html"));
});

// Start one-time Telegram login for a tenant.
// POST /telegram/login
// Body: client_id, api_id, api_hash, telegram_phone
app.post("/telegram/login", function (req, res) {
  try {
    const data = req.body || {};

    if (
      !data.client_id ||
      !data.api_id ||
      !data.api_hash ||
      !data.telegram_phone
    ) {
      return res.status(400).json({
        status: "error",
        message: "client_id, api_id, api_hash and telegram_phone are required.",
      });
    }

    const entry = getOrCreateClient(data);

    if (entry.status === "authorized") {
      return res.json({
        status: "ok",
        message: "This Telegram account is already logged in.",
        client_id: entry.clientId,
      });
    }

    if (entry.loginPromise) {
      return res.json({
        status: "pending",
        message: "Telegram login is already in progress.",
        client_id: entry.clientId,
        next_step: entry.requestedStep,
      });
    }

    entry.telegramPhone = String(data.telegram_phone).trim();

    startLogin(entry, entry.telegramPhone).catch(function () {});

    return res.status(202).json({
      status: "pending",
      message: "Telegram login started. Check login status for the next step.",
      client_id: entry.clientId,
    });
  } catch (err) {
    return res.status(400).json({
      status: "error",
      message: err.message,
    });
  }
});

// Check which login step is needed.
// GET /telegram/login/status/:client_id
app.get("/telegram/login/status/:client_id", function (req, res) {
  try {
    const clientId = getClientId(req.params.client_id);
    const entry = clients[clientId];

    if (!entry) {
      return res.status(404).json({
        status: "error",
        message: "No Telegram client found. Start login first.",
      });
    }

    return res.json({
      status: entry.status,
      client_id: clientId,
      next_step: entry.requestedStep,
      message: entry.lastError || "",
      password_hint: entry.passwordHint || "",
    });
  } catch (err) {
    return res.status(400).json({
      status: "error",
      message: err.message,
    });
  }
});

// Submit the Telegram OTP.
// POST /telegram/login/code
// Body: client_id, code
app.post("/telegram/login/code", function (req, res) {
  const data = req.body || {};
  const clientId = String(data.client_id || "");
  const entry = clients[clientId];

  if (!entry || entry.requestedStep !== "code" || !entry.inputResolver) {
    return res.status(409).json({
      status: "error",
      message: "This client is not currently waiting for a login code.",
    });
  }

  if (!data.code || !String(data.code).trim()) {
    return res.status(400).json({
      status: "error",
      message: "code is required.",
    });
  }

  const resolve = entry.inputResolver;
  entry.inputResolver = null;
  entry.requestedStep = null;
  entry.status = "verifying_code";

  resolve(String(data.code).trim());

  return res.json({
    status: "ok",
    message: "Login code submitted. Check login status.",
    client_id: clientId,
  });
});

// Submit the Telegram two-step verification password.
// POST /telegram/login/password
// Body: client_id, password
app.post("/telegram/login/password", function (req, res) {
  const data = req.body || {};
  const clientId = String(data.client_id || "");
  const entry = clients[clientId];

  if (!entry || entry.requestedStep !== "password" || !entry.inputResolver) {
    return res.status(409).json({
      status: "error",
      message: "This client is not currently waiting for a 2FA password.",
    });
  }

  if (!data.password) {
    return res.status(400).json({
      status: "error",
      message: "password is required.",
    });
  }

  const resolve = entry.inputResolver;
  entry.inputResolver = null;
  entry.requestedStep = null;
  entry.status = "verifying_password";

  resolve(String(data.password));

  return res.json({
    status: "ok",
    message: "Password submitted. Check login status.",
    client_id: clientId,
  });
});

// Make an outgoing Telegram call.
// POST /calls
// Body: client_id, api_id, api_hash, phone, [call_uid], [is_video], [audio_file]
// - audio_file given  -> plays that recorded raw PCM file from the audio folder
// - audio_file absent -> live call; response contains audio_token for the browser
app.post("/calls", async function (req, res) {
  let entry = null;
  let userKey = null;
  let token = "";

  try {
    const data = req.body || {};

    if (!data.client_id || !data.api_id || !data.api_hash || !data.phone) {
      return res.status(400).json({
        status: "error",
        message: "client_id, api_id, api_hash and phone are required.",
      });
    }

    // Validate the optional audio file name (no folders allowed).
    let audioFile = "";

    if (data.audio_file) {
      const name = String(data.audio_file);

      if (!/^[a-zA-Z0-9_.-]{1,100}$/.test(name)) {
        return res.status(400).json({
          status: "error",
          message: "Invalid audio_file name.",
        });
      }

      audioFile = path.join(AUDIO_DIR, name);

      if (!fs.existsSync(audioFile)) {
        return res.status(400).json({
          status: "error",
          message: "audio_file not found in the bridge audio folder.",
        });
      }
    }

    const callMode = audioFile
      ? "file"
      : data.external_tone === true ||
          data.external_tone === 1 ||
          data.external_tone === "1"
        ? "tone_test"
        : "live";

    console.log(
      "CALL MODE: " + callMode +
        (callMode === "file"
          ? " (plays the file, then hangs up; microphone NOT used)"
          : callMode === "tone_test"
            ? " (bridge beep, hangs up after 6 s; microphone NOT used)"
            : " (live microphone from the talk page; no automatic hang-up)"),
    );

    console.log("CALL REQUEST:", {
      client_id: data.client_id,
      call_uid: data.call_uid || "",
      phone: data.phone,
      is_video: !!data.is_video,
      audio_file: data.audio_file || "",
    });

    entry = getOrCreateClient(data);

    await ensureAuthorized(entry);

    console.log("Importing contact for client:", entry.clientId);

    const imported = await entry.client.invoke({
      _: "importContacts",
      contacts: [
        {
          _: "importedContact",
          phone_number: String(data.phone).trim(),
          first_name: "Paramantra",
          last_name: "",
        },
      ],
    });

    if (!imported.user_ids || !imported.user_ids.length) {
      return res.status(404).json({
        status: "error",
        message: "Telegram user was not found for this phone number.",
        call_uid: data.call_uid || "",
      });
    }

    const userId = imported.user_ids[0];
    userKey = String(userId);

    if (entry.calls[userKey]) {
      return res.status(409).json({
        status: "error",
        message: "A call to this Telegram user is already in progress.",
        call_uid: data.call_uid || "",
      });
    }

    // Secret token the browser must present to send live audio.
    if (!audioFile) {
      token = crypto.randomBytes(24).toString("hex");
      audioTokens[token] = { entry: entry, userKey: userKey };
    }

    // Register the call BEFORE createCall so no update is missed.
    entry.calls[userKey] = {
      callId: null,
      mode: callMode,
      audioFile: audioFile,
      token: token,
      ws: null,
      pcm: Buffer.alloc(0),
      started: false,
      connected: false,
      lastState: "",
      step: "",
      timer: null,
      recordIncoming:
        data.record_incoming === true ||
        data.record_incoming === 1 ||
        data.record_incoming === "1",
      externalTone:
        data.external_tone === true ||
        data.external_tone === 1 ||
        data.external_tone === "1",
      toneTimer: null,
      micTimer: null,
      stats: { browserBytes: 0, framesSent: 0, remoteFrames: 0, sigIn: 0, sigOut: 0, peak: 0, underruns: 0 },
    };

    const protocol = buildCallProtocol();

    console.log("CALL PROTOCOL BEING SENT:");
    console.log(JSON.stringify(protocol, null, 2));

    const result = await entry.client.invoke({
      _: "createCall",
      user_id: userId,
      protocol: protocol,
      is_video:
        data.is_video === true || data.is_video === 1 || data.is_video === "1",
    });

    entry.calls[userKey].callId = result.id;

    console.log("Telegram call requested:", {
      client_id: entry.clientId,
      call_uid: data.call_uid || "",
      telegram_user_id: userId,
    });

    return res.json({
      status: "ok",
      message: "Telegram call requested.",
      client_id: entry.clientId,
      call_uid: data.call_uid || "",
      telegram_user_id: userId,
      mode: callMode,
      audio_token: token,
      telegram_call: result,
    });
  } catch (err) {
    console.error("Call request failed:", err.message || err);

    if (entry && userKey && entry.calls[userKey] && !entry.calls[userKey].started) {
      delete entry.calls[userKey];
    }

    if (token) {
      delete audioTokens[token];
    }

    return res.status(400).json({
      status: "error",
      message: err.message || "Telegram call failed.",
    });
  }
});

// Hang up a live call.
// POST /calls/hangup
// Body: audio_token
app.post("/calls/hangup", async function (req, res) {
  const data = req.body || {};
  const ref = audioTokens[String(data.audio_token || "")];

  if (!ref) {
    return res.status(404).json({
      status: "error",
      message: "No active call for this token.",
    });
  }

  await hangUp(ref.entry, ref.entry.calls[ref.userKey], "POST /calls/hangup request");

  return res.json({ status: "ok", message: "Hang-up requested." });
});

/* ------------------------------------------------------------------ */
/* Server + WebSocket                                                  */
/* ------------------------------------------------------------------ */

const server = app.listen(PORT, function () {
  console.log("Telegram Bridge running on port " + PORT + " (build: recording-test-9)");
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", function (req, socket, head) {
  function reject(code, text) {
    socket.write("HTTP/1.1 " + code + " " + text + "\r\n\r\n");
    socket.destroy();
  }

  let url;

  try {
    url = new URL(req.url, "http://localhost");
  } catch (e) {
    return reject(400, "Bad Request");
  }

  if (url.pathname !== "/audio") {
    return reject(404, "Not Found");
  }

  const origin = String(req.headers.origin || "");

  if (ALLOWED_ORIGINS.indexOf(origin) === -1) {
    console.log("Rejected WebSocket from origin:", origin);
    return reject(403, "Forbidden");
  }

  const ref = audioTokens[url.searchParams.get("token") || ""];

  if (!ref) {
    return reject(401, "Unauthorized");
  }

  wss.handleUpgrade(req, socket, head, function (ws) {
    onAudioSocket(ws, ref);
  });
});