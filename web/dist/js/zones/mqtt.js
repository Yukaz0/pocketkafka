// PocketKafka MQTT Studio Zone Views & Components
import { state, esc, rel, fmtBytes, toast } from '../state.js';
import { api } from '../api.js';

function tableScroll(content) {
  return `<div class="tableScroll">${content}</div>`;
}

function pageHeader(title, desc) {
  return `<div class="pageHeading">
    <div><h2>${esc(title)}</h2><p>${esc(desc)}</p></div>
  </div>`;
}

function viewMQTTOverview() {
  const ms = state.mqttStatus || {};
  const enabled = !!ms.enabled;
  const port = (ms.listen || '').replace(/^.*:/, '') || '1883';
  const clients = ms.clients || [];
  const bridged = ms.bridged ?? 0;

  return `<div>
    ${pageHeader('MQTT Overview', 'Monitor listener status, active connections, and telemetry bridge metrics.')}
    <div class="row" style="gap:12px;flex-wrap:wrap;margin-bottom:16px">
      <div class="card" style="flex:1 1 180px;padding:14px 18px">
        <div class="muted" style="font-size:10px;text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px">Listener</div>
        <div style="font-size:14px;font-weight:600">${enabled
          ? `<span class="badge b-green">ACTIVE</span> <span class="mono" style="font-size:12px">:${esc(port)}</span>`
          : '<span class="badge muted">DISABLED</span>'}</div>
      </div>
      <div class="card" style="flex:1 1 180px;padding:14px 18px">
        <div class="muted" style="font-size:10px;text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px">Active Connections</div>
        <div class="statXL" style="font-size:24px">${ms.connections ?? 0}<small>conn</small></div>
      </div>
      <div class="card" style="flex:1 1 180px;padding:14px 18px">
        <div class="muted" style="font-size:10px;text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px">Bridged to Kafka</div>
        <div class="statXL" style="font-size:24px">${Number(bridged).toLocaleString()}<small>msgs</small></div>
      </div>
    </div>

    <div class="card" style="margin-bottom:16px">
      <div class="kicker">Quick Navigation</div>
      <h3 style="margin-bottom:12px">MQTT Studio Tools</h3>
      <div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(220px, 1fr));gap:12px">
        <div style="border:1px solid var(--border);border-radius:4px;padding:14px;cursor:pointer" onclick="goView('mqtt-tracker')">
          <div style="font-weight:600;font-size:13px;margin-bottom:4px;display:flex;align-items:center;gap:6px">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--primary-text)"><path d="M2 12h4l3-7 5 14 3-7h5"/></svg>
            <span>Live Tracker</span>
          </div>
          <div class="muted" style="font-size:11px">Inspect real-time telemetry stream across mqtt-* topics.</div>
        </div>
        <div style="border:1px solid var(--border);border-radius:4px;padding:14px;cursor:pointer" onclick="goView('mqtt-clients')">
          <div style="font-weight:600;font-size:13px;margin-bottom:4px;display:flex;align-items:center;gap:6px">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--primary-text)"><circle cx="9" cy="8" r="3"/><path d="M3 20v-2a6 6 0 0 1 12 0v2zM17 5a3 3 0 0 1 0 6M18 14a5 5 0 0 1 3 4v2"/></svg>
            <span>Active Clients (${clients.length})</span>
          </div>
          <div class="muted" style="font-size:11px">Inspect connected client IDs, IP addresses, and uptime.</div>
        </div>
        <div style="border:1px solid var(--border);border-radius:4px;padding:14px;cursor:pointer" onclick="goView('mqtt-publisher')">
          <div style="font-weight:600;font-size:13px;margin-bottom:4px;display:flex;align-items:center;gap:6px">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--primary-text)"><path d="M22 2 11 13"/><path d="m22 2-7 20-4-9-9-4z"/></svg>
            <span>Web Publisher</span>
          </div>
          <div class="muted" style="font-size:11px">Publish test MQTT messages directly from the dashboard.</div>
        </div>
        <div style="border:1px solid var(--border);border-radius:4px;padding:14px;cursor:pointer" onclick="goView('mqtt-mapping')">
          <div style="font-weight:600;font-size:13px;margin-bottom:4px;display:flex;align-items:center;gap:6px">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--primary-text)"><circle cx="12" cy="5" r="2"/><circle cx="5" cy="19" r="2"/><circle cx="19" cy="19" r="2"/><path d="M12 7v4M10.5 13 6.5 17M13.5 13l4 4M8 11h8"/></svg>
            <span>Topic Mapping</span>
          </div>
          <div class="muted" style="font-size:11px">Review the MQTT topic to Kafka topic conversion rules.</div>
        </div>
      </div>
    </div>
  </div>`;
}

function viewMQTTTracker() {
  const ss = state._mqttStream || {};
  return `<div>
    ${pageHeader('Live MQTT Stream Tracker', 'Track real-time MQTT message flow across derived Kafka topics with path hierarchy decoding.')}
    <div class="card">
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap">
        <input id="mqttTopicFilter" placeholder="Filter by MQTT topic (e.g. sensors/room1)" style="flex:1;min-width:200px;max-width:320px"
          oninput="mqttApplyStreamFilter(this.value)" value="${esc(ss.filter || '')}">
        <button class="pill ${ss.active ? 'active' : ''}" id="mqttTailBtn" onclick="mqttToggleTail()">
          ${ss.active ? '<span class="live-dot"></span> Pause' : '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" stroke="none" style="vertical-align:-1px;margin-right:4px"><polygon points="5 3 19 12 5 21 5 3"/></svg> Live Tail'}
        </button>
        <button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="mqttClearStream()">Clear Stream</button>
      </div>
      <div id="mqttStreamOut" style="background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;min-height:220px;max-height:460px;overflow:auto;font:11.5px/1.6 var(--font-mono);padding:10px 14px">
        ${ss.msgs && ss.msgs.length ? ss.msgs.map(formatStreamItem).join('') : '<div class="muted" style="text-align:center;padding:40px 0">Click <b>Live Tail</b> to begin streaming MQTT messages in real-time.</div>'}
      </div>
      <div class="muted" style="font-size:11px;margin-top:8px">Subscribes automatically to all active <code>mqtt-*</code> Kafka topics. Original MQTT path decoded from the <code>mqtt-topic</code> header.</div>
    </div>
  </div>`;
}

function formatStreamItem(msg) {
  if (msg._info) {
    return `<div class="muted" style="padding:4px 0;text-align:center">${esc(msg._info)}</div>`;
  }
  if (msg._err) {
    return `<div style="color:var(--danger);padding:3px 0">[error] ${esc(msg._err)}</div>`;
  }
  const ts = msg.ts ? new Date(msg.ts).toISOString().slice(11, 23) : '';
  let payload;
  try {
    if (msg.isJSON) {
      payload = `<span style="color:var(--str)">${esc(JSON.stringify(JSON.parse(msg.value), null, 2))}</span>`;
    } else {
      payload = `<span style="color:var(--key)">${esc(msg.value || '(empty)')}</span>`;
    }
  } catch(e) {
    payload = `<span>${esc(String(msg.value || ''))}</span>`;
  }
  return `<div style="padding:4px 0;border-bottom:1px solid var(--border-soft)">
    <span class="muted" style="margin-right:8px">${esc(ts)}</span>
    <span style="color:var(--primary-text);font-weight:600">${esc(msg.mqttTopic || '')}</span>
    <span class="muted" style="font-size:10px;margin-left:6px">→ ${esc(msg.kafkaTopic || '')}</span>
    <div style="margin-top:3px;padding-left:12px;word-break:break-all;white-space:pre-wrap">${payload}</div>
  </div>`;
}

function viewMQTTClients() {
  const ms = state.mqttStatus || {};
  const enabled = !!ms.enabled;
  const clients = ms.clients || [];

  const rows = clients.length
    ? clients.map(c => {
        const since = c.connectedAt ? rel(new Date(c.connectedAt).getTime()) : '-';
        return `<tr>
          <td class="mono" style="font-weight:600">${esc(c.clientId || '(anonymous)')}</td>
          <td class="mono" style="font-size:11px">${esc(c.remoteAddr || '-')}</td>
          <td class="mono" style="font-size:11px">${esc(since)}</td>
          <td class="mono" style="text-align:right">${c.subsCount ?? 0}</td>
        </tr>`;
      }).join('')
    : `<tr><td colspan="4" class="muted" style="padding:24px;text-align:center">${
        enabled ? 'No active MQTT clients connected right now.' : 'MQTT bridge listener is disabled.'
      }</td></tr>`;

  return `<div>
    ${pageHeader('Active MQTT Clients', 'Inspect active MQTT client sessions, remote network addresses, and active subscriptions.')}
    <div class="card" style="padding:0">
      <div style="padding:12px 16px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center">
        <span style="font-weight:600;font-size:13px">Connected Clients (${clients.length})</span>
        <button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="mqttRefreshStatus()">Refresh</button>
      </div>
      ${tableScroll(`<table><thead><tr>
        <th>Client ID</th><th>Remote Address</th><th>Connected</th><th style="text-align:right">Subscriptions</th>
      </tr></thead><tbody>${rows}</tbody></table>`)}
    </div>
  </div>`;
}

function viewMQTTPublisher() {
  const ms = state.mqttStatus || {};
  const enabled = !!ms.enabled;
  const pub = state._mqttPub || {};

  return `<div>
    ${pageHeader('Web MQTT Publisher', 'Publish test MQTT telemetry or control messages to the broker from your browser.')}
    <div class="card">
      ${!enabled ? `<div class="muted" style="font-size:11px;padding:8px 10px;background:var(--warn-soft);border:1px solid var(--warn-line);border-radius:4px;margin-bottom:12px">MQTT bridge is disabled in configuration. Messages cannot be published.</div>` : ''}
      <div style="display:grid;gap:12px;max-width:680px">
        <div>
          <label for="mqttPubTopic" style="margin-bottom:5px">MQTT Topic</label>
          <input id="mqttPubTopic" placeholder="sensors/temp/room1" value="${esc(pub.topic || '')}"
            oninput="mqttPubState('topic', this.value)" style="width:100%" autocomplete="off">
        </div>
        <div style="display:flex;gap:12px;flex-wrap:wrap">
          <div>
            <label for="mqttPubQoS" style="margin-bottom:5px">QoS Level</label>
            <select id="mqttPubQoS" onchange="mqttPubState('qos', +this.value)" style="width:auto">
              <option value="0" ${(pub.qos ?? 0) === 0 ? 'selected' : ''}>QoS 0 — At most once</option>
              <option value="1" ${pub.qos === 1 ? 'selected' : ''}>QoS 1 — At least once</option>
              <option value="2" ${pub.qos === 2 ? 'selected' : ''}>QoS 2 — Exactly once</option>
            </select>
          </div>
          <div>
            <label for="mqttPubFmt" style="margin-bottom:5px">Payload Format</label>
            <select id="mqttPubFmt" onchange="mqttPubState('fmt', this.value)" style="width:auto">
              <option value="text" ${(pub.fmt || 'text') === 'text' ? 'selected' : ''}>Plaintext / String</option>
              <option value="json" ${pub.fmt === 'json' ? 'selected' : ''}>JSON (Syntax Validated)</option>
            </select>
          </div>
        </div>
        <div>
          <label for="mqttPubPayload" style="margin-bottom:5px">Payload Body</label>
          <textarea id="mqttPubPayload" rows="5" placeholder='{"temperature": 23.4, "unit": "C"}'
            style="width:100%;resize:vertical;font-family:var(--font-mono);font-size:12px"
            oninput="mqttPubState('payload', this.value)">${esc(pub.payload || '')}</textarea>
        </div>
        <div style="display:flex;align-items:center;gap:12px">
          <button class="btn" onclick="mqttDoPublish()" ${!enabled ? 'disabled' : ''}>Publish Message</button>
          <span id="mqttPubStatus" style="font-size:11px;color:var(--muted)"></span>
        </div>
      </div>
    </div>
  </div>`;
}

function viewMQTTMapping() {
  return `<div>
    ${pageHeader('MQTT Topic Mapping', 'Understand how MQTT topics, hierarchy paths, and wildcard subscriptions map to Kafka commit logs.')}
    <div class="card">
      <div class="kicker">Mapping Rules</div>
      <h3 style="margin-bottom:8px">Deterministic Gateway Translation</h3>
      <p class="muted" style="font-size:12px;line-height:1.7;margin-bottom:14px">
        PocketKafka bridges MQTT publish events into partitioned Kafka topics without requiring an external translation daemon.
        The first path segment defines the Kafka topic name (prefixed with <code>mqtt-</code>), while subsequent segments become the record partition key and the original full path is retained in headers.
      </p>

      <div style="background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;padding:14px;font:11.5px/2 var(--font-mono);margin-bottom:16px">
        <div style="color:var(--muted);margin-bottom:6px"># Publishing translation:</div>
        <div><span style="color:var(--str)">sensors/room1/temp</span> ➔ Kafka Topic: <span style="color:var(--primary-text);font-weight:600">mqtt-sensors</span> | Record Key: <span style="color:var(--num)">room1</span></div>
        <div><span style="color:var(--str)">fleet/node-7/status</span> ➔ Kafka Topic: <span style="color:var(--primary-text);font-weight:600">mqtt-fleet</span> | Record Key: <span style="color:var(--num)">node-7</span></div>
        <div><span style="color:var(--str)">home/livingroom/ac/power</span> ➔ Kafka Topic: <span style="color:var(--primary-text);font-weight:600">mqtt-home</span> | Record Key: <span style="color:var(--num)">livingroom</span></div>
        <div style="margin-top:10px;color:var(--muted)"># Header preservation:</div>
        <div>Record Header <span style="color:var(--bool)">mqtt-topic</span> = exact original MQTT path (retained for consumer filtering)</div>
      </div>

      <div class="kicker">Subscription Wildcards</div>
      <h3 style="margin-bottom:8px">Multi-Level (#) &amp; Single-Level (+) Filters</h3>
      <p class="muted" style="font-size:12px;line-height:1.7">
        Subscribers using <code>sensors/#</code> read from <code>mqtt-sensors</code> and receive all child telemetry. Single-level wildcards like <code>sensors/+/temp</code> are matched dynamically against the header <code>mqtt-topic</code> before delivery to the MQTT client.
      </p>
    </div>
  </div>`;
}

// Helpers & Actions
function mqttPubState(key, val) {
  state._mqttPub = state._mqttPub || {};
  state._mqttPub[key] = val;
}

async function mqttRefreshStatus() {
  try {
    state.mqttStatus = await api('/api/v1/mqtt');
    if (state.zone === 'mqtt') {
      window.render?.();
    }
  } catch (e) {
    toast('Refresh failed: ' + e.message);
  }
}

function mqttStopTail() {
  const s = state._mqttStream;
  if (!s || !s.active) return;
  s.active = false;
  Object.values(s._wsMap || {}).forEach(ws => { try { ws.close(); } catch (e) {} });
  s._wsMap = {};
  const btn = document.getElementById('mqttTailBtn');
  if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" stroke="none" style="vertical-align:-1px;margin-right:4px"><polygon points="5 3 19 12 5 21 5 3"/></svg> Live Tail'; btn.classList.remove('active'); }
}

function mqttToggleTail() {
  const s = state._mqttStream = state._mqttStream || { active: false, _wsMap: {}, msgs: [], filter: '' };
  if (s.active) {
    mqttStopTail();
  } else {
    s.active = true;
    const btn = document.getElementById('mqttTailBtn');
    if (btn) { btn.innerHTML = '<span class="live-dot"></span> Pause'; btn.classList.add('active'); }
    mqttStartTail();
  }
}
window.mqttStopTail = mqttStopTail;

function mqttStartTail() {
  const s = state._mqttStream;
  if (!s || !s.active) return;
  Object.values(s._wsMap || {}).forEach(ws => { try { ws.close(); } catch (e) {} });
  s._wsMap = {};
  const topics = (state.topics || []).map(t => t.name).filter(n => n.startsWith('mqtt-'));
  if (!topics.length) {
    mqttStreamAppend({ _info: 'No active mqtt-* Kafka topics detected yet. Publish a message to initialize topics.' });
    return;
  }
  topics.slice(0, 8).forEach(topic => {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${proto}//${location.host}/api/v1/topics/${encodeURIComponent(topic)}/tail`);
    s._wsMap[topic] = ws;
    ws.onmessage = ev => {
      try {
        const data = JSON.parse(ev.data);
        const recs = Array.isArray(data) ? data : (data.error ? [{ _err: data.error }] : []);
        recs.forEach(rec => {
          const mqttTopic = (rec.headers && rec.headers['mqtt-topic']) || topic;
          const f = (state._mqttStream && state._mqttStream.filter) || '';
          if (f && !mqttTopic.toLowerCase().includes(f.toLowerCase())) return;
          mqttStreamAppend({ mqttTopic, kafkaTopic: topic, ts: rec.timestamp, value: rec.value, isJSON: rec.isJSON });
        });
      } catch (e) {}
    };
    ws.onerror = () => {};
    ws.onclose = () => { if (s._wsMap) delete s._wsMap[topic]; };
  });
}

function mqttStreamAppend(msg) {
  const s = state._mqttStream = state._mqttStream || { msgs: [], filter: '' };
  s.msgs = s.msgs || [];
  s.msgs.push(msg);
  if (s.msgs.length > 100) s.msgs = s.msgs.slice(-100);
  const out = document.getElementById('mqttStreamOut');
  if (!out) return;
  const html = formatStreamItem(msg);
  out.insertAdjacentHTML('beforeend', html);
  while (out.children.length > 100) {
    out.removeChild(out.firstElementChild);
  }
  out.scrollTop = out.scrollHeight;
}

function mqttApplyStreamFilter(val) {
  if (!state._mqttStream) return;
  state._mqttStream.filter = val || '';
}

function mqttClearStream() {
  if (state._mqttStream) state._mqttStream.msgs = [];
  const out = document.getElementById('mqttStreamOut');
  if (out) out.innerHTML = '<div class="muted" style="text-align:center;padding:30px 0">Stream cleared.</div>';
}

async function mqttDoPublish() {
  const pub = state._mqttPub || {};
  const topic = (document.getElementById('mqttPubTopic')?.value || pub.topic || '').trim();
  const payload = document.getElementById('mqttPubPayload')?.value || pub.payload || '';
  const qos = +(document.getElementById('mqttPubQoS')?.value ?? pub.qos ?? 0);
  const fmt = document.getElementById('mqttPubFmt')?.value || pub.fmt || 'text';
  const statusEl = document.getElementById('mqttPubStatus');

  if (!topic) { toast('MQTT topic is required'); return; }
  if (fmt === 'json') {
    try { JSON.parse(payload); }
    catch (e) { toast('Invalid JSON: ' + e.message); return; }
  }

  if (statusEl) { statusEl.textContent = 'Publishing...'; statusEl.style.color = ''; }
  try {
    const res = await api('/api/v1/mqtt/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic, payload, qos })
    });
    if (statusEl) {
      statusEl.textContent = '✓ Published ' + new Date().toLocaleTimeString();
      statusEl.style.color = 'var(--success)';
      setTimeout(() => { if (statusEl) statusEl.style.color = ''; }, 3000);
    }
    toast('Published to ' + topic);
  } catch (e) {
    if (statusEl) { statusEl.textContent = '✗ ' + e.message; statusEl.style.color = 'var(--danger)'; }
    toast('Publish failed: ' + e.message);
  }
}

// Window bindings for inline HTML events
window.viewMQTTOverview = viewMQTTOverview;
window.viewMQTTTracker = viewMQTTTracker;
window.viewMQTTClients = viewMQTTClients;
window.viewMQTTPublisher = viewMQTTPublisher;
window.viewMQTTMapping = viewMQTTMapping;
window.mqttPubState = mqttPubState;
window.mqttRefreshStatus = mqttRefreshStatus;
window.mqttToggleTail = mqttToggleTail;
window.mqttClearStream = mqttClearStream;
window.mqttApplyStreamFilter = mqttApplyStreamFilter;
window.mqttDoPublish = mqttDoPublish;
window.mqttStreamAppend = mqttStreamAppend;

export {
  viewMQTTOverview,
  viewMQTTTracker,
  viewMQTTClients,
  viewMQTTPublisher,
  viewMQTTMapping,
  mqttRefreshStatus,
  mqttToggleTail,
  mqttStopTail,
  mqttClearStream,
  mqttApplyStreamFilter,
  mqttDoPublish
};
