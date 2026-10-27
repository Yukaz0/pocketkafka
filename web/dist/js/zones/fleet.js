// PocketKafka Fleet Zone Views
import { state, $, I, esc, fmtBytes, fmtBytesOpt, toast, skel, rel, copyText2, jsArg } from '../state.js';
import { api, apiLocal } from '../api.js';

const render = (...args) => window.render?.(...args);
const refresh = (...args) => window.refresh?.(...args);
const goView = (...args) => window.goView?.(...args);
const openDrw = (...args) => window.openDrw?.(...args);
const closeDrw = (...args) => window.closeDrw?.(...args);
const pageHeader = (...args) => window.pageHeader?.(...args);
const tableScroll = (...args) => window.tableScroll?.(...args);


async function refreshFleetView(){
  if(!state.fleetEnabled)return;
  const tasks=[
    apiLocal('/api/v1/fleet/status').then(d=>{state.fleetStatus=d;}).catch(()=>{}),
    apiLocal('/api/v1/fleet/servers').then(d=>{state.fleetServers=(d&&d.servers)||[];}).catch(()=>{})
  ];
  if(state.view==='fleet-containers'){
    tasks.push(apiLocal('/api/v1/fleet/containers').then(d=>{state.fleetContainers=(d&&d.containers)||[];}).catch(()=>{}));
  }
  if(state.view==='fleet-jobs'){
    tasks.push(apiLocal('/api/v1/fleet/jobs').then(d=>{state.fleetJobs=(d&&d.jobs)||[];}).catch(()=>{}));
  }
  if(state.view==='fleet-audit'){
    tasks.push(apiLocal('/api/v1/fleet/audit-events').then(d=>{state.fleetAudit=(d&&d.events)||[];}).catch(()=>{}));
  }
  if(state.view==='fleet-settings'){
    tasks.push(apiLocal('/api/v1/fleet/enrollments').then(d=>{state.fleetEnrollments=(d&&d.enrollments)||[];}).catch(()=>{}));
  }
  await Promise.allSettled(tasks);
}
function patchFleetView(){
  const v=document.getElementById('view'); if(!v)return false;
  const ae=document.activeElement;
  if(ae&&v.contains(ae)&&(ae.tagName==='INPUT'||ae.tagName==='TEXTAREA'||ae.tagName==='SELECT'))return true;
  const html=state.view==='fleet-servers'?viewFleetServers()
    :state.view==='fleet-containers'?viewFleetContainers()
    :state.view==='fleet-jobs'?viewFleetJobs()
    :state.view==='fleet-audit'?viewFleetAudit()
    :state.view==='fleet-settings'?viewFleetSettings()
    :null;
  if(html==null)return false;
  const keepScroll=v.scrollTop;
  v.innerHTML=html; bindView();
  v.scrollTop=keepScroll; return true;
}
let fleetFilterTimer=null;
function fleetFilter(key,val){ state[key]=val; clearTimeout(fleetFilterTimer); fleetFilterTimer=setTimeout(()=>{ patchFleetView(); },200); }
async function waitFleetJobDone(jobId, timeoutMs){
  const t0 = Date.now();
  timeoutMs = timeoutMs || 12000;
  while(Date.now() - t0 < timeoutMs){
    try{
      const j = await apiLocal('/api/v1/fleet/jobs/' + encodeURIComponent(jobId));
      const st = j && (j.status || (j.job && j.job.status));
      if(st === 'succeeded' || st === 'failed') return j;
    }catch(e){}
    await new Promise(r => setTimeout(r, 800));
  }
  return null;
}
async function waitFleetContainerGone(cid, serverId, timeoutMs){
  const t0 = Date.now();
  timeoutMs = timeoutMs || 15000;
  const short = (cid||'').slice(0,12);
  while(Date.now() - t0 < timeoutMs){
    try{
      const d = await apiLocal('/api/v1/fleet/containers' + (serverId ? '?server_id=' + encodeURIComponent(serverId) : ''));
      const list = (d && d.containers) || [];
      if(!list.some(c => c.id === cid || (c.id||'').startsWith(short))) return true;
    }catch(e){}
    await new Promise(r => setTimeout(r, 1000));
  }
  return false;
}
function fleetStatusBadge(status, label){
  const s = (status || '').toLowerCase();
  const text = label || (s.charAt(0).toUpperCase() + s.slice(1));
  if(s === 'online' || s === 'running' || s === 'succeeded' || s === 'success'){
    return `<span class="badge b-green" style="display:inline-flex;align-items:center;gap:4px"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" aria-hidden="true"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg><span>${esc(text)}</span></span>`;
  }
  if(s === 'stale' || s === 'paused' || s === 'queued' || s === 'accepted' || s === 'removing'){
    return `<span class="badge b-amber" style="display:inline-flex;align-items:center;gap:4px"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" aria-hidden="true"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg><span>${esc(text)}</span></span>`;
  }
  if(s === 'revoked'){
    return `<span class="badge muted" style="display:inline-flex;align-items:center;gap:4px;border-color:var(--danger-line);color:var(--danger-text)"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" aria-hidden="true"><circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/></svg><span>${esc(text)}</span></span>`;
  }
  if(s === 'offline' || s === 'failed' || s === 'critical' || s === 'error' || s === 'dead'){
    return `<span class="badge b-red" style="display:inline-flex;align-items:center;gap:4px"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" aria-hidden="true"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg><span>${esc(text)}</span></span>`;
  }
  if(s === 'exited' || s === 'stopped'){
    return `<span class="badge muted" style="display:inline-flex;align-items:center;gap:4px"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" aria-hidden="true"><rect x="5" y="5" width="14" height="14" rx="2"/></svg><span>${esc(text)}</span></span>`;
  }
  if(s === 'restarting'){
    return `<span class="badge b-purple" style="display:inline-flex;align-items:center;gap:4px"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" aria-hidden="true"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg><span>${esc(text)}</span></span>`;
  }
  return `<span class="badge muted" style="display:inline-flex;align-items:center;gap:4px"><span>${esc(text)}</span></span>`;
}

function serverStatusInfo(s){
  if((s.status||'').toLowerCase()==='revoked') return { status: 'revoked', label: 'Revoked' };
  const updated = s.updated_at ? new Date(s.updated_at).getTime() : 0;
  if(!updated) return { status: 'offline', label: 'Offline' };
  const diff = Date.now() - updated;
  if(diff < 90000) return { status: 'online', label: 'Online' };
  if(diff < 300000) return { status: 'stale', label: 'Stale' };
  return { status: 'offline', label: 'Offline' };
}
function renderPortBadges(portsStr){
  if(!portsStr || portsStr === '-') return '<span class="muted" style="font-size:11px">-</span>';
  const items = portsStr.split(',').map(s => s.trim()).filter(Boolean);
  const seen = new Set();
  const badges = [];

  for(const it of items){
    const m = it.match(/^(?:(\S+):)?(\d+)->(\d+)(?:\/(\w+))?$/);
    if(m){
      const ip = m[1] || '';
      const pub = m[2];
      const priv = m[3];
      const proto = (m[4] || 'tcp').toLowerCase();
      const key = pub + ':' + priv + '/' + proto;
      if(seen.has(key)) continue;
      seen.add(key);

      const isLoopback = ip === '127.0.0.1' || ip === 'localhost';
      const portText = pub === priv ? pub : (pub + '➔' + priv);
      const protoText = proto === 'udp' ? '/udp' : '';
      const prefix = isLoopback ? '<span class="muted" style="font-size:9.5px;margin-right:2px">local:</span>' : '';
      badges.push(`<span class="chip mono" style="font-size:10.5px;padding:1px 6px;border-radius:3px;white-space:nowrap;display:inline-block" title="${esc(it)}">${prefix}<b>${esc(portText)}</b>${protoText}</span>`);
    } else {
      const m2 = it.match(/^(\d+)(?:\/(\w+))?$/);
      if(m2){
        const priv = m2[1];
        const proto = (m2[2] || 'tcp').toLowerCase();
        const key = priv + '/' + proto;
        if(seen.has(key)) continue;
        seen.add(key);
        badges.push(`<span class="chip mono muted" style="font-size:10.5px;padding:1px 6px;border-radius:3px;white-space:nowrap;display:inline-block;opacity:.7" title="Exposed in container: ${esc(it)}">${esc(priv)}/${esc(proto)}</span>`);
      }
    }
  }

  if(!badges.length) return '<span class="muted" style="font-size:11px">-</span>';
  return `<div style="display:flex;flex-wrap:wrap;gap:4px;align-items:center;max-width:320px">${badges.join('')}</div>`;
}

function viewFleetDisabled(){
  return `<div class="card" style="max-width:640px;margin:40px auto;padding:28px">
    <div style="display:flex;align-items:center;gap:14px;margin-bottom:14px">
      <div style="width:40px;height:40px;border-radius:4px;background:var(--warn-soft);color:var(--warn);display:flex;align-items:center;justify-content:center">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/></svg>
      </div>
      <div>
        <h3 style="font-size:17px;margin-bottom:4px">Fleet Management Disabled</h3>
        <span class="badge b-amber">Subsystem Inactive</span>
      </div>
    </div>
    <p class="muted" style="margin-bottom:12px;line-height:1.6">
      The Fleet subsystem is currently disabled on this PocketKafka broker (<code>fleet.enabled: false</code>). Host server enrollment, container orchestration, and remote agent management are turned off.
    </p>
    <p class="muted" style="margin-bottom:20px;font-size:12px">
      To enable Fleet management, configure <code>fleet.enabled: true</code> in your broker configuration and restart the broker.
    </p>
    <div style="display:flex;gap:10px">
      <button class="btn" onclick="switchZone('kafka')">Back to Kafka Overview</button>
      <button class="btn ghost" onclick="goView('settings')">Broker Settings</button>
    </div>
  </div>`;
}

function viewFleetServers(){
  const srvs = state.fleetServers || [];
  const onlineCount = srvs.filter(s => serverStatusInfo(s).status === 'online').length;
  const staleCount = srvs.filter(s => serverStatusInfo(s).status === 'stale').length;
  const offlineCount = srvs.filter(s => serverStatusInfo(s).status === 'offline').length;
  const revokedCount = srvs.filter(s => (s.status||'').toLowerCase() === 'revoked').length;
  const q = (state.fleetSearch || '').toLowerCase();
  const filtered = srvs.filter(s => !q || (s.id||'').toLowerCase().includes(q) || (s.name||'').toLowerCase().includes(q) || (s.host||'').toLowerCase().includes(q) || (s.agent_id||'').toLowerCase().includes(q));

  const stats = `<div class="overviewFacts" style="margin-bottom:16px">
    <div class="overviewFact"><dt>Enrolled Hosts</dt><dd>${srvs.length}</dd></div>
    <div class="overviewFact"><dt>Online</dt><dd style="color:var(--success)">${onlineCount}</dd></div>
    <div class="overviewFact"><dt>Stale / Offline</dt><dd style="color:var(--warn)">${staleCount + offlineCount}</dd></div>
    <div class="overviewFact"><dt>Revoked</dt><dd style="color:var(--muted)">${revokedCount}</dd></div>
  </div>`;

  const toolbar = `<div class="card" style="margin-bottom:16px;padding:12px">
    <div class="row" style="gap:10px;justify-content:space-between;flex-wrap:wrap">
      <div style="display:flex;gap:8px;align-items:center;flex:1;max-width:380px">
        <input placeholder="Filter hosts by name, ID, or IP..." value="${esc(state.fleetSearch||'')}" oninput="fleetFilter('fleetSearch',this.value)" style="width:100%" aria-label="Filter servers">
      </div>
      <div style="display:flex;gap:8px">
        <button class="btn" onclick="openEnrollModal()">+ Enroll Server</button>
        <button class="btn ghost" onclick="refreshAll()">Refresh</button>
      </div>
    </div>
  </div>`;

  const rows = filtered.map(s => {
    const st = serverStatusInfo(s);
    const badge = fleetStatusBadge(st.status, st.label);
    const updated = s.updated_at ? new Date(s.updated_at).getTime() : 0;
    const hbText = updated ? rel(updated) : 'never';
    const hbFull = updated ? new Date(updated).toISOString() : '-';
    const enrolled = s.created_at ? new Date(s.created_at).toLocaleDateString() : '-';
    const isRevoked = st.status === 'revoked';
    const serverActions = isRevoked
      ? `<button class="btn danger" style="padding:3px 8px;font-size:11px" data-srv="${esc(s.id)}" onclick="deleteServer(this.dataset.srv)" title="Hapus server permanen">Delete</button>`
      : `<button class="btn ghost" style="padding:3px 8px;font-size:11px;color:var(--danger)" data-srv="${esc(s.id)}" onclick="revokeServer(this.dataset.srv)">Revoke</button><button class="btn danger" style="padding:3px 8px;font-size:11px" data-srv="${esc(s.id)}" onclick="deleteServer(this.dataset.srv)" title="Hapus server permanen">Delete</button>`;
    const hostCnts = (state.fleetContainers || []).filter(c => c.server_id === s.id);
    const runCnt = hostCnts.filter(c => (c.state || '').toLowerCase() === 'running').length;
    const cntBadge = hostCnts.length
      ? `<button class="slink mono" style="font-size:11px;background:none;border:none;padding:0;cursor:pointer;color:var(--primary-text)" data-srv="${esc(s.id)}" onclick="state.fleetServerFilter=this.dataset.srv;goView('fleet-containers')" title="View containers on this host"><b>${runCnt}</b> run / ${hostCnts.length}</button>`
      : `<span class="muted" style="font-size:11px">0</span>`;
    const containerLink = `<button class="btn ghost" style="padding:3px 8px;font-size:11px" data-srv="${esc(s.id)}" onclick="goFleetContainers(this.dataset.srv)">Containers</button>`;

    return `<tr>
      <td class="mono" style="font-weight:600">${esc(s.name || s.id)}</td>
      <td>${badge}</td>
      <td class="mono">${esc(s.host || 'local-socket')}</td>
      <td>${cntBadge}</td>
      <td class="mono" style="font-size:11px;color:var(--muted)">${esc(s.agent_id ? s.agent_id.slice(0,12)+'...' : '-')}</td>
      <td title="${esc(hbFull)}">${esc(hbText)}</td>
      <td class="muted" style="font-size:11px">${esc(enrolled)}</td>
      <td><div class="clRowActions">${containerLink}${serverActions}</div></td>
    </tr>`;
  }).join('');

  const table = tableScroll(`<table><thead><tr>
    <th>Server Host / ID</th><th>Status</th><th>Endpoint</th><th>Containers</th><th>Agent ID</th><th>Last Heartbeat</th><th>Enrolled</th><th>Actions</th>
  </tr></thead><tbody>
    ${rows || '<tr><td colspan="8" class="muted" style="padding:28px;text-align:center">No servers match filter. Enroll a server to manage host containers.</td></tr>'}
  </tbody></table>`);

  return `<div>${pageHeader('Fleet Servers', 'Managed host nodes with fleet-agent and Docker Engine daemon.')}
    ${stats}
    ${toolbar}
    <div class="card" style="padding:0">${table}</div>
  </div>`;
}

function viewFleetContainers(){
  const srvs = state.fleetServers || [];
  const cnts = state.fleetContainers || [];
  const sFilter = state.fleetServerFilter || '';
  const stFilter = state.fleetStateFilter || 'all';
  const q = (state.fleetSearch || '').toLowerCase();

  const filtered = cnts.filter(c => {
    if(sFilter && c.server_id && c.server_id !== sFilter) return false;
    const st = (c.state || '').toLowerCase();
    if(stFilter === 'running' && st !== 'running') return false;
    if(stFilter === 'exited' && st !== 'exited' && st !== 'stopped') return false;
    if(stFilter === 'paused' && st !== 'paused') return false;
    if(q){
      const name = (c.names && c.names[0]) ? c.names[0].toLowerCase() : (c.id||'').toLowerCase();
      const img = (c.image || '').toLowerCase();
      if(!name.includes(q) && !img.includes(q) && !(c.id||'').toLowerCase().includes(q)) return false;
    }
    return true;
  });

  const runningCount = cnts.filter(c => (c.state||'').toLowerCase() === 'running').length;
  const exitedCount = cnts.filter(c => (c.state||'').toLowerCase() === 'exited' || (c.state||'').toLowerCase() === 'stopped').length;
  const pausedCount = cnts.filter(c => (c.state||'').toLowerCase() === 'paused').length;

  const stats = `<div class="overviewFacts" style="margin-bottom:16px">
    <div class="overviewFact"><dt>Total Containers</dt><dd>${cnts.length}</dd></div>
    <div class="overviewFact"><dt>Running</dt><dd style="color:var(--success)">${runningCount}</dd></div>
    <div class="overviewFact"><dt>Exited / Stopped</dt><dd style="color:var(--muted)">${exitedCount}</dd></div>
    <div class="overviewFact"><dt>Paused</dt><dd style="color:var(--warn)">${pausedCount}</dd></div>
  </div>`;

  const srvOpts = `<option value="">All Servers (${srvs.length})</option>` + srvs.map(s => `<option value="${esc(s.id)}" ${sFilter===s.id?'selected':''}>${esc(s.name||s.id)}</option>`).join('');

  const toolbar = `<div class="card" style="margin-bottom:16px;padding:12px">
    <div class="row" style="gap:10px;justify-content:space-between;flex-wrap:wrap">
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <label for="cntServerSel" class="muted" style="font-size:11px">Server</label>
        <select id="cntServerSel" style="width:auto" onchange="state.fleetServerFilter=this.value;render()">${srvOpts}</select>
        <label for="cntStateSel" class="muted" style="font-size:11px">State</label>
        <select id="cntStateSel" style="width:auto" onchange="state.fleetStateFilter=this.value;render()">
          <option value="all" ${stFilter==='all'?'selected':''}>All States</option>
          <option value="running" ${stFilter==='running'?'selected':''}>Running</option>
          <option value="exited" ${stFilter==='exited'?'selected':''}>Exited / Stopped</option>
          <option value="paused" ${stFilter==='paused'?'selected':''}>Paused</option>
        </select>
        <input placeholder="Search container or image..." value="${esc(state.fleetSearch||'')}" oninput="fleetFilter('fleetSearch',this.value)" style="max-width:240px" aria-label="Search containers">
      </div>
      <div style="display:flex;gap:8px;align-items:center">
        <button class="btn ghost" onclick="viewAllContainersLogs()">All Logs</button>
        <button class="btn ghost" onclick="refreshAll()">Refresh</button>
      </div>
    </div>
  </div>`;

  const rows = filtered.map(c => {
    const rawName = (c.names && c.names[0]) ? c.names[0].replace(/^\//, '') : c.id;
    const isRunning = (c.state || '').toLowerCase() === 'running';
    const badge = fleetStatusBadge(c.state, c.status || c.state);
    const shortId = (c.id || '').slice(0, 12);
    const srvId = c.server_id || (srvs.length === 1 ? srvs[0].id : '');
    const srvLabel = srvId || 'unknown';
    const noSrv = srvId === '';

    const actions = isRunning
      ? `<button class="btn ghost" style="padding:3px 8px;font-size:11px" ${noSrv?'disabled title="server unknown"':''} data-id="${esc(c.id)}" data-name="${esc(rawName)}" data-srv="${esc(srvId)}" onclick="restartContainer(this.dataset.id,this.dataset.name,this.dataset.srv)">Restart</button>
         <button class="btn ghost" style="padding:3px 8px;font-size:11px" ${noSrv?'disabled title="server unknown"':''} data-id="${esc(c.id)}" data-name="${esc(rawName)}" data-srv="${esc(srvId)}" onclick="stopContainer(this.dataset.id,this.dataset.name,this.dataset.srv)">Stop</button>
         <button class="btn ghost" style="padding:3px 8px;font-size:11px" data-id="${esc(c.id)}" data-name="${esc(rawName)}" data-srv="${esc(srvId)}" onclick="viewContainerLogs(this.dataset.id,this.dataset.name,this.dataset.srv)">Logs</button>
         <button class="btn danger" style="padding:3px 8px;font-size:11px" ${noSrv?'disabled title="server unknown"':''} data-id="${esc(c.id)}" data-name="${esc(rawName)}" data-srv="${esc(srvId)}" data-force="true" onclick="removeContainer(this.dataset.id,this.dataset.name,this.dataset.srv,true)">Remove</button>`
      : `<button class="btn" style="padding:3px 8px;font-size:11px" ${noSrv?'disabled title="server unknown"':''} data-id="${esc(c.id)}" data-srv="${esc(srvId)}" onclick="startContainer(this.dataset.id,this.dataset.srv)">Start</button>
         <button class="btn ghost" style="padding:3px 8px;font-size:11px" data-id="${esc(c.id)}" data-name="${esc(rawName)}" data-srv="${esc(srvId)}" onclick="viewContainerLogs(this.dataset.id,this.dataset.name,this.dataset.srv)">Logs</button>
         <button class="btn danger" style="padding:3px 8px;font-size:11px" ${noSrv?'disabled title="server unknown"':''} data-id="${esc(c.id)}" data-name="${esc(rawName)}" data-srv="${esc(srvId)}" data-force="false" onclick="removeContainer(this.dataset.id,this.dataset.name,this.dataset.srv,false)">Remove</button>`;

    return `<tr>
      <td class="mono">
        <a href="javascript:void(0)" data-id="${esc(c.id)}" onclick="viewContainerInspect(this.dataset.id)" style="font-weight:600;color:var(--text);text-decoration:none;border-bottom:1px dashed var(--border)" title="Inspect container details">${esc(rawName)}</a>
        <div class="muted" style="font-size:10px">${esc(shortId)}</div>
      </td>
      <td class="mono" style="font-size:11px">
        ${srvId ? `<button class="slink mono" style="font-size:11px;background:none;border:none;padding:0;cursor:pointer;color:var(--primary-text)" data-srv="${esc(srvId)}" onclick="state.fleetServerFilter=this.dataset.srv;render()" title="Filter containers on ${esc(srvLabel)}">${esc(srvLabel)}</button>` : `<span class="muted">${esc(srvLabel)}</span>`}
      </td>
      <td class="mono" style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(c.image||'')}">${esc(c.image || '-')}</td>
      <td>${badge}</td>
      <td class="mono" style="font-size:10px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(c.command||'')}">${esc(c.command || '-')}</td>
      <td class="mono" style="min-width:140px;max-width:320px">${renderPortBadges(c.ports)}</td>
      <td><div class="clRowActions">${actions}</div></td>
    </tr>`;
  }).join('');

  const emptyMsg = cnts.length
    ? 'No containers match filter criteria.'
    : 'No containers found. Ensure a fleet-agent is connected.';

  const table = tableScroll(`<table><thead><tr>
    <th>Container Name / ID</th><th>Host Server</th><th>Image</th><th>State / Status</th><th>Command</th><th>Ports</th><th>Actions</th>
  </tr></thead><tbody>
    ${rows || '<tr><td colspan="7" class="muted" style="padding:28px;text-align:center">'+emptyMsg+'</td></tr>'}
  </tbody></table>`);

  return `<div>${pageHeader('Containers', 'Docker containers across connected fleet host nodes.')}
    ${stats}
    ${toolbar}
    <div class="card" style="padding:0">${table}</div>
  </div>`;
}

function viewFleetJobs(){
  const jobs = state.fleetJobs || [];
  const statusFilter = state.fleetJobStatusFilter || 'all';
  const q = (state.fleetJobSearch || '').toLowerCase();

  const filtered = jobs.filter(j => {
    const st = (j.status || '').toLowerCase();
    if(statusFilter === 'active' && !(st==='queued'||st==='accepted'||st==='running')) return false;
    if(statusFilter === 'succeeded' && st !== 'succeeded') return false;
    if(statusFilter === 'failed' && st !== 'failed') return false;
    if(q){
      if(!(j.id||'').toLowerCase().includes(q) && !(j.action||'').toLowerCase().includes(q) && !(j.target||'').toLowerCase().includes(q) && !(j.server_id||'').toLowerCase().includes(q) && !(j.actor||'').toLowerCase().includes(q)) return false;
    }
    return true;
  });

  const activeCount = jobs.filter(j => {
    const st = (j.status||'').toLowerCase();
    return st === 'queued' || st === 'accepted' || st === 'running';
  }).length;
  const succCount = jobs.filter(j => (j.status||'').toLowerCase() === 'succeeded').length;
  const failCount = jobs.filter(j => (j.status||'').toLowerCase() === 'failed').length;

  const stats = `<div class="overviewFacts" style="margin-bottom:16px">
    <div class="overviewFact"><dt>Total Dispatched Jobs</dt><dd>${jobs.length}</dd></div>
    <div class="overviewFact"><dt>Active / Running</dt><dd style="color:var(--primary-text)">${activeCount}</dd></div>
    <div class="overviewFact"><dt>Succeeded</dt><dd style="color:var(--success)">${succCount}</dd></div>
    <div class="overviewFact"><dt>Failed</dt><dd style="color:var(--danger-text)">${failCount}</dd></div>
  </div>`;

  const toolbar = `<div class="card" style="margin-bottom:16px;padding:12px">
    <div class="row" style="gap:10px;justify-content:space-between;flex-wrap:wrap">
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <label for="jobStatusSel" class="muted" style="font-size:11px">Status</label>
        <select id="jobStatusSel" style="width:auto" onchange="state.fleetJobStatusFilter=this.value;render()">
          <option value="all" ${statusFilter==='all'?'selected':''}>All Jobs</option>
          <option value="active" ${statusFilter==='active'?'selected':''}>Active (Queued/Running)</option>
          <option value="succeeded" ${statusFilter==='succeeded'?'selected':''}>Succeeded</option>
          <option value="failed" ${statusFilter==='failed'?'selected':''}>Failed</option>
        </select>
        <input placeholder="Filter by job ID, target, host..." value="${esc(state.fleetJobSearch||'')}" oninput="fleetFilter('fleetJobSearch',this.value)" style="max-width:240px" aria-label="Search jobs">
      </div>
      <button class="btn ghost" onclick="refreshAll()">Refresh</button>
    </div>
  </div>`;

  const rows = filtered.map(j => {
    const badge = fleetStatusBadge(j.status);
    const shortId = (j.id || '').slice(0, 8);
    const created = j.created_at ? rel(new Date(j.created_at).getTime()) : '-';
    let duration = '-';
    if(j.completed_at && (j.started_at || j.created_at)){
      const start = new Date(j.started_at || j.created_at).getTime();
      const end = new Date(j.completed_at).getTime();
      duration = ((end - start) / 1000).toFixed(2) + 's';
    } else if(j.status === 'running' && j.started_at){
      duration = 'running (' + rel(new Date(j.started_at).getTime()) + ')';
    }

    const err = j.error ? `<div class="mono" style="color:var(--danger-text);font-size:11px;margin-top:2px">${esc(j.error)}</div>` : '';

    return `<tr>
      <td class="mono" title="${esc(j.id)}"><strong>${esc(shortId)}</strong></td>
      <td><span class="chip mono">${esc(j.action || '-')}</span></td>
      <td class="mono" style="font-size:11px">${j.target ? `<button class="slink mono" style="font-size:11px;background:none;border:none;padding:0;cursor:pointer;color:var(--primary-text)" data-target="${esc(j.target)}" data-srv="${esc(j.server_id||'')}" onclick="viewContainerLogs(this.dataset.target,this.dataset.target,this.dataset.srv)" title="View logs for ${esc(j.target)}">${esc(j.target)}</button>` : '-'}</td>
      <td class="mono" style="font-size:11px;color:var(--muted)">${esc(j.server_id || '-')}</td>
      <td>${badge}${err}</td>
      <td class="mono" style="font-size:11px">${esc(j.actor || '-')}</td>
      <td class="muted" style="font-size:11px">${esc(created)}</td>
      <td class="mono" style="font-size:11px">${esc(duration)}</td>
    </tr>`;
  }).join('');

  const table = tableScroll(`<table><thead><tr>
    <th>Job ID</th><th>Action</th><th>Target</th><th>Host Server</th><th>Status</th><th>Actor</th><th>Created</th><th>Duration</th>
  </tr></thead><tbody>
    ${rows || '<tr><td colspan="8" class="muted" style="padding:28px;text-align:center">No background jobs found.</td></tr>'}
  </tbody></table>`);

  return `<div>${pageHeader('Async Jobs', 'Container lifecycle tasks dispatched to remote fleet agents.')}
    ${stats}
    ${toolbar}
    <div class="card" style="padding:0">${table}</div>
  </div>`;
}

function viewFleetAudit(){
  const events = state.fleetAudit || [];
  const q = (state.fleetAuditSearch || '').toLowerCase();
  const filtered = events.filter(e => {
    if(!q) return true;
    return (e.actor||'').toLowerCase().includes(q) ||
           (e.action||'').toLowerCase().includes(q) ||
           (e.target||'').toLowerCase().includes(q) ||
           (e.result||'').toLowerCase().includes(q) ||
           (e.detail||'').toLowerCase().includes(q);
  });

  const stats = `<div class="overviewFacts" style="margin-bottom:16px">
    <div class="overviewFact"><dt>Total Audit Events</dt><dd>${events.length}</dd></div>
    <div class="overviewFact"><dt>Success / Accepted</dt><dd style="color:var(--success)">${events.filter(e=>e.result==='success'||e.result==='accepted').length}</dd></div>
    <div class="overviewFact"><dt>Failed / Rejected</dt><dd style="color:var(--danger-text)">${events.filter(e=>e.result==='failed'||e.result==='rejected').length}</dd></div>
    <div class="overviewFact"><dt>Retention Window</dt><dd style="font-size:14px;color:var(--muted)">30 days / 10k</dd></div>
  </div>`;

  const toolbar = `<div class="card" style="margin-bottom:16px;padding:12px">
    <div class="row" style="gap:10px;justify-content:space-between;flex-wrap:wrap">
      <input placeholder="Filter audit events by actor, action, target..." value="${esc(state.fleetAuditSearch||'')}" oninput="fleetFilter('fleetAuditSearch',this.value)" style="max-width:320px" aria-label="Search audit events">
      <button class="btn ghost" onclick="refreshAll()">Refresh</button>
    </div>
  </div>`;

  const rows = filtered.map(e => {
    const ts = e.time ? new Date(e.time).toISOString().replace('T',' ').slice(0,19) : '-';
    const badge = fleetStatusBadge(e.result);
    return `<tr>
      <td class="mono" style="font-size:11px;color:var(--muted)">${esc(ts)}</td>
      <td class="mono" style="font-weight:600">${esc(e.actor || '-')}</td>
      <td><span class="chip mono">${esc(e.action || '-')}</span></td>
      <td class="mono" style="font-size:11px">${esc(e.target || '-')}</td>
      <td>${badge}</td>
      <td class="mono" style="font-size:11px">${e.correlation_id ? `<button class="slink mono" style="font-size:11px;background:none;border:none;padding:0;cursor:pointer;color:var(--primary-text)" data-cid="${esc(e.correlation_id)}" onclick="state.fleetAuditSearch=this.dataset.cid;render()" title="Filter audit events by correlation ID ${esc(e.correlation_id)}">${esc(e.correlation_id.slice(0,8))}...</button>` : '-'}</td>
      <td style="font-size:11px;max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(e.detail||'')}">${esc(e.detail || '-')}</td>
    </tr>`;
  }).join('');

  const table = tableScroll(`<table><thead><tr>
    <th>Timestamp</th><th>Actor</th><th>Action</th><th>Target</th><th>Result</th><th>Correlation ID</th><th>Details</th>
  </tr></thead><tbody>
    ${rows || '<tr><td colspan="7" class="muted" style="padding:28px;text-align:center">No audit events recorded yet.</td></tr>'}
  </tbody></table>`);

  return `<div>${pageHeader('Fleet Audit Log', 'Append-only audit trail with correlation IDs and metadata sanitization.')}
    ${stats}
    ${toolbar}
    <div class="card" style="padding:0">${table}</div>
  </div>`;
}

function viewFleetSettings(){
  const st = state.fleetStatus || {};
  const enrs = state.fleetEnrollments || [];

  const statusCard = `<div class="card" style="margin-bottom:16px">
    <div class="kicker">Subsystem</div>
    <h3 style="margin-bottom:14px">Fleet Controller Status</h3>
    <div class="row" style="justify-content:space-between;margin-bottom:10px">
      <span>Operational State</span>
      <span class="badge b-green">ACTIVE (in-process mux)</span>
    </div>
    <div class="row" style="justify-content:space-between;margin-bottom:10px">
      <span>Enrolled Host Servers</span>
      <span class="mono">${st.servers ?? 0}</span>
    </div>
    <div class="row" style="justify-content:space-between;margin-bottom:10px">
      <span>Connected Agents</span>
      <span class="mono">${st.agents ?? 0}</span>
    </div>
    <div class="row" style="justify-content:space-between;margin-bottom:10px">
      <span>Active Jobs</span>
      <span class="mono">${st.active_jobs ?? 0}</span>
    </div>
    <div class="row" style="justify-content:space-between">
      <span>Audit Retention</span>
      <span class="mono">30 days / 10,000 entries</span>
    </div>
  </div>`;

  const enrRows = enrs.map(e => {
    const rawPrefix = (e.token_prefix || e.token || '').slice(0, 10);
    const tok = esc(rawPrefix) + '...';
    const isUsed = !!e.used;
    const isExpired = e.expires_at && (new Date(e.expires_at).getTime() < Date.now());
    let badge = '<span class="badge b-green">ACTIVE</span>';
    if(isUsed) badge = '<span class="badge muted">USED</span>';
    else if(isExpired) badge = '<span class="badge b-amber">EXPIRED</span>';

    const exp = e.expires_at ? rel(new Date(e.expires_at).getTime()) : 'never';
    const srv = esc(e.server_id || '');

    const actions = `
      <button class="btn ghost" style="padding:2px 8px;font-size:11px" onclick="reissueEnrollment('${esc(rawPrefix)}','${srv}')" title="Revoke & generate replacement token">Re-issue</button>
      <button class="btn danger" style="padding:2px 8px;font-size:11px" onclick="revokeEnrollment('${esc(rawPrefix)}','${srv}')" title="Revoke token">Revoke</button>
    `;

    return `<tr>
      <td class="mono">${tok}</td>
      <td class="mono">${esc(e.server_id || '-')}</td>
      <td>${badge}</td>
      <td class="muted" style="font-size:11px">${esc(exp)}</td>
      <td><div class="clRowActions">${actions}</div></td>
    </tr>`;
  }).join('');

  const enrCard = `<div class="card" style="margin-bottom:16px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:8px">
      <div>
        <div class="kicker">Security & Onboarding</div>
        <h3>Enrollment Tokens</h3>
      </div>
      <button class="btn" onclick="openEnrollModal()">+ Issue Token</button>
    </div>
    <div class="tableScroll"><table><thead><tr>
      <th>Token Prefix</th><th>Server ID</th><th>Status</th><th>Expires</th><th>Actions</th>
    </tr></thead><tbody>
      ${enrRows || '<tr><td colspan="5" class="muted" style="padding:16px;text-align:center">No active enrollment tokens.</td></tr>'}
    </tbody></table></div>
  </div>`;

  const quickstart = `<div class="card">
    <div class="kicker">Documentation</div>
    <h3 style="margin-bottom:10px">Agent Quickstart</h3>
    <p class="muted" style="font-size:12px;margin-bottom:12px;line-height:1.6">
      PocketKafka Fleet uses a lightweight daemon (<code>fleet-agent</code>) installed on target host nodes (Linux or WSL) to communicate with this controller over outbound TLS with HMAC mutual authentication and Docker Engine socket integration.
    </p>
    <div style="font-size:11px;font-family:var(--font-mono);background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;padding:12px;line-height:1.7">
      <div style="color:var(--muted)"># 1. Issue token above, then run on the target host:</div>
      <div>fleet-agent --token &lt;token&gt; --server ${window.location.origin}</div>
    </div>
  </div>`;

  return `<div>${pageHeader('Fleet Settings', 'Subsystem health, enrollment tokens, and agent configuration.')}
    ${statusCard}
    ${enrCard}
    ${quickstart}
  </div>`;
}

window.openEnrollModal=function(){
  const html=`<div class="overlay"><div class="modal" role="dialog" aria-modal="true" aria-labelledby="enrollTitle" style="max-width:540px">
    <h3 id="enrollTitle">Enroll New Server</h3>
    <p class="muted" style="margin:6px 0 14px;font-size:12px">Generate a single-use token to register a fleet-agent on a host.</p>
    <label for="enrollServerId">Server ID / Host Name</label>
    <input id="enrollServerId" placeholder="e.g. srv-prod-node-1" autocomplete="off" required maxlength="64">
    <div class="muted" style="font-size:11px;margin-top:2px">Letters, numbers, hyphens, underscores, dots only (max 64 chars).</div>
    <div style="height:10px"></div>
    <label for="enrollDuration">Token Expiration</label>
    <select id="enrollDuration">
      <option value="15m" selected>15 minutes</option>
      <option value="1h">1 hour</option>
      <option value="24h">24 hours</option>
      <option value="72h">3 days</option>
      <option value="never">No expiry</option>
    </select>
    <div id="enrollResult" style="display:none;margin-top:14px;padding:12px;background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px">
      <div style="font-weight:600;font-size:11px;color:var(--success);margin-bottom:6px">Token Generated Successfully</div>
      <div class="muted" style="font-size:11px;margin-bottom:6px">Run this command on your host:</div>
      <div style="display:flex;gap:6px;align-items:center">
        <input id="enrollCmd" class="mono" style="font-size:11px" readonly>
        <button class="btn ghost" style="padding:6px 10px;font-size:11px" onclick="copyEnrollCmd(this)">Copy</button>
      </div>
    </div>
    <div style="margin-top:16px;display:flex;gap:8px;justify-content:flex-end">
      <button class="btn" id="enrollSubmitBtn" onclick="doCreateEnrollment()">Generate Token</button>
      <button class="btn ghost" onclick="closeOverlay()">Close</button>
    </div>
  </div></div>`;
  document.body.insertAdjacentHTML('beforeend', html);
  document.getElementById('enrollServerId')?.focus();
};

window.doCreateEnrollment=async function(){
  const srvId = (document.getElementById('enrollServerId')?.value||'').trim();
  const dur = document.getElementById('enrollDuration')?.value || '15m';
  if(!srvId){ toast('Server ID is required'); return; }
  if(!/^[a-zA-Z0-9_.-]{1,64}$/.test(srvId)){
    toast('Server ID must be 1-64 characters and contain only letters, numbers, hyphens, underscores, and dots');
    return;
  }
  try{
    const res = await apiLocal('/api/v1/fleet/enrollments', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ server_id: srvId, duration: dur })
    });
    const cmd = `fleet-agent --token ${res.token} --server ${window.location.origin}`;
    const cmdInput = document.getElementById('enrollCmd');
    if(cmdInput) cmdInput.value = cmd;
    const resDiv = document.getElementById('enrollResult');
    if(resDiv) resDiv.style.display = 'block';
    const subBtn = document.getElementById('enrollSubmitBtn');
    if(subBtn) subBtn.style.display = 'none';
    toast('Enrollment token issued');
    await refresh();
  }catch(e){
    toast('Enrollment failed: ' + e.message);
  }
};

window.copyEnrollCmd=function(btn){
  const inp = document.getElementById('enrollCmd');
  if(!inp) return;
  copyText2(inp.value, btn);
  toast('Command copied to clipboard');
};

window.revokeServer=async function(id){
  if(!id) return;
  const ok = await askConfirm({
    title: 'Revoke Server Credentials',
    body: 'Revoking server "'+id+'" will immediately disconnect the agent and revoke all associated credentials. This action cannot be undone.',
    confirmLabel: 'Revoke Server',
    danger: true,
    typeToConfirm: id
  });
  if(!ok) return;
  try{
    await apiLocal('/api/v1/fleet/servers/' + encodeURIComponent(id) + '/revoke', { method: 'POST' });
    toast('Server "'+id+'" revoked');
    await refresh();
    render();
  }catch(e){
    toast('Revoke failed: ' + e.message);
  }
};

async function deleteServer(id){
  if(!id) return;
  const ok = await askConfirm({
    title: 'Hapus Server Permanen',
    body: 'PERINGATAN: Menghapus server "'+id+'" akan memutus koneksi agen, mencabut akses, dan menghapus data server ini secara permanen dari dashboard. Tindakan ini tidak dapat dibatalkan.',
    confirmLabel: 'Hapus Server',
    danger: true,
    typeToConfirm: id
  });
  if(!ok) return;
  try{
    await apiLocal('/api/v1/fleet/servers/' + encodeURIComponent(id), { method: 'DELETE' });
    toast('Server "'+id+'" deleted');
    await refresh();
    render();
  }catch(e){
    toast('Delete failed: ' + e.message);
  }
};

async function revokeEnrollment(prefix, srvId){
  if(!prefix) return;
  const ok = await askConfirm({
    title: 'Revoke Enrollment Token',
    body: 'PERINGATAN: Mencabut token "'+prefix+'..." untuk server "'+srvId+'" akan membatalkan token ini secara permanen. Host yang mencoba mendaftar dengan token ini akan ditolak.',
    confirmLabel: 'Revoke Token',
    danger: true
  });
  if(!ok) return;
  try{
    await apiLocal('/api/v1/fleet/enrollments/' + encodeURIComponent(prefix), { method: 'DELETE' });
    toast('Enrollment token revoked');
    await refresh();
    render();
  }catch(e){
    toast('Revoke failed: ' + e.message);
  }
};

async function reissueEnrollment(prefix, srvId){
  if(!prefix) return;
  const ok = await askConfirm({
    title: 'Revoke & Terbitkan Token Baru',
    body: 'PERINGATAN: Token lama "'+prefix+'..." akan segera dicabut (invalid) dan sistem akan membuat TOKEN BARU pengganti untuk server "'+srvId+'". Agen yang belum selesai mendaftar dengan token lama tidak akan bisa terkoneksi. Lanjutkan?',
    confirmLabel: 'Revoke & Terbitkan Token Baru',
    danger: true
  });
  if(!ok) return;
  try{
    const res = await apiLocal('/api/v1/fleet/enrollments/' + encodeURIComponent(prefix) + '/reissue', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ duration: '15m' })
    });
    toast('Token lama dicabut, token baru berhasil diterbitkan');
    await refresh();
    render();
    showTokenModal(res.token, res.server_id, res.expires_in);
  }catch(e){
    toast('Re-issue failed: ' + e.message);
  }
};

function showTokenModal(token, srvId, expiresIn){
  const cmd = 'fleet-agent --token ' + token + ' --server ' + window.location.origin;
  const html = `<div class="overlay"><div class="modal" role="dialog" aria-modal="true" style="max-width:540px">
    <h3>New Token Issued</h3>
    <p class="muted" style="margin:6px 0 14px;font-size:12px">Token pengganti untuk server <b>${esc(srvId)}</b> berhasil diterbitkan (berlaku: ${esc(expiresIn||'15m')}).</p>
    <div style="padding:12px;background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;margin-bottom:14px">
      <div style="font-weight:600;font-size:11px;color:var(--success);margin-bottom:6px">Jalankan perintah ini pada host target:</div>
      <div style="display:flex;gap:6px;align-items:center">
        <input id="newEnrollCmd" class="mono" style="font-size:11px" value="${esc(cmd)}" readonly>
        <button class="btn ghost" style="padding:6px 10px;font-size:11px" onclick="copyNewEnrollCmd(this)">Copy</button>
      </div>
    </div>
    <div style="display:flex;justify-content:flex-end">
      <button class="btn" onclick="closeOverlay()">Done</button>
    </div>
  </div></div>`;
  document.body.insertAdjacentHTML('beforeend', html);
  document.getElementById('newEnrollCmd')?.select();
}
window.showTokenModal = showTokenModal;

function copyNewEnrollCmd(btn){
  const inp = document.getElementById('newEnrollCmd');
  if(!inp) return;
  copyText2(inp.value, btn);
  toast('Command copied to clipboard');
}
window.copyNewEnrollCmd = copyNewEnrollCmd;

window.startContainer=async function(id, serverId){
  try{
    const q = serverId ? '?server_id=' + encodeURIComponent(serverId) : '';
    const res = await apiLocal('/api/v1/fleet/containers/' + encodeURIComponent(id) + '/start' + q, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ server_id: serverId })
    });
    toast('Start container job dispatched...');
    const finalJob = await waitFleetJobDone(res.job_id);
    handleJobOutcome(finalJob, 'Start container');
    await refresh();
    render();
  }catch(e){
    toast('Start container failed: ' + e.message);
  }
};

window.stopContainer=async function(id, name, serverId){
  const targetName = (name && name !== '-') ? name : id;
  const ok = await askConfirm({
    title: 'Stop Container',
    body: 'Stop container "'+targetName+'" on host server "'+(serverId||'-')+'"?',
    confirmLabel: 'Stop Container',
    danger: true
  });
  if(!ok) return;
  try{
    const q = serverId ? '?server_id=' + encodeURIComponent(serverId) : '';
    const res = await apiLocal('/api/v1/fleet/containers/' + encodeURIComponent(id) + '/stop' + q, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ server_id: serverId })
    });
    toast('Stop container job dispatched...');
    const finalJob = await waitFleetJobDone(res.job_id);
    handleJobOutcome(finalJob, 'Stop container');
    await refresh();
    render();
  }catch(e){
    toast('Stop container failed: ' + e.message);
  }
};

window.restartContainer=async function(id, name, serverId){
  const targetName = (name && name !== '-') ? name : id;
  const ok = await askConfirm({
    title: 'Restart Container',
    body: 'Restart container "'+targetName+'" on host server "'+(serverId||'-')+'"?',
    confirmLabel: 'Restart Container',
    danger: true
  });
  if(!ok) return;
  try{
    const q = serverId ? '?server_id=' + encodeURIComponent(serverId) : '';
    const res = await apiLocal('/api/v1/fleet/containers/' + encodeURIComponent(id) + '/restart' + q, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ server_id: serverId })
    });
    toast('Restart container job dispatched...');
    const finalJob = await waitFleetJobDone(res.job_id);
    handleJobOutcome(finalJob, 'Restart container');
    await refresh();
    render();
  }catch(e){
    toast('Restart container failed: ' + e.message);
  }
};

window.removeContainer=async function(id, name, serverId, isRunning){
  const targetName = (name && name !== '-') ? name : id;
  const ok = await askConfirm({
    title: 'Remove Container',
    body: isRunning
      ? 'Force-remove RUNNING container "'+targetName+'" on host "'+(serverId||'-')+'"? This stops and deletes it immediately.'
      : 'Permanently remove container "'+targetName+'" on host server "'+(serverId||'-')+'"? Data in anonymous volumes follows the container.',
    confirmLabel: isRunning ? 'Force Remove' : 'Remove Container',
    danger: true,
    typeToConfirm: targetName
  });
  if(!ok) return;
  try{
    const q = serverId ? '?server_id=' + encodeURIComponent(serverId) + (isRunning ? '&force=true' : '') : (isRunning ? '?force=true' : '');
    const res = await apiLocal('/api/v1/fleet/containers/' + encodeURIComponent(id) + '/remove' + q, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ server_id: serverId, force: !!isRunning })
    });
    toast('Remove container job dispatched...');
    const finalJob = await waitFleetJobDone(res.job_id);
    handleJobOutcome(finalJob, 'Remove container');
    await waitFleetContainerGone(id, serverId);
    await refresh();
    render();
  }catch(e){
    toast('Remove container failed: ' + e.message);
  }
};

function handleJobOutcome(j, actionLabel){
  if(!j){
    toast(actionLabel + ' still running in background. Check Jobs tab.');
    return;
  }
  const st = j.status || (j.job && j.job.status);
  const errMsg = j.error || (j.job && j.job.error);
  if(st === 'failed'){
    toast(actionLabel + ' failed on host: ' + (errMsg || 'unknown error'));
  } else {
    toast(actionLabel + ' succeeded');
  }
}
window.viewContainerInspect = function(id){
  const cnts = state.fleetContainers || [];
  const c = cnts.find(item => item.id === id || (item.id||'').startsWith(id));
  if(!c) return;
  const rawName = (c.names && c.names[0]) ? c.names[0].replace(/^\//, '') : c.id;
  const isRunning = (c.state || '').toLowerCase() === 'running';
  const badge = fleetStatusBadge(c.state, c.status || c.state);
  const createdDate = c.created ? new Date(c.created * 1000).toLocaleString() : '-';
  const labels = Object.entries(c.labels || {});

  const labelsHtml = labels.length
    ? `<div style="display:flex;flex-wrap:wrap;gap:5px">${labels.map(([k, v]) => `<span class="chip mono" style="font-size:10px" title="${esc(k)}=${esc(v)}"><strong>${esc(k)}</strong>: ${esc(v)}</span>`).join('')}</div>`
    : '<span class="muted" style="font-size:11px">No labels defined</span>';

  const body = `<div style="display:grid;gap:14px;font-family:var(--font-mono);font-size:12px">
    <div class="row" style="gap:16px;flex-wrap:wrap;background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;padding:12px">
      <div style="flex:1 1 180px">
        <div class="muted" style="font-size:10px;text-transform:uppercase;margin-bottom:2px">Container Name</div>
        <div style="font-weight:600;font-size:13px">${esc(rawName)}</div>
      </div>
      <div style="flex:1 1 180px">
        <div class="muted" style="font-size:10px;text-transform:uppercase;margin-bottom:2px">Status</div>
        <div>${badge}</div>
      </div>
      <div style="flex:1 1 180px">
        <div class="muted" style="font-size:10px;text-transform:uppercase;margin-bottom:2px">Host Server</div>
        <button class="slink mono" style="font-size:11px;background:none;border:none;padding:0;cursor:pointer;color:var(--primary-text)" data-srv="${esc(c.server_id||'')}" onclick="state.fleetServerFilter=this.dataset.srv;closeDrw();goView('fleet-containers')">${esc(c.server_id || 'local-host')}</button>
      </div>
    </div>

    <div style="background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;padding:12px;display:grid;gap:10px">
      <div>
        <span class="muted" style="font-size:10px;text-transform:uppercase;display:block;margin-bottom:2px">Full Container ID</span>
        <div style="display:flex;align-items:center;gap:8px">
          <code style="font-size:11px;word-break:break-all">${esc(c.id)}</code>
          <button class="cbtn" data-copy="${esc(c.id)}" onclick="copyText2(this.dataset.copy, this)">Copy</button>
        </div>
      </div>
      <div>
        <span class="muted" style="font-size:10px;text-transform:uppercase;display:block;margin-bottom:2px">Image</span>
        <code style="font-size:11px">${esc(c.image || '-')}</code>
      </div>
      <div>
        <span class="muted" style="font-size:10px;text-transform:uppercase;display:block;margin-bottom:2px">Command</span>
        <code style="font-size:11px">${esc(c.command || '-')}</code>
      </div>
      <div>
        <span class="muted" style="font-size:10px;text-transform:uppercase;display:block;margin-bottom:4px">Ports & Network Bindings</span>
        <div>${renderPortBadges(c.ports)}</div>
      </div>
      <div>
        <span class="muted" style="font-size:10px;text-transform:uppercase;display:block;margin-bottom:2px">Created At</span>
        <span style="font-size:11px">${esc(createdDate)}</span>
      </div>
    </div>

    <div style="background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;padding:12px">
      <span class="muted" style="font-size:10px;text-transform:uppercase;display:block;margin-bottom:6px">Docker Labels (${labels.length})</span>
      ${labelsHtml}
    </div>
  </div>`;

  state._inspectCid = c.id;
  state._inspectName = rawName;
  state._inspectSrv = c.server_id || '';

  const actions = [
    { label: 'View Logs', fn: `viewContainerLogs(state._inspectCid, state._inspectName, state._inspectSrv)` },
    isRunning
      ? { label: 'Restart', fn: `restartContainer(state._inspectCid, state._inspectName, state._inspectSrv)` }
      : { label: 'Start', fn: `startContainer(state._inspectCid, state._inspectSrv)` }
  ];

  nodeDrw('Inspect: ' + rawName, '', body, actions);
};

window.viewContainerLogs=async function(id, name, serverId){
  if(state._fleetLogsState && state._fleetLogsState.autoTimer){
    clearInterval(state._fleetLogsState.autoTimer);
    state._fleetLogsState.autoTimer = null;
  }
  const prevLevel = state._fleetLogsState ? state._fleetLogsState.level : 'all';
  const prevFilter = state._fleetLogsState ? state._fleetLogsState.filter : '';
  state._fleetLogsState = { mode: 'single', id, name, serverId, rawLines: [], filter: prevFilter, level: prevLevel, autoRefresh: true };
  openDrw('Logs: ' + (name || id), `<div id="fleetLogMeta" style="font:11px/1.9 var(--font-mono);color:var(--muted);margin-bottom:8px">Loading logs...</div><div id="fleetLogWrap" style="font-family:var(--font-mono);font-size:12px;line-height:1.7;background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;padding:8px 0;max-width:100%;overflow-x:hidden"><div class="muted" style="padding:14px;text-align:center">Fetching container logs...</div></div>`, true);
  renderFleetLogsActions();
  startFleetLogsAuto();
  await refreshFleetLogs();
};

window.viewAllContainersLogs=async function(){
  if(state._fleetLogsState && state._fleetLogsState.autoTimer){
    clearInterval(state._fleetLogsState.autoTimer);
    state._fleetLogsState.autoTimer = null;
  }
  const cnts = state.fleetContainers || [];
  const running = cnts.filter(c => (c.state||'').toLowerCase() === 'running');
  const prevLevel = state._fleetLogsState ? state._fleetLogsState.level : 'all';
  const prevFilter = state._fleetLogsState ? state._fleetLogsState.filter : '';
  state._fleetLogsState = { mode: 'all', id: 'all', name: 'All Running Containers', serverId: '', rawLines: [], filter: prevFilter, level: prevLevel, autoRefresh: true };
  openDrw('Logs: All Containers', `<div id="fleetLogMeta" style="font:11px/1.9 var(--font-mono);color:var(--muted);margin-bottom:8px">Fetching logs from ${running.length} containers...</div><div id="fleetLogWrap" style="font-family:var(--font-mono);font-size:12px;line-height:1.7;background:var(--bg-canvas);border:1px solid var(--border);border-radius:4px;padding:8px 0;max-width:100%;overflow-x:hidden"><div class="muted" style="padding:14px;text-align:center">Fetching logs from ${running.length} running containers...</div></div>`, true);
  renderFleetLogsActions();
  startFleetLogsAuto();
  await refreshFleetLogs();
};

function renderFleetLogsActions(){
  const s = state._fleetLogsState;
  if(!s) return;
  const act = document.getElementById('drwActions');
  if(!act) return;
  const cnts = state.fleetContainers || [];
  const runningCnts = cnts.filter(c => (c.state||'').toLowerCase() === 'running');

  const cntOpts = `<option value="all" ${s.mode==='all'?'selected':''}>All Running (${runningCnts.length})</option>`
    + cnts.map(c => {
        const rawName = (c.names && c.names[0]) ? c.names[0].replace(/^\//, '') : (c.id||'').slice(0, 12);
        const isSel = s.mode === 'single' && (s.id === c.id || s.name === rawName);
        return `<option value="${esc(c.id)}" ${isSel?'selected':''}>${esc(rawName)}</option>`;
      }).join('');

  act.innerHTML = `<select id="drwLogContainerSel" onchange="switchFleetLogContainer(this.value)" style="height:24px;padding:1px 6px;font:11px var(--font-mono);background:var(--bg-canvas);border:1px solid var(--border);border-radius:3px;color:var(--text);flex:1 1 110px;max-width:140px;min-width:90px" title="Pilih container" aria-label="Pilih container">${cntOpts}</select>`
    + `<select id="drwLogLevelSel" onchange="filterFleetLogLevel(this.value)" style="height:24px;padding:1px 6px;font:11px var(--font-mono);background:var(--bg-canvas);border:1px solid var(--border);border-radius:3px;color:var(--text);flex:0 0 auto;width:95px" title="Filter level log" aria-label="Filter level log">`
    + `<option value="all" ${s.level==='all'?'selected':''}>All Levels</option>`
    + `<option value="error" ${s.level==='error'?'selected':''}>ERROR</option>`
    + `<option value="warn" ${s.level==='warn'?'selected':''}>WARN</option>`
    + `<option value="info" ${s.level==='info'?'selected':''}>INFO</option>`
    + `<option value="debug" ${s.level==='debug'?'selected':''}>DEBUG</option>`
    + `</select>`
    + `<input id="drwLogQ" placeholder="Filter..." value="${esc(s.filter||'')}" oninput="filterFleetLogs(this.value)" style="height:22px;padding:2px 8px;font:11px var(--font-mono);background:var(--bg-canvas);border:1px solid var(--border);border-radius:3px;color:var(--text);flex:1 1 80px;max-width:110px;min-width:60px" aria-label="Filter container logs">`
    + `<button id="drwLogAutoBtn" class="cbtn ${s.autoRefresh?'ok':''}" onclick="toggleFleetLogsAuto()" title="${s.autoRefresh?'Pause live auto-refresh (3s)':'Start live auto-refresh every 3s'}">${s.autoRefresh?'⏸ Auto':'▶ Auto'}</button>`
    + `<button class="cbtn" onclick="copyFleetLogs(this)">Copy</button>`
    + `<button class="cbtn" onclick="refreshFleetLogs()">Refresh</button>`;
}

function startFleetLogsAuto(){
  const s = state._fleetLogsState;
  if(!s) return;
  if(s.autoTimer){ clearInterval(s.autoTimer); s.autoTimer = null; }
  if(s.autoRefresh){
    s.autoTimer = setInterval(async () => {
      if(!state._fleetLogsState || state._fleetLogsState !== s || !state._fleetLogsState.autoRefresh){
        if(s.autoTimer) { clearInterval(s.autoTimer); s.autoTimer = null; }
        return;
      }
      await refreshFleetLogs(true);
    }, 3000);
  }
}

window.toggleFleetLogsAuto = function(){
  const s = state._fleetLogsState;
  if(!s) return;
  s.autoRefresh = !s.autoRefresh;
  startFleetLogsAuto();
  const btn = document.getElementById('drwLogAutoBtn');
  if(btn){
    btn.textContent = s.autoRefresh ? '⏸ Auto' : '▶ Auto';
    btn.classList.toggle('ok', !!s.autoRefresh);
    btn.title = s.autoRefresh ? 'Pause live auto-refresh (3s)' : 'Start live auto-refresh every 3s';
  }
};

window.switchFleetLogContainer = async function(val){
  if(val === 'all'){
    await viewAllContainersLogs();
  } else {
    const cnts = state.fleetContainers || [];
    const c = cnts.find(item => item.id === val || (item.id||'').startsWith(val));
    const rawName = c ? ((c.names && c.names[0]) ? c.names[0].replace(/^\//, '') : c.id.slice(0, 12)) : val;
    const serverId = c ? (c.server_id || '') : '';
    await viewContainerLogs(val, rawName, serverId);
  }
};

window.filterFleetLogLevel = function(lvl){
  if(!state._fleetLogsState) return;
  state._fleetLogsState.level = lvl || 'all';
  updateFleetLogsBody(true);
};

async function refreshFleetLogs(keepScroll){
  const s = state._fleetLogsState;
  if(!s) return;
  try{
    if(s.mode === 'all'){
      const cnts = state.fleetContainers || [];
      const running = cnts.filter(c => (c.state||'').toLowerCase() === 'running');
      const fetched = await Promise.all(running.map(async c => {
        const rawName = (c.names && c.names[0]) ? c.names[0].replace(/^\//, '') : (c.id||'').slice(0, 12);
        const srv = c.server_id || '';
        const q = srv ? '?server_id=' + encodeURIComponent(srv) + '&tail=40' : '?tail=40';
        try{
          const res = await apiLocal('/api/v1/fleet/containers/' + encodeURIComponent(c.id) + '/logs' + q);
          return (res.lines || []).map(line => ({ container: rawName, line }));
        }catch(e){
          return [{ container: rawName, line: `(failed to load: ${e.message})` }];
        }
      }));
      const all = fetched.flat();
      all.sort((a, b) => {
        const tA = parseLogLine(a.line).ts;
        const tB = parseLogLine(b.line).ts;
        if(tA && tB) return tA.localeCompare(tB);
        return 0;
      });
      s.rawLines = all;
    } else {
      const q = s.serverId ? '?server_id=' + encodeURIComponent(s.serverId) + '&tail=150' : '?tail=150';
      const res = await apiLocal('/api/v1/fleet/containers/' + encodeURIComponent(s.id) + '/logs' + q);
      s.rawLines = (res.lines || []).map(line => ({ container: '', line }));
    }
    updateFleetLogsBody(!!keepScroll);
  }catch(e){
    toast('Failed to load logs: ' + e.message);
  }
}
window.refreshFleetLogs = refreshFleetLogs;

function filterFleetLogs(val){
  if(!state._fleetLogsState) return;
  state._fleetLogsState.filter = String(val || '');
  updateFleetLogsBody(true);
}
window.filterFleetLogs = filterFleetLogs;

function copyFleetLogs(btn){
  const s = state._fleetLogsState;
  if(!s) return;
  const text = (s.rawLines || []).map(entry => {
    const raw = typeof entry === 'string' ? entry : entry.line;
    const c = (typeof entry === 'object' && entry.container) ? `[${entry.container}] ` : '';
    return c + raw;
  }).join('\n');
  copyText2(text, btn);
}
window.copyFleetLogs = copyFleetLogs;

function matchLogLevel(parsed, rawText, filterLevel){
  if(!filterLevel || filterLevel === 'all') return true;
  const lvl = (parsed.lvl || '').toUpperCase();
  const raw = String(rawText || '').toLowerCase();
  if(filterLevel === 'error'){
    return lvl === 'ERROR' || lvl === 'FATAL' || /\b(error|fatal|panic|exception|failed|failure)\b/i.test(raw);
  }
  if(filterLevel === 'warn'){
    return lvl === 'WARN' || lvl === 'WARNING' || /\b(warn|warning)\b/i.test(raw);
  }
  if(filterLevel === 'info'){
    return lvl === 'INFO' || (!lvl && !/\b(error|fatal|panic|warn|warning|debug|trace)\b/i.test(raw));
  }
  if(filterLevel === 'debug'){
    return lvl === 'DEBUG' || lvl === 'TRACE' || /\b(debug|trace)\b/i.test(raw);
  }
  return true;
}

function updateFleetLogsBody(keepScroll){
  const s = state._fleetLogsState;
  if(!s) return;
  const metaEl = document.getElementById('fleetLogMeta');
  const wrapEl = document.getElementById('fleetLogWrap');
  if(!wrapEl) return;

  const rawEntries = s.rawLines || [];
  const q = (s.filter || '').toLowerCase();
  const targetLevel = s.level || 'all';

  const filtered = rawEntries.filter(entry => {
    const raw = typeof entry === 'string' ? entry : entry.line;
    const cName = (typeof entry === 'object' && entry.container) ? entry.container.toLowerCase() : '';
    const parsed = parseLogLine(raw);
    if(!matchLogLevel(parsed, raw, targetLevel)) return false;
    if(q){
      if(!raw.toLowerCase().includes(q) && !cName.includes(q)) return false;
    }
    return true;
  });

  const levelTag = targetLevel !== 'all' ? ` [${targetLevel.toUpperCase()}]` : '';
  const countLabel = (q || targetLevel !== 'all') ? `${filtered.length} of ${rawEntries.length} lines${levelTag}` : `Last ${rawEntries.length} lines`;

  const drwB = document.getElementById('drwBody');
  const wasAtBottom = drwB ? (drwB.scrollHeight - drwB.scrollTop - drwB.clientHeight < 60) : false;

  if(metaEl) metaEl.textContent = countLabel;
  wrapEl.innerHTML = filtered.length ? renderLogLines(filtered) : `<div class="muted" style="padding:14px;text-align:center">${(q || targetLevel !== 'all') ? 'No log lines match filter criteria.' : '(no log output captured)'}</div>`;

  if(drwB){
    if(!keepScroll || wasAtBottom){
      drwB.scrollTop = drwB.scrollHeight;
    }
  }
}

function renderLogLines(entries){
  return entries.map((item, i) => {
    const raw = typeof item === 'string' ? item : item.line;
    const container = (typeof item === 'object' && item.container) ? item.container : '';
    const parsed = parseLogLine(String(raw || ''));
    const badge = parsed.lvl === 'ERROR' ? '<span class="badge b-red">ERROR</span>'
      : parsed.lvl === 'WARN' || parsed.lvl === 'WARNING' ? '<span class="badge b-amber">WARN</span>'
      : parsed.lvl === 'INFO' ? '<span class="badge b-green">INFO</span>'
      : parsed.lvl === 'DEBUG' ? '<span class="badge muted">DEBUG</span>'
      : parsed.lvl ? '<span class="badge muted">' + esc(parsed.lvl) + '</span>' : '';
    const cntBadge = container ? `<span class="chip mono" style="font-size:9.5px;padding:1px 5px;flex:0 0 auto;background:var(--surface2)">${esc(container)}</span>` : '';
    return '<div style="display:flex;gap:8px;padding:2px 12px;max-width:100%;min-width:0;align-items:baseline">'
      + '<span class="muted" style="flex:0 0 34px;min-width:34px;text-align:right;user-select:none">' + (i + 1) + '</span>'
      + cntBadge
      + (parsed.ts ? '<span class="muted" style="flex:0 0 auto">' + esc(parsed.ts) + '</span>' : '')
      + (badge ? '<span style="flex:0 0 auto">' + badge + '</span>' : '')
      + '<span style="flex:1 1 auto;min-width:0;overflow-wrap:anywhere;word-break:break-word;white-space:pre-wrap">' + esc(parsed.msg) + '</span></div>';
  }).join('');
}

function parseLogLine(line){
  let s = String(line || '').replace(/\x1b\[[0-9;]*[a-zA-Z]/g, '').trim();
  if(s.startsWith('{') && s.endsWith('}')){
    try{
      const o = JSON.parse(s);
      const ts = o.time || o.timestamp || o.ts || '';
      const lvl = String(o.level || o.lvl || o.severity || '').toUpperCase();
      const msg = o.msg || o.message || o.log || line;
      return {ts: shortTs(ts), lvl, msg: String(msg)};
    }catch(e){}
  }
  let ts = '', lvl = '', msg = s;
  const mBracket = msg.match(/^\[(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}|\d{1,2}:\d{2}:\d{2}(?:\.\d+)?)\]\s*(.*)$/);
  if(mBracket){
    ts = shortTs(mBracket[1]);
    msg = mBracket[2];
  }else{
    const mIso = msg.match(/^(\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)\s*(.*)$/);
    if(mIso){ ts = shortTs(mIso[1]); msg = mIso[2]; }
  }
  const mLvlFmt = msg.match(/\blevel=([A-Za-z]+)\b/i);
  if(mLvlFmt){
    lvl = mLvlFmt[1].toUpperCase();
  }else{
    const mPrefLvl = msg.match(/^\[?(INFO|WARN|WARNING|ERROR|FATAL|DEBUG|TRACE)\]?:?\s*(.*)$/i);
    if(mPrefLvl){ lvl = mPrefLvl[1].toUpperCase(); msg = mPrefLvl[2]; }
  }
  if(!lvl){
    if(/\b(error|fatal|panic|exception)\b/i.test(msg)){
      lvl = 'ERROR';
    }else if(/\b(warn|warning)\b/i.test(msg)){
      lvl = 'WARN';
    }else if(/\b(info)\b/i.test(msg)){
      lvl = 'INFO';
    }else if(/\b(debug|trace)\b/i.test(msg)){
      lvl = 'DEBUG';
    }
  }
  if(!ts){
    const mTimeFmt = msg.match(/\btime="?([^"\s]+)"?/i);
    if(mTimeFmt) ts = shortTs(mTimeFmt[1]);
  }
  return {ts, lvl, msg};
}

function shortTs(ts){
  ts = String(ts || '').trim();
  if(/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}/.test(ts)) return ts.slice(11, 19);
  if(/^\[?\d{1,2}:\d{2}:\d{2}\]?$/.test(ts)) return ts.replace(/[\[\]]/g, '');
  return ts.length > 24 ? ts.slice(0, 24) : ts;
}
function goFleetContainers(serverId){
  state.fleetServerFilter=serverId||'';
  goView('fleet-containers');
}
window.goFleetContainers=goFleetContainers;

/* Broker logs viewer */



// Window bindings for Fleet actions and handlers
window.refreshFleetView = refreshFleetView;
window.patchFleetView = patchFleetView;
window.fleetFilter = fleetFilter;
window.waitFleetJobDone = waitFleetJobDone;
window.waitFleetContainerGone = waitFleetContainerGone;
window.fleetStatusBadge = fleetStatusBadge;
window.serverStatusInfo = serverStatusInfo;
window.renderPortBadges = renderPortBadges;
window.viewFleetDisabled = viewFleetDisabled;
window.viewFleetServers = viewFleetServers;
window.viewFleetContainers = viewFleetContainers;
window.viewFleetJobs = viewFleetJobs;
window.viewFleetAudit = viewFleetAudit;
window.viewFleetSettings = viewFleetSettings;
window.handleJobOutcome = handleJobOutcome;
window.renderFleetLogsActions = renderFleetLogsActions;
window.startFleetLogsAuto = startFleetLogsAuto;
window.refreshFleetLogs = refreshFleetLogs;
window.filterFleetLogs = filterFleetLogs;
window.copyFleetLogs = copyFleetLogs;
window.matchLogLevel = matchLogLevel;
window.updateFleetLogsBody = updateFleetLogsBody;
window.renderLogLines = renderLogLines;
window.parseLogLine = parseLogLine;
window.shortTs = shortTs;
window.deleteServer = deleteServer;
window.revokeEnrollment = revokeEnrollment;
window.reissueEnrollment = reissueEnrollment;
const openEnrollModal = (...args) => window.openEnrollModal?.(...args);
const doCreateEnrollment = (...args) => window.doCreateEnrollment?.(...args);
const copyEnrollCmd = (...args) => window.copyEnrollCmd?.(...args);
const revokeServer = (...args) => window.revokeServer?.(...args);
const startContainer = (...args) => window.startContainer?.(...args);
const stopContainer = (...args) => window.stopContainer?.(...args);
const restartContainer = (...args) => window.restartContainer?.(...args);
const removeContainer = (...args) => window.removeContainer?.(...args);
const viewContainerInspect = (...args) => window.viewContainerInspect?.(...args);
const viewContainerLogs = (...args) => window.viewContainerLogs?.(...args);
const viewAllContainersLogs = (...args) => window.viewAllContainersLogs?.(...args);
const toggleFleetLogsAuto = (...args) => window.toggleFleetLogsAuto?.(...args);
const switchFleetLogContainer = (...args) => window.switchFleetLogContainer?.(...args);
const filterFleetLogLevel = (...args) => window.filterFleetLogLevel?.(...args);


export {
  refreshFleetView, patchFleetView, fleetFilter, waitFleetJobDone, waitFleetContainerGone, fleetStatusBadge, serverStatusInfo, renderPortBadges, viewFleetDisabled, viewFleetServers, viewFleetContainers, viewFleetJobs, viewFleetAudit, viewFleetSettings, openEnrollModal, doCreateEnrollment, copyEnrollCmd, revokeServer, startContainer, stopContainer, restartContainer, removeContainer, handleJobOutcome, viewContainerInspect, viewContainerLogs, viewAllContainersLogs, renderFleetLogsActions, startFleetLogsAuto, toggleFleetLogsAuto, switchFleetLogContainer, filterFleetLogLevel, refreshFleetLogs, filterFleetLogs, copyFleetLogs, matchLogLevel, updateFleetLogsBody, renderLogLines, parseLogLine, shortTs, goFleetContainers,
  deleteServer, revokeEnrollment, reissueEnrollment, showTokenModal, copyNewEnrollCmd
};
