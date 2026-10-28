// PocketKafka Kafka Zone Views
import {
  state, $, I, esc, fmtBytes, fmtBytesOpt, fmtRateOpt, fmtPctOpt, isNum,
  toast, skel, redactTargetEndpoint, rel, online, jsArg, copyText2, copyText,
  checkAlarms, activeAlerts, groupLagWarn
} from '../state.js';
import {
  api, apiLocal, LOCAL_API_PATH, kafkaCapabilityFor, isKafkaTarget,
  isTargetUnavailable, targetUnavailableMessage, activeTargetEntry, activeTargetKind,
  activeClusterLabel, hasTargetCapability, setClusterRegistry, getClusterRegistry
} from '../api.js';

let clusterRegistry = null;

const render = (...args) => window.render?.(...args);
const refresh = (...args) => window.refresh?.(...args);
const goView = (...args) => window.goView?.(...args);
const applyHash = (...args) => window.applyHash?.(...args);
const syncHash = (...args) => window.syncHash?.(...args);
const openDrw = (...args) => window.openDrw?.(...args);
const closeDrw = (...args) => window.closeDrw?.(...args);
const nodeDrw = (...args) => window.nodeDrw?.(...args);
const pageHeader = (...args) => window.pageHeader?.(...args);
const tableScroll = (...args) => window.tableScroll?.(...args);
const updateChips = (...args) => window.updateChips?.(...args);
const updateBrokerPicker = (...args) => window.updateBrokerPicker?.(...args);
const bindView = (...args) => window.bindView?.(...args);


const KAFKA_UNSUPPORTED_VIEWS={devstudio:'Developer Studio writes are not supported for Kafka targets.',schemas:'Schema Registry APIs are not supported for Kafka targets.',topology:'Topology is not available for Kafka targets.',storage:'Storage inspection is not available for Kafka targets.',logs:'Broker logs are not available for Kafka targets.',metrics:'Broker metrics are not available for Kafka targets.',integrations:'Integrations is not available for Kafka targets.'};
function navUnavailableReason(view){
  if(isTargetUnavailable()&&!['clusters','settings','security'].includes(view))return targetUnavailableMessage();
  if(!isKafkaTarget())return '';
  if(KAFKA_UNSUPPORTED_VIEWS[view])return KAFKA_UNSUPPORTED_VIEWS[view];
  const needed={overview:'health.read',health:'health.read',connections:'cluster',topics:'topics.read',groups:'groups.read',databrowser:'topic.messages.read',producer:'topic.messages.write',integrations:'integrations.read'}[view];
  if(needed&&!hasTargetCapability(needed))return `Selected Kafka target does not provide ${needed}.`;
  if(view==='topics'&&state.topicDetail&&!hasTargetCapability('topic.partitions.read'))return 'Topic partition details are not supported by this Kafka target.';
  if(view==='groups'&&state.group&&!hasTargetCapability('group.detail.read'))return 'Consumer group details are not supported by this Kafka target.';
  return '';
}
function targetUnavailablePanel(){return `<div class="card" role="alert"><h3>Broker unavailable</h3><p class="muted" style="margin-top:8px">${esc(targetUnavailableMessage())}</p><p class="muted" style="margin-top:8px">The saved selection is preserved. Choose a registered broker from the global Broker selector to continue.</p></div>`;}
function unsupportedPanel(view){
  const reason=navUnavailableReason(view)||'This feature is not supported by the selected broker.';
  const back=view==='topics'&&state.topicDetail?`<button class="btn ghost" style="margin-top:12px" onclick="state.topicDetail=null;state.topicParts=[];state.topicCfg=null;render()">Back to Topics</button>`:
    view==='groups'&&state.group?`<button class="btn ghost" style="margin-top:12px" onclick="state.group=null;state.groupDetail=null;state.groupError=null;location.hash='#/groups';render()">Back to Consumer Groups</button>`:'';
  return `<div class="card" role="status"><h3>Unavailable for ${esc(activeClusterLabel())}</h3><p class="muted" style="margin-top:8px">${esc(reason)}</p>${back}</div>`;
}
function targetSwitchingPanel(){return `<div class="card" role="status" aria-live="polite"><h3>Switching broker</h3><p class="muted" style="margin-top:8px">Loading ${esc(activeClusterLabel())}. Data from the previous broker has been cleared.</p></div>`;}
function topoData(){
  const topics=(state.topics||[]).map(t=>{
    const ps=t.partitions||[];
    return {id:t.name,label:t.name,parts:ps.length,bytes:fmtBytes(t.totalBytes||0),
      end:ps.length?Math.max(...ps.map(p=>p.logEndOffset||0)):0,dlq:/-(dlq|dead)$/i.test(t.name),subscriptions:[]};
  });
  const groups=(state.groups||[]).map(g=>{
    const subs=[...new Set(Object.keys(g.lag||{}).map(k=>k.split('-').slice(0,-1).join('-')))];
    const lagTotal=Object.values(g.lag||{}).reduce((a,b)=>a+b,0);
    return {id:g.name,label:g.name,subs,lag:lagTotal,state:g.state};
  });
  groups.forEach(g=>g.subs.forEach(s=>{const t=topics.find(x=>x.id===s); if(t)t.subscriptions.push(g.id);}));
  return {topics,groups};
}
function topoLayout(n){
  if(n<=8)return {h:56,gap:14,pad:34,rowH:70};
  if(n<=14)return {h:44,gap:9,pad:28,rowH:53};
  return {h:34,gap:6,pad:24,rowH:40};
}
function renderTopoSvg(){
  const svg=document.getElementById('topoSvg'); if(!svg)return;
  const {topics,groups}=topoData();
  if(!topics.length&&!groups.length){svg.innerHTML='';return;}
  const L=topoLayout(topics.length);
  const H=Math.max(240,topics.length*L.rowH+40);
  const CX=470, GX=820, B={x:40,y:H/2-32,w:170,h:64};
  const colT=topics.map((t,i)=>({...t,x:CX,y:i*L.rowH+L.pad}));
  /* consumer groups: antre rapi dari atas (tidak bertumpuk), edge tetap mengikuti */
  const G_H=L.h, GAP=Math.max(12,(topics.length*L.rowH-groups.length*G_H)/(groups.length+1));
  const gc=groups.map((g,i)=>({...g,x:GX,y:L.pad+i*(G_H+GAP)}));
  const seenT=new Set(); let cyT=0;
  const T=colT.map(t=>{
    let y=t.y;
    while(seenT.has(Math.round(y))){y=t.y+(++cyT)*10;}
    seenT.add(Math.round(y)); return {...t,y};
  });
  let s=`<g>
    <rect class="nrect" x="${B.x}" y="${B.y}" width="${B.w}" height="${B.h}" rx="2"/>
    <text x="${B.x+16}" y="${B.y+24}">broker-${esc(String(state.cluster?.brokerId??'-'))}</text>
    <text class="sub" x="${B.x+16}" y="${B.y+42}">${esc(state.cluster?.security||'PLAINTEXT')} - ${esc(state.cluster?.version||'')}</text>
    <text class="sub" x="${B.x+16}" y="${B.y+56}">${fmtBytes(state.cluster?.totalBytes||0)} on disk</text>
  </g>`;
  T.forEach(t=>{
    s+=`<g><rect class="nrect" x="${t.x}" y="${t.y}" width="150" height="${L.h}" rx="2" onclick="topoPickTopic(${jsArg(t.id)})"/>
      <text x="${t.x+12}" y="${t.y+(L.h/2)-2}">${esc(t.id)}</text>
      <text class="sub" x="${t.x+12}" y="${t.y+L.h-6}">${t.parts} parts - ${t.bytes}${t.dlq?' <tspan fill="var(--danger)">DLQ</tspan>':''}</text></g>`;
  });
  gc.forEach(g=>{
    s+=`<g><rect class="nrect" x="${g.x}" y="${g.y}" width="180" height="${L.h}" rx="2" onclick="topoPickGroup(${jsArg(g.id)})"/>
      <text x="${g.x+12}" y="${g.y+(L.h/2)-2}">${esc(g.id)}</text>
      <text class="sub" x="${g.x+12}" y="${g.y+L.h-6}">lag ${g.lag.toLocaleString()} - ${esc(g.state||'')}</text>
      ${g.lag>groupLagWarn()?`<circle class="errdot" cx="${g.x+168}" cy="${g.y+10}" r="4"/>`:''}</g>`;
  });
  T.forEach(t=>{
    s+=`<path class="edge" d="M ${B.x+B.w} ${B.y+B.h/2} C ${B.x+B.w+80} ${B.y+B.h/2}, ${t.x-90} ${t.y+L.h/2}, ${t.x} ${t.y+L.h/2}"/>`;
  });
  T.forEach(t=>t.subscriptions.forEach(gid=>{
    const g=gc.find(x=>x.id===gid); if(!g)return;
    const hot=state.topoSel&&state.topoSel.type==='topic'&&state.topoSel.id===t.id;
    s+=`<path class="edge ${hot?'hot':''}" d="M ${t.x+150} ${t.y+L.h/2} C ${t.x+220} ${t.y+L.h/2}, ${g.x-110} ${g.y+L.h/2}, ${g.x} ${g.y+L.h/2}"/>`;
  }));
  s+=`<text class="role" x="${B.x}" y="${B.y-14}">cluster</text>
      <text class="role" x="${CX}" y="${L.pad-24}">topics</text>
      <text class="role" x="${GX}" y="${(gc[0]?gc[0].y:L.pad)-10}">consumer groups</text>`;
  svg.setAttribute('viewBox','0 0 1100 '+H);
  svg.innerHTML=s;
}
function topoDetailHtml(t){
  const parts=t.rawPartitions||[];
  const rows=parts.map(p=>
    `<tr><td>P${p.partition}</td><td>${(p.logEndOffset||0).toLocaleString()}</td><td>${(p.highWatermark||0).toLocaleString()}</td><td>${fmtBytes(p.bytes||0)}</td><td>${(p.segments||[]).length}</td></tr>`
  ).join('')||'<tr><td colspan="5" class="muted">no partitions</td></tr>';
  const subs=(t.subscriptions||[]).map(g=>`<span class="chip">${esc(g)}</span>`).join(' ')||'<span class="muted">(none)</span>';
  return `<div style="font:11px/1.9 var(--font-mono);color:var(--muted);margin-bottom:10px">
    ${t.parts} partitions &middot; ${t.bytes} &middot; log end ${t.end.toLocaleString()}
    &middot; subscribers: ${subs}</div>
    ${tableScroll(`<table><thead><tr><th>Part</th><th>Log End</th><th>High Watermark</th><th>Size</th><th>Segments</th></tr></thead><tbody>${rows}</tbody></table>`)}`;
}
window.topoPickTopic=function(name){
  const {topics}=topoData(); const t=topics.find(x=>x.id===name); if(!t)return;
  state.topoSel={type:'topic',id:name};
  api(`/api/v1/topics/${encodeURIComponent(name)}/partitions`).then(ps=>{
    t.rawPartitions=ps||[]; renderTopoSvg();
    nodeDrw('topic: '+name,'',topoDetailHtml(t),
      [{label:'Open in Data Browser',fn:`pickTopicNav('${esc(name)}');closeDrw()`},
       {label:'copy topic name',fn:`copyText2('${esc(name)}',this)`}]);
  }).catch(e=>toast(e.message));
};
window.topoPickGroup=function(name){
  const {groups}=topoData(); const g=groups.find(x=>x.id===name); if(!g)return;
  state.topoSel={type:'group',id:name}; renderTopoSvg();
  api(`/api/v1/groups/${encodeURIComponent(name)}`).then(d=>{
    const rows=(d.offsets||[]).map(o=>`<tr><td>${esc(o.topic)}</td><td>P${o.partition}</td><td>${(o.current_offset??0).toLocaleString()}</td><td>${(o.log_end_offset??0).toLocaleString()}</td><td><span class="badge ${o.lag>0?'b-amber':'b-green'}">${o.lag??0}</span></td></tr>`).join('')||'<tr><td colspan="5" class="muted">no committed offsets</td></tr>';
    const meta=`state ${esc(g.state||'-')} &middot; total lag ${g.lag.toLocaleString()} &middot; subscribed: ${g.subs.map(esc).join(', ')||'(none)'}`;
    nodeDrw('group: '+name,meta,tableScroll(`<table><thead><tr><th>Topic</th><th>Part</th><th>Current</th><th>Log End</th><th>Lag</th></tr></thead><tbody>${rows}</tbody></table>`),
      [{label:'open group page',fn:`closeDrw();openGroup(${jsArg(name)})`}]);
  }).catch(e=>{toast(e.message);});
};
function topoLayer(label,nodesHtml){ return `<div class="topoLayer"><div class="topoLabel">${label}</div><div class="topoNodes">${nodesHtml}</div></div>`; }
function viewTopology(){
  const tops=state.topics||[]; const grps=state.groups||[];
  /* Build subscription map: topic -> [group,...] */
  const topicSubs={};
  tops.forEach(t=>{topicSubs[t.name]=[];});
  grps.forEach(g=>{
    const subs=[...new Set(Object.keys(g.lag||{}).map(k=>k.split('-').slice(0,-1).join('-')))];
    subs.forEach(s=>{if(topicSubs[s])topicSubs[s].push(g.name);});
  });
  /* Card view for mobile */
  const brokerCard=`<div class="topoNode broker">
    <b>broker-${esc(String(state.cluster?.brokerId??'-'))}</b>
    <small>${esc(state.cluster?.security||'PLAINTEXT')} &middot; ${esc(state.cluster?.version||'-')}</small>
    <small>${fmtBytes(state.cluster?.totalBytes||0)} on disk</small>
  </div>`;
  const topicCards=tops.length?tops.map(t=>{
    const subs=topicSubs[t.name]||[];
    const ps=t.partitions||[];
    const bytes=fmtBytes(t.totalBytes||0);
    return `<div class="topoNode" onclick="topoPickTopic(${jsArg(t.name)})">
      <b>${esc(t.name)}</b>
      <small>${ps.length||t.partitionCount||'-'} parts &middot; ${bytes}</small>
      ${subs.length?`<div class="chips">${subs.map(g=>`<span title="${esc(g)}">${esc(g)}</span>`).join('')}</div>`:''}
    </div>`;
  }).join(''):'<div class="muted" style="font-size:11px;padding:8px 0">No topics.</div>';
  const groupCards=grps.length?grps.map(g=>{
    const lagTotal=Object.values(g.lag||{}).reduce((a,b)=>a+b,0);
    const subs=[...new Set(Object.keys(g.lag||{}).map(k=>k.split('-').slice(0,-1).join('-')))];
    return `<div class="topoNode ${lagTotal>0?'topoNodeErr':''}" onclick="topoPickGroup(${jsArg(g.name)})">
      <b>${esc(g.name)}</b>
      <small>lag ${lagTotal.toLocaleString()} &middot; ${esc(g.state||'-')}</small>
      ${subs.length?`<div class="chips">${subs.map(s=>`<span title="${esc(s)}">${esc(s)}</span>`).join('')}</div>`:''}
    </div>`;
  }).join(''):'<div class="muted" style="font-size:11px;padding:8px 0">No consumer groups.</div>';
  const cardView=`<div class="topoCardGrid">
    <div class="topoCol topoColDiv">
      <div class="topoColHead">Cluster</div>
      ${brokerCard}
    </div>
    <div class="topoCol topoColDiv">
      <div class="topoColHead">Topics (${tops.length})</div>
      ${topicCards}
    </div>
    <div class="topoCol">
      <div class="topoColHead">Consumer Groups (${grps.length})</div>
      ${groupCards}
    </div>
  </div>`;
  return `<div>${pageHeader('Topology','Map broker topics and consumer group subscriptions.')}<div class="card" style="padding:0">
    <div style="display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap">
      <h3>Stream Topology</h3>
      <span class="muted" style="font-size:11px;font-family:var(--font-mono)">broker &rarr; topics &rarr; groups &middot; click node for detail</span>
      <span style="flex:1"></span>
      <span class="chip2" style="display:none" id="svgLegend1"><i style="width:14px;height:2px;border-radius:0;background:var(--border);display:inline-block"></i>produce path</span>
      <span class="chip2" style="display:none" id="svgLegend2"><i style="width:14px;height:2px;border-radius:0;background:var(--primary);display:inline-block"></i>subscription (selected)</span>
      <button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="render()">Refresh</button>
    </div>
    <div class="topoWrap topoSvgWrap" style="padding:8px 10px 10px" id="topoSvgSection">
      <svg id="topoSvg" class="topoSvg" viewBox="0 0 1100 300" preserveAspectRatio="xMidYMin meet" style="min-height:${Math.max(240,(tops.length||1)*70+40)}px"></svg>
      ${!tops.length?'<div class="muted" style="padding:14px 8px;font-size:12px">No topics yet. Create a topic or produce a message first.</div>':''}
    </div>
    <div class="topoCardView" style="padding:0">${cardView}</div>
  </div></div>`;
}
window.refreshTopo=function(){renderTopoSvg();render();};
/* Show SVG legend only when SVG wrap is visible */
(function(){const obs=typeof ResizeObserver!=='undefined'?new ResizeObserver(()=>{
  const wrap=document.getElementById('topoSvgSection');
  const show=wrap&&getComputedStyle(wrap).display!=='none';
  ['svgLegend1','svgLegend2'].forEach(id=>{const el=document.getElementById(id);if(el)el.style.display=show?'':'none';});
}):null;
if(obs){const t=document.getElementById('topoSvgSection');if(t)obs.observe(t);}
})();

/* Fitur 13: hash router */
function viewLogin(){
  const v=state.version?(' v'+esc(state.version)):'';
  const isDark=state.theme==='dark';
  return `<div class="loginWrap">
    <div class="loginCard">
      <div style="text-align:center;margin-bottom:22px">
        <img src="/favicon.svg" alt="PocketKafka" width="48" height="48" style="display:block;margin:0 auto 12px;border-radius:10px">
        <h2 style="font-size:18px;font-weight:600;letter-spacing:-.01em;margin-bottom:4px">Sign in to PocketKafka</h2>
        <p class="muted" style="font-size:12px">Enter your credentials to continue</p>
      </div>
      <div id="loginErr" style="display:none;font-size:12px;padding:8px 10px;background:var(--danger-soft);border:1px solid var(--danger-line);color:var(--danger-text);border-radius:4px;margin-bottom:14px"></div>
      <div style="margin-bottom:12px">
        <label for="loginUser" style="margin-bottom:5px">Username</label>
        <input id="loginUser" autocomplete="username" placeholder="Enter username" style="height:36px;font-size:13px;width:100%" autofocus>
      </div>
      <div style="margin-bottom:18px">
        <label for="loginPass" style="margin-bottom:5px">Password</label>
        <input id="loginPass" type="password" autocomplete="current-password" placeholder="Enter password" style="height:36px;font-size:13px;width:100%">
      </div>
      <button class="btn" id="loginBtn" style="width:100%;height:36px;font-size:13px;font-weight:600" onclick="doLogin()">Sign in</button>
    </div>
    <div class="loginFooter">
      <span class="muted" style="font-size:11px">PocketKafka${v}</span>
      <button class="btn ghost" style="padding:3px 8px;font-size:11px;height:auto" onclick="setTheme(state.theme==='dark'?'light':'dark');render();">${isDark?'Light theme':'Dark theme'}</button>
    </div>
  </div>`;
}
function bindLogin(){
  const u=$('#loginUser'),p=$('#loginPass');
  if(u){
    u.addEventListener('keydown',e=>{if(e.key==='Enter'){if(p&&!p.value)p.focus();else doLogin();}});
    setTimeout(()=>{try{u.focus();}catch(e){}},50);
  }
  if(p){
    p.addEventListener('keydown',e=>{if(e.key==='Enter')doLogin();});
  }
}
window.showLogin=function(){state.auth={enabled:true,authenticated:false};render();};
window.doLogin=async function(){
  const u=($('#loginUser')?.value||'').trim(),p=$('#loginPass')?.value||'';
  const errEl=$('#loginErr'),btn=$('#loginBtn');
  if(errEl){errEl.style.display='none';errEl.textContent='';}
  if(!u||!p){
    const msg='Enter username and password';
    if(errEl){errEl.textContent=msg;errEl.style.display='block';}
    toast(msg);
    return;
  }
  if(btn){btn.disabled=true;btn.textContent='Signing in...';}
  try{
    const r=await fetch('/api/v1/auth/login',{
      method:'POST',
      headers:{'Content-Type':'application/json','X-PocketKafka-Request':'1'},
      body:JSON.stringify({username:u,password:p})
    });
    if(!r.ok){
      let msg='Invalid credentials';
      try{const j=await r.json();if(j&&j.error)msg=j.error;}catch(e){}
      if(errEl){errEl.textContent=msg;errEl.style.display='block';}
      toast(msg);
      if(btn){btn.disabled=false;btn.textContent='Sign in';}
      return;
    }
    state.auth={enabled:true,authenticated:true};
    toast('Welcome, '+u);
    await loadClusterList();
    if(!state.targetSwitching)await refresh();
    applyHash();
    render();
  }catch(e){
    if(errEl){errEl.textContent=e.message;errEl.style.display='block';}
    toast(e.message);
    if(btn){btn.disabled=false;btn.textContent='Sign in';}
  }
};
window.logout=async function(){
  try{
    await fetch('/api/v1/auth/logout',{method:'POST',headers:{'X-PocketKafka-Request':'1'}});
  }catch(e){}
  state.auth={enabled:true,authenticated:false};
  render();
};
/* Settings */
function viewGeneralSettings(){
  const c=state.cluster||{};
  const auth=state.auth||{};
  const refreshOpts=[1000,5000,15000,30000,0].map(ms=>`<option value="${ms}" ${state.refreshMs===ms?'selected':''}>${ms===0?'Off':(ms/1000)+'s'}</option>`).join('');
  return `<div>
    <div class="card">
      <div class="kicker">Preferences</div>
      <h3>Appearance</h3>
      <div class="row" style="justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border-soft)">
        <span>Theme</span>
        <span><button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="setTheme('dark')" ${state.theme==='dark'?'aria-pressed="true"':''}>Dark</button>
        <button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="setTheme('light')" ${state.theme==='light'?'aria-pressed="true"':''}>Light</button></span>
      </div>
      <div class="row" style="justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border-soft)">
        <span>Table density</span>
        <span><button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="setDensity('comfortable')">Comfortable</button>
        <button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="setDensity('compact')">Compact</button></span>
      </div>
      <div class="row" style="justify-content:space-between;padding:10px 0">
        <span>Timestamp format</span>
        <span><button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="setTimeFmt('rel')">Relative</button>
        <button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="setTimeFmt('abs')">Absolute</button></span>
      </div>
    </div>
    <div class="card">
      <h3 style="margin-bottom:10px">Auto-refresh</h3>
      <div class="row" style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-soft)">
        <span>Interval</span>
        <select id="refreshSel" style="width:auto" onchange="setRefresh(+this.value)">${refreshOpts}</select>
      </div>
      <div class="row" style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-soft)">
        <span>Consumer lag alert</span>
        <input id="groupLagWarnBox" type="number" min="0" step="1000" value="${state.groupLagWarn}" style="width:130px" onchange="setGroupLagWarn(this.value)">
      </div>
      <div class="muted" style="font-size:11px;margin-top:8px">Pause otomatis saat tab tidak aktif. Alert lag grup menyala di atas angka ini; kelompok sync pada topik besar wajar tertinggal ratusan juta pesan, jadi naikkan bila banner hanya berisik.</div>
    </div>
    <div class="card">
      <h3 style="margin-bottom:10px">Cluster</h3>
      <div class="row" style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-soft)"><span class="k muted">Cluster ID</span><span class="mono">${esc(c.clusterId||'-')}</span></div>
      <div class="row" style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-soft)"><span class="k muted">Broker</span><span class="mono">${esc(c.brokerId??'-')}</span></div>
      <div class="row" style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-soft)"><span class="k muted">Version</span><span class="mono">${esc(c.version||'-')}</span></div>
      <div class="row" style="display:flex;justify-content:space-between;padding:8px 0"><span class="k muted">Disk usage</span><span class="mono">${fmtBytes(c.totalBytes||0)}</span></div>
      <div style="margin-top:12px"><button class="btn ghost" onclick="refreshAll()">Refresh</button></div>
    </div>
    <div class="card">
      <h3 style="margin-bottom:10px">Session</h3>
      <div class="row" style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-soft)">
        <span>Authentication</span>
        <span class="badge ${auth.enabled?'b-amber':'b-green'}">${auth.enabled?'Enabled':'Disabled'}</span>
      </div>
      ${auth.enabled?`<div style="margin-top:12px"><button class="btn danger" onclick="logout()">Sign out</button></div>`:''}
      <div class="muted" style="font-size:11px;margin-top:8px">Web UI auth uses the broker's <code>security.users</code>.</div>
    </div>
    <div class="card">
      <div class="kicker">Read-only</div>
      <h3>Broker configuration</h3>
      <div id="brokerCfgBody" class="muted" style="font-size:12px">${skel(120)}</div>
    </div>
  </div>`;
}
function viewSettings(){
  const tab=state.settingsTab||'general';
  const navTabs=`<nav class="settingsTabs" aria-label="Settings sections">
    <button class="settingsTab ${tab==='general'?'active':''}" type="button" onclick="goSettingsTab('general')" ${tab==='general'?'aria-current="page"':''}>General</button>
    <button class="settingsTab ${tab==='clusters'?'active':''}" type="button" onclick="goSettingsTab('clusters')" ${tab==='clusters'?'aria-current="page"':''}>Clusters</button>
    <button class="settingsTab ${tab==='connections'?'active':''}" type="button" onclick="goSettingsTab('connections')" ${tab==='connections'?'aria-current="page"':''}>Connections</button>
    <button class="settingsTab ${tab==='storage'?'active':''}" type="button" onclick="goSettingsTab('storage')" ${tab==='storage'?'aria-current="page"':''}>Storage</button>
    <button class="settingsTab ${tab==='security'?'active':''}" type="button" onclick="goSettingsTab('security')" ${tab==='security'?'aria-current="page"':''}>Security & ACL</button>
  </nav>`;
  let body='';
  if(tab==='clusters') body=viewClusters();
  else if(tab==='connections') body=viewConnections();
  else if(tab==='storage') body=viewStorage();
  else if(tab==='security') body=viewSecurity();
  else body=viewGeneralSettings();
  return `<div>${navTabs}${body}</div>`;
}

/* Connections */
function viewConnections(){
  const c=state.cluster||{};
  const ingressBody = state.integrations ? renderIntegrations(state.integrations) : `<div id="integrationsBody" class="muted" style="font-size:12px">${skel(120)}</div>`;
  return `<div>${pageHeader('Connections','Broker identity, endpoints, listeners, and ingress surfaces.')}
    <div class="card conn">
      <div class="kicker">Cluster Overview</div>
      <div style="display:flex;gap:32px;flex-wrap:wrap;align-items:flex-end;margin:6px 0 4px">
        <div><div class="statXL"><span id="cvTopics">${c.topics??'--'}</span><small>topics</small></div><div class="statSub" id="cvParts">${c.partitions??'--'} partitions</div></div>
        <div><div class="statXL"><span id="cvBytes">${fmtBytes(c.totalBytes||0)}</span></div><div class="statSub">on disk</div></div>
        <div><div class="statXL"><span id="cvGroups">${c.groups??'--'}</span><small>groups</small></div><div class="statSub">consumer groups</div></div>
        <div style="margin-left:auto;text-align:right">
          <span class="brokerBadge"><span class="dot"></span>BROKER ${esc(String(c.brokerId??'0'))}</span>
          <div class="statSub" style="margin-top:6px">${esc(c.version||'')} &middot; <span id="cvDisk">${c.diskUsagePct!=null?c.diskUsagePct.toFixed(1)+'% disk':''}</span></div>
        </div>
      </div>
      <hr class="hr">
      <div style="display:flex;align-items:center;margin-bottom:8px">
        <span class="dot"></span><b>pocketkafka-local</b><span class="tag">KAFKA</span>
        ${c.clusterId?'<span style="margin-left:auto" class="badge b-green">ONLINE</span>':'<span style="margin-left:auto">'+skel(52)+'</span>'}
      </div>
      <div class="row"><span class="k">Cluster ID</span><span class="mono" id="cvClusterId">${esc(c.clusterId||'-')}</span></div>
      <div class="row"><span class="k">Endpoint</span><span class="mono" id="cvEndpoint">${esc(c.advertised||'localhost:9092')}</span></div>
      <div class="row"><span class="k">Listeners</span><span class="mono" id="cvListeners">${(c.listeners||['-']).map(esc).join(', ')}</span></div>
      <div class="row"><span class="k">Security</span><span class="mono" id="cvSecurity">${esc(c.security||'PLAINTEXT')}</span></div>
      <div class="actions">
        <button class="btn ghost" onclick="goView('databrowser')">Open Data Browser</button>
        <button class="btn ghost" onclick="goView('topology')">View Topology</button>
      </div>
    </div>
    <div class="kicker" style="margin:20px 0 10px">Ingress &amp; Integrations</div>
    <div id="connectionsIngress">${ingressBody}</div>
  </div>`;
}
function viewDataBrowser(){
  const readOnly=isKafkaTarget(),readOnlyNote='Kafka targets are read-only; import and replay are disabled.';
  const opts=(state.topics||[]).map(t=>`<option value="${esc(t.name)}" ${state.topic===t.name?'selected':''}>${esc(t.name)}</option>`).join('');
  const parts=state.topic?state.topics.find(t=>t.name===state.topic)?.partitions||[]:[];
  const act=(on)=>on?'background:var(--primary-soft);color:var(--primary-text)':'';
  const pills='<button class="pill'+(state.partition==='all'?' active':'')+'" onclick="setPart(\'all\')">All</button>'
    +parts.map(p=>`<button class="pill part" style="${act(state.partition===p.partition)}" onclick="setPart('${p.partition}')">P-${p.partition}</button>`).join('');
  return `${pageHeader('Data Browser','Inspect, filter, and tail topic messages.')}
    ${readOnly?`<p class="muted" style="font-size:11px;margin:0 0 8px">${readOnlyNote}</p>`:''}
    <div class="toolbar">
      <select id="topicSel" onchange="state.filters=[];pickTopicNav(this.value)" style="width:180px"><option value="" ${state.topic?'':'selected'}>Select topic</option>${opts}</select>
      <div class="group">${pills}</div>
      <select id="decoder" onchange="state.decoder=this.value;renderMsgRows()" style="width:140px">
        <option value="auto" ${state.decoder==='auto'?'selected':''}>Auto-Detect</option>
        <option value="json" ${state.decoder==='json'?'selected':''}>JSON Pretty</option>
        <option value="text" ${state.decoder==='text'?'selected':''}>Plaintext</option>
        <option value="hex" ${state.decoder==='hex'?'selected':''}>Hex Dump</option>
      </select>
      <span style="flex:1"></span>
      <div class="group"><button class="pill ${state.tail?'active':''}" id="liveBtn" onclick="toggleTail()">${state.tail?'<span class="live-dot"></span> Pause':'Live Tail'}</button></div>
      <div class="group"><button class="pill" onclick="seek('begin')">Beginning</button><button class="pill" onclick="seek('latest')">Latest</button><input id="offInput" class="mono" placeholder="offset" style="width:76px" onkeydown="if(event.key==='Enter')seekOffsetGo()"><button class="pill" onclick="seekOffsetGo()">Go</button></div>
      <span class="updatedAgo" id="posInfo"></span>
    </div>
    <div class="toolbar" style="flex-wrap:wrap;gap:6px">
      <input id="searchBox" placeholder="Search key/value…" value="${esc(state.search)}" style="max-width:160px" oninput="state.search=this.value" onkeydown="if(event.key==='Enter')loadMsgs()">
      <input id="fromTs" placeholder="From (ms)" value="${esc(state.fromTs)}" style="max-width:90px" oninput="state.fromTs=this.value" onkeydown="if(event.key==='Enter')loadMsgs()">
      <input id="toTs" placeholder="To (ms)" value="${esc(state.toTs)}" style="max-width:90px" oninput="state.toTs=this.value" onkeydown="if(event.key==='Enter')loadMsgs()">
      <button class="btn" onclick="loadMsgs()">Apply</button>
      <span style="flex:1;min-width:4px"></span>
      <div class="group">
        <button class="pill ${state.filterPanel?'active':''}" onclick="toggleFilterPanel()">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px"><path d="M3 6h18M7 12h10M11 18h2"/></svg>
          Filter${state.filters.length?' ('+state.filters.length+')':''}
        </button>
        <button class="pill ${state.projCols.length?'active':''}" onclick="openProj()">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px"><path d="M3 6h18M3 12h18M3 18h18"/><rect x="9" y="4" width="6" height="4" rx="1" fill="currentColor" stroke="none" opacity=".3"/></svg>
          Columns${state.projCols.length?' ('+state.projCols.length+')':''}
        </button>
        <button class="pill ${state.diffSel.length>=2?'active':''}" onclick="runDiff()" title="Diff two selected messages">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px"><path d="M5 8h6M5 12h4M5 16h5M13 8l2 2-2 2M17 12l2-2-2-2M13 16l4 0"/></svg>
          Diff
        </button>
      </div>
      <div class="group"><button class="pill" onclick="prevPage()" title="Previous page">‹ Prev</button><button class="pill" onclick="nextPage()" title="Next page">Next ›</button><span class="muted" id="pageInfo" style="padding:0 6px;font-size:11px"></span></div>
      <!-- Export dropdown -->
      <div class="tbDrop">
        <button class="tbDropBtn" tabindex="0">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v13M7 11l5 5 5-5M5 20h14"/></svg>
          Export <span class="caret">▾</span>
        </button>
        <div class="tbDropMenu right">
          <button class="tbDropItem" onclick="exportMsgs('jsonl')">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v13M7 11l5 5 5-5M5 20h14"/></svg>
            Download .jsonl <small>JSON Lines</small>
          </button>
          <button class="tbDropItem" onclick="exportMsgs('csv')">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v13M7 11l5 5 5-5M5 20h14"/></svg>
            Download .csv <small>Spreadsheet</small>
          </button>
          <div class="tbDropSep"></div>
          ${readOnly
            ? `<button class="tbDropItem" style="opacity:.45" onclick="toast('${readOnlyNote}')">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 16v-13M7 8l5-5 5 5M5 20h14"/></svg>
                Import .jsonl <small>Read-only</small>
              </button>`
            : `<label class="tbDropItem" title="Import .jsonl messages into this topic">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 16v-13M7 8l5-5 5 5M5 20h14"/></svg>
                Import .jsonl <small>Upload file</small>
                <input type="file" accept=".jsonl,.ndjson" style="display:none" onchange="doImportFile(this)">
              </label>`
          }
        </div>
      </div>
      <!-- Snippets dropdown -->
      <div class="tbDrop">
        <button class="tbDropBtn" tabindex="0">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
          Snippets <span class="caret">▾</span>
        </button>
        <div class="tbDropMenu right">
          <button class="tbDropItem" onclick="copyCurl(state.topic)">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
            Copy curl command <small>REST API</small>
          </button>
          <button class="tbDropItem" onclick="copyConsumer(state.topic)">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
            Copy consumer.go <small>Go snippet</small>
          </button>
        </div>
      </div>
    </div>
    <div id="tailBar"></div>
    ${state.filterPanel?filterPanelHtml():''}
    ${state.projCols.length?projBarHtml():''}
    <div class="bwrap">
      <div class="bmain">
        <div class="card tableScroll" style="padding:0;overflow-y:hidden;margin-bottom:0">
          <table><thead><tr><th class="chkCell"></th><th>Timestamp</th><th>Partition</th><th>Offset</th><th>Key</th><th>Headers</th>${state.projCols.map(c=>`<th class="mono">${esc(c)}</th>`).join('')}<th>Value</th><th></th></tr></thead>
          <tbody id="rows"><tr><td colspan="${8+state.projCols.length}" style="padding:20px">${state.topic?'<span class="skel" style="width:120px"></span> <span class="skel" style="width:80px"></span> <span class="skel" style="width:160px"></span>':'<span class="muted">Select a topic</span>'}</td></tr></tbody></table>
        </div>
        <div id="diffPanel"></div>
      </div>
      <aside class="sideIns hidden" id="inspector">
        <h3 style="display:flex;align-items:center;gap:10px;margin-bottom:8px">Message Inspector<span style="flex:1"></span><button class="cbtn" id="inspCpAll" onclick="copyInspJson(this)">copy json</button><button class="cbtn" id="inspCpVal" onclick="copyInspValue(this)">copy value</button></h3>
        <div id="inspMeta" class="muted mono" style="font-size:11px;padding:4px 0 8px;margin-bottom:8px;border-bottom:1px solid var(--border-soft);display:flex;gap:6px;flex-wrap:wrap;align-items:center"></div>
        <pre id="insp" style="max-height:calc(100vh - 300px)"></pre>
        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
          ${readOnly?`<button class="btn ghost" aria-disabled="true" title="${readOnlyNote}" onclick="toast('${readOnlyNote}')">Replay / DLQ</button>`:`<button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="replaySelected()">Replay / DLQ</button>`}
          <button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="closeInspector()">Close</button>
        </div>
      </aside>
    </div>`;
}
window.setPart=p=>{state.partition=p;state.off=null;loadMsgs();loadPosMeta();};
async function loadMsgs(){ if(!state.topic)return; try{
  const qs=new URLSearchParams(); qs.set('limit','100');
  if(state.partition!=='all')qs.set('partition',state.partition);
  if(state.search)qs.set('search',state.search);
  if(state.fromTs)qs.set('from_ts',state.fromTs);
  if(state.toTs)qs.set('to_ts',state.toTs);
  if(state.off)qs.set('offset',state.off);
  const res=await api(`/api/v1/topics/${encodeURIComponent(state.topic)}/messages?${qs.toString()}`);
  /* Bentuk respons divalidasi: `records` bisa null (hasil kosong), dan tanpa
     penjagaan ini yang tersimpan adalah objek respons - bukan daftar - sehingga
     pemakai berikutnya (handler live tail) meledak saat menyebarnya. */
  state.msgs=Array.isArray(res)?res:(Array.isArray(res&&res.records)?res.records:[]);
  syncHash();
  refreshFieldHints();
  const pi=$('#pageInfo'); if(pi)pi.textContent=res.count!=null?`${res.count} shown`:'';
  updatePosInfo();
  const oi=$('#offInput'); if(oi&&document.activeElement!==oi)oi.value=(state.off!=null&&state.off!=='')?state.off:'';
  renderMsgRows();
}catch(e){toast(e.message);} }
window.loadMsgs=loadMsgs;
function prevPage(){ if(!state.msgs.length)return; const first=Math.min(...state.msgs.map(m=>m.offset)); state.off=Math.max(0,first-100); loadMsgs(); }
window.prevPage=prevPage;
function nextPage(){ if(!state.msgs.length)return; const last=Math.max(...state.msgs.map(m=>m.offset)); state.off=last+1; loadMsgs(); }
window.nextPage=nextPage;
function filteredMsgs(){ return state.msgs.filter(passesFilters); }
function passesFilters(m){
  if(state.search){ const hay=(String(m.key||'')+'\u0000'+String(m.value||'')).toLowerCase(); if(!hay.includes(String(state.search).toLowerCase()))return false; }
  const fromTs=Number(state.fromTs||0), toTs=Number(state.toTs||0);
  if(fromTs>0&&Number(m.timestamp)<fromTs)return false;
  if(toTs>0&&Number(m.timestamp)>toTs)return false;
  if(!state.filters.length)return true;
  const v=jsonOf(m); return state.filters.every(f=>evalFilter(f,v));
}
function rowHtml(m){const val=fmtValue(m);const sel=state.diffSel.includes(m.offset);return `<td class="chkCell"><input type="checkbox" ${sel?'checked':''} onclick="event.stopPropagation();toggleDiffSel(${m.offset})"></td><td>${timeCell(m.timestamp)}</td><td><span class="chip">P${m.partition}</span></td><td>${m.offset}</td><td>${m.key?esc(m.key):'<span class="muted">(null)</span>'}</td><td><div class="hdr">${Object.entries(m.headers||{}).map(([k,v])=>`<span class="chip">${esc(k)}=${esc(v)}</span>`).join('')||'<span class="muted">-</span>'}</div></td>${state.projCols.map(c=>`<td style="max-width:180px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(getPath(jsonOf(m),c))}</td>`).join('')}<td style="max-width:300px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(val)}</td><td style="white-space:nowrap">${replayCell(m)}</td>`;}
function replayCell(m){
  if(!isKafkaTarget())return `<button class="btn ghost" style="padding:2px 8px;font-size:11px" onclick="event.stopPropagation();replayOne(${m.offset})">Replay</button>`;
  const note='Kafka targets are read-only; import and replay are disabled.';
  return `<button class="btn ghost" aria-disabled="true" style="padding:2px 8px;font-size:11px" title="${note}" onclick="toast('${note}')">Replay</button>`;
}
function rowTr(m){ return '<tr class="rowclick" onclick="inspOffset('+m.partition+','+m.offset+')">'+rowHtml(m)+'</tr>'; }
window.inspOffset=function(p,off){const m=state.msgs.find(x=>x.offset===off&&x.partition===p);if(m)insp(m);};
/* Di atas VIRT_MIN baris hanya potongan yang terlihat (plus cadangan) yang
   dibangun, dengan spacer di kedua ujung supaya panjang gulir tetap sepadan. */
const VIRT_MIN=300, VIRT_OVERSCAN=12;
/* Tinggi baris dipatok per kerapatan, bukan diukur: satu baris contoh tidak
   mewakili baris lain dan galatnya menumpuk di spacer. Sel Timestamp selalu dua
   baris, jadi tinggi = 2 x line-height + 1 px garis pemisah. */
const VIRT_ROW_H={comfortable:35,compact:27}, VIRT_LH={comfortable:17,compact:13};
let msgVirtStart=-1, msgVirtEnd=-1, msgVirtKey='';
function virtRowH(){ return VIRT_ROW_H[state.density]||VIRT_ROW_H.comfortable; }
function msgCols(){ return 8+state.projCols.length; }
/* Urutan tampilan ditegaskan di sini: jalur paging menyimpan terlama-dulu
   sementara jalur tail menyisipkan terbaru di depan, jadi satu render ulang
   akan membalik urutan yang terlihat tanpa ini. */
function displayOrder(filtered){ return filtered.slice().sort((a,b)=>(b.offset-a.offset)||(b.timestamp-a.timestamp)); }
function virtSpacer(n,h){ return n>0?'<tr class="virtSpacer"><td colspan="'+msgCols()+'" style="height:'+(n*h)+'px"></td></tr>':''; }
function renderMsgRows(){
  const rows=$('#rows'); if(!rows)return;
  const all=filteredMsgs();
  if(!all.length){ rows.classList.remove('virt'); msgVirtStart=msgVirtEnd=-1; msgVirtKey='';
    rows.innerHTML='<tr><td colspan="'+msgCols()+'" class="muted" style="padding:20px">'+(state.filters.length?'No messages match filters.':'No messages.')+'</td></tr>'; return; }
  const list=displayOrder(all);               /* terbaru di atas, apa pun urutan penyimpanannya */
  const mainEl=$('main');
  if(!mainEl||list.length<VIRT_MIN){         /* daftar kecil: render penuh, tanpa perubahan perilaku */
    rows.classList.remove('virt'); msgVirtStart=msgVirtEnd=-1; msgVirtKey='';
    rows.innerHTML=list.map(rowTr).join('');
    return;
  }
  rows.classList.add('virt');
  const h=virtRowH();
  rows.style.setProperty('--virtRowH',h+'px');   /* tinggi baris nyata, sama dengan yang dipakai spacer */
  rows.style.setProperty('--virtLh',(VIRT_LH[state.density]||VIRT_LH.comfortable)+'px');
  const bodyTop=rows.getBoundingClientRect().top-mainEl.getBoundingClientRect().top+mainEl.scrollTop;
  const view=mainEl.clientHeight||600;
  let start=Math.floor((mainEl.scrollTop-bodyTop)/h)-VIRT_OVERSCAN;
  if(start<0)start=0;
  let end=start+Math.ceil(view/h)+2*VIRT_OVERSCAN;
  if(end>list.length)end=list.length;
  const key=list.length+'|'+(list[0]?list[0].offset:'')+'|'+state.filters.length+'|'+state.projCols.length+'|'+state.diffSel.length;
  if(start===msgVirtStart&&end===msgVirtEnd&&key===msgVirtKey)return;   /* potongan dan data sama: jangan bangun ulang */
  msgVirtStart=start; msgVirtEnd=end; msgVirtKey=key;
  rows.innerHTML=virtSpacer(start,h)+list.slice(start,end).map(rowTr).join('')+virtSpacer(list.length-end,h);
}
window.renderMsgRows=renderMsgRows;
/* Pemantauan berkala, bukan pendengar event scroll. Halaman tersembunyi (tab
   latar) tidak mengirim event scroll sama sekali di beberapa browser - sudah
   diukur: scrollTop berubah tetapi nol event - sehingga jendela akan tertinggal
   basi. Penjaga di renderMsgRows() membuat pemeriksaan ini murah: kalau potongan
   dan datanya sama, tidak ada DOM yang dibangun ulang. */
let msgVirtTimer=null;
function startMsgVirtWatch(){
  if(msgVirtTimer)return;
  msgVirtTimer=setInterval(()=>{ const rows=$('#rows'); if(rows&&rows.classList.contains('virt'))renderMsgRows(); },120);
}
function stopMsgVirtWatch(){ if(msgVirtTimer){clearInterval(msgVirtTimer);msgVirtTimer=null;} }
let tnId=0;
function prim(v){if(v===null)return '<span class="jnull">null</span>';if(typeof v==='boolean')return '<span class="jbool">'+v+'</span>';if(typeof v==='number')return '<span class="jnum">'+v+'</span>';return '<span class="jstr">"'+esc(v)+'"</span>';}
function jnode(key,val,force){tnId++;const id=tnId;
  if(val===null||typeof val!=='object'){return '<div class="jrow" style="cursor:default;padding-left:0"><span class="jkey">"'+esc(key)+'"</span>: '+prim(val)+'</div>';}
  const open=force||(val&&Object.keys(val).length<=8);
  const body=Object.entries(val||{}).map(function(e){return jnode(e[0],e[1]);}).join('');
  const label=Array.isArray(val)?('['+val.length+']'):('{'+Object.keys(val).length+'}');
  return '<div><div class="jrow" onclick="toggleNode('+id+')"><span class="jarrow" id="ta-'+id+'">'+(open?'v':'>')+'</span><span class="jkey">"'+esc(key)+'"</span>: <span class="jnum">'+label+'</span></div><div class="jnode" id="tn-'+id+'" style="'+(open?'':'display:none')+'">'+(body||'<div class="muted" style="padding-left:14px">(empty)</div>')+'</div></div>';}
window.toggleNode=function(id){const b=document.getElementById('tn-'+id),a=document.getElementById('ta-'+id);if(b){const o=b.style.display!=='none';b.style.display=o?'none':'block';a.innerHTML=o?'>':'v';}};
let _inspCur=null;
window.insp=function(m){_inspCur=m;$('#inspector').classList.remove('hidden');let val=m.value;if(state.decoder==='hex')val=hex(m.valueB64);else if(m.isJSON){try{val=JSON.parse(m.value||'null');}catch(e){val=m.value;}}const metaEl=document.getElementById('inspMeta');if(metaEl){const hdrs=Object.entries(m.headers||{}).map(([k,v])=>`<span class="chip">${esc(k)}=${esc(v)}</span>`).join('');metaEl.innerHTML=`<span class="chip">P${m.partition}</span> <span class="chip">offset ${m.offset}</span> <span class="chip">${fmtBytes(m.valueSize||0)}</span> ${m.key?`<span class="chip">key: ${esc(m.key)}</span>`:''}${hdrs} <span style="margin-left:auto;font-size:10px" class="muted">${new Date(m.timestamp).toISOString()}</span>`;}let body='';if(val!==null&&typeof val==='object'){if(Array.isArray(val)){body=val.map((item,idx)=>jnode(idx,item,true)).join('');}else{for(const k in val)body+=jnode(k,val[k],true);}}else{body=`<div style="padding:4px 0">${esc(val)}</div>`;}$('#insp').innerHTML='<div class="jnode" style="border:none;margin:0;padding-left:0">'+(body||'<span class="muted">(empty)</span>')+'</div>';};
window.closeInspector=function(){_inspCur=null;const el=document.getElementById('inspector');if(el)el.classList.add('hidden');};
function inspJsonText(){
  const m=_inspCur; if(!m)return '';
  const obj={topic:m.topic,partition:m.partition,offset:m.offset,timestamp:new Date(m.timestamp).toISOString(),key:m.key,headers:m.headers||{},value:m.value??''};
  return JSON.stringify(obj,null,2);
}
function inspValueText(){
  const m=_inspCur; if(!m)return '';
  if(m.isJSON&&m.value){ try{ return JSON.stringify(JSON.parse(m.value),null,2); }catch(e){ return m.value; } }
  return m.value??'';
}
window.copyInspJson=function(btn){ const t=inspJsonText(); if(!t){toast('No message selected');return;} copyInspTo(t,btn,'copy json'); };
window.copyInspValue=function(btn){ const t=inspValueText(); if(!t){toast('No message selected');return;} copyInspTo(t,btn,'copy value'); };
function copyInspTo(t,btn,label){
  const done=()=>{ if(!btn)return; const old=btn.textContent; btn.textContent='copied'; btn.classList.add('ok');
    setTimeout(()=>{btn.textContent=label;btn.classList.remove('ok');},1200); };
  if(navigator.clipboard&&navigator.clipboard.writeText){ navigator.clipboard.writeText(t).then(done).catch(()=>{fallbackCopy(t);done();}); }
  else{ fallbackCopy(t);done(); }
}
function fmtValue(m){ if(state.decoder==='hex')return hex(m.valueB64); return m.value; }
function hex(b64){let s='',bin=atob(b64||'');for(let i=0;i<bin.length;i++)s+=bin.charCodeAt(i).toString(16).padStart(2,'0')+' ';return s.trim();}
function copyInsp(){const p=$('#insp');navigator.clipboard.writeText(p.textContent).then(()=>{const b=document.querySelector('.inspector .btn');if(b){const old=b.textContent;b.textContent='Copied';b.style.color='var(--success)';setTimeout(()=>{b.textContent=old;b.style.color='';},1500);}else toast('Copied');});}
window.copyInsp=copyInsp;

/* Fitur 3.2: Filter Builder */
function jsonOf(m){ if(m.isJSON){ try{return JSON.parse(m.value||'null');}catch(e){return null;} } return null; }
function pathKeys(obj,prefix){ let out=[]; for(const k in obj){ const p=prefix?prefix+'.'+k:k; if(obj[k]&&typeof obj[k]==='object'){out=out.concat(pathKeys(obj[k],p));} out.push(p); } return out; }
function refreshFieldHints(){ if(!state.topic)return; const s=state.msgs.slice(0,10); const seen={}; const keys=[]; s.forEach(m=>{ const v=jsonOf(m); if(v)pathKeys(v,'').forEach(p=>{if(!seen[p]){seen[p]=1;keys.push(p);}}); }); state.fieldHint=keys.slice(0,40); }
function filterPanelHtml(){
  const presets=JSON.parse(localStorage.getItem('pk_presets')||'[]');
  const rows=state.filters.map((f,i)=>`<div class="filterRow">
    <div class="autocomplete"><input id="fld-${i}" class="fField" placeholder="field (e.g. order.status)" value="${esc(f.field)}" oninput="acField(${i})" onfocus="acField(${i})" onchange="state.filters[${i}].field=this.value;applyFiltersNow()"><div id="acl-${i}" class="acList" style="display:none"></div></div>
    <select class="fOp" onchange="state.filters[${i}].op=this.value;applyFiltersNow()">
      ${['==','!=','>','<','>=','<=','contains','regex','exists'].map(o=>`<option value="${o}" ${f.op===o?'selected':''}>${o}</option>`).join('')}
    </select>
    <input class="fVal" placeholder="value" value="${esc(f.value||'')}" oninput="state.filters[${i}].value=this.value" onkeydown="if(event.key==='Enter')applyFiltersNow()">
    <button class="frmBtn" title="Remove" onclick="delFilter(${i})">&times;</button>
  </div>`).join('');
  return `<div class="filterPanel">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap">
      <b style="font-size:13px">Filter Builder</b>
      <button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="addFilter()">+ Add rule</button>
      <button class="btn" style="padding:4px 12px;font-size:11px" onclick="applyFiltersNow()">Apply filters</button>
      <button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="clearAllFilters()">Clear all</button>
      <button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="savePreset()">Save preset</button>
      <span class="sp" style="flex:1"></span>
      ${state.filters.length?`<span class="muted" style="font-size:11px">${filteredMsgs().length}/${state.msgs.length} messages</span>`:''}
      <button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="toggleFilterPanel()">Close</button>
    </div>
    ${rows||'<div class="muted" style="font-size:12px;margin-bottom:8px">No rules yet. Add a rule to filter messages by JSON field.</div>'}
    <div style="margin-top:8px">${presets.map(p=>`<span class="presetChip" onclick="applyPreset('${esc(JSON.stringify(p.filters))}')">${esc(p.name)}<button title="delete" onclick="event.stopPropagation();delPreset('${esc(p.name)}')">&times;</button></span>`).join('')||''}</div>
  </div>`; }
window.applyFiltersNow=function(){renderMsgRows();const fp=state.filterPanel?filterPanelHtml():'';const el=document.querySelector('.filterPanel');if(el&&fp){el.outerHTML=fp;}};
window.clearAllFilters=function(){state.filters=[];render();};
window.toggleFilterPanel=function(){state.filterPanel=!state.filterPanel;if(state.filterPanel)refreshFieldHints();render();};
window.addFilter=function(){state.filters.push({field:'',op:'==',value:''});render();};
window.delFilter=function(i){state.filters.splice(i,1);renderMsgRows();render();};
window.acField=function(i){ const inp=document.getElementById('fld-'+i); const box=document.getElementById('acl-'+i); if(!inp||!box)return; state.filters[i].field=inp.value; const q=inp.value.toLowerCase(); const items=state.fieldHint.filter(k=>k.toLowerCase().includes(q)); box.style.display=items.length?'block':'none'; box.innerHTML=items.map(k=>`<div onclick="pickField(${i},'${esc(k)}')">${esc(k)}</div>`).join(''); };
window.pickField=function(i,k){state.filters[i].field=k;const inp=document.getElementById('fld-'+i);inp.value=k;const box=document.getElementById('acl-'+i);if(box)box.style.display='none';applyFiltersNow();};
window.savePreset=function(){const nm=prompt('Preset name:');if(!nm)return;const presets=JSON.parse(localStorage.getItem('pk_presets')||'[]');presets.push({name:nm,filters:JSON.parse(JSON.stringify(state.filters))});localStorage.setItem('pk_presets',JSON.stringify(presets));render();};
window.applyPreset=function(s){state.filters=JSON.parse(s);renderMsgRows();render();};
window.delPreset=function(nm){let p=JSON.parse(localStorage.getItem('pk_presets')||'[]');p=p.filter(x=>x.name!==nm);localStorage.setItem('pk_presets',JSON.stringify(p));render();};
function getPath(o,path){ if(!o||!path)return''; return path.split('.').reduce((acc,k)=>(acc==null||typeof acc!=='object')?undefined:acc[k],o); }
function evalFilter(f,v){
  if(f.op==='exists') return f.value? getPath(v,f.field)!==undefined : getPath(v,f.field)===undefined;
  if(f.op==='regex'){ try{return new RegExp(f.value).test(String(getPath(v,f.field)??''));}catch(e){return false;} }
  const val=getPath(v,f.field);
  const a=val===undefined?undefined:String(val);
  const b=String(f.value);
  if(f.op==='contains') return a!=null&&a.includes(b);
  if(f.op==='==') return a!=null&&a===b;
  if(f.op==='!=') return a!=null&&a!==b;
  const na=Number(val), nb=Number(f.value);
  if(isNaN(na)||isNaN(nb))return false;
  if(f.op==='>') return na>nb;
  if(f.op==='<') return na<nb;
  if(f.op==='>=') return na>=nb;
  if(f.op==='<=') return na<=nb;
  return false;
}

/* Fitur 3.3: Column Projection */
function projBarHtml(){ return `<div class="projBar"><span class="muted" style="font-size:11px">Projected columns:</span>${state.projCols.map(c=>`<span class="projTag">${esc(c)}<button title="remove" onclick="removeProj('${esc(c)}')">&times;</button></span>`).join('')}<button class="btn ghost" style="padding:2px 9px;font-size:11px" onclick="openProj()">+ Add</button></div>`; }
window.openProj=function(){
  const opts=state.fieldHint.map(f=>`<option value="${esc(f)}">${esc(f)}</option>`).join('');
  if(!opts){toast('Load messages first to see available fields');return;}
  const current=state.projCols.length?`<div style="margin-bottom:12px"><div style="font-size:11px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.04em">Current columns</div><div style="display:flex;flex-wrap:wrap;gap:5px">${state.projCols.map(c=>`<span class="projTag">${esc(c)}<button title="remove" onclick="removeProj('${esc(c)}');document.querySelector('.overlay .modal').querySelector('#projPick').dispatchEvent(new Event('change'))">&#215;</button></span>`).join('')}</div></div>`:'';
  const $sel=document.createElement('div');
  $sel.innerHTML=`<div class="overlay"><div class="modal" style="min-width:320px;max-width:440px">
    <h3 style="margin-bottom:4px">Column Projection</h3>
    <p style="font-size:12px;color:var(--muted);margin-bottom:14px">Extract JSON fields as separate table columns for quicker scanning.</p>
    ${current}
    <label style="font-size:12px;color:var(--muted);display:block;margin-bottom:6px">Add field</label>
    <select id="projPick" style="width:100%">${opts}</select>
    <div style="margin-top:14px;display:flex;gap:8px;justify-content:flex-end">
      <button class="btn ghost" onclick="closeOverlay()">Close</button>
      <button class="btn" onclick="projPickAdd()">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="vertical-align:-2px"><path d="M12 5v14M5 12h14"/></svg>
        Add column
      </button>
    </div>
  </div></div>`;
  document.body.appendChild($sel);
  refreshFieldHints();
};
window.projPickAdd=function(){ const v=document.getElementById('projPick').value; if(v&&!state.projCols.includes(v)){state.projCols.push(v);localStorage.setItem('pk_proj',JSON.stringify(state.projCols));} closeOverlay(); render(); };
window.removeProj=function(c){state.projCols=state.projCols.filter(x=>x!==c);localStorage.setItem('pk_proj',JSON.stringify(state.projCols));render();};

/* Fitur 3.4: Message Diff */
window.toggleDiffSel=function(off){ if(state.diffSel.includes(off)){state.diffSel=state.diffSel.filter(x=>x!==off);} else { if(state.diffSel.length>=2)state.diffSel.shift(); state.diffSel.push(off);} renderMsgRows(); };
function byOff(off){return state.msgs.find(m=>m.offset===off);}
window.runDiff=function(){ const d=$('#diffPanel'); if(!d)return; if(state.diffSel.length<2){toast('Select 2 messages (checkboxes) to diff');return;} const a=byOff(state.diffSel[0]), b=byOff(state.diffSel[1]); if(!a||!b)return; const va=fmtValue(a).split('\n'), vb=fmtValue(b).split('\n'); const max=Math.max(va.length,vb.length); let ha='',hb=''; for(let i=0;i<max;i++){ const la=va[i]||'', lb=vb[i]||''; const same=la.trim()===lb.trim(); const da=la===lb?'' : (lb===''?'d-rem':(va.includes(lb)?'d-ctx':'d-rem')); ha+='<div class="'+(same?'d-ctx':'d-rem')+'">'+esc(la||' ')+'</div>'; hb+='<div class="'+(same?'d-ctx':'d-add')+'">'+esc(lb||' ')+'</div>'; } d.innerHTML=`<div class="diffPanel"><h3 style="font-size:15px;margin-bottom:4px">Message Diff <span class="muted" style="font-size:11px">offset ${a.offset} vs ${b.offset}</span></h3><div class="diffWrap"><div class="diffCol"><h4>Old / A (offset ${a.offset})</h4><pre>${ha}</pre></div><div class="diffCol"><h4>New / B (offset ${b.offset})</h4><pre>${hb}</pre></div></div></div>`; d.scrollIntoView({behavior:'smooth',block:'nearest'}); };

/* Fitur 3.5: Export & Replay / DLQ */
window.exportMsgs=function(fmt){ const all=filteredMsgs(); if(!all.length){toast('Nothing to export');return;} const name=`${state.topic||'msgs'}.${fmt==='csv'?'csv':'jsonl'}`; let content=''; if(fmt==='jsonl'){ content=all.map(m=>JSON.stringify({topic:m.topic,partition:m.partition,offset:m.offset,timestamp:m.timestamp,key:m.key,headers:m.headers,value:m.value})).join('\n'); } else { const cols=['topic','partition','offset','timestamp','key','value']; const escCsv=s=>{s=String(s??'');return /[",\n]/.test(s)?'"'+s.replace(/"/g,'""')+'"':s;}; content=cols.join(',')+'\n'+all.map(m=>cols.map(c=>escCsv(c==='value'?m.value:m[c])).join(',')).join('\n'); } const blob=new Blob([content],{type:'text/plain;charset=utf-8'}); const url=URL.createObjectURL(blob); const a=document.createElement('a'); a.href=url; a.download=name; a.click(); setTimeout(()=>URL.revokeObjectURL(url),100); };
function replayModal(m){ return `<div class="overlay"><div class="modal"><h3>Replay Message <span class="muted" style="font-size:11px">offset ${m.offset}</span></h3>
  <label>Target topic (DLQ)</label><input id="rpTopic" list="tl" value="${esc(state.topic+'-dlq')}"><datalist id="tl">${state.topics.map(t=>`<option>${esc(t.name)}</option>`).join('')}</datalist>
  <label>Key</label><input id="rpKey" value="${esc(m.key||'')}">
  <label>Value</label><textarea id="rpValue" rows="6">${esc(fmtValue(m))}</textarea>
  <div style="margin-top:14px;display:flex;gap:8px"><button class="btn" onclick="doReplay(${m.offset})">Send</button><button class="btn ghost" onclick="closeOverlay()">Cancel</button></div></div></div>`; }
window.replayOne=function(off){ const m=byOff(off); if(!m)return; document.body.insertAdjacentHTML('beforeend',replayModal(m)); };
window.replaySelected=function(){ const a=state.diffSel; const m=byOff(a[0])||byOff(a[1]); if(!m){toast('Select a message first');return;} document.body.insertAdjacentHTML('beforeend',replayModal(m)); };
window.closeOverlay=function(){ document.querySelectorAll('.overlay').forEach(o=>o.remove()); };
/* Fitur: dialog konfirmasi dalam aplikasi, pengganti confirm() bawaan browser.
   Dipakai lewat Promise supaya pemanggil bisa `await` seperti confirm() biasa,
   tapi tetap konsisten dengan pola overlay/modal yang sudah ada (replayModal). */
window.askConfirm=function(opts){
  opts=opts||{};
  const title=opts.title||'',body=opts.body||'',confirmLabel=opts.confirmLabel||'Confirm',danger=!!opts.danger,typeToConfirm=opts.typeToConfirm||null;
  return new Promise(resolve=>{
    let done=false;
    const onKey=e=>{ if(e.key==='Escape')finish(false); };
    const finish=v=>{ if(done)return; done=true; document.removeEventListener('keydown',onKey); closeOverlay(); resolve(v); };
    const typeHtml=typeToConfirm?`<label style="margin-top:10px" for="confirmType">Type <strong>${esc(typeToConfirm)}</strong> to confirm</label><input id="confirmType" autocomplete="off" aria-required="true">`:'';
    const html=`<div class="overlay"><div class="modal" role="dialog" aria-modal="true" aria-labelledby="confirmTitle"><h3 id="confirmTitle">${esc(title)}</h3><div class="muted" style="margin:8px 0">${esc(body)}</div>${typeHtml}<div style="margin-top:14px;display:flex;gap:8px"><button class="btn ${danger?'danger':''}" id="confirmOk"${typeToConfirm?' disabled aria-disabled="true"':''}>${esc(confirmLabel)}</button><button class="btn ghost" id="confirmCancel">Cancel</button></div></div></div>`;
    document.body.insertAdjacentHTML('beforeend',html);
    const overlay=document.querySelector('.overlay'),okBtn=document.getElementById('confirmOk'),cancelBtn=document.getElementById('confirmCancel'),input=document.getElementById('confirmType');
    if(typeToConfirm){
      const updateOk=()=>{ const match=input.value===typeToConfirm; okBtn.disabled=!match; if(match)okBtn.removeAttribute('aria-disabled'); else okBtn.setAttribute('aria-disabled','true'); };
      input.addEventListener('input',updateOk);
      input.addEventListener('keydown',e=>{ if(e.key==='Enter'&&input.value===typeToConfirm)finish(true); });
      input.focus();
    } else { cancelBtn.focus(); }
    okBtn.addEventListener('click',()=>{ if(typeToConfirm&&input.value!==typeToConfirm)return; finish(true); });
    cancelBtn.addEventListener('click',()=>finish(false));
    overlay.addEventListener('click',e=>{ if(e.target===overlay)finish(false); });
    document.addEventListener('keydown',onKey);
  });
};
window.doReplay=async function(off){ const m=byOff(off); if(!m)return; const topic=document.getElementById('rpTopic').value||m.topic; const body={key:document.getElementById('rpKey').value,value:document.getElementById('rpValue').value}; try{ const r=await api('/api/v1/topics/'+encodeURIComponent(topic)+'/messages',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); toast('Replayed to '+topic+' @ offset '+r.offset); closeOverlay(); }catch(e){toast(e.message);} };

async function seek(w){ if(!state.topic)return;
  if(w==='begin'){state.off=0;}
  else if(w==='latest'){ const leo=(state.leo!=null)?state.leo:0; state.off=Math.max(state.earliest||0,leo-100); }
  await loadMsgs(); }
window.seek=seek;
window.seekOffsetGo=function(){ const inp=$('#offInput'); if(!inp)return; const v=inp.value.trim();
  if(v===''){toast('Isi offset dulu');inp.focus();return;}
  const n=parseInt(v,10);
  if(isNaN(n)||n<0){toast('Offset harus angka >= 0');inp.focus();return;}
  if(state.leo!=null&&n>=state.leo){toast('Offset melebihi LEO ('+state.leo+')');inp.focus();return;}
  state.off=n; loadMsgs(); };
function loadPosMeta(){
  api(`/api/v1/topics/${encodeURIComponent(state.topic)}/partitions`).then(ps=>{
    const pno=state.partition==='all'?0:parseInt(state.partition,10)||0;
    const p=(ps||[]).find(x=>x.partition===pno)||ps[0];
    state.leo=p?p.logEndOffset:null; state.earliest=p?p.earliestOffset:null;
    updatePosInfo();
  }).catch(()=>{});
}
function updatePosInfo(){
  const el=document.getElementById('posInfo'); if(!el)return;
  const pno=state.partition==='all'?0:parseInt(state.partition,10)||0;
  let txt='P'+pno;
  if(state.leo!=null)txt+=' \u00b7 LEO '+state.leo+' \u00b7 earliest '+(state.earliest??0);
  const first=state.msgs.length?Math.min(...state.msgs.map(m=>m.offset)):null;
  const last=state.msgs.length?Math.max(...state.msgs.map(m=>m.offset)):null;
  if(first!=null)txt+=' \u00b7 window '+first+'-'+last;
  el.textContent=txt;
}
function closeTailSocket(){
  if(state.ws){state.ws.onclose=null;state.ws.onmessage=null;state.ws.onerror=null;state.ws.onopen=null;state.ws.close();state.ws=null;}
  if(state.wsRetry){clearTimeout(state.wsRetry);state.wsRetry=null;}
}
function toggleTail(){
  if(!state.topic){state.tail=false;const button=$('#liveBtn');if(button)button.textContent='Live Tail';return;}
  if(isTargetUnavailable()){
    state.tail=false;const button=$('#liveBtn');if(button)button.textContent='Live Tail';
    toast(targetUnavailableMessage());return;
  }
  if(isKafkaTarget()&&!hasTargetCapability('topic.tail')){
    state.tail=false;const button=$('#liveBtn');if(button)button.textContent='Live Tail';
    toast('Live tail is not supported by this Kafka target.');return;
  }
  state.tail=!state.tail;const button=$('#liveBtn');if(button)button.innerHTML=state.tail?'<span class="live-dot"></span> Pause':'Live Tail';
  closeTailSocket();if(!state.tail)return;
  state.tailSkipped=0;state.pendingTail=0;connectTail();
}
window.toggleTail=toggleTail;

/* Fitur 13: WebSocket auto-reconnect with exponential backoff */
function connectTail(){
  if(!state.tail||!state.topic)return;
  const targetName=state.activeCluster,generation=state.targetGeneration;
  if(targetName!=='local'){
    const entry=activeTargetEntry();
    if(!entry){state.tail=false;toast(targetUnavailableMessage());return;}
    if((entry.kind||'').toLowerCase()==='kafka'&&!hasTargetCapability('topic.tail')){state.tail=false;toast('Live tail is not supported by this Kafka target.');return;}
  }
  const proto=location.protocol==='https:'?'wss':'ws',part=(!state.partition||state.partition==='all')?'all':state.partition;
  const apiPath=`/api/v1/topics/${encodeURIComponent(state.topic)}/tail?partition=${encodeURIComponent(part)}`;
  const path=targetName==='local'?apiPath:`/api/v1/target/${encodeURIComponent(targetName)}${apiPath}`;
  const ws=new WebSocket(`${proto}://${location.host}${path}`),isCurrent=()=>generation===state.targetGeneration&&targetName===state.activeCluster&&state.ws===ws;
  state.ws=ws;state.retryCount=state.retryCount||0;
  ws.onopen=()=>{if(isCurrent())state.retryCount=0;};
  ws.onmessage=e=>{if(!isCurrent()||!state.tail)return;const data=JSON.parse(e.data);
    /* Bukan array = frame pemberitahuan: broker melewati pesan karena satu tick
       memuat lebih banyak daripada yang layak dikirim/ditampilkan. */
    if(!Array.isArray(data)){if(data&&data.skipped){state.tailSkipped=(state.tailSkipped||0)+data.skipped;updateTailBar();}return;}
    const recs=data;if(!recs.length)return;
    if(state.tailPaused){state.pendingTail+=recs.length;updateTailBar();return;}
    const fresh=recs.slice().reverse();state.msgs=[...fresh,...state.msgs];if(state.msgs.length>MSGS_MAX)state.msgs=state.msgs.slice(0,MSGS_MAX);
    const rows=$('#rows');
    if(rows){
      if(rows.classList.contains('virt')||state.msgs.length>=VIRT_MIN)renderMsgRows();
      else{fresh.forEach(m=>{if(passesFilters(m))prependMsgRow(m);});const want=filteredMsgs().length;while(rows.children.length>want)rows.removeChild(rows.lastElementChild);}
    }
    updateTailBar();
  };
  ws.onclose=()=>{
    if(!isCurrent())return;state.ws=null;if(!state.tail)return;
    const delay=Math.min(1000*Math.pow(1.5,state.retryCount++),10000);
    state.wsRetry=setTimeout(()=>{state.wsRetry=null;if(generation!==state.targetGeneration||targetName!==state.activeCluster||!state.tail||state.ws)return;connectTail();},delay);
  };
  ws.onerror=()=>{if(isCurrent())ws.close();};
}
function prependMsgRow(m){const rows=$('#rows');if(!rows)return;const tr=document.createElement('tr');tr.className='rowclick animate-row-entry';tr.innerHTML=rowHtml(m);tr.onclick=()=>insp(m);rows.prepend(tr);}

/* Topics */
function viewTopics(){
  const readOnly=isKafkaTarget(),readOnlyNote='Kafka targets are read-only; topic mutations are disabled.',canDetail=!isKafkaTarget()||hasTargetCapability('topic.partitions.read'),detailUnavailable='Topic partition details are not supported by this Kafka target.';
  const createControl=readOnly?`<button class="btn" aria-disabled="true" title="${readOnlyNote}" onclick="toast('${readOnlyNote}')">Create</button>`:`<input id="newT" placeholder="new topic name" style="max-width:220px"><input id="newP" type="number" min="1" max="64" value="1" title="Partitions" style="max-width:70px"><button class="btn" onclick="createTopic()">Create</button>`;
  return `<div>${pageHeader('Topics','Create and inspect topics, partitions, and log positions.')}<div class="card"><div style="display:flex;gap:16px;margin-bottom:14px;flex-wrap:wrap">
    <div><div class="kicker">Namespace</div><div class="statXL">${(state.topics||[]).length}<small>topics</small></div></div>
    <div style="margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap">${createControl}</div>
  </div>${readOnly?`<div class="card" style="padding:10px 12px;border-color:var(--warn-line);margin-bottom:10px"><span class="badge b-amber">Read-only</span><span class="muted" style="font-size:12px;margin-left:8px">${readOnlyNote}</span></div>`:''}
  <div class="tableScroll"><table><thead><tr><th class="sortable" onclick="sortRows('topics','name')">Topic<span class="sortInd">${sortInd('topics','name')}</span></th><th class="sortable" onclick="sortRows('topics','leo')">Log End<span class="sortInd">${sortInd('topics','leo')}</span></th><th class="sortable" onclick="sortRows('topics','size')">Size<span class="sortInd">${sortInd('topics','size')}</span></th><th>Partitions</th><th></th></tr></thead><tbody>
  ${sortedTopics().map(t=>{
    const ps=t.partitions||[],max=Math.max(1,...ps.map(p=>p.logEndOffset||0));
    const bars=ps.map(p=>`<div class="pbar" title="P${p.partition}: LEO ${p.logEndOffset||0}"><span style="width:${Math.max(4,((p.logEndOffset||0)/max)*100)}%"></span></div>`).join('')||'<span class="muted">-</span>',p=ps[0]||{};
    const topicLink=canDetail?`<a style="color:var(--primary-text);cursor:pointer" onclick="openTopicDetail(${jsArg(t.name)})">${esc(t.name)}</a>`:`<span title="${detailUnavailable}">${esc(t.name)}</span>`;
    const detail=canDetail?`<button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="openTopicDetail(${jsArg(t.name)})">Detail</button>`:`<button class="btn ghost" aria-disabled="true" title="${detailUnavailable}" onclick="toast('${detailUnavailable}')">Detail</button>`;
    const remove=readOnly?`<button class="btn danger" aria-disabled="true" title="${readOnlyNote}" onclick="toast('${readOnlyNote}')">Delete</button>`:`<button class="btn danger" style="padding:4px 10px;font-size:11px" onclick="delTopic(${jsArg(t.name)})">Delete</button>`;
    const roFlag=readOnly?'<span class="badge muted">Read-only</span>':'';
    return `<tr><td class="mono">${topicLink}</td><td>${p.logEndOffset??0}</td><td>${fmtBytesOpt(t.totalBytes)}</td><td style="min-width:90px">${bars}</td><td><div class="clRowActions">${detail}${remove}${roFlag}</div></td></tr>`;
  }).join('')||`<tr><td colspan="5" class="muted" style="padding:20px">No topics yet.${readOnly?'':' <button class="btn" style="padding:4px 12px;font-size:12px" onclick="document.getElementById(\'newT\').focus()">Create the first one</button>'}</td></tr>`}
  </tbody></table></div></div></div>`;
}
window.openTopic=async n=>{state.topic=n;state.partition='all';await goView('databrowser');};
window.openTopicDetail=async n=>{
  if(isTargetUnavailable()){toast(targetUnavailableMessage());return;}
  if(isKafkaTarget()&&!hasTargetCapability('topic.partitions.read')){toast('Topic partition details are not supported by this Kafka target.');return;}
  state.view='topics';state.topicDetail=n;state.topicParts=[];state.topicCfg=null;render();loadTopicDetail();
};
async function loadTopicDetail(){
  const generation=state.targetGeneration,name=state.topicDetail;if(!name)return;
  try{const parts=await api(`/api/v1/topics/${encodeURIComponent(name)}/partitions`);if(generation!==state.targetGeneration||name!==state.topicDetail)return;state.topicParts=parts;render();}
  catch(e){if(generation!==state.targetGeneration||e.staleTargetResponse)return;toast(e.message);}
}
window.loadTopicDetail=loadTopicDetail;
function viewTopicDetail(){
  const t=state.topicDetail;if(!t)return '';
  const readOnly=isKafkaTarget(),reason='Kafka targets are read-only; topic mutations are disabled.',ps=state.topicParts||[];
  const max=Math.max(1,...ps.map(p=>Math.max(p.logEndOffset||0,p.highWatermark||0)));
  const bars=ps.map(p=>{const leo=((p.logEndOffset||0)/max)*100,hwm=((p.highWatermark||0)/max)*100;
    return `<div style="margin-bottom:14px"><div style="display:flex;justify-content:space-between;font-family:var(--font-mono);font-size:11px;margin-bottom:4px"><span>P${p.partition}</span><span class="muted">LEO ${p.logEndOffset??'n/a'} / HWM ${p.highWatermark??'n/a'} / earliest ${p.earliestOffset??'n/a'}</span></div>
      <div class="pbar" style="height:14px"><span style="width:${leo}%"></span></div><div class="pbar" style="height:6px;margin-top:2px"><span style="width:${hwm}%;background:var(--live)"></span></div>
      <div class="muted" style="font-size:10px;margin-top:4px">${fmtBytesOpt(p.bytes)} &middot; ${(p.segments||[]).length} segment(s)</div><div style="margin-top:6px;display:flex;gap:6px">${readOnly?`<button class="btn ghost" aria-disabled="true" title="${reason}" onclick="toast('${reason}')">Truncate</button>`:`<button class="btn ghost" style="padding:3px 8px;font-size:11px" onclick="truncateTopic(${p.partition})">Truncate</button>`}</div></div>`;
  }).join('');
  const policy=readOnly?'cleanup.policy unavailable':(state.topicCfg&&state.topicCfg.compacted?'cleanup.policy=compact':'cleanup.policy=delete');
  return `<div>${pageHeader(esc(t),'Partition offsets, watermarks, and compaction controls.')}<div class="card"><div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;flex-wrap:wrap">
    <h3 style="font-size:15px">${esc(t)}</h3><span class="badge ${state.topicCfg&&state.topicCfg.compacted?'b-purple':'b-amber'}">${policy}</span>
    ${readOnly?`<button class="btn ghost" aria-disabled="true" title="${reason}" onclick="toast('${reason}')">Enable/disable compaction</button><button class="btn ghost" aria-disabled="true" title="${reason}" onclick="toast('${reason}')">Compact now</button>`:`<button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="toggleTopicCompact()">${state.topicCfg&&state.topicCfg.compacted?'Disable compaction':'Enable compaction'}</button><button class="btn ghost" style="padding:4px 10px;font-size:11px" onclick="compactTopic()">Compact now</button>`}
    <button class="btn ghost" style="padding:4px 10px;font-size:11px;margin-left:auto" onclick="state.topicDetail=null;render()">Back</button></div>
    ${readOnly?`<p class="muted" style="font-size:11px;margin-bottom:8px">${reason}</p>`:''}<div style="margin-bottom:6px" class="muted">LEO (log end) vs HWM (high watermark) per partition</div>${bars||'<div class="muted">No partitions</div>'}
  </div></div>`;
}
window.truncateTopic=async function(p){const o=prompt('Truncate partition '+p+' to offset:');if(o===null)return;try{await api(`/api/v1/topics/${encodeURIComponent(state.topicDetail)}/truncate`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({partition:p,offset:parseInt(o,10)})});toast('Truncated');loadTopicDetail();}catch(e){toast(e.message);}};
window.compactTopic=async function(){try{await api(`/api/v1/topics/${encodeURIComponent(state.topicDetail)}/compact`,{method:'POST'});toast('Compaction triggered');}catch(e){toast(e.message);}};
async function createTopic(){const n=$('#newT').value;if(!n)return;const parts=Math.max(1,Math.min(64,parseInt($('#newP')?.value||'1',10)||1));try{await api('/api/v1/topics',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:n,partitions:parts})});toast('Created '+n+' ('+parts+'p)');await refresh();render();}catch(e){toast(e.message);}}
window.createTopic=createTopic;
async function delTopic(n){if(!await askConfirm({title:'Delete topic',body:'Deleting "'+n+'" removes its data permanently. This cannot be undone.',confirmLabel:'Delete topic',danger:true,typeToConfirm:n}))return;try{await api('/api/v1/topics/'+encodeURIComponent(n),{method:'DELETE'});toast('Deleted '+n);await refresh();render();}catch(e){toast(e.message);}}
window.delTopic=delTopic;

/* Groups */
function viewGroups(){
  const readOnly=isKafkaTarget(),readOnlyNote='Kafka consumer groups are read-only; offset import/reset and deletion are disabled.',canDetail=!isKafkaTarget()||hasTargetCapability('group.detail.read'),detailUnavailable='Consumer group details are not supported by this Kafka target.';
  if(!(state.groups||[]).length)return `<div>${pageHeader('Consumer Groups','Review consumer membership, offsets, and lag.')}<div class="card" style="padding:32px;text-align:center"><div class="muted">No consumer groups reported by this broker.</div>${readOnly?`<div class="card" style="padding:10px 12px;border-color:var(--warn-line);margin-top:10px"><span class="badge b-amber">Read-only</span><span class="muted" style="font-size:12px;margin-left:8px">${readOnlyNote}</span></div>`:''}</div></div>`;
  return `<div>${pageHeader('Consumer Groups','Review consumer membership, offsets, and lag.')}<div class="card">${readOnly?`<div class="card" style="padding:10px 12px;border-color:var(--warn-line);margin-bottom:10px"><span class="badge b-amber">Read-only</span><span class="muted" style="font-size:12px;margin-left:8px">${readOnlyNote}</span></div>`:''}<div class="tableScroll"><table><thead><tr><th class="sortable" onclick="sortRows('groups','name')">Group<span class="sortInd">${sortInd('groups','name')}</span></th><th>State</th><th class="sortable" onclick="sortRows('groups','members')">Members<span class="sortInd">${sortInd('groups','members')}</span></th><th class="sortable" onclick="sortRows('groups','lag')">Lag<span class="sortInd">${sortInd('groups','lag')}</span></th><th></th></tr></thead><tbody>
  ${sortedGroups().map(g=>{
    const lag=Object.values(g.lag||{}).reduce((a,b)=>a+b,0),memberText=Array.isArray(g.members)?(g.members.join(', ')||'-'):'n/a',rowAttrs=canDetail?`class="rowclick" onclick="openGroup(${jsArg(g.name)})"`:`title="${detailUnavailable}"`;
    const detail=canDetail?'':`<button class="btn ghost" aria-disabled="true" title="${detailUnavailable}" onclick="toast('${detailUnavailable}')">Details</button>`;
    const groupExportNote='Group offset export is not supported by Kafka targets';
    const mutations=readOnly?`<button class="btn ghost" aria-disabled="true" title="${groupExportNote}" onclick="toast('${groupExportNote}')">Export</button><button class="btn ghost" aria-disabled="true" title="${readOnlyNote}" onclick="toast('${readOnlyNote}')">Import</button><button class="btn danger" aria-disabled="true" title="${readOnlyNote}" onclick="toast('${readOnlyNote}')">Delete</button>`:`<button class="btn ghost" style="padding:3px 8px;font-size:11px" onclick="event.stopPropagation();exportGroupOffsets(${jsArg(g.name)})" title="Export offset snapshot">Export</button><button class="btn ghost" style="padding:3px 8px;font-size:11px" onclick="event.stopPropagation();importGroupOffsets(${jsArg(g.name)})" title="Import offset snapshot">Import</button><button class="btn danger" style="padding:3px 8px;font-size:11px" onclick="event.stopPropagation();delGroup(${jsArg(g.name)})">Delete</button>`;
    return `<tr ${rowAttrs}><td class="mono">${esc(g.name)}</td><td><span class="badge ${online(g.state)?'b-green':'b-amber'}">${esc(g.state)}</span></td><td class="mono">${memberText}</td><td><span class="badge ${lag>0?'b-amber':'b-green'}">${lag}</span></td><td><div class="clRowActions">${detail}${mutations}</div></td></tr>`;
  }).join('')}
  </tbody></table></div></div></div>`;
}
let _groupDetailLoading = false;
window.openGroup=async g=>{
  if(isTargetUnavailable()){toast(targetUnavailableMessage());return;}
  if(isKafkaTarget()&&!hasTargetCapability('group.detail.read')){toast('Consumer group details are not supported by this Kafka target.');return;}
  state.group=g;state.groupDetail=null;state.groupError=null;
  const h='#/groups/'+encodeURIComponent(g);
  if(location.hash!==h)location.hash=h;
  else render();
};
async function loadGroupDetail(){
  const generation=state.targetGeneration,name=state.group;
  if(!name||_groupDetailLoading)return;
  _groupDetailLoading=true;
  state.groupError=null;
  try{
    const detail=await api(`/api/v1/groups/${encodeURIComponent(name)}`);
    if(generation!==state.targetGeneration||name!==state.group)return;
    state.groupError=null;
    state.groupDetail=detail;
    const gid=detail.groupId||detail.group_id||name;
    const lag=((detail.offsets)||[]).reduce((a,o)=>a+(o.lag||0),0);
    state.lagHist=state.lagHist||{};
    const h=(state.lagHist[gid]=state.lagHist[gid]||[]);
    h.push(lag);
    if(h.length>60)h.shift();
    render();
  }catch(e){
    if(generation!==state.targetGeneration||name!==state.group||e.staleTargetResponse)return;
    state.groupError=(e&&e.message)||'Failed to load consumer group';
    render();
  }finally{
    _groupDetailLoading=false;
    if(state.view==='groups'&&state.group&&!state.groupDetail&&!state.groupError)loadGroupDetail();
  }
}
window.loadGroupDetail=loadGroupDetail;
function viewGroupDetail(){
  if(state.groupError){
    return `<div class="card" role="alert"><h3>Failed to load consumer group</h3><p class="muted" style="margin:8px 0 14px">${esc(state.groupError)}</p><div style="display:flex;gap:8px"><button class="btn" onclick="loadGroupDetail()">Retry</button><button class="btn ghost" onclick="state.group=null;state.groupDetail=null;state.groupError=null;location.hash='#/groups';">Back</button></div></div>`;
  }
  const g=state.groupDetail;
  if(!g)return '<div class="card">'+skel(140)+' '+skel(90)+' '+skel(60)+'</div>';
  const readOnly=isKafkaTarget(),reason='Kafka consumer groups are read-only; offset reset is disabled.';
  const gid=g.groupId||g.group_id||state.group||'';
  const lagRows=((g.offsets)||[]).map(o=>{
    const cur=o.currentOffset??o.current_offset??'-';
    const end=o.logEndOffset??o.log_end_offset??'-';
    const lag=o.lag??0;
    return `<tr><td class="mono">${esc(o.topic)}</td><td>${o.partition}</td><td>${cur}</td><td>${end}</td><td><span class="badge ${lag>0?'b-amber':'b-green'}">${lag}</span></td><td>${readOnly?`<button class="btn ghost" aria-disabled="true" title="${reason}" onclick="toast('${reason}')">Reset</button>`:`<button class="btn ghost" style="padding:3px 8px;font-size:11px" onclick="resetGroupOffset(${jsArg(o.topic)},${o.partition})">Reset</button>`}</td></tr>`;
  }).join('')||'<tr><td colspan="6" class="muted">No committed offsets</td></tr>';
  const memberRecords=Array.isArray(g.members)?g.members:null;
  const members=memberRecords?memberRecords.map(m=>{
    const mid=m.memberId||m.member_id||'-';
    const host=m.clientHost||m.client_host||'';
    return `<div class="chip" style="padding:3px 10px;margin:2px">${esc(mid)}${host?' @ '+esc(host):''}</div>`;
  }).join('')||'<span class="muted">No members reported.</span>':'<span class="muted">Member details unavailable.</span>';
  const memberCount=memberRecords?`${memberRecords.length} member(s)`:'n/a (member details unavailable)';
  const lh=(state.lagHist||{})[gid]||[],maxLag=Math.max(1,...lh),spark=lh.length>1?`<div class="spark" title="Lag history (last ${lh.length} polls)">${lh.map(v=>`<span style="height:${Math.max(3,v/maxLag*100)}%"></span>`).join('')}</div>`:'';
  return `<div>${pageHeader(esc(gid),'Consumer membership and committed offsets.')}<div class="card"><div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;flex-wrap:wrap"><h3 style="font-size:15px">${esc(gid)}</h3><span class="badge ${online(g.state)?'b-green':'b-amber'}">${esc(g.state)}</span><button class="btn ghost" style="padding:4px 10px;font-size:11px;margin-left:auto" onclick="state.group=null;state.groupDetail=null;state.groupError=null;location.hash='#/groups';">Back</button></div>
    <div class="muted" style="margin-bottom:14px">Protocol: ${esc(g.protocol||'range')} &middot; ${memberCount}</div><div style="margin-bottom:16px">${members}</div>${spark}${readOnly?`<p class="muted" style="font-size:11px;margin:8px 0">${reason}</p>`:''}${tableScroll(`<table><thead><tr><th>Topic</th><th>Partition</th><th>Current</th><th>Log End</th><th>Lag</th><th></th></tr></thead><tbody>${lagRows}</tbody></table>`)}</div></div>`;
}
window.resetGroupOffset=async function(topic,part){
  const strategy=prompt('Reset strategy (to_earliest | to_latest | to_offset):','to_latest');if(!strategy)return;
  const group=state.group,generation=state.targetGeneration,body={topic,partition:part,strategy};
  if(strategy==='to_offset'){const offset=prompt('Target offset:');if(offset===null)return;body.target_offset=parseInt(offset,10);}
  try{await api(`/api/v1/groups/${encodeURIComponent(group)}/offsets/reset`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});if(generation!==state.targetGeneration||group!==state.group)return;toast('Offset reset');loadGroupDetail();}
  catch(e){if(generation!==state.targetGeneration||e.staleTargetResponse)return;toast(e.message);}
};
window.delGroup=async function(g){
  if(!await askConfirm({title:'Delete group',body:'Delete group '+g+'?',confirmLabel:'Delete group',danger:true}))return;const generation=state.targetGeneration;
  try{await api('/api/v1/groups/'+encodeURIComponent(g),{method:'DELETE'});if(generation!==state.targetGeneration)return;toast('Deleted '+g);await refresh();if(generation===state.targetGeneration)render();}
  catch(e){if(generation!==state.targetGeneration||e.staleTargetResponse)return;toast(e.message);}
};

/* Producer Studio */
function viewProducer(){ const opts=(state.topics||[]).map(t=>`<option value="${esc(t.name)}">${esc(t.name)}</option>`).join('');
  const curT=(state.topics||[]).find(t=>t.name===(state.topic||''));
  let partOpts='<option value="-1">Auto</option>';
  if(curT&&curT.partitions&&curT.partitions.length){
    partOpts+=curT.partitions.map(p=>`<option value="${p.partition??p.id??p}">${p.partition??p.id??p}</option>`).join('');
  } else {
    partOpts+='<option value="0">0</option>';
  }
  return `<div>${pageHeader('Producer Studio','Compose and publish messages, simulate mock stream traffic, and generate SDK code.')}
  <div class="card">
    <h3 style="margin-bottom:12px">Message Composer</h3>
    <label>Topic</label><input list="tl" id="pTopic" placeholder="my-topic (auto-created)" value="${esc(state.topic||'')}" onchange="updateProducerPartitions()"><datalist id="tl">${opts}</datalist>
    <label>Partition</label><select id="pPart" style="width:auto">${partOpts}</select>
    <label>Key</label><input id="pKey" placeholder="optional key">
    <label>Headers (key=value, one per line)</label><textarea id="pHeaders" rows="2" placeholder="traceId=abc"></textarea>
    <label>Payload (JSON / text)</label><textarea id="pValue" rows="6">{"event":"hello","ts":1700000000}</textarea>
    <div style="margin-top:12px"><button class="btn" onclick="sendMsg()">Publish Message</button></div>
  </div>
  <div class="card"><h3 style="margin-bottom:12px">Mock Stream Generator</h3>
    <label>Topic</label><input id="mkTopic" list="tl" value="${esc(state.topic||'demo')}">
    <label>Messages / sec (1-1000)</label><input id="mkRate" type="number" value="10" min="1" max="1000">
    <label>Template</label><select id="mkTpl">
      <option value="faker">Faker (name,email,price,uuid,status)</option>
      <option value="order">Order (id,item,price)</option>
      <option value="iot">IoT (device,temp,humidity)</option>
    </select>
    <div style="margin-top:12px"><button class="btn" onclick="mockGenStart()">Start Traffic</button> <button class="btn danger" onclick="mockGenStop()">Stop</button>
    <span class="muted" style="font-size:11px" id="mockStat"></span></div>
  </div>
  <div class="card"><h3 style="margin-bottom:12px">Payload to Code Generator</h3>
    <label>Paste a JSON payload</label><textarea id="pgInput" rows="5">${esc('{"user":{"id":"u1","name":"Alice","email":"a@x.com","active":true,"price":1200.5}}')}</textarea>
    <label>Language</label><select id="pgLang" onchange="genCode()"><option value="go">Go struct</option><option value="ts">TypeScript interface</option><option value="py">Python Pydantic</option></select>
    <div style="margin-top:8px"><button class="btn" onclick="genCode()">Generate</button></div>
    <div class="codeOut" id="pgOut" style="margin-top:10px"></div>
  </div>
  <div class="card"><h3 style="margin-bottom:12px">RPC Request / Reply Tester</h3>
    <div class="row">
      <label style="margin:0">Request topic</label><input id="rpcReq" list="tl" value="rpc-req" style="width:170px">
      <label style="margin:0">Reply topic</label><input id="rpcReply" value="rpc-reply" style="width:170px">
      <label style="margin:0">Method</label><input id="rpcMethod" value="getUser" style="width:150px">
      <label style="margin:0">Payload</label><input id="rpcPayload" value='{"id":1}' style="width:220px">
      <button class="btn" onclick="rpcSend()">Send</button>
    </div>
    <div class="muted" style="font-size:11px;margin-top:6px">Sends a message with <code>correlation_id</code> + <code>reply_to_topic</code> headers. A consumer on the reply topic echoes a response for demo.</div>
  </div>
  </div>`; }
function updateProducerPartitions(){
  const val=$('#pTopic')?.value;
  const curT=(state.topics||[]).find(t=>t.name===val);
  const sel=$('#pPart');
  if(!sel)return;
  let partOpts='<option value="-1">Auto</option>';
  if(curT&&curT.partitions&&curT.partitions.length){
    partOpts+=curT.partitions.map(p=>`<option value="${p.partition??p.id??p}">${p.partition??p.id??p}</option>`).join('');
  } else {
    partOpts+='<option value="0">0</option>';
  }
  sel.innerHTML=partOpts;
}
window.updateProducerPartitions=updateProducerPartitions;
async function sendMsg(){ const body={value:$('#pValue').value,key:$('#pKey').value}; const pv=$('#pPart')?$('#pPart').value:'-1';
  if(pv&&pv!=='-1')body.partition=parseInt(pv,10);
  const h={}; $('#pHeaders').value.split('\n').forEach(l=>{const i=l.indexOf('=');if(i>0)h[l.slice(0,i).trim()]=l.slice(i+1).trim();}); if(Object.keys(h).length)body.headers=h;
  const topic=$('#pTopic').value||'demo'; const btn=document.querySelector('#view .card .btn'); if(btn){btn.disabled=true;const o=btn.textContent;btn.textContent='Publishing...';}
  try{const r=await api('/api/v1/topics/'+encodeURIComponent(topic)+'/messages',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});toast(`Published to P${r.partition} @ offset ${r.offset}`);await refresh();}catch(e){toast(e.message);}
  if(btn){btn.disabled=false;btn.textContent='Publish Message';} }
window.sendMsg=sendMsg;


/* Fitur 4.1: Developer Studio */
function viewDevStudio(){
  return `<div>${pageHeader('Developer Studio','Generate sample traffic, code types, and test request/reply flows.')}<div class="grid">
    <div class="card"><h3 style="margin-bottom:12px">Mock Stream Generator</h3>
      <label>Topic</label><input id="mkTopic" list="tl" value="${esc(state.topic||'demo')}">
      <label>Messages / sec (1-1000)</label><input id="mkRate" type="number" value="10" min="1" max="1000">
      <label>Template</label><select id="mkTpl">
        <option value="faker">Faker (name,email,price,uuid,status)</option>
        <option value="order">Order (id,item,price)</option>
        <option value="iot">IoT (device,temp,humidity)</option>
      </select>
      <div style="margin-top:12px"><button class="btn" onclick="mockGenStart()">Start</button> <button class="btn danger" onclick="mockGenStop()">Stop</button>
      <span class="muted" style="font-size:11px" id="mockStat"></span></div>
    </div>
    <div class="card"><h3 style="margin-bottom:12px">Payload to Code Generator</h3>
      <label>Paste a JSON payload</label><textarea id="pgInput" rows="6">${esc('{"user":{"id":"u1","name":"Alice","email":"a@x.com","active":true,"price":1200.5}}')}</textarea>
      <label>Language</label><select id="pgLang" onchange="genCode()"><option value="go">Go struct</option><option value="ts">TypeScript interface</option><option value="py">Python Pydantic</option></select>
      <div style="margin-top:8px"><button class="btn" onclick="genCode()">Generate</button></div>
      <div class="codeOut" id="pgOut" style="margin-top:10px"></div>
    </div>
    <div class="card"><h3 style="margin-bottom:12px">RPC Request / Reply Tester</h3>
      <div class="row">
        <label style="margin:0">Request topic</label><input id="rpcReq" list="tl" value="rpc-req" style="width:170px">
        <label style="margin:0">Reply topic</label><input id="rpcReply" value="rpc-reply" style="width:170px">
        <label style="margin:0">Method</label><input id="rpcMethod" value="getUser" style="width:150px">
        <label style="margin:0">Payload</label><input id="rpcPayload" value='{"id":1}' style="width:220px">
        <button class="btn" onclick="rpcSend()">Send</button>
      </div>
      <div class="muted" style="font-size:11px;margin-top:6px">Sends a message with <code>correlation_id</code> + <code>reply_to_topic</code> headers. A consumer on the reply topic echoes a response for demo.</div>
    </div>
  </div></div>`; }
window.genCode=function(){ const inp=document.getElementById('pgInput').value; let obj; try{obj=JSON.parse(inp);}catch(e){toast('Invalid JSON');return;} const lang=document.getElementById('pgLang').value; const out=document.getElementById('pgOut'); out.textContent=objToCode(obj,lang); };
function cap(s){return s.charAt(0).toUpperCase()+s.slice(1);}
function tsType(v){if(v===null)return'unknown';if(Array.isArray(v))return (v.length?'Array<'+tsType(v[0])+'>':'unknown[]');if(typeof v==='object')return'Record<string, unknown>';if(typeof v==='number')return v%1===0?'number':'number';if(typeof v==='boolean')return'boolean';return'string';}
function goType(v){if(Array.isArray(v))return '[]'+goType(v[0]||'string');if(typeof v==='object')return 'map[string]any';if(typeof v==='number')return v%1===0?'int64':'float64';if(typeof v==='boolean')return'bool';return'string';}
function pyType(v){if(Array.isArray(v))return'List['+pyType(v[0]||'str')+']';if(typeof v==='object')return'Dict[str, Any]';if(typeof v==='number')return'float';if(typeof v==='boolean')return'bool';return'str';}
function objToCode(obj,lang){
  const name='Payload';
  if(lang==='go'){
    const lines=[]; lines.push('type '+name+' struct {'); for(const k in obj){ lines.push(`\t${cap(k)} ${goType(obj[k])} \`json:"${k}"\``); } lines.push('}'); return lines.join('\n');
  } else if(lang==='ts'){
    const lines=[]; lines.push('export interface '+name+' {'); for(const k in obj){ lines.push(`  ${k}: ${tsType(obj[k])};`); } lines.push('}'); return lines.join('\n');
  } else {
    const lines=[]; lines.push('from pydantic import BaseModel'); lines.push('from typing import Any, List, Dict'); lines.push(''); lines.push('class '+name+'(BaseModel):'); for(const k in obj){ lines.push(`    ${k}: ${pyType(obj[k])}`); } return lines.join('\n');
  }
}
let mockTimer=null;
function mockPayload(tpl){ const rnd=()=>Math.random().toString(36).slice(2,8); const rn=(a,b)=>a+Math.floor(Math.random()*(b-a)); if(tpl==='order')return JSON.stringify({id:rnd(),item:['laptop','phone','tablet','watch'][rn(0,4)],price:rn(50,1500),status:['placed','paid','shipped'][rn(0,3)]}); if(tpl==='iot')return JSON.stringify({device:'dev-'+rn(1,20),temp:rn(150,320)/10,humidity:rn(200,900)/10,ts:Date.now()}); return JSON.stringify({name:['alice','bob','carol','dave'][rn(0,4)],email:'u'+rnd()+'@x.com',price:rn(10,500),uuid:rnd(),status:['active','pending','failed'][rn(0,3)]}); }
window.mockGenStart=function(){ const topic=document.getElementById('mkTopic').value||'demo'; const rate=Math.max(1,Math.min(1000,parseInt(document.getElementById('mkRate').value)||10)); const tpl=document.getElementById('mkTpl').value; const st=document.getElementById('mockStat'); mockGenStop(); let count=0; const interval=Math.max(1,Math.round(1000/rate)); mockTimer=setInterval(async()=>{ try{ await api('/api/v1/topics/'+encodeURIComponent(topic)+'/messages',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({value:mockPayload(tpl)})}); count++; if(st)st.textContent=count+' msgs sent'; }catch(e){ mockGenStop(); if(st)st.textContent='error: '+e.message; } },interval); };
window.mockGenStop=function(){ if(mockTimer){clearInterval(mockTimer);mockTimer=null;} const st=document.getElementById('mockStat'); if(st)st.textContent+=' (stopped)'; };
window.rpcSend=function(){ const req=document.getElementById('rpcReq').value,reply=document.getElementById('rpcReply').value,method=document.getElementById('rpcMethod').value,payload=document.getElementById('rpcPayload').value; const corr=Math.random().toString(36).slice(2,10); const body={key:corr,value:JSON.stringify({method,request:JSON.parse(payload||'{}'),reply_to:reply}),headers:{correlation_id:corr,reply_to_topic:reply,type:'request'}}; api('/api/v1/topics/'+encodeURIComponent(req)+'/messages',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}).then(r=>toast('RPC request sent @ offset '+r.offset+' (corr '+corr+')')).catch(e=>toast(e.message)); };

/* Fitur 4.2: Stream Topology & Data Flow */
/* Dipindah: peta SVG broker -> topics -> groups ada di renderTopoSvg()/viewTopology()
   di atas (workbench), lengkap dengan node detail drawer. */

/* Fitur 4.3: Storage & Segment Inspector */
function viewStorage(){
  const tsel=(state.topics||[]).map(t=>`<option value="${esc(t.name)}">${esc(t.name)}</option>`).join('');
  return `<div>${pageHeader('Storage','Inspect topic segments and jump to a message offset.')}
    <div class="card"><div class="row"><h3 style="margin-right:10px">Segment Inspector</h3><select id="stTopic" onchange="loadSegments()" style="width:auto"><option value="">Select topic</option>${tsel}</select></div>
      <div id="segOut" class="muted" style="margin-top:10px;font-size:12px">Select a topic to view segment blocks.</div></div>
    <div class="card"><h3>Time-Travel Replay</h3>
      <div class="row"><label style="margin:0">Offset</label><input id="ttOff" type="number" value="0" style="width:120px"><label style="margin:0">Partition</label><input id="ttPart" type="number" value="0" style="width:100px"><button class="btn ghost" onclick="ttJump()">Jump &amp; View</button></div>
      <div class="muted" style="font-size:11px;margin-top:6px">Loads messages from a chosen offset as a time-travel point in the Data Browser.</div></div>
  </div></div>`; }
window.loadSegments=async function(){ const t=document.getElementById('stTopic').value; if(!t)return; const out=document.getElementById('segOut'); try{ const ps=await api('/api/v1/topics/'+encodeURIComponent(t)+'/partitions'); let html=''; ps.forEach(p=>{ const segs=p.segments||[]; const total=segs.reduce((a,s)=>a+s.sizeBytes,0); const remote=segs.filter(s=>s.remote).length; html+=`<div style="margin-top:10px"><div style="display:flex;justify-content:space-between;font-size:11px;font-family:var(--font-mono)"><span>P${p.partition}</span><span class="muted">${segs.length} segments &middot; ${fmtBytes(p.bytes)} &middot; ${remote} tiered</span></div>`; segs.forEach(s=>{ html+=`<div class="segBlock ${s.remote?'remote':(s.sizeBytes===segs[segs.length-1].sizeBytes?'act':'closed')}" title="base ${s.baseOffset} &middot; ${fmtBytes(s.sizeBytes)}">${s.baseOffset}${s.remote?' T':''}</div>`; }); html+='</div>'; }); out.innerHTML=html||'<div class="muted">no segments</div>'; }catch(e){out.textContent='error: '+e.message;} };
window.ttJump=function(){ const off=document.getElementById('ttOff').value, part=document.getElementById('ttPart').value; state.off=off; state.partition=part; state.topic=document.getElementById('stTopic').value; if(!state.topic){toast('Select topic first');return;} goView('databrowser').then(()=>loadMsgs()); };

/* Fitur 4.4: Schema Studio */
function viewSchemas(){
  if(!(state.schemas||[]).length)return `<div>${pageHeader('Schema Registry','Register schemas and review subject versions.')}<div class="card"><h3 style="margin-bottom:10px">Schema Registry Studio</h3>
    <div class="muted" style="font-size:12px;margin-bottom:14px">No schemas registered. Register the first one:</div>
    <label>Subject</label><input id="nsSub" placeholder="my-topic-value" style="max-width:280px">
    <label>Type</label><select id="nsType" style="width:auto"><option>AVRO</option><option>JSON</option></select>
    <label>Schema (JSON)</label><textarea id="nsSchema" rows="6">{"type":"record","name":"Payload","fields":[{"name":"id","type":"string"}]}</textarea>
    <div style="margin-top:10px"><button class="btn" onclick="registerSchema()">Register Schema</button></div></div></div>`;
  return `<div>${pageHeader('Schema Registry','Register schemas and review subject versions.')}<div class="card"><h3 style="margin-bottom:10px">Schema Registry Studio</h3>
    <div class="row" style="margin-bottom:12px"><select id="schSubject" onchange="loadSchemaDetail()" style="width:auto">${(state.schemas||[]).map(s=>`<option value="${esc(s)}">${esc(s)}</option>`).join('')}</select>
    <button class="btn ghost" onclick="loadSchemaDetail()">Load versions</button>
    <span style="flex:1"></span>
    <select id="nsType" style="width:auto"><option>AVRO</option><option>JSON</option></select>
    <input id="nsSub" placeholder="new subject" style="max-width:160px">
    <button class="btn" title="Register schema version baru" onclick="registerSchema()">+ Register</button></div>
    <textarea id="nsSchema" rows="4" placeholder='{"type":"record","name":"Payload","fields":[...]}' style="margin-bottom:10px"></textarea>
    <div id="schVersions"></div></div></div>`; }
async function loadSchemaDetail(){ const subj=document.getElementById('schSubject')?.value; if(!subj)return; try{ const vers=await api('/api/v1/schemas/'+encodeURIComponent(subj)); const out=document.getElementById('schVersions'); out.innerHTML=`<div class="tableScroll"><table><thead><tr><th>Version</th><th>Type</th><th>Schema</th><th>Compat</th></tr></thead><tbody>${vers.slice().reverse().map(v=>`<tr><td>${v.version}</td><td class="badge b-purple">${esc(v.schemaType||'AVRO')}</td><td style="max-width:420px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(v.schema)}</td><td><button class="btn ghost" style="padding:2px 8px;font-size:11px" onclick="schemaDiff('${esc(subj)}',${v.version})">Diff prev</button></td></tr>`).join('')||'<tr><td colspan="4" class="muted">no versions</td></tr>'}</tbody></table></div>`; }catch(e){toast(e.message);} }
window.schemaDiff=function(subj,ver){ const el=document.getElementById('schVersions'); if(!el)return; api('/api/v1/schemas/'+encodeURIComponent(subj)).then(vers=>{ const cur=vers.find(v=>v.version===ver); const prev=vers.find(v=>v.version===ver-1); if(!cur||!prev){toast('No previous version to diff');return;} const a=fmtSchema(prev.schema), b=fmtSchema(cur.schema); const la=a.split('\n'), lb=b.split('\n'); const max=Math.max(la.length,lb.length); let html='<div class="diffWrap" style="margin-top:8px">'; for(let i=0;i<max;i++){ const x=la[i]||'',y=lb[i]||''; const same=x.trim()===y.trim(); html+=`<div style="flex:1;font-family:var(--font-mono);font-size:11px"><div class="${same?'d-ctx':'d-rem'}">${esc(x||' ')}</div></div>`; html+=`<div style="flex:1;font-family:var(--font-mono);font-size:11px"><div class="${same?'d-ctx':'d-add'}">${esc(y||' ')}</div></div>`; } html+='</div>'; el.innerHTML+=`<div style="margin-top:14px"><b style="font-size:12px">Diff v${prev.version} -&gt; v${cur.version}</b>${html}</div>`; }).catch(e=>toast(e.message)); };
function fmtSchema(s){ try{return JSON.stringify(JSON.parse(s),null,2);}catch(e){return s;} }

/* Fitur 4.5: Security & ACL + Audit */
function delegatedTokenCard(){return `<div class="card"><h3>Delegated dashboard credentials</h3>
  <p class="muted" style="font-size:11px;margin:6px 0 10px">On the destination broker, issue a token for an existing security user with the required ACLs. The current signed-in user must have cluster Admin permission. Copy the raw token once, then paste it into the source dashboard's local Clusters form as the peer's Dashboard access token. This is separate from the read-only Health token.</p>
  <div class="row" style="align-items:flex-end;gap:8px;flex-wrap:wrap"><div><label for="delegatePrincipal">Existing security username</label><input id="delegatePrincipal" autocomplete="off" placeholder="configured username"></div><button class="btn" onclick="issueDelegatedToken()">Issue token</button><button class="btn ghost" onclick="loadDelegatedTokens()">Refresh tokens</button></div>
  <div id="delegatedTokenOnce" aria-live="polite"></div>
  <div id="delegatedTokenList" class="muted" style="margin-top:12px">Loading delegated tokens...</div>
  </div>`;}
function viewSecurity(){
  const aclAvailable=!isTargetUnavailable()&&!isKafkaTarget();
  const aclPanel=aclAvailable?`<div class="card"><div class="row"><h3 style="margin-right:10px">Visual ACL Manager</h3><button class="btn" onclick="addACLRow()">+ Add ACL</button><button class="btn ghost" onclick="loadACLs()">Refresh</button></div>
      <div class="muted" style="font-size:11px;margin:6px 0 10px">Grant Read / Write / Admin per topic or group. Changes are stored in-memory on the selected broker.</div><div class="tableScroll"><div id="aclOut" class="muted">Loading...</div></div></div>
    <div class="card"><h3>Audit Trail</h3><button class="btn ghost" style="margin:10px 0" onclick="loadAudit()">Refresh audit log</button>${tableScroll(`<table><thead><tr><th>Time</th><th>Actor</th><th>Action</th><th>Resource</th><th>Detail</th></tr></thead><tbody id="auditBody"><tr><td colspan="5" class="muted">No activity yet.</td></tr></tbody></table>`)}</div>`
    :`<div class="card" role="status"><h3>Broker ACL unavailable</h3><p class="muted" style="margin-top:8px">${isTargetUnavailable()?esc(targetUnavailableMessage()):'ACL administration is not supported for read-only Kafka targets.'} Delegated token administration below remains local to this dashboard.</p></div>`;
  return `<div>${pageHeader('Security & ACL','Manage local delegated credentials and the selected broker access rules.')}${delegatedTokenCard()}${aclPanel}</div>`;
}
async function loadACLs(){ const out=document.getElementById('aclOut'); if(!out)return; try{ const acls=await api('/api/v1/acls'); const principals=[...new Set((acls||[]).map(a=>a.principal))]; const resources=[...new Set((acls||[]).map(a=>a.resourceName))]; out.innerHTML=`<div class="tableScroll"><table><thead><tr><th>Principal</th><th>Resource</th><th>Read</th><th>Write</th><th>Admin</th><th></th></tr></thead><tbody>${(acls||[]).map(a=>`<tr><td class="mono">${esc(a.principal)}</td><td class="mono">${esc(a.resourceType)}:${esc(a.resourceName)}</td>${['Read','Write','Admin'].map(o=>`<td><button class="toggle ${a.operations.includes(o)?'on':''}" onclick="toggleOp('${esc(a.principal)}','${esc(a.resourceType)}','${esc(a.resourceName)}','${o}')" title="${o}"></button></td>`).join('')}<td><button class="btn danger" style="padding:2px 8px;font-size:11px" onclick="delACL('${esc(a.principal)}','${esc(a.resourceType)}','${esc(a.resourceName)}')">Delete</button></td></tr>`).join('')||'<tr><td colspan="6" class="muted">No ACLs defined.</td></tr>'}</tbody></table></div>`; }catch(e){out.textContent='error: '+e.message;} }
window.addACLRow=function(){ const el=document.createElement('div'); el.innerHTML=`<div class="overlay"><div class="modal"><h3>Add ACL</h3><label>Principal (user)</label><input id="aUser" placeholder="app-writer"><label>Resource type</label><select id="aType"><option value="topic">topic</option><option value="group">group</option></select><label>Resource name</label><input id="aName" placeholder="orders"><div style="margin-top:14px;display:flex;gap:8px"><button class="btn" onclick="aclsave()">Add</button><button class="btn ghost" onclick="closeOverlay()">Cancel</button></div></div></div>`; document.body.appendChild(el); };
window.aclsave=async function(){ const u=document.getElementById('aUser').value,t=document.getElementById('aType').value,n=document.getElementById('aName').value; if(!u||!n){toast('principal and resource required');return;} try{ await api('/api/v1/acls',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({principal:u,resourceType:t,resourceName:n,operations:['Read']})}); closeOverlay(); loadACLs(); loadAudit(); }catch(e){toast(e.message);} };
window.toggleOp=async function(p,rt,rn,op){ try{ const acls=await api('/api/v1/acls'); const rule=acls.find(a=>a.principal===p&&a.resourceType===rt&&a.resourceName===rn); const ops=(rule&&rule.operations)||[]; const has=ops.includes(op); const next=has?ops.filter(x=>x!==op):[...ops,op]; await api('/api/v1/acls',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({principal:p,resourceType:rt,resourceName:rn,operations:next})}); loadACLs(); }catch(e){toast(e.message);} };
window.delACL=async function(p,rt,rn){ try{ await api('/api/v1/acls',{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({principal:p,resourceType:rt,resourceName:rn})}); loadACLs(); loadAudit(); }catch(e){toast(e.message);} };
async function loadAudit(){ const b=document.getElementById('auditBody'); if(!b)return; try{ const log=await api('/api/v1/audit'); b.innerHTML=log.map(e=>`<tr><td class="mono">${new Date(e.time).toISOString()}</td><td>${esc(e.actor)}</td><td class="chip">${esc(e.action)}</td><td class="mono">${esc(e.resource)}</td><td>${esc(e.detail)}</td></tr>`).join('')||'<tr><td colspan="5" class="muted">No activity yet.</td></tr>'; }catch(e){b.innerHTML='<tr><td colspan="5" class="muted">error</td></tr>';} }
window.refreshSecurity=function(){loadDelegatedTokens();if(!isKafkaTarget()&&!isTargetUnavailable()){loadACLs();loadAudit();}};
async function loadDelegatedTokens(){
  const out=document.getElementById('delegatedTokenList');if(!out)return;
  try{const tokens=await apiLocal('/api/v1/auth/delegated-tokens');out.innerHTML=`<div class="tableScroll"><table><thead><tr><th>Token ID</th><th>Principal</th><th></th></tr></thead><tbody>${(tokens||[]).map(t=>`<tr><td class="mono">${esc(t.id)}</td><td>${esc(t.principal)}</td><td><button class="btn ghost" onclick="revokeDelegatedToken(${jsArg(t.id)})">Revoke</button></td></tr>`).join('')||'<tr><td colspan="3" class="muted">No delegated tokens issued.</td></tr>'}</tbody></table></div>`;}
  catch(e){out.textContent='Cannot load delegated tokens: '+e.message;}
}
window.issueDelegatedToken=async function(){
  const principal=(document.getElementById('delegatePrincipal')?.value||'').trim();if(!principal){toast('Enter an existing security username');return;}
  try{const issued=await apiLocal('/api/v1/auth/delegated-tokens',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({principal})});
    const host=document.getElementById('delegatedTokenOnce');if(host){const box=document.createElement('div');box.className='card';box.style.marginTop='12px';const title=document.createElement('strong');title.textContent='Copy this token now; it cannot be shown again.';const raw=document.createElement('code');raw.id='issuedDelegatedToken';raw.textContent=issued.token;raw.style.cssText='display:block;overflow-wrap:anywhere;margin:8px 0;font-family:var(--font-mono)';const actions=document.createElement('div');actions.className='row';actions.innerHTML='<button class="btn" onclick="copyIssuedDelegatedToken()">Copy token</button><button class="btn ghost" onclick="document.getElementById(\'delegatedTokenOnce\').replaceChildren()">Hide token</button>';box.append(title,raw,actions);host.replaceChildren(box);}
    const input=document.getElementById('delegatePrincipal');if(input)input.value='';loadDelegatedTokens();
  }catch(e){toast(e.message);}
};
window.copyIssuedDelegatedToken=async function(){
  const raw=document.getElementById('issuedDelegatedToken');if(!raw)return;
  let copied=false;
  try{if(navigator.clipboard&&navigator.clipboard.writeText){await navigator.clipboard.writeText(raw.textContent);copied=true;}}catch(e){}
  if(!copied)copied=fallbackCopy(raw.textContent);
  if(!copied){toast('Clipboard access failed; select and copy the displayed token manually.');return;}
  document.getElementById('delegatedTokenOnce')?.replaceChildren();
  toast('Delegated token copied. Paste it into the source Clusters form.');
};
window.revokeDelegatedToken=async function(id){if(!id||!await askConfirm({title:'Revoke delegated token',body:'Revoke delegated token '+id+'?',confirmLabel:'Revoke token',danger:true}))return;try{await apiLocal('/api/v1/auth/delegated-tokens/'+encodeURIComponent(id),{method:'DELETE'});toast('Delegated token revoked');loadDelegatedTokens();}catch(e){toast(e.message);}};

/* Fitur 4.6: In-App Health Alarms */
/* Setiap alarm punya id tetap (mis. "lag:<grup>") yang terpisah dari teksnya.
   Sebelumnya id = teks lengkap termasuk angka lag, jadi begitu lag bergerak
   sedikit alarm lama dianggap "sudah tidak aktif" dan Dismiss tidak pernah
   menempel. Ambang lag grup juga bisa diatur (pk_group_lag_warn): kelompok sync
   pada topik besar memang tertinggal ratusan juta pesan, dan ambang 1000
   membuat banner itu menyala selamanya. */
function timeCell(ts){ return state.timeAbs
  ? '<div>'+new Date(ts).toISOString()+'</div><div class="rel">'+rel(ts)+'</div>'
  : '<div>'+rel(ts)+'</div><div class="rel">'+new Date(ts).toISOString()+'</div>'; }
window.sortInd=function(k,f){const s=state.sort[k];return s&&s.f===f?(s.dir==='asc'?'^':'v'):''};
window.sortRows=function(k,f){const s=state.sort[k]||{};s.dir=(s.f===f&&s.dir==='asc')?'desc':'asc';s.f=f;state.sort[k]=s;render();};
function sortedTopics(){ const s=state.sort.topics; if(!s)return state.topics||[]; const mul=s.dir==='asc'?1:-1;
  return (state.topics||[]).slice().sort((a,b)=>{ let va,vb;
    if(s.f==='name'){va=a.name;vb=b.name;}
    else if(s.f==='size'){va=a.totalBytes||0;vb=b.totalBytes||0;}
    else {va=((a.partitions||[])[0]||{}).logEndOffset||0;vb=((b.partitions||[])[0]||{}).logEndOffset||0;}
    return (va>vb?1:va<vb?-1:0)*mul; }); }
function sortedGroups(){ const s=state.sort.groups; const arr=(state.groups||[]).slice(); if(!s)return arr; const mul=s.dir==='asc'?1:-1;
  return arr.sort((a,b)=>{ let va,vb;
    if(s.f==='name'){va=a.name;vb=b.name;}
    else if(s.f==='members'){va=(a.members||[]).length;vb=(b.members||[]).length;}
    else {va=Object.values(a.lag||{}).reduce((x,y)=>x+y,0);vb=Object.values(b.lag||{}).reduce((x,y)=>x+y,0);}
    return (va>vb?1:va<vb?-1:0)*mul; }); }

/* Metrics view: throughput sparkline + MQTT status */
function viewMetrics(){ return `<div>${pageHeader('Metrics','Review message throughput and MQTT bridge status.')}
  <div class="card"><div class="kicker">Ingest</div><h3>Throughput</h3>
    <div class="statXL" id="tpRate">--<small>msg/s</small></div><div class="statSub" id="tpTotal"></div>
    <div class="spark" id="tpSpark"></div>
    <div class="muted" style="font-size:11px;margin-top:8px">Dihitung dari delta total pesan antar polling (${state.refreshMs/1000}s). 60 sampel terakhir.</div></div>
  <div class="card"><div class="kicker">Edge</div><h3>MQTT Bridge</h3><div id="mqttBody" class="muted" style="font-size:12px">${skel(80)}</div></div>
  </div></div>`; }
async function loadThroughput(){
  try{ const d=await api('/api/v1/throughput'); state.tpSamples=d.samples||[]; state.tpTotal=d.totalMessages||0;
    const rate=state.tpSamples.length?state.tpSamples[state.tpSamples.length-1]:0;
    const rEl=document.getElementById('tpRate'); if(rEl)rEl.innerHTML=rate.toFixed(1)+'<small>msg/s</small>';
    const tEl=document.getElementById('tpTotal'); if(tEl)tEl.textContent='total '+state.tpTotal+' messages';
    const sp=document.getElementById('tpSpark');
    if(sp){ const win=state.tpSamples.slice(-60); const max=Math.max(1,...win);
      sp.innerHTML=win.map(v=>`<span style="height:${Math.max(3,v/max*100)}%"></span>`).join(''); }
  }catch(e){}
  const m=document.getElementById('mqttBody');
  if(m){ try{ const st=await api('/api/v1/mqtt');
      m.innerHTML=st.enabled?`<div class="row"><span class="badge b-green">LISTENING</span><span class="mono">${esc(st.listen||'')}</span></div><div class="row" style="margin-top:8px"><span class="statSub">${st.connections} client(s) aktif</span><span class="statSub">${st.bridged} pesan di-bridge</span></div>`:'<span class="muted">Bridge tidak aktif</span>';
    }catch(e){ m.textContent='error: '+e.message; } }
}
window.loadThroughput=loadThroughput;

/* Integrations view: alamat & contoh pakai tiap ingress surface (MQTT, REST Proxy,
   Schema Registry, Prometheus). Data datang dari satu endpoint read-only, jadi
   render dibuat defensif terhadap field yang hilang (peer bisa kirim parsial). */
function viewIntegrations(){
  const body=state.integrations?renderIntegrations(state.integrations):`<div id="integrationsBody" class="muted" style="font-size:12px">${skel(120)}</div>`;
  return `<div>${pageHeader('Integrations','Addresses and usage snippets for the ingress surfaces this broker exposes.')}
  ${state.integrations?`<div id="integrationsBody">${body}</div>`:body}
  </div>`;
}
/* pecah "host:port" secara defensif; 0.0.0.0 diganti localhost karena itu alamat bind,
   bukan alamat yang bisa dipakai klien untuk connect */
function splitHostPort(addr){
  const s=String(addr||'');
  const i=s.lastIndexOf(':');
  let host=i>=0?s.slice(0,i):s, port=i>=0?s.slice(i+1):'';
  const bindAll=host==='0.0.0.0'||host==='';
  if(bindAll)host='localhost';
  return {host:host||'localhost',port:port||'-',bindAll};
}
function copyRow(id,label){
  return `<button class="btn ghost" style="padding:2px 8px;font-size:11px" onclick="copyText('#${id}','${label}')">Copy</button>`;
}
function renderIntegrations(d){
  const mqtt=(d&&d.mqtt)||{}, restProxy=(d&&d.restProxy)||{}, schemaRegistry=(d&&d.schemaRegistry)||{}, metrics=(d&&d.metrics)||{};
  /* --- Kartu 1: MQTT --- */
  const mqttHp=splitHostPort(mqtt.listen);
  const mqttTopics=(state.topics||[]).filter(t=>(t.name||'').startsWith('mqtt-'));
  const mqttTopicsHtml=mqttTopics.length
    ? mqttTopics.map(t=>`<div class="row" style="justify-content:space-between"><span class="mono">${esc(t.name)}</span><button class="btn ghost" style="padding:2px 8px;font-size:11px" onclick="pickTopicNav('${esc(t.name)}')">Open in Data Browser</button></div>`).join('')
    : `<span class="muted">No mqtt-* topics yet.</span>`;
  const mqttCard=`<div class="card"><div class="kicker">Edge</div><h3>MQTT Bridge</h3>
    <div class="row">${mqtt.enabled===true?'<span class="badge b-green">LISTENING</span>':'<span class="badge muted">Bridge not running</span>'}
      <span class="mono" id="mqttAddr">${esc(mqtt.listen||'-')}</span>${copyRow('mqttAddr','Address')}</div>
    <div class="row" style="margin-top:8px"><span class="statSub">${mqtt.connections??0} / ${mqtt.maxConnections??0} clients</span><span class="statSub">${mqtt.bridged??0} messages bridged</span></div>
    <div class="row" style="margin-top:8px"><span class="mono">${esc(mqtt.topicPattern||'-')} &rarr; ${esc(mqtt.kafkaTopicPattern||'-')}, key &lt;device&gt;, header ${esc(mqtt.topicHeader||'-')}</span></div>
    <div class="hr"></div>
    <div class="kicker">mqtt-* topics</div>
    ${mqttTopicsHtml}
    <div class="hr"></div>
    <div class="kicker">Publish</div>
    <pre id="mqttPub">mosquitto_pub -h ${esc(mqttHp.host)} -p ${esc(mqttHp.port)} -t sensors/&lt;device&gt;/&lt;metric&gt; -m '{"v":23.5}'</pre>
    ${copyRow('mqttPub','Snippet')}
    <div class="kicker" style="margin-top:12px">Subscribe</div>
    <pre id="mqttSub">mosquitto_sub -h ${esc(mqttHp.host)} -p ${esc(mqttHp.port)} -t 'sensors/#' -v</pre>
    ${copyRow('mqttSub','Snippet')}
    ${mqttHp.bindAll?`<div class="muted" style="font-size:11px;margin-top:6px">Listener binds all interfaces (0.0.0.0); use the broker's reachable hostname when connecting remotely.</div>`:''}
  </div>`;
  /* --- Kartu 2: REST Proxy --- */
  const restHp=splitHostPort(restProxy.listen);
  const restHostPort=`${restHp.host}:${restHp.port}`;
  const restEndpoints=(restProxy.endpoints||[]).length
    ? (restProxy.endpoints||[]).map(e=>`<div class="mono">${esc(e)}</div>`).join('')
    : `<span class="muted">No endpoints reported.</span>`;
  const restAuthHtml=restProxy.auth===true
    ? `<span class="badge muted">authentication required</span><div class="muted" style="font-size:11px;margin-top:4px">SASL/ACL is enabled; send the credential documented in the README (section 4).</div>`
    : `<span class="badge muted">no auth</span>`;
  const restCard=`<div class="card"><div class="kicker">HTTP</div><h3>REST Proxy</h3>
    <div class="row"><span class="mono" id="restAddr">${esc(restProxy.listen||'-')}</span>${copyRow('restAddr','Address')}</div>
    <div class="kicker" style="margin-top:12px">Endpoints</div>
    ${restEndpoints}
    <div class="row" style="margin-top:8px">${restAuthHtml}</div>
    <div class="hr"></div>
    <div class="kicker">Publish</div>
    <pre id="restPub">curl -s -X POST http://${esc(restHostPort)}/topics/&lt;topic&gt;/messages -H 'Content-Type: application/json' -d '{"key":"k","value":"v"}'</pre>
    ${copyRow('restPub','Snippet')}
    <div class="kicker" style="margin-top:12px">Consume</div>
    <pre id="restSub">curl -s 'http://${esc(restHostPort)}/topics/&lt;topic&gt;/messages?limit=5'</pre>
    ${copyRow('restSub','Snippet')}
  </div>`;
  /* --- Kartu 3: Schema Registry --- */
  const srHp=splitHostPort(schemaRegistry.listen);
  const srAddr=schemaRegistry.listen||'-';
  const srCard=`<div class="card"><div class="kicker">Schemas</div><h3>Schema Registry</h3>
    <div class="row"><span class="mono" id="srAddr">${esc(srAddr)}</span>${copyRow('srAddr','Address')}
      <button class="btn ghost" onclick="goView('schemas')">Open Schemas</button></div>
    <div class="row" style="margin-top:8px"><span class="statSub">${schemaRegistry.subjects??0} subject(s)</span></div>
    ${schemaRegistry.enabled===false?`<div class="muted" style="font-size:12px;margin-top:6px">Schema registry is not available.</div>`:''}
    <div class="hr"></div>
    <pre id="srList">curl -s http://${esc(srHp.host)}:${esc(srHp.port)}/subjects</pre>
    ${copyRow('srList','Snippet')}
  </div>`;
  /* --- Kartu 4: Prometheus / Grafana --- */
  const scrapeYaml=`scrape_configs:
  - job_name: pocketkafka
    static_configs:
      - targets: ['${esc(window.location.host)}']
    metrics_path: ${esc(metrics.path||'/metrics')}`;
  const metricsCard=`<div class="card"><div class="kicker">Observability</div><h3>Prometheus &amp; Grafana</h3>
    <div class="row"><a href="/metrics" target="_blank" rel="noreferrer">/metrics</a></div>
    ${metrics.authRequired===true?`<div class="muted" style="font-size:11px;margin-top:6px">Requires a session cookie or bearer token while authentication is enabled.</div>`:''}
    <div class="hr"></div>
    <div class="kicker">Prometheus scrape config</div>
    <pre id="scrapeCfg">${esc(scrapeYaml)}</pre>
    ${copyRow('scrapeCfg','Snippet')}
    <div class="statSub" style="margin-top:8px">Import ${esc(metrics.grafanaDashboard||'-')} into Grafana.</div>
    <div class="muted" style="font-size:11px;margin-top:6px">Exported metrics</div>
    <div class="mono" style="font-size:11px">pocketkafka_messages_in_total{topic} &middot; pocketkafka_bytes_in_total{topic} &middot; pocketkafka_consumer_lag{group,topic,partition} &middot; pocketkafka_request_latency_seconds{api} &middot; pocketkafka_topic_log_end_offset{topic,partition} &middot; pocketkafka_topic_bytes{topic,partition} &middot; pocketkafka_disk_usage_ratio &middot; pocketkafka_mqtt_connections</div>
    <div class="muted" style="font-size:11px;margin-top:4px">Broker totals (pocketkafka_topics, pocketkafka_partitions, pocketkafka_total_bytes, pocketkafka_total_messages, pocketkafka_broker_uptime_seconds) are exported too; the two *_in_total counters appear once records flow.</div>
  </div>`;
  return mqttCard+restCard+srCard+metricsCard;
}
async function loadIntegrations(){
  const el=document.getElementById('integrationsBody');
  if(!el)return;
  try{
    const d=await api('/api/v1/integrations');
    state.integrations=d; state.integrationsError='';
    el.innerHTML=renderIntegrations(d);
  }catch(e){
    state.integrationsError=e.message;
    if(/404|not allowed|does not provide integrations/i.test(e.message)){
      el.innerHTML=`<div class="muted">This peer does not expose integrations.</div><div class="mono" style="margin-top:4px">${esc(e.message)}</div>`;
    }else{
      el.innerHTML=`Failed to load integrations: <span class="mono">${esc(e.message)}</span>`;
    }
  }
}
window.loadIntegrations=loadIntegrations;
function renderBrokerCfg(c){
  c=c||{};
  const r=(k,v)=>`<div class="row" style="justify-content:space-between;gap:12px;padding:6px 0;border-bottom:1px solid var(--border-soft)"><span class="muted">${esc(k)}</span><span class="mono">${esc(v==null?'-':v)}</span></div>`;
  const section=(title,rowsHtml)=>`<div class="kicker" style="margin-top:14px">${esc(title)}</div>${rowsHtml}`;
  const kv=(obj)=>Object.keys(obj||{}).length?Object.entries(obj||{}).map(([k,v])=>r(k,v)).join(''):r('(none)','-');

  const broker=c.broker||{};
  const brokerRows=r('Broker ID',broker.id)+r('Cluster ID',broker.clusterId)+r('Rack',broker.rack);

  const listenersRows=kv(c.listeners);
  const advListenersRows=kv(c.advertisedListeners);

  const storage=c.storage||{};
  const retention=storage.retention||{};
  const tiered=storage.tiered||{};
  const storageRows=r('Data dir',storage.dataDir)+r('Segment max bytes',storage.segmentMaxBytes)+r('Index interval bytes',storage.indexIntervalBytes)
    +r('Flush policy',storage.flushPolicy)+r('Flush interval (ms)',storage.flushIntervalMs)+r('Sync on acks=all',storage.syncOnAcksAll)
    +r('Retention check interval (ms)',retention.checkIntervalMs)+r('Retention hours',retention.retentionHours)+r('Retention bytes',retention.retentionBytes)
    +r('Tiered storage',tiered.enabled)+r('Tiered endpoint',tiered.endpoint)+r('Tiered bucket',tiered.bucket)+r('Tiered region',tiered.region)
    +r('Tiered prefix',tiered.prefix)+r('Offload after (hours)',tiered.offloadAfterHours)+r('Tiered check interval (ms)',tiered.checkIntervalMs);

  const topics=c.topics||{};
  const topicsRows=r('Auto-create',topics.autoCreate)+r('Default partitions',topics.defaultPartitions)+r('Default replication factor',topics.defaultReplicationFactor);

  const coordinator=c.coordinator||{};
  const coordinatorRows=r('Session timeout (ms)',coordinator.sessionTimeoutMs)+r('Rebalance timeout (ms)',coordinator.rebalanceTimeoutMs)
    +r('Heartbeat interval (ms)',coordinator.heartbeatIntervalMs)+r('Offsets retention (minutes)',coordinator.offsetsRetentionMinutes)+r('Storage backend',coordinator.storageBackend);

  const network=c.network||{};
  const networkRows=r('Max connections',network.maxConnections)+r('Read buffer bytes',network.readBufferBytes)+r('Write buffer bytes',network.writeBufferBytes)
    +r('Max request size bytes',network.maxRequestSizeBytes)+r('Read timeout (ms)',network.readTimeoutMs)+r('Write timeout (ms)',network.writeTimeoutMs)+r('Idle timeout (ms)',network.idleTimeoutMs);

  const logging=c.logging||{};
  const loggingRows=r('Level',logging.level)+r('Format',logging.format);

  const web=c.web||{};
  const webClusters=(web.clusters||[]).length
    ? web.clusters.map(cl=>r(cl&&cl.name,cl&&cl.url)).join('')
    : r('Clusters','none');
  const webRows=r('Listen',web.listen)+r('Enabled',web.enabled)+r('Auth',web.auth)+webClusters;

  const ingressRows=r('Schema registry listen',(c.schemaRegistry||{}).listen)+r('Gateway listen',(c.gateway||{}).listen)+r('MQTT listen',(c.mqtt||{}).listen);

  const security=c.security||{};
  const tls=security.tls||{};
  const users=(security.users||[]).length?security.users.map(u=>r('User',u)).join(''):r('Users','none');
  const securityRows=r('Enabled',security.enabled)+users+r('TLS enabled',tls.enabled)+r('TLS listen',tls.listen)+r('TLS cert file',tls.certFile)+r('TLS key file',tls.keyFile);

  return `<div class="row" style="justify-content:space-between;align-items:center">
      <span class="muted" style="font-size:11px">Effective values after defaults. Secrets (passwords, tokens, keys) are omitted by the server.</span>
      <button class="btn ghost" onclick="copyBrokerCfg()">Copy JSON</button>
    </div>`
    +section('Broker',brokerRows)
    +section('Listeners',listenersRows)
    +section('Advertised listeners',advListenersRows)
    +section('Storage',storageRows)
    +section('Topics',topicsRows)
    +section('Coordinator',coordinatorRows)
    +section('Network',networkRows)
    +section('Logging',loggingRows)
    +section('Web',webRows)
    +section('Ingress',ingressRows)
    +section('Security',securityRows);
}
async function loadBrokerConfig(){
  const el=document.getElementById('brokerCfgBody');
  if(!el)return;
  if(isKafkaTarget()){el.innerHTML='<span class="muted">Broker configuration is not available for Kafka targets.</span>';return;}
  if(state.brokerCfg)el.innerHTML=renderBrokerCfg(state.brokerCfg);
  try{
    const d=await api('/api/v1/config');
    state.brokerCfg=d; state.brokerCfgError='';
    el.innerHTML=renderBrokerCfg(d);
  }catch(e){
    state.brokerCfgError=e.message;
    if(/404|not allowed|not supported/i.test(e.message)){
      el.innerHTML='<span class="muted">This broker does not expose its configuration.</span><div class="mono" style="margin-top:4px">'+esc(e.message)+'</div>';
    }else{
      el.innerHTML='Failed to load configuration: <span class="mono">'+esc(e.message)+'</span>';
    }
  }
}
window.copyBrokerCfg=function(){ if(!state.brokerCfg)return; navigator.clipboard.writeText(JSON.stringify(state.brokerCfg,null,2)).then(()=>toast('Configuration copied')).catch(()=>toast('Copy failed')); };

/* Connections juga di-refresh oleh timer, jadi ia ikut di-patch di tempat
   supaya view yang sedang dilihat tidak berkedip tiap refreshMs. */
function patchConnections(){
  if(!document.getElementById('cvTopics'))return false;
  const c=state.cluster||{};
  const set=(id,txt)=>{const el=document.getElementById(id); if(el&&el.textContent!==txt)el.textContent=txt;};
  set('cvTopics',String(c.topics??'--'));
  set('cvParts',String(c.partitions??'--')+' partitions');
  set('cvBytes',fmtBytes(c.totalBytes||0));
  set('cvGroups',String(c.groups??'--'));
  if(c.diskUsagePct!=null)set('cvDisk',c.diskUsagePct.toFixed(1)+'% disk');
  set('cvClusterId',String(c.clusterId||'-'));
  set('cvEndpoint',String(c.advertised||'localhost:9092'));
  set('cvListeners',(c.listeners||['-']).join(', '));
  set('cvSecurity',String(c.security||'PLAINTEXT'));
  return true;
}

/* Health view: broker health overview (GET /api/v1/health/overview) */
let healthTimer=null,healthData=null,healthErr=null,healthWindow=60,healthOpenTopics=new Set();
let healthTopicSearch='',healthTopicPage=0,healthGroupSearch='',healthGroupPage=0;
const HEALTH_PAGE_SIZE=10;
function hStatusBadge(s){return s==='healthy'?'<span class="badge b-green">healthy</span>':s==='degraded'?'<span class="badge b-amber">degraded</span>':s==='critical'?'<span class="badge b-red">critical</span>':s==='idle'?'<span class="badge muted">idle</span>':'<span class="badge muted">'+esc(s||'-')+'</span>';}
function hTrend(t){return t==='rising'?'^ rising':t==='falling'?'v falling':'- flat';}
/* aturan kejujuran rate: rateReliable===false -> n/a, bukan 0 */
function hRate(reliable,v){return reliable===false?'n/a':(typeof v==='number'?v.toFixed(1):'0.0');}
function sortedHealthTopics(){const s=state.sort.health;const arr=((healthData&&healthData.topics)||[]).slice();if(!s)return arr;const mul=s.dir==='asc'?1:-1;
  return arr.sort((a,b)=>{let va,vb;
    if(s.f==='name'){va=a.name;vb=b.name;}
    else if(s.f==='partitions'){va=a.partitions||0;vb=b.partitions||0;}
    else if(s.f==='msgs'){va=a.messagesPerSec||0;vb=b.messagesPerSec||0;}
    else if(s.f==='bytes'){va=a.bytesPerSec||0;vb=b.bytesPerSec||0;}
    else if(s.f==='messages'){va=a.messages||0;vb=b.messages||0;}
    else if(s.f==='size'){va=a.bytes||0;vb=b.bytes||0;}
    else if(s.f==='lag'){va=a.lag||0;vb=b.lag||0;}
    else if(s.f==='lagps'){va=a.lagPerSec||0;vb=b.lagPerSec||0;}
    else {va=a.lastAppendAgeMs??Infinity;vb=b.lastAppendAgeMs??Infinity;}
    return (va>vb?1:va<vb?-1:0)*mul; }); }
/* Overview consumes only the existing /api/v1/health/overview response. */
let overviewTimer=null,overviewData=null,overviewErr=null,overviewLoading=true,overviewRequest=false,overviewRequestGeneration=null;
function overviewSpark(values,title){
  const vals=(values||[]).map(v=>Number(v)).filter(Number.isFinite);
  if(vals.length<2)return '<div class="muted" style="padding:10px 0" role="status">Not enough broker history yet to show a trend.</div>';
  if(vals.every(v=>v===0))return `<div class="muted" style="padding:10px 0" role="status">No ${esc(title)} recorded in this window.</div>`;
  const max=Math.max(1,...vals);
  return `<div class="spark" role="img" aria-label="${esc(title)}">${vals.map(v=>`<span style="height:${Math.max(3,v/max*100)}%"></span>`).join('')}</div>`;
}
function viewOverview(){
  let content;
  if(overviewErr){
    content=`<section class="card" role="alert"><h2>Overview unavailable</h2><p class="muted" style="margin-top:8px">Could not load broker health: ${esc(overviewErr)}</p><button class="btn" style="margin-top:12px" onclick="loadOverview()">Retry</button></section>`;
  }else if(overviewLoading&&!overviewData){
    content=`<div class="pageHeading"><div><h2>Overview</h2><p>Broker health, active issues, traffic, and consumer lag.</p></div></div><section class="card" aria-label="Loading broker overview" aria-busy="true">${skel(220)} <span style="margin-left:12px">${skel(140)}</span></section>`;
  }else if(!overviewData){
    content=`<section class="card"><h2>Overview unavailable</h2><p class="muted" style="margin-top:8px">No broker overview data is available yet.</p><button class="btn" style="margin-top:12px" onclick="loadOverview()">Retry</button></section>`;
  }else{
    const d=overviewData,c=d.cluster||{},s=d.sampling||{},att=Array.isArray(d.attention)?d.attention:[],hist=Array.isArray(d.history)?d.history:[];
    const win=Number.isFinite(s.windowSeconds)&&s.windowSeconds>0?s.windowSeconds:healthWindow;
    const rateReliable=s.rateReliable===true&&typeof c.messagesPerSec==='number'&&Number.isFinite(c.messagesPerSec);
    const rate=rateReliable?c.messagesPerSec.toFixed(1):'n/a';
    const count=v=>Number.isFinite(v)?v.toLocaleString():'n/a';
    const issues=att.length?att.slice(0,6).map(t=>`<div class="overviewIssue"><span class="mono">${esc(t.name||'Unnamed topic')}</span><span>${hStatusBadge(t.status)} <span class="muted">${esc((Array.isArray(t.reasons)?t.reasons:[]).map(r=>r.message).filter(Boolean).join('; ')||'Broker reported an attention state.')}</span></span></div>`).join(''):c.topics===0?'<div class="muted" style="padding:8px 0">No topics are reported yet, so there are no topic attention results.</div>':Number.isFinite(c.topics)?'<div class="muted" style="padding:8px 0">No topics currently exceed broker attention thresholds.</div>':'<div class="muted" style="padding:8px 0">Broker attention data is unavailable.</div>';
    const rateNote=!rateReliable?`<div class="statSub" style="margin-top:8px">Throughput is not yet reliable over the ${win}s sampling window; n/a is shown until a valid baseline is available.</div>`:'';
    const warmup=s.warmup===true?`<div class="statSub" style="margin-top:6px">Broker sampling is warming up (${count(s.samples)} samples, ${Number.isFinite(s.ageSeconds)?Math.max(0,Math.floor(s.ageSeconds)):0}s elapsed).</div>`:'';
    const lag=Number.isFinite(c.lag)?count(c.lag):'n/a';
    content=`
      <div id="overviewHead" class="pageHeading"><div><h2>Overview</h2><p>Broker health, active issues, traffic, and consumer lag.</p></div><div class="statSub" style="margin-left:auto">${esc(activeClusterLabel())}</div></div>
      <div id="overviewTop" class="overviewTop"><section class="card" aria-labelledby="overviewHealthTitle"><div class="kicker">Operations</div><h3 id="overviewHealthTitle">Broker health</h3><div class="row" style="margin-top:10px;justify-content:space-between"><span class="mono">${esc(c.clusterId||'Broker identity unavailable')}</span>${hStatusBadge(c.status||'unknown')}</div>${Array.isArray(c.reasons)&&c.reasons.length?`<div class="overviewAttention">${c.reasons.map(r=>`<div class="overviewIssue"><span class="mono">${esc(r.severity||'status')}</span><span class="muted">${esc(r.message||'')}</span></div>`).join('')}</div>`:''}<div class="overviewLinks"><a class="btn ghost" href="#/health">Open Health</a><a class="btn ghost" href="#/settings/clusters">View Clusters</a></div></section>
      <section class="card"><div class="row" style="justify-content:space-between"><h3>Needs attention</h3><span class="mono">${att.length} ${att.length===1?'topic':'topics'}</span></div><div class="overviewAttention">${issues}</div></section></div>
      <dl id="overviewFacts" class="overviewFacts" aria-label="Broker summary"><div class="overviewFact"><dt>Broker status</dt><dd>${esc(c.status||'n/a')}</dd></div><div class="overviewFact"><dt>Active topic issues</dt><dd>${att.length}</dd></div><div class="overviewFact"><dt>Throughput</dt><dd>${rate}<small style="font-size:10px;color:var(--muted)"> msg/s</small></dd></div><div class="overviewFact"><dt>Consumer lag</dt><dd>${lag}</dd></div></dl>
      <div id="overviewNotes">${rateNote}${warmup}</div>
      <div id="overviewCharts" class="overviewCharts" style="margin-top:12px"><section class="card"><h3 class="overviewChartTitle">Messages per second · last ${win}s</h3>${overviewSpark(hist.map(x=>x.messagesPerSec),`Messages per second over the last ${win} seconds`)}<div class="statSub">Messages / second</div></section><section class="card"><h3 class="overviewChartTitle">Total consumer lag · last ${win}s</h3>${overviewSpark(hist.map(x=>x.lag),`Total consumer lag over the last ${win} seconds`)}<div class="statSub">Messages behind latest offsets</div></section></div>
      <section id="overviewFactsCard" class="card" style="margin-top:12px"><div class="row" style="justify-content:space-between"><h3>Broker facts</h3><span class="statSub">${count(c.topics)} topics · ${count(c.partitions)} partitions · ${count(c.groups)} groups</span></div><div class="overviewLinks"><a class="btn ghost" href="#/topics">Topics</a><a class="btn ghost" href="#/groups">Consumer Groups</a><a class="btn ghost" href="#/databrowser">Data Browser</a><a class="btn ghost" href="#/settings/connections">Connections</a></div></section>`;
  }
  return `<div id="overviewRoot">${content}</div>`;
}
function patchOverview(){
  const root=document.getElementById('overviewRoot');
  if(!root)return false;
  const tmp=document.createElement('div');tmp.innerHTML=viewOverview();
  const next=tmp.firstElementChild;
  const sectionIds=['overviewHead','overviewTop','overviewFacts','overviewNotes','overviewCharts','overviewFactsCard'];
  const currentIds=sectionIds.filter(id=>document.getElementById(id));
  const nextIds=sectionIds.filter(id=>next.querySelector('#'+id));
  if(currentIds.join('|')!==nextIds.join('|')){root.innerHTML=next.innerHTML;return true;}
  sectionIds.forEach(id=>{const src=next.querySelector('#'+id),dst=document.getElementById(id);if(src&&dst&&src.innerHTML!==dst.innerHTML)dst.innerHTML=src.innerHTML;});
  return true;
}
async function loadOverview(){
  const generation=state.targetGeneration;
  if((overviewRequest&&overviewRequestGeneration===generation)||state.view!=='overview')return;
  overviewRequest=true;overviewRequestGeneration=generation;
  try{const data=await api(healthOverviewPath());if(generation!==state.targetGeneration)return;overviewData=data;overviewErr=null;}
  catch(e){if(generation!==state.targetGeneration||e.staleTargetResponse)return;overviewErr=e&&e.message?e.message:String(e);}
  finally{if(overviewRequestGeneration===generation){overviewLoading=false;overviewRequest=false;overviewRequestGeneration=null;}}
  if(generation!==state.targetGeneration||state.view!=='overview')return;
  if(!patchOverview())render();
}
window.loadOverview=loadOverview;
function startOverviewPoll(){if(overviewTimer)return;overviewLoading=!overviewData&&!overviewErr;loadOverview();if(state.refreshMs>0)overviewTimer=setInterval(loadOverview,state.refreshMs);}
function stopOverviewPoll(){if(overviewTimer){clearInterval(overviewTimer);overviewTimer=null;}}
function viewHealth(){
  if(healthErr)return `<div>${pageHeader('Health','Broker status, traffic, lag, and topic-level signals.')}<div class="card"><h3>Health</h3><div class="muted" style="margin-top:10px">Failed to load health overview: ${esc(healthErr)}</div><button class="btn" style="margin-top:12px" onclick="loadHealth()">Retry</button></div></div>`;
  if(!healthData)return `<div>${pageHeader('Health','Broker status, traffic, lag, and topic-level signals.')}<div class="card"><h3>Health</h3><div style="margin-top:10px">${skel(220)}<div style="margin-top:8px">${skel(160)}</div></div></div></div>`;
  const d=healthData,c=d.cluster||{},samp=d.sampling||{};
  const rateOk=samp.rateReliable!==false;
  const diskKnown=isNum(c.diskUsagePct);
  const msgsRate=rateOk?(c.messagesPerSec||0).toFixed(1):'n/a';
  const attention=d.attention||[];
  const topics=sortedHealthTopics();
  const groups=d.groups||[];
  const hist=d.history||[];
  const rateVals=hist.map(h=>h.messagesPerSec||0),lagVals=hist.map(h=>h.lag||0);
  const rMax=Math.max(1,...rateVals),lMax=Math.max(1,...lagVals);
  const rateSpark=rateVals.length>1?`<div class="spark" title="Messages per second, last ${samp.windowSeconds||healthWindow}s">${rateVals.map(v=>`<span style="height:${Math.max(3,v/rMax*100)}%"></span>`).join('')}</div>`:'<div class="muted" style="font-size:11px">Not enough history yet.</div>';
  const lagSpark=lagVals.length>1?`<div class="spark" title="Total consumer lag, last ${samp.windowSeconds||healthWindow}s">${lagVals.map(v=>`<span style="height:${Math.max(3,v/lMax*100)}%"></span>`).join('')}</div>`:'<div class="muted" style="font-size:11px">Not enough history yet.</div>';
  const attList=attention.length?attention.map(t=>`<div class="row" style="justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-soft)"><span class="mono">${esc(t.name)}</span>${hStatusBadge(t.status)}<span class="muted" style="font-size:11px">${esc((t.reasons||[]).map(r=>r.message).join('; ')||'no reason given')}</span></div>`).join(''):'<div class="muted" style="padding:6px 0">No topic needs attention: all topics are currently within thresholds. This list refreshes on the next poll.</div>';
  /* --- Topics with search + pagination --- */
  const tq=healthTopicSearch.toLowerCase();
  const filteredTopics=tq?topics.filter(t=>(t.name||'').toLowerCase().includes(tq)):topics;
  const tTotalPages=Math.max(1,Math.ceil(filteredTopics.length/HEALTH_PAGE_SIZE));
  const tPage=Math.min(healthTopicPage,tTotalPages-1);
  const pagedTopics=filteredTopics.slice(tPage*HEALTH_PAGE_SIZE,(tPage+1)*HEALTH_PAGE_SIZE);
  const topicRows=pagedTopics.map(t=>{
    const open=healthOpenTopics.has(t.name);
    const spk=t.sparkline||[],smax=Math.max(1,...spk);
    const mini=spk.length>1?`<div class="spark" style="height:20px">${spk.map(v=>`<span style="height:${Math.max(3,v/smax*100)}%"></span>`).join('')}</div>`:'<span class="muted">-</span>';
    const row=`<tr class="rowclick" tabindex="0" role="button" aria-expanded="${open}" onclick="toggleHealthTopic('${esc(t.name)}')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();toggleHealthTopic('${esc(t.name)}')}"><td>${hStatusBadge(t.status)}</td><td class="mono">${esc(t.name)}</td><td>${t.partitions??'-'}</td><td>${hRate(t.rateReliable,t.messagesPerSec)}</td><td>${t.rateReliable===false?'n/a':fmtRateOpt(t.bytesPerSec)}</td><td>${(t.messages||0).toLocaleString()}</td><td>${fmtBytesOpt(t.bytes)}</td><td>${t.lag??0}</td><td>${hRate(t.rateReliable,t.lagPerSec)}</td><td>${hTrend(t.trend)}</td><td>${t.lastAppendAgeMs==null?'-':rel(Date.now()-t.lastAppendAgeMs)}</td><td>${mini}</td></tr>`;
    const detail=open?`<tr><td colspan="12" style="background:var(--bg-canvas)"><div style="padding:10px 4px">
      <div class="muted" style="font-size:11px;margin-bottom:8px">Replication factor ${t.replicationFactor??'-'} &middot; skew ratio ${(t.skewRatio||0).toFixed(2)}</div>
      ${(t.reasons||[]).length?(t.reasons.map(r=>`<div class="row" style="gap:8px"><span class="chip">${esc(r.code)}</span><span class="muted" style="font-size:11px">${esc(r.severity)}: ${esc(r.message)}</span></div>`).join('')):''
      }
      <div class="tableScroll"><table><thead><tr><th>Partition</th><th>Leader</th><th>LEO</th><th>Earliest</th><th>Messages</th><th>Bytes</th><th>Lag</th><th>Msgs/s</th><th>Bytes/s</th><th>Skew</th></tr></thead><tbody>
      ${(t.partitionsDetail||[]).map(p=>`<tr><td>${p.partition}</td><td>${p.leader}</td><td>${p.logEndOffset}</td><td>${p.earliestOffset}</td><td>${p.messages}</td><td>${fmtBytesOpt(p.bytes)}</td><td>${p.lag}</td><td>${(p.messagesPerSec||0).toFixed(1)}</td><td>${fmtRateOpt(p.bytesPerSec)}</td><td>${p.skewRatio??0}</td></tr>`).join('')||'<tr><td colspan="10" class="muted">No partition detail.</td></tr>'}
      </tbody></table></div></div></td></tr>`:'';
    return row+detail;
  }).join('')||(tq?`<tr><td colspan="12" class="muted" style="padding:20px">No topics match "${esc(healthTopicSearch)}".</td></tr>`:'<tr><td colspan="12" class="muted" style="padding:20px">No topics reported by the broker yet. Create a topic, or wait for the next health poll.</td></tr>');
  const tPaginator=tTotalPages>1?`<div class="row" style="justify-content:space-between;margin-top:10px;flex-wrap:wrap;gap:6px"><span class="muted" style="font-size:11px">Showing ${tPage*HEALTH_PAGE_SIZE+1}–${Math.min((tPage+1)*HEALTH_PAGE_SIZE,filteredTopics.length)} of ${filteredTopics.length}</span><div class="row" style="gap:4px"><button class="btn ghost" style="padding:3px 10px;font-size:11px" onclick="hTopicPage(${tPage-1})" ${tPage===0?'disabled':''}>&#8249; Prev</button><button class="btn ghost" style="padding:3px 10px;font-size:11px" onclick="hTopicPage(${tPage+1})" ${tPage>=tTotalPages-1?'disabled':''}>Next &#8250;</button></div></div>`:'';
  /* --- Consumer Groups with search + pagination --- */
  const gq=healthGroupSearch.toLowerCase();
  const filteredGroups=gq?groups.filter(g=>(g.name||'').toLowerCase().includes(gq)):groups;
  const gTotalPages=Math.max(1,Math.ceil(filteredGroups.length/HEALTH_PAGE_SIZE));
  const gPage=Math.min(healthGroupPage,gTotalPages-1);
  const pagedGroups=filteredGroups.slice(gPage*HEALTH_PAGE_SIZE,(gPage+1)*HEALTH_PAGE_SIZE);
  const groupRows=pagedGroups.map(g=>`<tr><td class="mono">${esc(g.name)}</td><td><span class="badge ${online(g.state)?'b-green':'b-amber'}">${esc(g.state)}</span>${g.stalled?' <span class="badge muted">stalled</span>':''}</td><td>${g.members??0}</td><td class="mono">${(g.topics||[]).join(', ')||'-'}</td><td>${g.lag??0}</td><td>${hRate(samp.rateReliable,g.lagPerSec)}</td></tr>`).join('')||(gq?`<tr><td colspan="6" class="muted" style="padding:20px">No groups match "${esc(healthGroupSearch)}".</td></tr>`:'<tr><td colspan="6" class="muted" style="padding:20px">No consumer groups active right now. Consume from a topic to create one.</td></tr>');
  const gPaginator=gTotalPages>1?`<div class="row" style="justify-content:space-between;margin-top:10px;flex-wrap:wrap;gap:6px"><span class="muted" style="font-size:11px">Showing ${gPage*HEALTH_PAGE_SIZE+1}–${Math.min((gPage+1)*HEALTH_PAGE_SIZE,filteredGroups.length)} of ${filteredGroups.length}</span><div class="row" style="gap:4px"><button class="btn ghost" style="padding:3px 10px;font-size:11px" onclick="hGroupPage(${gPage-1})" ${gPage===0?'disabled':''}>&#8249; Prev</button><button class="btn ghost" style="padding:3px 10px;font-size:11px" onclick="hGroupPage(${gPage+1})" ${gPage>=gTotalPages-1?'disabled':''}>Next &#8250;</button></div></div>`:'';
  const warmupNote=samp.warmup?`<div class="muted" style="font-size:11px;margin-top:6px">Warmup: still collecting samples (started ${samp.ageSeconds??0}s ago). Numbers below may still change.</div>`:'';
  const rateNote=!rateOk?`<div class="muted" style="font-size:11px;margin-top:6px">Rate not reliable yet over the ${samp.windowSeconds??healthWindow}s window: the baseline may include traffic from before monitoring started, so n/a is shown instead of 0.</div>`:'';
  const extNote=c.source==='external'?`<div class="muted" style="font-size:11px;margin-top:6px">Read-only client view of an external cluster: on-disk size, byte rates, and the broker's own durability and retention state have no equivalent in the Kafka protocol, so they read n/a. Topic and group figures come from the cluster's own metadata and committed offsets.</div>`:'';
  const unknownNote=c.status==='unknown'?`<div class="muted" style="font-size:11px;margin-top:6px">First read of this cluster is still running. Figures appear once the first pass finishes.</div>`:'';
  return `<div>${pageHeader('Health','Broker status, traffic, lag, and topic-level signals.')}<div class="card">
    <div class="row" style="justify-content:space-between">
      <div><div class="kicker">Cluster &middot; ${esc(activeClusterLabel())}</div><h3 style="display:flex;align-items:center;gap:8px">${esc(c.clusterId||'broker')} <span id="hClusterStatus">${hStatusBadge(c.status)}</span></h3></div>
      <div class="row"><label class="muted" for="healthWinSel" style="font-size:11px">Window</label><select id="healthWinSel" onchange="setHealthWindow(this.value)"><option value="60" ${healthWindow==60?'selected':''}>60s</option><option value="300" ${healthWindow==300?'selected':''}>5m</option><option value="900" ${healthWindow==900?'selected':''}>15m</option></select><button class="btn ghost" onclick="loadHealth()">Refresh</button></div>
    </div>
    <span id="hNotes">${warmupNote}${rateNote}${extNote}${unknownNote}</span>
    <div class="row" style="gap:32px;margin-top:14px">
      <div><div class="kicker">Throughput</div><div class="statXL"><span id="hMsgsRate">${msgsRate}</span><small>msg/s</small></div><div class="statSub" id="hSubTop">${c.topics??0} topics &middot; ${c.partitions??0} partitions</div></div>
      <div><div class="kicker">Consumer Lag</div><div class="statXL"><span id="hLag">${c.lag??0}</span><small>total</small></div><div class="statSub" id="hSubLag">max partition lag ${c.maxPartitionLag??0}</div></div>
      <div><div class="kicker">Disk</div><div class="statXL"><span id="hDisk">${diskKnown?c.diskUsagePct.toFixed(1):'n/a'}</span><small>${diskKnown?'%':''}</small></div><div class="statSub" id="hSubDisk">${diskKnown?`uptime ${Math.floor((c.uptimeSeconds||0)/60)}m`:'not observable from a client'}</div></div>
    </div>
  </div>
  <div class="card"><div class="kicker">History</div>
    <div class="row" style="gap:24px;align-items:flex-start">
      <div style="flex:1;min-width:220px"><div class="statSub">Messages per second, last ${samp.windowSeconds||healthWindow}s</div><span id="hRateSpark">${rateSpark}</span></div>
      <div style="flex:1;min-width:220px"><div class="statSub">Total consumer lag, last ${samp.windowSeconds||healthWindow}s</div><span id="hLagSpark">${lagSpark}</span></div>
    </div>
  </div>
  <div class="card"><h3 style="margin-bottom:10px">Attention</h3><span id="hAttList">${attList}</span></div>
  <div class="card">
    <div class="row" style="justify-content:space-between;margin-bottom:10px;flex-wrap:wrap;gap:8px">
      <h3>Topics <span class="muted" style="font-size:12px;font-weight:400">(${filteredTopics.length}${tq?' matched':''})</span></h3>
      <input id="hTopicSearch" type="search" placeholder="Search topics…" value="${esc(healthTopicSearch)}" oninput="hTopicSearch(this.value)" style="width:200px;padding:5px 9px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);color:var(--text);font-size:12px;font-family:var(--font-mono)">
    </div>
    <div class="tableScroll"><table><thead><tr>
      <th>Status</th>
      <th class="sortable" onclick="sortRows('health','name')">Topic<span class="sortInd">${sortInd('health','name')}</span></th>
      <th class="sortable" onclick="sortRows('health','partitions')">Partitions<span class="sortInd">${sortInd('health','partitions')}</span></th>
      <th class="sortable" onclick="sortRows('health','msgs')">Msgs/s<span class="sortInd">${sortInd('health','msgs')}</span></th>
      <th class="sortable" onclick="sortRows('health','bytes')">Bytes/s<span class="sortInd">${sortInd('health','bytes')}</span></th>
      <th class="sortable" onclick="sortRows('health','messages')">Messages<span class="sortInd">${sortInd('health','messages')}</span></th>
      <th class="sortable" onclick="sortRows('health','size')">Size<span class="sortInd">${sortInd('health','size')}</span></th>
      <th class="sortable" onclick="sortRows('health','lag')">Lag<span class="sortInd">${sortInd('health','lag')}</span></th>
      <th class="sortable" onclick="sortRows('health','lagps')">Lag/s<span class="sortInd">${sortInd('health','lagps')}</span></th>
      <th>Trend</th>
      <th class="sortable" onclick="sortRows('health','age')">Last msg<span class="sortInd">${sortInd('health','age')}</span></th>
      <th>Series</th>
    </tr></thead><tbody id="hTopicsBody">${topicRows}</tbody></table></div>
    <span id="hTopicPager">${tPaginator}</span>
  </div>
  <div class="card">
    <div class="row" style="justify-content:space-between;margin-bottom:10px;flex-wrap:wrap;gap:8px">
      <h3>Consumer Groups <span class="muted" style="font-size:12px;font-weight:400">(${filteredGroups.length}${gq?' matched':''})</span></h3>
      <input id="hGroupSearch" type="search" placeholder="Search groups…" value="${esc(healthGroupSearch)}" oninput="hGroupSearch(this.value)" style="width:200px;padding:5px 9px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);color:var(--text);font-size:12px;font-family:var(--font-mono)">
    </div>
    <div class="tableScroll"><table><thead><tr><th>Group</th><th>State</th><th>Members</th><th>Topics</th><th>Lag</th><th>Lag/s</th></tr></thead><tbody id="hGroupsBody">${groupRows}</tbody></table></div>
    <span id="hGroupPager">${gPaginator}</span>
  </div>
</div>`;
}
/* Refresh TANPA membangun ulang view. Sebelumnya setiap poll memanggil render()
   yang mengganti seluruh #view dengan innerHTML: itulah kedipan, plus scroll dan
   fokus ikut hilang tiap refreshMs. Sekarang markup baru dibangun di node
   terlepas (tidak di-paint) lalu hanya bagian yang benar-benar berubah disalin
   ke DOM hidup, sehingga tabel, kontrol, dan posisi scroll tetap utuh. */
const HEALTH_PATCH=[['hMsgsRate','text'],['hLag','text'],['hDisk','text'],['hSubTop','text'],['hSubLag','text'],['hSubDisk','text'],
  ['hClusterStatus','html'],['hNotes','html'],['hRateSpark','html'],['hLagSpark','html'],['hAttList','html'],['hTopicsBody','html'],['hGroupsBody','html'],['hTopicPager','html'],['hGroupPager','html']];
function patchHealth(){
  if(!document.getElementById('hTopicsBody'))return false;
  const tmp=document.createElement('div'); tmp.innerHTML=viewHealth();
  HEALTH_PATCH.forEach(pair=>{
    const src=tmp.querySelector('#'+pair[0]),dst=document.getElementById(pair[0]);
    if(!src||!dst)return;
    if(pair[1]==='text'){ if(dst.textContent!==src.textContent)dst.textContent=src.textContent; }
    else if(dst.innerHTML!==src.innerHTML){ dst.innerHTML=src.innerHTML; }
  });
  return true;
}
async function loadHealth(){
  const generation=state.targetGeneration;
  try{const data=await api(healthOverviewPath());if(generation!==state.targetGeneration)return;healthData=data;healthErr=null;}
  catch(e){if(generation!==state.targetGeneration||e.staleTargetResponse)return;healthErr=e.message;}
  if(generation!==state.targetGeneration||state.view!=='health')return;
  if(!patchHealth())render();
}
window.hTopicSearch=function(v){healthTopicSearch=v;healthTopicPage=0;if(!patchHealth())render();};
window.hGroupSearch=function(v){healthGroupSearch=v;healthGroupPage=0;if(!patchHealth())render();};
window.hTopicPage=function(p){healthTopicPage=Math.max(0,p);if(!patchHealth())render();};
window.hGroupPage=function(p){healthGroupPage=Math.max(0,p);if(!patchHealth())render();};
window.loadHealth=loadHealth;
window.setHealthWindow=function(v){healthWindow=parseInt(v,10)||60;loadHealth();};
window.toggleHealthTopic=function(n){if(healthOpenTopics.has(n))healthOpenTopics.delete(n);else healthOpenTopics.add(n);render();};
/* Health polling is scoped to the currently selected global broker. */
function startHealthPoll(){if(healthTimer)return;loadHealth();if(state.refreshMs>0)healthTimer=setInterval(loadHealth,state.refreshMs);}
function stopHealthPoll(){if(healthTimer){clearInterval(healthTimer);healthTimer=null;}}

/* Multi-cluster registry and the shared Broker selector. Registry calls stay
   local; browser metadata strips stored endpoints and credential values. */
function clearTargetViewData(){
  state.cluster=null;state.topics=[];state.groups=[];state.schemas=[];state.topic=null;state.topicDetail=null;state.topicParts=[];state.topicCfg=null;
  state.group=null;state.groupDetail=null;state.groupError=null;state.lagHist={};state.msgs=[];state.fieldHint=[];state.off=null;state.leo=null;state.earliest=null;
  state.filters=[];state.diffSel=[];state.tpSamples=[];state.tpTotal=0;healthData=null;healthErr=null;overviewData=null;overviewErr=null;overviewLoading=true;healthOpenTopics.clear();
  state.integrations=null;state.integrationsError='';
  state.brokerCfg=null;state.brokerCfgError='';
}
function cancelTargetWork(){
  state.targetGeneration++;
  state.targetRequests.forEach(controller=>controller.abort());state.targetRequests.clear();
  stopOverviewPoll();stopHealthPoll();stopMsgVirtWatch();
  if(state.ws){state.ws.onclose=null;state.ws.close();state.ws=null;}
  if(state.wsRetry){clearTimeout(state.wsRetry);state.wsRetry=null;}
  state.tail=false;state.tailPaused=false;state.pendingTail=0;state.tailSkipped=0;overviewRequest=false;overviewRequestGeneration=null;
}
async function switchActiveCluster(name){
  const next=String(name||'local');
  if(next===state.activeCluster){updateBrokerPicker();return;}
  state.activeCluster=next;localStorage.setItem('pk_active_cluster',next);state.targetSwitching=true;
  cancelTargetWork();clearTargetViewData();updateBrokerPicker();
  const generation=state.targetGeneration;render();
  await refresh();
  if(generation!==state.targetGeneration)return;
  state.targetSwitching=false;render();
}
window.switchActiveCluster=switchActiveCluster;
async function loadClusterList(){
  const wasAvailable=!!activeTargetEntry();
  try{const d=await apiLocal('/api/v1/clusters');clusterRegistry=(d.clusters||[]).map(clusterMetadataForUI);setClusterRegistry(clusterRegistry);}
  catch(e){clusterRegistry=clusterRegistry||[];setClusterRegistry(clusterRegistry);updateBrokerPicker();return;}
  const available=!!activeTargetEntry();updateBrokerPicker();
  if(wasAvailable!==available){cancelTargetWork();clearTargetViewData();if(state.initialized){state.targetSwitching=true;const generation=state.targetGeneration;render();refresh().then(()=>{if(generation===state.targetGeneration){state.targetSwitching=false;render();}});}}
}
function healthOverviewPath(){return '/api/v1/health/overview?window='+healthWindow+(state.activeCluster==='local'?'&cluster=local':'');}

/* Clusters view: monitor every registered broker (GET /api/v1/health/clusters).
   Poll locally and patch only changed markup so the form keeps its typed values.
   Stored peer URLs are fetched just-in-time for an edit/test/save, not rendered. */
let clusterTimer=null,clusterData=null,clusterErr=null,editingCluster=null;

/* Argumen untuk atribut onclick: esc(JSON.stringify(x)) menjaga tanda kutip
   ganda tetap utuh sebagai atribut, dan nama yang mengandung kutip tunggal
   tetap sah sebagai string JavaScript. */
function clLabel(r){ return r.self?r.name+' (this broker)':r.name; }
function clusterMetadataForUI(row){
  const metadata=Object.assign({},row);delete metadata.url;
  if(metadata.error)metadata.error=redactTargetEndpoint(metadata.error);
  return metadata;
}

function clusterSummaryHtml(){
  if(!clusterData)return skel(180);
  const rows=clusterData.clusters||[],unreach=clusterData.unreachable||0;
  return `${rows.length} cluster(s) &middot; worst ${esc(clusterData.worst||'-')}${unreach?` &middot; <span style="color:var(--warn)">${unreach} tidak terjawab</span>`:''}`;
}

/* Dua macam target. PocketKafka peer: fan-out HTTP ke dashboard broker lain,
   token hanya bila login web peer itu aktif. Kafka cluster: klien read-only
   langsung ke broker, dengan kredensial SASL. Kedua himpunan isian selalu ada
   di DOM dan hanya yang relevan yang ditampilkan, supaya berpindah kind tidak
   menghapus apa yang sudah diketik. Konfigurasi (kind, brokers, user, ada
   sandi tersimpan atau tidak) dibaca dari registry; angka datang dari laporan
   health. */
function clusterFormCard(){
  const ed=editingCluster;
  const reg=ed?((clusterRegistry||[]).find(c=>c.name===ed.name)||null):null;
  const kafka=ed?(ed.kind||'peer')==='kafka':false;
  const vis=(on,extra)=>`style="${extra?extra+';':''}${on?'':'display:none'}"`;
  return `<div class="card"><h3 style="margin-bottom:10px">${ed?'Edit cluster':'Add cluster'}</h3>
    <form onsubmit="event.preventDefault();saveCluster();">
      <div class="row" style="gap:10px;align-items:flex-end;flex-wrap:wrap">
        <div><label class="muted" for="clKind" style="font-size:11px;display:block;margin-bottom:4px">Kind</label><select id="clKind" style="width:150px" onchange="clusterKindChanged(this.value)"><option value="peer" ${kafka?'':'selected'}>PocketKafka peer</option><option value="kafka" ${kafka?'selected':''}>Kafka cluster</option></select></div>
        <div><label class="muted" for="clName" style="font-size:11px;display:block;margin-bottom:4px">Name</label><input id="clName" style="width:150px" placeholder="scada25" autocomplete="off" value="${ed?esc(ed.name):''}" ${ed?'readonly':''}></div>
        <div id="clUrlWrap" ${vis(!kafka,'flex:1;min-width:230px')}><label class="muted" for="clUrl" style="font-size:11px;display:block;margin-bottom:4px">Dashboard URL</label><input id="clUrl" placeholder="${ed?'leave blank to keep the saved URL':'http://10.0.0.26:8080'}" autocomplete="off" value=""></div>
        <div id="clBrokersWrap" ${vis(kafka,'flex:1;min-width:230px')}><label class="muted" for="clBrokers" style="font-size:11px;display:block;margin-bottom:4px">Brokers</label><input id="clBrokers" placeholder="192.168.212.25:9092" autocomplete="off" value="${ed?esc((ed.brokers||[]).join(', ')):''}"></div>
        <div id="clSaslUserWrap" ${vis(kafka)}><label class="muted" for="clSaslUser" style="font-size:11px;display:block;margin-bottom:4px">SASL user</label><input id="clSaslUser" style="width:130px" autocomplete="off" value="${ed?esc(ed.saslUser||''):''}"></div>
        <div id="clSaslPassWrap" ${vis(kafka)}><label class="muted" for="clSaslPassword" style="font-size:11px;display:block;margin-bottom:4px">SASL password</label><input id="clSaslPassword" type="password" autocomplete="new-password" style="width:170px" placeholder="${reg&&reg.hasSaslPassword?'unchanged':'required'}"></div>
        <div id="clSaslMechWrap" ${vis(kafka)}><label class="muted" for="clSaslMechanism" style="font-size:11px;display:block;margin-bottom:4px">Mechanism</label><input id="clSaslMechanism" style="width:110px" placeholder="PLAIN" value="${ed?esc(ed.saslMechanism||''):''}"></div>
        <div id="clTokenWrap" ${vis(!kafka)}><label class="muted" for="clToken" style="font-size:11px;display:block;margin-bottom:4px">Health token (read-only monitoring)</label><input id="clToken" type="password" autocomplete="new-password" style="width:180px" placeholder="${ed&&ed.hasToken?'unchanged':'optional'}"></div>
        <div id="clDashboardTokenWrap" ${vis(!kafka,'min-width:210px')}><label class="muted" for="clDashboardToken" style="font-size:11px;display:block;margin-bottom:4px">Dashboard access token (peer mutations)</label><input id="clDashboardToken" type="password" autocomplete="new-password" style="width:210px" placeholder="${ed&&ed.hasDashboardToken?'unchanged':'paste delegated token'}"></div>
        <button class="btn" type="submit">${ed?'Save changes':'Add cluster'}</button>
        <button class="btn ghost" type="button" onclick="testCluster()">Test connection</button>
        ${ed?'<button class="btn ghost" type="button" onclick="cancelEditCluster()">Cancel</button>':''}
      </div>
      <div id="clClearWrap" ${vis(kafka,'margin-top:10px')}>${ed&&reg&&reg.hasSaslPassword?'<label class="row" style="gap:6px;font-size:11px"><input type="checkbox" id="clClearSaslPassword" style="width:auto"> remove the stored SASL password</label>':''}</div>
      ${ed?`<label class="row" id="clClearTokenWrap" ${vis(!kafka,'margin-top:10px')}><input type="checkbox" id="clClearToken" style="width:auto"> remove the stored Health token</label>`:''}
      ${ed&&reg&&reg.hasDashboardToken?`<label class="row" id="clClearDashboardTokenWrap" ${vis(!kafka,'margin-top:10px')}><input type="checkbox" id="clClearDashboardToken" style="width:auto"> remove the stored Dashboard access token</label>`:''}
    </form>
    <div class="statSub" id="clTest" style="margin-top:10px"></div>
    <div class="muted" style="font-size:11px;margin-top:8px">${kafka
      ? (ed?'Leave the SASL password empty to keep the stored one.':'SASL credentials are stored sealed and write-only, and never shown again. A Kafka cluster is always read-only.')
      : (ed?"Leave each password field empty to keep its stored credential. Health token is read-only; Dashboard access token authorizes the destination's configured principal for peer mutations.":"Health token is optional for read-only monitoring. For mutations, issue a delegated token on the destination broker and paste it here; credentials are sealed and never shown again.")} Only registered endpoints are ever connected to. Name: letters, digits, dot, underscore or dash.</div>
  </div>`;
}
/* Kind mengubah isian mana yang berlaku; isian yang tersembunyi tidak dikirim
   (clusterFormBody), jadi tidak ada nilai lama yang ikut tersimpan diam-diam. */
window.clusterKindChanged=function(v){
  const kafka=v==='kafka';
  const show=(id,on)=>{const el=document.getElementById(id); if(el)el.style.display=on?'':'none';};
  show('clUrlWrap',!kafka);show('clTokenWrap',!kafka);show('clDashboardTokenWrap',!kafka);show('clClearTokenWrap',!kafka);show('clClearDashboardTokenWrap',!kafka);
  show('clBrokersWrap',kafka);show('clSaslUserWrap',kafka);show('clSaslPassWrap',kafka);show('clSaslMechWrap',kafka);show('clClearWrap',kafka);
  const t=document.getElementById('clTest'); if(t){t.textContent='';t.style.color='';}
};

/* Baris ringkasan klaster eksternal datang dari struct angka (bukan pointer),
   jadi diskUsagePct-nya 0 dan bukan null: kolom itu harus diputuskan dari kind.
   Laporan penuh (view Health) memakai null + unavailable, di sana cukup isNum. */
function clusterRow(r){
  const kafka=r.kind==='kafka';
  const reg=kafka?((clusterRegistry||[]).find(c=>c.name===r.name)||null):null;
  const status=r.ok?hStatusBadge(r.status):'<span class="badge b-amber">unreachable</span>';
  const err=r.error?`<div class="muted" style="font-size:11px;margin-top:3px">${esc(r.error)}</div>`:'';
  const badge=r.self?'':(kafka?(reg&&reg.hasSaslPassword?' <span class="badge muted">sasl</span>':''):(r.hasToken?' <span class="badge muted">Health token</span>':''));
  const acts=r.self
    ?'<div class="clusterRowActions"><button class="btn ghost" onclick="openClusterHealth(\'local\')">Open</button></div>'
    :`<div class="clusterRowActions"><button class="btn ghost" onclick="openClusterHealth(${jsArg(r.name)})">Open</button><button class="btn ghost" onclick="editCluster(${jsArg(r.name)})">Edit</button><button class="btn ghost" onclick="deleteCluster(${jsArg(r.name)})">Remove</button></div>`;
  const n=v=>r.ok?(v??0):'-';
  const endpoint=r.self?'served by this dashboard':(kafka?'Kafka cluster':'PocketKafka peer');
  return `<tr><td class="mono">${esc(clLabel(r))}${badge}${err}<div class="muted" style="font-size:11px">${endpoint}</div></td>`
    +`<td>${status}</td><td>${n(r.topics)}</td>`
    +`<td>${r.ok?hRate(r.rateReliable,r.messagesPerSec):'-'}</td>`
    +`<td>${n(r.lag)}</td>`
    +`<td>${r.ok?(kafka?'n/a':fmtPctOpt(r.diskUsagePct)):'-'}</td>`
    +`<td>${n(r.attention)}</td>`
    +`<td class="mono" style="font-size:11px">${r.ok&&r.latencyMs>0?r.latencyMs+' ms':'-'}</td>`
    +`<td style="min-width:210px">${acts}</td></tr>`;
}

function viewClusters(){
  const head=`<div>${pageHeader('Clusters','Monitor and manage broker health across registered clusters.',`<button class="btn ghost" onclick="loadClusterList();loadClusters()">Refresh</button><span class="updatedAgo">${clusterData?'updated '+rel(clusterData.generatedAtMs):''}</span>`)}<div class="card"><div class="statSub" id="clSummary">${clusterSummaryHtml()}</div><span id="clUpdated" hidden>${clusterData?'updated '+rel(clusterData.generatedAtMs):''}</span></div></div>`;
  if(clusterErr)return head+`<div class="card"><h3 style="margin-bottom:10px">Cannot load clusters</h3><div class="muted">${esc(clusterErr)}</div><button class="btn" style="margin-top:12px" onclick="loadClusters()">Retry</button></div>`+clusterFormCard();
  if(!clusterData)return head+`<div class="card">${skel(260)}<div style="margin-top:8px">${skel(190)}</div></div>`+clusterFormCard();
  const rows=clusterData.clusters||[],peers=rows.filter(r=>!r.self);
  const note=peers.length?'':'<div class="muted" style="font-size:11px;margin-top:10px">No other cluster is monitored yet. Add one above: another PocketKafka dashboard by URL, or a Kafka cluster reachable over SASL.</div>';
  const unreach=clusterData.unreachable?`<div class="muted" style="font-size:11px;margin-top:10px">${clusterData.unreachable} cluster(s) did not answer. A broker that cannot be reached keeps its row and shows the error instead of its numbers.</div>`:'';
  return head+clusterFormCard()+`<div class="card"><h3 style="margin-bottom:10px">Monitored clusters</h3>
    <div class="tableScroll"><table><thead><tr><th>Cluster</th><th>Status</th><th>Topics</th><th>Msgs/s</th><th>Lag</th><th>Disk</th><th>Attention</th><th>Reply</th><th></th></tr></thead>
    <tbody id="clBody">${rows.map(clusterRow).join('')}</tbody></table></div>${note}${unreach}</div>`;
}

const CLUSTER_PATCH=[['clSummary','html'],['clUpdated','text'],['clBody','html']];
function patchClusters(){
  if(!document.getElementById('clBody'))return false;
  const tmp=document.createElement('div'); tmp.innerHTML=viewClusters();
  CLUSTER_PATCH.forEach(pair=>{
    const src=tmp.querySelector('#'+pair[0]),dst=document.getElementById(pair[0]);
    if(!src||!dst)return;
    if(pair[1]==='text'){ if(dst.textContent!==src.textContent)dst.textContent=src.textContent; }
    else if(dst.innerHTML!==src.innerHTML){ dst.innerHTML=src.innerHTML; }
  });
  return true;
}

async function loadClusters(){
  try{const result=await apiLocal('/api/v1/health/clusters?window='+healthWindow);clusterData=Object.assign({},result,{clusters:(result.clusters||[]).map(clusterMetadataForUI)});clusterErr=null;}
  catch(e){ clusterErr=redactTargetEndpoint(e.message); }
  const clustersActive=state.view==='clusters'||(state.view==='settings'&&state.settingsTab==='clusters');
  if(!clustersActive)return;
  if(!patchClusters())render();
}
window.loadClusters=loadClusters;
function startClusterPoll(){if(clusterTimer)return;loadClusterList();loadClusters();if(state.refreshMs>0)clusterTimer=setInterval(loadClusters,state.refreshMs);}
function stopClusterPoll(){ if(clusterTimer){clearInterval(clusterTimer);clusterTimer=null;} }

window.openClusterHealth=async function(name){const target=name||'local';state.view='health';if(location.hash!=='#/health')history.replaceState(null,'','#/health');if(target!==state.activeCluster)await switchActiveCluster(target);else{await refresh();render();}};
window.editCluster=function(name){
  const r=((clusterData&&clusterData.clusters)||[]).find(c=>c.name===name);
  if(!r)return;
  const reg=(clusterRegistry||[]).find(c=>c.name===name)||{};
  editingCluster={name:r.name,kind:r.kind||'peer',url:'',brokers:r.brokers||[],
    saslUser:reg.saslUser||'',saslMechanism:reg.saslMechanism||'',hasToken:!!r.hasToken,hasDashboardToken:!!reg.hasDashboardToken};
  render();
  const el=document.getElementById(r.kind==='kafka'?'clBrokers':'clUrl'); if(el)el.focus();
};
window.cancelEditCluster=function(){ editingCluster=null; render(); };
/* Body dari isian yang sedang berlaku. Himpunan isian kind yang tidak aktif
   tidak ikut dikirim sama sekali, supaya sandi atau token tersimpan tidak
   tertimpa nilai kosong dari field yang disembunyikan. */
async function clusterFormBody(){
  const kind=$('#clKind')?.value==='kafka'?'kafka':'peer';
  const body={kind,name:($('#clName')?.value||'').trim()};
  if(kind==='kafka'){
    body.brokers=($('#clBrokers')?.value||'').split(',').map(s=>s.trim()).filter(Boolean);
    body.saslUser=($('#clSaslUser')?.value||'').trim();
    body.saslMechanism=($('#clSaslMechanism')?.value||'').trim();
    const pw=$('#clSaslPassword')?.value||'';
    if(pw)body.saslPassword=pw;
    if($('#clClearSaslPassword')?.checked)body.clearSaslPassword=true;
  }else{
    body.url=($('#clUrl')?.value||'').trim();
    if(!body.url&&editingCluster?.kind==='peer'){
      const registry=await apiLocal('/api/v1/clusters');
      body.url=(registry.clusters||[]).find(c=>c.name===body.name)?.url||'';
    }
    const tok=$('#clToken')?.value||'';
    if(tok)body.token=tok;
    if($('#clClearToken')?.checked)body.clearToken=true;
    const dashboardToken=$('#clDashboardToken')?.value||'';
    if(dashboardToken)body.dashboardToken=dashboardToken;
    if($('#clClearDashboardToken')?.checked)body.clearDashboardToken=true;
  }
  return body;
}
/* Test memakai isian form tanpa mendaftarkan cluster; nama ikut dikirim supaya
   broker bisa memakai sandi/token tersimpan saat form mengosongkannya. */
window.testCluster=async function(){
  const out=document.getElementById('clTest');
  if(out){out.style.color='var(--muted)';out.textContent='Testing connection...';}
  try{
    const body=await clusterFormBody();
    if(body.kind==='kafka'&&!body.brokers.length){ toast('Enter at least one broker address'); return; }
    if(body.kind!=='kafka'&&!body.url){ toast('Enter the dashboard URL first'); return; }
    const r=await apiLocal('/api/v1/clusters/test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const ok=!!r.ok;
    if(out){
      out.style.color=ok?'var(--success)':'var(--danger)';
      const bits=[];
      if(r.topics>0)bits.push(r.topics+' topics');
      if(r.groups>0)bits.push(r.groups+' groups');
      if(r.latencyMs>0)bits.push(r.latencyMs+' ms');
      out.textContent=ok?`Reachable: ${r.clusterId||'cluster'}${bits.length?' - '+bits.join(', '):''}`:`Not reachable: ${redactTargetEndpoint(r.error||'unknown error')}`;
    }
    toast(ok?'Connection OK':'Connection failed');
  }catch(e){
    const message=redactTargetEndpoint(e.message);
    if(out){ out.style.color='var(--danger)'; out.textContent='Test failed: '+message; }
    toast('Error: '+message);
  }
};
window.saveCluster=async function(){
  try{
    const body=await clusterFormBody();
    if(!body.name){ toast('Name is required'); return; }
    if(body.kind==='kafka'&&!body.brokers.length){ toast('Enter at least one broker address'); return; }
    if(body.kind!=='kafka'&&!body.url){ toast('Name and URL are required'); return; }
    await apiLocal('/api/v1/clusters',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    editingCluster=null;
    await loadClusterList();await loadClusters();render();
  }catch(e){ toast('Error: '+redactTargetEndpoint(e.message)); }
};
window.deleteCluster=async function(name){
  if(!name)return;
  if(!await askConfirm({title:'Remove cluster',body:'Stop monitoring "'+name+'"? Its stored credentials are removed too.',confirmLabel:'Remove cluster',danger:true}))return;
  try{
    await apiLocal('/api/v1/clusters/'+encodeURIComponent(name),{method:'DELETE'});
    toast('Cluster removed');
    if(editingCluster&&editingCluster.name===name)editingCluster=null;
    await loadClusterList(); await loadClusters(); render();
  }catch(e){ toast('Error: '+e.message); }
};

/* ========================================================
   PocketKafka Fleet UI (Zona Host & Remote Agent Management)
   ======================================================== */

function viewLogs(){
  const s = state._brokerLogsState = state._brokerLogsState || { filter: '', level: 'all', auto: true, rawLines: [] };
  s.auto = true;
  return `<div>${pageHeader('Broker Logs','Review recent PocketKafka broker log entries with level filtering.')}
    <div class="card" style="margin-bottom:14px;padding:12px">
      <div class="row" style="gap:10px;justify-content:space-between;flex-wrap:wrap">
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <label for="brokerLogLvl" class="muted" style="font-size:11px">Level</label>
          <select id="brokerLogLvl" style="width:auto" onchange="filterBrokerLogLevel(this.value)">
            <option value="all" ${s.level==='all'?'selected':''}>All Levels</option>
            <option value="error" ${s.level==='error'?'selected':''}>ERROR</option>
            <option value="warn" ${s.level==='warn'?'selected':''}>WARN</option>
            <option value="info" ${s.level==='info'?'selected':''}>INFO</option>
            <option value="debug" ${s.level==='debug'?'selected':''}>DEBUG</option>
          </select>
          <input id="brokerLogQ" placeholder="Filter logs..." value="${esc(s.filter||'')}" oninput="filterBrokerLogs(this.value)" style="max-width:240px" aria-label="Search broker logs">
        </div>
        <div style="display:flex;gap:8px;align-items:center">
          <button id="brokerLogAutoBtn" class="btn ghost ${s.auto?'ok':''}" onclick="toggleBrokerLogsAuto()" title="${s.auto?'Pause live auto-refresh (3s)':'Start live auto-refresh every 3s'}">${s.auto?'⏸ Auto (3s)':'▶ Auto (3s)'}</button>
          <button class="btn ghost" onclick="copyBrokerLogs(this)">Copy</button>
          <button class="btn ghost" onclick="loadLogs()">Refresh</button>
        </div>
      </div>
    </div>
    <div class="card" style="padding:0">
      <div style="padding:8px 14px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;font:11px/1.8 var(--font-mono);color:var(--muted)">
        <span id="brokerLogMeta">Loading recent entries...</span>
        <span>Ring buffer: last 500 lines</span>
      </div>
      <div id="logOut" style="max-height:65vh;overflow:auto;padding:8px 0;background:var(--bg-canvas)">${skel(120)}</div>
    </div>
  </div>`;
}

async function loadLogs(silent){
  const out = document.getElementById('logOut');
  if(!out) return;
  const s = state._brokerLogsState = state._brokerLogsState || { filter: '', level: 'all', auto: true, rawLines: [] };
  if(s.auto && !s.timer){
    s.timer = setInterval(async () => {
      if(state.view !== 'logs' || !state._brokerLogsState || !state._brokerLogsState.auto){
        if(s.timer) { clearInterval(s.timer); s.timer = null; }
        return;
      }
      await loadLogs(true);
    }, 3000);
  }
  try{
    const lines = await api('/api/v1/logs');
    s.rawLines = lines || [];
    renderBrokerLogsBody(silent);
  }catch(e){
    if(!silent) out.innerHTML = `<div class="muted" style="padding:16px;color:var(--danger)">Failed to fetch broker logs: ${esc(e.message)}</div>`;
  }
}
window.loadLogs = loadLogs;

function filterBrokerLogLevel(lvl){
  if(!state._brokerLogsState) return;
  state._brokerLogsState.level = lvl || 'all';
  renderBrokerLogsBody(true);
}
window.filterBrokerLogLevel = filterBrokerLogLevel;

function filterBrokerLogs(val){
  if(!state._brokerLogsState) return;
  state._brokerLogsState.filter = String(val || '');
  renderBrokerLogsBody(true);
}
window.filterBrokerLogs = filterBrokerLogs;

function copyBrokerLogs(btn){
  const s = state._brokerLogsState;
  if(!s) return;
  const text = (s.rawLines || []).join('\n');
  copyText2(text, btn);
}
window.copyBrokerLogs = copyBrokerLogs;

function toggleBrokerLogsAuto(){
  const s = state._brokerLogsState = state._brokerLogsState || { filter: '', level: 'all', auto: false, rawLines: [] };
  s.auto = !s.auto;
  if(s.timer){ clearInterval(s.timer); s.timer = null; }
  if(s.auto){
    s.timer = setInterval(async () => {
      if(state.view !== 'logs' || !state._brokerLogsState || !state._brokerLogsState.auto){
        if(s.timer) clearInterval(s.timer);
        return;
      }
      await loadLogs(true);
    }, 3000);
  }
  const btn = document.getElementById('brokerLogAutoBtn');
  if(btn){
    btn.textContent = s.auto ? '⏸ Auto (3s)' : '▶ Auto (3s)';
    btn.classList.toggle('ok', !!s.auto);
  }
}
window.toggleBrokerLogsAuto = toggleBrokerLogsAuto;

function renderBrokerLogsBody(keepScroll){
  const s = state._brokerLogsState;
  if(!s) return;
  const out = document.getElementById('logOut');
  const meta = document.getElementById('brokerLogMeta');
  if(!out) return;

  const rawEntries = s.rawLines || [];
  const q = (s.filter || '').toLowerCase();
  const targetLevel = s.level || 'all';

  const filtered = rawEntries.filter(raw => {
    const parsed = parseLogLine(raw);
    if(!matchLogLevel(parsed, raw, targetLevel)) return false;
    if(q && !String(raw).toLowerCase().includes(q)) return false;
    return true;
  });

  const levelTag = targetLevel !== 'all' ? ` [${targetLevel.toUpperCase()}]` : '';
  const countLabel = (q || targetLevel !== 'all') ? `${filtered.length} of ${rawEntries.length} lines${levelTag}` : `Last ${rawEntries.length} lines`;

  const wasAtBottom = (out.scrollHeight - out.scrollTop - out.clientHeight) < 60;
  if(meta) meta.textContent = countLabel;
  out.innerHTML = filtered.length ? renderLogLines(filtered) : `<div class="muted" style="padding:16px;text-align:center">${(q || targetLevel !== 'all') ? 'No broker logs match filter criteria.' : 'Log buffer is empty.'}</div>`;

  if(!keepScroll || wasAtBottom){
    out.scrollTop = out.scrollHeight;
  }
}
/* Command palette (Ctrl+K / '/') */
function palItems(){
  const items=navItems.map(([k,label])=>({label,run:()=>goView(k),hint:'view'}));
  items.push({label:'Clusters',run:()=>goView('clusters'),hint:'settings'});
  items.push({label:'Connections',run:()=>goView('connections'),hint:'settings'});
  items.push({label:'Security & ACL',run:()=>goView('security'),hint:'settings'});
  if(state.fleetEnabled){
    items.push({label:'Fleet: Servers',run:()=>goView('fleet-servers'),hint:'fleet'});
    items.push({label:'Fleet: Containers',run:()=>goView('fleet-containers'),hint:'fleet'});
    items.push({label:'Fleet: Jobs',run:()=>goView('fleet-jobs'),hint:'fleet'});
    items.push({label:'Fleet: Audit Logs',run:()=>goView('fleet-audit'),hint:'fleet'});
    items.push({label:'Fleet: Settings',run:()=>goView('fleet-settings'),hint:'fleet'});
  }
  (state.topics||[]).forEach(t=>items.push({label:t.name,run:()=>{state.topic=t.name;goView('databrowser').then(()=>loadMsgs());},hint:'topic'}));
  (state.groups||[]).forEach(g=>items.push({label:g.name,run:()=>{goView('groups').then(()=>openGroup(g.name));},hint:'group'}));
  return items;
}
window.openPalette=function(){
  if(document.querySelector('.overlay.palette'))return;
  const el=document.createElement('div'); el.className='overlay palette';
  el.innerHTML='<div class="paletteBox"><input id="palQ" placeholder="Ketik view, topic, atau group..." autocomplete="off"></div>';
  document.body.appendChild(el);
  el.addEventListener('mousedown',e=>{if(e.target===el)closeOverlay();});
  const inp=el.querySelector('#palQ'); let sel=0;
  const renderList=()=>{ const q=inp.value.toLowerCase(); const items=palItems().filter(i=>i.label.toLowerCase().includes(q)).slice(0,12);
    el._items=items; el.querySelector('.palList')?.remove();
    const list=document.createElement('div'); list.className='palList';
    items.forEach((it,idx)=>{ const d=document.createElement('div'); d.className='palItem'+(idx===sel?' sel':'');
      d.innerHTML='<span>'+esc(it.label)+'</span><span class="palHint">'+it.hint+'</span>';
      d.onclick=()=>{closeOverlay();it.run();}; list.appendChild(d); });
    el.querySelector('.paletteBox').appendChild(list); };
  inp.oninput=()=>{sel=0;renderList();};
  inp.onkeydown=e=>{ const items=el._items||[];
    if(e.key==='ArrowDown'){sel=Math.min(sel+1,items.length-1);renderList();e.preventDefault();}
    else if(e.key==='ArrowUp'){sel=Math.max(sel-1,0);renderList();e.preventDefault();}
    else if(e.key==='Enter'){if(items[sel]){closeOverlay();items[sel].run();}}
    else if(e.key==='Escape'){closeOverlay();} };
  renderList(); inp.focus();
};
if(!window._palBound){ window._palBound=1;
  document.addEventListener('keydown',e=>{
    if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();openPalette();}
    else if(e.key==='/'&&!/input|textarea|select/i.test(document.activeElement?.tagName||'')){e.preventDefault();openPalette();}
    else if(e.key==='Escape'){closeDrawer();}
  });
}

/* Snippet generators (curl via REST proxy + sarama consumer) */
window.copyCurl=function(topic){ if(!topic){toast('Select a topic first');return;}
  const cmd='curl -X POST http://localhost:8082/topics/'+encodeURIComponent(topic)+'/messages -H "Content-Type: application/json" -d \'{"key":"k1","value":"hello"}\'';
  navigator.clipboard.writeText(cmd).then(()=>toast('curl copied')); };
window.copyConsumer=function(topic){ if(!topic){toast('Select a topic first');return;}
  const code='// Go + sarama (protocol 0.11)\nconfig := sarama.NewConfig()\nconfig.Version = sarama.V0_11_0_0\ncg, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "my-group", config)\nif err != nil { panic(err) }\n// consume topic: '+topic+'\nfor {\n\tif err := cg.Consume(ctx, []string{"'+topic+'"}, handler); err != nil { break }\n}';
  navigator.clipboard.writeText(code).then(()=>toast('consumer snippet copied')); };

/* JSONL import (kebalikan export) */
window.doImportFile=async function(inp){ const f=inp.files&&inp.files[0]; inp.value='';
  if(!f)return; if(!state.topic){toast('Select a topic first');return;}
  try{ const text=await f.text();
    const r=await api('/api/v1/topics/'+encodeURIComponent(state.topic)+'/import',{method:'POST',headers:{'Content-Type':'application/x-ndjson'},body:text});
    toast('Imported '+r.imported+' messages'); loadMsgs();
  }catch(e){toast(e.message);} };

/* Group offset snapshot */
window.exportGroupOffsets=async function(g){ try{ const d=await api('/api/v1/groups/'+encodeURIComponent(g)+'/offsets/export');
    const blob=new Blob([JSON.stringify(d,null,2)],{type:'application/json'}); const a=document.createElement('a');
    a.href=URL.createObjectURL(blob); a.download=g+'-offsets.json'; a.click(); toast('Snapshot diunduh');
  }catch(e){toast(e.message);} };
window.importGroupOffsets=async function(g){ const inp=document.createElement('input'); inp.type='file'; inp.accept='.json';
  inp.onchange=async()=>{ const f=inp.files[0]; if(!f)return;
    try{ const j=JSON.parse(await f.text());
      const r=await api('/api/v1/groups/'+encodeURIComponent(g)+'/offsets/import',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({offsets:j.offsets||j})});
      toast('Applied '+r.applied+' offsets'); loadGroupDetail();
    }catch(e){toast(e.message);} };
  inp.click(); };

/* Schema register */
window.registerSchema=async function(){ const s=document.getElementById('nsSub').value, sc=document.getElementById('nsSchema').value, t=document.getElementById('nsType').value;
  if(!s||!sc){toast('subject and schema required');return;}
  try{ await api('/api/v1/schemas/register',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subject:s,schema:sc,schemaType:t})});
    toast('Schema registered'); await refresh(); render();
  }catch(e){toast(e.message);} };

/* Topic config (cleanup.policy) */
window.toggleTopicCompact=async function(){ const next=!(state.topicCfg&&state.topicCfg.compacted);
  try{ await api(`/api/v1/topics/${encodeURIComponent(state.topicDetail)}/config`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({compacted:next})});
    state.topicCfg={compacted:next}; render();
  }catch(e){toast(e.message);} };

/* Live tail: pause on scroll-up */
/* Jumlah pesan terbaru yang ditahan di view selama tail. DOM dijaga sepadan
   dengan angka ini: sebelum ini baris lama tidak pernah dibuang sehingga DOM
   tumbuh tanpa batas selama tail menyala, dan itulah yang membuat halaman
   berat saat broker menerima data terus-menerus. */
const MSGS_MAX=5000;
function updateTailBar(){ const bar=document.getElementById('tailBar'); if(!bar)return;
  if(state.tailPaused){ bar.innerHTML=`<div class="tailPausedBar" onclick="resumeTail()"><b>${state.pendingTail}</b> pesan baru ditahan - klik untuk ikut lagi</div>`; return; }
  if(state.tailSkipped){ bar.innerHTML=`<div class="tailPausedBar" style="cursor:default"><b>${state.tailSkipped}</b> pesan dilewati: broker menerima data lebih cepat daripada yang bisa ditampilkan. Pause atau matikan tail untuk menelusuri riwayat.</div>`; return; }
  bar.innerHTML=''; }
window.resumeTail=function(){ state.tailPaused=false; state.pendingTail=0; state.tailSkipped=0; updateTailBar(); renderMsgRows(); const mainEl=$('main'); if(mainEl)mainEl.scrollTop=0; };
function bindTailPause(){ const mainEl=$('main'); if(!mainEl||mainEl._tailBind)return; mainEl._tailBind=1;
  mainEl.addEventListener('scroll',()=>{ if(state.view==='databrowser'&&state.tail&&!state.tailPaused&&mainEl.scrollTop>40){ state.tailPaused=true; updateTailBar(); } }); }




// Window bindings
window.navUnavailableReason = navUnavailableReason;
window.targetUnavailablePanel = targetUnavailablePanel;
window.unsupportedPanel = unsupportedPanel;
window.targetSwitchingPanel = targetSwitchingPanel;
window.topoData = topoData;
window.topoLayout = topoLayout;
window.renderTopoSvg = renderTopoSvg;
window.topoDetailHtml = topoDetailHtml;
window.topoLayer = topoLayer;
window.viewTopology = viewTopology;
window.viewLogin = viewLogin;
window.bindLogin = bindLogin;
window.viewGeneralSettings = viewGeneralSettings;
window.viewSettings = viewSettings;
window.viewConnections = viewConnections;
window.viewDataBrowser = viewDataBrowser;
window.loadMsgs = loadMsgs;
window.prevPage = prevPage;
window.nextPage = nextPage;
window.filteredMsgs = filteredMsgs;
window.passesFilters = passesFilters;
window.rowHtml = rowHtml;
window.replayCell = replayCell;
window.rowTr = rowTr;
window.renderMsgRows = renderMsgRows;
window.startMsgVirtWatch = startMsgVirtWatch;
window.stopMsgVirtWatch = stopMsgVirtWatch;
window.copyInspTo = copyInspTo;
window.fmtValue = fmtValue;
window.hex = hex;
window.jsonOf = jsonOf;
window.pathKeys = pathKeys;
window.refreshFieldHints = refreshFieldHints;
window.filterPanelHtml = filterPanelHtml;
window.getPath = getPath;
window.evalFilter = evalFilter;
window.projBarHtml = projBarHtml;
window.byOff = byOff;
window.replayModal = replayModal;
window.seek = seek;
window.loadPosMeta = loadPosMeta;
window.updatePosInfo = updatePosInfo;
window.closeTailSocket = closeTailSocket;
window.toggleTail = toggleTail;
window.connectTail = connectTail;
window.prependMsgRow = prependMsgRow;
window.viewTopics = viewTopics;
window.loadTopicDetail = loadTopicDetail;
window.viewTopicDetail = viewTopicDetail;
window.createTopic = createTopic;
window.delTopic = delTopic;
window.viewGroups = viewGroups;
window.loadGroupDetail = loadGroupDetail;
window.viewGroupDetail = viewGroupDetail;
window.viewProducer = viewProducer;
window.updateProducerPartitions = updateProducerPartitions;
window.sendMsg = sendMsg;
window.viewDevStudio = viewDevStudio;
window.cap = cap;
window.tsType = tsType;
window.goType = goType;
window.pyType = pyType;
window.objToCode = objToCode;
window.mockPayload = mockPayload;
window.viewStorage = viewStorage;
window.viewSchemas = viewSchemas;
window.loadSchemaDetail = loadSchemaDetail;
window.fmtSchema = fmtSchema;
window.delegatedTokenCard = delegatedTokenCard;
window.viewSecurity = viewSecurity;
window.loadACLs = loadACLs;
window.loadAudit = loadAudit;
window.loadDelegatedTokens = loadDelegatedTokens;
window.timeCell = timeCell;
window.sortedTopics = sortedTopics;
window.sortedGroups = sortedGroups;
window.viewMetrics = viewMetrics;
window.loadThroughput = loadThroughput;
window.viewIntegrations = viewIntegrations;
window.splitHostPort = splitHostPort;
window.copyRow = copyRow;
window.renderIntegrations = renderIntegrations;
window.loadIntegrations = loadIntegrations;
window.renderBrokerCfg = renderBrokerCfg;
window.loadBrokerConfig = loadBrokerConfig;
window.patchConnections = patchConnections;
window.hStatusBadge = hStatusBadge;
window.hTrend = hTrend;
window.hRate = hRate;
window.sortedHealthTopics = sortedHealthTopics;
window.overviewSpark = overviewSpark;
window.viewOverview = viewOverview;
window.patchOverview = patchOverview;
window.loadOverview = loadOverview;
window.startOverviewPoll = startOverviewPoll;
window.stopOverviewPoll = stopOverviewPoll;
window.viewHealth = viewHealth;
window.patchHealth = patchHealth;
window.loadHealth = loadHealth;
window.startHealthPoll = startHealthPoll;
window.stopHealthPoll = stopHealthPoll;
window.clearTargetViewData = clearTargetViewData;
window.cancelTargetWork = cancelTargetWork;
window.switchActiveCluster = switchActiveCluster;
window.loadClusterList = loadClusterList;
window.healthOverviewPath = healthOverviewPath;
window.clLabel = clLabel;
window.clusterMetadataForUI = clusterMetadataForUI;
window.clusterSummaryHtml = clusterSummaryHtml;
window.clusterFormCard = clusterFormCard;
window.clusterRow = clusterRow;
window.viewClusters = viewClusters;
window.patchClusters = patchClusters;
window.loadClusters = loadClusters;
window.startClusterPoll = startClusterPoll;
window.stopClusterPoll = stopClusterPoll;
window.clusterFormBody = clusterFormBody;
window.viewLogs = viewLogs;
window.loadLogs = loadLogs;
window.filterBrokerLogLevel = filterBrokerLogLevel;
window.filterBrokerLogs = filterBrokerLogs;
window.copyBrokerLogs = copyBrokerLogs;
window.toggleBrokerLogsAuto = toggleBrokerLogsAuto;
window.renderBrokerLogsBody = renderBrokerLogsBody;
window.palItems = palItems;
window.updateTailBar = updateTailBar;
window.bindTailPause = bindTailPause;


const topoPickTopic = (...args) => window.topoPickTopic?.(...args);
const topoPickGroup = (...args) => window.topoPickGroup?.(...args);
const refreshTopo = (...args) => window.refreshTopo?.(...args);
const showLogin = (...args) => window.showLogin?.(...args);
const doLogin = (...args) => window.doLogin?.(...args);
const logout = (...args) => window.logout?.(...args);
const inspOffset = (...args) => window.inspOffset?.(...args);
const toggleNode = (...args) => window.toggleNode?.(...args);
const insp = (...args) => window.insp?.(...args);
const closeInspector = (...args) => window.closeInspector?.(...args);
const copyInspJson = (...args) => window.copyInspJson?.(...args);
const copyInspValue = (...args) => window.copyInspValue?.(...args);
const applyFiltersNow = (...args) => window.applyFiltersNow?.(...args);
const clearAllFilters = (...args) => window.clearAllFilters?.(...args);
const toggleFilterPanel = (...args) => window.toggleFilterPanel?.(...args);
const addFilter = (...args) => window.addFilter?.(...args);
const delFilter = (...args) => window.delFilter?.(...args);
const acField = (...args) => window.acField?.(...args);
const pickField = (...args) => window.pickField?.(...args);
const savePreset = (...args) => window.savePreset?.(...args);
const applyPreset = (...args) => window.applyPreset?.(...args);
const delPreset = (...args) => window.delPreset?.(...args);
const openProj = (...args) => window.openProj?.(...args);
const projPickAdd = (...args) => window.projPickAdd?.(...args);
const removeProj = (...args) => window.removeProj?.(...args);
const toggleDiffSel = (...args) => window.toggleDiffSel?.(...args);
const runDiff = (...args) => window.runDiff?.(...args);
const exportMsgs = (...args) => window.exportMsgs?.(...args);
const replayOne = (...args) => window.replayOne?.(...args);
const replaySelected = (...args) => window.replaySelected?.(...args);
const closeOverlay = (...args) => window.closeOverlay?.(...args);
const askConfirm = (...args) => window.askConfirm?.(...args);
const doReplay = (...args) => window.doReplay?.(...args);
const seekOffsetGo = (...args) => window.seekOffsetGo?.(...args);
const setPart = (...args) => window.setPart?.(...args);
const openTopic = (...args) => window.openTopic?.(...args);
const openTopicDetail = (...args) => window.openTopicDetail?.(...args);
const truncateTopic = (...args) => window.truncateTopic?.(...args);
const compactTopic = (...args) => window.compactTopic?.(...args);
const openGroup = (...args) => window.openGroup?.(...args);
const resetGroupOffset = (...args) => window.resetGroupOffset?.(...args);
const delGroup = (...args) => window.delGroup?.(...args);
const genCode = (...args) => window.genCode?.(...args);
const mockGenStart = (...args) => window.mockGenStart?.(...args);
const mockGenStop = (...args) => window.mockGenStop?.(...args);
const rpcSend = (...args) => window.rpcSend?.(...args);
const loadSegments = (...args) => window.loadSegments?.(...args);
const ttJump = (...args) => window.ttJump?.(...args);
const schemaDiff = (...args) => window.schemaDiff?.(...args);
const addACLRow = (...args) => window.addACLRow?.(...args);
const aclsave = (...args) => window.aclsave?.(...args);
const toggleOp = (...args) => window.toggleOp?.(...args);
const delACL = (...args) => window.delACL?.(...args);
const refreshSecurity = (...args) => window.refreshSecurity?.(...args);
const issueDelegatedToken = (...args) => window.issueDelegatedToken?.(...args);
const copyIssuedDelegatedToken = (...args) => window.copyIssuedDelegatedToken?.(...args);
const revokeDelegatedToken = (...args) => window.revokeDelegatedToken?.(...args);
const sortInd = (...args) => window.sortInd?.(...args);
const sortRows = (...args) => window.sortRows?.(...args);
const copyBrokerCfg = (...args) => window.copyBrokerCfg?.(...args);
const hTopicSearch = (...args) => window.hTopicSearch?.(...args);
const hGroupSearch = (...args) => window.hGroupSearch?.(...args);
const hTopicPage = (...args) => window.hTopicPage?.(...args);
const hGroupPage = (...args) => window.hGroupPage?.(...args);
const setHealthWindow = (...args) => window.setHealthWindow?.(...args);
const toggleHealthTopic = (...args) => window.toggleHealthTopic?.(...args);
const openClusterHealth = (...args) => window.openClusterHealth?.(...args);
const editCluster = (...args) => window.editCluster?.(...args);
const cancelEditCluster = (...args) => window.cancelEditCluster?.(...args);
const clusterKindChanged = (...args) => window.clusterKindChanged?.(...args);
const testCluster = (...args) => window.testCluster?.(...args);
const saveCluster = (...args) => window.saveCluster?.(...args);
const deleteCluster = (...args) => window.deleteCluster?.(...args);
const openPalette = (...args) => window.openPalette?.(...args);
const copyCurl = (...args) => window.copyCurl?.(...args);
const copyConsumer = (...args) => window.copyConsumer?.(...args);
const doImportFile = (...args) => window.doImportFile?.(...args);
const exportGroupOffsets = (...args) => window.exportGroupOffsets?.(...args);
const importGroupOffsets = (...args) => window.importGroupOffsets?.(...args);
const registerSchema = (...args) => window.registerSchema?.(...args);
const toggleTopicCompact = (...args) => window.toggleTopicCompact?.(...args);
const resumeTail = (...args) => window.resumeTail?.(...args);

export {
  KAFKA_UNSUPPORTED_VIEWS, navUnavailableReason, targetUnavailablePanel, unsupportedPanel, targetSwitchingPanel, topoData, topoLayout, renderTopoSvg, topoDetailHtml, topoPickTopic, topoPickGroup, topoLayer, viewTopology, refreshTopo, viewLogin, bindLogin, showLogin, doLogin, logout, viewGeneralSettings, viewSettings, viewConnections, viewDataBrowser, loadMsgs, prevPage, nextPage, filteredMsgs, passesFilters, rowHtml, replayCell, rowTr, renderMsgRows, startMsgVirtWatch, stopMsgVirtWatch, inspOffset, toggleNode, insp, closeInspector, copyInspJson, copyInspValue, copyInsp, applyFiltersNow, clearAllFilters, toggleFilterPanel, addFilter, delFilter, acField, pickField, savePreset, applyPreset, delPreset, openProj, projPickAdd, removeProj, toggleDiffSel, runDiff, exportMsgs, replayOne, replaySelected, closeOverlay, askConfirm, doReplay, seek, seekOffsetGo, loadPosMeta, updatePosInfo, closeTailSocket, toggleTail, connectTail, prependMsgRow, setPart, viewTopics, openTopic, openTopicDetail, loadTopicDetail, viewTopicDetail, truncateTopic, compactTopic, createTopic, delTopic, viewGroups, openGroup, loadGroupDetail, viewGroupDetail, resetGroupOffset, delGroup, viewProducer, updateProducerPartitions, sendMsg, viewDevStudio, genCode, mockGenStart, mockGenStop, rpcSend, viewStorage, loadSegments, ttJump, viewSchemas, loadSchemaDetail, schemaDiff, fmtSchema, delegatedTokenCard, viewSecurity, loadACLs, addACLRow, aclsave, toggleOp, delACL, loadAudit, refreshSecurity, loadDelegatedTokens, issueDelegatedToken, copyIssuedDelegatedToken, revokeDelegatedToken, timeCell, sortInd, sortRows, sortedTopics, sortedGroups, viewMetrics, loadThroughput, viewIntegrations, loadIntegrations, loadBrokerConfig, copyBrokerCfg, patchConnections, hStatusBadge, hTrend, hRate, sortedHealthTopics, viewOverview, patchOverview, loadOverview, startOverviewPoll, stopOverviewPoll, viewHealth, patchHealth, loadHealth, hTopicSearch, hGroupSearch, hTopicPage, hGroupPage, setHealthWindow, toggleHealthTopic, startHealthPoll, stopHealthPoll, clearTargetViewData, cancelTargetWork, switchActiveCluster, loadClusterList, healthOverviewPath, viewClusters, patchClusters, loadClusters, startClusterPoll, stopClusterPoll, openClusterHealth, editCluster, cancelEditCluster, clusterKindChanged, clusterFormBody, testCluster, saveCluster, deleteCluster, viewLogs, loadLogs, filterBrokerLogLevel, filterBrokerLogs, copyBrokerLogs, toggleBrokerLogsAuto, renderBrokerLogsBody, palItems, openPalette, copyCurl, copyConsumer, doImportFile, exportGroupOffsets, importGroupOffsets, registerSchema, toggleTopicCompact, updateTailBar, resumeTail, bindTailPause
};
