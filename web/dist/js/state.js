// PocketKafka State & Helpers
const render = (...args) => window.render?.(...args);
const viewSettings = (...args) => window.viewSettings?.(...args);
const bindView = (...args) => window.bindView?.(...args);


const $=s=>document.querySelector(s);
const I={
  overview:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 13a9 9 0 0 1 18 0"/><path d="M12 13l4-4"/><path d="M5 19h14"/></svg>',
  server:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="6"/><rect x="3" y="14" width="18" height="6"/><path d="M7 7h.01M7 17h.01M11 7h6M11 17h6"/></svg>',
  pulse:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M2 12h4l3-7 5 14 3-7h5"/></svg>',
  cluster:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="3" width="6" height="5"/><rect x="3" y="16" width="6" height="5"/><rect x="15" y="16" width="6" height="5"/><path d="M12 8v4M6 16v-4h12v4"/></svg>',
  chart:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="M7 14l4-4 4 3 5-7"/></svg>',
  document:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 3h9l4 4v14H6z"/><path d="M14 3v5h5M9 13h7M9 17h7"/></svg>',
  shield:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3l8 3v5c0 5-3.5 8-8 10-4.5-2-8-5-8-10V6z"/><path d="M9 12l2 2 4-4"/></svg>',
  search:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5M8 10h5M10.5 8v5"/></svg>',
  tag:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 12V4h8l10 10-7 7z"/><circle cx="7.5" cy="8" r="1"/></svg>',
  users:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="9" cy="8" r="3"/><path d="M3 20v-2a6 6 0 0 1 12 0v2zM17 5a3 3 0 0 1 0 6M18 14a5 5 0 0 1 3 4v2"/></svg>',
  schema:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 4H5v16h3M16 4h3v16h-3M10 8h4M10 12h4M10 16h4"/></svg>',
  send:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 2 11 13"/><path d="m22 2-7 20-4-9-9-4z"/></svg>',
  flask:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 3h6M10 3v7l-5 9a1.5 1.5 0 0 0 1.3 2h11.4a1.5 1.5 0 0 0 1.3-2l-5-9V3M8 16h8"/></svg>',
  topology:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="5" r="2"/><circle cx="5" cy="19" r="2"/><circle cx="19" cy="19" r="2"/><path d="M12 7v4M10.5 13 6.5 17M13.5 13l4 4M8 11h8"/></svg>',
  archive:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16v5H4zM6 9v11h12V9M10 13h4"/></svg>',
  gear:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="m19 13 2 1-2 4-2-1a8 8 0 0 1-2 1l-.3 2h-5L9 18a8 8 0 0 1-2-1l-2 1-2-4 2-1v-2l-2-1 2-4 2 1a8 8 0 0 1 2-1l.3-2h5L15 6a8 8 0 0 1 2 1l2-1 2 4-2 1z"/></svg>',
  sun:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.9 4.9l1.4 1.4m11.4 11.4 1.4 1.4M2 12h2m16 0h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>',
  moon:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.8A9 9 0 1 1 11.2 3 7 7 0 0 0 21 12.8z"/></svg>',
  box:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>',
  terminal:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>',
  mqtt:'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="2"/><path d="M12 2C6.48 2 2 6.48 2 12"/><path d="M12 6a6 6 0 0 1 6 6"/><path d="M12 10a2 2 0 0 1 2 2"/></svg>'
};

const state={zone:'kafka',fleetEnabled:false,fleetStatus:null,fleetServers:[],fleetContainers:[],fleetJobs:[],fleetAudit:[],fleetEnrollments:[],fleetServerFilter:'',fleetStateFilter:'all',fleetSearch:'',fleetJobStatusFilter:'all',fleetAuditSearch:'',view:'overview',settingsTab:'general',theme:'dark',sidebarCollapsed:false,cluster:null,topics:[],groups:[],schemas:[],topic:null,partition:'all',decoder:'auto',msgs:[],tail:false,ws:null,group:null,search:'',fromTs:'',toTs:'',auth:null,activeCluster:localStorage.getItem('pk_active_cluster')||'local',targetGeneration:0,targetRequests:new Set(),targetSwitching:false,groupLagWarn:Number(localStorage.getItem('pk_group_lag_warn'))||1000000000,alarmsDismissed:new Set((()=>{try{return JSON.parse(localStorage.getItem('pk_alarms_dismissed')||'[]');}catch(e){return [];}})()),
  integrations:null, integrationsError:'',
  brokerCfg:null, brokerCfgError:'',
  mqttEnabled:false, mqttStatus:null, mqttSub:'overview',
  /* pengaturan persisten */
  refreshMs:+(localStorage.getItem('pk_refreshMs')||5000)||5000,
  density:localStorage.getItem('pk_density')||'comfortable',
  timeAbs:localStorage.getItem('pk_timeAbs')==='1',
  navGroupsCollapsed:{more:true},
  sort:{},
  tpSamples:[], tpTotal:0, tailPaused:false, pendingTail:0,
  /* Fitur 3.2 filter builder */
  filters:[], fieldHint:[], filterPanel:false,
  /* Fitur 3.3 column projection */
  projCols:(JSON.parse(localStorage.getItem('pk_proj')||'[]')),
  /* Fitur 3.4 message diff */
  diffSel:[], diffOpen:false,
};
const navItems=[
  ['overview','Overview',I.overview,'core'],
  ['topics','Topics',I.tag,'core'],
  ['databrowser','Data Browser',I.search,'core'],
  ['groups','Consumer Groups',I.users,'core'],
  ['metrics','Metrics',I.chart,'core'],
  ['producer','Producer Studio',I.send,'more'],
  ['schemas','Schemas',I.schema,'more'],
  ['logs','Broker Logs',I.document,'more'],
  ['settings','Settings',I.gear,'more']
];

function initTheme(){
  const saved=localStorage.getItem('pk_theme');
  state.theme=(saved==='dark'||saved==='light')?saved:(window.matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light');
  localStorage.setItem('pk_theme',state.theme);
  document.documentElement.dataset.theme=state.theme;
  state.sidebarCollapsed=localStorage.getItem('pk_sidebar_collapsed')==='1';
  try{state.navGroupsCollapsed=Object.assign({more:true},JSON.parse(localStorage.getItem('pk_nav_groups')||'{}'));}catch(e){state.navGroupsCollapsed={more:true};}
}

function toast(t){const m=$('#msg');m.textContent=redactTargetEndpoint(t);m.classList.add('show');setTimeout(()=>m.classList.remove('show'),2500);}
/* error tak tertangkap (inline onclick dsb) -> toast, tidak lagi senyap */
function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function fmtBytes(n){if(n<1024)return n+' B';if(n<1048576)return (n/1024).toFixed(1)+' KB';return (n/1048576).toFixed(1)+' MB';}
/* Klaster yang dipantau sebagai klien tidak bisa melaporkan semua angka: ukuran
   di disk dan laju byte tidak punya API-nya di protokol Kafka, jadi field itu
   datang sebagai null. null ditampilkan n/a, bukan 0 yang menyesatkan. */
function isNum(v){return typeof v==='number'&&isFinite(v);}
function fmtBytesOpt(v){return isNum(v)?fmtBytes(v):'n/a';}
function fmtRateOpt(v){return isNum(v)?fmtBytes(v)+'/s':'n/a';}
function fmtPctOpt(v){return isNum(v)?v.toFixed(1)+'%':'n/a';}
function rel(t){const d=Math.max(0,Date.now()-t);if(d<1000)return 'now';if(d<60000)return (Math.floor(d/5000)*5)+'s ago';if(d<3600000)return Math.floor(d/60000)+'m ago';return Math.floor(d/3600000)+'h ago';}
function online(s){s=(s||'').toLowerCase();return s==='stable'||s==='green';}

function isReducedMotion(){
  return window.matchMedia&&window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}
/* dinamis: auto-refresh berkala + indikator "updated Xs ago" */
function skel(w){return '<span class="skel" style="width:'+(w||60)+'px"></span>';}

function fallbackCopy(t){
  const ta=document.createElement('textarea'); ta.value=t; ta.style.position='fixed'; ta.style.opacity='0';
  document.body.appendChild(ta); ta.select();
  let copied=false;try{copied=document.execCommand('copy');}catch(e){}
  document.body.removeChild(ta);return copied;
}
function copyText2(t,btn){
  const done=()=>{ if(!btn)return; const old=btn.textContent; btn.textContent='copied'; btn.classList.add('ok');
    setTimeout(()=>{btn.textContent=old;btn.classList.remove('ok');},1200); };
  if(navigator.clipboard&&navigator.clipboard.writeText){ navigator.clipboard.writeText(t).then(done).catch(()=>{fallbackCopy(t);done();}); }
  else{ fallbackCopy(t);done(); }
}
window.copyText2=copyText2;
/* topology: peta broker -> topics -> groups dari data asli */
window.setDensity=function(d){state.density=d;localStorage.setItem('pk_density',d);applyDensity();};
window.setTimeFmt=function(m){state.timeAbs=m==='abs';localStorage.setItem('pk_timeAbs',state.timeAbs?'1':'0');render();};
window.setRefresh=function(ms){state.refreshMs=ms;localStorage.setItem('pk_refreshMs',String(ms));};
function applyDensity(){document.body.classList.toggle('compact',state.density==='compact');}
window.setTheme=function(mode){
  if(mode!=='dark'&&mode!=='light')return;
  state.theme=mode;localStorage.setItem('pk_theme',mode);document.documentElement.dataset.theme=mode;
  const next=mode==='dark'?'light':'dark',toggle=document.getElementById('themeToggle');
  if(toggle){toggle.setAttribute('aria-label','Switch to '+next+' theme');toggle.title='Switch to '+next+' theme';toggle.querySelector('.navActionText').textContent=next.charAt(0).toUpperCase()+next.slice(1)+' theme';}
  const settings=$('#view');
  if(state.view==='settings'&&settings){settings.innerHTML=viewSettings();bindView();applyDensity();}
};
function groupLagWarn(){ return Number(state.groupLagWarn)||1000000000; }
function activeAlerts(){
  const alerts=[];
  const gs=Array.isArray(state.groups)?state.groups:[];
  const warn=groupLagWarn();
  for(const g of gs){
    const lag=Object.values(g.lag||{}).reduce((a,b)=>a+b,0);
    if(lag>warn){ alerts.push({id:'lag:'+g.name, text:'Consumer lag '+lag+' on group '+g.name+' (>'+warn+')'}); }
  }
  const c=state.cluster||{};
  if(c.diskUsagePct && c.diskUsagePct>85){ alerts.push({id:'disk', text:'Disk usage '+c.diskUsagePct+'% (>85%)'}); }
  for(const t of (Array.isArray(state.topics)?state.topics:[])){
    if(/-(dlq|dead)$/i.test(t.name)){
      const hasMsgs=(t.totalBytes||0)>0||(t.partitions||[]).some(p=>(p.logEndOffset||0)>0);
      if(hasMsgs){ alerts.push({id:'dlq:'+t.name, text:'DLQ topic '+t.name+' has messages'}); }
    }
  }
  return alerts;
}
function checkAlarms(){
  const alerts=activeAlerts();
  const live=new Set(alerts.map(a=>a.id));
  for(const k of [...state.alarmsDismissed]) if(!live.has(k)) state.alarmsDismissed.delete(k);
  const shown=alerts.filter(a=>!state.alarmsDismissed.has(a.id));
  const existing=document.getElementById('alarmBanner');
  if(shown.length){
    if(!existing){ const b=document.createElement('div'); b.id='alarmBanner'; b.className='alarmBanner'; document.body.prepend(b); }
    const b=document.getElementById('alarmBanner');
    b.innerHTML='<b>ALERT</b> '+shown.map(a=>esc(a.text)).join(' &middot; ')+' <button class="btn ghost" style="padding:2px 8px;font-size:11px" onclick="dismissAlarms()">Dismiss</button>';
  }
  else if(existing){ existing.remove(); }
}
window.dismissAlarms=function(){
  for(const a of activeAlerts()) state.alarmsDismissed.add(a.id);
  try{ localStorage.setItem('pk_alarms_dismissed',JSON.stringify([...state.alarmsDismissed])); }catch(e){}
  const b=document.getElementById('alarmBanner'); if(b)b.remove();
};
window.setGroupLagWarn=function(v){
  const n=Number(v);
  if(!isFinite(n)||n<0){ toast('Threshold harus angka >= 0'); return; }
  state.groupLagWarn=n; localStorage.setItem('pk_group_lag_warn',String(n));
  state.alarmsDismissed.clear();
  try{ localStorage.setItem('pk_alarms_dismissed','[]'); }catch(e){}
  checkAlarms(); render();
};
/* Batch 1-3: fitur frontend */
/* URL sync + sorting helpers */
window.copyText=function(sel,label){const n=document.querySelector(sel);if(!n)return;navigator.clipboard.writeText(n.textContent).then(()=>toast((label||'Text')+' copied')).catch(()=>toast('Copy failed'));};

/* Broker configuration (read-only): satu endpoint efektif (default sudah diterapkan,
   rahasia sudah diredaksi server). Dibaca defensif karena peer lama bisa kirim payload
   parsial; view tidak boleh melempar exception hanya karena satu field hilang. */
function jsArg(s){ return esc(JSON.stringify(String(s))); }
function redactTargetEndpoint(value){return String(value||'').replace(/https?:\/\/[^\s"'<>]+/gi,'[endpoint redacted]');}



// Window bindings for HTML event handlers & global access
window.$ = $;
window.I = I;
window.state = state;
window.esc = esc;
window.fmtBytes = fmtBytes;
window.fmtBytesOpt = fmtBytesOpt;
window.fmtRateOpt = fmtRateOpt;
window.fmtPctOpt = fmtPctOpt;
window.isNum = isNum;
window.toast = toast;
window.skel = skel;
window.redactTargetEndpoint = redactTargetEndpoint;
window.rel = rel;
window.online = online;
window.jsArg = jsArg;
window.copyText2 = copyText2;
window.fallbackCopy = fallbackCopy;
window.isReducedMotion = isReducedMotion;
window.initTheme = initTheme;
window.applyDensity = applyDensity;
window.groupLagWarn = groupLagWarn;
window.activeAlerts = activeAlerts;
window.checkAlarms = checkAlarms;
window.navItems = navItems;


const copyText = (...args) => window.copyText?.(...args);
const setTheme = (...args) => window.setTheme?.(...args);
const setDensity = (...args) => window.setDensity?.(...args);
const setTimeFmt = (...args) => window.setTimeFmt?.(...args);
const setRefresh = (...args) => window.setRefresh?.(...args);
const dismissAlarms = (...args) => window.dismissAlarms?.(...args);
const setGroupLagWarn = (...args) => window.setGroupLagWarn?.(...args);

export {
  $, I, state, navItems, esc, fmtBytes, fmtBytesOpt, fmtRateOpt, fmtPctOpt, isNum, toast, skel, redactTargetEndpoint, rel, online, jsArg, copyText, copyText2, fallbackCopy, isReducedMotion, initTheme, setTheme, setDensity, applyDensity, setTimeFmt, setRefresh, groupLagWarn, activeAlerts, checkAlarms, dismissAlarms, setGroupLagWarn
};
