// PocketKafka Polling & Refresh Loop
import { state, rel, isReducedMotion, toast, checkAlarms } from './state.js';
import { api, apiLocal, isTargetUnavailable, isKafkaTarget } from './api.js';

const render = (...args) => window.render?.(...args);
const buildStatusbar = (...args) => window.buildStatusbar?.(...args);
const updateChips = (...args) => window.updateChips?.(...args);
const patchConnections = (...args) => window.patchConnections?.(...args);
const loadThroughput = (...args) => window.loadThroughput?.(...args);
const refreshFleetView = (...args) => window.refreshFleetView?.(...args);
const refreshMQTTView = (...args) => window.refreshMQTTView?.(...args);
const activeTargetEntry = (...args) => window.activeTargetEntry?.(...args);


async function loadInfo(){
  try{
    const info=await apiLocal('/api/v1/info');
    state.fleetEnabled=!!(info&&info.fleetEnabled);
    state.mqttEnabled=!!(info&&info.mqttEnabled);
    if(info&&info.version)state.version=info.version;
  }catch(e){
    state.fleetEnabled=false;
    state.mqttEnabled=false;
  }
}
async function refresh(){
  if(state.auth&&state.auth.enabled&&!state.auth.authenticated)return;
  const generation=state.targetGeneration;
  await loadInfo();
  if(state.zone==='fleet'){
    if(state.fleetEnabled)await refreshFleetView();
    state.lastRefresh=Date.now();
    updAgo();
    return;
  }
  if(state.zone==='mqtt'){
    if(state.mqttEnabled)await refreshMQTTView();
    state.lastRefresh=Date.now();
    updAgo();
    return;
  }
  const tasks=[api('/api/v1/cluster').then(d=>state.cluster=d).catch(()=>{}),
    api('/api/v1/topics').then(d=>state.topics=Array.isArray(d)?d:(d&&Array.isArray(d.topics)?d.topics:[])).catch(()=>{}),
    api('/api/v1/groups').then(d=>state.groups=Array.isArray(d)?d:(d&&Array.isArray(d.groups)?d.groups:[])).catch(()=>{}),
    apiLocal('/api/v1/auth/status').then(d=>state.auth=d).catch(()=>{})];
  if(!isKafkaTarget()||hasTargetCapability('schemas.read'))tasks.push(api('/api/v1/schemas').then(d=>state.schemas=Array.isArray(d)?d:(d&&Array.isArray(d.schemas)?d.schemas:[])).catch(()=>{}));
  await Promise.allSettled(tasks);
  if(generation!==state.targetGeneration)return;
  state.lastRefresh=Date.now();updAgo();checkAlarms();
  if(state.view==='topology')renderTopoSvg();
}
function updAgo(){
  if(document.hidden)return;
  const el=document.getElementById('updAgo');
  if(!el||!state.lastRefresh)return;
  if(isReducedMotion()){
    el.textContent='polling paused (reduced-motion)';
    el.title='Auto-refresh disabled by prefers-reduced-motion';
    return;
  }
  const s=Math.max(0,Math.floor((Date.now()-state.lastRefresh)/1000));
  el.textContent='updated '+s+'s ago';
  el.title='Auto-refresh: '+(state.refreshMs/1000)+'s';
}
setInterval(updAgo,1000);
function scheduleRefresh(){ setTimeout(async()=>{ if(!document.hidden&&!isReducedMotion()&&state.zone!=='fleet'){ if(state.auth&&state.auth.enabled&&!state.auth.authenticated){scheduleRefresh();return;} await refresh(); const connActive=state.view==='connections'||(state.view==='settings'&&(state.settingsTab==='connections'||state.settingsTab==='general'||state.settingsTab==='clusters')); if(connActive&&!patchConnections())render(); if(state.view==='metrics')loadThroughput(); } scheduleRefresh(); },state.refreshMs); }



// Window bindings
window.loadInfo = loadInfo;
window.refresh = refresh;
window.scheduleRefresh = scheduleRefresh;
window.updAgo = updAgo;




export {
  loadInfo, refresh, scheduleRefresh, updAgo
};
