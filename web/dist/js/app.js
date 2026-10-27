// PocketKafka Root Application Entrypoint
import { state, initTheme, toast } from './state.js';
import { apiLocal } from './api.js';
import { render, bindView } from './render.js';
import { loadInfo, refresh, scheduleRefresh } from './refresh.js';
import { applyHash } from './router.js';
import {
  loadClusterList, startOverviewPoll, stopOverviewPoll,
  startHealthPoll, stopHealthPoll, startClusterPoll, stopClusterPoll,
  startMsgVirtWatch, stopMsgVirtWatch
} from './zones/kafka.js';
import './zones/fleet.js';
import './zones/mqtt.js';

// Global uncaught error listener
window.addEventListener('error', function(e){
  if(e && e.message && !/ResizeObserver|Script error/.test(e.message)){
    try { toast('Error: ' + e.message); } catch(_){}
  }
});

// Visibility change: pause polling when tab hidden
document.addEventListener('visibilitychange', () => {
  if(document.hidden) {
    stopOverviewPoll();
    stopHealthPoll();
    stopClusterPoll();
    stopMsgVirtWatch();
    if(state.tail&&typeof window.closeTailSocket==='function'){
      window.closeTailSocket();
      state.tail=false;
      const btn=document.getElementById('liveBtn');
      if(btn)btn.innerHTML='Live Tail';
    }
    if(typeof window.mqttStopTail==='function')window.mqttStopTail();
    if(typeof window.mockGenStop==='function')window.mockGenStop();
  } else {
    bindView();
  }
});

// Initialize theme and navigation groups
initTheme();

// Bootstrap application
(async function(){
  try {
    const st = await apiLocal('/api/v1/auth/status');
    if(st) state.auth = st;
  } catch(e){}

  if(state.auth && state.auth.enabled && !state.auth.authenticated){
    render();
    return;
  }

  await loadClusterList();
  await loadInfo();
  await refresh();
  if(!location.hash) history.replaceState(null, '', '#/overview');
  applyHash();
  state.initialized = true;
  scheduleRefresh();
})();
