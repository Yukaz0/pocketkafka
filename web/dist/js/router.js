// PocketKafka Hash Router
import { state, navItems } from './state.js';

const render = (...args) => window.render?.(...args);
const refresh = (...args) => window.refresh?.(...args);
const closeMobileDrawers = (...args) => window.closeMobileDrawers?.(...args);
const bindView = (...args) => window.bindView?.(...args);
const openDrawer = (...args) => window.openDrawer?.(...args);
const stopOverviewPoll = (...args) => window.stopOverviewPoll?.(...args);
const stopHealthPoll = (...args) => window.stopHealthPoll?.(...args);
const stopClusterPoll = (...args) => window.stopClusterPoll?.(...args);
const stopMsgVirtWatch = (...args) => window.stopMsgVirtWatch?.(...args);
const patchFleetView = (...args) => window.patchFleetView?.(...args);
const setDensity = (...args) => window.setDensity?.(...args);


function currentHash(){
  const h=location.hash.replace(/^#\/?/,'');
  if(!h)return ['overview',[]];
  const parts=h.split('/');
  try{return [decodeURIComponent(parts[0]),parts.slice(1).map(decodeURIComponent)];}
  catch(e){return ['__invalid__',[]];}
}
function applyHash(){
  const [view,parts]=currentHash();
  if(view==='fleet'){
    state.zone='fleet';
    const sub=parts[0]||'servers';
    const validFleetSubs=['servers','containers','jobs','audit','settings'];
    const activeSub=validFleetSubs.includes(sub)?sub:'servers';
    state.view='fleet-'+activeSub;
    state.fleetSub=activeSub;
    state.fleetTarget=parts[1]||null;
    if(location.hash!=='#/fleet/'+activeSub){
      history.replaceState(null,'','#/fleet/'+activeSub);
    }
    if(state.fleetEnabled){
      refresh().then(()=>render());
    }
  }else if(view==='mqtt'){
    state.zone='mqtt';
    const sub=parts[0]||'overview';
    const validMqttSubs=['overview','tracker','clients','publisher','mapping'];
    const activeSub=validMqttSubs.includes(sub)?sub:'overview';
    state.view='mqtt-'+activeSub;
    state.mqttSub=activeSub;
    if(location.hash!=='#/mqtt/'+activeSub){
      history.replaceState(null,'','#/mqtt/'+activeSub);
    }
    if(state.mqttEnabled){
      refresh().then(()=>render());
    }
  }else if(view==='clusters'||view==='connections'||view==='security'||view==='storage'){
    state.zone='kafka';
    state.view='settings';
    state.settingsTab=view;
    history.replaceState(null,'','#/settings/'+view);
  }else if(view==='integrations'){
    state.zone='kafka';
    state.view='settings';
    state.settingsTab='connections';
    history.replaceState(null,'','#/settings/connections');
  }else if(view==='devstudio'){
    state.zone='kafka';
    state.view='producer';
    history.replaceState(null,'','#/producer');
  }else if(view==='settings'){
    state.zone='kafka';
    state.view='settings';
    state.settingsTab=parts[0]||'general';
  }else{
    state.zone='kafka';
    state.view=view;
  }
  if(state.zone==='kafka'){
    if(state.view!=='databrowser'){state.topic=null;state.partition='all';state.off=null;}
    else{state.topic=parts[0]||null;state.partition=parts[1]||'all';state.off=parts[2]||null;}
    const prevGroup=state.group;
    state.group=state.view==='groups'?(parts[0]||null):null;
    if(state.group!==prevGroup)state.groupDetail=null;
    state.topicDetail=state.view==='topics'?(parts[0]||null):null;
  }
  render();
}
window.addEventListener('hashchange',applyHash);
function syncHash(){
  if(state.view!=='databrowser')return;
  let h='#/databrowser';
  if(state.topic)h+='/'+encodeURIComponent(state.topic);
  if(state.partition&&state.partition!=='all')h+='/'+encodeURIComponent(state.partition);
  if(state.off!=null&&state.off!=='')h+='/'+encodeURIComponent(state.off);
  if(location.hash!==h)history.replaceState(null,'',h);
}

window.goView=async function(v,sub){
  try{ closeDrw(); }catch(e){}
  if(state._brokerLogsState && state._brokerLogsState.timer){
    clearInterval(state._brokerLogsState.timer);
    state._brokerLogsState.timer = null;
    state._brokerLogsState.auto = false;
  }
  if(v!=='databrowser'){
    if(typeof window.stopMsgVirtWatch==='function')window.stopMsgVirtWatch();
    if(state.tail&&typeof window.closeTailSocket==='function'){
      window.closeTailSocket();
      state.tail=false;
      const btn=document.getElementById('liveBtn');
      if(btn)btn.innerHTML='Live Tail';
    }
  }
  if(!v.startsWith('mqtt-')&&typeof window.mqttStopTail==='function')window.mqttStopTail();
  if(v!=='producer'&&typeof window.mockGenStop==='function')window.mockGenStop();
  if(v.startsWith('fleet-')){
    const subName=v.replace(/^fleet-/,'');
    state.zone='fleet';
    state.view=v;
    state.fleetSub=subName;
    const h='#/fleet/'+subName;
    if(location.hash!==h)location.hash=h;
    if(state.fleetEnabled){
      await refresh();
    }
    render();
    return;
  }
  if(v.startsWith('mqtt-')){
    const subName=v.replace(/^mqtt-/,'');
    state.zone='mqtt';
    state.view=v;
    state.mqttSub=subName;
    const h='#/mqtt/'+subName;
    if(location.hash!==h)location.hash=h;
    if(state.mqttEnabled){
      await refresh();
    }
    render();
    return;
  }
  state.zone='kafka';
  if(v==='clusters'||v==='connections'||v==='security'||v==='storage'){
    state.view='settings';
    state.settingsTab=v;
    state.topic=null;state.partition='all';state.off=null;state.group=null;state.topicDetail=null;
    const h='#/settings/'+v;
    if(location.hash!==h)location.hash=h;
    await refresh();
    render();
    return;
  }
  if(v==='integrations'){
    return goView('settings','connections');
  }
  if(v==='devstudio'){
    return goView('producer');
  }
  if(v==='settings'){
    state.view='settings';
    state.settingsTab=sub||state.settingsTab||'general';
    state.topic=null;state.partition='all';state.off=null;state.group=null;state.topicDetail=null;
    const h=state.settingsTab==='general'?'#/settings':'#/settings/'+state.settingsTab;
    if(location.hash!==h)location.hash=h;
    await refresh();
    render();
    return;
  }
  if(!navItems.some(([key])=>key===v)&&!['health','topology'].includes(v))return;
  if(v!=='databrowser'){state.topic=null;state.partition='all';state.off=null;}
  if(v!=='groups')state.group=null;
  if(v!=='topics')state.topicDetail=null;
  state.view=v;
  const h=v==='overview'?'#/overview':'#/'+v;
  if(location.hash!==h)location.hash=h;
  await refresh();
  render();
};
window.switchZone=async function(z){
  if(z==='fleet'){
    state.zone='fleet';
    const sub=(state.view&&state.view.startsWith('fleet-'))?state.view.replace(/^fleet-/,''):'servers';
    const target='fleet-'+sub;
    state.view=target;
    state.fleetSub=sub;
    const h='#/fleet/'+sub;
    if(location.hash!==h)location.hash=h;
    if(state.fleetEnabled)await refresh();
    render();
  }else if(z==='mqtt'){
    state.zone='mqtt';
    const sub=(state.view&&state.view.startsWith('mqtt-'))?state.view.replace(/^mqtt-/,''):(state.mqttSub||'overview');
    const target='mqtt-'+sub;
    state.view=target;
    state.mqttSub=sub;
    const h='#/mqtt/'+sub;
    if(location.hash!==h)location.hash=h;
    if(state.mqttEnabled)await refresh();
    render();
  }else{
    state.zone='kafka';
    state.view='overview';
    const h='#/overview';
    if(location.hash!==h)location.hash=h;
    await refresh();
    render();
  }
};
window.goZoneRoot=function(){
  if(state.zone==='fleet')switchZone('fleet');
  else if(state.zone==='mqtt')switchZone('mqtt');
  else switchZone('kafka');
};

/* Fitur 12: login */
window.goSettingsTab=function(tab){
  state.settingsTab=tab;
  const h=tab==='general'?'#/settings':'#/settings/'+tab;
  if(location.hash!==h)location.hash=h;
  render();
};



// Window bindings
window.currentHash = currentHash;
window.applyHash = applyHash;
window.syncHash = syncHash;


const goView = (...args) => window.goView?.(...args);
const switchZone = (...args) => window.switchZone?.(...args);
const goZoneRoot = (...args) => window.goZoneRoot?.(...args);
const goSettingsTab = (...args) => window.goSettingsTab?.(...args);

export {
  currentHash, applyHash, syncHash, goView, switchZone, goZoneRoot, goSettingsTab
};
