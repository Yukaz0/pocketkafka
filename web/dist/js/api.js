// PocketKafka API layer
import { state, toast, redactTargetEndpoint } from './state.js';

function setClusterRegistry(val) { clusterRegistry = val; window.clusterRegistry = val; }
function getClusterRegistry() { return window.clusterRegistry ?? clusterRegistry; }


const LOCAL_API_PATH=/^\/api\/v1\/(?:auth(?:\/|$)|clusters(?:\/|$)|health\/clusters(?:\/|$)|info(?:\/|$)|fleet(?:\/|$))/;
function kafkaCapabilityFor(path,method){
  const u=new URL(path,location.href),p=u.pathname,m=(method||'GET').toUpperCase();
  if(p==='/api/v1/cluster'&&m==='GET')return 'cluster';
  if(p==='/api/v1/topics'&&m==='GET')return 'topics.read';
  if(/^\/api\/v1\/topics\/[^/]+\/partitions$/.test(p)&&m==='GET')return 'topic.partitions.read';
  if(/^\/api\/v1\/topics\/[^/]+\/messages$/.test(p)&&m==='GET')return 'topic.messages.read';
  if(/^\/api\/v1\/topics\/[^/]+\/messages$/.test(p)&&m==='POST')return 'topic.messages.write';
  if(p==='/api/v1/groups'&&m==='GET')return 'groups.read';
  if(/^\/api\/v1\/groups\/[^/]+$/.test(p)&&m==='GET')return 'group.detail.read';
  if(p==='/api/v1/health/overview'&&m==='GET')return 'health.read';
  return null;
}
function activeTargetEntry(){return (getClusterRegistry()||[]).find(c=>c.name===state.activeCluster)||null;}
function activeTargetKind(){return state.activeCluster==='local'?'local':(activeTargetEntry()?.kind||'');}
function isTargetUnavailable(){return state.activeCluster!=='local'&&!activeTargetEntry();}
function targetUnavailableMessage(){return `Selected broker "${state.activeCluster}" is unavailable. Select a registered broker; requests are not sent to this broker.`;}
function isKafkaTarget(){return activeTargetKind()==='kafka';}
async function apiError(r,j,targetRequest){
  if(r.status===401){
    if(targetRequest){
      let localSessionExpired=false;
      try{localSessionExpired=(await fetch('/api/v1/auth/status')).status===401;}catch(e){}
      if(!localSessionExpired)throw new Error(redactTargetEndpoint(j.error||r.status));
    }
    showLogin();
    throw new Error('Authentication required');
  }
  if(!r.ok)throw new Error(redactTargetEndpoint(j.error||r.status));
  return j;
}
async function requestApi(p,o,targetRequest){
  o=Object.assign({},o||{});const m=(o.method||'GET').toUpperCase();
  if(m!=='GET'&&m!=='HEAD')o.headers=Object.assign({'X-PocketKafka-Request':'1'},o.headers||{});
  const r=await fetch(p,o);let j;try{j=await r.json();}catch(e){j={};}
  return apiError(r,j,targetRequest);
}
async function apiLocal(p,o){return requestApi(p,o);}
async function api(p,o){
  const u=new URL(p,location.href),method=(o&&o.method||'GET').toUpperCase();
  if(LOCAL_API_PATH.test(u.pathname))return apiLocal(p,o);
  const name=state.activeCluster,entry=name==='local'?null:activeTargetEntry();
  if(name!=='local'&&!entry)throw new Error(targetUnavailableMessage());
  if(entry&&(entry.kind||'').toLowerCase()==='kafka'){
    const capability=kafkaCapabilityFor(p,method);
    if(!capability)throw new Error(`Unsupported for Kafka targets: ${method} ${u.pathname}`);
    if(!(entry.capabilities||[]).includes(capability))throw new Error(`Selected broker "${name}" does not provide ${capability}.`);
  }
  const generation=state.targetGeneration,controller=new AbortController(),callerSignal=o&&o.signal;
  if(callerSignal?.aborted)throw new DOMException('Request aborted','AbortError');
  const abortFromCaller=()=>controller.abort();callerSignal?.addEventListener('abort',abortFromCaller,{once:true});state.targetRequests.add(controller);
  try{
    const path=name==='local'?p:`/api/v1/target/${encodeURIComponent(name)}${u.pathname}${u.search}`;
    const result=await requestApi(path,Object.assign({},o||{},{signal:controller.signal}),name!=='local');
    if(generation!==state.targetGeneration||name!==state.activeCluster){const stale=new Error('Target changed while request was in flight');stale.staleTargetResponse=true;throw stale;}
    return result;
  }catch(e){
    if(generation!==state.targetGeneration||name!==state.activeCluster){const stale=new Error('Target changed while request was in flight');stale.staleTargetResponse=true;throw stale;}
    throw e;
  }finally{state.targetRequests.delete(controller);callerSignal?.removeEventListener('abort',abortFromCaller);}
}
function hasTargetCapability(cap){if(state.activeCluster==='local')return true;const target=activeTargetEntry();if(!target)return false;if((target.kind||'').toLowerCase()!=='kafka')return true;return (target.capabilities||[]).includes(cap);}

let clusterRegistry=null;   /* registry metadata; never contains credential values */
function activeClusterLabel(){return state.activeCluster==='local'?'local (this broker)':(isTargetUnavailable()?state.activeCluster+' (unavailable)':state.activeCluster);}



// Window bindings
window.api = api;
window.apiLocal = apiLocal;
window.requestApi = requestApi;
window.apiError = apiError;
window.LOCAL_API_PATH = LOCAL_API_PATH;
window.kafkaCapabilityFor = kafkaCapabilityFor;
window.activeTargetEntry = activeTargetEntry;
window.activeTargetKind = activeTargetKind;
window.isTargetUnavailable = isTargetUnavailable;
window.targetUnavailableMessage = targetUnavailableMessage;
window.isKafkaTarget = isKafkaTarget;
window.hasTargetCapability = hasTargetCapability;
window.activeClusterLabel = activeClusterLabel;




export {
  LOCAL_API_PATH, kafkaCapabilityFor, activeTargetEntry, activeTargetKind, isTargetUnavailable, targetUnavailableMessage, isKafkaTarget, hasTargetCapability, activeClusterLabel, apiError, requestApi, apiLocal, api, clusterRegistry, setClusterRegistry, getClusterRegistry
};
