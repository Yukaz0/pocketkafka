// PocketKafka Render Orchestration
import { state, $, I, esc, redactTargetEndpoint, activeAlerts, groupLagWarn } from './state.js';
import {
  isTargetUnavailable, isKafkaTarget, activeTargetEntry, activeTargetKind,
  targetUnavailableMessage, activeClusterLabel
} from './api.js';
import {
  navUnavailableReason, targetUnavailablePanel, unsupportedPanel, targetSwitchingPanel,
  viewOverview, viewTopics, viewDataBrowser, viewGroups, viewMetrics, viewIntegrations,
  viewLogs, viewSettings, viewStorage, viewTopology, viewProducer, viewDevStudio,
  viewSchemas, viewHealth, viewLogin, bindLogin, viewTopicDetail, viewGroupDetail,
  viewConnections, viewClusters, viewSecurity,
  startOverviewPoll, stopOverviewPoll, startHealthPoll, stopHealthPoll,
  startClusterPoll, stopClusterPoll, startMsgVirtWatch, stopMsgVirtWatch,
  loadMsgs, loadPosMeta, loadGroupDetail, loadDelegatedTokens, loadACLs, loadAudit,
  loadThroughput, loadIntegrations, loadBrokerConfig, loadLogs, renderTopoSvg, bindTailPause
} from './zones/kafka.js';
import {
  viewFleetDisabled, viewFleetServers, viewFleetContainers, viewFleetJobs,
  viewFleetAudit, viewFleetSettings
} from './zones/fleet.js';
import {
  viewMQTTOverview, viewMQTTTracker, viewMQTTClients,
  viewMQTTPublisher, viewMQTTMapping
} from './zones/mqtt.js';

const syncHash = (...args) => window.syncHash?.(...args);
const goView = (...args) => window.goView?.(...args);
const refresh = (...args) => window.refresh?.(...args);


const navItems=[['overview','Overview',I.overview,'core'],['topics','Topics',I.tag,'core'],['databrowser','Data Browser',I.search,'core'],['groups','Consumer Groups',I.users,'core'],['metrics','Metrics',I.chart,'core'],['health','Health',I.pulse,'more'],['integrations','Integrations',I.topology,'more'],['logs','Broker Logs',I.document,'more'],['schemas','Schemas',I.schema,'more'],['producer','Producer Studio',I.send,'more'],['devstudio','Dev Studio',I.flask,'more'],['topology','Topology',I.topology,'more'],['storage','Storage',I.archive,'more'],['settings','Settings',I.gear,'more']];

function renderFatal(err){ const v=document.getElementById('view'); if(!v)return; const msg=esc(redactTargetEndpoint(String((err&&err.message)||err||'unknown'))); v.innerHTML='<div class="card"><h3>Something went wrong</h3><div class="muted" style="font-family:var(--font-mono);font-size:11px;margin:8px 0 14px;word-break:break-word">'+msg+'</div><button class="btn" onclick="recoverFatal()">Back to Overview</button></div>'; }
window.recoverFatal=function(){ state.group=null; state.topicDetail=null; goView('connections'); };
function buildNav(){
  const nav=document.getElementById('navItems'); if(!nav)return;
  const headText=document.querySelector('.appNavHead span');
  if(state.zone==='mqtt'){
    if(headText)headText.textContent='MQTT Studio';
    const mqttNavItems=[
      ['mqtt-overview','Overview',I.overview],
      ['mqtt-tracker','Live Tracker',I.pulse],
      ['mqtt-clients','Clients',I.users],
      ['mqtt-publisher','Publisher',I.send],
      ['mqtt-mapping','Topic Mapping',I.topology]
    ];
    nav.innerHTML=`<section class="navGroup" aria-label="MQTT Navigation">
      <div class="navGroupLinks">
        ${mqttNavItems.map(([key,name,icon])=>`
          <button class="navLink ${state.view===key?'active':''}" type="button" onclick="goView('${key}')" ${state.view===key?'aria-current="page"':''}>
            <span>${icon}</span><span class="navText">${esc(name)}</span>
          </button>
        `).join('')}
      </div>
    </section>`;
  }else if(state.zone==='fleet'){
    if(headText)headText.textContent='Fleet Host Control';
    const fleetNavItems=[
      ['fleet-servers','Servers',I.server],
      ['fleet-containers','Containers',I.box],
      ['fleet-jobs','Jobs',I.terminal],
      ['fleet-audit','Audit Logs',I.shield],
      ['fleet-settings','Settings',I.gear]
    ];
    nav.innerHTML=`<section class="navGroup" aria-label="Fleet Navigation">
      <div class="navGroupLinks">
        ${fleetNavItems.map(([key,name,icon])=>`
          <button class="navLink ${state.view===key?'active':''}" type="button" onclick="goView('${key}')" ${state.view===key?'aria-current="page"':''}>
            <span>${icon}</span><span class="navText">${esc(name)}</span>
          </button>
        `).join('')}
      </div>
    </section>`;
  }else{
    if(headText)headText.textContent='Control center';
    const groups=[['core',''],['more','More Tools']];
    nav.innerHTML=groups.map(([group,label])=>{
      const items=navItems.filter(item=>item[3]===group);
      if(!items.length)return '';
      const collapsible=group==='more';
      const collapsed=collapsible&&state.navGroupsCollapsed[group];
      const heading=label?`<button class="navGroupToggle" id="navGroupToggle-${group}" type="button" onclick="toggleNavGroup('${group}')" aria-label="Toggle ${esc(label)} navigation" aria-expanded="${collapsed?'false':'true'}" aria-controls="navGroup-${group}"><span>${esc(label)}</span><span class="navGroupChevron" aria-hidden="true"></span></button>`:'';
      const links=items.map(([key,name,icon])=>{
        const unavailable=navUnavailableReason(key);
        return `<button class="navLink ${state.view===key?'active':''}" type="button" ${unavailable?'disabled aria-disabled="true" title="'+esc(unavailable)+'"':'onclick="goView(\''+key+'\')"'} ${state.view===key?'aria-current="page"':''}><span>${icon}</span><span class="navText">${esc(name)}</span></button>`;
      }).join('');
      return `<section class="navGroup ${collapsed?'is-collapsed':''}" aria-label="${label||'Primary'}">${heading}<div class="navGroupLinks" ${label?`id="navGroup-${group}"`:''}>${links}</div></section>`;
    }).join('');
  }
  const navEl=document.getElementById('appNav');
  navEl.classList.toggle('collapsed',!!state.sidebarCollapsed);
  const toggle=document.getElementById('navToggle');
  if(toggle){const expanded=!state.sidebarCollapsed;toggle.setAttribute('aria-expanded',String(expanded));toggle.setAttribute('aria-label',expanded?'Collapse sidebar':'Expand sidebar');toggle.title=expanded?'Collapse sidebar':'Expand sidebar';toggle.querySelector('.navActionText').textContent=expanded?'Collapse sidebar':'Expand sidebar';}
  const theme=document.getElementById('themeToggle');
  if(theme){const next=state.theme==='dark'?'light':'dark';theme.setAttribute('aria-label','Switch to '+next+' theme');theme.title='Switch to '+next+' theme';theme.querySelector('.navActionText').textContent=next.charAt(0).toUpperCase()+next.slice(1)+' theme';}
}
window.toggleNavGroup=function(group){
  if(group!=='more')return;
  state.navGroupsCollapsed[group]=!state.navGroupsCollapsed[group];
  localStorage.setItem('pk_nav_groups',JSON.stringify(state.navGroupsCollapsed));
  buildNav();
  const btn=document.getElementById('navGroupToggle-'+group);
  if(btn)btn.focus();
};
window.toggleSidebar=function(){state.sidebarCollapsed=!state.sidebarCollapsed;localStorage.setItem('pk_sidebar_collapsed',state.sidebarCollapsed?'1':'0');buildNav();};
function openDrawer(){const nav=document.getElementById('appNav');nav.classList.add('mobileOpen');document.getElementById('navBackdrop').classList.add('visible');const b=document.getElementById('burgerBtn');b.setAttribute('aria-expanded','true');nav.querySelector('.navLink')?.focus();}
function closeDrawer(){const nav=document.getElementById('appNav');if(!nav)return;nav.classList.remove('mobileOpen');document.getElementById('navBackdrop').classList.remove('visible');const b=document.getElementById('burgerBtn');if(b){b.setAttribute('aria-expanded','false');if(window.matchMedia('(max-width: 760px)').matches)b.focus();}}
function toggleTopicDrawer(){const p=document.getElementById('plist'),b=document.getElementById('topicBtn');const open=p.classList.toggle('mobileOpen');document.getElementById('navBackdrop').classList.toggle('visible',open);b.setAttribute('aria-expanded',String(open));if(open)document.getElementById('tfilter')?.focus();else b.focus();}
function closeMobileDrawers(){document.getElementById('plist')?.classList.remove('mobileOpen');document.getElementById('topicBtn')?.setAttribute('aria-expanded','false');closeDrawer();}
window.closeDrawer=closeDrawer;window.openDrawer=openDrawer;
document.addEventListener('click',e=>{if(e.target.closest('#appNav .navLink'))closeMobileDrawers();});
let _tlistQ='';
function buildTList(){
  const wrap=document.getElementById('tlist'); if(!wrap)return;
  const topics=state.topics||[];
  wrap.innerHTML=(_tlistQ?topics.filter(t=>t.name.toLowerCase().includes(_tlistQ)):topics)
    .map(t=>{
      const ps=t.partitions||[];
      const parts=ps.length, leo=parts?Math.max(...ps.map(p=>p.logEndOffset||0)):0;
      const sel=state.view==='databrowser'&&state.topic===t.name;
      const dlq=/-(dlq|dead)$/i.test(t.name);
      return `<div class="titem ${sel?'on':''}" onclick="pickTopicNav('${esc(t.name)}')" title="${esc(t.name)} - ${parts} partitions - ${fmtBytes(t.totalBytes||0)}">
        <b>${esc(t.name)}${dlq?' <span style="color:var(--danger)">DLQ</span>':''}</b>
        <span>${parts} parts - ${fmtBytes(t.totalBytes||0)} - LEO ${leo}</span></div>`;
    }).join('')||'<div class="muted" style="padding:10px 12px;font-size:11px">No topics yet.</div>';
}
function renderTList(q){_tlistQ=(q||'').toLowerCase();buildTList();}
window.renderTList=renderTList;
function buildStatusbar(){
  let sb=document.getElementById('sbar');
  if(!sb){
    sb=document.createElement('div'); sb.id='sbar'; sb.className='sbar';
    document.body.appendChild(sb);
  }
  const c=state.cluster||{};
  const topicChip=state.view==='databrowser'&&state.topic?`<span>p: ${esc(state.partition)}</span>`:'';
  const posChip=(state.view==='databrowser'&&state.leo!=null)?`<span>LEO ${state.leo}</span>`:'';
  sb.innerHTML=`<span><span class="sdot"></span>broker-${esc(String(c.brokerId??'-'))}</span>`
    +`<span class="slink" onclick="goView('security')">${esc(c.security||'PLAINTEXT')}</span>`
    +topicChip+posChip
    +`<span style="flex:1"></span>`
    +`<span class="slink" onclick="goView('storage')">${fmtBytes(c.totalBytes||0)} on disk</span>`
    +`<span class="slink" onclick="goView('metrics')">throughput</span>`
    +`<span class="slink" onclick="goView('logs')">logs</span>`
    +`<span class="slink" onclick="goView('settings')">settings</span>`;
}
function updateChips(){
  const c=state.cluster||{};
  const b=document.getElementById('chipBroker'); if(b)b.innerHTML=`<i></i>broker-${esc(String(c.brokerId??'-'))}`;
  const s=document.getElementById('chipSec'); if(s)s.textContent=c.security||'PLAINTEXT';
  const v=document.getElementById('chipVer'); if(v)v.textContent=c.version?('v'+c.version):'v-';
}
/* Navigasi topik lewat hash: router (applyHash) yang mengisi state.topic, sehingga
   topik tetap terpilih baik dipanggil dari dalam Data Browser maupun dari view lain.
   Hash yang sama tidak memicu hashchange, jadi applyHash dipanggil langsung. */
window.pickTopicNav=function(name){
  if(!name)return;
  const h='#/databrowser/'+encodeURIComponent(name);
  if(location.hash===h){applyHash();return;}
  location.hash=h;
};
/* node detail drawer (topologi) */
/* node detail / log drawer */
function openDrw(title,innerHtml,keepMaximized){
  if(!title.startsWith('Logs:') && state._fleetLogsState && state._fleetLogsState.autoTimer){
    clearInterval(state._fleetLogsState.autoTimer);
    state._fleetLogsState.autoTimer = null;
    state._fleetLogsState.autoRefresh = false;
  }
  setupDrwResizer();
  $('#drwT').textContent=title;
  $('#drwBody').innerHTML=innerHtml;
  const d=document.getElementById('drw');
  const wasMax=!!keepMaximized&&d.classList.contains('maximized');
  if(!wasMax){
    d.classList.remove('maximized');
    const btn=document.getElementById('drwMaxBtn');
    if(btn){btn.textContent='[⛶]';btn.title='Maximize drawer';}
  }
  d.classList.add('open');
}
function closeDrw(){
  if(state._fleetLogsState && state._fleetLogsState.autoTimer){
    clearInterval(state._fleetLogsState.autoTimer);
    state._fleetLogsState.autoTimer = null;
    state._fleetLogsState.autoRefresh = false;
  }
  const d=document.getElementById('drw');
  if(d){d.classList.remove('open');d.classList.remove('maximized');}
  const act=document.getElementById('drwActions');
  if(act)act.innerHTML='';
  const btn=document.getElementById('drwMaxBtn');
  if(btn){btn.textContent='[⛶]';btn.title='Maximize drawer';}
}
window.closeDrw=closeDrw;
window.toggleDrwMax=function(){
  const d=document.getElementById('drw');
  if(!d)return;
  const isMax=d.classList.toggle('maximized');
  const btn=document.getElementById('drwMaxBtn');
  if(btn){btn.textContent=isMax?'[−]':'[⛶]';btn.title=isMax?'Restore drawer':'Maximize drawer';}
};
function setupDrwResizer(){
  const resizer=document.getElementById('drwResizer');
  if(!resizer||resizer._bound)return;
  resizer._bound=true;
  let startY=0,startH=0;
  function onMove(e){
    const clientY=e.touches?e.touches[0].clientY:e.clientY;
    const parentH=document.querySelector('.content')?.clientHeight||window.innerHeight;
    const newH=Math.max(180,Math.min(parentH-40,startH+(startY-clientY)));
    const d=document.getElementById('drw');
    d.style.height=newH+'px';
    d.classList.remove('maximized');
    const btn=document.getElementById('drwMaxBtn');
    if(btn){btn.textContent='[⛶]';btn.title='Maximize drawer';}
  }
  function onUp(){
    window.removeEventListener('mousemove',onMove);
    window.removeEventListener('mouseup',onUp);
    window.removeEventListener('touchmove',onMove);
    window.removeEventListener('touchend',onUp);
    document.getElementById('drwResizer')?.classList.remove('active');
  }
  function onDown(e){
    startY=e.touches?e.touches[0].clientY:e.clientY;
    startH=document.getElementById('drw').offsetHeight;
    document.getElementById('drwResizer')?.classList.add('active');
    window.addEventListener('mousemove',onMove);
    window.addEventListener('mouseup',onUp);
    window.addEventListener('touchmove',onMove);
    window.addEventListener('touchend',onUp);
  }
  resizer.addEventListener('mousedown',onDown);
  resizer.addEventListener('touchstart',onDown);
}
function nodeDrw(title,meta,tableHtml,actions){
  openDrw(title,`${meta?`<div style="font:11px/1.9 var(--font-mono);color:var(--muted);margin-bottom:10px">${meta}</div>`:''}${tableHtml}`);
  const act=document.getElementById('drwActions');
  if(act){
    act.innerHTML=(actions||[]).map(a=>`<button class="cbtn" onclick="${a.fn}">${esc(a.label)}</button>`).join('');
  }
}
window.addEventListener('keydown',e=>{if(e.key==='Escape'&&window.matchMedia('(max-width: 760px)').matches)closeMobileDrawers();});
function render(){
  try{
    const authRequired=!!(state.auth&&state.auth.enabled&&!state.auth.authenticated);
    document.body.classList.toggle('auth-mode',authRequired);
    if(authRequired){
      document.title='PocketKafka - Sign In';
      const sb=document.getElementById('sbar');if(sb)sb.remove();
      const ab=document.getElementById('alarmBanner');if(ab)ab.remove();
      closeDrw();
      closeMobileDrawers();
      bindView();
      const v=$('#view');
      if(v){v.innerHTML=viewLogin();bindLogin();}
      window.__renderedView='__login__';
      return;
    }
    document.title='PocketKafka';
    const scroller=$('#view'),keepScroll=(scroller&&window.__renderedView===state.view)?scroller.scrollTop:0;
    updateBrokerPicker();buildNav();buildTList();buildStatusbar();updateChips();
    const zoneSwitcherEl=document.getElementById('zoneSwitcher');
    if(zoneSwitcherEl){
      const showSwitcher=!!(state.fleetEnabled||state.mqttEnabled);
      zoneSwitcherEl.style.display=showSwitcher?'inline-flex':'none';
      const bK=document.getElementById('zoneBtnKafka'),bF=document.getElementById('zoneBtnFleet'),bM=document.getElementById('zoneBtnMqtt');
      if(bK){bK.classList.toggle('active',state.zone==='kafka');bK.setAttribute('aria-selected',String(state.zone==='kafka'));}
      if(bF){bF.classList.toggle('active',state.zone==='fleet');bF.setAttribute('aria-selected',String(state.zone==='fleet'));bF.style.display=state.fleetEnabled?'':'none';}
      if(bM){bM.classList.toggle('active',state.zone==='mqtt');bM.setAttribute('aria-selected',String(state.zone==='mqtt'));bM.style.display=state.mqttEnabled?'':'none';}
    }
    document.body.dataset.zone=state.zone;
    const unavailableNav=navUnavailableReason(state.view),localOnlyView=['clusters','settings','security'].includes(state.view);
    const topicPane=document.getElementById('plist'),topicBtn=document.getElementById('topicBtn'),browsing=state.zone==='kafka'&&state.view==='databrowser'&&!unavailableNav;
    if(topicPane){topicPane.hidden=!browsing;if(!browsing)topicPane.classList.remove('mobileOpen');}
    if(topicBtn){topicBtn.hidden=!browsing;topicBtn.setAttribute('aria-expanded',String(browsing&&topicPane.classList.contains('mobileOpen')));}
    
    if(state.zone==='fleet'){
      const fleetTitles={'fleet-servers':'Servers','fleet-containers':'Containers','fleet-jobs':'Jobs','fleet-audit':'Audit Logs','fleet-settings':'Fleet Settings'};
      const curTitle=fleetTitles[state.view]||'Fleet';
      $('#title').textContent=curTitle;
      $('#crumb').textContent=state.fleetEnabled?('Fleet / '+curTitle):'Fleet / Disabled';
      $('#topActions').innerHTML=state.fleetEnabled?(`<button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="refreshAll()">Refresh</button>`+(state.view==='fleet-servers'?`<button class="btn" style="padding:5px 12px;font-size:12px" onclick="openEnrollModal()">+ Enroll Server</button>`:'')):'';
    }else if(state.zone==='mqtt'){
      const mqttTitles={'mqtt-overview':'Overview','mqtt-tracker':'Live Tracker','mqtt-clients':'Clients','mqtt-publisher':'Publisher','mqtt-mapping':'Topic Mapping'};
      const curTitle=mqttTitles[state.view]||'MQTT Studio';
      $('#title').textContent=curTitle;
      $('#crumb').textContent=state.mqttEnabled?('MQTT / '+curTitle):'MQTT / Disabled';
      $('#topActions').innerHTML=`<button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="refreshAll()">Refresh</button>`;
    }else{
      const item=navItems.find(([k])=>k===state.view);
      $('#title').textContent=item?item[1]:(state.view==='settings'?'Settings':'Overview');
      const tabLabels={general:'General',clusters:'Clusters',connections:'Connections',security:'Security & ACL'};
      $('#crumb').textContent=state.view==='databrowser'&&state.topic?('Kafka / Topics / '+state.topic):(state.view==='groups'&&state.group?('Kafka / Groups / '+state.group):(state.view==='settings'&&state.settingsTab&&state.settingsTab!=='general'?('Kafka / Settings / '+(tabLabels[state.settingsTab]||state.settingsTab)):('Kafka / '+(item?item[1]:'Overview'))));
      const isConnActive=state.view==='connections'||(state.view==='settings'&&(state.settingsTab==='connections'||state.settingsTab==='general'||state.settingsTab==='clusters'));
      $('#topActions').innerHTML=(state.view==='overview'||isConnActive?`<button class="btn ghost" style="padding:5px 12px;font-size:12px" onclick="refreshAll()">Refresh</button>`:
        state.view==='databrowser'?`<button class="btn" style="padding:5px 12px;font-size:12px" onclick="goView('connections')">Connections</button>`:'');
    }
    const v=$('#view');
    if(state.zone==='fleet'&&!state.fleetEnabled)v.innerHTML=viewFleetDisabled();
    else if(state.view==='fleet-servers')v.innerHTML=viewFleetServers();
    else if(state.view==='fleet-containers')v.innerHTML=viewFleetContainers();
    else if(state.view==='fleet-jobs')v.innerHTML=viewFleetJobs();
    else if(state.view==='fleet-audit')v.innerHTML=viewFleetAudit();
    else if(state.view==='fleet-settings')v.innerHTML=viewFleetSettings();
    else if(state.view==='mqtt-overview')v.innerHTML=viewMQTTOverview();
    else if(state.view==='mqtt-tracker')v.innerHTML=viewMQTTTracker();
    else if(state.view==='mqtt-clients')v.innerHTML=viewMQTTClients();
    else if(state.view==='mqtt-publisher')v.innerHTML=viewMQTTPublisher();
    else if(state.view==='mqtt-mapping')v.innerHTML=viewMQTTMapping();
    else if(state.targetSwitching&&!['clusters','settings'].includes(state.view))v.innerHTML=targetSwitchingPanel();
    else if(isTargetUnavailable()&&!localOnlyView)v.innerHTML=targetUnavailablePanel();
    else if(unavailableNav&&!localOnlyView)v.innerHTML=unsupportedPanel(state.view);
    else if(state.view==='overview')v.innerHTML=viewOverview();
    else if(state.view==='connections')v.innerHTML=viewConnections();
    else if(state.view==='databrowser')v.innerHTML=viewDataBrowser();
    else if(state.view==='topics')v.innerHTML=state.topicDetail?viewTopicDetail():viewTopics();
    else if(state.view==='groups')v.innerHTML=state.group?viewGroupDetail():viewGroups();
    else if(state.view==='schemas')v.innerHTML=viewSchemas();
    else if(state.view==='producer')v.innerHTML=viewProducer();
    else if(state.view==='devstudio')v.innerHTML=viewDevStudio();
    else if(state.view==='topology')v.innerHTML=viewTopology();
    else if(state.view==='storage')v.innerHTML=viewStorage();
    else if(state.view==='clusters')v.innerHTML=viewClusters();
    else if(state.view==='health')v.innerHTML=viewHealth();
    else if(state.view==='metrics')v.innerHTML=viewMetrics();
    else if(state.view==='integrations')v.innerHTML=viewIntegrations();
    else if(state.view==='logs')v.innerHTML=viewLogs();
    else if(state.view==='security')v.innerHTML=viewSecurity();
    else if(state.view==='settings')v.innerHTML=viewSettings();
    else v.innerHTML='<div class="card"><h3>404</h3><div class="muted" style="margin-top:6px">Unknown view. <button class="btn ghost" onclick="goView(\'overview\')">Open Overview</button></div></div>';
    bindView();
    applyDensity();
    if(keepScroll&&scroller)scroller.scrollTop=keepScroll;
    window.__renderedView=state.view;
  }catch(err){console.error('render error:',err);renderFatal(err);}
}
async function refreshAll(){await refresh();render();}
window.refreshAll=refreshAll;
function pageHeader(title,description,actions=''){
  return `<div class="pageHeading"><div><h2>${title}</h2><p>${description}</p></div>${actions?`<div class="pageHeadingActions">${actions}</div>`:''}</div>`;
}
function tableScroll(content){return `<div class="tableScroll">${content}</div>`;}

/* Data Browser */
function updateBrokerPicker(){
  const sel=document.getElementById('activeClusterSelect');if(!sel)return;
  const targets=window.clusterRegistry||[];
  let options='<option value="local">local (this broker)</option>';
  targets.forEach(c=>{options+=`<option value="${esc(c.name)}">${esc(c.name)}${c.kind==='kafka'?' (Kafka)':''}</option>`;});
  if(state.activeCluster!=='local'&&!targets.some(c=>c.name===state.activeCluster))options+=`<option value="${esc(state.activeCluster)}">${esc(state.activeCluster)} (unavailable)</option>`;
  sel.innerHTML=options;sel.value=state.activeCluster;
}
function bindView(){
  const active=!document.hidden,authRequired=!!(state.auth&&state.auth.enabled&&!state.auth.authenticated),viewReady=active&&!authRequired&&!state.targetSwitching&&!navUnavailableReason(state.view),localViewReady=active&&!authRequired;
  if(viewReady&&state.view==='overview')startOverviewPoll();else stopOverviewPoll();
  if(viewReady&&state.view==='health')startHealthPoll();else stopHealthPoll();
  const clustersActive=state.view==='clusters'||(state.view==='settings'&&state.settingsTab==='clusters');
  if(localViewReady&&clustersActive)startClusterPoll();else stopClusterPoll();
  if(viewReady&&state.view==='databrowser')startMsgVirtWatch();else stopMsgVirtWatch();
  if(viewReady&&state.view==='databrowser'&&state.topic){loadMsgs();loadPosMeta();}
  if(viewReady&&state.view==='groups'&&state.group&&!state.groupDetail)loadGroupDetail();
  const securityActive=state.view==='security'||(state.view==='settings'&&state.settingsTab==='security');
  if(localViewReady&&securityActive){
    loadDelegatedTokens();
    if(!state.targetSwitching&&!isTargetUnavailable()&&!isKafkaTarget()){loadACLs();loadAudit();}
  }
  if(viewReady&&state.view==='metrics')loadThroughput();
  if(viewReady&&state.view==='integrations')loadIntegrations();
  const generalSettingsActive=state.view==='settings'&&(!state.settingsTab||state.settingsTab==='general');
  if(localViewReady&&generalSettingsActive)loadBrokerConfig();
  if(viewReady&&state.view==='logs')loadLogs();
  if(viewReady&&state.view==='topology')renderTopoSvg();
  if(viewReady&&state.view==='databrowser')bindTailPause();
}



// Window bindings
window.render = render;
window.refreshAll = refreshAll;
window.buildNav = buildNav;
window.buildStatusbar = buildStatusbar;
window.updateChips = updateChips;
window.updateBrokerPicker = updateBrokerPicker;
window.bindView = bindView;
window.pageHeader = pageHeader;
window.tableScroll = tableScroll;
window.openDrawer = openDrawer;
window.closeDrawer = closeDrawer;
window.toggleTopicDrawer = toggleTopicDrawer;
window.closeMobileDrawers = closeMobileDrawers;
window.openDrw = openDrw;
window.closeDrw = closeDrw;
window.setupDrwResizer = setupDrwResizer;
window.nodeDrw = nodeDrw;
window.renderFatal = renderFatal;
window.renderTList = renderTList;
window.buildTList = buildTList;


const toggleDrwMax = (...args) => window.toggleDrwMax?.(...args);
const recoverFatal = (...args) => window.recoverFatal?.(...args);
const pickTopicNav = (...args) => window.pickTopicNav?.(...args);
const toggleNavGroup = (...args) => window.toggleNavGroup?.(...args);
const toggleSidebar = (...args) => window.toggleSidebar?.(...args);

export {
  render, refreshAll, buildNav, buildStatusbar, updateChips, updateBrokerPicker, bindView, pageHeader, tableScroll, openDrawer, closeDrawer, toggleTopicDrawer, closeMobileDrawers, openDrw, closeDrw, toggleDrwMax, setupDrwResizer, nodeDrw, renderFatal, recoverFatal, renderTList, buildTList, pickTopicNav, toggleNavGroup, toggleSidebar, navItems
};
