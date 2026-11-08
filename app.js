'use strict';
/* ============================================================
   APEX KISS ABSOLUTE CORE ENGINE (ULTRA DYNAMIC TABLE & SCHEMA)
   ============================================================ */
const W=window,L=localStorage;
let db=null,syncing=false,syncDebounce=null;
const ch=(typeof BroadcastChannel!=='undefined')?new BroadcastChannel('sync'):{postMessage:()=>{},onmessage:null};
ch.onmessage=e=>{if(e.data==='synced')render();else if(e.data==='kick'&&!syncing)syn();};

async function pullStream(url,onRow){
  try{
    const res=await fetch(url);if(!res.body)return;
    const reader=res.body.pipeThrough(new TextDecoderStream()).getReader();let buf='';
    while(true){
      const{value,done}=await reader.read();if(done)break;
      buf+=value;const lines=buf.split('\n');buf=lines.pop();
      for(const l of lines){if(l.trim()){try{onRow(JSON.parse(l));}catch{}}}
    }
    if(buf.trim()){try{onRow(JSON.parse(buf));}catch{}}
  }catch(_){}
}

function idbPut(row,st='mut'){if(!db)return;try{db.transaction(st,'readwrite').objectStore(st).put(row);}catch{}}
function idbClearStore(st){if(!db)return;try{if(db.objectStoreNames.contains(st))db.transaction(st,'readwrite').objectStore(st).clear();}catch{}}

function save(row,targetTable=K.cur,actionType=2){
  const item={i:Date.now().toString(36)+Math.random().toString(36).slice(2,6),t:TI(targetTable),y:actionType,r:Array.isArray(row)?row:Object.values(row),ts:Date.now()};
  idbPut(item,'mut');ch.postMessage('kick');
  if(!syncing){clearTimeout(syncDebounce);syncDebounce=setTimeout(()=>syn(false),40);}
}

function initDB(tables=[],onReady){
  try{
    const req=indexedDB.open('r',112);
    req.onupgradeneeded=e=>{
      const d=e.target.result;
      tables.forEach(t=>!d.objectStoreNames.contains(t)&&d.createObjectStore(t,{keyPath:'_seq',autoIncrement:true}));
      if(!d.objectStoreNames.contains('mut'))d.createObjectStore('mut',{keyPath:'i'});
    };
    req.onsuccess=e=>{db=e.target.result;onReady?.(db);};
    req.onerror=()=>{onReady?.(null);};
  }catch(_){onReady?.(null);}
}

async function req(p={}){
  p.u=p.u||L.u||'';
  if(HARD.d1Url&&['pull_batch','pull','manifest','push','stream','ensure_all','schema_crud'].includes(p.a)){
    try{
      const r=await(await fetch(HARD.d1Url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)})).json();
      if(r?.ok||r?.s==='ok'||r?.status==='ok')return r;
    }catch{}
  }
  return new Promise(res=>{
    if(W.google?.script?.run){W.google.script.run.withSuccessHandler(r=>res({ok:r?.ok??r?.s==='ok',...r})).withFailureHandler(e=>res({ok:false,e})).core(p);return;}
    if(HARD.gasUrl&&/^https?:\/\//i.test(HARD.gasUrl)){
      fetch(HARD.gasUrl,{method:'POST',headers:{'Content-Type':'text/plain;charset=utf-8'},body:JSON.stringify(p)}).then(r=>r.json()).then(j=>res({ok:j?.ok??j?.s==='ok',...j})).catch(e=>res({ok:false,e}));return;
    }
    res({ok:false,e:'offline'});
  });
}

function mut(table,data,actionType){save(data,table,actionType);}

async function testConn(){
  msg('cek koneksi...');
  let d1Ok=false,gasOk=false;
  if(HARD.d1Url){
    try{
      const res=await req({a:'manifest',tn:K.shs,cols:K.cols});
      if(res&&(res.ok||res.v||res.data?.v))d1Ok=true;
    }catch{}
  }
  if(HARD.gasUrl){
    try{
      const r=await fetch(HARD.gasUrl,{method:'POST',headers:{'Content-Type':'text/plain;charset=utf-8'},body:JSON.stringify({a:'manifest',tn:K.shs,cols:K.cols})});
      const j=await r.json();
      if(j&&(j.ok||j.v||j.data?.v))gasOk=true;
    }catch{}
  }
  const statusStr=`D1: ${d1Ok?'OK':'OFFLINE'} | GAS: ${gasOk?'OK':'OFFLINE'}`;
  msg(statusStr);
  setTimeout(()=>msg(),3000);
  return {d1:d1Ok,gas:gasOk};
}

async function syn(force=false){
  if(!navigator.onLine||(syncing&&!force)||!db)return;
  syncing=true;
  try{
    const tables=K.shs||HARD.shs;
    if(force)L.vs='[]';
    const ver=JSON.parse(L.vs||'[]'),muts=await ga('mut');
    let m=null;

    // 1. PUSH DELTA MUTATIONS BESERTA SCHEMA AKTIF
    if(muts.length){
      const pushRes=await req({a:'push',m:typeof pack==='function'?pack(muts):muts,tn:tables,db:K.db,cols:K.cols,shs:K.shs,gasUrl:HARD.gasUrl});
      if(pushRes?.ok){await rm('mut',muts.map(x=>x.i));m=pushRes.manifest||pushRes.data?.manifest;}
    }

    // 2. STREAM PULL (CLOUDFLARE D1) DENGAN AUTO RESOLVE SKEMA DINAMIS
    if(HARD.d1Url&&/^https?:\/\//i.test(HARD.d1Url)){
      try{
        let streamChanged=false;
        await pullStream(HARD.d1Url+'?stream=1',o=>{
          if(o?.t){
            if(!K.shs.includes(o.t)){K.shs.push(o.t);streamChanged=true;}
            if(Array.isArray(o.cols)&&o.cols.length){K.cols[o.t]=o.cols.slice();if(!K.vis[o.t])K.vis[o.t]=o.cols.slice(0,4);streamChanged=true;}
            if(Array.isArray(o.r)){K.db[o.t]=o.r;streamChanged=true;}
          }
        });
        if(streamChanged){persist();ch.postMessage('synced');render();return;}
      }catch{}
    }

    // 3. MANIFEST CEK DENGAN SINKRONISASI DAFTAR TABEL & KOLOM
    if(!m)m=(await req({a:'manifest',tn:tables,cols:K.cols}))?.data||(await req({a:'manifest',tn:tables,cols:K.cols}));
    if(m?.shs&&Array.isArray(m.shs)&&m.shs.length){
      m.shs.forEach(s=>{if(!K.shs.includes(s))K.shs.push(s);});
    }
    if(m?.cols&&typeof m.cols==='object')Object.assign(K.cols,m.cols);
    if(m?.vis&&typeof m.vis==='object')Object.assign(K.vis,m.vis);

    if(m?.v){
      const toPull=tables.map((_,i)=>({t:i,v:force?'':(ver[i]||'')})).filter((_,i)=>force||(m.v[i]&&m.v[i]!==ver[i]));
      if(toPull.length){
        const b=await req({a:'pull_batch',reqs:toPull,tn:tables,cols:K.cols});
        (b?.data?.results||b?.results||[]).forEach(async r=>{
          if(r?.v){
            ver[r.t]=r.v;
            await putRows(r.t,r.r,r.cols);
          }
        });
        L.vs=JSON.stringify(ver);ch.postMessage('synced');render();
      }
    }else if(m?.db){
      Object.assign(K.db,m.db);persist();ch.postMessage('synced');render();
    }
  }finally{syncing=false;}
}
Object.assign(W,{pullStream,idbPut,idbClearStore,save,initDB,req,mut,syn,testConn});

const T=HARD.shs,TI=s=>K.shs.indexOf(s),R=(t,d)=>Array.isArray(d)?d:Object.values(d);
const ga=table=>new Promise(res=>{if(!db)return res([]);try{const rq=db.transaction(table,'readonly').objectStore(table).getAll();rq.onsuccess=()=>res(rq.result||[]);rq.onerror=()=>res([]);}catch{res([]);}});
const rm=(table,keys)=>new Promise(res=>{if(!db||!keys?.length)return res();try{const tx=db.transaction(table,'readwrite'),st=tx.objectStore(table);keys.forEach(k=>st.delete(k));tx.oncomplete=()=>res();tx.onerror=()=>res();}catch{res();}});

const putRows=async(idx,rows,cNames)=>{
  const tn=K.shs[idx];if(!tn)return false;
  K.db[tn]=rows.slice();
  if(Array.isArray(cNames)&&cNames.length)K.cols[tn]=cNames.slice();
  persist();return true;
};

const K={
  u:L.u||'',cur:'penjualan',v:'dash',hist:[],idx:null,page:1,sort:0,dir:1,grid:false,
  conf:null,prefill:null,parent:null,receiptRaw:'',prodIdx:null,readyOnly:false,
  df:{u:'',from:'',to:'',kat:''},store:{...HARD.store},
  shs:[...HARD.shs],cols:JSON.parse(JSON.stringify(HARD.cols)),vis:JSON.parse(JSON.stringify(HARD.vis)),
  fvis:{},db:JSON.parse(JSON.stringify(HARD.db)),scanTarget:'q',scanStream:null,scanActive:false,
  refTargetInp:null,savedFormState:null,pwaEvent:null,writing:false
};

const esc=s=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])),
      qEsc=s=>W.CSS&&CSS.escape?CSS.escape(s):String(s).replace(/["'\\]/g,'\\$&'),
      isSafeUrl=u=>{const s=String(u||'').trim();return /^https?:\/\//i.test(s)||/^data:image\/(png|jpe?g|webp|gif);base64,/i.test(s)},
      $=id=>document.getElementById(id),
      dom={h:$('h'),q:$('q'),msg:$('msg'),main:$('main'),tools:$('tools'),f:$('f'),fCam:$('f-cam'),fImg:$('f-img'),fDoc:$('f-doc'),sh:$('sh-sel'),st:$('st-sel'),act:$('act-sel'),fb:$('box-form'),db:$('box-dash'),ls:$('list'),ft:$('ft'),tape:$('tape'),bn:$('box-nota'),scBox:$('scan-box'),scVid:$('scan-vid'),ib:$('img-box'),it:$('img-trg')};

const S2=s=>String(s||'').trim().toLowerCase(),
      N=v=>{if(v==null)return 0;let s=String(v).trim().replace(/\s+/g,'');if(s.includes(',')&&s.includes('.'))s=s.lastIndexOf(',')>s.lastIndexOf('.')?s.replace(/\./g,'').replace(',','.'):s.replace(/,/g,'');else if(s.includes(','))s=s.replace(',','.');const n=parseFloat(s);return(!isFinite(n)||n<0)?0:n},
      cols=(s=K.cur)=>K.cols[s]||[],
      cI=(s,k)=>cols(s).indexOf(k),
      cV=(r,s,k)=>{const i=cI(s,k);return(r&&i>-1&&r[i]!==undefined)?String(r[i]):''},
      vis=(s=K.cur)=>(K.vis[s]||cols(s)).filter(c=>cols(s).includes(c)),
      fvis=(s=K.cur)=>((K.fvis?.[s]?.length)?K.fvis[s]:cols(s)).filter(c=>cols(s).includes(c));

const TRXM={penjualan:{ch:'produk_jual',fk:'idpenjualan',p:'hargajual'},pembelian:{ch:'produk_beli',fk:'idpembelian',p:'hargabeli'}},
      isTrx=(s=K.cur)=>Boolean(TRXM[s]),
      childOf=s=>TRXM[s]?.ch||null,
      parentOf=s=>s==='produk_jual'?'penjualan':(s==='produk_beli'?'pembelian':null),
      fkCol=s=>s==='produk_jual'?'idpenjualan':(s==='produk_beli'?'idpembelian':(s==='cicil'?'idjual':null)),
      NO_ADD=['penjualan','pembelian','produk_jual','produk_beli','cicil','ecer'],
      EXEMPT=['back','menu','q','scan','dir','grid','closemsg','img-cls','scan-stop','more','reset-db','add','df-today','df-clear','login','reg','bypass','pwa-install','v-toko','v-dash','v-kas','v-shop','logout','refresh'];

const setView=(v,idx=null)=>{
  if(dom.q&&dom.q.value.trim()!==''){dom.q.value='';K.page=1;}
  if(K.v!==v||K.idx!==idx){
    K.hist.push({v:K.v,cur:K.cur,idx:K.idx,parent:K.parent?{...K.parent}:null,readyOnly:K.readyOnly,savedFormState:K.savedFormState});
    if(K.hist.length>25)K.hist.shift();
  }
  K.v=v;K.idx=idx;K.conf=null;
};
const getRole=()=>{if(!K.u)return'user';const uR=K.db.users?.find(r=>S2(r[0])===S2(K.u));return S2(uR?.[1]||'user');};
const isAdmin=()=>getRole()==='admin';
const isUserVisible=(r,sh=K.cur)=>{
  if(isAdmin()||sh==='produk')return true;
  const uCol=cI(sh,'user');
  if(uCol!==-1)return S2(r[uCol])===S2(K.u);
  const pSh=parentOf(sh);
  if(pSh){
    const fk=cV(r,sh,fkCol(sh)),pRow=K.db[pSh]?.find(p=>S2(p[0])===S2(fk));
    return pRow?isUserVisible(pRow,pSh):true;
  }
  return true;
};
const getRoleRules=()=>{if(!K.u)return[];const role=getRole(),v=(K.db.roles?.find(r=>S2(r[0])===role)?.[2]||'').toLowerCase();return v.split(',').map(S2).filter(Boolean);};
const hasAccess=(k,s=K.cur)=>{
  const rawK=S2(k);
  if(EXEMPT.includes(rawK)||rawK.startsWith('df-')||rawK.startsWith('ref-'))return true;
  if(!K.u)return false;
  if(isAdmin())return true;
  const role=getRole(),h=(K.db.roles?.find(r=>S2(r[0])===role)?.[1]||'').toLowerCase();
  if(!h)return true;
  const list=h.split(',').map(S2);
  return!(list.includes('all')||list.includes('*')||list.includes(rawK)||(s&&list.includes(`${S2(s)}.${rawK}`))||(s&&list.includes(S2(s))));
};
const addFooter=(a,txt,targetSheet=K.cur,extraAttr='')=>{if(hasAccess(a,targetSheet))dom.ft.insertAdjacentHTML('beforeend',`<button data-a="${esc(a)}" ${extraAttr}>${esc(txt)}</button> `);};
const genId=()=>{const d=new Date();return String(d.getHours()).padStart(2,'0')+String(d.getMinutes()).padStart(2,'0')+Math.random().toString(36).slice(2,5).padEnd(3,'0');};
const fmtNum=n=>N(n).toLocaleString('id-ID'),today=()=>new Date().toISOString().slice(0,10),openImg=src=>{if(isSafeUrl(src)){dom.it.src=src;dom.ib.hidden=false;render();}};
const syncStockDelta=(sh,o,n)=>{
  if(!['produk_jual','produk_beli','ecer'].includes(sh))return;
  const m=sh==='produk_beli'?1:-1,p=r=>r&&{id:cV(r,sh,'idproduk'),q:N(cV(r,sh,'qty'))};
  [{r:o,s:-m},{r:n,s:m}].forEach(({r,s})=>{
    const x=p(r),row=x?.id&&K.db.produk?.find(k=>S2(k[0])===S2(x.id));
    if(row&&cI('produk','stok')>-1)row[cI('produk','stok')]=String(N(row[cI('produk','stok')])+x.q*s);
  });
};
const msg=t=>{dom.msg.innerHTML='';dom.msg.hidden=!t;if(t)dom.msg.textContent=t;},
      dl=(str,name,mime)=>{const u=URL.createObjectURL(new Blob([str],{type:mime})),a=document.createElement('a');a.href=u;a.download=name;document.body.appendChild(a);a.click();setTimeout(()=>{a.remove();URL.revokeObjectURL(u);},500);},
      copyTxt=t=>{const ta=document.createElement('textarea');ta.value=t;document.body.appendChild(ta);ta.select();document.execCommand('copy');ta.remove();};

const calcItems=(t,id)=>{
  const ch=childOf(t);if(!ch)return{raw:0,disc:0,cogs:0,pot:0,items:[]};
  return(K.db[ch]||[]).reduce((a,r)=>{
    if(S2(cV(r,ch,fkCol(ch)))!==S2(id))return a;
    const q=N(cV(r,ch,'qty')),p=N(cV(r,ch,t==='penjualan'?'hargajual':'hargabeli')),b=N(cV(r,ch,'hargabeli')),s=N(cV(r,ch,'hargajual')),d=N(cV(r,ch,'diskon')),dLine=d>0?(d<p?d*q:d):0,dU=q>0?dLine/q:0;
    a.raw+=q*p;a.disc+=dLine;a.cogs+=q*b;a.pot+=q*s;
    a.items.push({name:cV(r,ch,'namaproduk')||'item',id:cV(r,ch,'idproduk'),q,p,b,s,dU,eff:Math.max(0,p-dU),tot:q*p-dLine});
    return a;
  },{raw:0,disc:0,cogs:0,pot:0,items:[]});
};
const calcContactDebt=kId=>{
  if(!kId)return 0;const cMap={};
  (K.db.cicil||[]).forEach(c=>{const cid=S2(cV(c,'cicil','idjual'));cMap[cid]=(cMap[cid]||0)+N(cV(c,'cicil','jumlah'));});
  return(K.db.penjualan||[]).reduce((sum,r)=>{
    if(S2(cV(r,'penjualan','idkontak'))!==S2(kId)||!isUserVisible(r,'penjualan'))return sum;
    const pid=r[0],{raw,disc}=calcItems('penjualan',pid),net=Math.max(0,raw-disc-N(cV(r,'penjualan','diskon'))),paid=N(cV(r,'penjualan','bayar'))+(cMap[S2(pid)]||0);
    return sum+Math.max(0,net-paid);
  },0);
};
const formatNota=id=>{
  const row=K.db.penjualan?.find(r=>S2(r[0])===S2(id));if(!row)return'nota kosong';
  const val=k=>cV(row,'penjualan',k),paid=N(val('bayar'))+(K.db.cicil||[]).reduce((a,c)=>a+(S2(cV(c,'cicil','idjual'))===S2(id)?N(cV(c,'cicil','jumlah')):0),0),{raw,disc,items}=calcItems('penjualan',id),gTot=Math.max(0,raw-disc-N(val('diskon'))),diff=paid-gTot,sisa=Math.abs(diff),W_L=32,
        pad=(s,c=' ')=>{const str=String(s||'').trim();if(str.length>=W_L)return str.slice(0,W_L);const lP=Math.floor((W_L-str.length)/2),rP=W_L-str.length-lP;return c.repeat(lP)+str+c.repeat(rP);},
        lr=(l,r)=>l+' '.repeat(Math.max(1,W_L-l.length-String(r).length))+r,
        lines=[pad(K.store.name),K.store.sub&&pad(K.store.sub),K.store.phone&&pad(K.store.phone),K.store.bank&&pad(K.store.bank),'='.repeat(W_L),lr(`${val('date')||today()} ${id}`,val('user')||K.u||'admin'),'-'.repeat(W_L)].filter(Boolean);
  if(!items.length)lines.push('(kosong)');else items.forEach(it=>{lines.push(it.name);if(it.dU>0)lines.push(`${fmtNum(it.p)} -disc ${fmtNum(it.dU)}`);lines.push(lr(`${it.q}x ${fmtNum(it.eff)}`,fmtNum(it.tot)));});
  lines.push('-'.repeat(W_L),lr('Subtotal',fmtNum(raw)));if(disc>0)lines.push(lr('Diskon Item','-'+fmtNum(disc)));if(N(val('diskon'))>0)lines.push(lr('Diskon','-'+fmtNum(val('diskon'))));lines.push(lr('Total',fmtNum(gTot)),lr('Bayar',fmtNum(paid)),lr(diff>=0?'Kembali':'Kurang',fmtNum(sisa)),'-'.repeat(W_L));
  if(K.store.footer)lines.push(pad(K.store.footer));
  const kId=val('idkontak'),kRow=kId&&K.db.kontak?.find(r=>S2(r[0])===S2(kId)),kDisp=kRow?(cV(kRow,'kontak','nama')||kId):kId;
  if(kDisp)lines.push(' '+kDisp);if(val('catatan'))lines.push(' '+val('catatan'));
  const totPiutangKontak=calcContactDebt(kId);if(totPiutangKontak>0)lines.push(lr('Total Piutang','-'+fmtNum(totPiutangKontak)));
  return lines.join('\n');
};
const syncTrx=t=>{
  const id=K.parent?.id||(K.idx>=0?K.db[t]?.[K.idx]?.[0]:null);if(!id)return;
  const r=K.db[t]?.find(x=>S2(x[0])===S2(id));if(!r)return;
  const{raw,disc}=calcItems(t,id),net=Math.max(0,raw-disc-N(cV(r,t,'diskon'))),paid=N(cV(r,t,'bayar'))+(t==='penjualan'?(K.db.cicil||[]).reduce((a,c)=>a+(S2(cV(c,'cicil','idjual'))===S2(id)?N(cV(c,'cicil','jumlah')):0),0):0),kId=cV(r,t,'idkontak'),sI=cI(t,'status');
  if(sI>-1&&r[sI]!=='revisi'){
    if(net<=0)r[sI]='draft';else if(paid>=net)r[sI]='lunas';else if(kId)r[sI]='piutang';else r[sI]='draft';
  }
  persist();save(r,t,1);
};
const getOpenTrx=t=>{
  if(!t)return null;const rows=K.db[t]||[],u=S2(K.u||'admin');
  if(K.parent?.type===t){const r=rows.find(x=>S2(x[0])===S2(K.parent.id));if(r&&isUserVisible(r,t)&&['draft','revisi'].includes(S2(cV(r,t,'status'))))return{type:t,id:r[0],row:r,status:cV(r,t,'status')};}
  const r=rows.find(x=>(isAdmin()||S2(cV(x,t,'user'))===u)&&['draft','revisi'].includes(S2(cV(x,t,'status'))));
  return r?{type:t,id:r[0],row:r,status:cV(r,t,'status')||'draft'}:null;
};
const hasDraftItems=t=>{const ch=childOf(t),op=getOpenTrx(t);return Boolean(ch&&op&&K.db[ch]?.some(r=>S2(cV(r,ch,fkCol(ch)))===S2(op.id)));};
const openProdForm=p=>{
  const isB=K.parent?.type==='pembelian',pType=isB?'pembelian':'penjualan';
  K.parent=getOpenTrx(pType)||{type:pType,id:genId(),open:true};
  K.cur=isB?'produk_beli':'produk_jual';
  K.prefill={idproduk:p[0],namaproduk:p[3],hargabeli:p[4],hargajual:p[5],qty:'1',diskon:'',catatan:''};
  K.prodIdx=(K.db.produk||[]).indexOf(p);setView('form',-1);render();
};
const resolveRef=c=>{
  const col=S2(c);
  if(col==='user'||col==='idproduk'||col==='namaproduk'||['produk_jual','produk_beli','ecer'].includes(K.cur))return null;
  const direct=K.shs.find(s=>S2(s)===col);if(direct)return direct;
  if(col.startsWith('id')){
    const naked=col.slice(2),exact=K.shs.find(s=>S2(s)===naked);if(exact)return exact;
    if(naked==='jual'||naked==='penjualan')return K.shs.find(s=>s==='penjualan')||null;
    if(naked==='beli'||naked==='pembelian')return K.shs.find(s=>s==='pembelian')||null;
    return K.shs.find(s=>S2(s).includes(naked)||naked.includes(S2(s)))||null;
  }
  return null;
};
const queryRows=()=>{
  const rows=K.db[K.cur]||[],q=S2(dom.q.value).split(/\s+/).filter(Boolean),pType=parentOf(K.cur),op=pType?getOpenTrx(pType):null,res=[];
  for(let i=0;i<rows.length;i++){
    const r=rows[i]||[];if(!isUserVisible(r,K.cur))continue;
    if(K.cur==='produk'&&K.readyOnly&&N(cV(r,'produk','stok'))<=0)continue;
    if(pType&&op&&S2(cV(r,K.cur,fkCol(K.cur)))!==S2(op.id))continue;
    if(!q.length||q.every(w=>r.join(' ').toLowerCase().includes(w)))res.push(i);
  }
  if(K.sort>0&&K.sort<=cols().length){
    const ci=K.sort-1;res.sort((a,b)=>String(rows[a]?.[ci]||'').localeCompare(String(rows[b]?.[ci]||''),undefined,{numeric:true})*K.dir);
  }else if(K.dir===-1)res.reverse();
  return res;
};

const renderChrome=()=>{
  const allowed=K.shs.filter(s=>hasAccess(s));
  if(dom.sh.dataset.shs!==allowed.join(',')){dom.sh.innerHTML=allowed.map(s=>`<option value="${esc(s)}">${esc(s)}</option>`).join('');dom.sh.dataset.shs=allowed.join(',');}
  if(!allowed.includes(K.cur)&&allowed.length)K.cur=allowed[0];
  dom.sh.value=K.cur;dom.sh.hidden=!hasAccess('sh-sel');
  const c=cols();
  if(dom.st.dataset.cur!==K.cur||dom.st.children.length!==(c.length+1)){
    dom.st.innerHTML='<option value="0">srt</option>'+c.map((col,i)=>`<option value="${i+1}">${esc(col)}</option>`).join('');
    dom.st.dataset.cur=K.cur;
  }
  dom.st.value=String(K.sort);dom.st.hidden=!hasAccess('st-sel');
  dom.tools.querySelector('[data-a="dir"]').hidden=!hasAccess('dir');
  dom.tools.querySelector('[data-a="grid"]').hidden=!hasAccess('grid');
  dom.act.hidden=!hasAccess('act-sel');
  Array.from(dom.act.options).forEach(opt=>{if(opt.value)opt.hidden=!hasAccess(opt.value);});
  const addBtn=dom.h.querySelector('[data-a="add"]');if(addBtn)addBtn.hidden=K.v!=='list'||NO_ADD.includes(K.cur)||!hasAccess('add',K.cur);
};
const renderMenuFooters=()=>{addFooter('back','<',K.cur);addFooter('refresh','refresh',K.cur);addFooter('v-toko','toko');addFooter('reset-db','reset db');if(K.pwaEvent)addFooter('pwa-install','install pwa');addFooter('logout','logout');};
const renderNavFooters=()=>{if(K.v!=='dash')addFooter('v-dash','dashboard');if(K.cur!=='kas'||K.v==='dash')addFooter('v-kas','kas');if(K.cur!=='produk'||K.v==='dash')addFooter('v-shop','produk');if(K.cur!=='produk_jual'&&hasDraftItems('penjualan'))addFooter('v-jual','jual');if(K.cur!=='produk_beli'&&hasDraftItems('pembelian'))addFooter('v-beli','beli');};

const renderDash=()=>{
  msg();
  dom.db.insertAdjacentHTML('beforeend',`<input placeholder="user" data-df="u" value="${esc(K.df.u)}"> <input type="date" data-df="from" value="${esc(K.df.from)}"> <input type="date" data-df="to" value="${esc(K.df.to)}"> <input placeholder="kategori" data-df="kat" value="${esc(K.df.kat)}">`);
  addFooter('df-today','hari ini');addFooter('df-clear','clear filter');
  const qW=S2(dom.q.value).split(/\s+/).filter(Boolean),effU=isAdmin()?K.df.u:(K.u||''),passFlt=(u,d,kt)=>(!effU||S2(u).includes(S2(effU)))&&(!K.df.from||String(d||'')>=K.df.from)&&(!K.df.to||String(d||'')<=K.df.to)&&(!K.df.kat||S2(kt).includes(S2(K.df.kat)))&&(!qW.length||qW.every(w=>`${u} ${d} ${kt}`.toLowerCase().includes(w)));
  let mAll=0,kAll=0,mFil=0,kFil=0;
  K.db.kas?.forEach(r=>{if(!isUserVisible(r,'kas'))return;const m=N(cV(r,'kas','masuk')),k=N(cV(r,'kas','keluar'));mAll+=m;kAll+=k;if(passFlt(cV(r,'kas','user'),cV(r,'kas','date'),`${cV(r,'kas','kategori')} ${cV(r,'kas','uraian')}`)){mFil+=m;kFil+=k;}});
  const cMap={},cFilMap={};
  K.db.cicil?.forEach(c=>{if(!isUserVisible(c,'cicil'))return;const cid=S2(cV(c,'cicil','idjual')),jml=N(cV(c,'cicil','jumlah'));cMap[cid]=(cMap[cid]||0)+jml;if(passFlt(cV(c,'cicil','user'),cV(c,'cicil','date'),''))cFilMap[cid]=(cFilMap[cid]||0)+jml;});
  let jAll=0,profFil=0,jFil=0,pAll=0,pFil=0;
  K.db.penjualan?.forEach(r=>{
    if(!isUserVisible(r,'penjualan'))return;
    const pid=r[0],{raw,disc,cogs,items}=calcItems('penjualan',pid),net=Math.max(0,raw-disc-N(cV(r,'penjualan','diskon'))),paid=N(cV(r,'penjualan','bayar'));
    jAll+=net;if((paid+(cMap[S2(pid)]||0))<net)pAll+=(net-(paid+(cMap[S2(pid)]||0)));
    const ktExtra=`${cV(r,'penjualan','idkontak')} ${cV(r,'penjualan','catatan')} `+items.map(i=>`${cV(K.db.produk?.find(p=>p[0]===i.id),'produk','kategori')}`).join(' ');
    if(passFlt(cV(r,'penjualan','user'),cV(r,'penjualan','date'),ktExtra)){jFil+=net;profFil+=(net-cogs);if((paid+(cFilMap[S2(pid)]||0))<net)pFil+=(net-(paid+(cFilMap[S2(pid)]||0)));}
  });
  let ecQtyFil=0;
  K.db.ecer?.forEach(r=>{if(!isUserVisible(r,'ecer'))return;const q=N(cV(r,'ecer','qty')),net=Math.max(0,(q*N(cV(r,'ecer','hargajual')))-N(cV(r,'ecer','diskon')));jAll+=net;if(passFlt(cV(r,'ecer','user'),cV(r,'ecer','date'),cV(r,'ecer','catatan'))){jFil+=net;profFil+=(net-(q*N(cV(r,'ecer','hargabeli'))));ecQtyFil+=q;}});
  let bFil=0;
  K.db.pembelian?.forEach(r=>{if(!isUserVisible(r,'pembelian'))return;if(passFlt(cV(r,'pembelian','user'),cV(r,'pembelian','date'),`${cV(r,'pembelian','idkontak')} ${cV(r,'pembelian','catatan')}`)){const{raw,disc}=calcItems('pembelian',r[0]);bFil+=Math.max(0,raw-disc-N(cV(r,'pembelian','diskon')));}});
  let sStok=0,rCount=0,eMod=0,eJual=0;
  K.db.produk?.forEach(r=>{const s=N(cV(r,'produk','stok'));if(passFlt('','',`${cV(r,'produk','subkategori')} ${cV(r,'produk','kategori')}`)){sStok+=s;if(s>0){rCount++;eMod+=(s*N(cV(r,'produk','hargabeli')));eJual+=(s*N(cV(r,'produk','hargajual')));}}});
  const stMasuk=(K.db.produk_beli||[]).reduce((a,r)=>a+(isUserVisible(r,'produk_beli')?N(cV(r,'produk_beli','qty')):0),0),stKeluar=(K.db.produk_jual||[]).reduce((a,r)=>a+(isUserVisible(r,'produk_jual')?N(cV(r,'produk_jual','qty')):0),0)+ecQtyFil;
  [{l:'saldo total',v:fmtNum(mAll+jAll-kAll-pAll),s:'kas',k:'saldo_total'},{l:'saldo filter',v:fmtNum(mFil+jFil-kFil-pFil),s:'penjualan',k:'saldo_filter'},{l:'user',v:K.u||'admin',s:'users',k:'users'},{l:'profit',v:fmtNum(profFil),s:'penjualan',k:'profit'},{l:'kas masuk',v:fmtNum(mFil),s:'kas',k:'kas_masuk'},{l:'kas keluar',v:fmtNum(kFil),s:'kas',k:'kas_keluar'},{l:'total penjualan',v:fmtNum(jFil),s:'penjualan',k:'total_penjualan'},{l:'total pembelian',v:fmtNum(bFil),s:'pembelian',k:'total_pembelian'},{l:'total piutang',v:fmtNum(pFil),s:'penjualan',k:'total_piutang'},{l:'stok masuk',v:fmtNum(stMasuk),s:'produk_beli',k:'stok_masuk'},{l:'stok keluar',v:fmtNum(stKeluar),s:'produk_jual',k:'stok_keluar'},{l:'sisa stok',v:fmtNum(sStok),s:'produk',k:'sisa_stok'},{l:'produk ready stok',v:rCount+' item',s:'produk',slice:'ready',k:'ready'},{l:'estimasi modal ready',v:fmtNum(eMod),s:'produk',slice:'ready',k:'modal_ready'},{l:'estimasi jual ready',v:fmtNum(eJual),s:'produk',slice:'ready',k:'jual_ready'},{l:'estimasi profit ready',v:fmtNum(eJual-eMod),s:'produk',slice:'ready',k:'profit_ready'}].forEach(c=>{if(hasAccess(c.k)&&hasAccess(c.s))dom.db.insertAdjacentHTML('beforeend',`<button data-a="nav" data-s="${esc(c.s)}" ${c.slice?`data-slice="${esc(c.slice)}"`:''}>${esc(c.l)}: <b>${esc(c.v)}</b></button>`);});
  renderNavFooters();
};

const renderForm=()=>{
  if(!K.u){msg('login');dom.fb.innerHTML='<input placeholder="user" autocomplete="off" data-f="user"><input type="password" placeholder="password" data-f="pass">';addFooter('login','login');addFooter('reg','register');addFooter('bypass','bypass');return;}
  if(K.cur==='cms'&&K.v==='cms_detail'){msg('cms detail');const r=K.db.cms?.[K.idx]||[],gV=k=>cV(r,'cms',k),fotoSafe=isSafeUrl(gV('foto'))?gV('foto'):'';dom.fb.innerHTML=`<b>${esc(gV('judul')||'(tanpa judul)')}</b><small>${esc(gV('date'))} ${gV('user')?'| '+esc(gV('user')):''} ${gV('kategori')?'| '+esc(gV('kategori')):''} ${gV('status')?'| ['+esc(gV('status'))+']':''}</small><p>${esc(gV('konten'))}</p>${fotoSafe?`<img src="${esc(fotoSafe)}" data-img-open="${esc(fotoSafe)}" alt="cms">`:''}`;addFooter('back','<',K.cur);addFooter('edit-cms','edit',K.cur);if(fotoSafe||gV('file')||gV('konten'))addFooter('dl-cms-file','download',K.cur);renderNavFooters();return;}
  if(K.v==='bulksheet'){msg('bulksheet');dom.fb.innerHTML=`<textarea rows="10" placeholder="sheet:kolom,kolom">${esc(K.shs.map(s=>`${s}:${(K.cols[s]||[]).join(',')}`).join('\n'))}</textarea>`;addFooter('back','<',K.cur);addFooter('save-bulksheet','save',K.cur);renderNavFooters();return;}
  if(K.v==='toko'){msg('toko');dom.fb.innerHTML=`<input placeholder="name" value="${esc(K.store.name)}" data-cfg="name"><input placeholder="sub" value="${esc(K.store.sub)}" data-cfg="sub"><input placeholder="phone" value="${esc(K.store.phone)}" data-cfg="phone"><input placeholder="bank" value="${esc(K.store.bank)}" data-cfg="bank"><textarea placeholder="footer" data-cfg="footer">${esc(K.store.footer)}</textarea>`;addFooter('back','<',K.cur);addFooter('save-store','save',K.cur);renderNavFooters();return;}
  if(K.v==='schema'){msg(`schema ${K.cur}`);dom.fb.innerHTML=`<input placeholder="cols" value="${esc(cols().join(','))}" data-f="cols"><input placeholder="vis (list)" value="${esc(vis().join(','))}" data-f="vis"><input placeholder="form cols" value="${esc(fvis().join(','))}" data-f="form">`;addFooter('back','<',K.cur);addFooter('save-schema','save',K.cur);renderNavFooters();return;}
  const c=cols(),bypassAdd=K.idx===-1&&['produk','kas'].includes(K.cur),formCols=bypassAdd?fvis():fvis().filter(col=>hasAccess(col,K.cur)),row=(K.idx!==null&&K.idx>=0)?(K.db[K.cur][K.idx]||[]):[],isDraft=isTrx()&&S2(cV(row,K.cur,'status'))==='draft';
  if(isTrx()&&K.idx!==null&&K.idx>=0){const{raw,disc,cogs,pot}=calcItems(K.cur,row[0]),net=Math.max(0,raw-disc-N(cV(row,K.cur,'diskon'))),showProf=hasAccess('profit',K.cur);msg(`total: ${fmtNum(net)}${showProf?` | profit: ${fmtNum(K.cur==='penjualan'?(net-cogs):(pot-net))}`:''}`);}else msg(K.cur);
  formCols.forEach(col=>{
    let val=(K.idx===-1&&c.indexOf(col)===0)?genId():(row[c.indexOf(col)]??'');
    if(K.savedFormState&&K.savedFormState[col]!=null)val=K.savedFormState[col];
    else if(K.idx===-1&&K.prefill?.[col]!=null)val=K.prefill[col];
    else if(K.idx===-1&&!val&&/date|tanggal/i.test(col))val=today();
    else if(K.idx===-1&&!val&&col==='user')val=K.u||'admin';
    else if(K.idx===-1&&!val&&K.cur==='cms'&&col==='status')val='publish';
    const isLong=/catatan|alamat|uraian|konten|akses|hide|validasi/i.test(col),isDate=/date|tanggal/i.test(col),tag=isLong?'textarea':'input',typeAttr=isDate?'type="date"':'';
    dom.fb.insertAdjacentHTML('beforeend',`<${tag} ${typeAttr} placeholder="${esc(col)}" value="${esc(val)}" data-col="${esc(col)}">${isLong?esc(val):''}</${tag}>`);
    updateFormThumb(dom.fb.querySelector(`[data-col="${qEsc(col)}"]`));
  });
  K.savedFormState=null;
  if(K.idx===-1&&getRoleRules().includes('wajiblokasi')){
    const locInp=Array.from(dom.fb.querySelectorAll('[data-col]')).find(i=>/lokasi|alamat/i.test(i.dataset.col));
    if(locInp&&!locInp.value&&navigator.geolocation)navigator.geolocation.getCurrentPosition(p=>{if(!locInp.value)locInp.value=`${p.coords.latitude.toFixed(6)},${p.coords.longitude.toFixed(6)}`;},()=>{locInp.value='here()';},{timeout:4000});
  }
  addFooter('back','<',K.cur);addFooter('save-row','save',K.cur);
  if(isTrx())addFooter('trx-pick-kontak','kontak',K.cur);
  formCols.forEach(col=>{const trgSh=resolveRef(col),val=(dom.fb.querySelector(`[data-col="${qEsc(col)}"]`)?.value||'').trim();if(trgSh&&trgSh!==K.cur&&K.shs.includes(trgSh)&&(K.db[trgSh]||[]).length>0&&val!=='')addFooter(`ref-${trgSh}`,`ref ${trgSh}`,K.cur,`data-ref-col="${esc(col)}"`);});
  if(K.cur==='produk'&&K.idx===-1)addFooter('scan-prod','scan',K.cur);
  if(formCols.some(k=>/foto/i.test(k))){addFooter('cam','kamera',K.cur);addFooter('pic','file',K.cur);}
  if(K.cur==='produk')addFooter('gimg','gimg',K.cur);
  if(K.cur==='cms'){addFooter('up-cms-file','upload',K.cur);if(formCols.some(k=>/foto|file/i.test(k))&&(cV(row,K.cur,'foto')||cV(row,K.cur,'file')))addFooter('dl-cms-file','download',K.cur);}
  if(formCols.some(k=>/alamat|lokasi/i.test(k))){addFooter('gps','gps',K.cur);if(/(-?\d+\.\d+),\s*(-?\d+\.\d+)/.test(formCols.map(col=>row[c.indexOf(col)]||'').join(' ')))addFooter('navigasi','navigasi',K.cur);}
  if(K.cur==='produk'&&K.idx!==null&&K.idx>=0){addFooter('jual-prod','jual',K.cur);addFooter('beli-prod','beli',K.cur);addFooter('ecer-prod','ecer',K.cur);}
  if(['produk_jual','produk_beli','ecer'].includes(K.cur)&&row.length){const lk=cV(row,K.cur,'idproduk');if(lk){const pi=(K.db.produk||[]).findIndex(p=>S2(cV(p,'produk','idproduk')||p[0])===S2(lk));if(pi!==-1)K.prodIdx=pi;}}
  if(K.prodIdx!==null)addFooter('edit-prod','edit produk',K.cur);
  if(isTrx()&&K.idx!==null&&K.idx>=0){
    if(!isDraft)addFooter('revisi','revisi',K.cur);
    addFooter('pay-pas','pas',K.cur);
    if(!isDraft)addFooter('trx-rincian',childOf(K.cur),K.cur);
    if(K.cur==='penjualan')addFooter('show-nota','nota',K.cur);
    addFooter('cicil','cicil',K.cur);
    if(!isDraft)addFooter('print','print',K.cur);
  }
  if(K.cur==='kontak'&&(K.parent||getOpenTrx('penjualan')||getOpenTrx('pembelian')))addFooter('trx-set-kontak','trx',K.cur);
  if(K.idx!==null&&K.idx>=0){if(!isDraft)addFooter('dup-row','dup',K.cur);addFooter('del-row',K.conf==='del'?'sure?':'del',K.cur);}
  renderNavFooters();
};

const renderList=isBulkEdit=>{
  msg();
  const c=cols(),vCols=vis(),rows=K.db[K.cur]||[],matched=queryRows(),vIdxs=c.map((col,i)=>vCols.includes(col)?i:-1).filter(i=>i!==-1),eff=vIdxs.length?vIdxs:[0],imgI=cI(K.cur,'foto')!==-1?cI(K.cur,'foto'):cI(K.cur,'foto_url'),cmsFotoI=K.cur==='cms'?cI('cms','foto'):-1,limit=Math.min(K.page*25,matched.length);
  for(let m=0;m<limit;m++){
    const trg=matched[m],r=rows[trg]||[],rowEl=document.createElement('div');
    rowEl.dataset.idx=trg;
    if(isBulkEdit){
      rowEl.contentEditable='plaintext-only';if(!rowEl.isContentEditable)rowEl.contentEditable='true';
      rowEl.textContent=r.join('|');
    }else{
      if(K.grid&&imgI!==-1&&isSafeUrl(r[imgI])){const im=document.createElement('img');im.src=r[imgI];im.alt='prod';im.onerror=()=>im.remove();rowEl.appendChild(im);rowEl.appendChild(document.createElement('br'));}
      if(K.cur==='cms'&&cmsFotoI!==-1&&isSafeUrl(r[cmsFotoI])){const cTh=document.createElement('img');cTh.src=r[cmsFotoI];cTh.alt='cms';cTh.dataset.imgOpen=r[cmsFotoI];cTh.onerror=()=>cTh.remove();rowEl.appendChild(cTh);rowEl.appendChild(document.createTextNode(' '));}
      rowEl.appendChild(document.createTextNode(eff.map(i=>{const val=r[i];if(!val)return'';if(K.cur==='cms'&&i===cmsFotoI)return'';return c[i]==='konten'?(String(val).replace(/\s+/g,' ').slice(0,50)+'...'):val;}).filter(Boolean).join(' ')||'-'));
    }
    dom.ls.appendChild(rowEl);
  }
  if(isBulkEdit){addFooter('back','<',K.cur);addFooter('save-bulk','save',K.cur);}
  else{
    const pType=parentOf(K.cur),op=pType?getOpenTrx(pType):null;
    if(pType&&op){
      K.parent={type:pType,id:op.id,open:true};const{raw,disc}=calcItems(pType,op.id);
      addFooter('clear-items',K.conf==='clear-items'?'sure?':'clear',K.cur);addFooter('add-cart-item','+item',K.cur);addFooter('v-selesai',`${pType} (${fmtNum(raw-disc)})`,K.cur);if(pType==='penjualan')addFooter('show-nota','nota',K.cur);renderNavFooters();
    }else renderNavFooters();
  }
  if(limit<matched.length)addFooter('more','more',K.cur);
};

const renderNota=()=>{msg('nota');dom.tape.textContent=K.receiptRaw;addFooter('back','<',K.cur);addFooter('rawbt','rawbt',K.cur);addFooter('jpg','jpg',K.cur);addFooter('print','print',K.cur);addFooter('copy-nota','copy',K.cur);renderNavFooters();};
const updateFormThumb=inp=>{if(!inp)return;const col=inp.dataset.col||'',val=(inp.value||'').trim();let th=dom.fb.querySelector(`img[data-thumb-col="${qEsc(col)}"]`);if(val&&(/foto/i.test(col)||isSafeUrl(val))){if(!th){th=document.createElement('img');th.dataset.thumbCol=col;th.alt='thumb';th.onerror=()=>th.remove();inp.insertAdjacentElement('afterend',th);}th.src=val;th.dataset.imgOpen=val;}else if(th)th.remove();};
const stopScan=()=>{K.scanActive=false;if(K.scanStream){K.scanStream.getTracks().forEach(t=>t.stop());K.scanStream=null;}dom.scBox.hidden=true;render();};
const startScan=trg=>{
  K.scanTarget=trg||'q';if(!navigator.mediaDevices?.getUserMedia)return msg('kamera tidak didukung');
  dom.scBox.hidden=false;K.scanActive=true;render();
  navigator.mediaDevices.getUserMedia({video:{facingMode:'environment'}}).then(s=>{
    if(!K.scanActive){s.getTracks().forEach(t=>t.stop());return;}
    K.scanStream=s;dom.scVid.srcObject=s;dom.scVid.play();
    const det=W.BarcodeDetector?new W.BarcodeDetector():null,loop=()=>{
      if(dom.scBox.hidden||!K.scanActive)return;
      if(det)det.detect(dom.scVid).then(b=>{
        if(b.length){
          const sc=String(b[0].rawValue||'').trim();stopScan();
          if(K.scanTarget==='q'){dom.q.value=sc;K.page=1;render();}
          else{
            const inp=dom.fb.querySelector('[data-col="idproduk"]');
            if(inp){inp.value=sc;const f=K.db.produk?.find(p=>S2(cV(p,'produk','idproduk')||p[0])===S2(sc));if(f)cols('produk').forEach((c,i)=>{const el=dom.fb.querySelector(`[data-col="${qEsc(c)}"]`);if(el)el.value=f[i]||'';});}
          }
        }else if(K.scanActive)requestAnimationFrame(loop);
      }).catch(()=>{if(K.scanActive)requestAnimationFrame(loop);});
      else if(K.scanActive)requestAnimationFrame(loop);
    };
    if(K.scanActive)requestAnimationFrame(loop);
  }).catch(()=>{msg('kamera offline/diblokir');stopScan();});
};

const persist=()=>{try{L.setItem('apex',JSON.stringify({cur:K.cur,u:K.u,store:K.store,shs:K.shs,cols:K.cols,vis:K.vis,fvis:K.fvis,db:K.db,parent:K.parent,grid:K.grid,df:K.df}));}catch{}};

const render=()=>{
  dom.act.value='';dom.fb.innerHTML=dom.ft.innerHTML=dom.db.innerHTML=dom.ls.innerHTML='';document.body.dataset.grid=K.grid?'1':'0';
  if(!dom.scBox.hidden)addFooter('scan-stop','x');if(!dom.ib.hidden)addFooter('img-cls','tutup gambar');
  if(!K.u){dom.h.hidden=dom.tools.hidden=dom.db.hidden=dom.bn.hidden=dom.ls.hidden=true;dom.fb.hidden=false;renderForm();return;}
  const isF=['toko','schema','bulksheet','cms_detail'].includes(K.v)||K.idx!==null,m=K.v==='menu'?'menu':K.v==='dash'?'dash':K.v==='nota'?'nota':isF?'form':K.v==='edit'?'edit':'list';
  dom.h.hidden=m==='form';dom.tools.hidden=m!=='list'&&m!=='edit';dom.fb.hidden=m!=='form';dom.db.hidden=m!=='dash';dom.bn.hidden=m!=='nota';dom.ls.hidden=m!=='list'&&m!=='edit';
  renderChrome();
  if(m==='menu')renderMenuFooters();else if(m==='dash')renderDash();else if(m==='nota')renderNota();else if(m==='form')renderForm();else renderList(m==='edit');
};

const actions={
  closemsg:()=>{K.conf=null;msg();render();},
  login:()=>{
    const u=S2(dom.fb.querySelector('[data-f="user"]')?.value),p=(dom.fb.querySelector('[data-f="pass"]')?.value||'').trim();
    if(!u)return msg('user required');
    const f=K.db.users?.find(r=>S2(r[0])===u);
    if(f){if(f[2]&&f[2]!==p&&f[1]!==p)return msg('wrong pass');K.u=L.u=f[0];}
    else if(!K.db.users?.length&&u==='admin'){K.db.users=[['admin','admin','admin']];K.u=L.u='admin';}
    else return msg('user not found');
    persist();syn(true);setView('dash',null);msg();render();
  },
  reg:()=>{
    const u=S2(dom.fb.querySelector('[data-f="user"]')?.value),p=(dom.fb.querySelector('[data-f="pass"]')?.value||'').trim();
    if(!u)return msg('user required');if(K.db.users?.some(r=>S2(r[0])===u))return msg('user exists');
    const newU=[u,'user',p];(K.db.users=K.db.users||[]).push(newU);K.u=L.u=u;persist();save(newU,'users',2);setView('dash',null);msg();render();
  },
  bypass:()=>{if(!K.db.users?.length)K.db.users=[['admin','admin','admin']];K.u=L.u=K.db.users[0]?.[0]||'admin';persist();syn(true);setView('dash',null);msg();render();},
  back:()=>{
    if(!dom.scBox.hidden)return stopScan();if(!dom.ib.hidden)return actions['img-cls']();if(K.v==='menu'){dom.q.value='';K.v='dash';return render();}
    const inForm=['form','toko','schema','bulksheet','cms_detail','edit'].includes(K.v)||K.idx!==null;
    K.idx=null;K.prefill=null;K.prodIdx=null;K.conf=null;K.refTargetInp=null;K.savedFormState=null;
    if(inForm){while(K.hist.length){const prev=K.hist.pop();if(prev&&!['form','toko','schema','bulksheet','cms_detail','edit'].includes(prev.v)&&prev.idx===null){K.cur=prev.cur||K.cur;K.v=prev.v||'list';K.parent=prev.parent||null;K.readyOnly=Boolean(prev.readyOnly);return render();}}K.v=K.cur?'list':'dash';return render();}
    if(K.hist.length){const p=K.hist.pop();K.cur=p.cur||K.cur;K.v=p.v||'dash';K.idx=p.idx!=null?p.idx:null;K.parent=p.parent||null;K.readyOnly=Boolean(p.readyOnly);return render();}
    if(K.v!=='dash'){K.v='dash';K.idx=null;K.readyOnly=false;return render();}
  },
  menu:()=>{if(!dom.scBox.hidden)stopScan();if(K.v==='menu'){dom.q.value='';K.v='dash';}else{K.v='menu';dom.q.value='';}render();},
  refresh:async()=>{
    msg('syncing dataset & tables...');
    await req({a:'ensure_all', tn:K.shs, cols:K.cols, gasUrl:HARD.gasUrl});
    const muts = await ga('mut');
    await req({
      a: 'push',
      m: typeof pack === 'function' ? pack(muts) : muts,
      tn: K.shs,
      db: K.db,
      cols: K.cols,
      shs: K.shs,
      gasUrl: HARD.gasUrl
    });
    if (muts.length) await rm('mut', muts.map(x => x.i));
    await syn(true);
    await testConn();
    render();
  },
  'reset-db':async()=>{
    if(K.conf!=='reset-db'){K.conf='reset-db';return msg('kosongkan db & cache? klik lagi');}
    K.conf=null;
    msg('membersihkan cache & db offline...');

    if('caches' in window){
      try{
        const ks=await caches.keys();
        await Promise.all(ks.map(k=>caches.delete(k)));
      }catch(_){}
    }

    K.shs.forEach(s=>{
      K.db[s]=[];
      idbClearStore(s);
    });
    idbClearStore('mut');

    L.vs='[]';
    persist();
    render();

    msg('db & cache bersih. sync...');
    await syn(true);
    await testConn();
    render();
  },
  f:el=>{
    const file=el.files?.[0];if(!file)return;el.value='';const rd=new FileReader();
    rd.onload=e=>{
      const ls=(e.target.result||'').split(/\r?\n/).map(s=>s.trim()).filter(Boolean);if(!ls.length)return msg('empty');
      const c=cols(),res=[];
      ls.slice(1).forEach(l=>{const r=l.split('\t');if(r.some(v=>v.trim()!=='')){while(r.length<c.length)r.push('');const rowItem=r.slice(0,c.length);res.push(rowItem);save(rowItem,K.cur,2);}});
      if(res.length){K.db[K.cur]=res;persist();syn(true);render();}else msg('data kosong');
    };
    rd.readAsText(file);
  }
};

/* Delegasi Peristiwa Global */
document.addEventListener('click',e=>{
  const b=e.target.closest('[data-a]');
  if(b){
    const a=b.dataset.a;
    if(a.startsWith('ref-')){
      const trg=a.replace('ref-',''),cName=b.dataset.refCol||'',val=cName?(dom.fb.querySelector(`[data-col="${qEsc(cName)}"]`)?.value||''):'',curState={};
      dom.fb.querySelectorAll('[data-col]').forEach(inp=>{curState[inp.dataset.col]=inp.value;});
      K.savedFormState=curState;K.refTargetInp=cName;K.cur=trg;dom.q.value=val.trim();K.page=1;setView('list',null);return render();
    }
    if(!['del-row','clear-items','act-sel','reset-db'].includes(a)&&K.conf){K.conf=null;msg();}
    return actions[a]?.(b,e);
  }
  const imgTrg=e.target.closest('[data-img-open]');
  if(imgTrg){e.stopPropagation();return openImg(imgTrg.dataset.imgOpen);}
  const r=e.target.closest('#list>[data-idx]');
  if(r&&K.v==='list'&&K.idx===null){
    const idx=parseInt(r.dataset.idx,10);
    if(K.refTargetInp&&K.hist.length){
      const prevSnap=K.hist.pop(),selId=K.db[K.cur]?.[idx]?.[0]||'';
      K.cur=prevSnap.cur;K.v=prevSnap.v;K.idx=prevSnap.idx;K.parent=prevSnap.parent;K.readyOnly=Boolean(prevSnap.readyOnly);
      if(prevSnap.savedFormState)K.savedFormState=prevSnap.savedFormState;
      if(!K.savedFormState)K.savedFormState={};
      K.savedFormState[K.refTargetInp]=selId;K.refTargetInp=null;return render();
    }
    if(K.cur==='cms'){setView('cms_detail',idx);return render();}
    if(K.cur==='produk')return openProdForm(K.db.produk[idx]||[]);
    if(isTrx()&&K.db[K.cur]?.[idx]?.[0])K.parent={type:K.cur,id:K.db[K.cur][idx][0]};
    setView('form',idx);render();
  }
});

document.addEventListener('input',e=>{
  if(e.target.id==='q'){if(K.v==='menu'){const qm=S2(e.target.value);Array.from(dom.ft.querySelectorAll('button[data-a]')).forEach(btn=>{btn.hidden=qm&&!btn.textContent.toLowerCase().includes(qm);});}else{K.page=1;render();}return;}
  if(e.target.dataset.df){
    const k=e.target.dataset.df;K.df[k]=e.target.value;persist();const pos=e.target.selectionStart;render();
    const next=dom.db.querySelector(`[data-df="${qEsc(k)}"]`);if(next){next.focus();if(typeof pos==='number')try{next.setSelectionRange(pos,pos);}catch{}}
    return;
  }
  if(e.target.dataset.col){
    const col=e.target.dataset.col,val=(e.target.value||'').trim(),trgSh=resolveRef(col);
    updateFormThumb(e.target);
    if(trgSh){const refBtn=dom.ft.querySelector(`[data-a="ref-${qEsc(trgSh)}"]`);if(val===''&&refBtn)refBtn.remove();else if(val!==''&&!refBtn&&K.shs.includes(trgSh)&&(K.db[trgSh]||[]).length>0)addFooter(`ref-${trgSh}`,`ref ${trgSh}`,K.cur,`data-ref-col="${esc(col)}"`);}
    if(['idproduk','namaproduk'].includes(col)&&K.cur!=='produk'){
      const q=S2(val);if(q){const f=K.db.produk?.find(p=>S2(cV(p,'produk','idproduk')||p[0])===q||S2(cV(p,'produk','namaproduk'))===q);if(f)dom.fb.querySelectorAll('[data-col]').forEach(inp=>{const fi=cI('produk',inp.dataset.col);if(fi!==-1)inp.value=f[fi]||'';});}
    }
  }
});

document.addEventListener('change',e=>{
  if(e.target.dataset.a&&actions[e.target.dataset.a])actions[e.target.dataset.a](e.target);
  if(e.target===dom.fCam||e.target===dom.fImg||e.target===dom.fDoc){
    const file=e.target.files?.[0];if(!file)return;
    const isDoc=e.target===dom.fDoc,trgCol=isDoc?'file':'foto',tInp=Array.from(dom.fb.querySelectorAll('[data-col]')).find(i=>i.dataset.col===trgCol)||Array.from(dom.fb.querySelectorAll('[data-col]')).find(i=>new RegExp(trgCol,'i').test(i.dataset.col)),jInp=isDoc?Array.from(dom.fb.querySelectorAll('[data-col]')).find(i=>/judul|nama/i.test(i.dataset.col)):null;
    if(jInp&&!jInp.value)jInp.value=file.name;
    const rd=new FileReader();rd.onload=ev=>{if(tInp){tInp.value=ev.target.result||'';updateFormThumb(tInp);}};rd.onerror=()=>msg('err');rd.readAsDataURL(file);e.target.value='';
  }
});

document.addEventListener('keydown',e=>{
  if(e.key==='Enter'&&e.target.tagName!=='TEXTAREA'&&dom.fb.contains(e.target)){
    e.preventDefault();if(K.writing)return;
    if(!K.u)actions.login();else if(['bulksheet','toko','schema'].includes(K.v))actions[`save-${K.v}`]();else actions['save-row']();
  }else if(e.key==='Escape'){if(!dom.scBox.hidden)stopScan();else if(!dom.ib.hidden)actions['img-cls']();else actions.back();}
});

W.addEventListener('beforeinstallprompt',e=>{e.preventDefault();K.pwaEvent=e;if(K.v==='menu'||K.v==='dash')render();});
if('serviceWorker' in navigator&&(W.location.protocol.startsWith('http')||W.location.hostname==='localhost')){
  W.addEventListener('load',()=>{navigator.serviceWorker.register('sw.js').catch(()=>{});});
}
W.addEventListener('online',()=>syn(true));

(()=>{
  try{
    const d=JSON.parse(L.getItem('apex')||'null');
    if(d&&typeof d==='object'){
      if(d.store&&typeof d.store==='object')K.store=Object.assign(K.store,d.store);
      ['shs','cols','vis','fvis','db'].forEach(k=>{if(d[k])K[k]=d[k];});
      if(d.parent&&typeof d.parent==='object')K.parent=d.parent;
      if(d.cur&&K.shs.includes(d.cur))K.cur=d.cur;
      if(d.u)K.u=L.u=d.u;
      if(d.df&&typeof d.df==='object')K.df=Object.assign(K.df,d.df);
      K.grid=Boolean(d.grid);
    }
  }catch{}
  persist();
  initDB(K.shs,()=>{syn(true);});
  render();
})();
