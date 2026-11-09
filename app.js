'use strict';
/* ============================================================
   APEX ULTRA KISS ABSOLUTE — MONOFLOW, ZERO-BLOAT, ANTI-BUG
   ============================================================ */
const W=window,L=localStorage;
let db=null,syncing=false,syncDebounce=null,scanStream=null,scanTimer=null;
const ch=(typeof BroadcastChannel!=='undefined')?new BroadcastChannel('sync'):{postMessage:()=>{},onmessage:null};
ch.onmessage=e=>{if(e.data==='synced')render();else if(e.data==='kick'&&!syncing)syn();};

// IndexedDB Dua Store Abadi ('kv' & 'mut')
function initDB(cb){
  try{
    const req=indexedDB.open('apex_db',1);
    req.onupgradeneeded=e=>{
      const d=e.target.result;
      if(!d.objectStoreNames.contains('kv'))d.createObjectStore('kv');
      if(!d.objectStoreNames.contains('mut'))d.createObjectStore('mut',{keyPath:'i'});
    };
    req.onsuccess=e=>{db=e.target.result;cb?.(db);};
    req.onerror=()=>{cb?.(null);};
  }catch(_){cb?.(null);}
}
function idbPut(val,key=null,st='kv'){
  if(!db)return;
  try{
    const tx=db.transaction(st,'readwrite');
    const s=tx.objectStore(st);
    if(st==='mut')s.put(val);
    else if(key!=null)s.put(val,key);
    else s.put(val);
  }catch(_){}
}
function idbGet(key,st='kv'){return new Promise(res=>{if(!db)return res(null);try{const rq=db.transaction(st,'readonly').objectStore(st).get(key);rq.onsuccess=()=>res(rq.result);rq.onerror=()=>res(null);}catch{res(null);}});}
function idbGetAll(st='mut'){return new Promise(res=>{if(!db)return res([]);try{const rq=db.transaction(st,'readonly').objectStore(st).getAll();rq.onsuccess=()=>res(rq.result||[]);rq.onerror=()=>res([]);}catch{res([]);}});}
function idbClearStore(st){if(!db)return;try{if(db.objectStoreNames.contains(st))db.transaction(st,'readwrite').objectStore(st).clear();}catch{}}
function idbDelKeys(keys,st='mut'){if(!db||!keys?.length)return;try{const tx=db.transaction(st,'readwrite'),s=tx.objectStore(st);keys.forEach(k=>s.delete(k));}catch{}}

function save(row,targetTable=K.cur,actionType=2){
  const tIdx=TI(targetTable);
  const item={
    i:Date.now().toString(36)+Math.random().toString(36).slice(2,6),
    t:tIdx>-1?tIdx:targetTable,
    tn:targetTable,
    y:actionType,
    r:Array.isArray(row)?row:Object.values(row),
    ts:Date.now()
  };
  idbPut(item,null,'mut');
  ch.postMessage('kick');
  if(!syncing){clearTimeout(syncDebounce);syncDebounce=setTimeout(()=>syn(false),50);}
}

async function req(p={}){
  p.u=p.u||L.u||K.u||'admin';
  if(HARD.d1Url&&['pull_batch','manifest','push'].includes(p.a)){
    try{
      const res=await fetch(HARD.d1Url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});
      if(res.ok){
        const r=await res.json();
        if(r?.ok||r?.s==='ok'||r?.status==='ok')return r;
      }
    }catch{}
  }
  return new Promise(res=>{
    if(W.google?.script?.run){W.google.script.run.withSuccessHandler(r=>res({ok:r?.ok??r?.s==='ok',...r})).withFailureHandler(e=>res({ok:false,e})).core(p);return;}
    if(HARD.gasUrl&&/^https?:\/\//i.test(HARD.gasUrl)){
      fetch(HARD.gasUrl,{method:'POST',headers:{'Content-Type':'text/plain;charset=utf-8'},body:JSON.stringify(p),redirect:'follow'}).then(r=>r.json()).then(j=>res({ok:j?.ok??j?.s==='ok',...j})).catch(e=>res({ok:false,e}));return;
    }
    res({ok:false,e:'offline'});
  });
}

async function syn(force=false){
  if(!navigator.onLine||(syncing&&!force)||!db)return false;
  syncing=true;
  msg('sedang sync...');
  let isSuccess=false;
  try{
    const tables=K.shs||HARD.shs;
    if(force)L.vs='[]';
    const ver=JSON.parse(L.vs||'[]'),muts=await idbGetAll('mut');
    let m=null;

    if(muts.length){
      const pushRes=await req({a:'push',m:muts,tn:tables,db:K.db,cols:K.cols,shs:K.shs,gasUrl:HARD.gasUrl});
      if(pushRes?.ok){
        idbDelKeys(muts.map(x=>x.i),'mut');
        m=pushRes.manifest||pushRes.data?.manifest;
        isSuccess=true;
      }
    }

    if(!m)m=(await req({a:'manifest',tn:tables,cols:K.cols}))?.data||(await req({a:'manifest',tn:tables,cols:K.cols}));
    if(m?.shs&&Array.isArray(m.shs)&&m.shs.length){
      m.shs.forEach(s=>{if(!K.shs.includes(s))K.shs.push(s);});
    }
    if(m?.cols&&typeof m.cols==='object')Object.assign(K.cols,m.cols);
    if(m?.vis&&typeof m.vis==='object')Object.assign(K.vis,m.vis);

    if(m?.v){
      isSuccess=true;
      const toPull=tables.map((_,i)=>({t:i,v:force?'':(ver[i]||'')})).filter((_,i)=>force||(m.v[i]&&m.v[i]!==ver[i]));
      if(toPull.length){
        const b=await req({a:'pull_batch',reqs:toPull,tn:tables,cols:K.cols});
        (b?.data?.results||b?.results||[]).forEach(r=>{
          if(r?.v){
            ver[r.t]=r.v;
            const tn=K.shs[r.t];
            if(tn){
              K.db[tn]=r.r||[];
              if(Array.isArray(r.cols)&&r.cols.length)K.cols[tn]=r.cols;
            }
          }
        });
        L.vs=JSON.stringify(ver);persist();ch.postMessage('synced');render();
      }
    }else if(m?.db){
      isSuccess=true;
      Object.assign(K.db,m.db);persist();ch.postMessage('synced');render();
    }
    if(isSuccess)msg();
  }catch(_){
    isSuccess=false;
    msg('offline');
  }finally{
    syncing=false;
  }
  return isSuccess;
}

const TI=s=>K.shs.indexOf(s),
      dl=(content,fileName,mimeType='text/plain;charset=utf-8')=>{
        const blob=new Blob([content],{type:mimeType}),u=URL.createObjectURL(blob),a=document.createElement('a');
        a.href=u;a.download=fileName;document.body.appendChild(a);a.click();
        setTimeout(()=>{a.remove();URL.revokeObjectURL(u);},400);
      };

const K={
  u:L.u||'',cur:'penjualan',v:'dash',hist:[],idx:null,page:1,sort:0,dir:1,grid:false,
  conf:null,prefill:null,parent:null,receiptRaw:'',prodIdx:null,readyOnly:false,
  df:{u:'',from:'',to:'',kat:''},
  shs:[...HARD.shs],cols:JSON.parse(JSON.stringify(HARD.cols)),vis:JSON.parse(JSON.stringify(HARD.vis)),
  fvis:{},db:JSON.parse(JSON.stringify(HARD.db)),
  refTargetInp:null,savedFormState:null,pwaEvent:null,writing:false
};

// Pastikan tabel toko terdaftar dengan skema: ['id', 'name', 'sub', 'phone', 'bank', 'footer']
if(!K.shs.includes('toko'))K.shs.push('toko');
if(!K.cols.toko)K.cols.toko=['id','name','sub','phone','bank','footer'];
if(!K.vis.toko)K.vis.toko=['id','name','phone'];
if(!K.db.toko)K.db.toko=[];

const getStore=()=>{
  const r=K.db.toko?.[0]||[];
  const c=K.cols.toko||['id','name','sub','phone','bank','footer'];
  const gV=k=>{const i=c.indexOf(k);return i>-1&&r[i]!==undefined?r[i]:(HARD.store?.[k]||'');};
  return {name:gV('name'),sub:gV('sub'),phone:gV('phone'),bank:gV('bank'),footer:gV('footer')};
};

const esc=s=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])),
      isSafeUrl=u=>{const s=String(u||'').trim();return /^https?:\/\//i.test(s)||/^data:(image|application|text)\/[a-zA-Z0-9.\-_+]+;base64,/i.test(s);},
      dom={
        h:document.getElementById('h'),q:document.getElementById('q'),msg:document.getElementById('msg'),
        main:document.getElementById('main'),tools:document.getElementById('tools'),
        scanBox:document.getElementById('scan-box'),scanVid:document.getElementById('scan-vid'),
        fCam:document.getElementById('f-cam'),fImg:document.getElementById('f-img'),fDoc:document.getElementById('f-doc'),fImp:document.getElementById('f-imp'),
        sh:document.getElementById('sh-sel'),st:document.getElementById('st-sel'),act:document.getElementById('act-sel'),
        fb:document.getElementById('box-form'),db:document.getElementById('box-dash'),ls:document.getElementById('list'),
        ft:document.getElementById('ft'),tape:document.getElementById('tape'),bn:document.getElementById('box-nota')
      };

const S2=s=>String(s||'').trim().toLowerCase(),
      N=v=>{if(v==null)return 0;const n=parseFloat(String(v).replace(/[^\d.-]/g,''));return(!isFinite(n)||n<0)?0:n;},
      cols=(s=K.cur)=>K.cols[s]||[],
      cI=(s,k)=>cols(s).indexOf(k),
      cV=(r,s,k)=>{const i=cI(s,k);return(r&&i>-1&&r[i]!==undefined)?String(r[i]):'';},
      vis=(s=K.cur)=>(K.vis[s]||cols(s)).filter(c=>cols(s).includes(c)),
      fvis=(s=K.cur)=>((K.fvis?.[s]?.length)?K.fvis[s]:cols(s)).filter(c=>cols(s).includes(c));

const TRXM={penjualan:{ch:'produk_jual',fk:'idpenjualan',p:'hargajual'},pembelian:{ch:'produk_beli',fk:'idpembelian',p:'hargabeli'}},
      isTrx=(s=K.cur)=>Boolean(TRXM[s]),
      childOf=s=>TRXM[s]?.ch||null,
      parentOf=s=>s==='produk_jual'?'penjualan':(s==='produk_beli'?'pembelian':null),
      fkCol=s=>s==='produk_jual'?'idpenjualan':(s==='produk_beli'?'idpembelian':(s==='cicil'?'idjual':null)),
      NO_ADD=['penjualan','pembelian','produk_jual','produk_beli','cicil','ecer'],
      EXEMPT=['back','menu','q','dir','grid','closemsg','more','reset-db','add','df-today','df-clear','login','reg','bypass','pwa-install','v-toko','v-dash','v-kas','v-shop','logout','refresh','scan','stop-scan'];

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
const fmtNum=n=>N(n).toLocaleString('id-ID'),today=()=>new Date().toISOString().slice(0,10);
const syncStockDelta=(sh,o,n)=>{
  if(!['produk_jual','produk_beli','ecer'].includes(sh))return;
  const m=sh==='produk_beli'?1:-1,p=r=>r&&{id:cV(r,sh,'idproduk'),q:N(cV(r,sh,'qty'))};
  [{r:o,s:-m},{r:n,s:m}].forEach(({r,s})=>{
    const x=p(r),row=x?.id&&K.db.produk?.find(k=>S2(k[0])===S2(x.id));
    if(row&&cI('produk','stok')>-1)row[cI('produk','stok')]=String(N(row[cI('produk','stok')])+x.q*s);
  });
};
const msg=t=>{dom.msg.innerHTML='';dom.msg.hidden=!t;if(t)dom.msg.textContent=t;};

/* Barcode Scanner Ringkas */
const stopScan=()=>{
  if(scanTimer){clearInterval(scanTimer);scanTimer=null;}
  if(scanStream){scanStream.getTracks().forEach(t=>t.stop());scanStream=null;}
  dom.scanBox.hidden=true;
};
const startScan=()=>{
  if(!('BarcodeDetector' in W))return msg('kamera tidak didukung');
  const detector=new BarcodeDetector({formats:['qr_code','ean_13','ean_8','code_128','code_39','upc_a']});
  navigator.mediaDevices.getUserMedia({video:{facingMode:'environment'}}).then(stream=>{
    scanStream=stream;dom.scanVid.srcObject=stream;dom.scanVid.play();dom.scanBox.hidden=false;
    scanTimer=setInterval(async()=>{
      try{
        const codes=await detector.detect(dom.scanVid);
        if(codes&&codes.length>0){
          const val=codes[0].rawValue;
          stopScan();dom.q.value=val;K.page=1;render();
        }
      }catch(_){}
    },250);
  }).catch(()=>msg('kamera tidak aktif'));
};

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
  const row=K.db.penjualan?.find(r=>S2(r[0])===S2(id));if(!row)return'';
  const val=k=>cV(row,'penjualan',k),paid=N(val('bayar'))+(K.db.cicil||[]).reduce((a,c)=>a+(S2(cV(c,'cicil','idjual'))===S2(id)?N(cV(c,'cicil','jumlah')):0),0),{raw,disc,items}=calcItems('penjualan',id),gTot=Math.max(0,raw-disc-N(val('diskon'))),diff=paid-gTot,sisa=Math.abs(diff),W_L=32,
        store=getStore(),
        pad=(s,c=' ')=>{const str=String(s||'').trim();if(str.length>=W_L)return str.slice(0,W_L);const lP=Math.floor((W_L-str.length)/2),rP=W_L-str.length-lP;return c.repeat(lP)+str+c.repeat(rP);},
        lr=(l,r)=>l+' '.repeat(Math.max(1,W_L-l.length-String(r).length))+r,
        lines=[pad(store.name),store.sub&&pad(store.sub),store.phone&&pad(store.phone),store.bank&&pad(store.bank),'='.repeat(W_L),lr(`${val('date')||today()} ${id}`,val('user')||K.u||'admin'),'-'.repeat(W_L)].filter(Boolean);
  if(!items.length)lines.push('(kosong)');else items.forEach(it=>{lines.push(it.name);if(it.dU>0)lines.push(`${fmtNum(it.p)} -disc ${fmtNum(it.dU)}`);lines.push(lr(`${it.q}x ${fmtNum(it.eff)}`,fmtNum(it.tot)));});
  lines.push('-'.repeat(W_L),lr('Subtotal',fmtNum(raw)));if(disc>0)lines.push(lr('Diskon Item','-'+fmtNum(disc)));if(N(val('diskon'))>0)lines.push(lr('Diskon','-'+fmtNum(val('diskon'))));lines.push(lr('Total',fmtNum(gTot)),lr('Bayar',fmtNum(paid)),lr(diff>=0?'Kembali':'Kurang',fmtNum(sisa)),'-'.repeat(W_L));
  if(store.footer)lines.push(pad(store.footer));
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
  K.db.pembelian?.forEach(r=>{if(!isUserVisible(r,'pembelian'))return;if(passFlt(cV(r,'pembelian','user'),cV(r,'pembelian','date'),`${cV(r,'pembelian','idkontak')} ${cV(r,'pembelian','catatan')}`)){const{raw,disc}=calcItems('pembelian',r[0]);bFil+=Math.max(0,raw-disc-N(cV(r,'pembelian','diskon')))}});
  let sStok=0,rCount=0,eMod=0,eJual=0;
  K.db.produk?.forEach(r=>{const s=N(cV(r,'produk','stok'));if(passFlt('','',`${cV(r,'produk','subkategori')} ${cV(r,'produk','kategori')}`)){sStok+=s;if(s>0){rCount++;eMod+=(s*N(cV(r,'produk','hargabeli')));eJual+=(s*N(cV(r,'produk','hargajual')));}}});
  const stMasuk=(K.db.produk_beli||[]).reduce((a,r)=>a+(isUserVisible(r,'produk_beli')?N(cV(r,'produk_beli','qty')):0),0),stKeluar=(K.db.produk_jual||[]).reduce((a,r)=>a+(isUserVisible(r,'produk_jual')?N(cV(r,'produk_jual','qty')):0),0)+ecQtyFil;
  [{l:'saldo total',v:fmtNum(mAll+jAll-kAll-pAll),s:'kas',k:'saldo_total'},{l:'saldo filter',v:fmtNum(mFil+jFil-kFil-pFil),s:'penjualan',k:'saldo_filter'},{l:'user',v:K.u||'admin',s:'users',k:'users'},{l:'profit',v:fmtNum(profFil),s:'penjualan',k:'profit'},{l:'kas masuk',v:fmtNum(mFil),s:'kas',k:'kas_masuk'},{l:'kas keluar',v:fmtNum(kFil),s:'kas',k:'kas_keluar'},{l:'total penjualan',v:fmtNum(jFil),s:'penjualan',k:'total_penjualan'},{l:'total pembelian',v:fmtNum(bFil),s:'pembelian',k:'total_pembelian'},{l:'total piutang',v:fmtNum(pFil),s:'penjualan',k:'total_piutang'},{l:'stok masuk',v:fmtNum(stMasuk),s:'produk_beli',k:'stok_masuk'},{l:'stok keluar',v:fmtNum(stKeluar),s:'produk_jual',k:'stok_keluar'},{l:'sisa stok',v:fmtNum(sStok),s:'produk',k:'sisa_stok'},{l:'produk ready stok',v:rCount+' item',s:'produk',slice:'ready',k:'ready'},{l:'estimasi modal ready',v:fmtNum(eMod),s:'produk',slice:'ready',k:'modal_ready'},{l:'estimasi jual ready',v:fmtNum(eJual),s:'produk',slice:'ready',k:'jual_ready'},{l:'estimasi profit ready',v:fmtNum(eJual-eMod),s:'produk',slice:'ready',k:'profit_ready'}].forEach(c=>{if(hasAccess(c.k)&&hasAccess(c.s))dom.db.insertAdjacentHTML('beforeend',`<button data-a="nav" data-s="${esc(c.s)}" ${c.slice?`data-slice="${esc(c.slice)}"`:''}>${esc(c.l)}: <b>${esc(c.v)}</b></button>`);});
  renderNavFooters();
};

const renderForm=()=>{
  if(!K.u){dom.fb.innerHTML='<input placeholder="user" autocomplete="off" data-f="user"><input type="password" placeholder="password" data-f="pass">';addFooter('login','login');addFooter('reg','register');addFooter('bypass','bypass admin');return;}
  if(K.cur==='cms'&&K.v==='cms_detail'){
    const r=K.db.cms?.[K.idx]||[],gV=k=>cV(r,'cms',k),fotoSafe=isSafeUrl(gV('foto'))?gV('foto'):'',fileVal=gV('file');
    dom.fb.innerHTML=`<b>${esc(gV('judul')||'-')}</b>
    <small>${esc(gV('date'))} ${gV('user')?'| '+esc(gV('user')):''} ${gV('kategori')?'| '+esc(gV('kategori')):''} ${gV('status')?'| ['+esc(gV('status'))+']':''}</small>
    <p>${esc(gV('konten'))}</p>
    ${fotoSafe?`<img src="${esc(fotoSafe)}" alt="cms"><br>`:''}
    ${fileVal?`<button data-a="dl-cms-file">unduh lampiran</button><br>`:''}`;
    addFooter('back','<',K.cur);
    if(fileVal)addFooter('dl-cms-file','unduh file',K.cur);
    addFooter('edit-cms','edit',K.cur);
    renderNavFooters();
    return;
  }
  if(K.v==='bulksheet'){dom.fb.innerHTML=`<textarea rows="10" placeholder="sheet:kolom,kolom">${esc(K.shs.map(s=>`${s}:${(K.cols[s]||[]).join(',')}`).join('\n'))}</textarea>`;addFooter('back','<',K.cur);addFooter('save-bulksheet','save',K.cur);renderNavFooters();return;}
  if(K.v==='toko'){
    const store=getStore();
    dom.fb.innerHTML=`<input placeholder="name" value="${esc(store.name)}" data-cfg="name"><input placeholder="sub" value="${esc(store.sub)}" data-cfg="sub"><input placeholder="phone" value="${esc(store.phone)}" data-cfg="phone"><input placeholder="bank" value="${esc(store.bank)}" data-cfg="bank"><textarea placeholder="footer" data-cfg="footer">${esc(store.footer)}</textarea>`;
    addFooter('back','<',K.cur);addFooter('save-store','save',K.cur);renderNavFooters();return;
  }
  if(K.v==='schema'){dom.fb.innerHTML=`<input placeholder="cols" value="${esc(cols().join(','))}" data-f="cols"><input placeholder="vis (list)" value="${esc(vis().join(','))}" data-f="vis"><input placeholder="form cols" value="${esc(fvis().join(','))}" data-f="form">`;addFooter('back','<',K.cur);addFooter('save-schema','save',K.cur);renderNavFooters();return;}
  
  const c=cols(),bypassAdd=K.idx===-1&&['produk','kas'].includes(K.cur),formCols=bypassAdd?fvis():fvis().filter(col=>hasAccess(col,K.cur)),row=(K.idx!==null&&K.idx>=0)?(K.db[K.cur][K.idx]||[]):[],isDraft=isTrx()&&S2(cV(row,K.cur,'status'))==='draft';
  if(isTrx()&&K.idx!==null&&K.idx>=0){const{raw,disc,cogs,pot}=calcItems(K.cur,row[0]),net=Math.max(0,raw-disc-N(cV(row,K.cur,'diskon'))),showProf=hasAccess('profit',K.cur);msg(`total: ${fmtNum(net)}${showProf?` | profit: ${fmtNum(K.cur==='penjualan'?(net-cogs):(pot-net))}`:''}`);}else msg();
  
  formCols.forEach(col=>{
    let val=(K.idx===-1&&c.indexOf(col)===0)?genId():(row[c.indexOf(col)]??'');
    if(K.savedFormState&&K.savedFormState[col]!=null)val=K.savedFormState[col];
    else if(K.idx===-1&&K.prefill?.[col]!=null)val=K.prefill[col];
    else if(K.idx===-1&&!val&&/date|tanggal/i.test(col))val=today();
    else if(K.idx===-1&&!val&&col==='user')val=K.u||'admin';
    else if(K.idx===-1&&!val&&K.cur==='cms'&&col==='status')val='publish';
    else if(K.idx===-1&&!val&&K.cur==='roles'&&col==='hide')val='hargabeli,profit,users,roles,toko,bulksheet,schema,clear,del-row,reset-db';
    else if(K.idx===-1&&!val&&K.cur==='roles'&&col==='validasi')val='wajibstok,wajibkontak';
    const isLong=/catatan|alamat|uraian|konten|akses|hide|validasi/i.test(col),isDate=/date|tanggal/i.test(col),tag=isLong?'textarea':'input',typeAttr=isDate?'type="date"':'';
    dom.fb.insertAdjacentHTML('beforeend',`<${tag} ${typeAttr} placeholder="${esc(col)}" value="${esc(val)}" data-col="${esc(col)}">${isLong?esc(val):''}</${tag}>`);
    updateFormThumb(dom.fb.querySelector(`[data-col="${CSS.escape(col)}"]`));
  });
  K.savedFormState=null;

  if(K.idx===-1&&getRoleRules().includes('wajiblokasi')){
    const locInp=Array.from(dom.fb.querySelectorAll('[data-col]')).find(i=>/lokasi|alamat/i.test(i.dataset.col));
    if(locInp&&!locInp.value&&navigator.geolocation)navigator.geolocation.getCurrentPosition(p=>{if(!locInp.value)locInp.value=`${p.coords.latitude.toFixed(6)},${p.coords.longitude.toFixed(6)}`;},()=>{locInp.value='here()';},{timeout:4000});
  }
  addFooter('back','<',K.cur);addFooter('save-row','save',K.cur);
  if(isTrx())addFooter('trx-pick-kontak','kontak',K.cur);
  formCols.forEach(col=>{const trgSh=resolveRef(col),val=(dom.fb.querySelector(`[data-col="${CSS.escape(col)}"]`)?.value||'').trim();if(trgSh&&trgSh!==K.cur&&K.shs.includes(trgSh)&&(K.db[trgSh]||[]).length>0&&val!=='')addFooter(`ref-${trgSh}`,`ref ${trgSh}`,K.cur,`data-ref-col="${esc(col)}"`);});
  if(formCols.some(k=>/foto/i.test(k))){addFooter('cam','kamera',K.cur);addFooter('pic','file foto',K.cur);}
  if(formCols.some(k=>/file/i.test(k)))addFooter('doc-upload','upload berkas',K.cur);
  if(K.cur==='produk')addFooter('gimg','gimg',K.cur);
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
  if(K.cur==='roles'){addFooter('preset-kasir','role kasir',K.cur);addFooter('preset-gudang','role gudang',K.cur);}
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
      if(K.cur==='cms'&&cmsFotoI!==-1&&isSafeUrl(r[cmsFotoI])){const cTh=document.createElement('img');cTh.src=r[cmsFotoI];cTh.alt='cms';cTh.onerror=()=>cTh.remove();rowEl.appendChild(cTh);rowEl.appendChild(document.createTextNode(' '));}
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

const renderNota=()=>{msg();dom.tape.textContent=K.receiptRaw;addFooter('back','<',K.cur);addFooter('rawbt','rawbt',K.cur);addFooter('jpg','jpg',K.cur);addFooter('print','print',K.cur);renderNavFooters();};
const updateFormThumb=inp=>{if(!inp)return;const col=inp.dataset.col||'',val=(inp.value||'').trim();let th=dom.fb.querySelector(`img[data-thumb-col="${CSS.escape(col)}"]`);if(val&&(/foto/i.test(col)||isSafeUrl(val))){if(!th){th=document.createElement('img');th.dataset.thumbCol=col;th.alt='thumb';th.onerror=()=>th.remove();inp.insertAdjacentElement('afterend',th);}th.src=val;}else if(th)th.remove();};

const persist=()=>{
  try{
    const payload={cur:K.cur,u:K.u,shs:K.shs,cols:K.cols,vis:K.vis,fvis:K.fvis,parent:K.parent,grid:K.grid,df:K.df};
    L.setItem('apex',JSON.stringify(payload));
    idbPut(K.db,'apex_matrix');
  }catch{}
};

const render=()=>{
  dom.act.value='';dom.fb.innerHTML=dom.ft.innerHTML=dom.db.innerHTML=dom.ls.innerHTML='';document.body.dataset.grid=K.grid?'1':'0';
  if(!K.u){dom.h.hidden=dom.tools.hidden=dom.db.hidden=dom.bn.hidden=dom.ls.hidden=true;dom.fb.hidden=false;renderForm();return;}
  const isF=['toko','schema','bulksheet','cms_detail'].includes(K.v)||K.idx!==null,m=K.v==='menu'?'menu':K.v==='dash'?'dash':K.v==='nota'?'nota':isF?'form':K.v==='edit'?'edit':'list';
  dom.h.hidden=m==='form';dom.tools.hidden=m!=='list'&&m!=='edit';dom.fb.hidden=m!=='form';dom.db.hidden=m!=='dash';dom.bn.hidden=m!=='nota';dom.ls.hidden=m!=='list'&&m!=='edit';
  renderChrome();
  if(m==='menu')renderMenuFooters();else if(m==='dash')renderDash();else if(m==='nota')renderNota();else if(m==='form')renderForm();else renderList(m==='edit');
};

const actions={
  closemsg:()=>{K.conf=null;msg();render();},
  scan:()=>startScan(),
  'stop-scan':()=>stopScan(),
  login:()=>{
    const u=S2(dom.fb.querySelector('[data-f="user"]')?.value),p=(dom.fb.querySelector('[data-f="pass"]')?.value||'').trim();
    if(!u)return msg('user required');
    const f=K.db.users?.find(r=>S2(r[0])===u);
    if(f){if(f[2]&&f[2]!==p&&f[1]!==p)return msg('password salah');K.u=L.u=f[0];}
    else if(!K.db.users?.length&&u==='admin'){K.db.users=[['admin','admin','admin']];K.u=L.u='admin';}
    else return msg('user tidak terdaftar');
    persist();syn(true);setView('dash',null);msg();render();
  },
  reg:()=>{
    const u=S2(dom.fb.querySelector('[data-f="user"]')?.value),p=(dom.fb.querySelector('[data-f="pass"]')?.value||'').trim();
    if(!u)return msg('user required');if(K.db.users?.some(r=>S2(r[0])===u))return msg('user sudah ada');
    const newU=[u,'user',p];(K.db.users=K.db.users||[]).push(newU);K.u=L.u=u;persist();save(newU,'users',2);setView('dash',null);msg();render();
  },
  bypass:()=>{
    K.db.users=K.db.users||[];
    let adm=K.db.users.find(r=>S2(r[0])==='admin');
    if(!adm){
      adm=['admin','admin','admin'];
      K.db.users.unshift(adm);
      save(adm,'users',2);
    }
    K.u=L.u='admin';
    persist();
    syn(true);
    setView('dash',null);
    msg();
    render();
  },
  back:()=>{
    stopScan();
    if(K.v==='menu'){dom.q.value='';K.v='dash';return render();}
    const inForm=['form','toko','schema','bulksheet','cms_detail','edit'].includes(K.v)||K.idx!==null;
    K.idx=null;K.prefill=null;K.prodIdx=null;K.conf=null;K.refTargetInp=null;K.savedFormState=null;
    if(inForm){while(K.hist.length){const prev=K.hist.pop();if(prev&&!['form','toko','schema','bulksheet','cms_detail','edit'].includes(prev.v)&&prev.idx===null){K.cur=prev.cur||K.cur;K.v=prev.v||'list';K.parent=prev.parent||null;K.readyOnly=Boolean(prev.readyOnly);return render();}}K.v=K.cur?'list':'dash';return render();}
    if(K.hist.length){const p=K.hist.pop();K.cur=p.cur||K.cur;K.v=p.v||'dash';K.idx=p.idx!=null?p.idx:null;K.parent=p.parent||null;K.readyOnly=Boolean(p.readyOnly);return render();}
    if(K.v!=='dash'){K.v='dash';K.idx=null;K.readyOnly=false;return render();}
  },
  menu:()=>{if(K.v==='menu'){dom.q.value='';K.v='dash';}else{K.v='menu';dom.q.value='';}render();},
  refresh:async()=>{
    const ok = await syn(true);
    if(!ok)msg('offline');
    else msg();
    render();
  },
  'df-today':()=>{K.df.from=K.df.to=today();persist();render();},
  'df-clear':()=>{K.df={u:'',from:'',to:'',kat:''};persist();render();},
  'v-dash':()=>{K.prefill=K.prodIdx=null;setView('dash',null);render();},
  'v-toko':()=>{setView('toko',null);render();},
  'v-kas':()=>{if(!K.shs.includes('kas'))K.shs.push('kas');K.cur='kas';K.prefill=K.prodIdx=null;setView('form',-1);render();},
  'v-shop':()=>{K.cur='produk';K.grid=true;K.prefill=K.prodIdx=null;setView('list',null);K.readyOnly=false;render();},
  'v-jual':()=>{const op=getOpenTrx('penjualan');if(op)K.parent={type:'penjualan',id:op.id,open:true};K.cur='produk_jual';K.grid=false;setView('list',null);render();},
  'v-beli':()=>{const op=getOpenTrx('pembelian');if(op)K.parent={type:'pembelian',id:op.id,open:true};K.cur='produk_beli';K.grid=false;setView('list',null);render();},
  'v-selesai':()=>{if(!K.parent)return;const fi=(K.db[K.parent.type]||[]).findIndex(r=>S2(r[0])===S2(K.parent.id));K.cur=K.parent.type;setView('form',fi!==-1?fi:null);render();},
  'clear-items':()=>{
    if(K.conf!=='clear-items'){K.conf='clear-items';render();return;}
    if(!K.parent)return;
    const pType=K.parent.type,pid=S2(K.parent.id),ch=childOf(pType),fkI=cI(ch,fkCol(ch));
    if(fkI!==-1){(K.db[ch]||[]).filter(r=>S2(r[fkI])===pid).forEach(r=>{syncStockDelta(ch,r,null);save(r,ch,0);});K.db[ch]=(K.db[ch]||[]).filter(r=>S2(r[fkI])!==pid);}
    if(K.db[pType]){const pRow=K.db[pType].find(r=>S2(r[0])===pid);if(pRow)save(pRow,pType,0);K.db[pType]=K.db[pType].filter(r=>S2(r[0])!==pid);}
    if(pType==='penjualan'&&K.db.cicil){K.db.cicil.filter(c=>S2(cV(c,'cicil','idjual'))===pid).forEach(c=>save(c,'cicil',0));K.db.cicil=K.db.cicil.filter(c=>S2(cV(c,'cicil','idjual'))!==pid);}
    K.parent=K.conf=null;persist();syn(true);K.cur='produk';setView('list',null);render();
  },
  'add-cart-item':()=>{K.prefill=null;K.prodIdx=null;setView('form',-1);render();},
  'edit-cms':()=>{setView('form',K.idx);render();},
  'dl-cms-file':()=>{
    const r=K.db.cms?.[K.idx]||[],fileData=cV(r,'cms','file'),judul=cV(r,'cms','judul')||'berkas';
    if(!fileData)return;
    if(/^https?:\/\//i.test(fileData)){W.open(fileData,'_blank');return;}
    if(/^data:/i.test(fileData)){
      const a=document.createElement('a');a.href=fileData;a.download=`${judul.replace(/\s+/g,'_')}`;
      document.body.appendChild(a);a.click();setTimeout(()=>a.remove(),400);return;
    }
    dl(fileData,`${judul}.txt`);
  },
  nav:b=>{K.cur=b.dataset.s;setView('list',null);K.page=1;K.readyOnly=b.dataset.slice==='ready';render();},
  add:()=>{if(!NO_ADD.includes(K.cur&&hasAccess('add',K.cur))){setView('form',-1);render();}},
  'edit-prod':()=>{if(K.prodIdx!==null){K.cur='produk';const pi=K.prodIdx;K.prodIdx=K.prefill=null;setView('form',pi);render();}},
  'jual-prod':()=>{if(K.idx>=0){const p=K.db.produk[K.idx]||[];if(!K.shs.includes('produk_jual'))K.shs.push('produk_jual');K.cur='produk_jual';K.prefill={idproduk:p[0],namaproduk:p[3],hargabeli:p[4],hargajual:p[5],qty:'1',diskon:'',catatan:''};K.prodIdx=K.idx;setView('form',-1);render();}},
  'beli-prod':()=>{if(K.idx>=0){const p=K.db.produk[K.idx]||[];if(!K.shs.includes('produk_beli'))K.shs.push('produk_beli');K.cur='produk_beli';K.prefill={idproduk:p[0],namaproduk:p[3],hargabeli:p[4],hargajual:p[5],qty:'1'};K.prodIdx=K.idx;setView('form',-1);render();}},
  'ecer-prod':()=>{if(K.idx>=0){const p=K.db.produk[K.idx]||[];if(!K.shs.includes('ecer'))K.shs.push('ecer');K.cur='ecer';K.prefill={idproduk:p[0],namaproduk:p[3],hargabeli:p[4],hargajual:p[5],qty:'1',diskon:'',catatan:''};K.prodIdx=K.idx;setView('form',-1);render();}},
  'save-bulksheet':()=>{
    const val=dom.fb.querySelector('textarea')?.value||'',updated=[];
    val.split(/\r?\n/).filter(Boolean).forEach(l=>{
      const p=l.split(':'),n=S2(p[0]);if(!n||updated.includes(n))return;updated.push(n);K.db[n]=K.db[n]||[];
      const colsList=p.slice(1).join(':').split(',').map(S2).filter(Boolean);
      K.cols[n]=colsList.length?colsList:(K.cols[n]||[]);if(!K.vis[n]?.length)K.vis[n]=K.cols[n].slice(0,2);
    });
    if(!updated.length)return;
    K.shs.forEach(s=>{if(!updated.includes(s)){delete K.db[s];delete K.cols[s];delete K.vis[s];delete K.fvis[s];}});
    K.shs=updated;if(!K.shs.includes(K.cur))K.cur=K.shs[0];persist();syn(true);setView('list',null);render();
  },
  'save-row':()=>{
    if(K.writing)return;K.writing=true;
    try{
      const c=cols(),oldRow=(K.idx>=0&&K.db[K.cur][K.idx])?K.db[K.cur][K.idx].slice():null,row=oldRow?oldRow.slice(0,c.length):new Array(c.length).fill('');
      while(row.length<c.length)row.push('');
      dom.fb.querySelectorAll('[data-col]').forEach(inp=>{const ci=c.indexOf(inp.dataset.col);if(ci!==-1)row[ci]=inp.value.trim();});
      const rules=getRoleRules(),valOf=k=>row[c.indexOf(k)]||'';
      if(rules.includes('wajiblokasi')&&c.some(k=>/lokasi|alamat/i.test(k))){if(!valOf(c.find(k=>/lokasi|alamat/i.test(k))))return msg('lokasi kosong');}
      if(rules.includes('wajibfoto')&&c.some(k=>/foto/i.test(k))){const fv=valOf(c.find(k=>/foto/i.test(k)));if(!fv||!isSafeUrl(fv))return msg('foto kosong');}
      if(rules.includes('wajibkontak')&&c.includes('idkontak')&&!valOf('idkontak'))return msg('idkontak kosong');
      if(rules.includes('wajibabsen')&&['penjualan','kas','pembelian'].includes(K.cur)&&!(K.db.absen||[]).some(a=>S2(cV(a,'absen','user'))===S2(K.u||'admin')&&cV(a,'absen','date')===today()))return msg('absen dulu');
      if(rules.includes('wajibstok')&&['produk_jual','ecer'].includes(K.cur)){
        const pRow=K.db.produk?.find(p=>S2(p[0])===S2(valOf('idproduk'))),stkCur=pRow?N(cV(pRow,'produk','stok')):0;
        if(pRow&&N(valOf('qty'))>stkCur)return msg('stok kurang');
      }
      const pType=parentOf(K.cur);
      if(pType&&K.idx===-1){
        if(!K.parent||K.parent.type!==pType)K.parent=getOpenTrx(pType)||{type:pType,id:genId(),open:true};
        const pCols=cols(pType);
        if(!(K.db[pType]||[]).some(r=>S2(r[0])===S2(K.parent.id))){
          const newP=new Array(pCols.length).fill('');newP[0]=K.parent.id;
          pCols.forEach((col,i)=>{if(/date|tanggal/i.test(col))newP[i]=today();else if(col==='user')newP[i]=K.u||'admin';else if(col==='status')newP[i]='draft';});
          (K.db[pType]||[]).push(newP);save(newP,pType,2);
        }
        const fkI=c.indexOf(fkCol(K.cur));if(fkI!==-1)row[fkI]=K.parent.id;
      }
      if(!row[0])row[0]=genId();
      if(isTrx()){
        const sI=cI(K.cur,'status');
        if(sI===-1||row[sI]!=='revisi'){
          const{raw,disc}=calcItems(K.cur,row[0]),net=Math.max(0,raw-disc-N(cV(row,K.cur,'diskon'))),paid=N(cV(row,K.cur,'bayar')),cicil=K.cur==='penjualan'?(K.db.cicil||[]).reduce((a,c)=>a+(S2(cV(c,'cicil','idjual'))===S2(row[0])?N(cV(c,'cicil','jumlah')):0),0):0,kId=cV(row,K.cur,'idkontak'),totPaid=paid+cicil;
          if(sI>-1){if(net<=0)row[sI]='draft';else if(totPaid>=net)row[sI]='lunas';else if(kId)row[sI]='piutang';else row[sI]='draft';}
          if((row[sI]==='piutang'||(net>0&&totPaid<net))&&cI(K.cur,'idkontak')!==-1&&!kId)return msg('piutang butuh kontak');
        }
      }
      if(K.idx===-1){(K.db[K.cur]=K.db[K.cur]||[]).push(row);syncStockDelta(K.cur,null,row);save(row,K.cur,2);}
      else if(K.idx>=0){K.db[K.cur][K.idx]=row;syncStockDelta(K.cur,oldRow,row);save(row,K.cur,1);}
      persist();
      if(pType&&K.parent?.id)syncTrx(pType);
      if(isTrx()&&row[0]){syncTrx(K.cur);const st=S2(cV(row,K.cur,'status'));if(st!=='draft'&&st!=='revisi')K.parent=null;}
      if(K.cur==='cicil'){const cJual=cV(row,'cicil','idjual');if(cJual)syncTrx('penjualan');}
      const justTrxId=isTrx()?row[0]:null,wasPenjualan=K.cur==='penjualan';
      K.prefill=K.conf=K.prodIdx=K.savedFormState=null;
      if(wasPenjualan&&justTrxId){K.receiptRaw=formatNota(justTrxId);setView('nota',null);render();}
      else if(pType){K.cur='produk';K.grid=true;setView('list',null);render();}
      else{setView('list',null);render();}
    }finally{K.writing=false;}
  },
  'del-row':()=>{
    if(K.conf!=='del'){K.conf='del';render();return;}
    if(K.idx==null||K.idx<0)return;
    const pType=parentOf(K.cur),pid=pType?(K.db[K.cur][K.idx]?.[cI(K.cur,fkCol(K.cur))]||K.parent?.id):null,id=K.db[K.cur][K.idx]?.[0],delCicilJual=K.cur==='cicil'?cV(K.db[K.cur][K.idx],'cicil','idjual'):null,rem=K.db[K.cur].splice(K.idx,1)[0];
    syncStockDelta(K.cur,rem,null);save(rem,K.cur,0);
    if(isTrx()&&id){
      const ch=childOf(K.cur),fkI=cI(ch,fkCol(ch));
      if(fkI!==-1){(K.db[ch]||[]).filter(r=>S2(r[fkI])===S2(id)).forEach(r=>{syncStockDelta(ch,r,null);save(r,ch,0);});K.db[ch]=K.db[ch].filter(r=>S2(r[fkI])!==id);}
      if(K.cur==='penjualan'&&K.db.cicil){K.db.cicil.filter(c=>S2(cV(c,'cicil','idjual'))===S2(id)).forEach(c=>save(c,'cicil',0));K.db.cicil=K.db.cicil.filter(c=>S2(cV(c,'cicil','idjual'))!==id);}
    }
    if(K.cur==='kontak'&&id){['penjualan','pembelian','cicil'].forEach(sh=>{const ki=cI(sh,'idkontak');if(ki!==-1&&K.db[sh]){K.db[sh].forEach(r=>{if(S2(r[ki])===S2(id)){r[ki]='';save(r,sh,1);}});}});}
    persist();if(pType&&pid)syncTrx(pType);if(delCicilJual)syncTrx('penjualan');setView('list',null);render();
  },
  'dup-row':()=>{if(K.idx>=0){const old=(K.db[K.cur][K.idx]||[]).slice(),c=cols();K.prefill={};c.forEach((col,i)=>{K.prefill[col]=old[i]||'';});K.prefill[c[0]]=genId();setView('form',-1);render();}},
  'trx-pick-kontak':()=>{if(!K.shs.includes('kontak'))return;const curState={};dom.fb.querySelectorAll('[data-col]').forEach(inp=>{curState[inp.dataset.col]=inp.value;});K.savedFormState=curState;K.refTargetInp='idkontak';K.cur='kontak';setView('list',null);render();},
  'trx-set-kontak':()=>{
    const pType=K.parent?.type||'penjualan',op=getOpenTrx(pType);if(!op)return;
    const r=K.db.kontak?.[K.idx]||[],kId=cV(r,'kontak','idkontak')||r[0]||'';if(!kId)return;
    const trgRow=K.db[pType]?.find(x=>S2(x[0])===S2(op.id));
    if(trgRow&&cI(pType,'idkontak')!==-1){trgRow[cI(pType,'idkontak')]=kId;save(trgRow,pType,1);}
    persist();if(op.id)syncTrx(pType);K.cur=pType;setView('form',(K.db[pType]||[]).indexOf(trgRow));render();
  },
  'pay-pas':()=>{
    const r=(K.idx!==null&&K.idx>=0)?K.db[K.cur][K.idx]:null,pid=r?r[0]:(dom.fb.querySelector(`[data-col="${CSS.escape(cols()[0])}"]`)?.value||''),bInp=dom.fb.querySelector('[data-col="bayar"]');
    if(bInp&&pid){const dVal=N(dom.fb.querySelector('[data-col="diskon"]')?.value||(r?cV(r,K.cur,'diskon'):0)),{raw,disc}=calcItems(K.cur,pid);bInp.value=String(Math.max(0,raw-disc-dVal));}
  },
  revisi:()=>{if(K.idx>=0){const r=K.db[K.cur][K.idx],pid=r?.[0];if(pid){K.parent={type:K.cur,id:pid,open:true};if(cI(K.cur,'status')!==-1){r[cI(K.cur,'status')]='revisi';save(r,K.cur,1);persist();}K.cur=childOf(K.cur);setView('list',null);render();}}},
  cicil:()=>{
    if(K.idx>=0){
      const r=K.db[K.cur][K.idx],pid=r?.[0];
      if(pid){
        if(!K.shs.includes('cicil'))K.shs.push('cicil');
        const{raw,disc}=calcItems('penjualan',pid),net=Math.max(0,raw-disc-N(cV(r,'penjualan','diskon'))),paid=N(cV(r,'penjualan','bayar'))+(K.db.cicil||[]).reduce((a,c)=>a+(S2(cV(c,'cicil','idjual'))===S2(pid)?N(cV(c,'cicil','jumlah')):0),0),remDebt=Math.max(0,net-paid);
        K.cur='cicil';K.prefill={idjual:pid,idkontak:cV(r,'penjualan','idkontak'),date:today(),user:K.u||'admin',jumlah:remDebt>0?String(remDebt):''};setView('form',-1);render();
      }
    }
  },
  'trx-rincian':()=>{if(K.idx>=0){const pid=K.db[K.cur][K.idx]?.[0];if(pid){K.parent={type:K.cur,id:pid,open:true};K.cur=childOf(K.cur);setView('list',null);render();}}},
  'show-nota':()=>{const pid=(K.cur==='penjualan'&&K.idx>=0)?K.db.penjualan[K.idx]?.[0]:(K.parent?.type==='penjualan'?K.parent.id:K.db.penjualan?.[0]?.[0]);if(!pid)return;K.receiptRaw=formatNota(pid);setView('nota',null);render();},
  rawbt:()=>{if(!K.receiptRaw)return;const txt=K.receiptRaw.trim()+'\r\n\r\n\r\n',u=encodeURIComponent(txt);if(/Android/i.test(navigator.userAgent))W.location.href=`intent:${u}#Intent;scheme=rawbt;package=ru.a402d.rawbtprinter;end;`;else W.location.href=`rawbt:${u}`;},
  jpg:()=>{
    if(!K.receiptRaw)return;
    const lines=K.receiptRaw.split(/\r?\n/),lh=18,fs=13,fontStr=`${fs}px monospace`,mCan=document.createElement('canvas'),mCtx=mCan.getContext('2d');
    mCtx.font=fontStr;let maxW=0;lines.forEach(l=>{const w=mCtx.measureText(l).width;if(w>maxW)maxW=w;});
    const w=Math.ceil(maxW)||280,h=lines.length*lh+4,can=document.createElement('canvas');can.width=w;can.height=h;
    const ctx=can.getContext('2d');ctx.fillStyle='#ffffff';ctx.fillRect(0,0,w,h);ctx.fillStyle='#000000';ctx.font=fontStr;ctx.textBaseline='top';
    lines.forEach((l,i)=>ctx.fillText(l,0,i*lh+2));
    can.toBlob(blob=>{
      if(!blob)return;
      const dlImg=()=>{const u=URL.createObjectURL(blob),a=document.createElement('a');a.href=u;a.download=`nota_${Date.now()}.png`;document.body.appendChild(a);a.click();setTimeout(()=>{a.remove();URL.revokeObjectURL(u);},500);};
      try{if(navigator.clipboard&&W.ClipboardItem)navigator.clipboard.write([new ClipboardItem({'image/png':blob})]).catch(()=>dlImg());else dlImg();}catch{dlImg();}
    },'image/png');
  },
  print:()=>W.print(),
  'save-store':()=>{
    const c=K.cols.toko||['id','name','sub','phone','bank','footer'];
    const curRow=K.db.toko?.[0]||new Array(c.length).fill('');
    const newRow=curRow.slice(0,c.length);
    while(newRow.length<c.length)newRow.push('');
    newRow[0]='store_1';
    dom.fb.querySelectorAll('[data-cfg]').forEach(i=>{
      const ci=c.indexOf(i.dataset.cfg);
      if(ci>-1)newRow[ci]=i.value.trim();
    });
    K.db.toko=[newRow];
    save(newRow,'toko',K.db.toko.length?1:2);
    persist();
    syn(true);
    setView('list',null);
    render();
  },
  'reset-db':async()=>{
    if(K.conf!=='reset-db'){K.conf='reset-db';return msg('reset? klik lagi');}
    K.conf=null;
    if('caches' in window){
      try{
        const ks=await caches.keys();
        await Promise.all(ks.map(k=>caches.delete(k)));
      }catch(_){}
    }
    idbClearStore('kv');
    idbClearStore('mut');
    K.shs.forEach(s=>{K.db[s]=[];});
    L.vs='[]';
    persist();
    render();
    const ok = await syn(true);
    if(!ok)msg('offline');
    render();
  },
  'pwa-install':()=>{if(K.pwaEvent){K.pwaEvent.prompt();K.pwaEvent.userChoice.then(()=>{K.pwaEvent=null;render();});}},
  'save-schema':()=>{
    const nc=(dom.fb.querySelector('[data-f="cols"]')?.value||'').split(',').map(S2).filter(Boolean);
    if(nc.length){
      K.cols[K.cur]=nc;K.vis[K.cur]=(dom.fb.querySelector('[data-f="vis"]')?.value||'').split(',').map(S2).filter(x=>nc.includes(x))||nc.slice(0,2);
      K.fvis[K.cur]=(dom.fb.querySelector('[data-f="form"]')?.value||'').split(',').map(S2).filter(x=>nc.includes(x))||nc.slice();
      (K.db[K.cur]||[]).forEach(r=>{if(r.length>nc.length)r.length=nc.length;while(r.length<nc.length)r.push('');});
      persist();syn(true);
    }
    setView('list',null);render();
  },
  'save-bulk':()=>{
    const cLen=cols().length;
    dom.ls.querySelectorAll('[data-idx]').forEach(div=>{
      const idx=parseInt(div.dataset.idx,10);
      if(!isNaN(idx)&&K.db[K.cur][idx]){const pts=div.textContent.split('|').map(s=>s.trim());while(pts.length<cLen)pts.push('');const row=pts.slice(0,cLen);K.db[K.cur][idx]=row;save(row,K.cur,1);}
    });
    persist();syn(true);setView('list',null);render();
  },
  grid:()=>{K.grid=!K.grid;render();},
  dir:()=>{K.dir=-K.dir;K.page=1;render();},
  more:()=>{K.page++;render();},
  logout:()=>{K.u=L.u='';K.hist=[];K.readyOnly=false;persist();render();},
  cam:()=>dom.fCam.click(),
  pic:()=>dom.fImg.click(),
  'doc-upload':()=>dom.fDoc.click(),
  gimg:()=>{const q=dom.fb.querySelector('[data-col="namaproduk"]')?.value||dom.fb.querySelector('[data-col="nama"]')?.value||dom.q.value||'';if(q)W.open('https://www.google.com/search?tbm=isch&q='+encodeURIComponent(q),'_blank','noopener,noreferrer');},
  navigasi:()=>{const loc=(Array.from(dom.fb.querySelectorAll('[data-col]')).find(i=>/alamat|lokasi/i.test(i.dataset.col))?.value||'').trim();if(loc){if(/Android|iPhone|iPad/i.test(navigator.userAgent))W.location.href=`geo:${loc}?q=${encodeURIComponent(loc)}`;else W.open(`https://www.google.com/maps/dir/?api=1&destination=${encodeURIComponent(loc)}`,'_blank','noopener,noreferrer');}},
  gps:()=>{const trg=Array.from(dom.fb.querySelectorAll('[data-col]')).find(i=>/alamat|lokasi/i.test(i.dataset.col));if(!trg||!navigator.geolocation)return;navigator.geolocation.getCurrentPosition(p=>{trg.value=`${p.coords.latitude.toFixed(6)},${p.coords.longitude.toFixed(6)}`;},()=>msg('gps err'),{enableHighAccuracy:true,timeout:5000});},
  'sh-sel':el=>{K.cur=el.value;K.parent=K.prefill=K.prodIdx=K.savedFormState=null;setView('list',null);K.page=1;K.readyOnly=false;render();},
  'st-sel':el=>{K.sort=parseInt(el.value,10)||0;K.page=1;render();},
  'act-sel':el=>{
    const a=el.value;el.value='';if(!a)return;
    if(a==='edit'){setView(K.v==='edit'?'list':'edit',null);render();}
    else if(a==='bulksheet'||a==='schema'){setView(a,null);render();}
    else if(a==='tsv'){
      const c=cols(),rows=K.db[K.cur]||[],tsvContent=[c.join('\t'),...rows.map(r=>r.join('\t'))].join('\n');
      dl(tsvContent,`${K.cur}_${today()}.tsv`,'text/tab-separated-values;charset=utf-8');
    }
    else if(a==='imp'){dom.fImp.click();}
    else if(a==='clear'){if(K.conf!=='clear'){K.conf='clear';msg(`clear ${K.cur}?`);return;}K.db[K.cur].forEach(r=>save(r,K.cur,0));K.db[K.cur]=[];K.conf=null;persist();syn(true);render();}
  },
  'preset-kasir':()=>{
    const hInp=dom.fb.querySelector('[data-col="hide"]'),vInp=dom.fb.querySelector('[data-col="validasi"]');
    if(hInp)hInp.value='hargabeli,profit,users,roles,toko,bulksheet,schema,clear,del-row,reset-db';
    if(vInp)vInp.value='wajibstok,wajibkontak,wajibabsen';
  },
  'preset-gudang':()=>{
    const hInp=dom.fb.querySelector('[data-col="hide"]'),vInp=dom.fb.querySelector('[data-col="validasi"]');
    if(hInp)hInp.value='hargajual,profit,penjualan,produk_jual,cicil,users,roles,toko,bulksheet,schema,clear,reset-db';
    if(vInp)vInp.value='wajibstok,wajibfoto';
  }
};

/* Delegasi Event Global */
document.addEventListener('click',e=>{
  const b=e.target.closest('[data-a]');
  if(b){
    const a=b.dataset.a;
    if(a.startsWith('ref-')){
      const trg=a.replace('ref-',''),cName=b.dataset.refCol||'',val=cName?(dom.fb.querySelector(`[data-col="${CSS.escape(cName)}"]`)?.value||''):'',curState={};
      dom.fb.querySelectorAll('[data-col]').forEach(inp=>{curState[inp.dataset.col]=inp.value;});
      K.savedFormState=curState;K.refTargetInp=cName;K.cur=trg;dom.q.value=val.trim();K.page=1;setView('list',null);return render();
    }
    if(!['del-row','clear-items','act-sel','reset-db'].includes(a)&&K.conf){K.conf=null;msg();}
    return actions[a]?.(b,e);
  }
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
    const next=dom.db.querySelector(`[data-df="${CSS.escape(k)}"]`);if(next){next.focus();if(typeof pos==='number')try{next.setSelectionRange(pos,pos);}catch{}}
    return;
  }
  if(e.target.dataset.col){
    const col=e.target.dataset.col,val=(e.target.value||'').trim(),trgSh=resolveRef(col);
    updateFormThumb(e.target);
    if(trgSh){const refBtn=dom.ft.querySelector(`[data-a="ref-${CSS.escape(trgSh)}"]`);if(val===''&&refBtn)refBtn.remove();else if(val!==''&&!refBtn&&K.shs.includes(trgSh)&&(K.db[trgSh]||[]).length>0)addFooter(`ref-${trgSh}`,`ref ${trgSh}`,K.cur,`data-ref-col="${esc(col)}"`);}
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
    const rd=new FileReader();
    rd.onload=ev=>{
      if(tInp){
        tInp.value=ev.target.result||'';
        updateFormThumb(tInp);
      }
    };
    rd.readAsDataURL(file);
    e.target.value='';
  }
  if(e.target===dom.fImp){
    const file=e.target.files?.[0];if(!file)return;
    const rd=new FileReader();
    rd.onload=ev=>{
      const text=ev.target.result||'',lines=text.split(/\r?\n/).filter(Boolean);
      if(!lines.length)return;
      const delimiter=lines[0].includes('\t')?'\t':(lines[0].includes(';')?';':','),firstRow=lines[0].split(delimiter).map(s=>s.trim().toLowerCase()),c=cols();
      let startIdx=0;
      if(firstRow.every(h=>c.includes(h)))startIdx=1;
      for(let i=startIdx;i<lines.length;i++){
        const parts=lines[i].split(delimiter).map(s=>s.trim()),row=new Array(c.length).fill('');
        for(let j=0;j<Math.min(parts.length,c.length);j++)row[j]=parts[j];
        if(!row[0])row[0]=genId();
        (K.db[K.cur]=K.db[K.cur]||[]).push(row);
        save(row,K.cur,2);
      }
      persist();syn(true);render();
    };
    rd.readAsText(file);
    e.target.value='';
  }
});

document.addEventListener('keydown',e=>{
  if(e.key==='Enter'&&e.target.tagName!=='TEXTAREA'&&dom.fb.contains(e.target)){
    e.preventDefault();if(K.writing)return;
    if(!K.u)actions.login();else if(['bulksheet','toko','schema'].includes(K.v))actions[`save-${K.v}`]();else actions['save-row']();
  }else if(e.key==='Escape'){
    if(!dom.scanBox.hidden)stopScan();
    else actions.back();
  }
});

W.addEventListener('beforeinstallprompt',e=>{e.preventDefault();K.pwaEvent=e;if(K.v==='menu'||K.v==='dash')render();});
if('serviceWorker' in navigator&&(W.location.protocol.startsWith('http')||W.location.hostname==='localhost')){
  W.addEventListener('load',()=>{navigator.serviceWorker.register('sw.js').catch(()=>{});});
}
W.addEventListener('online',()=>syn(true));

// Booting Monoflow
(()=>{
  try{
    const d=JSON.parse(L.getItem('apex')||'null');
    if(d&&typeof d==='object'){
      ['shs','cols','vis','fvis'].forEach(k=>{if(d[k])K[k]=d[k];});
      if(d.parent&&typeof d.parent==='object')K.parent=d.parent;
      if(d.cur&&K.shs.includes(d.cur))K.cur=d.cur;
      if(d.u)K.u=L.u=d.u;
      if(d.df&&typeof d.df==='object')K.df=Object.assign(K.df,d.df);
      K.grid=Boolean(d.grid);
    }
  }catch{}
  
  initDB(async()=>{
    const localMatrix=await idbGet('apex_matrix');
    if(localMatrix&&typeof localMatrix==='object')Object.assign(K.db,localMatrix);
    render();
    syn(true);
  });
})();