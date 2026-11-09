'use strict';
/* ============================================================
   APEX ULTRA KISS ABSOLUTE — MONOFLOW, ZERO-BLOAT, ANTI-BUG
   ============================================================ */
const W=window,L=localStorage,$=id=>document.getElementById(id);
let db=null,syncing=false,syncDebounce=null,scanStream=null,scanTimer=null;

const bc=typeof BroadcastChannel!=='undefined'?new BroadcastChannel('sync'):{postMessage(){},onmessage:null};
bc.onmessage=e=>{if(e.data==='synced')render();else if(e.data==='kick'&&!syncing)syn();};

/* ---------------- IndexedDB: store 'kv' (matrix) & 'mut' (antrean mutasi) ---------------- */
function initDB(cb){
  try{
    const rq=indexedDB.open('apex_db',1);
    rq.onupgradeneeded=e=>{
      const d=e.target.result;
      if(!d.objectStoreNames.contains('kv'))d.createObjectStore('kv');
      if(!d.objectStoreNames.contains('mut'))d.createObjectStore('mut',{keyPath:'i'});
    };
    rq.onsuccess=e=>{db=e.target.result;cb?.(db);};
    rq.onerror=()=>cb?.(null);
  }catch{cb?.(null);}
}
const idbStore=(st,mode)=>db.transaction(st,mode).objectStore(st);
const idbRead=(st,method,arg,fallback)=>new Promise(res=>{
  if(!db)return res(fallback);
  try{const rq=idbStore(st,'readonly')[method](arg);rq.onsuccess=()=>res(rq.result??fallback);rq.onerror=()=>res(fallback);}
  catch{res(fallback);}
});
const idbGet=(key,st='kv')=>idbRead(st,'get',key,null);
const idbGetAll=(st='mut')=>idbRead(st,'getAll',undefined,[]);
function idbPut(val,key=null,st='kv'){
  if(!db)return;
  try{const s=idbStore(st,'readwrite');if(st==='kv'&&key!=null)s.put(val,key);else s.put(val);}catch{}
}
function idbClearStore(st){if(!db)return;try{if(db.objectStoreNames.contains(st))idbStore(st,'readwrite').clear();}catch{}}
function idbDelKeys(keys,st='mut'){
  if(!db||!keys?.length)return;
  try{const s=idbStore(st,'readwrite');keys.forEach(k=>s.delete(k));}catch{}
}

/* ---------------- Antrean mutasi ---------------- */
function save(row,targetTable=K.cur,actionType=2){
  const tIdx=TI(targetTable);
  idbPut({
    i:Date.now().toString(36)+Math.random().toString(36).slice(2,6),
    t:tIdx>-1?tIdx:targetTable,
    tn:targetTable,
    y:actionType,
    r:Array.isArray(row)?row:Object.values(row),
    ts:Date.now()
  },null,'mut');
  bc.postMessage('kick');
  if(!syncing){clearTimeout(syncDebounce);syncDebounce=setTimeout(()=>syn(false),50);}
}

/* ---------------- Transport: D1 -> Apps Script ---------------- */
const isOk=r=>Boolean(r?.ok||r?.s==='ok'||r?.status==='ok');
const SYNC_ACTIONS=['pull_batch','manifest','push'];

async function req(p={}){
  p.u=p.u||L.u||K.u||'admin';
  if(HARD.d1Url&&SYNC_ACTIONS.includes(p.a)){
    try{
      const res=await fetch(HARD.d1Url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});
      if(res.ok){const r=await res.json();if(isOk(r))return r;}
    }catch{}
  }
  return new Promise(res=>{
    const done=j=>res({ok:j?.ok??j?.s==='ok',...j});
    if(W.google?.script?.run){
      W.google.script.run.withSuccessHandler(done).withFailureHandler(e=>res({ok:false,e})).core(p);
      return;
    }
    if(HARD.gasUrl&&/^https?:\/\//i.test(HARD.gasUrl)){
      fetch(HARD.gasUrl,{method:'POST',headers:{'Content-Type':'text/plain;charset=utf-8'},body:JSON.stringify(p),redirect:'follow'})
        .then(r=>r.json()).then(done).catch(e=>res({ok:false,e}));
      return;
    }
    res({ok:false,e:'offline'});
  });
}

/* ---------------- Sync: push mutasi -> manifest -> pull yang berubah ---------------- */
async function syn(force=false){
  if(!navigator.onLine||(syncing&&!force)||!db)return false;
  syncing=true;msg('sedang sync...');
  let ok=false;
  try{
    const tables=K.shs||HARD.shs;
    if(force)L.vs='[]';
    const ver=JSON.parse(L.vs||'[]'),muts=await idbGetAll('mut');
    let m=null;

    // PUSH: mutasi antrean; snapshot lengkap hanya saat refresh paksa
    if(muts.length||force){
      const pr=await req({a:'push',m:muts,tn:tables,db:force?K.db:undefined,cols:K.cols,shs:K.shs,gasUrl:HARD.gasUrl});
      if(pr?.ok){
        if(muts.length)idbDelKeys(muts.map(x=>x.i),'mut');
        m=pr.manifest||pr.data?.manifest||null;
        ok=true;
      }
    }

    // MANIFEST
    if(!m){const r=await req({a:'manifest',tn:tables,cols:K.cols});m=r?.data||r;}
    if(Array.isArray(m?.shs))m.shs.forEach(s=>{if(!K.shs.includes(s))K.shs.push(s);});
    if(m?.cols&&typeof m.cols==='object')Object.assign(K.cols,m.cols);
    if(m?.vis&&typeof m.vis==='object')Object.assign(K.vis,m.vis);

    // PULL
    if(m?.v){
      ok=true;
      const toPull=[];
      tables.forEach((_,i)=>{if(force||(m.v[i]&&m.v[i]!==ver[i]))toPull.push({t:i,v:force?'':(ver[i]||'')});});
      if(toPull.length){
        const b=await req({a:'pull_batch',reqs:toPull,tn:tables,cols:K.cols});
        (b?.data?.results||b?.results||[]).forEach(r=>{
          if(!r?.v)return;
          ver[r.t]=r.v;
          const name=K.shs[r.t];
          if(!name)return;
          K.db[name]=r.r||[];
          if(Array.isArray(r.cols)&&r.cols.length)K.cols[name]=r.cols;
        });
        L.vs=JSON.stringify(ver);persist();bc.postMessage('synced');render();
      }
    }else if(m?.db){
      ok=true;
      Object.assign(K.db,m.db);persist();bc.postMessage('synced');render();
    }
  }catch{
    ok=false;
  }finally{
    syncing=false;
  }
  msg(ok?'':'offline');
  return ok;
}

/* ---------------- State ---------------- */
const TI=s=>K.shs.indexOf(s);
const dl=(content,fileName,mime='text/plain;charset=utf-8')=>{
  const u=URL.createObjectURL(new Blob([content],{type:mime})),a=document.createElement('a');
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

// Tabel toko: ['id','name','sub','phone','bank','footer']
if(!K.shs.includes('toko'))K.shs.push('toko');
if(!K.cols.toko)K.cols.toko=['id','name','sub','phone','bank','footer'];
if(!K.vis.toko)K.vis.toko=['id','name','phone'];
if(!K.db.toko)K.db.toko=[];

const getStore=()=>{
  const r=K.db.toko?.[0]||[],c=K.cols.toko;
  const g=k=>{const i=c.indexOf(k);return i>-1&&r[i]!==undefined?r[i]:(HARD.store?.[k]||'');};
  return{name:g('name'),sub:g('sub'),phone:g('phone'),bank:g('bank'),footer:g('footer')};
};

const esc=s=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const isSafeUrl=u=>{const s=String(u||'').trim();return/^https?:\/\//i.test(s)||/^data:(image|application|text)\/[a-zA-Z0-9.\-_+]+;base64,/i.test(s);};
const dom={
  h:$('h'),q:$('q'),msg:$('msg'),main:$('main'),tools:$('tools'),
  scanBox:$('scan-box'),scanVid:$('scan-vid'),
  fCam:$('f-cam'),fImg:$('f-img'),fDoc:$('f-doc'),fImp:$('f-imp'),
  sh:$('sh-sel'),st:$('st-sel'),act:$('act-sel'),
  fb:$('box-form'),db:$('box-dash'),ls:$('list'),ft:$('ft'),tape:$('tape'),bn:$('box-nota')
};

/* ---------------- Helper data ---------------- */
const S2=s=>String(s||'').trim().toLowerCase();
const N=v=>{if(v==null)return 0;const n=parseFloat(String(v).replace(/[^\d.-]/g,''));return(!isFinite(n)||n<0)?0:n;};
const cols=(s=K.cur)=>K.cols[s]||[];
const cI=(s,k)=>cols(s).indexOf(k);
const cV=(r,s,k)=>{const i=cI(s,k);return(r&&i>-1&&r[i]!==undefined)?String(r[i]):'';};
const vis=(s=K.cur)=>(K.vis[s]||cols(s)).filter(c=>cols(s).includes(c));
const fvis=(s=K.cur)=>((K.fvis?.[s]?.length)?K.fvis[s]:cols(s)).filter(c=>cols(s).includes(c));

const TRXM={penjualan:{ch:'produk_jual'},pembelian:{ch:'produk_beli'}};
const isTrx=(s=K.cur)=>Boolean(TRXM[s]);
const childOf=s=>TRXM[s]?.ch||null;
const parentOf=s=>s==='produk_jual'?'penjualan':(s==='produk_beli'?'pembelian':null);
const fkCol=s=>s==='produk_jual'?'idpenjualan':(s==='produk_beli'?'idpembelian':(s==='cicil'?'idjual':null));
const STOCK_SHEETS=['produk_jual','produk_beli','ecer'];
const NO_ADD=['penjualan','pembelian','produk_jual','produk_beli','cicil','ecer'];
const EXEMPT=['back','menu','q','dir','grid','closemsg','more','reset-db','add','df-today','df-clear','login','reg','bypass','pwa-install','v-toko','v-dash','v-kas','v-shop','logout','refresh','scan','stop-scan','theme'];
const FORM_VIEWS=['form','toko','schema','bulksheet','cms_detail','edit'];
const GEO_RE=/alamat|lokasi/i;

const PRESETS={
  kasir:{hide:'hargabeli,profit,users,roles,toko,bulksheet,schema,clear,del-row,reset-db',validasi:'wajibstok,wajibkontak,wajibabsen'},
  gudang:{hide:'hargajual,profit,penjualan,produk_jual,cicil,users,roles,toko,bulksheet,schema,clear,reset-db',validasi:'wajibstok,wajibfoto'}
};

const genId=()=>{const d=new Date();return String(d.getHours()).padStart(2,'0')+String(d.getMinutes()).padStart(2,'0')+Math.random().toString(36).slice(2,5).padEnd(3,'0');};
const fmtNum=n=>N(n).toLocaleString('id-ID');
const today=()=>new Date(Date.now()-new Date().getTimezoneOffset()*60000).toISOString().slice(0,10); // tanggal lokal, bukan UTC
const msg=t=>{dom.msg.innerHTML='';dom.msg.hidden=!t;if(t)dom.msg.textContent=t;};

/* Tema light / dark (default: ikut sistem sampai tombol ditekan) */
const getTheme=()=>L.theme||(W.matchMedia?.('(prefers-color-scheme: dark)').matches?'dark':'light');
const applyTheme=t=>{
  document.documentElement.dataset.theme=t;
  document.querySelector('meta[name="theme-color"]')?.setAttribute('content',t==='dark'?'#000':'#fff');
};

/* Selector form */
const fld=c=>dom.fb.querySelector(`[data-col="${CSS.escape(c)}"]`);
const flds=()=>[...dom.fb.querySelectorAll('[data-col]')];
const fldBy=re=>flds().find(i=>re.test(i.dataset.col));
const formState=()=>Object.fromEntries(flds().map(i=>[i.dataset.col,i.value]));

/* ---------------- Navigasi & hak akses ---------------- */
const setView=(v,idx=null)=>{
  if(dom.q&&dom.q.value.trim()!==''){dom.q.value='';K.page=1;}
  if(K.v!==v||K.idx!==idx){
    K.hist.push({v:K.v,cur:K.cur,idx:K.idx,parent:K.parent?{...K.parent}:null,readyOnly:K.readyOnly,savedFormState:K.savedFormState});
    if(K.hist.length>25)K.hist.shift();
  }
  K.v=v;K.idx=idx;K.conf=null;
};
const getRole=()=>{
  if(!K.u)return'user';
  const u=K.db.users?.find(r=>S2(r[0])===S2(K.u));
  return S2(u?.[1]||'user');
};
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
const getRoleRules=()=>{
  if(!K.u)return[];
  const v=(K.db.roles?.find(r=>S2(r[0])===getRole())?.[2]||'').toLowerCase();
  return v.split(',').map(S2).filter(Boolean);
};
const hasAccess=(k,s=K.cur)=>{
  const key=S2(k);
  if(EXEMPT.includes(key)||key.startsWith('df-')||key.startsWith('ref-'))return true;
  if(!K.u)return false;
  if(isAdmin())return true;
  const hidden=(K.db.roles?.find(r=>S2(r[0])===getRole())?.[1]||'').toLowerCase();
  if(!hidden)return true;
  const list=hidden.split(',').map(S2),sh=S2(s);
  return!(list.includes('all')||list.includes('*')||list.includes(key)||(s&&list.includes(`${sh}.${key}`))||(s&&list.includes(sh)));
};
const addFooter=(a,txt,sheet=K.cur,extra='')=>{
  if(hasAccess(a,sheet))dom.ft.insertAdjacentHTML('beforeend',`<button data-a="${esc(a)}" ${extra}>${esc(txt)}</button> `);
};

/* ---------------- Stok ---------------- */
const syncStockDelta=(sh,o,n)=>{
  if(!STOCK_SHEETS.includes(sh))return;
  const m=sh==='produk_beli'?1:-1,si=cI('produk','stok');
  [{r:o,s:-m},{r:n,s:m}].forEach(({r,s})=>{
    if(!r)return;
    const id=cV(r,sh,'idproduk'),row=id&&K.db.produk?.find(k=>S2(k[0])===S2(id));
    if(row&&si>-1)row[si]=String(N(row[si])+N(cV(r,sh,'qty'))*s);
  });
};

/* ---------------- Barcode scanner ---------------- */
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
        if(codes?.length){const val=codes[0].rawValue;stopScan();dom.q.value=val;K.page=1;render();}
      }catch{}
    },250);
  }).catch(()=>msg('kamera tidak aktif'));
};

/* ---------------- Perhitungan transaksi ---------------- */
const calcItems=(t,id)=>{
  const ch=childOf(t);
  if(!ch)return{raw:0,disc:0,cogs:0,pot:0,items:[]};
  return(K.db[ch]||[]).reduce((a,r)=>{
    if(S2(cV(r,ch,fkCol(ch)))!==S2(id))return a;
    const q=N(cV(r,ch,'qty')),p=N(cV(r,ch,t==='penjualan'?'hargajual':'hargabeli')),
          b=N(cV(r,ch,'hargabeli')),s=N(cV(r,ch,'hargajual')),d=N(cV(r,ch,'diskon')),
          dLine=d>0?(d<p?d*q:d):0,dU=q>0?dLine/q:0;
    a.raw+=q*p;a.disc+=dLine;a.cogs+=q*b;a.pot+=q*s;
    a.items.push({name:cV(r,ch,'namaproduk')||'item',id:cV(r,ch,'idproduk'),q,p,b,s,dU,eff:Math.max(0,p-dU),tot:q*p-dLine});
    return a;
  },{raw:0,disc:0,cogs:0,pot:0,items:[]});
};
const trxNet=(t,r)=>{const{raw,disc}=calcItems(t,r[0]);return Math.max(0,raw-disc-N(cV(r,t,'diskon')));};
const cicilMap=(pass=()=>true)=>{
  const m={};
  (K.db.cicil||[]).forEach(c=>{if(!pass(c))return;const id=S2(cV(c,'cicil','idjual'));m[id]=(m[id]||0)+N(cV(c,'cicil','jumlah'));});
  return m;
};
const cicilSum=id=>(K.db.cicil||[]).reduce((a,c)=>a+(S2(cV(c,'cicil','idjual'))===S2(id)?N(cV(c,'cicil','jumlah')):0),0);
const paidOf=(t,r)=>N(cV(r,t,'bayar'))+(t==='penjualan'?cicilSum(r[0]):0);
const statusFor=(net,paid,kId)=>net<=0?'draft':paid>=net?'lunas':kId?'piutang':'draft';

const calcContactDebt=kId=>{
  if(!kId)return 0;
  const cm=cicilMap();
  return(K.db.penjualan||[]).reduce((sum,r)=>{
    if(S2(cV(r,'penjualan','idkontak'))!==S2(kId)||!isUserVisible(r,'penjualan'))return sum;
    const paid=N(cV(r,'penjualan','bayar'))+(cm[S2(r[0])]||0);
    return sum+Math.max(0,trxNet('penjualan',r)-paid);
  },0);
};

const formatNota=id=>{
  const row=K.db.penjualan?.find(r=>S2(r[0])===S2(id));
  if(!row)return'';
  const val=k=>cV(row,'penjualan',k),paid=paidOf('penjualan',row),{raw,disc,items}=calcItems('penjualan',id),
        gTot=Math.max(0,raw-disc-N(val('diskon'))),diff=paid-gTot,WL=32,store=getStore(),
        pad=s=>{const t=String(s||'').trim();if(t.length>=WL)return t.slice(0,WL);const l=Math.floor((WL-t.length)/2);return' '.repeat(l)+t+' '.repeat(WL-t.length-l);},
        lr=(l,r)=>l+' '.repeat(Math.max(1,WL-l.length-String(r).length))+r,
        bar=c=>c.repeat(WL),
        lines=[pad(store.name),store.sub&&pad(store.sub),store.phone&&pad(store.phone),store.bank&&pad(store.bank),bar('='),
               lr(`${val('date')||today()} ${id}`,val('user')||K.u||'admin'),bar('-')].filter(Boolean);

  if(!items.length)lines.push('(kosong)');
  items.forEach(it=>{
    lines.push(it.name);
    if(it.dU>0)lines.push(`${fmtNum(it.p)} -disc ${fmtNum(it.dU)}`);
    lines.push(lr(`${it.q}x ${fmtNum(it.eff)}`,fmtNum(it.tot)));
  });
  lines.push(bar('-'),lr('Subtotal',fmtNum(raw)));
  if(disc>0)lines.push(lr('Diskon Item','-'+fmtNum(disc)));
  if(N(val('diskon'))>0)lines.push(lr('Diskon','-'+fmtNum(val('diskon'))));
  lines.push(lr('Total',fmtNum(gTot)),lr('Bayar',fmtNum(paid)),lr(diff>=0?'Kembali':'Kurang',fmtNum(Math.abs(diff))),bar('-'));
  if(store.footer)lines.push(pad(store.footer));

  const kId=val('idkontak'),kRow=kId&&K.db.kontak?.find(r=>S2(r[0])===S2(kId)),kDisp=kRow?(cV(kRow,'kontak','nama')||kId):kId;
  if(kDisp)lines.push(' '+kDisp);
  if(val('catatan'))lines.push(' '+val('catatan'));
  const debt=calcContactDebt(kId);
  if(debt>0)lines.push(lr('Total Piutang','-'+fmtNum(debt)));
  return lines.join('\n');
};

const syncTrx=t=>{
  const id=K.parent?.id||(K.idx>=0?K.db[t]?.[K.idx]?.[0]:null);
  if(!id)return;
  const r=K.db[t]?.find(x=>S2(x[0])===S2(id));
  if(!r)return;
  const sI=cI(t,'status');
  if(sI>-1&&r[sI]!=='revisi')r[sI]=statusFor(trxNet(t,r),paidOf(t,r),cV(r,t,'idkontak'));
  persist();save(r,t,1);
};

const getOpenTrx=t=>{
  if(!t)return null;
  const rows=K.db[t]||[],u=S2(K.u||'admin'),isOpen=r=>['draft','revisi'].includes(S2(cV(r,t,'status')));
  if(K.parent?.type===t){
    const r=rows.find(x=>S2(x[0])===S2(K.parent.id));
    if(r&&isUserVisible(r,t)&&isOpen(r))return{type:t,id:r[0],row:r,status:cV(r,t,'status')};
  }
  const r=rows.find(x=>(isAdmin()||S2(cV(x,t,'user'))===u)&&isOpen(x));
  return r?{type:t,id:r[0],row:r,status:cV(r,t,'status')||'draft'}:null;
};
const hasDraftItems=t=>{
  const ch=childOf(t),op=getOpenTrx(t);
  return Boolean(ch&&op&&K.db[ch]?.some(r=>S2(cV(r,ch,fkCol(ch)))===S2(op.id)));
};

/* Hapus baris yang cocok: sinkron stok + antre mutasi hapus */
const dropRows=(sh,pred)=>{
  const keep=[];
  (K.db[sh]||[]).forEach(r=>{if(pred(r)){syncStockDelta(sh,r,null);save(r,sh,0);}else keep.push(r);});
  K.db[sh]=keep;
};
const dropTrxDeps=(t,id)=>{
  const ch=childOf(t),fkI=ch?cI(ch,fkCol(ch)):-1;
  if(fkI!==-1)dropRows(ch,r=>S2(r[fkI])===S2(id));
  if(t==='penjualan')dropRows('cicil',c=>S2(cV(c,'cicil','idjual'))===S2(id));
};

/* ---------------- Produk -> form transaksi ---------------- */
const prefillProd=p=>({idproduk:p[0],namaproduk:p[3],hargabeli:p[4],hargajual:p[5],qty:'1',diskon:'',catatan:''});
const openProdForm=p=>{
  const isB=K.parent?.type==='pembelian',pType=isB?'pembelian':'penjualan';
  K.parent=getOpenTrx(pType)||{type:pType,id:genId(),open:true};
  K.cur=isB?'produk_beli':'produk_jual';
  K.prefill=prefillProd(p);
  K.prodIdx=(K.db.produk||[]).indexOf(p);
  setView('form',-1);render();
};
const prodTo=sheet=>{
  if(!(K.idx>=0))return;
  if(!K.shs.includes(sheet))K.shs.push(sheet);
  K.prefill=prefillProd(K.db.produk[K.idx]||[]);
  K.cur=sheet;K.prodIdx=K.idx;
  setView('form',-1);render();
};

const resolveRef=c=>{
  const col=S2(c);
  if(col==='user'||col==='idproduk'||col==='namaproduk'||STOCK_SHEETS.includes(K.cur))return null;
  const direct=K.shs.find(s=>S2(s)===col);
  if(direct)return direct;
  if(!col.startsWith('id'))return null;
  const naked=col.slice(2),exact=K.shs.find(s=>S2(s)===naked);
  if(exact)return exact;
  if(naked==='jual'||naked==='penjualan')return K.shs.find(s=>s==='penjualan')||null;
  if(naked==='beli'||naked==='pembelian')return K.shs.find(s=>s==='pembelian')||null;
  return K.shs.find(s=>S2(s).includes(naked)||naked.includes(S2(s)))||null;
};

const queryRows=()=>{
  const rows=K.db[K.cur]||[],q=S2(dom.q.value).split(/\s+/).filter(Boolean),
        pType=parentOf(K.cur),op=pType?getOpenTrx(pType):null,res=[];
  for(let i=0;i<rows.length;i++){
    const r=rows[i]||[];
    if(!isUserVisible(r,K.cur))continue;
    if(K.cur==='produk'&&K.readyOnly&&N(cV(r,'produk','stok'))<=0)continue;
    if(pType&&op&&S2(cV(r,K.cur,fkCol(K.cur)))!==S2(op.id))continue;
    if(!q.length||q.every(w=>r.join(' ').toLowerCase().includes(w)))res.push(i);
  }
  if(K.sort>0&&K.sort<=cols().length){
    const ci=K.sort-1;
    res.sort((a,b)=>String(rows[a]?.[ci]||'').localeCompare(String(rows[b]?.[ci]||''),undefined,{numeric:true})*K.dir);
  }else if(K.dir===-1)res.reverse();
  return res;
};

/* ---------------- Render ---------------- */
const renderChrome=()=>{
  const allowed=K.shs.filter(s=>hasAccess(s));
  if(dom.sh.dataset.shs!==allowed.join(',')){
    dom.sh.innerHTML=allowed.map(s=>`<option value="${esc(s)}">${esc(s)}</option>`).join('');
    dom.sh.dataset.shs=allowed.join(',');
  }
  if(!allowed.includes(K.cur)&&allowed.length)K.cur=allowed[0];
  dom.sh.value=K.cur;dom.sh.hidden=!hasAccess('sh-sel');
  const c=cols();
  if(dom.st.dataset.cur!==K.cur||dom.st.children.length!==c.length+1){
    dom.st.innerHTML='<option value="0">srt</option>'+c.map((col,i)=>`<option value="${i+1}">${esc(col)}</option>`).join('');
    dom.st.dataset.cur=K.cur;
  }
  dom.st.value=String(K.sort);dom.st.hidden=!hasAccess('st-sel');
  dom.tools.querySelector('[data-a="dir"]').hidden=!hasAccess('dir');
  dom.tools.querySelector('[data-a="grid"]').hidden=!hasAccess('grid');
  dom.act.hidden=!hasAccess('act-sel');
  [...dom.act.options].forEach(o=>{if(o.value)o.hidden=!hasAccess(o.value);});
  const addBtn=dom.h.querySelector('[data-a="add"]');
  if(addBtn)addBtn.hidden=K.v!=='list'||NO_ADD.includes(K.cur)||!hasAccess('add',K.cur);
};

const renderMenuFooters=()=>{
  addFooter('back','<');addFooter('refresh','refresh');addFooter('theme',getTheme()==='dark'?'light mode':'dark mode');addFooter('v-toko','toko');addFooter('reset-db','reset db');
  if(K.pwaEvent)addFooter('pwa-install','install pwa');
  addFooter('logout','logout');
};
const renderNavFooters=()=>{
  if(K.v!=='dash')addFooter('v-dash','dashboard');
  if(K.cur!=='kas'||K.v==='dash')addFooter('v-kas','kas');
  if(K.cur!=='produk'||K.v==='dash')addFooter('v-shop','produk');
  if(K.cur!=='produk_jual'&&hasDraftItems('penjualan'))addFooter('v-jual','jual');
  if(K.cur!=='produk_beli'&&hasDraftItems('pembelian'))addFooter('v-beli','beli');
};

const renderDash=()=>{
  msg();
  dom.db.insertAdjacentHTML('beforeend',`<input placeholder="user" data-df="u" value="${esc(K.df.u)}"> <input type="date" data-df="from" value="${esc(K.df.from)}"> <input type="date" data-df="to" value="${esc(K.df.to)}"> <input placeholder="kategori" data-df="kat" value="${esc(K.df.kat)}">`);
  addFooter('df-today','hari ini');addFooter('df-clear','clear filter');

  const qW=S2(dom.q.value).split(/\s+/).filter(Boolean),effU=isAdmin()?K.df.u:(K.u||'');
  const passFlt=(u,d,kt)=>
    (!effU||S2(u).includes(S2(effU)))&&
    (!K.df.from||String(d||'')>=K.df.from)&&
    (!K.df.to||String(d||'')<=K.df.to)&&
    (!K.df.kat||S2(kt).includes(S2(K.df.kat)))&&
    (!qW.length||qW.every(w=>`${u} ${d} ${kt}`.toLowerCase().includes(w)));

  // Kas
  let mAll=0,kAll=0,mFil=0,kFil=0;
  (K.db.kas||[]).forEach(r=>{
    if(!isUserVisible(r,'kas'))return;
    const m=N(cV(r,'kas','masuk')),k=N(cV(r,'kas','keluar'));
    mAll+=m;kAll+=k;
    if(passFlt(cV(r,'kas','user'),cV(r,'kas','date'),`${cV(r,'kas','kategori')} ${cV(r,'kas','uraian')}`)){mFil+=m;kFil+=k;}
  });

  // Cicilan (semua & terfilter)
  const cMap={},cFilMap={};
  (K.db.cicil||[]).forEach(c=>{
    if(!isUserVisible(c,'cicil'))return;
    const id=S2(cV(c,'cicil','idjual')),jml=N(cV(c,'cicil','jumlah'));
    cMap[id]=(cMap[id]||0)+jml;
    if(passFlt(cV(c,'cicil','user'),cV(c,'cicil','date'),''))cFilMap[id]=(cFilMap[id]||0)+jml;
  });

  // Penjualan
  let jAll=0,profFil=0,jFil=0,pAll=0,pFil=0;
  (K.db.penjualan||[]).forEach(r=>{
    if(!isUserVisible(r,'penjualan'))return;
    const pid=r[0],{raw,disc,cogs,items}=calcItems('penjualan',pid),
          net=Math.max(0,raw-disc-N(cV(r,'penjualan','diskon'))),paid=N(cV(r,'penjualan','bayar')),
          paidAll=paid+(cMap[S2(pid)]||0);
    jAll+=net;
    if(paidAll<net)pAll+=net-paidAll;
    const katExtra=`${cV(r,'penjualan','idkontak')} ${cV(r,'penjualan','catatan')} `+
      items.map(i=>cV(K.db.produk?.find(p=>p[0]===i.id),'produk','kategori')).join(' ');
    if(passFlt(cV(r,'penjualan','user'),cV(r,'penjualan','date'),katExtra)){
      const paidFil=paid+(cFilMap[S2(pid)]||0);
      jFil+=net;profFil+=net-cogs;
      if(paidFil<net)pFil+=net-paidFil;
    }
  });

  // Ecer
  let ecQtyFil=0;
  (K.db.ecer||[]).forEach(r=>{
    if(!isUserVisible(r,'ecer'))return;
    const q=N(cV(r,'ecer','qty')),net=Math.max(0,q*N(cV(r,'ecer','hargajual'))-N(cV(r,'ecer','diskon')));
    jAll+=net;
    if(passFlt(cV(r,'ecer','user'),cV(r,'ecer','date'),cV(r,'ecer','catatan'))){
      jFil+=net;profFil+=net-q*N(cV(r,'ecer','hargabeli'));ecQtyFil+=q;
    }
  });

  // Pembelian
  let bFil=0;
  (K.db.pembelian||[]).forEach(r=>{
    if(!isUserVisible(r,'pembelian'))return;
    if(passFlt(cV(r,'pembelian','user'),cV(r,'pembelian','date'),`${cV(r,'pembelian','idkontak')} ${cV(r,'pembelian','catatan')}`))bFil+=trxNet('pembelian',r);
  });

  // Stok
  let sStok=0,rCount=0,eMod=0,eJual=0;
  (K.db.produk||[]).forEach(r=>{
    const s=N(cV(r,'produk','stok'));
    if(!passFlt('','',`${cV(r,'produk','subkategori')} ${cV(r,'produk','kategori')}`))return;
    sStok+=s;
    if(s>0){rCount++;eMod+=s*N(cV(r,'produk','hargabeli'));eJual+=s*N(cV(r,'produk','hargajual'));}
  });
  const sumQty=sh=>(K.db[sh]||[]).reduce((a,r)=>a+(isUserVisible(r,sh)?N(cV(r,sh,'qty')):0),0);
  const stMasuk=sumQty('produk_beli'),stKeluar=sumQty('produk_jual')+ecQtyFil;

  [
    {l:'saldo total',v:fmtNum(mAll+jAll-kAll-pAll),s:'kas',k:'saldo_total'},
    {l:'saldo filter',v:fmtNum(mFil+jFil-kFil-pFil),s:'penjualan',k:'saldo_filter'},
    {l:'user',v:K.u||'admin',s:'users',k:'users'},
    {l:'profit',v:fmtNum(profFil),s:'penjualan',k:'profit'},
    {l:'kas masuk',v:fmtNum(mFil),s:'kas',k:'kas_masuk'},
    {l:'kas keluar',v:fmtNum(kFil),s:'kas',k:'kas_keluar'},
    {l:'total penjualan',v:fmtNum(jFil),s:'penjualan',k:'total_penjualan'},
    {l:'total pembelian',v:fmtNum(bFil),s:'pembelian',k:'total_pembelian'},
    {l:'total piutang',v:fmtNum(pFil),s:'penjualan',k:'total_piutang'},
    {l:'stok masuk',v:fmtNum(stMasuk),s:'produk_beli',k:'stok_masuk'},
    {l:'stok keluar',v:fmtNum(stKeluar),s:'produk_jual',k:'stok_keluar'},
    {l:'sisa stok',v:fmtNum(sStok),s:'produk',k:'sisa_stok'},
    {l:'produk ready stok',v:rCount+' item',s:'produk',slice:'ready',k:'ready'},
    {l:'estimasi modal ready',v:fmtNum(eMod),s:'produk',slice:'ready',k:'modal_ready'},
    {l:'estimasi jual ready',v:fmtNum(eJual),s:'produk',slice:'ready',k:'jual_ready'},
    {l:'estimasi profit ready',v:fmtNum(eJual-eMod),s:'produk',slice:'ready',k:'profit_ready'}
  ].forEach(c=>{
    if(hasAccess(c.k)&&hasAccess(c.s))
      dom.db.insertAdjacentHTML('beforeend',`<button data-a="nav" data-s="${esc(c.s)}" ${c.slice?`data-slice="${esc(c.slice)}"`:''}>${esc(c.l)}: <b>${esc(c.v)}</b></button>`);
  });
  renderNavFooters();
};

const renderForm=()=>{
  // Login
  if(!K.u){
    dom.fb.innerHTML='<input placeholder="user" autocomplete="off" data-f="user"><input type="password" placeholder="password" data-f="pass">';
    addFooter('login','login');addFooter('reg','register');addFooter('bypass','bypass admin');
    return;
  }

  // Detail CMS
  if(K.cur==='cms'&&K.v==='cms_detail'){
    const r=K.db.cms?.[K.idx]||[],g=k=>cV(r,'cms',k),foto=isSafeUrl(g('foto'))?g('foto'):'',file=g('file');
    dom.fb.innerHTML=`<b>${esc(g('judul')||'-')}</b>
    <small>${esc(g('date'))} ${g('user')?'| '+esc(g('user')):''} ${g('kategori')?'| '+esc(g('kategori')):''} ${g('status')?'| ['+esc(g('status'))+']':''}</small>
    <p>${esc(g('konten'))}</p>
    ${foto?`<img src="${esc(foto)}" alt="cms"><br>`:''}
    ${file?`<button data-a="dl-cms-file">unduh lampiran</button><br>`:''}`;
    addFooter('back','<');
    if(file)addFooter('dl-cms-file','unduh file');
    addFooter('edit-cms','edit');renderNavFooters();
    return;
  }

  // Form konfigurasi
  if(K.v==='bulksheet'){
    dom.fb.innerHTML=`<textarea rows="10" placeholder="sheet:kolom,kolom">${esc(K.shs.map(s=>`${s}:${(K.cols[s]||[]).join(',')}`).join('\n'))}</textarea>`;
    addFooter('back','<');addFooter('save-bulksheet','save');renderNavFooters();
    return;
  }
  if(K.v==='toko'){
    const s=getStore();
    dom.fb.innerHTML=['name','sub','phone','bank'].map(k=>`<input placeholder="${k}" value="${esc(s[k])}" data-cfg="${k}">`).join('')+
      `<textarea placeholder="footer" data-cfg="footer">${esc(s.footer)}</textarea>`;
    addFooter('back','<');addFooter('save-store','save');renderNavFooters();
    return;
  }
  if(K.v==='schema'){
    dom.fb.innerHTML=`<input placeholder="cols" value="${esc(cols().join(','))}" data-f="cols"><input placeholder="vis (list)" value="${esc(vis().join(','))}" data-f="vis"><input placeholder="form cols" value="${esc(fvis().join(','))}" data-f="form">`;
    addFooter('back','<');addFooter('save-schema','save');renderNavFooters();
    return;
  }

  // Form baris
  const c=cols(),bypassAdd=K.idx===-1&&['produk','kas'].includes(K.cur),
        formCols=bypassAdd?fvis():fvis().filter(col=>hasAccess(col,K.cur)),
        row=(K.idx!==null&&K.idx>=0)?(K.db[K.cur][K.idx]||[]):[],
        isDraft=isTrx()&&S2(cV(row,K.cur,'status'))==='draft';

  if(isTrx()&&K.idx!==null&&K.idx>=0){
    const{raw,disc,cogs,pot}=calcItems(K.cur,row[0]),net=Math.max(0,raw-disc-N(cV(row,K.cur,'diskon'))),showProf=hasAccess('profit',K.cur);
    msg(`total: ${fmtNum(net)}${showProf?` | profit: ${fmtNum(K.cur==='penjualan'?(net-cogs):(pot-net))}`:''}`);
  }else msg();

  formCols.forEach(col=>{
    let val=(K.idx===-1&&c.indexOf(col)===0)?genId():(row[c.indexOf(col)]??'');
    if(K.savedFormState&&K.savedFormState[col]!=null)val=K.savedFormState[col];
    else if(K.idx===-1&&K.prefill?.[col]!=null)val=K.prefill[col];
    else if(K.idx===-1&&!val&&/date|tanggal/i.test(col))val=today();
    else if(K.idx===-1&&!val&&col==='user')val=K.u||'admin';
    else if(K.idx===-1&&!val&&K.cur==='cms'&&col==='status')val='publish';
    else if(K.idx===-1&&!val&&K.cur==='roles'&&col==='hide')val=PRESETS.kasir.hide;
    else if(K.idx===-1&&!val&&K.cur==='roles'&&col==='validasi')val='wajibstok,wajibkontak';
    const isLong=/catatan|alamat|uraian|konten|akses|hide|validasi/i.test(col),tag=isLong?'textarea':'input',typeAttr=/date|tanggal/i.test(col)?'type="date"':'';
    dom.fb.insertAdjacentHTML('beforeend',`<${tag} ${typeAttr} placeholder="${esc(col)}" value="${esc(val)}" data-col="${esc(col)}">${isLong?esc(val):''}</${tag}>`);
    updateFormThumb(fld(col));
  });
  K.savedFormState=null;

  // Lokasi otomatis bila role mewajibkan
  if(K.idx===-1&&getRoleRules().includes('wajiblokasi')){
    const loc=fldBy(GEO_RE);
    if(loc&&!loc.value&&navigator.geolocation)
      navigator.geolocation.getCurrentPosition(
        p=>{if(!loc.value)loc.value=`${p.coords.latitude.toFixed(6)},${p.coords.longitude.toFixed(6)}`;},
        ()=>{loc.value='here()';},{timeout:4000});
  }

  addFooter('back','<');addFooter('save-row','save');
  if(isTrx())addFooter('trx-pick-kontak','kontak');
  formCols.forEach(col=>{
    const trg=resolveRef(col),val=(fld(col)?.value||'').trim();
    if(trg&&trg!==K.cur&&K.shs.includes(trg)&&(K.db[trg]||[]).length>0&&val!=='')addFooter(`ref-${trg}`,`ref ${trg}`,K.cur,`data-ref-col="${esc(col)}"`);
  });
  if(formCols.some(k=>/foto/i.test(k))){addFooter('cam','kamera');addFooter('pic','file foto');}
  if(formCols.some(k=>/file/i.test(k)))addFooter('doc-upload','upload berkas');
  if(K.cur==='produk')addFooter('gimg','gimg');
  if(formCols.some(k=>GEO_RE.test(k))){
    addFooter('gps','gps');
    if(/(-?\d+\.\d+),\s*(-?\d+\.\d+)/.test(formCols.map(col=>row[c.indexOf(col)]||'').join(' ')))addFooter('navigasi','navigasi');
  }
  if(K.cur==='produk'&&K.idx!==null&&K.idx>=0){addFooter('jual-prod','jual');addFooter('beli-prod','beli');addFooter('ecer-prod','ecer');}
  if(STOCK_SHEETS.includes(K.cur)&&row.length){
    const lk=cV(row,K.cur,'idproduk');
    if(lk){const pi=(K.db.produk||[]).findIndex(p=>S2(cV(p,'produk','idproduk')||p[0])===S2(lk));if(pi!==-1)K.prodIdx=pi;}
  }
  if(K.prodIdx!==null)addFooter('edit-prod','edit produk');
  if(isTrx()&&K.idx!==null&&K.idx>=0){
    if(!isDraft)addFooter('revisi','revisi');
    addFooter('pay-pas','pas');
    if(!isDraft)addFooter('trx-rincian',childOf(K.cur));
    if(K.cur==='penjualan')addFooter('show-nota','nota');
    addFooter('cicil','cicil');
    if(!isDraft)addFooter('print','print');
  }
  if(K.cur==='kontak'&&(K.parent||getOpenTrx('penjualan')||getOpenTrx('pembelian')))addFooter('trx-set-kontak','trx');
  if(K.cur==='roles'){addFooter('preset-kasir','role kasir');addFooter('preset-gudang','role gudang');}
  if(K.idx!==null&&K.idx>=0){
    if(!isDraft)addFooter('dup-row','dup');
    addFooter('del-row',K.conf==='del'?'sure?':'del');
  }
  renderNavFooters();
};

const renderList=isBulkEdit=>{
  msg();
  const c=cols(),vCols=vis(),rows=K.db[K.cur]||[],matched=queryRows(),
        vIdxs=c.map((col,i)=>vCols.includes(col)?i:-1).filter(i=>i!==-1),eff=vIdxs.length?vIdxs:[0],
        imgI=cI(K.cur,'foto')!==-1?cI(K.cur,'foto'):cI(K.cur,'foto_url'),
        cmsFotoI=K.cur==='cms'?cI('cms','foto'):-1,limit=Math.min(K.page*25,matched.length);

  const addThumb=(el,src,alt)=>{const im=document.createElement('img');im.src=src;im.alt=alt;im.onerror=()=>im.remove();el.appendChild(im);};

  for(let m=0;m<limit;m++){
    const trg=matched[m],r=rows[trg]||[],el=document.createElement('div');
    el.dataset.idx=trg;
    if(isBulkEdit){
      el.contentEditable='plaintext-only';if(!el.isContentEditable)el.contentEditable='true';
      el.textContent=r.join('|');
    }else{
      if(K.grid&&imgI!==-1&&isSafeUrl(r[imgI])){addThumb(el,r[imgI],'prod');el.appendChild(document.createElement('br'));}
      if(cmsFotoI!==-1&&isSafeUrl(r[cmsFotoI])){addThumb(el,r[cmsFotoI],'cms');el.appendChild(document.createTextNode(' '));}
      el.appendChild(document.createTextNode(eff.map(i=>{
        const val=r[i];
        if(!val||(K.cur==='cms'&&i===cmsFotoI))return'';
        return c[i]==='konten'?String(val).replace(/\s+/g,' ').slice(0,50)+'...':val;
      }).filter(Boolean).join(' ')||'-'));
    }
    dom.ls.appendChild(el);
  }

  if(isBulkEdit){addFooter('back','<');addFooter('save-bulk','save');}
  else{
    const pType=parentOf(K.cur),op=pType?getOpenTrx(pType):null;
    if(pType&&op){
      K.parent={type:pType,id:op.id,open:true};
      const{raw,disc}=calcItems(pType,op.id);
      addFooter('clear-items',K.conf==='clear-items'?'sure?':'clear');
      addFooter('add-cart-item','+item');
      addFooter('v-selesai',`${pType} (${fmtNum(raw-disc)})`);
      if(pType==='penjualan')addFooter('show-nota','nota');
    }
    renderNavFooters();
  }
  if(limit<matched.length)addFooter('more','more');
};

const renderNota=()=>{
  msg();dom.tape.textContent=K.receiptRaw;
  addFooter('back','<');addFooter('rawbt','rawbt');addFooter('jpg','jpg');addFooter('print','print');renderNavFooters();
};

const updateFormThumb=inp=>{
  if(!inp)return;
  const col=inp.dataset.col||'',val=(inp.value||'').trim();
  let th=dom.fb.querySelector(`img[data-thumb-col="${CSS.escape(col)}"]`);
  if(val&&(/foto/i.test(col)||isSafeUrl(val))){
    if(!th){th=document.createElement('img');th.dataset.thumbCol=col;th.alt='thumb';th.onerror=()=>th.remove();inp.insertAdjacentElement('afterend',th);}
    th.src=val;
  }else if(th)th.remove();
};

const persist=()=>{
  try{
    L.setItem('apex',JSON.stringify({cur:K.cur,u:K.u,shs:K.shs,cols:K.cols,vis:K.vis,fvis:K.fvis,parent:K.parent,grid:K.grid,df:K.df}));
    idbPut(K.db,'apex_matrix');
  }catch{}
};

const render=()=>{
  dom.act.value='';
  dom.fb.innerHTML=dom.ft.innerHTML=dom.db.innerHTML=dom.ls.innerHTML='';
  document.body.dataset.grid=K.grid?'1':'0';
  if(!K.u){
    dom.h.hidden=dom.tools.hidden=dom.db.hidden=dom.bn.hidden=dom.ls.hidden=true;
    dom.fb.hidden=false;renderForm();return;
  }
  const isF=['toko','schema','bulksheet','cms_detail'].includes(K.v)||K.idx!==null,
        m=K.v==='menu'?'menu':K.v==='dash'?'dash':K.v==='nota'?'nota':isF?'form':K.v==='edit'?'edit':'list';
  dom.h.hidden=m==='form';
  dom.tools.hidden=m!=='list'&&m!=='edit';
  dom.fb.hidden=m!=='form';
  dom.db.hidden=m!=='dash';
  dom.bn.hidden=m!=='nota';
  dom.ls.hidden=m!=='list'&&m!=='edit';
  renderChrome();
  if(m==='menu')renderMenuFooters();
  else if(m==='dash')renderDash();
  else if(m==='nota')renderNota();
  else if(m==='form')renderForm();
  else renderList(m==='edit');
};

/* ---------------- Actions ---------------- */
const loginCreds=()=>[S2($$f('user')),($$f('pass')||'').trim()];
function $$f(k){return dom.fb.querySelector(`[data-f="${k}"]`)?.value||'';}
const enter=()=>{persist();syn(true);setView('dash',null);msg();render();};

const actions={
  closemsg:()=>{K.conf=null;msg();render();},
  scan:()=>startScan(),
  'stop-scan':()=>stopScan(),

  login:()=>{
    const[u,p]=loginCreds();
    if(!u)return msg('user required');
    const f=K.db.users?.find(r=>S2(r[0])===u);
    if(f){if(f[2]&&f[2]!==p&&f[1]!==p)return msg('password salah');K.u=L.u=f[0];}
    else if(!K.db.users?.length&&u==='admin'){K.db.users=[['admin','admin','admin']];K.u=L.u='admin';}
    else return msg('user tidak terdaftar');
    enter();
  },
  reg:()=>{
    const[u,p]=loginCreds();
    if(!u)return msg('user required');
    if(K.db.users?.some(r=>S2(r[0])===u))return msg('user sudah ada');
    const row=[u,'user',p];
    (K.db.users=K.db.users||[]).push(row);
    K.u=L.u=u;persist();save(row,'users',2);setView('dash',null);msg();render();
  },
  bypass:()=>{
    K.db.users=K.db.users||[];
    if(!K.db.users.find(r=>S2(r[0])==='admin')){
      const adm=['admin','admin','admin'];
      K.db.users.unshift(adm);save(adm,'users',2);
    }
    K.u=L.u='admin';
    enter();
  },
  back:()=>{
    stopScan();
    if(K.v==='menu'){dom.q.value='';K.v='dash';return render();}
    const inForm=FORM_VIEWS.includes(K.v)||K.idx!==null;
    K.idx=K.prefill=K.prodIdx=K.conf=K.refTargetInp=K.savedFormState=null;
    if(inForm){
      while(K.hist.length){
        const prev=K.hist.pop();
        if(prev&&!FORM_VIEWS.includes(prev.v)&&prev.idx===null){
          K.cur=prev.cur||K.cur;K.v=prev.v||'list';K.parent=prev.parent||null;K.readyOnly=Boolean(prev.readyOnly);
          return render();
        }
      }
      K.v=K.cur?'list':'dash';return render();
    }
    if(K.hist.length){
      const p=K.hist.pop();
      K.cur=p.cur||K.cur;K.v=p.v||'dash';K.idx=p.idx!=null?p.idx:null;K.parent=p.parent||null;K.readyOnly=Boolean(p.readyOnly);
      return render();
    }
    if(K.v!=='dash'){K.v='dash';K.idx=null;K.readyOnly=false;render();}
  },
  menu:()=>{K.v=K.v==='menu'?'dash':'menu';dom.q.value='';render();},
  theme:()=>{const t=getTheme()==='dark'?'light':'dark';L.theme=t;applyTheme(t);render();},
  refresh:async()=>{await syn(true);render();},

  'df-today':()=>{K.df.from=K.df.to=today();persist();render();},
  'df-clear':()=>{K.df={u:'',from:'',to:'',kat:''};persist();render();},

  'v-dash':()=>{K.prefill=K.prodIdx=null;setView('dash',null);render();},
  'v-toko':()=>{setView('toko',null);render();},
  'v-kas':()=>{if(!K.shs.includes('kas'))K.shs.push('kas');K.cur='kas';K.prefill=K.prodIdx=null;setView('form',-1);render();},
  'v-shop':()=>{K.cur='produk';K.grid=true;K.prefill=K.prodIdx=null;setView('list',null);K.readyOnly=false;render();},
  'v-jual':()=>{const op=getOpenTrx('penjualan');if(op)K.parent={type:'penjualan',id:op.id,open:true};K.cur='produk_jual';K.grid=false;setView('list',null);render();},
  'v-beli':()=>{const op=getOpenTrx('pembelian');if(op)K.parent={type:'pembelian',id:op.id,open:true};K.cur='produk_beli';K.grid=false;setView('list',null);render();},
  'v-selesai':()=>{
    if(!K.parent)return;
    const fi=(K.db[K.parent.type]||[]).findIndex(r=>S2(r[0])===S2(K.parent.id));
    K.cur=K.parent.type;setView('form',fi!==-1?fi:null);render();
  },
  'clear-items':()=>{
    if(K.conf!=='clear-items'){K.conf='clear-items';render();return;}
    if(!K.parent)return;
    const{type,id}=K.parent;
    dropTrxDeps(type,id);
    dropRows(type,r=>S2(r[0])===S2(id));
    K.parent=K.conf=null;persist();syn(true);K.cur='produk';setView('list',null);render();
  },
  'add-cart-item':()=>{K.prefill=null;K.prodIdx=null;setView('form',-1);render();},
  'edit-cms':()=>{setView('form',K.idx);render();},
  'dl-cms-file':()=>{
    const r=K.db.cms?.[K.idx]||[],data=cV(r,'cms','file'),judul=cV(r,'cms','judul')||'berkas';
    if(!data)return;
    if(/^https?:\/\//i.test(data)){W.open(data,'_blank','noopener,noreferrer');return;}
    if(/^data:/i.test(data)){
      const a=document.createElement('a');a.href=data;a.download=judul.replace(/\s+/g,'_');
      document.body.appendChild(a);a.click();setTimeout(()=>a.remove(),400);return;
    }
    dl(data,`${judul}.txt`);
  },
  nav:b=>{K.cur=b.dataset.s;setView('list',null);K.page=1;K.readyOnly=b.dataset.slice==='ready';render();},
  add:()=>{if(!NO_ADD.includes(K.cur)&&hasAccess('add',K.cur)){setView('form',-1);render();}},
  'edit-prod':()=>{if(K.prodIdx!==null){K.cur='produk';const pi=K.prodIdx;K.prodIdx=K.prefill=null;setView('form',pi);render();}},
  'jual-prod':()=>prodTo('produk_jual'),
  'beli-prod':()=>prodTo('produk_beli'),
  'ecer-prod':()=>prodTo('ecer'),

  'save-bulksheet':()=>{
    const updated=[];
    (dom.fb.querySelector('textarea')?.value||'').split(/\r?\n/).filter(Boolean).forEach(l=>{
      const p=l.split(':'),n=S2(p[0]);
      if(!n||updated.includes(n))return;
      updated.push(n);K.db[n]=K.db[n]||[];
      const list=p.slice(1).join(':').split(',').map(S2).filter(Boolean);
      K.cols[n]=list.length?list:(K.cols[n]||[]);
      if(!K.vis[n]?.length)K.vis[n]=K.cols[n].slice(0,2);
    });
    if(!updated.length)return;
    K.shs.forEach(s=>{if(!updated.includes(s)){delete K.db[s];delete K.cols[s];delete K.vis[s];delete K.fvis[s];}});
    K.shs=updated;if(!K.shs.includes(K.cur))K.cur=K.shs[0];
    persist();syn(true);setView('list',null);render();
  },

  'save-row':()=>{
    if(K.writing)return;
    K.writing=true;
    try{
      const c=cols(),oldRow=(K.idx>=0&&K.db[K.cur][K.idx])?K.db[K.cur][K.idx].slice():null,
            row=oldRow?oldRow.slice(0,c.length):new Array(c.length).fill('');
      while(row.length<c.length)row.push('');
      flds().forEach(inp=>{const ci=c.indexOf(inp.dataset.col);if(ci!==-1)row[ci]=inp.value.trim();});

      // Validasi role
      const rules=getRoleRules(),valOf=k=>row[c.indexOf(k)]||'';
      const geoCol=c.find(k=>GEO_RE.test(k)),fotoCol=c.find(k=>/foto/i.test(k));
      if(rules.includes('wajiblokasi')&&geoCol&&!valOf(geoCol))return msg('lokasi kosong');
      if(rules.includes('wajibfoto')&&fotoCol){const fv=valOf(fotoCol);if(!fv||!isSafeUrl(fv))return msg('foto kosong');}
      if(rules.includes('wajibkontak')&&c.includes('idkontak')&&!valOf('idkontak'))return msg('idkontak kosong');
      if(rules.includes('wajibabsen')&&['penjualan','kas','pembelian'].includes(K.cur)&&
         !(K.db.absen||[]).some(a=>S2(cV(a,'absen','user'))===S2(K.u||'admin')&&cV(a,'absen','date')===today()))return msg('absen dulu');
      if(rules.includes('wajibstok')&&['produk_jual','ecer'].includes(K.cur)){
        const pRow=K.db.produk?.find(p=>S2(p[0])===S2(valOf('idproduk')));
        if(pRow&&N(valOf('qty'))>N(cV(pRow,'produk','stok')))return msg('stok kurang');
      }

      // Item transaksi baru -> pastikan induk ada
      const pType=parentOf(K.cur);
      if(pType&&K.idx===-1){
        if(!K.parent||K.parent.type!==pType)K.parent=getOpenTrx(pType)||{type:pType,id:genId(),open:true};
        if(!(K.db[pType]||[]).some(r=>S2(r[0])===S2(K.parent.id))){
          const pCols=cols(pType),newP=new Array(pCols.length).fill('');
          newP[0]=K.parent.id;
          pCols.forEach((col,i)=>{
            if(/date|tanggal/i.test(col))newP[i]=today();
            else if(col==='user')newP[i]=K.u||'admin';
            else if(col==='status')newP[i]='draft';
          });
          (K.db[pType]=K.db[pType]||[]).push(newP);save(newP,pType,2);
        }
        const fkI=c.indexOf(fkCol(K.cur));
        if(fkI!==-1)row[fkI]=K.parent.id;
      }
      if(!row[0])row[0]=genId();

      // Status transaksi
      if(isTrx()){
        const sI=cI(K.cur,'status');
        if(sI===-1||row[sI]!=='revisi'){
          const net=trxNet(K.cur,row),paid=paidOf(K.cur,row),kId=cV(row,K.cur,'idkontak');
          if(sI>-1)row[sI]=statusFor(net,paid,kId);
          if((row[sI]==='piutang'||(net>0&&paid<net))&&cI(K.cur,'idkontak')!==-1&&!kId)return msg('piutang butuh kontak');
        }
      }

      // Simpan
      if(K.idx===-1){(K.db[K.cur]=K.db[K.cur]||[]).push(row);syncStockDelta(K.cur,null,row);save(row,K.cur,2);}
      else if(K.idx>=0){K.db[K.cur][K.idx]=row;syncStockDelta(K.cur,oldRow,row);save(row,K.cur,1);}
      persist();
      if(pType&&K.parent?.id)syncTrx(pType);
      if(isTrx()&&row[0]){syncTrx(K.cur);const st=S2(cV(row,K.cur,'status'));if(st!=='draft'&&st!=='revisi')K.parent=null;}
      if(K.cur==='cicil'&&cV(row,'cicil','idjual'))syncTrx('penjualan');

      const justTrxId=isTrx()?row[0]:null,wasPenjualan=K.cur==='penjualan';
      K.prefill=K.conf=K.prodIdx=K.savedFormState=null;
      if(wasPenjualan&&justTrxId){K.receiptRaw=formatNota(justTrxId);setView('nota',null);}
      else if(pType){K.cur='produk';K.grid=true;setView('list',null);}
      else setView('list',null);
      render();
    }finally{K.writing=false;}
  },

  'del-row':()=>{
    if(K.conf!=='del'){K.conf='del';render();return;}
    if(K.idx==null||K.idx<0)return;
    const target=K.db[K.cur][K.idx];
    if(!target)return;
    const id=target[0],pType=parentOf(K.cur),pid=pType?(target[cI(K.cur,fkCol(K.cur))]||K.parent?.id):null,
          delCicilJual=K.cur==='cicil'?cV(target,'cicil','idjual'):null;
    dropRows(K.cur,r=>r===target);
    if(isTrx()&&id)dropTrxDeps(K.cur,id);
    if(K.cur==='kontak'&&id){
      ['penjualan','pembelian','cicil'].forEach(sh=>{
        const ki=cI(sh,'idkontak');
        if(ki!==-1)(K.db[sh]||[]).forEach(r=>{if(S2(r[ki])===S2(id)){r[ki]='';save(r,sh,1);}});
      });
    }
    persist();
    if(pType&&pid)syncTrx(pType);
    if(delCicilJual)syncTrx('penjualan');
    setView('list',null);render();
  },
  'dup-row':()=>{
    if(!(K.idx>=0))return;
    const old=(K.db[K.cur][K.idx]||[]).slice(),c=cols();
    K.prefill={};c.forEach((col,i)=>{K.prefill[col]=old[i]||'';});
    K.prefill[c[0]]=genId();
    setView('form',-1);render();
  },

  'trx-pick-kontak':()=>{
    if(!K.shs.includes('kontak'))return;
    K.savedFormState=formState();K.refTargetInp='idkontak';K.cur='kontak';
    setView('list',null);render();
  },
  'trx-set-kontak':()=>{
    const pType=K.parent?.type||'penjualan',op=getOpenTrx(pType);
    if(!op)return;
    const r=K.db.kontak?.[K.idx]||[],kId=cV(r,'kontak','idkontak')||r[0]||'';
    if(!kId)return;
    const trg=K.db[pType]?.find(x=>S2(x[0])===S2(op.id));
    if(trg&&cI(pType,'idkontak')!==-1){trg[cI(pType,'idkontak')]=kId;save(trg,pType,1);}
    persist();syncTrx(pType);
    K.cur=pType;setView('form',(K.db[pType]||[]).indexOf(trg));render();
  },
  'pay-pas':()=>{
    const r=(K.idx!==null&&K.idx>=0)?K.db[K.cur][K.idx]:null,
          pid=r?r[0]:(fld(cols()[0])?.value||''),bayar=fld('bayar');
    if(bayar&&pid){
      const dVal=N(fld('diskon')?.value||(r?cV(r,K.cur,'diskon'):0)),{raw,disc}=calcItems(K.cur,pid);
      bayar.value=String(Math.max(0,raw-disc-dVal));
    }
  },
  revisi:()=>{
    if(!(K.idx>=0))return;
    const r=K.db[K.cur][K.idx],pid=r?.[0];
    if(!pid)return;
    K.parent={type:K.cur,id:pid,open:true};
    const sI=cI(K.cur,'status');
    if(sI!==-1){r[sI]='revisi';save(r,K.cur,1);persist();}
    K.cur=childOf(K.cur);setView('list',null);render();
  },
  cicil:()=>{
    if(!(K.idx>=0))return;
    const r=K.db[K.cur][K.idx],pid=r?.[0];
    if(!pid)return;
    if(!K.shs.includes('cicil'))K.shs.push('cicil');
    const remDebt=Math.max(0,trxNet('penjualan',r)-paidOf('penjualan',r));
    K.cur='cicil';
    K.prefill={idjual:pid,idkontak:cV(r,'penjualan','idkontak'),date:today(),user:K.u||'admin',jumlah:remDebt>0?String(remDebt):''};
    setView('form',-1);render();
  },
  'trx-rincian':()=>{
    if(!(K.idx>=0))return;
    const pid=K.db[K.cur][K.idx]?.[0];
    if(pid){K.parent={type:K.cur,id:pid,open:true};K.cur=childOf(K.cur);setView('list',null);render();}
  },
  'show-nota':()=>{
    const pid=(K.cur==='penjualan'&&K.idx>=0)?K.db.penjualan[K.idx]?.[0]:(K.parent?.type==='penjualan'?K.parent.id:K.db.penjualan?.[0]?.[0]);
    if(!pid)return;
    K.receiptRaw=formatNota(pid);setView('nota',null);render();
  },
  rawbt:()=>{
    if(!K.receiptRaw)return;
    const u=encodeURIComponent(K.receiptRaw.trim()+'\r\n\r\n\r\n');
    W.location.href=/Android/i.test(navigator.userAgent)?`intent:${u}#Intent;scheme=rawbt;package=ru.a402d.rawbtprinter;end;`:`rawbt:${u}`;
  },
  jpg:()=>{
    if(!K.receiptRaw)return;
    const lines=K.receiptRaw.split(/\r?\n/),lh=18,font='13px monospace',
          mctx=document.createElement('canvas').getContext('2d');
    mctx.font=font;
    const w=Math.ceil(Math.max(0,...lines.map(l=>mctx.measureText(l).width)))||280,h=lines.length*lh+4,
          can=document.createElement('canvas');
    can.width=w;can.height=h;
    const ctx=can.getContext('2d');
    ctx.fillStyle='#fff';ctx.fillRect(0,0,w,h);ctx.fillStyle='#000';ctx.font=font;ctx.textBaseline='top';
    lines.forEach((l,i)=>ctx.fillText(l,0,i*lh+2));
    can.toBlob(blob=>{
      if(!blob)return;
      const save=()=>{
        const u=URL.createObjectURL(blob),a=document.createElement('a');
        a.href=u;a.download=`nota_${Date.now()}.png`;document.body.appendChild(a);a.click();
        setTimeout(()=>{a.remove();URL.revokeObjectURL(u);},500);
      };
      try{
        if(navigator.clipboard&&W.ClipboardItem)navigator.clipboard.write([new ClipboardItem({'image/png':blob})]).catch(save);
        else save();
      }catch{save();}
    },'image/png');
  },
  print:()=>W.print(),

  'save-store':()=>{
    const c=K.cols.toko,row=(K.db.toko?.[0]||[]).slice(0,c.length);
    while(row.length<c.length)row.push('');
    row[0]='store_1';
    dom.fb.querySelectorAll('[data-cfg]').forEach(i=>{const ci=c.indexOf(i.dataset.cfg);if(ci>-1)row[ci]=i.value.trim();});
    K.db.toko=[row];
    save(row,'toko',1);persist();syn(true);setView('list',null);render();
  },
  'reset-db':async()=>{
    if(K.conf!=='reset-db'){K.conf='reset-db';return msg('reset? klik lagi');}
    K.conf=null;
    if('caches' in W){try{await Promise.all((await caches.keys()).map(k=>caches.delete(k)));}catch{}}
    idbClearStore('kv');idbClearStore('mut');
    K.shs.forEach(s=>{K.db[s]=[];});
    L.vs='[]';persist();render();
    await syn(true);render();
  },
  'pwa-install':()=>{
    if(!K.pwaEvent)return;
    K.pwaEvent.prompt();
    K.pwaEvent.userChoice.then(()=>{K.pwaEvent=null;render();});
  },
  'save-schema':()=>{
    const list=k=>(dom.fb.querySelector(`[data-f="${k}"]`)?.value||'').split(',').map(S2).filter(Boolean),
          nc=list('cols');
    if(nc.length){
      const v=list('vis').filter(x=>nc.includes(x)),f=list('form').filter(x=>nc.includes(x));
      K.cols[K.cur]=nc;
      K.vis[K.cur]=v.length?v:nc.slice(0,2);
      K.fvis[K.cur]=f.length?f:nc.slice();
      (K.db[K.cur]||[]).forEach(r=>{if(r.length>nc.length)r.length=nc.length;while(r.length<nc.length)r.push('');});
      persist();syn(true);
    }
    setView('list',null);render();
  },
  'save-bulk':()=>{
    const n=cols().length;
    dom.ls.querySelectorAll('[data-idx]').forEach(div=>{
      const idx=parseInt(div.dataset.idx,10);
      if(isNaN(idx)||!K.db[K.cur][idx])return;
      const p=div.textContent.split('|').map(s=>s.trim());
      while(p.length<n)p.push('');
      const row=p.slice(0,n);
      K.db[K.cur][idx]=row;save(row,K.cur,1);
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
  gimg:()=>{
    const q=fld('namaproduk')?.value||fld('nama')?.value||dom.q.value||'';
    if(q)W.open('https://www.google.com/search?tbm=isch&q='+encodeURIComponent(q),'_blank','noopener,noreferrer');
  },
  navigasi:()=>{
    const loc=(fldBy(GEO_RE)?.value||'').trim();
    if(!loc)return;
    if(/Android|iPhone|iPad/i.test(navigator.userAgent))W.location.href=`geo:${loc}?q=${encodeURIComponent(loc)}`;
    else W.open(`https://www.google.com/maps/dir/?api=1&destination=${encodeURIComponent(loc)}`,'_blank','noopener,noreferrer');
  },
  gps:()=>{
    const trg=fldBy(GEO_RE);
    if(!trg||!navigator.geolocation)return;
    navigator.geolocation.getCurrentPosition(
      p=>{trg.value=`${p.coords.latitude.toFixed(6)},${p.coords.longitude.toFixed(6)}`;},
      ()=>msg('gps err'),{enableHighAccuracy:true,timeout:5000});
  },
  'sh-sel':el=>{K.cur=el.value;K.parent=K.prefill=K.prodIdx=K.savedFormState=null;setView('list',null);K.page=1;K.readyOnly=false;render();},
  'st-sel':el=>{K.sort=parseInt(el.value,10)||0;K.page=1;render();},
  'act-sel':el=>{
    const a=el.value;el.value='';
    if(!a)return;
    if(a==='edit'){setView(K.v==='edit'?'list':'edit',null);render();}
    else if(a==='bulksheet'||a==='schema'){setView(a,null);render();}
    else if(a==='tsv'){
      dl([cols().join('\t'),...(K.db[K.cur]||[]).map(r=>r.join('\t'))].join('\n'),`${K.cur}_${today()}.tsv`,'text/tab-separated-values;charset=utf-8');
    }
    else if(a==='imp')dom.fImp.click();
    else if(a==='clear'){
      if(K.conf!=='clear'){K.conf='clear';msg(`clear ${K.cur}?`);return;}
      (K.db[K.cur]||[]).forEach(r=>save(r,K.cur,0));
      K.db[K.cur]=[];K.conf=null;persist();syn(true);render();
    }
  },
  'preset-kasir':()=>applyPreset('kasir'),
  'preset-gudang':()=>applyPreset('gudang')
};

function applyPreset(name){
  const h=fld('hide'),v=fld('validasi');
  if(h)h.value=PRESETS[name].hide;
  if(v)v.value=PRESETS[name].validasi;
}

/* ---------------- Event global ---------------- */
document.addEventListener('click',e=>{
  const b=e.target.closest('[data-a]');
  if(b){
    const a=b.dataset.a;
    if(a.startsWith('ref-')){
      const cName=b.dataset.refCol||'',val=cName?(fld(cName)?.value||''):'';
      K.savedFormState=formState();K.refTargetInp=cName;K.cur=a.slice(4);
      dom.q.value=val.trim();K.page=1;setView('list',null);
      return render();
    }
    if(!['del-row','clear-items','act-sel','reset-db'].includes(a)&&K.conf){K.conf=null;msg();}
    return actions[a]?.(b,e);
  }

  const r=e.target.closest('#list>[data-idx]');
  if(r&&K.v==='list'&&K.idx===null){
    const idx=parseInt(r.dataset.idx,10);
    // Mode pilih referensi: kembalikan id ke form sebelumnya
    if(K.refTargetInp&&K.hist.length){
      const prev=K.hist.pop(),selId=K.db[K.cur]?.[idx]?.[0]||'';
      K.cur=prev.cur;K.v=prev.v;K.idx=prev.idx;K.parent=prev.parent;K.readyOnly=Boolean(prev.readyOnly);
      if(prev.savedFormState)K.savedFormState=prev.savedFormState;
      K.savedFormState=K.savedFormState||{};
      K.savedFormState[K.refTargetInp]=selId;K.refTargetInp=null;
      return render();
    }
    if(K.cur==='cms'){setView('cms_detail',idx);return render();}
    if(K.cur==='produk')return openProdForm(K.db.produk[idx]||[]);
    if(isTrx()&&K.db[K.cur]?.[idx]?.[0])K.parent={type:K.cur,id:K.db[K.cur][idx][0]};
    setView('form',idx);render();
  }
});

document.addEventListener('input',e=>{
  const t=e.target;
  if(t.id==='q'){
    if(K.v==='menu'){
      const qm=S2(t.value);
      dom.ft.querySelectorAll('button[data-a]').forEach(btn=>{btn.hidden=qm&&!btn.textContent.toLowerCase().includes(qm);});
    }else{K.page=1;render();}
    return;
  }
  if(t.dataset.df){
    const k=t.dataset.df,pos=t.selectionStart;
    K.df[k]=t.value;persist();render();
    const next=dom.db.querySelector(`[data-df="${CSS.escape(k)}"]`);
    if(next){next.focus();if(typeof pos==='number')try{next.setSelectionRange(pos,pos);}catch{}}
    return;
  }
  if(t.dataset.col){
    const col=t.dataset.col,val=(t.value||'').trim(),trg=resolveRef(col);
    updateFormThumb(t);
    if(trg){
      const btn=dom.ft.querySelector(`[data-a="ref-${CSS.escape(trg)}"]`);
      if(val===''&&btn)btn.remove();
      else if(val!==''&&!btn&&K.shs.includes(trg)&&(K.db[trg]||[]).length>0)addFooter(`ref-${trg}`,`ref ${trg}`,K.cur,`data-ref-col="${esc(col)}"`);
    }
    // Auto-isi dari produk
    if(['idproduk','namaproduk'].includes(col)&&K.cur!=='produk'){
      const q=S2(val),f=q&&K.db.produk?.find(p=>S2(cV(p,'produk','idproduk')||p[0])===q||S2(cV(p,'produk','namaproduk'))===q);
      if(f)flds().forEach(inp=>{const fi=cI('produk',inp.dataset.col);if(fi!==-1)inp.value=f[fi]||'';});
    }
  }
});

const readFile=(file,mode,cb)=>{const rd=new FileReader();rd.onload=ev=>cb(ev.target.result||'');rd[mode](file);};

function importText(text){
  const lines=text.split(/\r?\n/).filter(Boolean);
  if(!lines.length)return;
  const sep=lines[0].includes('\t')?'\t':(lines[0].includes(';')?';':','),c=cols(),
        start=lines[0].split(sep).map(s=>s.trim().toLowerCase()).every(h=>c.includes(h))?1:0;
  for(let i=start;i<lines.length;i++){
    const parts=lines[i].split(sep).map(s=>s.trim()),row=new Array(c.length).fill('');
    for(let j=0;j<Math.min(parts.length,c.length);j++)row[j]=parts[j];
    if(!row[0])row[0]=genId();
    (K.db[K.cur]=K.db[K.cur]||[]).push(row);save(row,K.cur,2);
  }
  persist();syn(true);render();
}

document.addEventListener('change',e=>{
  const t=e.target;
  if(t.dataset.a&&actions[t.dataset.a])actions[t.dataset.a](t);
  const file=t.files?.[0];
  if(!file)return;

  if(t===dom.fCam||t===dom.fImg||t===dom.fDoc){
    const isDoc=t===dom.fDoc,trgCol=isDoc?'file':'foto',
          tInp=fld(trgCol)||fldBy(new RegExp(trgCol,'i')),jInp=isDoc?fldBy(/judul|nama/i):null;
    if(jInp&&!jInp.value)jInp.value=file.name;
    readFile(file,'readAsDataURL',data=>{if(tInp){tInp.value=data;updateFormThumb(tInp);}});
    t.value='';
  }else if(t===dom.fImp){
    readFile(file,'readAsText',importText);
    t.value='';
  }
});

document.addEventListener('keydown',e=>{
  if(e.key==='Enter'&&e.target.tagName!=='TEXTAREA'&&dom.fb.contains(e.target)){
    e.preventDefault();
    if(K.writing)return;
    if(!K.u)actions.login();
    else if(['bulksheet','toko','schema'].includes(K.v))actions[`save-${K.v}`]();
    else actions['save-row']();
  }else if(e.key==='Escape'){
    if(!dom.scanBox.hidden)stopScan();else actions.back();
  }
});

W.addEventListener('beforeinstallprompt',e=>{e.preventDefault();K.pwaEvent=e;if(K.v==='menu'||K.v==='dash')render();});
if('serviceWorker' in navigator&&(W.location.protocol.startsWith('http')||W.location.hostname==='localhost'))
  W.addEventListener('load',()=>{navigator.serviceWorker.register('sw.js').catch(()=>{});});
W.addEventListener('online',()=>syn(true));

/* ---------------- Boot ---------------- */
(()=>{
  if(L.theme)applyTheme(L.theme);
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
    const matrix=await idbGet('apex_matrix');
    if(matrix&&typeof matrix==='object')Object.assign(K.db,matrix);
    render();
    syn(true);
  });
})();