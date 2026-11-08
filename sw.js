const C='apex-v4',FILES=['./','index.html','style.css','config.js','app.js','manifest.json','icon.svg'];

self.addEventListener('install',e=>{
  e.waitUntil(caches.open(C).then(c=>c.addAll(FILES)).catch(()=>{}));
  self.skipWaiting();
});

self.addEventListener('activate',e=>{
  e.waitUntil(
    caches.keys().then(ks=>Promise.all(ks.map(k=>k!==C&&caches.delete(k)))).then(()=>clients.claim())
  );
});

self.addEventListener('fetch',e=>{
  if(e.request.method!=='GET')return;
  e.respondWith(
    caches.match(e.request,{ignoreSearch:true}).then(r=>{
      return r||fetch(e.request).then(res=>{
        if(res.status===200){
          const cl=res.clone();
          caches.open(C).then(c=>c.put(e.request,cl));
        }
        return res;
      });
    }).catch(()=>caches.match('./')||caches.match('index.html'))
  );
});
