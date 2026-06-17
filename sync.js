/* sync.js — блочный realtime-синхрон «Колибри» через Supabase.
   Изолированный модуль (<800 строк). Подключается одной строкой в index.html:
   <script src="sync.js"></script>
   Документ должен предоставить:
     window.kolibriApplyState()         — переприменить ВСЁ состояние из localStorage к DOM (первая загрузка)
     window.kolibriApplyKey(key, skip)  — переприменить ОДИН ключ (realtime); skip=true => не трогать активно редактируемый блок
   Ключи синхронизации: kolibri-edit-*, kolibri-edit2-*, kolibri-notes-*, kolibri-scene-refs-v1
*/
(function () {
  var URL = 'https://dywxhzqnpprzserdlffu.supabase.co';
  var KEY = 'sb_publishable_Z4HfKdR3q0qUvYHQUP848w_mVgon6VQ';
  var TABLE = 'kolibri_state';
  var PREFIX = 'kolibri-';
  var origSet = localStorage.setItem.bind(localStorage);
  var origRemove = localStorage.removeItem.bind(localStorage);

  function loadLib(cb) {
    if (window.supabase && window.supabase.createClient) return cb();
    var s = document.createElement('script');
    s.src = 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2';
    s.onload = cb;
    s.onerror = function () { console.error('[sync] supabase-js не загрузился'); status('offline'); };
    document.head.appendChild(s);
  }

  function status(s) { document.documentElement.setAttribute('data-sync', s); }

  loadLib(init);

  function init() {
    var sb = window.supabase.createClient(URL, KEY);
    window.__sb = sb;

    // --- ПУШ локальных правок в облако (debounce) ---
    var pending = {}, timer = null;
    var ownLast = {}; // что мы сами записали — чтобы не применять своё эхо из realtime
    function queue(k, v) { pending[k] = v; clearTimeout(timer); timer = setTimeout(flush, 400); }
    async function flush() {
      var keys = Object.keys(pending); if (!keys.length) return;
      var dels = keys.filter(function (k) { return pending[k] === null; });
      var ups = keys.filter(function (k) { return pending[k] !== null; })
        .map(function (k) { return { key: k, value: pending[k], updated_at: new Date().toISOString() }; });
      keys.forEach(function (k) { delete pending[k]; });
      ups.forEach(function (u) { ownLast[u.key] = u.value; });
      dels.forEach(function (k) { ownLast[k] = null; });
      try {
        if (ups.length) { var r1 = await sb.from(TABLE).upsert(ups); if (r1.error) console.error('[sync] upsert', r1.error); }
        if (dels.length) { var r2 = await sb.from(TABLE).delete().in('key', dels); if (r2.error) console.error('[sync] delete', r2.error); }
      } catch (e) { console.error('[sync] flush', e); }
    }

    // --- перехват записей документа в localStorage по нашим ключам ---
    localStorage.setItem = function (k, v) { origSet(k, v); if (k && k.indexOf(PREFIX) === 0) queue(k, v); };
    localStorage.removeItem = function (k) { origRemove(k); if (k && k.indexOf(PREFIX) === 0) queue(k, null); };

    // --- ПЕРВАЯ ЗАГРУЗКА: тянем общее состояние, мигрируем локальные правки ---
    (async function () {
      try {
        var res = await sb.from(TABLE).select('key,value');
        if (res.error) { console.error('[sync] pull', res.error); status('offline'); return; }
        var remote = {};
        (res.data || []).forEach(function (row) { remote[row.key] = true; origSet(row.key, row.value); });
        // локальные ключи, которых нет в облаке — это текущие правки юзера, поднимаем наверх
        var migrate = [];
        for (var i = 0; i < localStorage.length; i++) {
          var k = localStorage.key(i);
          if (k && k.indexOf(PREFIX) === 0 && !remote[k]) migrate.push(k);
        }
        if (window.kolibriApplyState) window.kolibriApplyState();
        migrate.forEach(function (k) { queue(k, localStorage.getItem(k)); });
        status('online');
      } catch (e) { console.error('[sync] init', e); status('offline'); }
    })();

    // --- REALTIME: чужие правки прилетают и применяются точечно ---
    sb.channel('kolibri-rt')
      .on('postgres_changes', { event: '*', schema: 'public', table: TABLE }, function (p) {
        var row = p.new || p.old; if (!row || !row.key) return;
        var val = p.eventType === 'DELETE' ? null : row.value;
        // своё эхо — обновим localStorage, но НЕ перерисовываем DOM (иначе сбиваем своё же редактирование)
        if (ownLast.hasOwnProperty(row.key) && ownLast[row.key] === val) {
          delete ownLast[row.key];
          if (val === null) origRemove(row.key); else origSet(row.key, val);
          return;
        }
        if (val === null) origRemove(row.key); else origSet(row.key, val);
        if (window.kolibriApplyKey) window.kolibriApplyKey(row.key, true);
      })
      .subscribe(function (st) { if (st === 'SUBSCRIBED') status('online'); });
  }
})();
