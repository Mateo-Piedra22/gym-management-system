// UI global: density toggle, kiosk mode, command palette, side-sheet, sidebar
(function(){
  'use strict';

  function onReady(fn){ if(document.readyState!=='loading'){ fn(); } else { document.addEventListener('DOMContentLoaded', fn); } }

  function qs(sel, root){ return (root||document).querySelector(sel); }
  function qsa(sel, root){ return Array.prototype.slice.call((root||document).querySelectorAll(sel)); }

  function getParam(name){ try { var u = new URL(window.location.href); return u.searchParams.get(name); } catch(e){ return null; } }

  function applyKioskFromQuery(){ try { var kiosk = getParam('kiosk'); if(kiosk==='1' || kiosk==='true'){ document.body.classList.add('kiosk-mode'); } } catch(e){}
  }

  function applyDensityFromStorage(){ try { var v = localStorage.getItem('ui:density'); if(v==='compact'){ document.body.classList.add('density-compact'); } else { document.body.classList.remove('density-compact'); } } catch(e){}
  }

  function toggleDensity(){ try { var compact = document.body.classList.toggle('density-compact'); localStorage.setItem('ui:density', compact ? 'compact' : 'comfortable'); showToastSafe(compact ? 'Densidad compacta activada' : 'Densidad cómoda activada', 'info', 2000); } catch(e){}
  }

  function bindDensityToggle(){ qsa('[data-density-toggle],#densityToggle').forEach(function(el){ el.addEventListener('click', toggleDensity); }); }

  /* Toast wrapper */
  function showToastSafe(message, type, duration, actionText){ try { if(typeof window.showToast === 'function'){ return window.showToast(message, type, duration, actionText); } else { console.log('[Toast]', type||'info', message); } } catch(e){ console.log('[Toast]', message); } }

  /* Side-sheet helpers */
  function openSideSheet(id){
    try {
      var sheet = (typeof id==='string' ? qs(id.charAt(0)==='#'? id : ('#'+id)) : id);
      var backdrop = qs('.side-sheet-backdrop');
      if(!sheet) return;
      // Cerrar cualquier otro side-sheet abierto para evitar superposición
      qsa('.side-sheet.open').forEach(function(s){ if(s!==sheet){ s.classList.remove('open'); } });
      // Abrir el solicitado y asegurar el backdrop visible
      sheet.classList.add('open');
      if(backdrop) backdrop.classList.add('open');
      trapFocus(sheet);
    } catch(e){}
  }
  function closeSideSheet(id){
    try {
      var sheet = (typeof id==='string' ? qs(id.charAt(0)==='#'? id : ('#'+id)) : id);
      var backdrop = qs('.side-sheet-backdrop');
      if(!sheet) return;
      sheet.classList.remove('open');
      // Mantener backdrop si aún quedan otros sheets abiertos; si no, cerrarlo
      var remaining = qsa('.side-sheet.open');
      if(remaining.length === 0){
        if(backdrop) backdrop.classList.remove('open');
        releaseFocus();
      } else {
        if(backdrop) backdrop.classList.add('open');
        // Pasar el foco al último sheet abierto
        trapFocus(remaining[remaining.length-1]);
      }
    } catch(e){}
  }
  function bindSideSheet(){ qsa('[data-open-sheet]').forEach(function(el){ el.addEventListener('click', function(){ openSideSheet(el.getAttribute('data-open-sheet')); }); }); qsa('[data-close-sheet]').forEach(function(el){ el.addEventListener('click', function(){ closeSideSheet(el.getAttribute('data-close-sheet')); }); }); var backdrop = qs('.side-sheet-backdrop'); if(backdrop){ backdrop.addEventListener('click', function(){ qsa('.side-sheet.open').forEach(function(s){ s.classList.remove('open'); }); backdrop.classList.remove('open'); releaseFocus(); }); }
    // Cierre con ESC: cierra el último side-sheet abierto
    document.addEventListener('keydown', function(ev){
      if(ev.key==='Escape'){
        try {
          var openSheets = qsa('.side-sheet.open');
          if(openSheets.length>0){ closeSideSheet(openSheets[openSheets.length-1]); }
        } catch(e){}
      }
    });
  }

  /* Command palette (Ctrl+K) */
  function openCmdk(){ try { var b = qs('.cmdk-backdrop'); var c = qs('.cmdk'); if(b) b.classList.add('open'); if(c){ c.classList.add('open'); var input = qs('.cmdk-input', c); setTimeout(function(){ try { if(input) input.focus(); trapFocus(c); } catch(e){} }, 10); } } catch(e){} }
  function closeCmdk(){ try { var b = qs('.cmdk-backdrop'); var c = qs('.cmdk'); if(b) b.classList.remove('open'); if(c) c.classList.remove('open'); releaseFocus(); } catch(e){} }
  function bindCmdk(){ document.addEventListener('keydown', function(ev){ var isMac = navigator.platform.toUpperCase().indexOf('MAC')>=0; var ctrlOrCmd = (isMac ? ev.metaKey : ev.ctrlKey); if(ctrlOrCmd && ev.key.toLowerCase()==='k'){ ev.preventDefault(); openCmdk(); } else if(ev.key==='Escape'){ closeCmdk(); } }); var backdrop = qs('.cmdk-backdrop'); if(backdrop){ backdrop.addEventListener('click', closeCmdk); }
    qsa('[data-cmdk-open]').forEach(function(el){ el.addEventListener('click', function(){ openCmdk(); }); }); qsa('[data-cmdk-close]').forEach(function(el){ el.addEventListener('click', function(){ closeCmdk(); }); }); }

  /* Sidebar collapse */
  function applySidebarFromStorage(){ try { var v = localStorage.getItem('ui:sidebar'); if(v==='collapsed'){ document.body.classList.add('sidebar-collapsed'); } else { document.body.classList.remove('sidebar-collapsed'); } } catch(e){}
    // reflejar estado en toggles
    qsa('[data-sidebar-toggle]').forEach(function(el){ el.setAttribute('aria-expanded', document.body.classList.contains('sidebar-collapsed') ? 'false' : 'true'); });
  }
  function toggleSidebar(){
    var collapsed = document.body.classList.toggle('sidebar-collapsed');
    try { localStorage.setItem('ui:sidebar', collapsed ? 'collapsed' : 'expanded'); } catch(e){}
    qsa('[data-sidebar-toggle]').forEach(function(el){ el.setAttribute('aria-expanded', collapsed ? 'false' : 'true'); });
    // Seguridad: cerrar overlays que puedan cubrir la UI
    try {
      // Side-sheet
      qsa('.side-sheet.open').forEach(function(s){ s.classList.remove('open'); });
      var backdrop = qs('.side-sheet-backdrop'); if(backdrop) backdrop.classList.remove('open');
      // Command palette
      var cmdkBackdrop = qs('.cmdk-backdrop'); if(cmdkBackdrop) cmdkBackdrop.classList.remove('open');
      var cmdk = qs('.cmdk'); if(cmdk) cmdk.classList.remove('open');
      // Modales básicos con .modal-backdrop
      qsa('.modal-backdrop.active').forEach(function(el){
        var locked = (el.getAttribute('data-locked') === '1');
        if(locked) return;
        el.classList.remove('active');
        try { el.style.display = 'none'; el.setAttribute('aria-hidden','true'); } catch(e){}
      });
      // Liberar focus trap si quedó activo
      try { releaseFocus(); } catch(e){}
    } catch(e){}
  }
  function bindSidebarToggle(){ qsa('[data-sidebar-toggle]').forEach(function(el){ el.addEventListener('click', toggleSidebar); }); }

  /* Focus trap for modals/sheets */
  var focusTrapEl = null; var focusables = [];
  function trapFocus(root){ try { focusTrapEl = root; focusables = qsa('a[href], button:not([disabled]), textarea, input, select, [tabindex]:not([tabindex="-1"])', root).filter(function(el){ return el.offsetWidth>0 || el.offsetHeight>0 || el === document.activeElement; }); var first = focusables[0]; var last = focusables[focusables.length-1]; root.addEventListener('keydown', function(e){ if(e.key==='Tab'){ if(focusables.length===0){ e.preventDefault(); return; } if(e.shiftKey){ if(document.activeElement===first){ e.preventDefault(); last.focus(); } } else { if(document.activeElement===last){ e.preventDefault(); first.focus(); } } } }); } catch(e){} }
  function releaseFocus(){ focusTrapEl = null; focusables = []; }

  /* Breadcrumbs helper */
  function setBreadcrumbs(items){ try { var bc = qs('.breadcrumbs'); if(!bc) return; bc.innerHTML=''; items = Array.isArray(items)? items : []; items.forEach(function(it, idx){ var a = document.createElement('a'); a.href = it.href || '#'; a.textContent = it.label || ('Item '+(idx+1)); bc.appendChild(a); if(idx < items.length-1){ var sep = document.createElement('span'); sep.className = 'sep'; sep.textContent = '/'; bc.appendChild(sep); } }); } catch(e){} }

  function initUI(){ applyKioskFromQuery(); applyDensityFromStorage(); bindDensityToggle(); bindCmdk(); bindSideSheet(); applySidebarFromStorage(); bindSidebarToggle(); }

  onReady(initUI);

  // Expose helpers
  try { window.UI = { toggleDensity: toggleDensity, openSideSheet: openSideSheet, closeSideSheet: closeSideSheet, setBreadcrumbs: setBreadcrumbs, openCmdk: openCmdk, closeCmdk: closeCmdk, showToast: showToastSafe }; } catch(e){}

  function showMaintenanceModal(data){
    try {
      var msg = String((data&&data.message)||'');
      var until = (data&&data.until)||null;
      var root = document.createElement('div');
      root.className = 'modal-backdrop active';
      root.setAttribute('data-locked','0');
      root.style.position='fixed';
      root.style.inset='0';
      root.style.background='rgba(0,0,0,0.6)';
      root.style.zIndex='3000';
      var box = document.createElement('div');
      box.className = 'box';
      box.style.maxWidth='640px';
      box.style.margin='0 auto';
      box.style.background='rgba(17,24,39,0.9)';
      box.style.border='1px solid var(--border)';
      box.style.borderRadius='16px';
      box.style.padding='20px';
      box.style.color='var(--text)';
      box.style.transform='translateY(20vh)';
      var h = document.createElement('div');
      h.textContent = 'Mantenimiento programado';
      h.style.fontWeight='600';
      h.style.fontSize='18px';
      var p = document.createElement('div');
      p.textContent = msg || '';
      p.style.marginTop='8px';
      var meta = document.createElement('div');
      meta.style.marginTop='6px';
      meta.style.fontSize='12px';
      meta.style.color='var(--muted)';
      if(until){ meta.textContent = 'Hasta: '+ String(until); }
      var actions = document.createElement('div');
      actions.style.marginTop='12px';
      actions.style.display='flex';
      actions.style.justifyContent='flex-end';
      var ok = document.createElement('button');
      ok.className = 'btn';
      ok.textContent = 'Entendido';
      ok.addEventListener('click', function(){ try { root.classList.remove('active'); root.style.display='none'; root.setAttribute('aria-hidden','true'); } catch(e){} });
      actions.appendChild(ok);
      box.appendChild(h); box.appendChild(p); box.appendChild(meta); box.appendChild(actions);
      root.appendChild(box);
      document.body.appendChild(root);
    } catch(e){}
  }

  function initMaintenanceNotice(){
    try {
      fetch('/api/maintenance_status', { credentials: 'same-origin' })
        .then(function(r){ return r.json(); })
        .then(function(j){
          try {
            var active = !!(j && j.active);
            var activeNow = !!(j && j.active_now);
            var until = j && j.until;
            var sched = false;
            if(active && !activeNow){
              if(until){
                try { var dt = new Date(String(until)); sched = (dt.getTime() > Date.now()); } catch(e){ sched = false; }
              } else { sched = true; }
            }
            if(sched){ showMaintenanceModal(j); }
          } catch(e){}
        })
        .catch(function(){ });
    } catch(e){}
  }

  onReady(initMaintenanceNotice);
})();
