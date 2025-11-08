(function(){
  'use strict';

  function ensureToastContainer(){
    var cont = document.getElementById('toast-container');
    if (!cont) {
      cont = document.createElement('div');
      cont.id = 'toast-container';
      cont.className = 'toast-container';
      document.body.appendChild(cont);
    }
    return cont;
  }

  function dismissToast(el){
    if(!el) return;
    try {
      el.style.animation = 'toast-out 160ms ease-in forwards';
    } catch(e) {}
    setTimeout(function(){
      try { el.remove(); } catch(e) {}
    }, 160);
  }

  function showToast(message, type, duration, actionText){
    if (typeof message !== 'string') {
      try { message = String(message); } catch(e) { message = 'Mensaje'; }
    }
    // Normalizar mensaje para mayor claridad
    try {
      message = message.trim();
      if (message.length > 0) {
        var first = message.charAt(0).toUpperCase();
        message = first + message.slice(1);
        if (!/[\.\!\?]$/.test(message)) message += '.';
      }
    } catch(e) {}
    type = type || 'info';
    duration = (typeof duration === 'number' ? duration : 3500);
    actionText = actionText || 'Cerrar';

    var cont = ensureToastContainer();
    var t = document.createElement('div');
    t.className = 'toast ' + (type || 'info');
    try { t.style.animation = 'toast-in 200ms ease-out'; } catch(e) {}
    try { t.setAttribute('role', 'alert'); t.setAttribute('aria-live', type === 'error' ? 'assertive' : 'polite'); } catch(e) {}

    var span = document.createElement('span');
    span.className = 'message';
    span.textContent = message;

    var actions = document.createElement('div');
    actions.className = 'actions';

    var btn = document.createElement('button');
    btn.className = 'btn';
    btn.textContent = actionText;
    btn.addEventListener('click', function(){ dismissToast(t); });

    actions.appendChild(btn);
    t.appendChild(span);
    t.appendChild(actions);
    cont.appendChild(t);

    var timer = null;
    if (duration && duration > 0) {
      timer = setTimeout(function(){ dismissToast(t); }, duration);
    }
    t.addEventListener('mouseenter', function(){ if (timer) { clearTimeout(timer); timer = null; } });
    t.addEventListener('mouseleave', function(){ if (!timer && duration && duration > 0) { timer = setTimeout(function(){ dismissToast(t); }, duration); } });

    return t;
  }

  // Exponer globalmente
  try { window.dismissToast = dismissToast; } catch(e) {}
  try { window.showToast = showToast; } catch(e) {}
})();