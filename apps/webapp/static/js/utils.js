// Utilidades comunes de formato numérico y monetario
// Centraliza fmtCurrency y fmtAmount para toda la webapp
// Locale: es-AR, Moneda: ARS

;(function(){
  function _fmtAmount(n){ try { return Number(n||0).toLocaleString('es-AR', { minimumFractionDigits:2, maximumFractionDigits:2 }); } catch(e){ return (Number(n)||0).toFixed(2); } }
  function _fmtCurrency(n){ try { return new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS' }).format(Number(n||0)); } catch(e){ return `$${(Number(n)||0).toFixed(2)}`; } }
  if(typeof window !== 'undefined'){ window.fmtAmount = _fmtAmount; window.fmtCurrency = _fmtCurrency; }
})();

;(function(){
  if(typeof window === 'undefined') return;
  try{
    if(!window.__orig_fetch__){ window.__orig_fetch__ = window.fetch.bind(window); }
    var host = String(window.location.hostname||'').toLowerCase();
    var parts = host.split('.');
    var isSub = (parts.length >= 3 && parts[0] && parts[0] !== 'www');
    var tenantId = isSub ? parts[0] : '';
    window.__TENANT_ID__ = tenantId;
    window.fetch = function(url, init){
      var u0 = String(url||'');
      var u = u0;
      var opts = Object.assign({}, init||{});
      var hdrs = Object.assign({}, opts.headers||{});
      if(u.startsWith('/api/')){
        if(tenantId){
          if(!u.startsWith('/api/'+tenantId+'/')){ u = '/api/'+tenantId + (u.startsWith('/api/') ? u.substring(4) : u); }
          hdrs['X-Tenant-ID'] = tenantId;
        }
      }
      opts.headers = hdrs;
      return window.__orig_fetch__(u, opts);
    };
  }catch(e){ }
})();
