// Utilidades comunes de formato numérico y monetario
// Centraliza fmtCurrency y fmtAmount para toda la webapp
// Locale: es-AR, Moneda: ARS

;(function(){
  function _fmtAmount(n){
    try { return Number(n||0).toLocaleString('es-AR', { minimumFractionDigits:2, maximumFractionDigits:2 }); }
    catch(e){ return (Number(n)||0).toFixed(2); }
  }
  function _fmtCurrency(n){
    try { return new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS' }).format(Number(n||0)); }
    catch(e){ return `$${(Number(n)||0).toFixed(2)}`; }
  }
  // Exponer en window
  if(typeof window !== 'undefined'){
    window.fmtAmount = _fmtAmount;
    window.fmtCurrency = _fmtCurrency;
  }
})();
