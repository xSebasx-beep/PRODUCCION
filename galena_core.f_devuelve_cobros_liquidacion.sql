prompt create or replace function galena_core.f_devuelve_cobros_liquidacion
create or replace function galena_core.f_devuelve_cobros_liquidacion(
  p_id_liquidacion in number
)
return number
is
  v_id_transaccion  number(20);
  v_monto_pago_fc   number(20,2);
  v_monto_total_fc  number(20,2):=0 ;
  v_monto_trans     number(20,2) ;
  v_monto_total     number(20,2);
  v_cotizacion      number(20,2);
  v_monto_transaccion NUMBER(20,2);
begin 
  select 
    nvl(cotiz_comercial, 1)
  into 
    v_cotizacion
  from galena_core.dpa_liquidacion_despacho dpa
  where id_liquidacion = p_id_liquidacion;
  for fc in (
    select distinct 
      t.id_transaccion
    from  
      dpa_det_liquidacion_desp dlp,
      factura_venta fv,
      det_pag_cli dp,
      pag_cli pg,
      transaccion t
    where dlp.fact_ven_id_factura_venta = fv.id_factura_venta
      and fv.id_factura_venta = dp.fac_vent_id_factura_venta
      and dp.pag_id_pago = pg.id_pago
      and pg.tran_id_transaccion = t.id_transaccion
      and t.estado_transaccion != 'ANULADO'
      and liq_id_liquidacion_desp = p_id_liquidacion
  )loop
    begin 
      select 
        case when t.mon_id_moneda = 1 then t.monto_trans 
          else obtiene_cotizacion_hacienda(t.mon_id_moneda,'COMPRA',t.fecha) * t.monto_trans 
        end monto
        --select  t.monto_trans -- monto
        into 
          v_monto_pago_fc
        from transaccion t
        where t.id_transaccion = fc.id_transaccion
          and t.estado_transaccion != 'ANULADO';
    exception
      when no_data_found then
        v_monto_pago_fc := 0;
    end;
    v_monto_total_fc := Nvl(v_monto_total_fc,0) + Nvl(v_monto_pago_fc,0);
  end loop; 
  begin
    select 
      monto_trans
      --sum(case when t.mon_id_moneda = 1 then sum(dt.monto) 
      --  else obtiene_cotizacion_hacienda(t.mon_id_moneda,'VENTA',t.fecha) * sum(dt.monto) 
      --end) monto
      --select sum(dt.monto) --monto
    into 
      v_monto_trans
    from  
      dpa_liquidacion_transaccion l ,
      dpa_liquidacion_transaccion_detalle dt,
      transaccion t
    where l.id_liq_trans = dt.liq_tran_id_liq_trans
      and dt.trans_id_transaccion = t.id_transaccion
      and t.estado_transaccion != 'ANULADO'
      and l.liq_id_liquidacion = p_id_liquidacion ;
      --group by t.mon_id_moneda,t.fecha;  
  exception
    when no_data_found then
      v_monto_trans := 0;
  end;
    v_monto_total:= Nvl(v_monto_total_fc,0) + Nvl(v_monto_trans,0);
  RETURN v_monto_total;
END ;
/
