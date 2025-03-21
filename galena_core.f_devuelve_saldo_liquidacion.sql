PROMPT CREATE OR REPLACE FUNCTION galena_core.f_devuelve_saldo_liquidacion
CREATE OR REPLACE FUNCTION galena_core.f_devuelve_saldo_liquidacion(p_id_liquidacion IN NUMBER)
RETURN NUMBER
IS
v_id_transaccion NUMBER(20);
v_monto_pago_fc NUMBER(22,4);
v_monto_total_fc NUMBER(22,4) :=0 ;
v_monto_trans NUMBER(22,4);
v_monto_total NUMBER(22,4);
v_saldo  NUMBER(22,4);
v_total_gastos dpa_liquidacion_despacho.total_gastos%TYPE;
v_cotizacion NUMBER(22,4);
v_moneda_trans NUMBER(20);
v_monto_trans_acumulado NUMBER(22,4) := 0 ;
v_saldo_negativo NUMBER(24,4);
BEGIN

  SELECT Nvl(cotiz_comercial, 1)
    INTO v_cotizacion
    FROM galena_core.dpa_liquidacion_despacho dpa
   WHERE id_liquidacion = p_id_liquidacion;

    select Nvl(total_gastos,0)-- -Nvl (entrega,0) -- se comenta porque el campo entrega es referencial
      into v_total_gastos
      from dpa_liquidacion_despacho
     where id_liquidacion = p_id_liquidacion;


  DECLARE
    CURSOR fc IS
        select distinct t.id_transaccion
          from dpa_det_liquidacion_desp dlp,
               factura_venta fv,
               det_pag_cli dp,
               pag_cli pg,
               transaccion t
         where dlp.fact_ven_id_factura_venta = fv.id_factura_venta
           and fv.id_factura_venta = dp.fac_vent_id_factura_venta
           and dp.pag_id_pago = pg.id_pago
           and pg.tran_id_transaccion = t.id_transaccion
           and t.estado_transaccion != 'ANULADO'
           and liq_id_liquidacion_desp = p_id_liquidacion ;
    begin
      open fc;
      fetch fc into v_id_transaccion;
       while fc%found
         /*loop
            BEGIN
             select case when t.mon_id_moneda = 1 then t.monto_trans ELSE Round (galena_core.obtiene_cotizacion_hacienda(t.mon_id_moneda,'VENTA',t.fecha) * t.monto_trans) end monto
               into v_monto_pago_fc
               from transaccion t
              where t.id_transaccion = v_id_transaccion
              and t.estado_transaccion != 'ANULADO';
            exception
              when no_data_found then
                v_monto_pago_fc := 0;
            end;

            v_monto_total_fc := v_monto_total_fc + v_monto_pago_fc;
         fetch fc into v_id_transaccion ;
        exit when fc%notfound;
      end loop;*/

       --Ticket GC9A
       loop
            BEGIN
             select case when t.mon_id_moneda = 1 then td.monto ELSE Round (galena_core.obtiene_cotizacion_hacienda(t.mon_id_moneda,'VENTA',t.fecha) * td.monto) end monto
               into v_monto_pago_fc
               from transaccion t,
                    dpa_liquidacion_transaccion_detalle td
              where t.id_transaccion = v_id_transaccion
              and td.trans_id_transaccion = v_id_transaccion
              and t.estado_transaccion != 'ANULADO';
            exception
              when no_data_found then
                v_monto_pago_fc := 0;
            end;

            v_monto_total_fc := v_monto_total_fc + v_monto_pago_fc;
         fetch fc into v_id_transaccion ;
        exit when fc%notfound;
      end loop;
      --

      close fc;
    END;

    DECLARE
      CURSOR tr IS
        select dt.trans_id_transaccion id_transaccion
          from dpa_liquidacion_transaccion l ,
               dpa_liquidacion_transaccion_detalle dt,
               transaccion t
        where l.id_liq_trans = dt.liq_tran_id_liq_trans
          and dt.trans_id_transaccion = t.id_transaccion
          and t.estado_transaccion != 'ANULADO'
          and l.liq_id_liquidacion = p_id_liquidacion;
      BEGIN
        FOR rec IN tr
          /*LOOP
                select --(case when t.mon_id_moneda = 1 then sum(dt.monto)
                       --         else Round(galena_core.obtiene_cotizacion_hacienda(t.mon_id_moneda,'VENTA',t.fecha) * Sum(dt.monto))
                       --    END) monto,
                       Sum(dt.monto),
                       t.mon_id_moneda,
                       galena_core.obtiene_cotizacion_hacienda(t.mon_id_moneda,'VENTA',t.fecha) cotizacion
                  into v_monto_trans, v_moneda_trans, v_cotizacion
                  from det_transaccion dt,
                       transaccion t
                where dt.tran_id_transaccion = t.id_transaccion
                  AND t.id_transaccion = rec.id_transaccion
                GROUP BY t.mon_id_moneda, t.fecha;

                IF v_moneda_trans != 1 THEN
                  IF Round(v_total_gastos / v_cotizacion,4) = v_monto_trans THEN
                    v_monto_trans := v_total_gastos;
                  ELSE
                    v_monto_trans := Round((v_monto_trans * v_cotizacion));
                  END IF;
                ELSE
                  v_monto_trans := v_monto_trans * v_cotizacion;
                END IF;
                v_monto_trans_acumulado := v_monto_trans_acumulado + v_monto_trans;
                Dbms_Output.Put_Line('Gastos: '||v_total_gastos||' acumulado: '||v_monto_trans_acumulado||' monto: '||v_monto_trans);
          END LOOP; */

          --Ticket GC9A
          LOOP
                select --(case when t.mon_id_moneda = 1 then sum(dt.monto)
                       --         else Round(galena_core.obtiene_cotizacion_hacienda(t.mon_id_moneda,'VENTA',t.fecha) * Sum(dt.monto))
                       --    END) monto,
                       Sum(dt.monto),
                       t.mon_id_moneda,
                       galena_core.obtiene_cotizacion_hacienda(t.mon_id_moneda,'VENTA',t.fecha) cotizacion
                  into v_monto_trans, v_moneda_trans, v_cotizacion
                  from galena_core.dpa_liquidacion_transaccion_detalle dt,
                       galena_core.transaccion t,
                       galena_Core.dpa_liquidacion_transaccion dlt
                where dt.trans_id_transaccion = t.id_transaccion
                  AND t.id_transaccion = rec.id_transaccion
                  AND dlt.id_liq_trans = dt.liq_tran_id_liq_trans
                  AND dlt.liq_id_liquidacion = p_id_liquidacion
                GROUP BY t.mon_id_moneda, t.fecha;

                IF v_moneda_trans != 1 THEN
                  IF Round(v_total_gastos / v_cotizacion,4) = v_monto_trans THEN
                    v_monto_trans := v_total_gastos;
                  ELSE
                    v_monto_trans := Round((v_monto_trans * v_cotizacion));
                  END IF;
                ELSE
                  v_monto_trans := v_monto_trans * v_cotizacion;
                END IF;
                v_monto_trans_acumulado := v_monto_trans_acumulado + v_monto_trans;
                Dbms_Output.Put_Line('Gastos: '||v_total_gastos||' acumulado: '||v_monto_trans_acumulado||' monto: '||v_monto_trans);
          END LOOP;
          --

      END;


    v_monto_total:= Nvl(v_monto_total_fc,0) + Nvl(v_monto_trans_acumulado,0);

    v_saldo :=  v_total_gastos - v_monto_total;

    BEGIN   -- Fabricio Franco
      SELECT saldo
      INTO v_saldo_negativo
      FROM  dpa_liquidacion_Saldo   -- tabla para los saldos negativos
      WHERE liq_id_liquidacion_original = p_id_liquidacion;
    EXCEPTION WHEN No_Data_Found THEN
      v_saldo_negativo := 0;
    END;

    IF v_saldo < 0  THEN -- Si el saldo queda como negativo se sumar? para que la liquidacion quede como pagada (saldo 0)
      v_saldo := v_saldo + v_saldo_negativo;
    END IF;


   RETURN v_saldo;
END ;
/

