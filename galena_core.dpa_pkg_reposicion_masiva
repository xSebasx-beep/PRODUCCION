PROMPT CREATE OR REPLACE PACKAGE galena_core.dpa_pkg_reposicion_masiva
CREATE OR REPLACE package galena_core.dpa_pkg_reposicion_masiva is
  /**
  ** PROCEDURES
  **/
  procedure pr_carga_auxiliar_liquidacion(
    pi_id_cliente          in number,
    pi_fecha_desde         in date,
    pi_fecha_hasta         in date
  );

  procedure pr_genera_reposicion_liquidacion(
    pi_banco            in  number    ,
    pi_observacion      in  varchar2  ,
    pi_moneda           in  number    ,
    pi_fecha            in  date      ,
    pi_permiso_per      in  number    ,
    pi_cliente          in  number    ,
    pi_ruc              in  varchar2  ,
    pi_tipo_trans       in  number    ,
    pi_concepto         in  number    ,
    pi_numero           in  number    ,
    pi_id_liquidacion   in  number    ,
    po_id_liq_trans     out number    ,
    po_id_transaccion   out number    ,
    pi_monto            in  number
  );

  PROCEDURE pr_elimina_no_procesado;

end dpa_pkg_reposicion_masiva;
/

PROMPT CREATE OR REPLACE PACKAGE BODY galena_core.dpa_pkg_reposicion_masiva
CREATE OR REPLACE package body galena_core.dpa_pkg_reposicion_masiva is

  /**
  ** PROCEDURES
  **/
  procedure pr_carga_auxiliar_liquidacion(
    pi_id_cliente          in number,
    pi_fecha_desde         in date,
    pi_fecha_hasta         in date
  ) IS
  -------------------------------------------------------
    -- variables
  -------------------------------------------------------
    v_dummy          char(2);
    v_razon_social   cliente.razon_social%type;
  -------------------------------------------------------
  begin

  /* Consultamos si existen registros en la tabla auxiliar para el usuario en cuestión */
    begin
      select 1
      into v_dummy
      from dpa_aux_reposicion_liquidacion
      where usuario = v('APP_USER')
        and rownum < 2 ;
    exception
      when no_data_found then
        null;
    end;

  /* Si existe algún registro, procedemos a limpiar la tabla */
      if v_dummy = 1 then
        delete dpa_aux_reposicion_liquidacion
        where usuario = v('APP_USER');
        commit;
      end if;


  /* Consultamos la razon social del cliente. Esto para imprimir el nombre y apellido del cliente en caso de error. */
    begin
      select nvl(cl.razon_social, cl.nombre||' '||cl.apellido)
      into  v_razon_social
      from cliente cl
      where id_cliente = pi_id_cliente;
    exception
      when no_data_found then
        Raise_Application_Error(-20999,'Error al recuperar la razon social del cliente con id: '||pi_id_cliente);
    end;

  /* Procedemos a insertar los registros recuperados del select, en nuestra tabla auxiliar. */
    begin
      insert into galena_core.dpa_aux_reposicion_liquidacion
        select ld.id_liquidacion,
               ld.mon_id_moneda,
               ld.total_gastos_dolar,
               ld.total_gastos,
               f_devuelve_cobros_liquidacion(ld.id_liquidacion),
               f_devuelve_saldo_liquidacion(ld.id_liquidacion),
               0,
               nvl(f_user, v('APP_USER')),
               'NO',
               f_devuelve_saldo_liquidacion(ld.id_liquidacion),
               DECODE(LD.NRO_LIQUIDACION, NULL, NULL, LPAD(LD.NRO_LIQUIDACION, 4, '0') || '/' || SUBSTR(LD.PERIODO, 3, 4))
        from  dpa_liquidacion_despacho ld
        where ld.cli_id_cliente = pi_id_cliente
          and f_devuelve_saldo_liquidacion(ld.id_liquidacion) > 0
          and ld.fecha between To_Date(pi_fecha_desde,'DD-MM-RR') and To_Date(pi_fecha_hasta,'DD-MM-RR');
    exception
      when no_data_found then
        Raise_Application_Error(-20999,'No se encontraron liquidaciones Pendientes para el client: '||v_razon_social);
      when others then
        Raise_Application_Error(-20999,'Error al insertar los registros en la tabla Auxiliar. Error: '||sqlerrm);
    end;

  exception
    when others then
       raise_application_error (-20000, 'Error: '|| SQLERRM ||' línea: '||dbms_utility.format_error_backtrace);
  end pr_carga_auxiliar_liquidacion;
-----------------------------------------------------------------------------------------------------------------------
--Eliminamos las filas que no se seleccionaron para ser procesadas
 procedure pr_elimina_no_procesado IS
  BEGIN
    DELETE FROM galena_core.dpa_aux_reposicion_liquidacion
    WHERE procesar = 'NO'
    AND usuario = v('APP_USER');
  end pr_elimina_no_procesado;
-----------------------------------------------------------------------------------------------------------------------
  procedure pr_genera_reposicion_liquidacion (
    pi_banco            in  number    ,
    pi_observacion      in  varchar2  ,
    pi_moneda           in  number    ,
    pi_fecha            in  date      ,
    pi_permiso_per      in  number    ,
    pi_cliente          in  number    ,
    pi_ruc              in  varchar2  ,
    pi_tipo_trans       in  number    ,
    pi_concepto         in  number    ,
    pi_numero           in  number    ,
    pi_id_liquidacion   in  number    ,
    po_id_liq_trans     out number    ,
    po_id_transaccion   out number    ,
    pi_monto            in  number
  )
  is
  -------------------------------------------------------
    -- variables
  -------------------------------------------------------
    v_id_transaccion       number(20);
    v_id_pag_cli           number(20);
    v_secuencia            number(20) := 0;
    v_total_gastos         dpa_liquidacion_despacho.total_gastos%type;
    v_saldo                number(20,2);
    v_id_moneda_banco      banco.mon_id_moneda%type;
    v_monto                number(20,2);
    v_cambio               number(20,2);
    v_forma_pago           number(20);
    v_banco_tarj           number(20);
    v_num_cheque           number(20);
    v_num_tarjeta          number(20);
    v_num_retencion        varchar2(60);
    v_fecha_retencion      date;
    v_fecha_cheque         date;
    v_fecha_cheque_venc    date;
    v_facutra_retenc       number(20);
    v_boleta_deposito      number(20);
    v_fecha_deposito       date;
    v_cambio_retencion     number(20);
    v_porcentaje           number(20,2);
    v_numero_cta_dep       number (20);
    v_num_recibo           number(20);
    v_num_recibo_antiguo   number(20);
    v_tit_cta_dep          varchar2 (200);
    v_nota_credito_cred    number (20);
    v_cod_giftcard         varchar2(100);
    v_id_factura_compra    number (20);
    v_id_cliente_gif       cliente.id_cliente%type;
    v_num_factura_gif      number(20);
    v_id_proveedor         number ;
    dummy                  number (20);
    v_tipo_cobro           varchar2(20);
    v_monto_cotizacion     number(22,2);
    v_total_gastos_cotiz   number(22,2);

  -------------------------------------------------------
    -- Constantes
  -------------------------------------------------------
    c_usuario              constant persona.usuario%type    := v('APP_USER');
    c_id_persona           constant persona.id_persona%type := devuelve_id_usuario(V('APP_USER'));
    c_id_empresa           constant empresa.id_empresa%type := devuelve_id_empresa(V('APP_USER'));

  -------------------------------------------------------
  begin
  /* Consultamos la moneda cargada para el banco seleccionado */
    begin
      select mon_id_moneda
      into v_id_moneda_banco
      from banco
      where id_banco = pi_banco;
    exception
      when no_data_found then
        Raise_Application_Error(-20999,'El banco seleccionado, no cuenta con una moneda cargada. Favor verificar.');
    end;

  /* Validamos que la moneda seleccionada no sea diferente a la moneda del banco seleccionado */
    if pi_moneda != v_id_moneda_banco then
      raise_application_error(-20999,'NO SE PUEDE CONCRETAR LA TRANSACCION SI LA MONEDA DEL BANCO SELECCIONADO ES DISTINTA A LA MONEDA DEL COBRO, FAVOR VERIFICAR.');
    end if;

  /* Consultamos el total de monto de la transacción */
    begin
      select sum(Nvl(det.monto,0))
      into v_monto
      from dpa_aux_det_trans_cobro_liq det
      where det.usuario = c_usuario;
    end;

  /* Asiganamos valores (ID) a las variables a utilizar en nuestros inserts */
    cgui$cg_code_controls('SEQ_TRANSACCION',v_id_transaccion);
    cgui$cg_code_controls('SEQ_PAG_CLI'    ,v_id_pag_cli);

  /* Obtenemos el punto de expedición para insertar la numeración del recibo */
    v_num_recibo := core_ventas.f_obtiene_timbrado('RECIBO', pi_fecha, 'NRO_FACTURA');

  /* Consultamos si ya existe o si existe mas de una vez el numero de recibo a insertar */
  begin
    select pg.numero
    into v_num_recibo_antiguo
    from pag_cli pg
    where pg.numero = v_num_recibo;
  exception
    when no_data_found then
      v_num_recibo_antiguo := null;
     when too_many_rows then
      print_error('El numero de recibo: '||v_num_recibo ||' ya existe mas de ua vez. Favor verificar');
  end;

  /* Si ya existe el numero de recibo, levantamos un mensaje */
  if v_num_recibo_antiguo is not null then
     print_error('El numero de recibo ya existe:'||v_num_recibo ||'. Favor verificar');
  end if;

  /* Procedemos a insertar la cabecera de la transacción */
  begin
    insert into transaccion values (current_date,
                                    v_id_transaccion,
                                    c_id_persona,
                                    pi_tipo_trans,
                                    pi_banco,
                                    v_monto,
                                    null,
                                    null,
                                    pi_observacion,
                                    pi_moneda,
                                    pi_concepto,
                                    pi_fecha,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null ,
                                    null,
                                    'ACTIVO',
                                    c_id_empresa,
                                    NULL
                                   );
  exception
    when dup_val_on_index then
      Raise_Application_Error(-20999,'Ya existe una transacción con id: '||v_id_transaccion || '. Favor verificar.');
    when others then
      Raise_Application_Error(-20999,'Error al insertar la transacción. Error: '||sqlerrm);
  end;

   /* Procedemos a insertar la cabecera del Pago Cliente (PAG_CLI) */
  begin
    insert into pag_cli values ( pi_observacion,
                                 v_monto,
                                 pi_fecha,
                                 v_id_pag_cli,
                                 v_id_transaccion,
                                 v_num_recibo,
                                 pi_cliente,
                                 'PAGADO',
                                 v_tipo_cobro,
                                 'ACTIVO',
                                 pi_permiso_per,
                                 NULL,
                                 NULL,
                                 c_id_empresa,
                                 NULL,
                                 'NO',
                                 'NO'
                                );
  exception
    when dup_val_on_index then
      Raise_Application_Error(-20999,'Ya existe una cobro con id: '||v_id_pag_cli || '. Favor verificar.');
    when others then
      Raise_Application_Error(-20999,'Error al insertar la cabecera del cobro a cliente. Error: '||sqlerrm);
  end;

  /* Declaramos un cursor para consultar la tabla auxiliar, para luego insertar los registros en nuestro detalle transacción */
   declare
     cursor c_introd_det is
       select dtl.for_pag_id_forma_pago,
              dtl.monto,
              dtl.ban_tar_id_banco_tarjeta,
              dtl.num_cheque,
              dtl.num_tarjeta,
              dtl.num_retencion,
              dtl.fecha_retencion,
              dtl.fecha_cheque,
              dtl.fecha_vencimiento_cheque,
              dtl.fac_ven_id_factura_venta,
              dtl.boleta_deposito,
              dtl.fecha_deposito,
              dtl.cambio_retencion,
              dtl.porcentaje,
              dtl.numero_cta_deposito,
              dtl.titular_cta_deposito,
              dtl.not_cre_id_nota_credito,
              dtl.cod_giftcard,
              dtl.fac_com_id_factura_compra
        from dpa_aux_det_trans_cobro_liq dtl
        where dtl.usuario = c_usuario;

  /* Abrimos el cursor y cargamos las variables correspondientes */
    begin
      open c_introd_det;
      fetch c_introd_det into v_forma_pago,
                              v_monto,
                              v_banco_tarj,
                              v_num_cheque,
                              v_num_tarjeta,
                              v_num_retencion,
                              v_fecha_retencion,
                              v_fecha_cheque,
                              v_fecha_cheque_venc,
                              v_facutra_retenc,
                              v_boleta_deposito,
                              v_fecha_deposito,
                              v_cambio_retencion,
                              v_porcentaje,
                              v_numero_cta_dep,
                              v_tit_cta_dep,
                              v_nota_credito_cred,
                              v_cod_giftcard,
                              v_id_factura_compra;
      while c_introd_det%found
        loop

  /* Validación para forma de pago: CHEQUE */
          if v_forma_pago = 2 and (v_num_cheque        is null
                              or   v_fecha_cheque      is null
                              or   v_fecha_cheque_venc is null
                              or   v_banco_tarj        is null
                              ) then
              raise_application_error (-20000, 'DEBE INGRESAR TODOS LOS DATOS DEL CHEQUE (NUMERO DE CHEQUE, BANCO DEL CHEQUE, FECHA DEL CHEQUE Y VENCIMIENTO DEL CHEQUE).');
          end if;

  /* Validación para formas de pago: TARJETA CREDITO, TARJETA DEBITO*/
          if  v_forma_pago in  (3, 4)
                              and (v_banco_tarj  is null
                              or   v_num_tarjeta is null
                              ) then
              raise_application_error (-20000, 'DEBE INGRESAR TODOS LOS DATOS DE LA TARJETA (VOUCHER Y BANCO DE LA TARJETA).');
          end if;

  /* Validación para formas de pago: RETENCION IVA, RETENCION RENTA*/
          if  v_forma_pago in  (6,7)
                              and (v_num_retencion is null
                              or   v_fecha_retencion is null
                              or    v_facutra_retenc is null
                              ) then
              raise_application_error (-20000, 'DEBE INGRESAR TODOS LOS DATOS DE LA RETENCION (FECHA DE RETENCION, NUMERO DE RETENCION Y FACTURA A SER RETENIDA).');
          end if;

  /* Validación para formas de pago: Retencion Ley 2051: 491.781 entes publicas (D)*/
          if  v_forma_pago in  (11)
                              and (v_nota_credito_cred is null
                              ) then
              raise_application_error (-20000, 'DEBE INGRESAR TODOS LOS DATOS DE LA NOTA DE CREDITO (NUMEROD E NOTA DE CREDITO).');
          end if;

  /* Validación para formas de pago: TRANSFERENCIA BANCARIA, DEPOSITO BANCARIO */
          if  v_forma_pago in  (9,10)
                              and (v_boleta_deposito is null
                              ) then
                raise_application_error (-20000, 'DEBE INGRESAR TODOS LOS DATOS DE LA TRANSFERENCIA BANCARIA (NUMERO DE BOLETA DEPOSITO.');
          end if;

  /* Validaciones en caso de utilizar Giftcard */
          if v_cod_giftcard is not null then
            begin
             select 1
             into   dummy
             from   factura_venta
             where  cod_giftcard = v_cod_giftcard ;
            exception
               when no_data_found then
                    RAISE_APPLICATION_ERROR(-20000,'ERROR: EL CODIGO '||V_COD_GIFTCARD||' NO FUE REGISTRADA EN NINGUNA VENTA, NO SE PUEDE REALIZAR EL PAGO CON EL MISMO. GRACIAS') ;
            end;
            begin
              select cli_id_cliente
              into   v_id_cliente_gif
              from   factura_venta
              where  cod_giftcard = v_cod_giftcard ;
              if  v_id_cliente_gif != pi_cliente then
                  RAISE_APPLICATION_ERROR(-20000,'ERROR: PARA REALIZAR EL PAGO CON EL CODIGO '||V_COD_GIFTCARD||', LA FACTURA DEBE DE ESTAR A NOMBRE DEL MISMO CLIENTE.');
              end if;
            end ;
            begin
              select fv.num_factura
              into   v_num_factura_gif
              from   det_pag_cli d ,
                     pag_cli p,
                     factura_venta fv,
                     det_transaccion dt
              where  d.pag_id_pago =  p.id_pago
              and    d.fac_vent_id_factura_venta = fv.id_factura_venta
              and    dt.tran_id_transaccion = p.tran_id_transaccion
              and    dt.cod_giftcard = v_cod_giftcard ;
            exception
              when no_data_found then
                null ;
            end ;
          end if ;

  /* Insertamos el detalle de la transacciòn */
          begin
            v_secuencia := v_secuencia +1;

            insert into det_transaccion values ( v_monto,
                                                 v_id_transaccion,
                                                 v_forma_pago,
                                                 v_banco_tarj,
                                                 v_num_cheque,
                                                 null,
                                                 v_num_tarjeta,
                                                 v_num_retencion,
                                                 v_fecha_retencion,
                                                 v_fecha_cheque,
                                                 v_fecha_cheque_venc,
                                                 v_facutra_retenc,
                                                 v_boleta_deposito,
                                                 v_fecha_deposito,
                                                 nvl (v_cambio_retencion, 1),
                                                 case
                                                   when v_forma_pago in  (6,7) then v_monto * nvl (v_cambio_retencion, 1)
                                                   else 0
                                                 end,
                                                 v_secuencia,
                                                 v_porcentaje,
                                                 v_numero_cta_dep,
                                                 v_tit_cta_dep,
                                                 v_nota_credito_cred,
                                                 v_cod_giftcard,
                                                 null,
                                                 v_id_factura_compra,
                                                 null,
                                                 null,
                                                 null
                                               );
          exception
            when others then
              Raise_Application_Error(-20999,'Error al insertar la el detalle de la transacción. Error: '||sqlerrm);
          end;
          fetch c_introd_det into v_forma_pago,
                                  v_monto,
                                  v_banco_tarj,
                                  v_num_cheque,
                                  v_num_tarjeta,
                                  v_num_retencion,
                                  v_fecha_retencion,
                                  v_fecha_cheque,
                                  v_fecha_cheque_venc,
                                  v_facutra_retenc,
                                  v_boleta_deposito,
                                  v_fecha_deposito,
                                  v_cambio_retencion,
                                  v_porcentaje,
                                  v_numero_cta_dep,
                                  v_tit_cta_dep,
                                  v_nota_credito_cred ,
                                  v_cod_giftcard ,
                                  v_id_factura_compra;
      end loop;
      close c_introd_det;
    exception
      when others then
        Raise_Application_Error(-20999,'Error al generar el detalle de la transacción. Error: '||sqlerrm);
    end;

  /*Actualizamos el timbrado/punto de expedición */
    begin
      update timbrado t
      set t.numero_fact = t.numero_fact +1
      where t.tipo = 'RECIBO'
        and pi_fecha between t.fecha_timbrado and t.fecha_vencimiento;
    exception
      when others then
        Raise_Application_Error(-20999,'Error al actualizar el timbrado. Error: '||sqlerrm);
    end;

    po_id_transaccion := v_id_transaccion;

  /* Limpiamos la tabla auxiliar */
    begin
      delete dpa_aux_det_trans_cobro_liq
      where usuario = c_usuario;
    exception
      when others then
        Raise_Application_Error(-20999,'Error al limpiar la tabla auxiliar. Error: '||sqlerrm);
    end;
---------------------------------------------------------------------------------------------------------------
  /* REALIZAMOS LA ASOCIACIÓN ENTRE LA TRANSACCIÓN Y LA REPOSICIÓN */
---------------------------------------------------------------------------------------------------------------
    DECLARE
    -------------------------------------------------------
    -- variables
    -------------------------------------------------------
      v_id_liq_trans        dpa_liquidacion_despacho.id_liquidacion%type;
      v_total_gastos_liq    dpa_liquidacion_despacho.total_gastos%type;
      v_monto_liq           dpa_liquidacion_transaccion_detalle.monto%type;
      v_monto_total         dpa_liquidacion_transaccion_detalle.monto%type;
      v_moneda number(20);
      v_fecha date;
    -------------------------------------------------------
    begin
      for reg in (select aux.id_liquidacion,
                         aux.mon_id_moneda,
                         aux.saldo,
                         aux.saldo_usd,
                         aux.total_gasto_usd,
                         aux.total_gasto_gs,
                         aux.monto_procesar
                  from galena_core.dpa_aux_reposicion_liquidacion aux
                  where usuario = c_usuario
                    and procesar = 'SI'
                 )
      loop
        select Nvl(dpa.total_gastos,0)
        into v_total_gastos
        from dpa_liquidacion_despacho dpa
        where dpa.id_liquidacion = reg.id_liquidacion;

        cgui$cg_code_controls ('SEQ_LIQ_TRANS', v_id_liq_trans);
        begin
          insert into dpa_liquidacion_transaccion values(v_id_liq_trans,
                                                         reg.id_liquidacion,
                                                         reg.monto_procesar
                                                         --v_total_gastos
                                                        );
        exception
          when dup_val_on_index then
            Raise_Application_Error(-20999,'Ya existe un registro con id: '|| v_id_liq_trans ||' ,dentro de la tabla: "DPA_LIQUIDACION_TRANSACCION" ');
        end;

        begin
          select monto_trans,
                 mon_id_moneda,
                 fecha
          into v_monto_liq,
               v_moneda,
               v_fecha
          from transaccion tr
          where tr.id_transaccion = v_id_transaccion;
        exception
          when no_data_found then
            null;
        end;

        begin
          insert into dpa_liquidacion_transaccion_detalle values( null,
                                                                  v_id_liq_trans,
                                                                  v_id_transaccion,
                                                                  null,
                                                                  reg.monto_procesar
                                                                  --v_monto_liq
                                                                 );
        exception
          when dup_val_on_index then
            Raise_Application_Error(-20999,'Ya existe un registro con id: '|| v_id_liq_trans ||' ,dentro de la tabla: DPA_LIQUIDACION_TRANSACCION_DETALLE ');
        end;
      end loop;
    end;
  exception
    when others then
      Raise_Application_Error(-20999,'Error código: '||SQLCODE ||' Mensaje de Error: '|| sqlerrm);
  end pr_genera_reposicion_liquidacion;

end dpa_pkg_reposicion_masiva;
/

