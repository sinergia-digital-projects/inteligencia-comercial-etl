library(odbc)
library(tidyverse)
library(lubridate)

#conexion a base de datos---------------------------------------------------------------------
con <- odbc::dbConnect(odbc::odbc(),
                       Driver   = "SQL Server",
                       Server   = "10.112.254.11",
                       Database = "dbFortalezaCore",
                       UID      = ****,
                       PWD      = ****)

dw = dbConnect(odbc::odbc(),
               Driver   = "MySQL ODBC 8.0 Unicode Driver",
               Server   = "10.112.254.7",
               Database = "dw_inteligencia_comercial",
               UID      = ****,
               PWD      = ****)
#historico de cartera-------------------------------------------------------------------------
historico_cartera = tbl(dw, "cartera") %>%
  collect()

fechas_estado = seq(from = as.Date("2022-12-07"),
             to = as.Date(lubridate::floor_date(Sys.Date(), unit = "month")),
             by = "month")

Historico_Cartera = fechas_estado %>%
  set_names() %>%
  map(~historico_cartera %>%
        filter(Fecha_Actualizacion <= .x) %>%
        group_by(Contrato_Numero) %>%
        filter(Fecha_Actualizacion == max(Fecha_Actualizacion)) %>%
        filter(Contrato_Estado_Nombre %in% c("Adjudicado",
                                             "Posesión",
                                             "Activo",
                                             "Gestión")) %>% 
        ungroup() %>%
        mutate(Estado_al = lubridate::floor_date(.x, unit = "months"))) %>%
  map_df(rbind)
#traer aportes de la base de datos------------------------------------------------------------
aportes = tbl(con,"Contrato_Articulo_Cuota") %>% 
  #articulos de espera, posesion y extraordinarios
  filter(Articulo_Id %in% c(16,18,20,21,22,25,392,393,394,397,14256,14258,14260,14262),
         is.na(Contrato_Articulo_Cuota_Padre_Id),
         Cuota_Fecha_Vencimiento <= sql("(SELECT EOMONTH(GETDATE(),0))") |
          (Cuota_Fecha_Vencimiento > sql("(SELECT EOMONTH(GETDATE(),0))") &
             Cuota_Estado_Id %in% c(5,7))
         ) %>% 
  #traer ultima factura del aporte
  left_join(tbl(con, "Comprobante_Pago") %>% 
              filter(!is.na(Contrato_Articulo_Cuota_Id)) %>% 
              group_by(Contrato_Articulo_Cuota_Id) %>% 
              filter(Comprobante_Pago_Id == max(Comprobante_Pago_Id)) %>% 
              left_join(tbl(con, "Comprobante"),
                        by = "Comprobante_Id") %>% 
              select(Contrato_Articulo_Cuota_Id,
                     Comprobante_Fecha),
            by = "Contrato_Articulo_Cuota_Id") %>% 
  transmute(Contrato_Articulo_Id,
         Articulo_Id,
         Cuota_Nro,
         Cuota_Estado_Id,
         Cuota_Fecha_Vencimiento = CAST(sql("Cuota_Fecha_Vencimiento AS date")),
         Cuota_Monto,
         Cuota_Saldo_Pendiente,
         Comprobante_Fecha) %>% 
  left_join(tbl(con, "Cuota_Estado") %>% 
              select(Cuota_Estado_Id,
                     Cuota_Estado_Nombre),
            by = "Cuota_Estado_Id") %>% 
  left_join(tbl(con,"Contrato_Articulo") %>% 
              select(Contrato_Articulo_Id,
                     Contrato_Id,
                     DiaDeCobro),
            by = "Contrato_Articulo_Id") %>% 
  inner_join(tbl(con,"Contrato") %>% 
                select(Contrato_Id,
                      Contrato_Numero,
                      Contrato_Estado_Id),
             by = "Contrato_Id") %>% 
  left_join(tbl(con, "Contrato_Estado") %>% 
              select(Contrato_Estado_Id,
                     Contrato_Estado_Nombre),
            by = "Contrato_Estado_Id") %>% 
  left_join(tbl(con, "Articulo") %>% 
              select(Articulo_Id,
                     Articulo_Nombre),
            by = "Articulo_Id") %>% 
  #se agrega precio de los articulos
  left_join(tbl(con,"Articulo") %>% 
              filter(Articulo_Id %in% c(16,18,20,21,22,25,392,393,394,397,14256,14258,14260,14262)) %>% 
              select(Articulo_Id) %>% 
              left_join(tbl(con,"Articulo_Concepto") %>% 
                          select(Articulo_Id,
                                 Articulo_Concepto_Id),
                        by = "Articulo_Id") %>% 
              left_join(tbl(con,"Articulo_Concepto_Precio") %>% 
                          select(Articulo_Concepto_Id,
                                 Precio),
                        by = "Articulo_Concepto_Id") %>% 
              group_by(Articulo_Id) %>% 
              summarise(Precio = sum(Precio)) %>% 
              ungroup(),
            by = "Articulo_Id") %>% 
  #agregar metodo de pago. instruccion_id not null seria el vigente
  left_join(tbl(con, "Contrato_Articulo_PagoMetodo") %>% 
              group_by(Contrato_Articulo_Id) %>% 
              #tomamos el instruccion_id que nos sea null
              filter(is.na(Contrato_Articulo_Instruccion_Id)) %>% 
              filter(Contrato_Articulo_PagoMetodo_Id == max(Contrato_Articulo_PagoMetodo_Id)) %>% 
              ungroup() %>% 
              distinct() %>%  
              select(Contrato_Articulo_Id,
                     Pago_Metodo_Id),
            by = "Contrato_Articulo_Id") %>% 
  left_join(tbl(con, "Pago_Metodo") %>% 
              select(Pago_Metodo_Id,
                     Pago_Metodo_Nombre),
            by = "Pago_Metodo_Id") %>% 
  mutate(Tipo_Aporte = ifelse(Articulo_Nombre %like% "%Extraordinario%",
                              "Extraordinario",
                              "Ordinario"),
         Fecha_Reporte = GETDATE()) %>% 
  select(Contrato_Numero,
         Contrato_Estado_Nombre,
         DiaDeCobro,
         Pago_Metodo_Nombre,
         Tipo_Aporte,
         Articulo_Nombre,
         Precio,
         Cuota_Nro,
         Cuota_Estado_Nombre,
         Cuota_Monto,
         Cuota_Saldo_Pendiente,
         Cuota_Fecha_Vencimiento,
         Comprobante_Fecha,
         Fecha_Reporte) %>% 
  arrange(Contrato_Numero, desc(Tipo_Aporte), Cuota_Nro) %>% 
  collect()


dbDisconnect(con); rm(con)

aportes = aportes %>% 
  mutate_if(lubridate::is.POSIXct,
            as.Date)

fechas = seq(from = as.Date("2023-01-01"),
             to = ceiling_date(Sys.Date(), unit = "month"),
             by = "month")-1
#Cantidad de aportes--------------------------------------------------------------------------
mes_actual_cantidad = aportes %>% 
  filter(Contrato_Estado_Nombre %in% c("Adjudicado",
                                       "Posesión",
                                       "Activo",
                                       "Gestión")) %>% 
  group_by(Tipo_Aporte,
           Contrato_Estado_Nombre,
           Pago_Metodo_Nombre,
           Articulo_Nombre,
           DiaDeCobro) %>% 
  summarise(Cuotas_Atrasadas = sum(
    Cuota_Fecha_Vencimiento < fechas[length(fechas)] &
      (
        !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
          (
            Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
              Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")
          )
      ),
    na.rm = T
  ),
  Objetivo_Mes = sum(
    Cuota_Fecha_Vencimiento == fechas[length(fechas)] &
      (
        !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
          (
            Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
              Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")
          )
      ),
    na.rm = T
  ),
  Pagado_Atrasadas = sum(
    Cuota_Fecha_Vencimiento < fechas[length(fechas)] &
      Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
      Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months"),
    na.rm = T
  ),
  Pagado_Mes = sum(
    Cuota_Fecha_Vencimiento == fechas[length(fechas)] &
      Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
      Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months"),
    na.rm = T
  ),
  Pagado_Adelantado = sum(
    Cuota_Fecha_Vencimiento > fechas[length(fechas)] &
      Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
      Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months"),
    na.rm = T
  )
  ) %>% 
  ungroup() %>% 
  mutate(Mes = floor_date(fechas[length(fechas)], unit = "months"))

meses_anteriores_cantidad = tibble()

for (i in 1:(length(fechas)-1)) {
  a = aportes %>% 
    filter(!str_detect(Contrato_Numero,".*-[:alpha:]")) %>% 
    left_join(Historico_Cartera %>% 
              filter(Estado_al == floor_date(fechas_estado[i], unit = "months")) %>% 
              distinct(Contrato_Numero, .keep_all = T) %>% 
              select(Contrato_Numero,
                     Contrato_Estado_Nombre2 = Contrato_Estado_Nombre,
                     Pago_Metodo_Nombre2 = Pago_Metodo_Nombre,
                     DiaDeCobro2 = DiaDeCobro),
              by = "Contrato_Numero") %>% 
    filter(Contrato_Estado_Nombre2 %in% c("Adjudicado",
                                          "Posesión",
                                          "Activo",
                                          "Gestión")) %>% 
    mutate(Contrato_Estado_Nombre = Contrato_Estado_Nombre2,
           Pago_Metodo_Nombre = Pago_Metodo_Nombre2,
           DiaDeCobro = DiaDeCobro2) %>% 
    select(-Pago_Metodo_Nombre2, -Contrato_Estado_Nombre2) %>% 
    group_by(Tipo_Aporte,
             Contrato_Estado_Nombre,
             Pago_Metodo_Nombre,
             Articulo_Nombre,
             DiaDeCobro) %>% 
    summarise(Cuotas_Atrasadas = sum(
      Cuota_Fecha_Vencimiento < fechas[i] &
        (
          !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
            (
              Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                Comprobante_Fecha >= floor_date(fechas[i], unit = "months")
            )
        ),
      na.rm = T
    ),
    Objetivo_Mes = sum(
      Cuota_Fecha_Vencimiento == fechas[i] &
        (
          !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
            (
              Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                Comprobante_Fecha >= floor_date(fechas[i], unit = "months")
            )
        ),
      na.rm = T
    ),
    Pagado_Atrasadas = sum(
      Cuota_Fecha_Vencimiento < fechas[i] &
        Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
        Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
        Comprobante_Fecha <= fechas[i],
      na.rm = T
    ),
    Pagado_Mes = sum(
      Cuota_Fecha_Vencimiento == fechas[i] &
        Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
        Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
        Comprobante_Fecha <= fechas[i],
      na.rm = T
    ),
    Pagado_Adelantado = sum(
      Cuota_Fecha_Vencimiento > fechas[i] &
        Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
        Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
        Comprobante_Fecha <= fechas[i],
      na.rm = T
    )
    ) %>% 
    ungroup() %>% 
    mutate(Mes = floor_date(fechas[i], unit = "months"))
  
  meses_anteriores_cantidad = meses_anteriores_cantidad %>% 
    bind_rows(a)
}

compilado_cantidad = mes_actual_cantidad %>% 
  bind_rows(meses_anteriores_cantidad) %>% 
  complete(Mes = seq.Date(as.Date("2023-01-01"),as.Date("2023-12-01"), by = "months")) %>% 
  arrange(Mes)

#Cantidad de contratos------------------------------------------------------------------------

mes_actual_contratos = aportes %>% 
  filter(Contrato_Estado_Nombre %in% c("Adjudicado",
                                       "Posesión",
                                       "Activo",
                                       "Gestión")) %>% 
  group_by(Contrato_Numero,
           Tipo_Aporte,
           Contrato_Estado_Nombre,
           Pago_Metodo_Nombre,
           Articulo_Nombre,
           DiaDeCobro) %>% 
  summarise(Cuotas_Atrasadas = sum(
    Cuota_Fecha_Vencimiento < fechas[length(fechas)] &
      (
        !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
          (
            Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
              Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")
          )
      ),
    na.rm = T
  ),
  Objetivo_Mes = sum(
    Cuota_Fecha_Vencimiento == fechas[length(fechas)] &
      (
        !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
          (
            Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
              Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")
          )
      ),
    na.rm = T
  ),
  Pagado_Atrasadas = sum(
    Cuota_Fecha_Vencimiento < fechas[length(fechas)] &
      Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
      Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months"),
    na.rm = T
  ),
  Pagado_Mes = sum(
    Cuota_Fecha_Vencimiento == fechas[length(fechas)] &
      Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
      Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months"),
    na.rm = T
  ),
  Pagado_Adelantado = sum(
    Cuota_Fecha_Vencimiento > fechas[length(fechas)] &
      Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
      Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months"),
    na.rm = T
  )
  ) %>% 
  ungroup() %>% 
  summarise(across(2:6, ~sum(.x > 0)), .by = c(Tipo_Aporte,
                                               Contrato_Estado_Nombre,
                                               Pago_Metodo_Nombre,
                                               Articulo_Nombre,
                                               DiaDeCobro)) %>% 
  mutate(Mes = floor_date(fechas[length(fechas)], unit = "months"))

meses_anteriores_contratos = tibble()

for (i in 1:(length(fechas)-1)) {
  a = aportes %>% 
    filter(!str_detect(Contrato_Numero,".*-[:alpha:]")) %>% 
    left_join(Historico_Cartera %>% 
                filter(Estado_al == floor_date(fechas_estado[i], unit = "months")) %>% 
                distinct(Contrato_Numero, .keep_all = T) %>% 
                select(Contrato_Numero,
                       Contrato_Estado_Nombre2 = Contrato_Estado_Nombre,
                       Pago_Metodo_Nombre2 = Pago_Metodo_Nombre,
                       DiaDeCobro2 = DiaDeCobro),
              by = "Contrato_Numero") %>% 
    filter(Contrato_Estado_Nombre2 %in% c("Adjudicado",
                                          "Posesión",
                                          "Activo",
                                          "Gestión")) %>% 
    mutate(Contrato_Estado_Nombre = Contrato_Estado_Nombre2,
           Pago_Metodo_Nombre = Pago_Metodo_Nombre2,
           DiaDeCobro = DiaDeCobro2) %>% 
    select(-Pago_Metodo_Nombre2, -Contrato_Estado_Nombre2) %>% 
    group_by(Contrato_Numero,
             Tipo_Aporte,
             Contrato_Estado_Nombre,
             Pago_Metodo_Nombre,
             Articulo_Nombre,
             DiaDeCobro) %>% 
    summarise(Cuotas_Atrasadas = sum(
      Cuota_Fecha_Vencimiento < fechas[i] &
        (
          !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
            (
              Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                Comprobante_Fecha >= floor_date(fechas[i], unit = "months")
            )
        ),
      na.rm = T
    ),
    Objetivo_Mes = sum(
      Cuota_Fecha_Vencimiento == fechas[i] &
        (
          !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
            (
              Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                Comprobante_Fecha >= floor_date(fechas[i], unit = "months")
            )
        ),
      na.rm = T
    ),
    Pagado_Atrasadas = sum(
      Cuota_Fecha_Vencimiento < fechas[i] &
        Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
        Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
        Comprobante_Fecha <= fechas[i],
      na.rm = T
    ),
    Pagado_Mes = sum(
      Cuota_Fecha_Vencimiento == fechas[i] &
        Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
        Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
        Comprobante_Fecha <= fechas[i],
      na.rm = T
    ),
    Pagado_Adelantado = sum(
      Cuota_Fecha_Vencimiento > fechas[i] &
        Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
        Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
        Comprobante_Fecha <= fechas[i],
      na.rm = T
    )
    ) %>% 
    ungroup() %>% 
    summarise(across(2:6, ~sum(.x > 0)), .by = c(Tipo_Aporte,
                                                 Contrato_Estado_Nombre,
                                                 Pago_Metodo_Nombre,
                                                 Articulo_Nombre,
                                                 DiaDeCobro)) %>%
    mutate(Mes = floor_date(fechas[i], unit = "months"))
  
  meses_anteriores_contratos = meses_anteriores_contratos %>% 
    bind_rows(a)
}

compilado_contratos = mes_actual_contratos %>% 
  bind_rows(meses_anteriores_contratos) %>% 
  complete(Mes = seq.Date(as.Date("2023-01-01"),as.Date("2023-12-01"), by = "months")) %>% 
  arrange(Mes)

#Monto de aportes-----------------------------------------------------------------------------
mes_actual_monto = aportes %>% 
  filter(Contrato_Estado_Nombre %in% c("Adjudicado",
                                       "Posesión",
                                       "Activo",
                                       "Gestión")) %>% 
  group_by(Tipo_Aporte,
           Contrato_Estado_Nombre,
           Pago_Metodo_Nombre,
           Articulo_Nombre,
           DiaDeCobro) %>% 
  summarise(Cuotas_Atrasadas = sum(
    Precio[Cuota_Fecha_Vencimiento < fechas[length(fechas)] &
             (
               !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
                 (
                   Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                     Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")
                 )
             )],
    na.rm = T
  ),
  Objetivo_Mes = sum(
    Precio[Cuota_Fecha_Vencimiento == fechas[length(fechas)] &
             (
               !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
                 (
                   Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                     Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")
                 )
             )],
    na.rm = T
  ),
  Pagado_Atrasadas = sum(
    Cuota_Monto[Cuota_Fecha_Vencimiento < fechas[length(fechas)] &
                  Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                  Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")],
    na.rm = T
  ),
  Pagado_Mes = sum(
    Cuota_Monto[Cuota_Fecha_Vencimiento == fechas[length(fechas)] &
                  Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                  Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")],
    na.rm = T
  ),
  Pagado_Adelantado = sum(
    Cuota_Monto[Cuota_Fecha_Vencimiento > fechas[length(fechas)] &
                  Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                  Comprobante_Fecha >= floor_date(fechas[length(fechas)], unit = "months")],
    na.rm = T
  )
  ) %>% 
  ungroup() %>% 
  mutate(Mes = floor_date(fechas[length(fechas)], unit = "months"))

meses_anteriores_monto= tibble()

for (i in 1:(length(fechas)-1)) {
  a = aportes %>% 
    filter(!str_detect(Contrato_Numero,".*-[:alpha:]")) %>% 
    left_join(Historico_Cartera %>% 
                filter(Estado_al == floor_date(fechas_estado[i], unit = "months")) %>% 
                distinct(Contrato_Numero, .keep_all = T) %>% 
                select(Contrato_Numero,
                       Contrato_Estado_Nombre2 = Contrato_Estado_Nombre,
                       Pago_Metodo_Nombre2 = Pago_Metodo_Nombre,
                       DiaDeCobro2 = DiaDeCobro),
              by = "Contrato_Numero") %>% 
    filter(Contrato_Estado_Nombre2 %in% c("Adjudicado",
                                          "Posesión",
                                          "Activo",
                                          "Gestión")) %>% 
    mutate(Contrato_Estado_Nombre = Contrato_Estado_Nombre2,
           Pago_Metodo_Nombre = Pago_Metodo_Nombre2,
           DiaDeCobro = DiaDeCobro2) %>% 
    select(-Pago_Metodo_Nombre2, -Contrato_Estado_Nombre2) %>% 
    group_by(Tipo_Aporte,
             Contrato_Estado_Nombre,
             Pago_Metodo_Nombre,
             Articulo_Nombre,
             DiaDeCobro) %>% 
    summarise(Cuotas_Atrasadas = sum(
     Precio[ Cuota_Fecha_Vencimiento < fechas[i] &
               (
                 !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
                   (
                     Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                       Comprobante_Fecha >= floor_date(fechas[i], unit = "months")
                   )
               )],
      na.rm = T
    ),
    Objetivo_Mes = sum(
      Precio[Cuota_Fecha_Vencimiento == fechas[i] &
               (
                 !Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") |
                   (
                     Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                       Comprobante_Fecha >= floor_date(fechas[i], unit = "months")
                   )
               )],
      na.rm = T
    ),
    Pagado_Atrasadas = sum(
      Precio[Cuota_Fecha_Vencimiento < fechas[i] &
               Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
               Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
               Comprobante_Fecha <= fechas[i]],
      na.rm = T
    ),
    Pagado_Mes = sum(
      Cuota_Monto[Cuota_Fecha_Vencimiento == fechas[i] &
                    Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                    Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
                    Comprobante_Fecha <= fechas[i]],
      na.rm = T
    ),
    Pagado_Adelantado = sum(
      Cuota_Monto[Cuota_Fecha_Vencimiento > fechas[i] &
                    Cuota_Estado_Nombre %in% c("Cancelada", "Cancelada por Exoneración") &
                    Comprobante_Fecha >= floor_date(fechas[i], unit = "months") &
                    Comprobante_Fecha <= fechas[i]],
      na.rm = T
    )
    ) %>% 
    ungroup() %>% 
    mutate(Mes = floor_date(fechas[i], unit = "months"))
  
  meses_anteriores_monto = meses_anteriores_monto %>% 
    bind_rows(a)
}

compilado_monto = mes_actual_monto %>% 
  bind_rows(meses_anteriores_monto) %>% 
  complete(Mes = seq.Date(as.Date("2023-01-01"),as.Date("2023-12-01"), by = "months")) %>% 
  arrange(Mes)
#calculo de cumplimiento----------------------------------------------------------------------
Cumplimiento_Acumulado = compilado_cantidad %>% 
  filter(Mes >= floor_date(Sys.Date(), unit = "year"),
         Mes <= Sys.Date()) %>% 
  summarise(across(6:10, sum), .by = Mes) %>% 
  summarise(across(2:6, sum)) %>% 
  summarise(Cumplimiento = (Pagado_Atrasadas + Pagado_Mes) / (Cuotas_Atrasadas + Objetivo_Mes))

Cumplimiento_Acumulado_Monto = compilado_monto %>% 
  filter(Mes >= floor_date(Sys.Date(), unit = "year"),
         Mes <= Sys.Date()) %>% 
  summarise(across(6:10, sum), .by = Mes) %>% 
  summarise(across(2:6, sum)) %>% 
  summarise(Cumplimiento = (Pagado_Atrasadas + Pagado_Mes) / (Cuotas_Atrasadas + Objetivo_Mes))

Cumplimiento = compilado_cantidad %>% 
  filter(Mes >= floor_date(Sys.Date(), unit = "year"),
         Mes <= Sys.Date()) %>% 
  summarise(across(6:10, sum), .by = Mes) %>% 
  summarise(Cumplimiento_Atrasadas = Pagado_Atrasadas / Cuotas_Atrasadas,
            Cumplimiento_Mes = Pagado_Mes / Objetivo_Mes,
            .by = Mes) %>% 
  complete(Mes = seq.Date(as.Date("2023-01-01"),as.Date("2023-12-01"), by = "months"))

Cumplimiento_Monto = compilado_monto %>% 
  filter(Mes >= floor_date(Sys.Date(), unit = "year"),
         Mes <= Sys.Date()) %>% 
  summarise(across(6:10, sum), .by = Mes) %>% 
  summarise(Cumplimiento_Atrasadas = Pagado_Atrasadas / Cuotas_Atrasadas,
            Cumplimiento_Mes = Pagado_Mes / Objetivo_Mes,
            .by = Mes) %>% 
  complete(Mes = seq.Date(as.Date("2023-01-01"),as.Date("2023-12-01"), by = "months"))

rm(list = ls()[!grepl("compilado|Acumulado|dw",ls())])

DBI::dbWriteTable(conn = dw,
                  name = DBI::Id(catalog = "dw_inteligencia_comercial",
                                 table = "compilado_cantidad"),
                  value = compilado_cantidad %>% 
                    mutate(Fecha_Actualizacion = Sys.Date()),
                  overwrite = T)

DBI::dbWriteTable(conn = dw,
                  name = DBI::Id(catalog = "dw_inteligencia_comercial",
                                 table = "compilado_contratos"),
                  value = compilado_contratos %>% 
                    mutate(Fecha_Actualizacion = Sys.Date()),
                  overwrite = T)

DBI::dbWriteTable(conn = dw,
                  name = DBI::Id(catalog = "dw_inteligencia_comercial",
                                 table = "compilado_monto"),
                  value = compilado_monto %>% 
                    mutate(Fecha_Actualizacion = Sys.Date()),
                  overwrite = T)

DBI::dbWriteTable(conn = dw,
                  name = DBI::Id(catalog = "dw_inteligencia_comercial",
                                 table = "cumplimiento_acumulado"),
                  value = Cumplimiento_Acumulado %>% 
                    mutate(Fecha_Actualizacion = Sys.Date()),
                  overwrite = T)

DBI::dbWriteTable(conn = dw,
                  name = DBI::Id(catalog = "dw_inteligencia_comercial",
                                 table = "cumplimiento_acumulado_monto"),
                  value = Cumplimiento_Acumulado_Monto %>% 
                    mutate(Fecha_Actualizacion = Sys.Date()),
                  overwrite = T)

dbDisconnect(dw)