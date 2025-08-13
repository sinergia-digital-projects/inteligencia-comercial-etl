#importar paquetes
library(odbc)
library(tidyverse)
library(dbplyr)

#abrir conexiones
dw <- odbc::dbConnect(odbc::odbc(),
                       Driver   = "MySQL ODBC 8.2 Unicode Driver",
                       Server   = "10.112.254.7",
                       Database = "dw",
                       UID      = ****,
                       PWD      = ****)

core <- odbc::dbConnect(odbc::odbc(),
                       Driver   = "SQL Server",
                       Server   = "10.112.254.11",
                       Database = "dbFortalezaCore",
                       UID      = ****,
                       PWD      = ****)

#cartera activa----
contratos = tbl(core, "Contrato") %>% 
  select(Contrato_Id,
         Contrato_Numero, 
         Plan_Id, 
         Fecha_Ingreso, 
         Contrato_Estado_Id,
         Cliente_Id) %>% 
  #agregamos el metodo de pago de la tabla contrato_articulo_pagometodo
  left_join(tbl(core, "Contrato_Articulo_PagoMetodo") %>% 
              select(Contrato_Articulo_Instruccion_Id,
                     Contrato_Articulo_PagoMetodo_Id,
                     Contrato_Articulo_Id,
                     Pago_Metodo_Id) %>% 
              #un articulo puede tener mas de un metodo de pago
              group_by(Contrato_Articulo_Id) %>% 
              #tomamos el instruccion_id que nos sea null
              filter(is.na(Contrato_Articulo_Instruccion_Id)) %>% 
              filter(Contrato_Articulo_PagoMetodo_Id == max(Contrato_Articulo_PagoMetodo_Id)) %>% 
              ungroup() %>% 
              distinct() %>% 
              #seleccionamos los articulos de espera y posesion
              left_join(tbl(core, "Contrato_Articulo") %>% 
                          select(Contrato_Articulo_Id,
                                 Contrato_Id,
                                 Articulo_Id),
                        by = "Contrato_Articulo_Id") %>% 
              filter(Articulo_Id %in% c(16,18,20,25,392,397,14256,14258,14260,14262)),
            by = "Contrato_Id") %>% 
  #dia de cobro de los articulos espera y posesion
  left_join(tbl(core, "Contrato_Articulo") %>% 
              filter(Articulo_Id %in% c(16,18,20,25,392,397,14256,14258,14260,14262)) %>% 
              select(Contrato_Id,
                     DiaDeCobro),
            by = "Contrato_Id") %>% 
  #hallar la cantidad de cuotas se tiene en cuenta espera, posesion y pos pura
  left_join(tbl(core, "Contrato_Articulo") %>% 
              filter(Articulo_Id %in% c(16,18,20,24,25,392,396,397,14256,14258,14260,14262)) %>% 
              select(Contrato_Articulo_Id, 
                     Contrato_Id) %>% 
              #cuotas canceladas
              left_join(tbl(core, "Contrato_Articulo_Cuota") %>% 
                          filter(Cuota_Estado_Id %in% c(5,7),
                                 is.na(Contrato_Articulo_Cuota_Padre_Id)) %>% 
                          #cantidad de cuotas canceladas por articulo
                          count(Contrato_Articulo_Id, name = "Cant_Aportes_Pag"),
                        by = "Contrato_Articulo_Id") %>% 
              #agrupamos por contrato_id y sumamos la cantidad de aportes de los articulos
              group_by(Contrato_Id) %>% 
              summarise(Cant_Aportes_Pag = sum(Cant_Aportes_Pag)),
            by = "Contrato_Id") %>% 
  left_join(tbl(core, "Cliente") %>% 
              select(Cliente_Id,
                     RUC,
                     Razon_Social),
            by = "Cliente_Id") %>% 
  left_join(tbl(core, "Plan") %>% 
              select(Plan_Id,
                     Plan_Nombre),
            by = "Plan_Id") %>% 
  left_join(tbl(core, "Contrato_Estado") %>% 
              select(Contrato_Estado_Id,
                     Contrato_Estado_Nombre),
            by = "Contrato_Estado_Id") %>% 
  left_join(tbl(core, "Pago_Metodo") %>% 
              select(Pago_Metodo_Id,
                     Pago_Metodo_Nombre),
            by = "Pago_Metodo_Id") %>% 
  #filter(Contrato_Estado_Nombre %in% c("Activo","Gestión","Posesión","Adjudicado")) %>% 
  select(Contrato_Numero, 
         Plan_Nombre, 
         Fecha_Ingreso, 
         DiaDeCobro, 
         Pago_Metodo_Nombre, 
         Cant_Aportes_Pag,
         Contrato_Estado_Nombre,
         Razon_Social,
         RUC) %>% 
  collect() %>% 
  mutate(Fecha_Ingreso = as.Date(Fecha_Ingreso))

#cartera del dataware-----
contratos_dw = tbl(dw, in_schema("dw_inteligencia_comercial", "cartera")) %>% 
  group_by(Contrato_Numero) %>% 
  filter(Fecha_Actualizacion == max(Fecha_Actualizacion)) %>% 
  ungroup %>% 
  collect()

#diferencia entre ambos----
diferencia = contratos %>% 
  setdiff(contratos_dw %>% 
            select(-Fecha_Actualizacion))

#escribir diferencia en dataware
tbl(dw, in_schema("dw_inteligencia_comercial", "cartera")) %>% 
  rows_append(diferencia %>% 
                mutate(Fecha_Actualizacion = lubridate::force_tz(Sys.time(), "UTC")),
              copy = T,
              in_place = T)

#cerrar conexiones
dbDisconnect(core)
dbDisconnect(dw)