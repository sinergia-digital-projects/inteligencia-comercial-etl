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

query = "SELECT * 
,CASE WHEN A.MODO_INGRESO LIKE ('%React%') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 THEN 0.469
WHEN A.MODO_INGRESO LIKE ('%React%') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS = 0 THEN 0.064
WHEN A.MODO_INGRESO LIKE ('%React%') AND A.CANT_POSPURA > 0 THEN 0.068
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 5 AND A.PAG_MIN IS NULL THEN 0.842
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 13 AND A.PAG_MIN IS NULL THEN 0.628
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 25 AND A.PAG_MIN IS NULL THEN 0.368
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 250 AND A.PAG_MIN IS NULL THEN 0.022
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 5 AND A.PAG_MIN IS NOT NULL THEN 0.689
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 9 AND A.PAG_MIN IS NOT NULL THEN 0.652
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 13 AND A.PAG_MIN IS NOT NULL THEN 0.405
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 25 AND A.PAG_MIN IS NOT NULL THEN 0.221
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 AND A.TOTAL_APORTES < 250 AND A.PAG_MIN IS NOT NULL THEN 0.167
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS = 0 AND A.TOTAL_APORTES < 5 THEN 0.133
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS = 0 AND A.TOTAL_APORTES < 9 THEN 0.124
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS = 0 AND A.TOTAL_APORTES < 13 THEN 0.071
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS = 0 AND A.TOTAL_APORTES < 25 AND A.PAG_MIN IS NULL THEN 0.036
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS = 0 AND A.TOTAL_APORTES < 25 AND A.PAG_MIN IS NOT NULL THEN 0.046
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA = 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS = 0 AND A.TOTAL_APORTES < 250 THEN 0.007
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA > 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS > 0 THEN 0.353
WHEN (A.MODO_INGRESO = 'Normal' OR A.MODO_INGRESO =  'Inscripcion') AND A.CANT_POSPURA > 0 AND A.CUOTAS_ORDINARIAS_VENCIDAS = 0 THEN 0.017 ELSE 0.038 END AS RIESGO_BAJA
FROM (SELECT C.Contrato_Numero AS CONTRATO
		,C_TIPO.Contrato_Tipo_Nombre AS TIPO
		,CAST(C.Fecha_Ingreso AS DATE) AS FECHA_INGRESO
		,CAST(C.Fecha_Contractual AS DATE) AS FECHA_CONTRATUAL
		,MODO_INGRESO.Contro_Modo_Ingreso_Nombre AS MODO_INGRESO
		,C_MOTIVO.Contrato_Motivo_Nombre MOTIVO_INGRESO
		--CATEGORIA_CONTRATO
		,CASE WHEN C_TIPO.Contrato_Tipo_Nombre = '20 Años' AND C_EST.Contrato_Estado_Codigo = 'POSESIÓN'
				AND ISNULL(CANT_AP.[16],0)+ISNULL(CANT_AP.[18],0)+ISNULL(CANT_AP.[20],0)+ISNULL(CANT_AP.[24],0)+ISNULL(CANT_AP.[25],0) > 149
				THEN 'Inversor Propietario'
				WHEN C_TIPO.Contrato_Tipo_Nombre = '20 Años' AND C_EST.Contrato_Estado_Codigo = 'POSESIÓN'
				AND ISNULL(CANT_AP.[16],0)+ISNULL(CANT_AP.[18],0)+ISNULL(CANT_AP.[20],0)+ISNULL(CANT_AP.[24],0)+ISNULL(CANT_AP.[25],0) < 150
				THEN 'Inversor No Propietario'
				WHEN C_TIPO.Contrato_Tipo_Nombre = '20 Años' AND C_EST.Contrato_Estado_Codigo = 'ADJUDICADO'
				THEN 'Adjudicado'
				WHEN C_EST.Contrato_Estado_Codigo IN ('ACTIVO', 'GESTION')
				THEN 'Inversor'
				WHEN C_EST.Contrato_Estado_Codigo = 'INACTIVO'
				THEN 'Inactivo'
				WHEN C_EST.Contrato_Estado_Codigo = 'BAJA'
				THEN 'Baja'
  				WHEN C_EST.Contrato_Estado_Codigo = 'FINALIZADO'
				THEN 'Finalizado'
				END AS CATEGORIA_CONTRATO
		,C_EST.Contrato_Estado_Nombre AS ESTADO
		,C_SUBEST.Contrato_SubEstado_Nombre AS SUBESTADO
		,CLI.Razon_Social AS RAZON_SOCIAL
		,CLI.Cod_Cliente_Sap AS CODCLIE
		,CLI.Nro_Documento NROCOUMENTO
		,CLI.RUC AS RUC
		,CLICON.Telefono_movil AS TELEFONO
		,CLICON.Correo_electronico AS CORREO
		,direccion.Direccion AS DIRECCION
		,direccion.Direccion_Ciudad_Nombre AS CIUDAD
		,CAST(CLICON.Fecha_nacimiento AS DATE) FECHANACIMIENTO
		,CLI.Estado_Civil ESTADOCIVIL
		,CLI.Profesion	PROFESION
		,CLICON.Ocupacion_laboral OCUPACION
		,CLI.Sexo SEXO
		,CLI.Situacion_Vivienda SITVIVIENDA
		,CLI.Tipo_Ingreso TIPOINGRESO
		,CLI.Estudios ESTUDIOS
		,CLI.Cant_Hijos Hijos
		,ASE_CIERRE.Asesor_Nombre AS ASESOR_CIERRE
		,ASE_ACT.Asesor_Nombre AS ASESOR_ACTUAL
		,SUC.Sucursal_Nombre AS SUCURSAL
		,ATC.Ejecutivo_Cuenta_Nombre
		,NULL AS RIESGO_EQUIFAX
		,CA_ESP.DiaDeCobro AS DIACOBRO_ESP
		,PAGMET_ESP.Pago_Metodo_Nombre AS METODOPAGO_ESP
		,PAGMET_ESP.Entidad_Procesadora_Nombre AS PROCESADORA_ESPERA
		,PAGMET_ESP.Tarjeta_Tipo TARJETA_TIPO
		,CA_EXT.DiaDeCobro AS DIACOBRO_EXT
		,PAGMET_EXT.Pago_Metodo_Nombre AS METODOPAGO_EXT
		,ESP_CAN.Cuota_Nro AS NROCUOTA_CANC
		,ESP_CAN.MaxFechaVenc AS FECHAVENC_NROCUOTA
		,ISNULL(ESP_CAN.MaxFechaComprobante,CA_ESP.UltimoPago_Fecha) AS FECHACANC_NROCUOTA
		,DATEDIFF(MONTH,DATEFROMPARTS(YEAR(ISNULL(ESP_CAN.MaxFechaComprobante,CA_ESP.UltimoPago_Fecha)),MONTH(ISNULL(ESP_CAN.MaxFechaComprobante,CA_ESP.UltimoPago_Fecha)),1),DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1))-1
			AS MESES_NO_PAGO
		,ISNULL(ESP_PEND.Cuota_Nro,ESP_PEND_NO_VENC.Cuota_Nro) AS NROCUOTA_PEND
		,ISNULL(ESP_PEND.MinFechaVenc,ESP_PEND_NO_VENC.MinFechaVenc) AS FECHAVENC_PEND
		,ISNULL(ESP_PEND.CuotasVenc,0) AS CUOTAS_ORDINARIAS_VENCIDAS
		,EXT_CAN.Cuota_Nro AS NROEXT_CANC
		,EXT_CAN.MaxFechaVenc AS FECHAVENC_NROEXT
		,ISNULL(EXT_CAN.MaxFechaComprobante,CA_EXT.UltimoPago_Fecha) AS FECHACANC_NROEXT
		,DATEDIFF(MONTH,DATEFROMPARTS(YEAR(ISNULL(EXT_CAN.MaxFechaComprobante,CA_EXT.UltimoPago_Fecha)),MONTH(ISNULL(EXT_CAN.MaxFechaComprobante,CA_EXT.UltimoPago_Fecha)),1),DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1))-1
			AS MESES_NO_PAGOEXT
		,ISNULL(EXT_PEND.Cuota_Nro,EXT_PEND_NO_VENC.Cuota_Nro) EXT_PEND
		,ISNULL(EXT_PEND.MinFechaVenc,EXT_PEND_NO_VENC.MinFechaVenc) AS FECHAVENC_EXT_PEND
		,ISNULL(EXT_PEND.CuotasVenc,0) AS CUOTAS_EXT_VENC
		,NULL AS PLAN_AMIGO

--- CANT APORTES
		
		,CASE WHEN (C_TIPO.Contrato_Tipo_Nombre = '20 Años'  or C_TIPO.Contrato_Tipo_Nombre IS NULL)
				THEN ISNULL(CANT_AP.[20],0)
				WHEN C_TIPO.Contrato_Tipo_Nombre = '10 Años'
				THEN ISNULL(CANT_AP.[18],0)
				WHEN C_TIPO.Contrato_Tipo_Nombre = '5 Años'
				THEN ISNULL(CANT_AP.[16],0)+ISNULL(CANT_AP.[14256],0)+ISNULL(CANT_AP.[14258],0)+ISNULL(CANT_AP.[14260],0)+ISNULL(CANT_AP.[14262],0)
				WHEN C_TIPO.Contrato_Tipo_Nombre = '5U'
				THEN ISNULL(CANT_AP.[392],0)
		END AS CANT_ESPERA 
		,ISNULL(CANT_AP.[24],0) AS CANT_POSPURA
		,ISNULL(CANT_AP.[25],0)+ISNULL(CANT_AP.[397],0) AS CANT_POSESION
		,ISNULL(CANT_AP.[16],0)+ISNULL(CANT_AP.[18],0)+ISNULL(CANT_AP.[20],0)
		+ISNULL(CANT_AP.[24],0)+ISNULL(CANT_AP.[25],0)+ISNULL(CANT_AP.[392],0)+ISNULL(CANT_AP.[397],0)
		+ISNULL(CANT_AP.[14256],0)+ISNULL(CANT_AP.[14258],0)+ISNULL(CANT_AP.[14260],0)+ISNULL(CANT_AP.[14262],0)
		AS TOTAL_APORTES
		,ISNULL(CANT_AP.[21],0)+ISNULL(CANT_AP.[22],0)+ISNULL(CANT_AP.[393],0)+ISNULL(CANT_AP.[394],0) AS CANT_EXT

--- MONTO APORTES
		
		,CASE WHEN (C_TIPO.Contrato_Tipo_Nombre = '20 Años'  or C_TIPO.Contrato_Tipo_Nombre IS NULL)
				THEN ISNULL(MONTO_AP.[20],0)
				WHEN C_TIPO.Contrato_Tipo_Nombre = '10 Años'
				THEN ISNULL(MONTO_AP.[18],0)
				WHEN C_TIPO.Contrato_Tipo_Nombre = '5 Años'
				THEN ISNULL(MONTO_AP.[16],0)+ISNULL(MONTO_AP.[14256],0)+ISNULL(MONTO_AP.[14258],0)+ISNULL(MONTO_AP.[14260],0)+ISNULL(MONTO_AP.[14262],0)
				WHEN C_TIPO.Contrato_Tipo_Nombre = '5U'
				THEN ISNULL(MONTO_AP.[392],0)
		END AS ABDONADO_ESPERA 
		,ISNULL(MONTO_AP.[24],0) AS ABONADO_POSPURA
		,ISNULL(MONTO_AP.[25],0)+ISNULL(MONTO_AP.[397],0) AS ABONADO_POSESION
		,ISNULL(MONTO_AP.[16],0)+ISNULL(MONTO_AP.[18],0)+ISNULL(MONTO_AP.[20],0)
			    +ISNULL(MONTO_AP.[24],0)+ISNULL(MONTO_AP.[25],0)+ISNULL(MONTO_AP.[392],0)+ISNULL(MONTO_AP.[397],0)
			    +ISNULL(MONTO_AP.[14256],0)+ISNULL(MONTO_AP.[14258],0)+ISNULL(MONTO_AP.[14260],0)+ISNULL(MONTO_AP.[14262],0)
		AS ABONADOS_APORTES
		,ISNULL(MONTO_AP.[21],0)+ISNULL(MONTO_AP.[22],0)+ISNULL(MONTO_AP.[393],0)+ISNULL(MONTO_AP.[394],0) AS ABONADO_EXT

--TIPO PAGO MINIMO

		,PagMin.TipoPagMin PAG_MIN

--ESTADO DE PAGO ORDINARIOS

		,IIF((ISNULL(ESP_PEND.CuotasVenc,0))>0,'ATRASADO','AL DIA') AS ESTADO_PAGO_ORDINARIOS
		
--ESTADO DE PAGO EXTRAORDINARIOS

		,CASE WHEN CAST(C.Fecha_Ingreso AS DATE) < '2009-05-11'
				THEN 'NO'
				WHEN ISNULL(MONTO_AP.[21],0)+ISNULL(MONTO_AP.[22],0)+ISNULL(MONTO_AP.[393],0)+ISNULL(MONTO_AP.[394],0) > 39
				THEN 'CANCELADO'
				ELSE 'CORRESPONDE'
		END ABONA_EXT
		,IIF((ISNULL(EXT_PEND.CuotasVenc,0))>0,'ATRASADO','AL DIA') AS ESTADO_PAGO_EXT


		FROM Contrato C
		LEFT JOIN Contrato_Tipo C_TIPO ON C_TIPO.Contrato_Tipo_Id = C.Contrato_Tipo_id
		LEFT JOIN Contrato_Motivo C_MOTIVO ON C_MOTIVO.Contrato_Motivo_Id = C.Contrato_Motivo_Id
		LEFT JOIN Contrato_SubEstado C_SUBEST ON C_SUBEST.Contrato_SubEstado_Id = C.Contrato_SubEstado_Id
		LEFT JOIN Contrato_Modo_Ingreso MODO_INGRESO ON MODO_INGRESO.Contrato_Modo_Ingreso_Id = C.Contrato_Modo_Ingreso_Id
		LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
		LEFT JOIN Cliente CLI ON CLI.Cliente_Id = C.Cliente_Id
		LEFT JOIN Cliente_Contacto CLICON ON CLICON.Cliente_Id = CLI.Cliente_Id
		LEFT JOIN Asesor ASE_CIERRE ON ASE_CIERRE.Asesor_Id = C.Asesor_Cierre_Id
		LEFT JOIN Asesor ASE_ACT ON ASE_ACT.Asesor_Id = C.Asesor_Actual_Id
		LEFT JOIN Ejecutivo_Cuenta ATC ON ATC.Ejecutivo_Cuenta_Id = CLI.Ejecutivo_Cuenta_Id
		LEFT JOIN Sucursal AS SUC ON C.Sucursal_Id = SUC.Sucursal_Id
		LEFT JOIN Contrato_Articulo CA_ESP ON CA_ESP.Contrato_Id = C.Contrato_Id AND CA_ESP.Articulo_Id IN (16,18,20,25,392,397,14256,14258,14260,14262) --ARTICULOS DE ESPERA ORDINARIOS
		--LEFT JOIN Contrato_Articulo CA_POSE ON CA_POSE.Contrato_Id = C.Contrato_Id AND CA_POSE.Articulo_Id IN (25,397) --ARTICULOS DE POSESION ORDINARIOS
		LEFT JOIN Contrato_Articulo CA_EXT ON CA_EXT.Contrato_Id = C.Contrato_Id AND CA_EXT.Articulo_Id IN (21,22,393,394) --ARTICULOS DE EXTRAORDINARIOS

		--LEFT JOIN CON METODOS DE PAGO
		--ESPERA
		LEFT JOIN (SELECT	CA_PAGMET.Contrato_Articulo_Id
							,CASE WHEN COUNT(*) OVER (PARTITION BY CA_PAGMET.Contrato_Articulo_Id) > 1
							THEN 'COMBINADO' ELSE PAG_MET.Pago_Metodo_Nombre END AS 'Pago_Metodo_Nombre'
							,CASE WHEN COUNT(*) OVER (PARTITION BY CA_PAGMET.Contrato_Articulo_Id) > 1
							THEN 'COMBINADO' ELSE ENT_PRO.Entidad_Procesadora_Nombre END AS 'Entidad_Procesadora_Nombre'
							,CASE WHEN COUNT(*) OVER (PARTITION BY CA_PAGMET.Contrato_Articulo_Id) > 1
							THEN '' ELSE TARTIPO.Tarjeta_Tipo_Nombre END AS 'Tarjeta_Tipo'
					FROM 
					Contrato_Articulo_PagoMetodo CA_PAGMET
					LEFT JOIN Pago_Metodo PAG_MET ON PAG_MET.Pago_Metodo_Id = CA_PAGMET.Pago_Metodo_Id
					LEFT JOIN Cliente_Tarjeta CLITA ON CLITA.Cliente_Tarjeta_Id = CA_PAGMET.Cliente_Tarjeta_Id
					LEFT JOIN Tarjeta_Tipo TARTIPO ON TARTIPO.Tarjeta_Tipo_Id = CLITA.Tarjeta_Tipo_Id
					LEFT JOIN Entidad_Procesadora ENT_PRO ON ENT_PRO.Entidad_Procesadora_Id = CLITA.Entidad_Procesadora_Id
					LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CA_PAGMET.Contrato_Articulo_Id
					WHERE CA_PAGMET.Contrato_Articulo_Instruccion_Id IS NULL AND CA.Articulo_Id IN (16,18,20,25,392,397,14256,14258,14260,14262)
					GROUP BY CA_PAGMET.Contrato_Articulo_Id, PAG_MET.Pago_Metodo_Nombre, ENT_PRO.Entidad_Procesadora_Nombre, TARTIPO.Tarjeta_Tipo_Nombre) PAGMET_ESP
					ON PAGMET_ESP.Contrato_Articulo_Id = CA_ESP.Contrato_Articulo_Id

		----POSESION
		--LEFT JOIN (SELECT	CA_PAGMET.Contrato_Articulo_Id
		--					,CASE WHEN COUNT(*) OVER (PARTITION BY CA_PAGMET.Contrato_Articulo_Id) > 1
		--					THEN 'COMBINADO' ELSE PAG_MET.Pago_Metodo_Nombre END AS 'Pago_Metodo_Nombre'
		--					,CASE WHEN COUNT(*) OVER (PARTITION BY CA_PAGMET.Contrato_Articulo_Id) > 1
		--					THEN 'COMBINADO' ELSE ENT_PRO.Entidad_Procesadora_Nombre END AS 'Entidad_Procesadora_Nombre'
		--			FROM 
		--			Contrato_Articulo_PagoMetodo CA_PAGMET
		--			LEFT JOIN Pago_Metodo PAG_MET ON PAG_MET.Pago_Metodo_Id = CA_PAGMET.Pago_Metodo_Id
		--			LEFT JOIN Cliente_Tarjeta CLITA ON CLITA.Cliente_Tarjeta_Id = CA_PAGMET.Cliente_Tarjeta_Id
		--			LEFT JOIN Entidad_Procesadora ENT_PRO ON ENT_PRO.Entidad_Procesadora_Id = CLITA.Entidad_Procesadora_Id
		--			LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CA_PAGMET.Contrato_Articulo_Id
		--			WHERE CA_PAGMET.Contrato_Articulo_Instruccion_Id IS NULL AND CA.Articulo_Id IN (25,397)
		--			GROUP BY CA_PAGMET.Contrato_Articulo_Id, PAG_MET.Pago_Metodo_Nombre, ENT_PRO.Entidad_Procesadora_Nombre) PAGMET_POSE
		--			ON PAGMET_POSE.Contrato_Articulo_Id = CA_POSE.Contrato_Articulo_Id

		--EXTRAORDINARIOS
		LEFT JOIN (SELECT	CA_PAGMET.Contrato_Articulo_Id
							,CASE WHEN COUNT(*) OVER (PARTITION BY CA_PAGMET.Contrato_Articulo_Id) > 1
							THEN 'COMBINADO' ELSE PAG_MET.Pago_Metodo_Nombre END AS 'Pago_Metodo_Nombre'
							,CASE WHEN COUNT(*) OVER (PARTITION BY CA_PAGMET.Contrato_Articulo_Id) > 1
							THEN 'COMBINADO' ELSE ENT_PRO.Entidad_Procesadora_Nombre END AS 'Entidad_Procesadora_Nombre'
					FROM 
					Contrato_Articulo_PagoMetodo CA_PAGMET
					LEFT JOIN Pago_Metodo PAG_MET ON PAG_MET.Pago_Metodo_Id = CA_PAGMET.Pago_Metodo_Id
					LEFT JOIN Cliente_Tarjeta CLITA ON CLITA.Cliente_Tarjeta_Id = CA_PAGMET.Cliente_Tarjeta_Id
					LEFT JOIN Entidad_Procesadora ENT_PRO ON ENT_PRO.Entidad_Procesadora_Id = CLITA.Entidad_Procesadora_Id
					LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CA_PAGMET.Contrato_Articulo_Id
					WHERE CA_PAGMET.Contrato_Articulo_Instruccion_Id IS NULL AND CA.Articulo_Id IN (21,22,393,394)
					GROUP BY CA_PAGMET.Contrato_Articulo_Id, PAG_MET.Pago_Metodo_Nombre, ENT_PRO.Entidad_Procesadora_Nombre) PAGMET_EXT
					ON PAGMET_EXT.Contrato_Articulo_Id = CA_EXT.Contrato_Articulo_Id

		--LEFT JOIN CON APORTES PENDIENTES VENCIDOS
		--ESPERA
		LEFT JOIN (SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MinFechaVenc, A.CuotasVenc, A.Cuota_Nro, A.Contrato_Articulo_Cuota_Id
					FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
							,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
							,CAC.Cuota_Fecha_Vencimiento
							,MIN(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MinFechaVenc
							,COUNT(CAC.Contrato_Articulo_Cuota_Id) OVER (PARTITION BY CA.Contrato_Id) CuotasVenc
							,MIN(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MIN
							,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
							FROM Contrato_Articulo_Cuota CAC
							LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
							LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
							LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
							LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
							LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
							WHERE CAC.Articulo_Id IN (16,18,20,25,392,397,14256,14258,14260,14262) AND CAC.Contrato_Articulo_Cuota_Padre_Id IS NULL AND
							(CUO_EST.Cuota_Estado_Nombre  LIKE ('%pend%') OR CUO_EST.Cuota_Estado_Nombre = 'Parcialmente Cancelada' OR CUO_EST.Cuota_Estado_Nombre = 'En Proceso de Cobro') --APORTES PENDIENTES
							AND CAC.Cuota_Fecha_Vencimiento < DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1)) A 
					WHERE A.CUOTA_MIN = A.Cuota_Nro) ESP_PEND ON ESP_PEND.Contrato_Articulo_Id = CA_ESP.Contrato_Articulo_Id
		--POSESION
		--LEFT JOIN (SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MinFechaVenc, A.CuotasVenc, A.Cuota_Nro, A.Contrato_Articulo_Cuota_Id
		--			FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
		--					,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
		--					,CAC.Cuota_Fecha_Vencimiento
		--					,MIN(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MinFechaVenc
		--					,COUNT(CAC.Contrato_Articulo_Cuota_Id) OVER (PARTITION BY CA.Contrato_Id) CuotasVenc
		--					,MIN(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MIN
		--					,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
		--					FROM Contrato_Articulo_Cuota CAC
		--					LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
		--					LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
		--					LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
		--					LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
		--					LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
		--					WHERE CAC.Articulo_Id IN (25,397) AND
		--					(CUO_EST.Cuota_Estado_Nombre  LIKE ('%pend%') OR CUO_EST.Cuota_Estado_Nombre = 'Parcialmente Cancelada' OR CUO_EST.Cuota_Estado_Nombre = 'En Proceso de Cobro') AND --APORTES PENDIENTES
		--					C_EST.Contrato_Estado_Codigo IN ('POSESIÓN') 
		--					AND CAC.Cuota_Fecha_Vencimiento < DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1)) A 
		--			WHERE A.CUOTA_MIN = A.Cuota_Nro) POSE_PEND ON POSE_PEND.Contrato_Articulo_Id = CA_POSE.Contrato_Articulo_Id

		--EXTRAORDINARIOS
		LEFT JOIN  (SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MinFechaVenc, A.CuotasVenc, A.Cuota_Nro, A.Contrato_Articulo_Cuota_Id
					FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
							,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
							,CAC.Cuota_Fecha_Vencimiento
							,MIN(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MinFechaVenc
							,COUNT(CAC.Contrato_Articulo_Cuota_Id) OVER (PARTITION BY CA.Contrato_Id) CuotasVenc
							,MIN(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MIN
							,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
							FROM Contrato_Articulo_Cuota CAC
							LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
							LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
							LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
							LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
							LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
							WHERE CAC.Articulo_Id IN (21,22,393,394) AND CAC.Contrato_Articulo_Cuota_Padre_Id IS NULL AND
							(CUO_EST.Cuota_Estado_Nombre  LIKE ('%pend%') OR CUO_EST.Cuota_Estado_Nombre = 'Parcialmente Cancelada' OR CUO_EST.Cuota_Estado_Nombre = 'En Proceso de Cobro') AND --APORTES PENDIENTES
							CAC.Cuota_Fecha_Vencimiento < DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1)) A 
					WHERE A.CUOTA_MIN = A.Cuota_Nro) EXT_PEND ON EXT_PEND.Contrato_Articulo_Id = CA_EXT.Contrato_Articulo_Id

		----LEFT JOIN CON APORTES PENDIENTES NO VENCIDOS
		----ESPERA
		LEFT JOIN (SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MinFechaVenc, A.Contrato_Articulo_Cuota_Id, A.Cuota_Nro
					FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
							,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
							,CAC.Cuota_Fecha_Vencimiento
							,MIN(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MinFechaVenc
							,MIN(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MIN
							,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
							FROM Contrato_Articulo_Cuota CAC
							LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
							LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
							LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
							LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
							LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
							WHERE CAC.Articulo_Id IN (16,18,20,25,392,397,14256,14258,14260,14262) AND CAC.Contrato_Articulo_Cuota_Padre_Id IS NULL AND
							(CUO_EST.Cuota_Estado_Nombre  LIKE ('%pend%') OR CUO_EST.Cuota_Estado_Nombre = 'Parcialmente Cancelada' OR CUO_EST.Cuota_Estado_Nombre = 'En Proceso de Cobro') --APORTES PENDIENTES
							AND CAC.Cuota_Fecha_Vencimiento > DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1)) A 
					WHERE A.CUOTA_MIN = A.Cuota_Nro) ESP_PEND_NO_VENC ON ESP_PEND_NO_VENC.Contrato_Articulo_Id = CA_ESP.Contrato_Articulo_Id
		----POSESION
		--LEFT JOIN (SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MinFechaVenc, A.Contrato_Articulo_Cuota_Id, A.Cuota_Nro
		--			FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
		--					,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
		--					,CAC.Cuota_Fecha_Vencimiento
		--					,MIN(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MinFechaVenc
		--					,MIN(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MIN
		--					,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
		--					FROM Contrato_Articulo_Cuota CAC
		--					LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
		--					LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
		--					LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
		--					LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
		--					LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
		--					WHERE CAC.Articulo_Id IN (25,397) AND
		--					(CUO_EST.Cuota_Estado_Nombre  LIKE ('%pend%') OR CUO_EST.Cuota_Estado_Nombre = 'Parcialmente Cancelada' OR CUO_EST.Cuota_Estado_Nombre = 'En Proceso de Cobro') AND --APORTES PENDIENTES
		--					C_EST.Contrato_Estado_Codigo IN ('POSESIÓN') 
		--					AND CAC.Cuota_Fecha_Vencimiento > DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1)) A 
		--			WHERE A.CUOTA_MIN = A.Cuota_Nro) POSE_PEND_NO_VENC ON POSE_PEND_NO_VENC.Contrato_Articulo_Id = CA_POSE.Contrato_Articulo_Id

		--EXTRAORDINARIOS

		LEFT JOIN  (SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MinFechaVenc, A.Contrato_Articulo_Cuota_Id, A.Cuota_Nro
					FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
							,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
							,CAC.Cuota_Fecha_Vencimiento
							,MIN(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MinFechaVenc
							,MIN(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MIN
							,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
							FROM Contrato_Articulo_Cuota CAC
							LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
							LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
							LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
							LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
							LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
							WHERE CAC.Articulo_Id IN (21,22,393,394) AND CAC.Contrato_Articulo_Cuota_Padre_Id IS NULL AND
							(CUO_EST.Cuota_Estado_Nombre  LIKE ('%pend%') OR CUO_EST.Cuota_Estado_Nombre = 'Parcialmente Cancelada' OR CUO_EST.Cuota_Estado_Nombre = 'En Proceso de Cobro') AND --APORTES PENDIENTES
							CAC.Cuota_Fecha_Vencimiento > DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1)) A 
					WHERE A.CUOTA_MIN = A.Cuota_Nro) EXT_PEND_NO_VENC ON EXT_PEND_NO_VENC.Contrato_Articulo_Id = CA_EXT.Contrato_Articulo_Id

		----LEFT JOIN CON APORTES CANCELADOS
		----ESPERA

		LEFT JOIN	(SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MaxFechaVenc, A.Contrato_Articulo_Cuota_Id, A.Cuota_Nro, MaxFechaComprobante
					FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
							,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
							,CAC.Cuota_Fecha_Vencimiento
							,MAX(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MaxFechaVenc
							,MAX(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MAX
							,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
							FROM Contrato_Articulo_Cuota CAC
							LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
							LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
							LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
							LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
							LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
							WHERE CAC.Articulo_Id IN (16,18,20,25,392,397,14256,14258,14260,14262) AND (CAC.Cuota_Estado_Id IN (5,6,7,8))) A --APORTES CANCELADOS
					LEFT JOIN (SELECT  DISTINCT(CAC.Contrato_Articulo_Cuota_Id)
							,MAX(CAST(COM.Comprobante_Fecha AS DATE)) OVER (PARTITION BY CAC.Contrato_Articulo_Id) MaxFechaComprobante
							FROM
							Comprobante COM
							JOIN Comprobante_Pago COM_PAG ON COM.Comprobante_Id = COM_PAG.Comprobante_Id
							JOIN Contrato_Articulo_Cuota CAC ON CAC.Contrato_Articulo_Cuota_Id = COM_PAG.Contrato_Articulo_Cuota_Id
							WHERE CAST(COM.Comprobante_Fecha AS DATE) > '2022-09-10' AND COM.Comprobante_Tipo_Id IN (1,2) AND COM.Comprobante_Estado_Id in (4,8) AND
							COM_PAG.Contrato_Articulo_Cuota_Id IS NOT NULL) MaxFechaCom
							ON MaxFechaCom.Contrato_Articulo_Cuota_Id = A.Contrato_Articulo_Cuota_Id
					WHERE A.CUOTA_MAX = A.Cuota_Nro) ESP_CAN
					ON ESP_CAN.Contrato_Articulo_Id = CA_ESP.Contrato_Articulo_Id

		--POSESION

		--LEFT JOIN	(SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MaxFechaVenc, A.Contrato_Articulo_Cuota_Id, A.Cuota_Nro, MaxFechaComprobante
		--			FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
		--					,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
		--					,CAC.Cuota_Fecha_Vencimiento
		--					,MAX(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MaxFechaVenc
		--					,MAX(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MAX
		--					,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
		--					FROM Contrato_Articulo_Cuota CAC
		--					LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
		--					LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
		--					LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
		--					LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
		--					LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
		--					WHERE CAC.Articulo_Id IN (25,397) AND (CAC.Cuota_Estado_Id IN (5,6,7,8)) AND --APORTES CANCELADOS
		--					C_EST.Contrato_Estado_Codigo IN ('POSESIÓN')) A
		--			LEFT JOIN (SELECT  DISTINCT(CAC.Contrato_Articulo_Cuota_Id)
		--					,MAX(CAST(COM.Comprobante_Fecha AS DATE)) OVER (PARTITION BY CAC.Contrato_Articulo_Id) MaxFechaComprobante
		--					FROM
		--					Comprobante COM
		--					JOIN Comprobante_Pago COM_PAG ON COM.Comprobante_Id = COM_PAG.Comprobante_Id
		--					JOIN Contrato_Articulo_Cuota CAC ON CAC.Contrato_Articulo_Cuota_Id = COM_PAG.Contrato_Articulo_Cuota_Id
		--					WHERE CAST(COM.Comprobante_Fecha AS DATE) > '2022-09-10' AND COM.Comprobante_Tipo_Id IN (1,2) AND
		--					COM_PAG.Contrato_Articulo_Cuota_Id IS NOT NULL) MaxFechaCom
		--					ON MaxFechaCom.Contrato_Articulo_Cuota_Id = A.Contrato_Articulo_Cuota_Id
		--			WHERE A.CUOTA_MAX = A.Cuota_Nro) POSE_CAN
		--			ON POSE_CAN.Contrato_Articulo_Id = CA_POSE.Contrato_Articulo_Id

		--EXTRAORDINARIOS

		LEFT JOIN	(SELECT DISTINCT A.Contrato_Articulo_Id, a.Contrato_Numero, A.MaxFechaVenc, A.Contrato_Articulo_Cuota_Id, A.Cuota_Nro, MaxFechaComprobante
					FROM (SELECT CA.Contrato_Id, CA.Contrato_Articulo_Id, CA.Articulo_Id, A.Articulo_Codigo
							,CAC.Contrato_Articulo_Cuota_Id, C.Contrato_Numero, C_EST.Contrato_Estado_Nombre, CAC.Cuota_Nro
							,CAC.Cuota_Fecha_Vencimiento
							,MAX(CAC.Cuota_Fecha_Vencimiento) OVER (PARTITION BY CA.Contrato_Id) MaxFechaVenc
							,MAX(CAC.Cuota_Nro) OVER (PARTITION BY CA.Contrato_Id) CUOTA_MAX
							,CAST(CAC.Cuota_EstaFraccionada AS INT) Cuota_EstaFraccionada
							FROM Contrato_Articulo_Cuota CAC
							LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
							LEFT JOIN Articulo A ON A.Articulo_Id = CA.Articulo_Id
							LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = CAC.Cuota_Estado_Id
							LEFT JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
							LEFT JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
							WHERE CAC.Articulo_Id IN (21,22,393,394) AND (CAC.Cuota_Estado_Id IN (5,6,7,8))) A
					LEFT JOIN (SELECT  DISTINCT(CAC.Contrato_Articulo_Cuota_Id)
							,MAX(CAST(COM.Comprobante_Fecha AS DATE)) OVER (PARTITION BY CAC.Contrato_Articulo_Id) MaxFechaComprobante
							FROM
							Comprobante COM
							JOIN Comprobante_Pago COM_PAG ON COM.Comprobante_Id = COM_PAG.Comprobante_Id
							JOIN Contrato_Articulo_Cuota CAC ON CAC.Contrato_Articulo_Cuota_Id = COM_PAG.Contrato_Articulo_Cuota_Id
							WHERE CAST(COM.Comprobante_Fecha AS DATE) > '2022-09-10' AND COM.Comprobante_Tipo_Id IN (1,2) AND COM.Comprobante_Estado_Id in (4,8) AND
							COM_PAG.Contrato_Articulo_Cuota_Id IS NOT NULL) MaxFechaCom
							ON MaxFechaCom.Contrato_Articulo_Cuota_Id = A.Contrato_Articulo_Cuota_Id
					WHERE A.CUOTA_MAX = A.Cuota_Nro) EXT_CAN
					ON EXT_CAN.Contrato_Articulo_Id = CA_EXT.Contrato_Articulo_Id

		--CONTAMOS LOS APORTES

		LEFT JOIN (SELECT Contrato_Id AS Contrato_Id, [16],[18],[20],[21],[22],[24],[25],[392],[393],[394],[397],[14256],[14258],[14260],[14262]
					FROM (SELECT CA.Contrato_Id, CAC.Articulo_Id, COUNT(1) CONTAR 
							FROM Contrato_Articulo_Cuota CAC
							LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
							WHERE CAC.Articulo_Id IN (16,18,20,21,22,24,25,392,397,14256,14258,14260,14262) AND CAC.Cuota_Estado_Id IN (5,7) AND CAC.Contrato_Articulo_Cuota_Padre_Id IS NULL
							GROUP BY CA.Contrato_Id, CAC.Articulo_Id) CANT_AP
					PIVOT (SUM(CONTAR) FOR CANT_AP.Articulo_Id IN ([16],[18],[20],[21],[22],[24],[25],[392],[393],[394],[397],[14256],[14258],[14260],[14262])) AS PIVOT_TABLE) AS CANT_AP
					ON CANT_AP.Contrato_Id = C.Contrato_Id

		--SUMAMOS LOS MONTOS DE APORTES

		LEFT JOIN (SELECT Contrato_Id AS Contrato_Id, [16],[18],[20],[21],[22],[24],[25],[392],[393],[394],[397],[14256],[14258],[14260],[14262]
					FROM (SELECT CA.Contrato_Id, CAC.Articulo_Id, (SUM(ISNULL(CAC.Cuota_Monto,0)) - SUM(ISNULL(CAC.Cuota_Monto_Descuento,0))) MONTO
							FROM Contrato_Articulo_Cuota CAC
							LEFT JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
							WHERE CAC.Articulo_Id IN (16,18,20,21,22,24,25,392,397,14256,14258,14260,14262) AND CAC.Cuota_Estado_Id IN (5,7) AND CAC.Contrato_Articulo_Cuota_Padre_Id IS NULL
							GROUP BY CA.Contrato_Id, CAC.Articulo_Id) CANT_AP
					PIVOT (SUM(MONTO) FOR CANT_AP.Articulo_Id IN ([16],[18],[20],[21],[22],[24],[25],[392],[393],[394],[397],[14256],[14258],[14260],[14262])) AS PIVOT_TABLE) AS MONTO_AP
					ON MONTO_AP.Contrato_Id = C.Contrato_Id

		--DIRECCIONES DE CLIENTES

		LEFT JOIN       (SELECT cliente.Cliente_Id, dir.Direccion, ciudad.Direccion_Ciudad_Nombre
							FROM    dbo.Cliente cliente JOIN
										dbo.cliente_direccion AS cli_dir ON cliente.cliente_id = cli_dir.cliente_id JOIN
										dbo.direccion AS dir ON cli_dir.direccion_id = dir.direccion_id JOIN
										dbo.direccion_tipo AS dir_tipo ON dir.direccion_tipo_id = dir_tipo.direccion_tipo_id JOIN
										dbo.direccion_ciudad AS ciudad ON dir.direccion_ciudad_id = ciudad.direccion_ciudad_id
							WHERE dir_tipo.direccion_tipo_codigo LIKE 'PRIN') AS direccion ON cli.Cliente_Id = direccion.Cliente_Id

		--ANEXAMOS PAGOS MINIMOS

LEFT JOIN   (SELECT DISTINCT Contrato_Id, TipoPagMin
			FROM
			(SELECT * FROM (SELECT CA.Contrato_Articulo_Id, C.Contrato_Numero, C.Contrato_Id, C_EST.Contrato_Estado_Codigo, CAC.Contrato_Articulo_Cuota_Id, CAC.Cuota_Nro, Cuota_Estado_Id
								,MIN(CAC.Contrato_Articulo_Cuota_Id) OVER (PARTITION BY CA.Contrato_Articulo_Id) IdMin
								,CASE WHEN PagosMinimos.CONTAR = 2 THEN 'PagMin50%'
										WHEN PagosMinimos.CONTAR = 4 THEN 'PagMin25%'
										ELSE 'VER' END AS TipoPagMin
								FROM Contrato_Articulo_Cuota CAC
								JOIN (SELECT CAC.Contrato_Articulo_Cuota_Padre_Id, COUNT(*) CONTAR
											FROM Contrato_Articulo_Cuota CAC
											WHERE CAC.Contrato_Articulo_Cuota_Padre_Id IS NOT NULL AND
											CAC.Articulo_Id IN (20,25,392,397)
											GROUP BY CAC.Contrato_Articulo_Cuota_Padre_Id ) PagosMinimos
								ON PagosMinimos.Contrato_Articulo_Cuota_Padre_Id = CAC.Contrato_Articulo_Cuota_Id
								JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
								JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
								JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
								) A
			UNION 
			SELECT * FROM (SELECT CA.Contrato_Articulo_Id, C.Contrato_Numero,  C.Contrato_Id, C_EST.Contrato_Estado_Codigo, CAC.Contrato_Articulo_Cuota_Id, CAC.Cuota_Nro, Cuota_Estado_Id
								,MIN(CAC.Contrato_Articulo_Cuota_Id) OVER (PARTITION BY CA.Contrato_Articulo_Id) IdMin
								,CASE WHEN PagosMinimos.CONTAR = 2 THEN 'PagMin50%'
										WHEN PagosMinimos.CONTAR = 4 THEN 'PagMin25%'
										ELSE 'VER' END AS TipoPagMin
								FROM Contrato_Articulo_Cuota CAC
								JOIN (SELECT CAC.Contrato_Articulo_Cuota_Padre_Id, COUNT(*) CONTAR
											FROM Contrato_Articulo_Cuota CAC
											WHERE CAC.Contrato_Articulo_Cuota_Padre_Id IS NOT NULL AND
											CAC.Articulo_Id IN (20,25,392,397)
											GROUP BY CAC.Contrato_Articulo_Cuota_Padre_Id ) PagosMinimos
								ON PagosMinimos.Contrato_Articulo_Cuota_Padre_Id = CAC.Contrato_Articulo_Cuota_Id
								JOIN Contrato_Articulo CA ON CA.Contrato_Articulo_Id = CAC.Contrato_Articulo_Id
								JOIN Contrato C ON C.Contrato_Id = CA.Contrato_Id
								JOIN Contrato_Estado C_EST ON C_EST.Contrato_Estado_Id = C.Contrato_Estado_Id
								) B) C
			LEFT JOIN Cuota_Estado CUO_EST ON CUO_EST.Cuota_Estado_Id = C.Cuota_Estado_Id
			WHERE CUO_EST.Cuota_Estado_Nombre  LIKE ('%pend%') OR CUO_EST.Cuota_Estado_Nombre = 'Parcialmente Cancelada' OR CUO_EST.Cuota_Estado_Nombre = 'En Proceso de Cobro'
			GROUP BY Contrato_Id, TipoPagMin) PagMin
		ON PagMin.Contrato_Id = C.Contrato_Id


		--WHERE C_EST.Contrato_Estado_Codigo  IN ('ACTIVO', 'GESTION', 'ADJUDICADO', 'POSESIÓN', 'INACTIVO') -- AND 
		WHERE C.Contrato_Numero NOT LIKE ('%-B%') and C.Contrato_Numero NOT LIKE ('%A%') and C.Contrato_Numero NOT LIKE ('%N%')
		and C.Contrato_Numero NOT LIKE ('%P%')and C.Contrato_Numero NOT LIKE ('%S%')) A


--ORDER BY 3 desc --60 ASC"

#cartera completa----
contratos = dbGetQuery(core, query) %>% 
  distinct() %>% 
  tibble()

#cartera del dataware-----
contratos_dw = tbl(dw, in_schema("dw_inteligencia_comercial", "cartera_completa")) %>% 
  group_by(CONTRATO) %>% 
  filter(Fecha_Actualizacion == max(Fecha_Actualizacion)) %>% 
  ungroup %>% 
  collect()

#diferencia entre ambos----
diferencia = contratos %>% 
  setdiff(contratos_dw %>% 
            select(-Fecha_Actualizacion))

#escribir diferencia en dataware
tbl(dw, in_schema("dw_inteligencia_comercial", "cartera_completa")) %>% 
  rows_append(diferencia %>% 
                mutate(Fecha_Actualizacion = lubridate::force_tz(Sys.time(), "UTC")),
              copy = T,
              in_place = T)

#cerrar conexiones
dbDisconnect(core)
dbDisconnect(dw)
