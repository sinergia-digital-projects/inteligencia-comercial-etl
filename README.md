# Proyecto Sinergia Digital - ETLs de Cartera y Cobranzas

Este repositorio contiene los scripts R para los procesos de Extracción, Transformación y Carga (ETL) relacionados con la gestión de cartera y el cálculo de indicadores de cobranzas para Sinergia Digital.

## 🚀 Visión General

El objetivo de este proyecto es mantener actualizadas las tablas analíticas en el Data Warehouse (DW) de Kizoft, extrayendo datos del sistema transaccional (dbFortalezaCore) y transformándolos para su uso en reportes y análisis de negocio.

Actualmente, el proyecto consta de tres ETLs principales:

1. **popular tabla cartera dataware.R**: Procesa la información básica de los contratos activos y la carga en una tabla histórica en el DW.

2. **popular tabla cartera completa dataware.R**: Genera una vista detallada y enriquecida de la cartera, incluyendo métricas de aportes, estados de pago y un modelo de riesgo de baja, cargándola también en el DW.

3. **CMI Cobranzas.R**: Calcula indicadores clave de rendimiento (KPIs) de cobranzas (cantidad de cuotas, cantidad de contratos, monto) a lo largo del tiempo, utilizando el histórico de cartera y los detalles de los aportes.

## 🛠️ Estructura del Proyecto

```
.
├── CMI Cobranzas.R
├── popular tabla cartera completa dataware.R
├── popular tabla cartera dataware.R
└── README.md
```

## 📊 Flujo de Datos (Dependencias)

Los scripts tienen las siguientes dependencias:

* **popular tabla cartera dataware.R**:

  * Origen: `dbFortalezaCore` (SQL Server)

  * Destino: `dw_inteligencia_comercial.cartera` (MySQL)

  * Función: Crea un registro histórico de los estados básicos de los contratos.

* **popular tabla cartera completa dataware.R**:

  * Origen: `dbFortalezaCore` (SQL Server)

  * Destino: `dw_inteligencia_comercial.cartera_completa` (MySQL)

  * Función: Genera una tabla de hechos detallada de la cartera con múltiples dimensiones y métricas.

* **CMI Cobranzas.R**:

  * Origen: `dbFortalezaCore` (SQL Server) y `dw_inteligencia_comercial.cartera` (MySQL)

  * Destino: `dw_inteligencia_comercial.compilado_cantidad`, `dw_inteligencia_comercial.compilado_contratos`, `dw_inteligencia_comercial.compilado_monto`, `dw_inteligencia_comercial.cumplimiento_acumulado`, `dw_inteligencia_comercial.cumplimiento_acumulado_monto` (MySQL)

  * Función: Calcula y almacena KPIs de cobranzas. Depende directamente de la ejecución previa de `popular tabla cartera dataware.R` para obtener el histórico de cartera.

Orden de Ejecución Recomendado:

1. `popular tabla cartera dataware.R`

2. `popular tabla cartera completa dataware.R`

3. `CMI Cobranzas.R`

## ⚙️ Configuración y Requisitos

### Requisitos de R

Asegúrate de tener instaladas las siguientes librerías de R:

* `odbc`

* `tidyverse`

* `dbplyr`

* `lubridate`

Puedes instalarlas usando:

```r
install.packages(c("odbc", "tidyverse", "dbplyr", "lubridate"))
```

### Conexión a Bases de Datos

Los scripts se conectan a dos bases de datos:

* SQL Server (dbFortalezaCore): Servidor `10.112.254.11`, Base de datos `dbFortalezaCore`.

* MySQL (Data Warehouse - dw_inteligencia_comercial): Servidor `10.112.254.7`, Base de datos `dw` (para `popular tabla cartera dataware.R`) y `dw_inteligencia_comercial` (para `CMI Cobranzas.R` y `popular tabla cartera completa dataware.R`).

Credenciales:

Las credenciales de usuario (`UID`) y contraseña (`PWD`) están ofuscadas en los scripts (`****`). Es crucial que estas credenciales se manejen de forma segura y no se suban directamente al repositorio de GitHub. Se recomienda utilizar variables de entorno o un sistema de gestión de secretos para inyectar estas credenciales en tiempo de ejecución.

### Artículos y Estados

Los scripts hacen referencia a `Articulo_Id` y `Cuota_Estado_Id` específicos. Asegúrate de que estos IDs correspondan a los valores correctos en tu base de datos `dbFortalezaCore`.

## ⚠️ Consideraciones Importantes y Mejoras Futuras

* Manejo de Credenciales: Implementar un método seguro para la gestión de credenciales (ej. variables de entorno, HashiCorp Vault, etc.).

* Consistencia de Lógica: Revisar y unificar las listas de `Articulo_Id` y la lógica para determinar el método de pago "vigente" (`Contrato_Articulo_Instruccion_Id IS NULL` vs `IS NOT NULL`) a través de todos los scripts.

* Idempotencia y Crecimiento de Tablas:

  * Para `cartera` y `cartera_completa`: Considerar implementar lógica de `UPSERT` (INSERT ... ON DUPLICATE KEY UPDATE en MySQL) en lugar de `rows_append` con `setdiff` para manejar actualizaciones de registros existentes de manera más eficiente y evitar duplicados innecesarios si un registro cambia mínimamente.

  * Para las tablas `compilado_*` y `cumplimiento_*`: Si se desea mantener un histórico de estos KPIs, se debería modificar la lógica de `overwrite = TRUE` para insertar nuevos registros por mes o implementar un `UPSERT` basado en la columna `Mes`.

* Rango de Fechas Dinámico: En `CMI Cobranzas.R`, el rango de meses para `complete()` está hardcodeado (`2023-01-01` a `2023-12-01`). Debería ser dinámico, por ejemplo, desde el inicio del año actual o el mes más antiguo disponible en los datos.

* Optimización de Consultas: Para grandes volúmenes de datos, evaluar la posibilidad de empujar más transformaciones a la base de datos (usando `dbplyr` o SQL puro) antes de `collect()` para reducir la transferencia de datos a R. Asegurar que las tablas de origen y destino tengan los índices adecuados para optimizar las `JOIN` y `WHERE` cláusulas.

* Manejo de Errores: Añadir bloques `tryCatch` para una gestión robusta de errores y asegurar que las conexiones a la base de datos se cierren correctamente incluso si el script falla.

* Logging: Implementar un sistema de logging para registrar el inicio/fin de cada ETL, errores, y métricas de rendimiento.

* Pruebas Unitarias: Para la lógica de negocio crítica (ej. cálculo de `RIESGO_BAJA`, clasificación de aportes), considerar escribir pruebas unitarias para asegurar la consistencia y corrección de los cálculos.

## 🤝 Contribuciones

¡Las contribuciones son bienvenidas! Si tienes sugerencias para mejorar estos ETLs, por favor, abre un "issue" o envía un "pull request".

---

Sinergia Digital Data Team