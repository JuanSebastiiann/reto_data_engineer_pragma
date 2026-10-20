@Juan Sebastian Hurtado Serna


## 1. Tecnologías utilizadas

- **Python:** procesamiento de los archivos y ejecución de la lógica del pipeline.
- **PostgreSQL:** almacenamiento de las transacciones y las estadísticas.
- **Psycopg:** conexión y ejecución de operaciones sobre la base de datos.
- **CSV:** formato de los archivos de entrada.

---

## 2. Creación de la base de datos

Es necesario ejecutar los siguientes scripts en PostgreSQL para crear las tablas utilizadas en el proceso.

### 2.1. Tabla `public.pipeline_stats`

```sql
-- Table: public.pipeline_stats

-- DROP TABLE IF EXISTS public.pipeline_stats;

CREATE TABLE IF NOT EXISTS public.pipeline_stats
(
    id integer NOT NULL,
    row_count bigint NOT NULL DEFAULT 0,
    price_sum numeric(30,4) NOT NULL DEFAULT 0,
    price_min numeric(18,4),
    price_max numeric(18,4),
    price_count bigint NOT NULL DEFAULT 0,
    CONSTRAINT pipeline_stats_pkey PRIMARY KEY (id),
    CONSTRAINT pipeline_stats_id_check CHECK (id = 1)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.pipeline_stats
    OWNER TO postgres;
```

### 2.2. Tabla `public.transactions`

```sql
-- Table: public.transactions

-- DROP TABLE IF EXISTS public.transactions;

CREATE TABLE IF NOT EXISTS public.transactions
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY
        (INCREMENT 1 START 1 MINVALUE 1
         MAXVALUE 9223372036854775807 CACHE 1),
    "timestamp" timestamp without time zone NOT NULL,
    price numeric(18,4),
    user_id bigint NOT NULL,
    source_file text COLLATE pg_catalog."default" NOT NULL,
    source_row_number integer,
    CONSTRAINT transactions_pkey PRIMARY KEY (id)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.transactions
    OWNER TO postgres;
```

### 2.3. Índice único para el control de duplicados

```sql
-- Index: uq_transactions_source_row

-- DROP INDEX IF EXISTS public.uq_transactions_source_row;

CREATE UNIQUE INDEX IF NOT EXISTS uq_transactions_source_row
    ON public.transactions USING btree
    (
        source_file COLLATE pg_catalog."default" ASC NULLS LAST,
        source_row_number ASC NULLS LAST
    );
```

---

## 3. Modelo de datos

La solución utiliza dos tablas en PostgreSQL.

### 3.1. Tabla `public.transactions`

Almacena las transacciones leídas desde los archivos CSV.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | BIGINT | Identificador autogenerado de la transacción (PK). |
| `timestamp` | TIMESTAMP | Fecha de la transacción. |
| `price` | NUMERIC(18,4) | Precio de la transacción; admite valores nulos. |
| `user_id` | BIGINT | Identificador del usuario. |
| `source_file` | TEXT | Nombre del archivo de origen. |
| `source_row_number` | INTEGER | Posición de la fila dentro del archivo. |

La tabla utiliza `id` como clave primaria. Además, dispone de un índice único sobre `source_file` y `source_row_number`, que permite identificar registros ya procesados y evitar inserciones duplicadas.

### 3.2. Tabla `public.pipeline_stats`

Mantiene un resumen acumulado del procesamiento.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INTEGER | Identificador de la fila de estadísticas; se utiliza el valor `1`. |
| `row_count` | BIGINT | Cantidad total de transacciones insertadas. |
| `price_sum` | NUMERIC(30,4) | Suma acumulada de los precios no nulos. |
| `price_min` | NUMERIC(18,4) | Precio mínimo registrado. |
| `price_max` | NUMERIC(18,4) | Precio máximo registrado. |
| `price_count` | BIGINT | Cantidad de transacciones con precio informado. |

La tabla está diseñada para mantener una única fila de estadísticas, identificada por `id = 1`.

El precio promedio se calcula a partir de `price_sum / price_count`, siempre que `price_count` sea mayor que cero.

---

## 4. Funcionamiento del pipeline

El procesamiento sigue estas etapas:

1. **Lectura:** se leen los archivos CSV definidos en el script.
2. **Microbatches:** los registros se agrupan en lotes pequeños para procesarlos de forma controlada.
3. **Validación:** se convierten los campos a los tipos de datos requeridos y se verifica la información necesaria para la carga.
4. **Inserción:** los registros nuevos se insertan en `transactions`.
5. **Control de duplicados:** se utiliza la combinación de `source_file` y `source_row_number` para omitir los registros que ya existen.
6. **Actualización incremental:** las estadísticas se actualizan únicamente con los registros que se insertaron correctamente.
7. **Seguimiento:** se muestran los resultados de cada microbatch y el resumen final del procesamiento.

Las operaciones de inserción y actualización de estadísticas de cada microbatch se ejecutan dentro de una transacción de base de datos para mantener la consistencia de los datos.

---

## 5. Configuración de la conexión

El pipeline utiliza las siguientes variables de entorno para conectarse a PostgreSQL:

| Variable | Descripción |
|---|---|
| `PGHOST` | Servidor de PostgreSQL. |
| `PGPORT` | Puerto de conexión. |
| `PGDATABASE` | Nombre de la base de datos. |
| `PGUSER` | Usuario de conexión. |
| `PGPASSWORD` | Contraseña del usuario. |

Los valores deben corresponder a la configuración del entorno donde se ejecutará el pipeline.

---

## 6. Ejecución del pipeline

Ejecutar los siguientes comandos desde la terminal, en el directorio del proyecto.

### 6.1. Procesamiento de los archivos principales

```bash
python pipeline.py main
```

Este modo procesa los cinco archivos principales definidos en el script:

- `2012-1.csv`
- `2012-2.csv`
- `2012-3.csv`
- `2012-4.csv`
- `2012-5.csv`

### 6.2. Procesamiento del archivo de validación

Una vez completado el proceso principal, ejecutar:

```bash
python pipeline.py validation
```

Este modo procesa el archivo `validation.csv` utilizando la misma lógica de inserción, control de duplicados y actualización de estadísticas.

---

## 7. Consultas de comprobación

### 7.1. Cantidad de transacciones almacenadas

```sql
SELECT COUNT(*) AS total_transactions
FROM public.transactions;
```

### 7.2. Estadísticas calculadas directamente desde las transacciones

```sql
SELECT
    COUNT(*) AS row_count,
    COUNT(price) AS price_count,
    COALESCE(SUM(price), 0) AS price_sum,
    MIN(price) AS price_min,
    MAX(price) AS price_max,
    AVG(price) AS price_avg
FROM public.transactions;
```

Estas consultas permiten contrastar la información almacenada en `pipeline_stats` con los resultados calculados directamente desde la tabla de transacciones.

---

## 8. Resultados de ejecución

### 8.1. Procesamiento de los archivos principales

**Estadísticas antes de iniciar:**

| Métrica | Valor |
|---|---:|
| Total de filas | 0 |
| Precios válidos | 0 |
| Suma acumulada | 0.0000 |
| Promedio acumulado | `None` |
| Mínimo acumulado | `None` |
| Máximo acumulado | `None` |

#### Archivo `2012-1.csv`

**Microbatch 1**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 16 |
| Precios válidos insertados | 15 |
| Precios vacíos insertados | 1 |
| Total de filas | 16 |
| Precios válidos | 15 |
| Suma acumulada | 844.0000 |
| Promedio acumulado | 56.2666666666666667 |
| Mínimo acumulado | 14.0000 |
| Máximo acumulado | 97.0000 |

**Microbatch 2**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 6 |
| Precios válidos insertados | 5 |
| Precios vacíos insertados | 1 |
| Total de filas | 22 |
| Precios válidos | 20 |
| Suma acumulada | 1193.0000 |
| Promedio acumulado | 59.6500000000000000 |
| Mínimo acumulado | 14.0000 |
| Máximo acumulado | 97.0000 |

**Resumen del archivo**

- Microbatches procesados: 2.
- Filas nuevas insertadas: 22.

#### Archivo `2012-2.csv`

**Microbatch 1**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 16 |
| Precios válidos insertados | 16 |
| Precios vacíos insertados | 0 |
| Total de filas | 38 |
| Precios válidos | 36 |
| Suma acumulada | 2155.0000 |
| Promedio acumulado | 59.8611111111111111 |
| Mínimo acumulado | 12.0000 |
| Máximo acumulado | 100.0000 |

**Microbatch 2**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 13 |
| Precios válidos insertados | 13 |
| Precios vacíos insertados | 0 |
| Total de filas | 51 |
| Precios válidos | 49 |
| Suma acumulada | 2783.0000 |
| Promedio acumulado | 56.7959183673469388 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

**Resumen del archivo**

- Microbatches procesados: 2.
- Filas nuevas insertadas: 29.

#### Archivo `2012-3.csv`

**Microbatch 1**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 16 |
| Precios válidos insertados | 16 |
| Precios vacíos insertados | 0 |
| Total de filas | 67 |
| Precios válidos | 65 |
| Suma acumulada | 3731.0000 |
| Promedio acumulado | 57.4000000000000000 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

**Microbatch 2**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 15 |
| Precios válidos insertados | 15 |
| Precios vacíos insertados | 0 |
| Total de filas | 82 |
| Precios válidos | 80 |
| Suma acumulada | 4633.0000 |
| Promedio acumulado | 57.9125000000000000 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

**Resumen del archivo**

- Microbatches procesados: 2.
- Filas nuevas insertadas: 31.

#### Archivo `2012-4.csv`

**Microbatch 1**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 16 |
| Precios válidos insertados | 16 |
| Precios vacíos insertados | 0 |
| Total de filas | 98 |
| Precios válidos | 96 |
| Suma acumulada | 5624.0000 |
| Promedio acumulado | 58.5833333333333333 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

**Microbatch 2**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 14 |
| Precios válidos insertados | 12 |
| Precios vacíos insertados | 2 |
| Total de filas | 112 |
| Precios válidos | 108 |
| Suma acumulada | 6240.0000 |
| Promedio acumulado | 57.7777777777777778 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

**Resumen del archivo**

- Microbatches procesados: 2.
- Filas nuevas insertadas: 30.

#### Archivo `2012-5.csv`

**Microbatch 1**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 16 |
| Precios válidos insertados | 16 |
| Precios vacíos insertados | 0 |
| Total de filas | 128 |
| Precios válidos | 124 |
| Suma acumulada | 7071.0000 |
| Promedio acumulado | 57.0241935483870968 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

**Microbatch 2**

| Métrica | Resultado |
|---|---:|
| Filas nuevas insertadas | 15 |
| Precios válidos insertados | 15 |
| Total de filas | 143 |
| Precios válidos | 139 |
| Suma acumulada | 8046.0000 |
| Promedio acumulado | 57.8848920863309353 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

**Resumen del archivo**

- Microbatches procesados: 2.
- Filas nuevas insertadas: 31.

### 8.2. Estadísticas finales de los archivos principales

| Métrica | Resultado |
|---|---:|
| Total de filas | 143 |
| Precios válidos | 139 |
| Suma acumulada | 8046.0000 |
| Promedio acumulado | 57.8848920863309353 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

---

### 8.3. Estadísticas antes de ejecutar `validation.csv`

| Métrica | Resultado |
|---|---:|
| Total de filas | 143 |
| Precios válidos | 139 |
| Suma acumulada | 8046.0000 |
| Promedio acumulado | 57.8848920863309353 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

### 8.4. Procesamiento de `validation.csv`

**Microbatch 1**

| Métrica | Resultado |
|---|---:|
| Filas leídas | 8 |
| Filas nuevas insertadas | 8 |
| Precios válidos insertados | 8 |
| Precios vacíos insertados | 0 |
| Total de filas | 151 |
| Precios válidos | 147 |
| Promedio acumulado | 57.0068027210884354 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |

**Resumen del archivo**

- Microbatches procesados: 1.
- Filas leídas: 8.
- Filas nuevas insertadas: 8.
- Filas omitidas por duplicidad: 0.

El archivo `validation.csv` se cargó correctamente.

### 8.5. Estadísticas finales después de la validación

| Métrica | Resultado |
|---|---:|
| Total de filas | 151 |
| Precios válidos | 147 |
| Suma acumulada | 8380.0000 |
| Promedio acumulado | 57.0068027210884354 |
| Mínimo acumulado | 10.0000 |
| Máximo acumulado | 100.0000 |



### 9. Comparación de estadísticas antes y después de procesar `validation.csv`

| Métrica | Antes de `validation.csv` | Después de `validation.csv` |
|---|---:|---:|
| Total de filas | 143 | 151 |
| Precios válidos | 139 | 147 |
| Suma de precios | 8046.0000 | 8380.0000 |
| Promedio | 57.8849 | 57.0068 |
| Mínimo | 10.0000 | 10.0000 |
| Máximo | 100.0000 | 100.0000 |