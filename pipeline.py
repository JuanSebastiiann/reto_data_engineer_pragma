#@Juan Sebastian Hurtado Serna

import argparse
import csv
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import psycopg


DATA_DIR = Path("data")
BATCH_SIZE = 16

MAIN_FILES = [
    DATA_DIR / "2012-1.csv",
    DATA_DIR / "2012-2.csv",
    DATA_DIR / "2012-3.csv",
    DATA_DIR / "2012-4.csv",
    DATA_DIR / "2012-5.csv",
]

VALIDATION_FILE = DATA_DIR / "validation.csv"


def read_microbatches(path, batch_size):
    """Lee un archivo CSV y entrega sus filas en microbatches."""
    with path.open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.DictReader(file)

        expected = {"timestamp", "price", "user_id"}

        if not reader.fieldnames or not expected.issubset(
            reader.fieldnames
        ):
            raise ValueError(
                f"El archivo {path.name} no contiene las columnas esperadas."
            )

        batch = []

        # Identifica cada fila de datos dentro de su archivo.
        for source_row_number, row in enumerate(reader, start=1):
            row["source_row_number"] = source_row_number
            batch.append(row)

            if len(batch) == batch_size:
                yield batch
                batch = []

        if batch:
            yield batch


def connect_db():
    """Establece la conexion con PostgreSQL."""
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "reto_data_engineer"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        connect_timeout=5,
    )


def get_current_stats(cur):
    """Consulta las estadisticas acumuladas del pipeline."""
    cur.execute(
        """
        SELECT
            row_count,
            price_count,
            price_sum,
            price_sum / NULLIF(price_count, 0) AS price_avg,
            price_min,
            price_max
        FROM pipeline_stats
        WHERE id = 1;
        """
    )

    stats = cur.fetchone()

    if stats is None:
        raise RuntimeError(
            "No existe el registro inicial id = 1 en pipeline_stats."
        )

    return stats


def print_stats(stats):
    """Muestra las estadisticas acumuladas."""
    print("  Total de filas:", stats[0])
    print("  Precios validos:", stats[1])
    print("  Suma acumulada:", stats[2])
    print("  Promedio acumulado:", stats[3])
    print("  Minimo acumulado:", stats[4])
    print("  Maximo acumulado:", stats[5])


def process_microbatch(conn, batch, file_path):
    """Inserta solo filas nuevas y actualiza las estadisticas."""
    transactions = []

    # Validacion y transformacion del microbatch.
    for row in batch:
        timestamp_text = (row.get("timestamp") or "").strip()
        user_id_text = (row.get("user_id") or "").strip()
        raw_price = (row.get("price") or "").strip()

        source_row_number = int(row["source_row_number"])

        if not timestamp_text or not user_id_text:
            raise ValueError(
                f"Timestamp o user_id vacio en {file_path.name}: {row}"
            )

        try:
            timestamp = datetime.strptime(
                timestamp_text,
                "%m/%d/%Y",
            ).date()

            user_id = int(user_id_text)
            price = Decimal(raw_price) if raw_price else None

        except (ValueError, ArithmeticError) as error:
            raise ValueError(
                f"Registro invalido en {file_path.name}: {row}"
            ) from error

        if price is not None and not price.is_finite():
            raise ValueError(
                f"Precio no valido en {file_path.name}: {row}"
            )

        transactions.append(
            (
                timestamp,
                price,
                user_id,
                file_path.name,
                source_row_number,
            )
        )

    inserted_count = 0
    valid_prices = []

    # La insercion y las estadisticas se confirman juntas.
    with conn.transaction():
        with conn.cursor() as cur:

            for transaction in transactions:
                cur.execute(
                    """
                    INSERT INTO transactions
                        (
                            timestamp,
                            price,
                            user_id,
                            source_file,
                            source_row_number
                        )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (source_file, source_row_number)
                    DO NOTHING
                    RETURNING price;
                    """,
                    transaction,
                )

                inserted_row = cur.fetchone()

                # Solo contabiliza las filas realmente insertadas.
                if inserted_row is not None:
                    inserted_count += 1
                    inserted_price = inserted_row[0]

                    if inserted_price is not None:
                        valid_prices.append(inserted_price)

            price_count = len(valid_prices)
            price_sum = sum(valid_prices, Decimal("0"))

            batch_min = (
                min(valid_prices) if valid_prices else None
            )
            batch_max = (
                max(valid_prices) if valid_prices else None
            )

            # Si todas las filas eran duplicadas, no actualiza stats.
            if inserted_count > 0:
                cur.execute(
                    """
                    UPDATE pipeline_stats
                    SET row_count = row_count + %s,
                        price_count = price_count + %s,
                        price_sum = price_sum + %s,
                        price_min = CASE
                            WHEN %s::NUMERIC IS NULL THEN price_min
                            WHEN price_min IS NULL THEN %s::NUMERIC
                            ELSE LEAST(price_min, %s::NUMERIC)
                        END,
                        price_max = CASE
                            WHEN %s::NUMERIC IS NULL THEN price_max
                            WHEN price_max IS NULL THEN %s::NUMERIC
                            ELSE GREATEST(price_max, %s::NUMERIC)
                        END
                    WHERE id = 1;
                    """,
                    (
                        inserted_count,
                        price_count,
                        price_sum,
                        batch_min,
                        batch_min,
                        batch_min,
                        batch_max,
                        batch_max,
                        batch_max,
                    ),
                )

                if cur.rowcount != 1:
                    raise RuntimeError(
                        "Debe existir exactamente un registro de "
                        "estadisticas con id = 1."
                    )

            stats = get_current_stats(cur)

    return stats, inserted_count, price_count


def process_file(conn, file_path):
    """Procesa un archivo completo, lote por lote."""
    if not file_path.is_file():
        raise FileNotFoundError(
            f"No se encontro el archivo: {file_path}"
        )

    print(f"\nProcesando archivo: {file_path.name}")

    total_batches = 0
    total_rows_read = 0
    total_inserted = 0

    for batch_number, batch in enumerate(
        read_microbatches(file_path, BATCH_SIZE),
        start=1,
    ):
        stats, inserted_count, price_count = process_microbatch(
            conn,
            batch,
            file_path,
        )

        total_batches += 1
        total_rows_read += len(batch)
        total_inserted += inserted_count

        print(f"\nMicrobatch {batch_number}")
        print("  Filas leidas:", len(batch))
        print("  Filas nuevas insertadas:", inserted_count)
        print("  Precios validos insertados:", price_count)
        print(
            "  Precios vacios insertados:",
            inserted_count - price_count,
        )
        print_stats(stats)

    print(f"\nArchivo terminado: {file_path.name}")
    print("  Microbatches procesados:", total_batches)
    print("  Filas leidas:", total_rows_read)
    print("  Filas nuevas insertadas:", total_inserted)
    print("  Filas omitidas por duplicidad:",
          total_rows_read - total_inserted)

    if total_rows_read > 0 and total_inserted == 0:
        print(
            f"El archivo {file_path.name} ya estaba cargado. "
            "No se insertaron registros nuevos."
        )
    elif total_inserted < total_rows_read:
        print(
            f"El archivo {file_path.name} se proceso parcialmente: "
            "se omitieron filas que ya existian."
        )
    else:
        print(
            f"El archivo {file_path.name} se cargo correctamente."
        )


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline de ingesta por microbatches."
    )

    parser.add_argument(
        "mode",
        choices=["main", "validation"],
        help="Procesar los cinco archivos principales o validation.csv.",
    )

    args = parser.parse_args()

    if args.mode == "main":
        files_to_process = MAIN_FILES
    else:
        files_to_process = [VALIDATION_FILE]

    with connect_db() as conn:
        print("Conexion a PostgreSQL establecida.")

        print("\nEstadisticas antes de iniciar:")
        with conn.cursor() as cur:
            print_stats(get_current_stats(cur))

        for file_path in files_to_process:
            process_file(conn, file_path)

        print("\nEstadisticas finales:")
        with conn.cursor() as cur:
            print_stats(get_current_stats(cur))


if __name__ == "__main__":
    main()
