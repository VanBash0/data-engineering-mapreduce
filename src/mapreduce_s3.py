import argparse
import csv
import itertools
import io
import json
import os
import sqlite3
from collections import Counter
from pathlib import Path

import boto3
from botocore.config import Config

from selection import is_python_developer


BUCKET = "vacancies"
OBJECT_KEY = "it_vacancies_2024.csv"

OUTPUT_DIR = Path("results/mapreduce")

RESULT_NAMES = {
    1: "skills",
    2: "pairs",
    3: "triples",
}

REQUIRED_COLUMNS = {"role", "name", "key_skills"}


def create_s3_client():
    endpoint = os.environ.get("RUSTFS_ENDPOINT")
    access_key = os.environ.get("RUSTFS_ACCESS_KEY")
    secret_key = os.environ.get("RUSTFS_SECRET_KEY")

    if not all((endpoint, access_key, secret_key)):
        raise RuntimeError(
            "Задайте RUSTFS_ENDPOINT, RUSTFS_ACCESS_KEY "
            "и RUSTFS_SECRET_KEY в переменных окружения."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.environ.get("RUSTFS_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def parse_skills(raw_skills):
    if not raw_skills:
        return []

    return sorted({
        skill.strip().lower()
        for skill in raw_skills.split(";")
        if skill.strip()
    })


def create_counters():
    return {
        1: Counter(),
        2: Counter(),
        3: Counter(),
    }


def map_chunk(rows):
    counters = create_counters()
    selected_rows = 0

    for row in rows:
        if not is_python_developer(row):
            continue

        selected_rows += 1
        skills = parse_skills(row["key_skills"])

        for size, counter in counters.items():
            if len(skills) >= size:
                counter.update(itertools.combinations(skills, size))

    return counters, selected_rows


def init_reduce_db(connection):
    connection.execute("""
        CREATE TABLE combinations (
            size INTEGER NOT NULL,
            items TEXT NOT NULL,
            count INTEGER NOT NULL,
            PRIMARY KEY (size, items)
        )
    """)
    connection.commit()


def reduce_chunk(connection, counters):
    for size, counter in counters.items():
        connection.executemany(
            """
            INSERT INTO combinations (size, items, count)
            VALUES (?, ?, ?)
            ON CONFLICT(size, items)
            DO UPDATE SET count = count + excluded.count
            """,
            (
                (
                    size,
                    json.dumps(items, ensure_ascii=False),
                    count,
                )
                for items, count in counter.items()
            ),
        )

    connection.commit()


def export_results(connection, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)

    for size, name in RESULT_NAMES.items():
        output_path = output_dir / f"{name}.json"

        cursor = connection.execute(
            """
            SELECT items, count
            FROM combinations
            WHERE size = ?
            ORDER BY count DESC, items ASC
            """,
            (size,),
        )

        with output_path.open("w", encoding="utf-8") as file:
            file.write("[\n")
            first = True

            for items_json, count in cursor:
                if not first:
                    file.write(",\n")

                record = {
                    "items": json.loads(items_json),
                    "count": count,
                }

                file.write(
                    "  " + json.dumps(record, ensure_ascii=False)
                )
                first = False

            file.write("\n]\n")


def upload_results(s3, output_dir, object_prefix):
    for filename in (
        "skills.json",
        "pairs.json",
        "triples.json",
        "summary.json",
    ):
        s3.upload_file(
            str(output_dir / filename),
            BUCKET,
            f"{object_prefix}/{filename}",
        )

        print(
            f"Загружен: s3://{BUCKET}/"
            f"{object_prefix}/{filename}"
        )


def run_mapreduce(chunk_size, should_upload=True):
    s3 = create_s3_client()

    output_dir = OUTPUT_DIR / f"chunk_size_{chunk_size}"
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = output_dir / "reduce.sqlite3"

    if db_path.exists():
        db_path.unlink()

    for filename in (
        "skills.json",
        "pairs.json",
        "triples.json",
        "summary.json",
    ):
        output_path = output_dir / filename
        if output_path.exists():
            output_path.unlink()

    response = s3.get_object(
        Bucket=BUCKET,
        Key=OBJECT_KEY,
    )

    connection = sqlite3.connect(db_path)

    total_rows = 0
    selected_rows = 0
    chunks_processed = 0

    object_prefix = f"results/mapreduce/chunk_size_{chunk_size}"

    try:
        init_reduce_db(connection)

        text_stream = io.TextIOWrapper(
            response["Body"],
            encoding="utf-8-sig",
            newline="",
        )

        reader = csv.reader(text_stream)

        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV-файл пуст.")

        header = [column.strip() for column in header]

        if len(header) != len(set(header)):
            raise ValueError(
                "В CSV есть повторяющиеся названия колонок."
            )

        missing = REQUIRED_COLUMNS - set(header)

        if missing:
            raise ValueError(
                f"В CSV отсутствуют колонки: {sorted(missing)}"
            )

        indexes = {
            name: header.index(name)
            for name in REQUIRED_COLUMNS
        }

        while True:
            selected_fields = []
            chunk_rows = 0

            for row in itertools.islice(reader, chunk_size):
                if not row:
                    continue

                if len(row) != len(header):
                    raise ValueError(
                        f"Строка CSV содержит {len(row)} полей, "
                        f"ожидалось {len(header)}. "
                        "Проверьте корректность CSV."
                    )

                chunk_rows += 1
                selected_fields.append({
                    name: row[index]
                    for name, index in indexes.items()
                })

            if chunk_rows == 0:
                break

            chunks_processed += 1
            total_rows += chunk_rows

            counters, chunk_selected = map_chunk(selected_fields)
            selected_rows += chunk_selected

            reduce_chunk(connection, counters)

            if chunks_processed % 10 == 0:
                print(
                    f"Обработано строк: {total_rows}; "
                    f"отобрано вакансий: {selected_rows}; "
                    f"чанков: {chunks_processed}"
                )

            del selected_fields
            del counters

        export_results(connection, output_dir)

        unique_counts = {
            size: connection.execute(
                """
                SELECT COUNT(*)
                FROM combinations
                WHERE size = ?
                """,
                (size,),
            ).fetchone()[0]
            for size in RESULT_NAMES
        }

        summary = {
            "source": f"s3://{BUCKET}/{OBJECT_KEY}",
            "total_rows": total_rows,
            "selected_vacancies": selected_rows,
            "chunk_size": chunk_size,
            "chunks_processed": chunks_processed,
            "unique_skills": unique_counts[1],
            "unique_pairs": unique_counts[2],
            "unique_triples": unique_counts[3],
            "results_prefix": f"s3://{BUCKET}/{object_prefix}/",
        }

        with (output_dir / "summary.json").open(
            "w", encoding="utf-8"
        ) as file:
            json.dump(
                summary,
                file,
                ensure_ascii=False,
                indent=2,
            )

    finally:
        response["Body"].close()
        connection.close()

    if should_upload:
        upload_results(s3, output_dir, object_prefix)

    print("\nОбработка завершена.")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="Chunked MapReduce для навыков Python-вакансий"
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Количество исходных строк CSV в чанке",
    )

    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Не загружать результаты обратно в RustFS",
    )

    args = parser.parse_args()

    if args.chunk_size <= 0:
        parser.error("--chunk-size должен быть положительным")

    run_mapreduce(
        chunk_size=args.chunk_size,
        should_upload=not args.no_upload,
    )


if __name__ == "__main__":
    main()