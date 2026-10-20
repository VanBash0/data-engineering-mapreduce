import csv
import io
import json
import os
from pathlib import Path

import boto3
import duckdb
from botocore.config import Config

from selection import is_python_developer


BUCKET = "vacancies"
OBJECT_KEY = "it_vacancies_2024.csv"
OUTPUT_DIR = Path("results/duckdb")

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
            "и RUSTFS_SECRET_KEY."
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



def export_json(connection, size, name):
    output_path = OUTPUT_DIR / f"{name}.json"

    cursor = connection.execute(
        """
        SELECT items, count
        FROM combinations
        WHERE size = ?
        ORDER BY count DESC, items ASC
        """,
        [size],
    )

    with output_path.open("w", encoding="utf-8") as file:
        file.write("[\n")
        first = True

        while True:
            rows = cursor.fetchmany(1000)

            if not rows:
                break

            for items, count in rows:
                record = {
                    "items": json.loads(items),
                    "count": count,
                }

                if not first:
                    file.write(",\n")

                file.write(
                    "  " + json.dumps(record, ensure_ascii=False)
                )
                first = False

        file.write("\n]\n")


def run_analysis():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for name in (*RESULT_NAMES.values(), "summary"):
        path = OUTPUT_DIR / f"{name}.json"
        if path.exists():
            path.unlink()

    s3 = create_s3_client()
    response = s3.get_object(
        Bucket=BUCKET,
        Key=OBJECT_KEY,
    )

    connection = duckdb.connect()

    total_rows = 0
    selected_rows = 0

    try:
        connection.execute("""
            CREATE TABLE selected_vacancies (
                vacancy_id BIGINT,
                key_skills VARCHAR
            )
        """)

        with io.TextIOWrapper(
            response["Body"],
            encoding="utf-8-sig",
            newline="",
        ) as text_stream:
            reader = csv.DictReader(text_stream)

            if not reader.fieldnames:
                raise ValueError("CSV-файл пуст.")

            reader.fieldnames = [
                name.strip() for name in reader.fieldnames
            ]

            if len(reader.fieldnames) != len(set(reader.fieldnames)):
                raise ValueError(
                    "В CSV есть повторяющиеся названия колонок."
                )

            missing = REQUIRED_COLUMNS - set(reader.fieldnames)

            if missing:
                raise ValueError(
                    f"В CSV отсутствуют колонки: {sorted(missing)}"
                )

            batch = []
            batch_size = 1000

            for row_id, row in enumerate(reader, start=1):
                if not row or all(value is None for value in row.values()):
                    continue

                total_rows += 1

                if not is_python_developer(row):
                    continue

                selected_rows += 1
                batch.append((
                    row_id,
                    row.get("key_skills") or "",
                ))

                if len(batch) >= batch_size:
                    connection.executemany(
                        """
                        INSERT INTO selected_vacancies
                        VALUES (?, ?)
                        """,
                        batch,
                    )
                    batch.clear()

            if batch:
                connection.executemany(
                    """
                    INSERT INTO selected_vacancies
                    VALUES (?, ?)
                    """,
                    batch,
                )

        connection.execute("""
            CREATE TABLE vacancy_skills AS
            SELECT DISTINCT
                v.vacancy_id,
                lower(trim(s.skill)) AS skill
            FROM selected_vacancies AS v,
            UNNEST(string_split(v.key_skills, ';')) AS s(skill)
            WHERE trim(s.skill) <> ''
        """)

        connection.execute("""
            CREATE TABLE combinations (
                size INTEGER NOT NULL,
                items VARCHAR NOT NULL,
                count BIGINT NOT NULL,
                PRIMARY KEY (size, items)
            )
        """)

        connection.execute("""
            INSERT INTO combinations
            SELECT
                1,
                to_json([skill]),
                count(*)
            FROM vacancy_skills
            GROUP BY skill
        """)

        connection.execute("""
            INSERT INTO combinations
            SELECT
                2,
                to_json([a.skill, b.skill]),
                count(*)
            FROM vacancy_skills AS a
            JOIN vacancy_skills AS b
                ON a.vacancy_id = b.vacancy_id
                AND a.skill < b.skill
            GROUP BY a.skill, b.skill
        """)

        connection.execute("""
            INSERT INTO combinations
            SELECT
                3,
                to_json([a.skill, b.skill, c.skill]),
                count(*)
            FROM vacancy_skills AS a
            JOIN vacancy_skills AS b
                ON a.vacancy_id = b.vacancy_id
                AND a.skill < b.skill
            JOIN vacancy_skills AS c
                ON a.vacancy_id = c.vacancy_id
                AND b.skill < c.skill
            GROUP BY a.skill, b.skill, c.skill
        """)

        unique_counts = {}

        for size, name in RESULT_NAMES.items():
            export_json(connection, size, name)

            unique_counts[name] = connection.execute(
                """
                SELECT count(*)
                FROM combinations
                WHERE size = ?
                """,
                [size],
            ).fetchone()[0]

        summary = {
            "source": f"s3://{BUCKET}/{OBJECT_KEY}",
            "total_rows": total_rows,
            "selected_vacancies": selected_rows,
            "unique_skills": unique_counts["skills"],
            "unique_pairs": unique_counts["pairs"],
            "unique_triples": unique_counts["triples"],
        }

        with (OUTPUT_DIR / "summary.json").open(
            "w", encoding="utf-8"
        ) as file:
            json.dump(
                summary,
                file,
                ensure_ascii=False,
                indent=2,
            )

        print(json.dumps(summary, ensure_ascii=False, indent=2))

    finally:
        response["Body"].close()
        connection.close()


if __name__ == "__main__":
    run_analysis()
