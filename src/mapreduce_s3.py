import argparse
import csv
import itertools
import json
import os
import re
from collections import Counter
from pathlib import Path

import boto3
from botocore.config import Config

from selection import is_python_developer


BUCKET = "vacancies"
OBJECT_KEY = "it_vacancies_2024.csv"

OUTPUT_DIR = Path("results/mapreduce")

SKILL_SEPARATOR = re.compile(r"[\r\n;|]+")


def create_s3_client():
    endpoint_url = os.environ.get("RUSTFS_ENDPOINT")
    access_key = os.environ.get("RUSTFS_ACCESS_KEY")
    secret_key = os.environ.get("RUSTFS_SECRET_KEY")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
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

    skills = {
        skill.strip().lower()
        for skill in raw_skills.split(";")
        if skill.strip()
    }

    return sorted(skills)


def count_combinations(skills, counters):
    for size, counter in counters.items():
        if len(skills) < size:
            continue

        for combination in itertools.combinations(skills, size):
            counter[combination] += 1


def save_counter(counter, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    records = [
        {"items": list(items), "count": count}
        for items, count in sorted(
            counter.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(records, file, ensure_ascii=False, indent=2)


def run_mapreduce(batch_size):
    s3 = create_s3_client()

    response = s3.get_object(
        Bucket=BUCKET,
        Key=OBJECT_KEY,
    )

    counters = {
        1: Counter(),
        2: Counter(),
        3: Counter(),
    }

    total_rows = 0
    selected_rows = 0
    processed_in_batch = 0

    try:
        import io

        text_stream = io.TextIOWrapper(
            response["Body"],
            encoding="utf-8-sig",
            newline="",
        )

        reader = csv.DictReader(text_stream)

        for row in reader:
            total_rows += 1

            if not is_python_developer(row):
                continue

            selected_rows += 1
            skills = parse_skills(row.get("key_skills", ""))

            count_combinations(skills, counters)
            processed_in_batch += 1

            if processed_in_batch >= batch_size:
                print(
                    f"Обработано строк: {total_rows}; "
                    f"отобрано вакансий: {selected_rows}"
                )
                processed_in_batch = 0

    finally:
        response["Body"].close()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    save_counter(counters[1], OUTPUT_DIR / "skills.json")
    save_counter(counters[2], OUTPUT_DIR / "pairs.json")
    save_counter(counters[3], OUTPUT_DIR / "triples.json")

    summary = {
        "source": f"s3://{BUCKET}/{OBJECT_KEY}",
        "total_rows": total_rows,
        "selected_vacancies": selected_rows,
        "unique_skills": len(counters[1]),
        "unique_pairs": len(counters[2]),
        "unique_triples": len(counters[3]),
        "batch_size": batch_size,
    }

    with (OUTPUT_DIR / "summary.json").open(
        "w", encoding="utf-8"
    ) as file:
        json.dump(summary, file, ensure_ascii=False, indent=2)

    print("\nОбработка завершена.")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="MapReduce-подсчёт навыков вакансий из RustFS"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Число отобранных вакансий между сообщениями о прогрессе",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        parser.error("--batch-size должен быть положительным")

    run_mapreduce(args.batch_size)


if __name__ == "__main__":
    main()
