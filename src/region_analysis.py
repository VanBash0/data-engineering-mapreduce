import csv
import io
import json
import os
from collections import Counter
from itertools import combinations
from pathlib import Path
from statistics import median

import boto3
from botocore.config import Config

from selection import is_python_developer

BUCKET = "vacancies"
OBJECT_KEY = "it_vacancies_2024.csv"
OUTPUT_DIR = Path("results/regions")

# В текущем анализе каждый регион представлен одним основным городом.
REGIONS = {
    "Приморский край": "Владивосток",
    "Пермский край": "Пермь",
    "Калининградская область": "Калининград",
}

EXCHANGE_RATES_TO_RUB = {
    "RUR": 1.0, "RUB": 1.0, "USD": 82.8504,
    "EUR": 93.2316, "KZT": 0.183249,
    "BYR": 27.6629, "BYN": 27.6629,
}
TAX_BRACKETS = (
    (2_400_000, 0.13),
    (5_000_000, 0.15),
    (20_000_000, 0.18),
    (50_000_000, 0.20),
    (float("inf"), 0.22),
)
TOP_SKILLS = 10
TOP_PAIRS = 10


def create_s3_client():
    required = ("RUSTFS_ENDPOINT", "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY")
    if not all(os.environ.get(name) for name in required):
        raise RuntimeError(
            "Задайте RUSTFS_ENDPOINT, RUSTFS_ACCESS_KEY и RUSTFS_SECRET_KEY."
        )
    return boto3.client(
        "s3",
        endpoint_url=os.environ["RUSTFS_ENDPOINT"],
        aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
        region_name=os.environ.get("RUSTFS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def get_salary(row):
    try:
        low = float(row.get("salary_from") or 0)
        high = float(row.get("salary_to") or 0)
    except (TypeError, ValueError):
        return None

    if low > 0 and high > 0:
        salary = (low + high) / 2
    else:
        salary = low or high
    if salary <= 0:
        return None

    rate = EXCHANGE_RATES_TO_RUB.get(
        (row.get("currency") or "").strip().upper()
    )
    if rate is None:
        return None
    salary *= rate

    # Прогрессивный НДФЛ применяется только к gross=true.
    if (row.get("gross") or "").strip().casefold() in {"true", "1", "да"}:
        annual = salary * 12
        tax = 0.0
        lower = 0.0
        for upper, rate in TAX_BRACKETS:
            tax += max(0.0, min(annual, upper) - lower) * rate
            if annual <= upper:
                break
            lower = upper
        salary = (annual - tax) / 12

    return salary


def parse_skills(value):
    return sorted({
        skill.strip().casefold()
        for skill in (value or "").split(";")
        if skill.strip()
    })


def main():
    city_to_region = {
        city.casefold(): region for region, city in REGIONS.items()
    }
    response = create_s3_client().get_object(Bucket=BUCKET, Key=OBJECT_KEY)
    stats = {
        region: {"count": 0, "salaries": [], "skills": Counter(), "pairs": Counter(),
                 "without_salary": 0}
        for region in REGIONS
    }

    try:
        with io.TextIOWrapper(response["Body"], encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames:
                raise ValueError("CSV-файл пуст.")
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            required = {"area", "role", "name", "key_skills", "salary_from",
                        "salary_to", "currency", "gross"}
            missing = required - set(reader.fieldnames)
            if missing:
                raise ValueError(f"В CSV отсутствуют колонки: {sorted(missing)}")

            for row in reader:
                if not is_python_developer(row):
                    continue
                city = (row.get("area") or "").strip().casefold()
                region = city_to_region.get(city)
                if region is None:
                    continue

                item = stats[region]
                item["count"] += 1
                salary = get_salary(row)
                if salary is None:
                    item["without_salary"] += 1
                else:
                    item["salaries"].append(salary)

                skills = parse_skills(row.get("key_skills"))
                item["skills"].update(skills)
                item["pairs"].update(" + ".join(pair) for pair in combinations(skills, 2))
    finally:
        response["Body"].close()

    result = {
        "source": f"s3://{BUCKET}/{OBJECT_KEY}",
        "method": "Каждый регион представлен одним основным городом; учитываются вакансии, прошедшие is_python_developer.",
        "regions": {},
    }

    print("Сравнение выбранных городов:\n")
    for region, city in REGIONS.items():
        item = stats[region]
        salaries = item["salaries"]
        med = median(salaries) if salaries else None
        top_skills = [
            {
                "skill": skill,
                "vacancies": count,
                "percent": round(count / item["count"] * 100, 1),
            }
            for skill, count in item["skills"].most_common(TOP_SKILLS)
        ]
        top_pairs = [
            {"pair": pair, "vacancies": count}
            for pair, count in item["pairs"].most_common(TOP_PAIRS)
        ]
        result["regions"][region] = {
            "city": city,
            "vacancies": item["count"],
            "vacancies_with_salary": len(salaries),
            "vacancies_without_salary": item["without_salary"],
            "median_salary_rub": round(med, 2) if med is not None else None,
            "top_skills": top_skills,
            "top_pairs": top_pairs,
        }

        med_text = f"{med:,.0f} RUB".replace(",", " ") if med is not None else "нет данных"
        print(f"{region} ({city}):")
        print(f"  Вакансий: {item['count']}")
        print(f"  Вакансий с зарплатой: {len(salaries)}")
        print(f"  Медианная зарплата: {med_text}")
        print("  Топ навыков:")
        for entry in top_skills:
            print(
                f"    {entry['skill']}: {entry['vacancies']} "
                f"({entry['percent']}%)"
            )
        print("  Топ пар:")
        for entry in top_pairs:
            print(f"    {entry['pair']}: {entry['vacancies']}")
        print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "region_analysis.json"
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False, indent=2)
    print(f"Полные результаты сохранены: {output_path}")


if __name__ == "__main__":
    main()
