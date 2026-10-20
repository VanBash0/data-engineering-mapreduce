import csv
import io
import json
import os
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from statistics import median

import boto3
from botocore.config import Config

from selection import is_python_developer


BUCKET = "vacancies"
OBJECT_KEY = "it_vacancies_2024.csv"
OUTPUT_DIR = Path("results/duckdb")

EXCHANGE_RATES = {
    "RUR": 1.0,
    "RUB": 1.0,
    "USD": 82.8504,
    "EUR": 93.2316,
    "KZT": 0.183249,
    "BYR": 27.6629,
    "BYN": 27.6629,
}

TAX_BRACKETS = (
    (2_400_000, 0.13),
    (5_000_000, 0.15),
    (20_000_000, 0.18),
    (50_000_000, 0.20),
    (float("inf"), 0.22),
)

MIN_COMBINATION_VACANCIES = 5
TOP_RESULTS = 10


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
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def salary_before_tax(row):
    try:
        low = float(row.get("salary_from") or 0)
        high = float(row.get("salary_to") or 0)
    except (TypeError, ValueError):
        return None

    if low > 0 and high > 0:
        return (low + high) / 2
    if low > 0:
        return low
    if high > 0:
        return high
    return None


def apply_progressive_tax(monthly_salary):
    annual_income = monthly_salary * 12
    tax = 0.0
    lower_bound = 0.0

    for upper_bound, rate in TAX_BRACKETS:
        taxable = max(0.0, min(annual_income, upper_bound) - lower_bound)
        tax += taxable * rate
        if annual_income <= upper_bound:
            break
        lower_bound = upper_bound

    return (annual_income - tax) / 12


def get_salary_rub(row):
    salary = salary_before_tax(row)
    if salary is None:
        return None

    currency = (row.get("currency") or "").strip().upper()
    rate = EXCHANGE_RATES.get(currency)
    if rate is None:
        return None

    salary *= rate
    gross = (row.get("gross") or "").strip().casefold()

    if gross in {"true", "1", "да"}:
        salary = apply_progressive_tax(salary)

    return salary


def parse_skills(value):
    return sorted({
        skill.strip().casefold()
        for skill in (value or "").split(";")
        if skill.strip()
    })


def write_json(path, data):
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def analyse_salary():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    s3 = create_s3_client()
    response = s3.get_object(Bucket=BUCKET, Key=OBJECT_KEY)

    vacancies = []
    total_rows = 0
    selected_rows = 0
    skipped_unknown_currency = 0

    try:
        with io.TextIOWrapper(
            response["Body"], encoding="utf-8-sig", newline=""
        ) as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames:
                raise ValueError("CSV-файл пуст.")

            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            required = {
                "role", "name", "key_skills", "salary_from", "salary_to",
                "currency", "gross",
            }
            missing = required - set(reader.fieldnames)
            if missing:
                raise ValueError(f"В CSV отсутствуют колонки: {sorted(missing)}")

            for row_id, row in enumerate(reader, start=1):
                if not row or all(value is None for value in row.values()):
                    continue
                total_rows += 1

                if not is_python_developer(row):
                    continue
                selected_rows += 1

                salary_raw = salary_before_tax(row)
                salary = get_salary_rub(row)
                if salary_raw is not None and salary is None:
                    skipped_unknown_currency += 1
                if salary is None:
                    continue

                vacancies.append({
                    "id": row_id,
                    "salary": salary,
                    "skills": parse_skills(row.get("key_skills")),
                })
    finally:
        response["Body"].close()

    if not vacancies:
        raise ValueError("Не найдено выбранных вакансий с учитываемой зарплатой.")

    overall_median = median(vacancy["salary"] for vacancy in vacancies)
    stats = {1: defaultdict(list), 2: defaultdict(list), 3: defaultdict(list)}

    for vacancy in vacancies:
        skills = vacancy["skills"]
        for size in (1, 2, 3):
            for items in combinations(skills, size):
                stats[size][items].append(vacancy["salary"])

    def build_results(size):
        results = []
        for items, salaries in stats[size].items():
            count = len(salaries)
            if count < MIN_COMBINATION_VACANCIES:
                continue

            combo_median = median(salaries)
            delta = combo_median - overall_median
            results.append({
                "items": list(items),
                "vacancies": count,
                "median_salary_rub": round(combo_median, 2),
                "overall_median_rub": round(overall_median, 2),
                "deviation_rub": round(delta, 2),
                "deviation_percent": round(delta / overall_median * 100, 2)
                    if overall_median else None,
            })

        return sorted(
            results,
            key=lambda item: (
                -abs(item["deviation_rub"]),
                -item["vacancies"],
                item["items"],
            ),
        )

    pair_results = build_results(2)
    triple_results = build_results(3)

    write_json(OUTPUT_DIR / "salary_pairs.json", pair_results)
    write_json(OUTPUT_DIR / "salary_triples.json", triple_results)

    summary = {
        "source": f"s3://{BUCKET}/{OBJECT_KEY}",
        "total_rows": total_rows,
        "selected_vacancies": selected_rows,
        "vacancies_with_salary": len(vacancies),
        "skipped_unknown_currency": skipped_unknown_currency,
        "overall_median_salary_rub": round(overall_median, 2),
        "minimum_vacancies_per_combination": MIN_COMBINATION_VACANCIES,
        "exchange_rates_to_rub": EXCHANGE_RATES,
        "salary_method": (
            "Среднее границ диапазона; если дана одна граница — используется она. "
            "Годовой прогрессивный НДФЛ применяется только при gross=true. "
            "При gross=false и неизвестном gross налог не пересчитывается."
        ),
    }
    write_json(OUTPUT_DIR / "salary_summary.json", summary)

    print(f"Всего строк: {total_rows}")
    print(f"Выбрано вакансий: {selected_rows}")
    print(f"Вакансий с учтённой зарплатой: {len(vacancies)}")
    print(f"Пропущено из-за отсутствующей/неизвестной валюты или зарплаты: "
          f"{selected_rows - len(vacancies)}")
    print(f"Общая медианная зарплата: {overall_median:,.0f} RUB".replace(",", " "))
    print(f"Пар для анализа: {len(pair_results)}")
    print(f"Троек для анализа: {len(triple_results)}")

    for title, results in (
        ("Самые высокие отклонения для пар", pair_results),
        ("Самые низкие отклонения для пар", sorted(
            pair_results, key=lambda x: (x["deviation_rub"], -x["vacancies"])
        )),
        ("Самые высокие отклонения для троек", triple_results),
        ("Самые низкие отклонения для троек", sorted(
            triple_results, key=lambda x: (x["deviation_rub"], -x["vacancies"])
        )),
    ):
        print(f"\n{title}:")
        for item in results[:TOP_RESULTS]:
            skills = " + ".join(item["items"])
            print(
                f"{skills}: {item['deviation_rub']:+,.0f} RUB "
                f"({item['deviation_percent']:+.1f}%), "
                f"медиана {item['median_salary_rub']:,.0f} RUB, "
                f"вакансий {item['vacancies']}".replace(",", " ")
            )

    print(f"\nJSON сохранены в: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    analyse_salary()
