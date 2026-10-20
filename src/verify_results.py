import json
from pathlib import Path


MAPREDUCE_DIR = Path("results/mapreduce/chunk_size_100")
DUCKDB_DIR = Path("results/duckdb")

FILES = ("skills.json", "pairs.json", "triples.json")


def load_results(path):
    with path.open("r", encoding="utf-8") as file:
        records = json.load(file)

    return {
        tuple(record["items"]): record["count"]
        for record in records
    }


def compare_file(filename):
    mapreduce = load_results(MAPREDUCE_DIR / filename)
    duckdb = load_results(DUCKDB_DIR / filename)

    all_items = set(mapreduce) | set(duckdb)
    differences = []

    for items in all_items:
        mr_count = mapreduce.get(items)
        sql_count = duckdb.get(items)

        if mr_count != sql_count:
            differences.append((items, mr_count, sql_count))

    print(f"\n{filename}")
    print(f"MapReduce: {len(mapreduce)} комбинаций")
    print(f"DuckDB:    {len(duckdb)} комбинаций")

    if not differences:
        print("Результат: полное совпадение")
        return True

    print(f"Расхождений: {len(differences)}")

    for items, mr_count, sql_count in sorted(
        differences,
        key=lambda item: str(item[0]),
    )[:20]:
        print(
            f"{items}: MapReduce={mr_count}, DuckDB={sql_count}"
        )

    return False


def main():
    results = [
        compare_file(filename)
        for filename in FILES
    ]

    print("\nИтог:", "Все результаты совпали" if all(results)
          else "Обнаружены расхождения")


if __name__ == "__main__":
    main()