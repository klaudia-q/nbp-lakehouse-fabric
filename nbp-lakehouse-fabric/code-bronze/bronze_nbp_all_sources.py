# ============================================================
# Notebook: bronze_nbp_all_sources.py
# Warstwa: Bronze
# Cel: Pobranie danych z 3 endpointów API NBP (tabela A, C, złoto)
#      dla tego samego zakresu dat i zapis do osobnych tabel Delta
# ============================================================

import requests
import json
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# --- SCHEMATY (jawne, aby uniknąć CANNOT_DETERMINE_TYPE) -------------------
# Spark nie potrafi wywnioskować typu kolumny, gdy wszystkie wartości to None.
# Np. tabela A nie ma bid_rate/ask_rate, a złoto nie ma pól walutowych.

SCHEMA_EXCHANGE_RATES = StructType([
    StructField("table_no",        StringType(), True),
    StructField("table_type",      StringType(), True),
    StructField("effective_date",  StringType(), True),
    StructField("trading_date",    StringType(), True),
    StructField("currency_name",   StringType(), True),
    StructField("currency_code",   StringType(), True),
    StructField("mid_rate",        DoubleType(), True),
    StructField("bid_rate",        DoubleType(), True),
    StructField("ask_rate",        DoubleType(), True),
    StructField("raw_json",        StringType(), True),
])

SCHEMA_GOLD = StructType([
    StructField("effective_date",  StringType(), True),
    StructField("gold_price_pln",  DoubleType(), True),
    StructField("raw_json",        StringType(), True),
])

# --- KONFIGURACJA -----------------------------------------------------------

DATE_FROM = "2020-01-01"
DATE_TO = datetime.today().strftime("%Y-%m-%d")
MAX_DAYS_PER_REQUEST = 93  # limit API NBP

# Definicja trzech źródeł — różnią się URL-em, polami i ścieżką zapisu
SOURCES = {
    "table_a": {
        "url_template": "https://api.nbp.pl/api/exchangerates/tables/A/{start}/{end}/?format=json",
        "bronze_path": "Tables/bronze_nbp_table_a",
        "parser": "exchange_rates",
        "table_type": "A",
        "schema": SCHEMA_EXCHANGE_RATES,
    },
    "table_c": {
        "url_template": "https://api.nbp.pl/api/exchangerates/tables/C/{start}/{end}/?format=json",
        "bronze_path": "Tables/bronze_nbp_table_c",
        "parser": "exchange_rates",
        "table_type": "C",
        "schema": SCHEMA_EXCHANGE_RATES,
    },
    "gold": {
        "url_template": "https://api.nbp.pl/api/cenyzlota/{start}/{end}/?format=json",
        "bronze_path": "Tables/bronze_nbp_gold",
        "parser": "gold",
        "table_type": "gold",
        "schema": SCHEMA_GOLD,
    },
}


# --- FUNKCJE POMOCNICZE -----------------------------------------------------

def generate_date_ranges(start: str, end: str, step_days: int = 93):
    fmt = "%Y-%m-%d"
    current = datetime.strptime(start, fmt)
    end_dt = datetime.strptime(end, fmt)
    ranges = []
    while current <= end_dt:
        chunk_end = min(current + timedelta(days=step_days - 1), end_dt)
        ranges.append((current.strftime(fmt), chunk_end.strftime(fmt)))
        current = chunk_end + timedelta(days=1)
    return ranges


def fetch_nbp(url: str) -> list:
    """Uniwersalna funkcja pobierająca dane z dowolnego endpointu NBP."""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json()
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"  [BŁĄD] {type(e).__name__}: {e}")
        return []


def parse_exchange_rates(tables: list, table_type: str) -> list:
    """
    Parser dla tabel A i C.
    Tabela A zwraca pole 'mid'.
    Tabela C zwraca pola 'bid' i 'ask' (bez 'mid').
    """
    records = []
    for table in tables:
        for rate in table.get("rates", []):
            records.append({
                "table_no":        table.get("no", ""),
                "table_type":      table_type,
                "effective_date":  table.get("effectiveDate", ""),
                "trading_date":    table.get("tradingDate", None),
                "currency_name":   rate.get("currency", ""),
                "currency_code":   rate.get("code", ""),
                "mid_rate":        rate.get("mid", None),
                "bid_rate":        rate.get("bid", None),
                "ask_rate":        rate.get("ask", None),
                "raw_json":        json.dumps(rate, ensure_ascii=False),
            })
    return records


def parse_gold(entries: list, table_type: str) -> list:
    """
    Parser dla cen złota.
    Odpowiedź to prosta lista: [{"data": "2024-01-02", "cena": 267.43}, ...]
    """
    records = []
    for entry in entries:
        records.append({
            "effective_date":  entry.get("data", ""),
            "gold_price_pln":  entry.get("cena", None),
            "raw_json":        json.dumps(entry, ensure_ascii=False),
        })
    return records


PARSERS = {
    "exchange_rates": parse_exchange_rates,
    "gold": parse_gold,
}


# --- GŁÓWNA PĘTLA — iteracja po źródłach ------------------------------------

date_ranges = generate_date_ranges(DATE_FROM, DATE_TO, MAX_DAYS_PER_REQUEST)

for source_name, config in SOURCES.items():
    print(f"\n{'='*60}")
    print(f"  Źródło: {source_name.upper()}")
    print(f"  Zakres: {DATE_FROM} — {DATE_TO}")
    print(f"{'='*60}")

    all_records = []
    parser_fn = PARSERS[config["parser"]]

    for i, (d_from, d_to) in enumerate(date_ranges, 1):
        url = config["url_template"].format(start=d_from, end=d_to)
        print(f"  [{i}/{len(date_ranges)}] {d_from} — {d_to}")
        raw_data = fetch_nbp(url)
        records = parser_fn(raw_data, config["table_type"])
        all_records.extend(records)

    print(f"  Pobrano łącznie: {len(all_records)} rekordów")

    if not all_records:
        print(f"  [UWAGA] Brak danych dla {source_name} — pomijam zapis.")
        continue

    # Konwersja do DataFrame Spark + metadane audytu
    # Jawny schemat zapobiega błędowi CANNOT_DETERMINE_TYPE
    df = spark.createDataFrame(all_records, schema=config["schema"])
    df_bronze = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source", lit("api.nbp.pl"))
        .withColumn("_source_name", lit(source_name))
    )

    # Zapis do Delta
    (
        df_bronze
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(config["bronze_path"])
    )

    print(f"  Zapisano {df_bronze.count()} wierszy → {config['bronze_path']}")

print(f"\n{'='*60}")
print("=== BRONZE — wszystkie źródła załadowane ===")
print(f"{'='*60}")


# --- WERYFIKACJA (opcjonalnie) -----------------------------------------------
# Szybki podgląd ile danych wylądowało w każdej tabeli Bronze

for source_name, config in SOURCES.items():
    try:
        df_check = spark.read.format("delta").load(config["bronze_path"])
        print(f"  {source_name:12s} → {df_check.count():>8,} wierszy")
    except Exception:
        print(f"  {source_name:12s} → BRAK TABELI")
