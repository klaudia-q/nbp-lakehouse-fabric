#!/usr/bin/env python
# coding: utf-8

# ## gold_star_schema.py
# 
# New notebook

# In[1]:


# Czy tabele Silver są widoczne?
for t in ["silver_exchange_rates_merged", "silver_gold_prices"]:
    try:
        count = spark.read.format("delta").table(t).count()
        print(f"  {t} → {count:,} wierszy OK")
    except Exception as e:
        print(f"  {t} → BŁĄD: {e}")


# In[2]:


import os


def resolve_base_path() -> str:
    """
    Zwraca pełną ścieżkę do katalogu Tables/ w bieżącym Lakehouse.

    Ścieżka nie jest wpisana na sztywno, bo zawiera identyfikatory workspace'u
    i lakehouse'u — po sklonowaniu workspace'u lub migracji byłyby nieaktualne.
    Zamiast tego odczytujemy ją ze środowiska w czasie działania notebooka.

    Kolejność: zmienna środowiskowa (przydatna przy uruchomieniu z pipeline'u)
    → automatyczne wykrycie przez notebookutils.
    """
    override = os.environ.get("NBP_LAKEHOUSE_TABLES_PATH")
    if override:
        return override.rstrip("/")

    entries = notebookutils.fs.ls("Tables")
    if not entries:
        raise RuntimeError(
            "Katalog Tables/ jest pusty — uruchom najpierw notebooki Bronze i Silver."
        )
    # entries[0].path to pełny abfss:// do konkretnej tabeli; obcinamy nazwę tabeli
    return entries[0].path.rstrip("/").rsplit("/", 1)[0]


BASE_PATH = resolve_base_path()
print("Base path:", BASE_PATH)

for t in ["silver_exchange_rates_a", "silver_exchange_rates_c", "silver_exchange_rates_merged", "silver_gold_prices"]:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {t} USING DELTA LOCATION '{BASE_PATH}/{t}'")
    print(f"  Zarejestrowano: {t}")


# In[3]:


# ============================================================
# Notebook: gold_star_schema.py
# Warstwa: Gold
# Cel: Budowa modelu gwiazdy (Star Schema) z danych Silver
#      - dim_date         (wymiar daty)
#      - dim_currency     (wymiar waluty)
#      - fact_exchange_rate (fakty — kursy walut)
#      - fact_gold_price   (fakty — ceny złota)
# ============================================================

from pyspark.sql.functions import (
    col, lit, year, quarter, month, dayofweek, dayofmonth,
    weekofyear, date_format, when, current_timestamp,
    row_number, lag, round as spark_round, coalesce
)
from pyspark.sql.window import Window

# --- Odczyt danych Silver ---------------------------------------------------
df_rates  = spark.read.format("delta").table("silver_exchange_rates_merged")
df_gold   = spark.read.format("delta").table("silver_gold_prices")


# =============================================================================
# 1. DIM_DATE — wymiar daty
# =============================================================================
print("=== Gold: dim_date ===")

# Zbieramy unikalne daty ze wszystkich źródeł
df_all_dates = (
    df_rates.select(col("effective_date").alias("full_date"))
    .union(df_gold.select(col("effective_date").alias("full_date")))
    .distinct()
)

df_dim_date = (
    df_all_dates
    .withColumn("date_key",      date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year",          year("full_date"))
    .withColumn("quarter",       quarter("full_date"))
    .withColumn("month",         month("full_date"))
    .withColumn("month_name",    date_format("full_date", "MMMM"))
    .withColumn("week_of_year",  weekofyear("full_date"))
    .withColumn("day_of_month",  dayofmonth("full_date"))
    .withColumn("day_of_week",   dayofweek("full_date"))
    .withColumn("day_name",      date_format("full_date", "EEEE"))
    .withColumn("is_weekend",    when(dayofweek("full_date").isin(1, 7), True).otherwise(False))
    .orderBy("full_date")
)

df_dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_date")
print(f"  Zapisano {df_dim_date.count()} wierszy")


# =============================================================================
# 2. DIM_CURRENCY — wymiar waluty
# =============================================================================
print("\n=== Gold: dim_currency ===")

window_cur = Window.orderBy("currency_code")

df_dim_currency = (
    df_rates
    .select("currency_code", "currency_name")
    .filter(col("currency_code").isNotNull())
    .distinct()
    .withColumn("currency_key", row_number().over(window_cur))
    .withColumn("is_active", lit(True))
    .select("currency_key", "currency_code", "currency_name", "is_active")
)

df_dim_currency.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_currency")
print(f"  Zapisano {df_dim_currency.count()} walut")


# =============================================================================
# 3. FACT_EXCHANGE_RATE — fakty kursów walut
# =============================================================================
print("\n=== Gold: fact_exchange_rate ===")

# Dołączenie kluczy surogatowych
df_fact_rates = (
    df_rates
    .join(
        df_dim_currency.select("currency_key", "currency_code"),
        on="currency_code",
        how="left"
    )
    .join(
        df_dim_date.select(col("full_date"), col("date_key")),
        df_rates["effective_date"] == df_dim_date["full_date"],
        how="left"
    )
)

# Obliczenie dziennej zmiany kursu (window function)
window_rate = Window.partitionBy("currency_key").orderBy("effective_date")

df_fact_rates_final = (
    df_fact_rates
    .withColumn("prev_mid", lag("mid_rate", 1).over(window_rate))
    .withColumn(
        "daily_change",
        spark_round(col("mid_rate") - col("prev_mid"), 6)
    )
    .withColumn(
        "daily_change_pct",
        spark_round((col("daily_change") / col("prev_mid")) * 100, 4)
    )
    .select(
        "date_key",
        "currency_key",
        "mid_rate",
        "bid_rate",
        "ask_rate",
        "spread",
        "daily_change",
        "daily_change_pct",
    )
    .withColumn("_load_timestamp", current_timestamp())
)

df_fact_rates_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_exchange_rate")
print(f"  Zapisano {df_fact_rates_final.count()} wierszy")


# =============================================================================
# 4. FACT_GOLD_PRICE — fakty cen złota
# =============================================================================
print("\n=== Gold: fact_gold_price ===")

# Dołączenie date_key
df_fact_gold = (
    df_gold
    .join(
        df_dim_date.select(col("full_date"), col("date_key")),
        df_gold["effective_date"] == df_dim_date["full_date"],
        how="left"
    )
)

# Dzienna zmiana ceny złota
window_gold = Window.orderBy("effective_date")

df_fact_gold_final = (
    df_fact_gold
    .withColumn("prev_price", lag("gold_price_pln", 1).over(window_gold))
    .withColumn(
        "daily_change",
        spark_round(col("gold_price_pln") - col("prev_price"), 4)
    )
    .withColumn(
        "daily_change_pct",
        spark_round((col("daily_change") / col("prev_price")) * 100, 4)
    )
    .select(
        "date_key",
        "gold_price_pln",
        "daily_change",
        "daily_change_pct",
    )
    .withColumn("_load_timestamp", current_timestamp())
)

df_fact_gold_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_gold_price")
print(f"  Zapisano {df_fact_gold_final.count()} wierszy")


# =============================================================================
# 5. OPTYMALIZACJA TABEL GOLD
# =============================================================================
print("\n=== Optymalizacja tabel Gold ===")

spark.sql("OPTIMIZE gold_fact_exchange_rate ZORDER BY (date_key, currency_key)")
spark.sql("OPTIMIZE gold_fact_gold_price ZORDER BY (date_key)")
print("  ZORDER wykonany")


# =============================================================================
# 6. PODSUMOWANIE
# =============================================================================
print("\n" + "=" * 60)
print("=== GOLD — PODSUMOWANIE ===")
print("=" * 60)

for t in ["gold_dim_date", "gold_dim_currency", "gold_fact_exchange_rate", "gold_fact_gold_price"]:
    try:
        c = spark.read.format("delta").table(t).count()
        print(f"  {t:35s} → {c:>8,} wierszy")
    except Exception as e:
        print(f"  {t:35s} → BŁĄD: {e}")

print("\n=== GOLD STAR SCHEMA DONE ===")
print("Tabele gotowe do podłączenia w Power BI (DirectLake)")

