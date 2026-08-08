#!/usr/bin/env python
# coding: utf-8

# ## silver_transform.py
# 
# New notebook

# In[6]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%configure -f
# {
#     "defaultLakehouse": {
#         "name": "nbp_lakehouse"
#     }
# }


# In[1]:


# ============================================================
# Notebook: silver_transform.py
# Warstwa: Silver
# Cel: Oczyszczenie danych Bronze → Silver
#      - rzutowanie typów (daty, liczby)
#      - usunięcie duplikatów
#      - walidacja zakresów (kursy > 0)
#      - ujednolicenie schematów tabel A i C
# ============================================================

from pyspark.sql.functions import (
    col, to_date, trim, upper, initcap,
    current_timestamp, lit, row_number
)
from pyspark.sql.window import Window

# =============================================================================
# 1. SILVER — TABELA A (kursy średnie)
# =============================================================================
print("=== Silver: Tabela A ===")

df_bronze_a = spark.read.format("delta").table("bronze_nbp_table_a")

df_silver_a = (
    df_bronze_a
    # Rzutowanie typów
    .withColumn("effective_date", to_date(col("effective_date"), "yyyy-MM-dd"))
    .withColumn("trading_date",   to_date(col("trading_date"), "yyyy-MM-dd"))
    .withColumn("mid_rate",       col("mid_rate").cast("double"))
    .withColumn("currency_code",  upper(trim(col("currency_code"))))
    .withColumn("currency_name",  initcap(trim(col("currency_name"))))

    # Walidacja — kurs średni musi być dodatni
    .filter(col("mid_rate").isNotNull() & (col("mid_rate") > 0))

    # Usunięcie duplikatów (ten sam kurs, ten sam dzień, ta sama waluta)
    .dropDuplicates(["effective_date", "currency_code"])

    # Wybór kolumn (bez raw_json i metadanych Bronze)
    .select(
        "table_no",
        "table_type",
        "effective_date",
        "trading_date",
        "currency_code",
        "currency_name",
        "mid_rate",
        lit(None).cast("double").alias("bid_rate"),   # brak w tabeli A
        lit(None).cast("double").alias("ask_rate"),    # brak w tabeli A
    )
    # Metadane Silver
    .withColumn("_silver_timestamp", current_timestamp())
    .withColumn("_source_table", lit("bronze_nbp_table_a"))
)

df_silver_a.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_exchange_rates_a")
print(f"  Zapisano {df_silver_a.count()} wierszy → silver_exchange_rates_a")


# =============================================================================
# 2. SILVER — TABELA C (kursy kupna/sprzedaży)
# =============================================================================
print("\n=== Silver: Tabela C ===")

df_bronze_c = spark.read.format("delta").table("bronze_nbp_table_c")

df_silver_c = (
    df_bronze_c
    .withColumn("effective_date", to_date(col("effective_date"), "yyyy-MM-dd"))
    .withColumn("trading_date",   to_date(col("trading_date"), "yyyy-MM-dd"))
    .withColumn("bid_rate",       col("bid_rate").cast("double"))
    .withColumn("ask_rate",       col("ask_rate").cast("double"))
    .withColumn("currency_code",  upper(trim(col("currency_code"))))
    .withColumn("currency_name",  initcap(trim(col("currency_name"))))

    # Walidacja — bid i ask muszą być dodatnie, bid < ask
    .filter(
        col("bid_rate").isNotNull()
        & col("ask_rate").isNotNull()
        & (col("bid_rate") > 0)
        & (col("ask_rate") > 0)
        & (col("bid_rate") < col("ask_rate"))
    )

    .dropDuplicates(["effective_date", "currency_code"])

    .select(
        "table_no",
        "table_type",
        "effective_date",
        "trading_date",
        "currency_code",
        "currency_name",
        lit(None).cast("double").alias("mid_rate"),   # brak w tabeli C
        "bid_rate",
        "ask_rate",
    )
    .withColumn("_silver_timestamp", current_timestamp())
    .withColumn("_source_table", lit("bronze_nbp_table_c"))
)

df_silver_c.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_exchange_rates_c")
print(f"  Zapisano {df_silver_c.count()} wierszy → silver_exchange_rates_c")


# =============================================================================
# 3. SILVER — ZŁĄCZONA TABELA KURSÓW (A + C)
# =============================================================================
# Łączymy tabele A i C po dacie i walucie, żeby w jednym wierszu mieć
# mid_rate (z A) oraz bid/ask (z C). To ułatwi budowę Gold.
print("\n=== Silver: Złączenie A + C ===")

df_merged = (
    df_silver_a.alias("a")
    .join(
        df_silver_c.alias("c"),
        on=["effective_date", "currency_code"],
        how="full_outer"    # full outer — bo A i C mogą mieć różne daty
    )
    .select(
        col("effective_date"),
        col("a.currency_code").alias("currency_code"),
        # Bierzemy nazwę waluty z tego źródła, które istnieje
        col("a.currency_name").alias("currency_name"),
        col("a.table_no").alias("table_no_a"),
        col("c.table_no").alias("table_no_c"),
        col("a.mid_rate"),
        col("c.bid_rate"),
        col("c.ask_rate"),
        # Spread = ask - bid (koszt transakcyjny)
        (col("c.ask_rate") - col("c.bid_rate")).alias("spread"),
    )
    .withColumn("_silver_timestamp", current_timestamp())
)

df_merged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_exchange_rates_merged")
print(f"  Zapisano {df_merged.count()} wierszy → silver_exchange_rates_merged")


# =============================================================================
# 4. SILVER — ZŁOTO
# =============================================================================
print("\n=== Silver: Złoto ===")

df_bronze_gold = spark.read.format("delta").table("bronze_nbp_gold")

df_silver_gold = (
    df_bronze_gold
    .withColumn("effective_date",  to_date(col("effective_date"), "yyyy-MM-dd"))
    .withColumn("gold_price_pln",  col("gold_price_pln").cast("double"))

    # Walidacja
    .filter(col("gold_price_pln").isNotNull() & (col("gold_price_pln") > 0))

    .dropDuplicates(["effective_date"])

    .select(
        "effective_date",
        "gold_price_pln",
    )
    .withColumn("_silver_timestamp", current_timestamp())
    .withColumn("_source_table", lit("bronze_nbp_gold"))
)

df_silver_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_gold_prices")
print(f"  Zapisano {df_silver_gold.count()} wierszy → silver_gold_prices")


# =============================================================================
# 5. PODSUMOWANIE
# =============================================================================
print("\n" + "=" * 60)
print("=== SILVER — PODSUMOWANIE ===")
print("=" * 60)

for table_name in [
    "silver_exchange_rates_a",
    "silver_exchange_rates_c",
    "silver_exchange_rates_merged",
    "silver_gold_prices",
]:
    try:
        count = spark.read.format("delta").table(table_name).count()
        print(f"  {table_name:40s} → {count:>8,} wierszy")
    except Exception as e:
        print(f"  {table_name:40s} → BŁĄD: {e}")

print("\n=== SILVER DONE ===")


# In[5]:


# Sprawdźmy co Spark widzi
print("Domyślny katalog:", spark.catalog.currentDatabase())
print("\nDostępne tabele:")
for t in spark.catalog.listTables():
    print(f"  {t.database}.{t.name} (type: {t.tableType})")


# In[9]:


# 1. omyślny lakehouse dla sparka
print("Domyślny katalog:", spark.catalog.currentDatabase())

# 2. Sprawdzenie co fizycznie jest w Lakehouse
files = notebookutils.fs.ls("Tables/")
for f in files:
    print(f"  {f.name}  (isDir: {f.isDir})")


# In[13]:


for table_name in ["bronze_nbp_table_a", "bronze_nbp_table_c", "bronze_nbp_gold"]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION 'Tables/{table_name}'
    """)
    print(f"  Zarejestrowano: {table_name}")

# Weryfikacja
for t in spark.catalog.listTables():
    print(f"  {t.name} ({t.tableType})")


# In[14]:


# Pobranie pełnej ścieżki do Lakehouse
lakehouse_path = notebookutils.fs.getMountPath("/default")
print("Mount path:", lakehouse_path)

# Albo sprawdźmy pełną ścieżkę abfss
files = notebookutils.fs.ls("Tables/bronze_nbp_table_a")
print("Pełna ścieżka:", files[0].path)


# In[16]:


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
            "Katalog Tables/ jest pusty — uruchom najpierw notebook warstwy Bronze."
        )
    # entries[0].path to pełny abfss:// do konkretnej tabeli; obcinamy nazwę tabeli
    return entries[0].path.rstrip("/").rsplit("/", 1)[0]


BASE_PATH = resolve_base_path()
print("Base path:", BASE_PATH)

for table_name in ["bronze_nbp_table_a", "bronze_nbp_table_c", "bronze_nbp_gold"]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{BASE_PATH}/{table_name}'
    """)
    print(f"  Zarejestrowano: {table_name}")

# Weryfikacja
for t in spark.catalog.listTables():
    print(f"  {t.name} ({t.tableType})")

