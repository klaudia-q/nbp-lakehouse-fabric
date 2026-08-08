# Pipeline danych NBP — architektura medalionowa (Bronze / Silver / Gold)

*[English version](README.en.md)*

Projekt ETL pobierający dane z publicznego API Narodowego Banku Polskiego, przetwarzający je w architekturze medalionowej na **Microsoft Fabric** (PySpark + Delta Lake) i udostępniający jako model gwiazdy w **Power BI** (tryb DirectLake).

## Źródła danych

| Endpoint NBP | Zawartość |
|---|---|
| `exchangerates/tables/A` | Średnie kursy walut (`mid`) |
| `exchangerates/tables/C` | Kursy kupna/sprzedaży (`bid` / `ask`) |
| `cenyzlota` | Ceny złota w PLN |

Zakres danych: od `2020-01-01` do dnia uruchomienia.

## Przepływ danych

```
API NBP  →  Bronze (surowe Delta)  →  Silver (czyste, walidowane)  →  Gold (star schema)  →  Power BI
```

| Warstwa | Notebook | Tabele wyjściowe |
|---|---|---|
| **Bronze** | `code-bronze/bronze_nbp_all_sources.py` | `bronze_nbp_table_a`, `bronze_nbp_table_c`, `bronze_nbp_gold` |
| **Silver** | `code-silver/silver_transform.py` | `silver_exchange_rates_a`, `silver_exchange_rates_c`, `silver_exchange_rates_merged`, `silver_gold_prices` |
| **Gold** | `code-gold/gold_star_schema.py` | `gold_dim_date`, `gold_dim_currency`, `gold_fact_exchange_rate`, `gold_fact_gold_price` |

![Warstwy Lakehouse w Microsoft Fabric](lakehouse.png)

## Model semantyczny

Dwie tabele faktów (kursy walut, ceny złota) połączone z wymiarami daty i waluty przez klucze surogatowe `date_key` / `currency_key`.

![Model semantyczny w Power BI](semantic-model.png)

## Raport

![Dashboard w Power BI](dashboard.png)

## Struktura repozytorium

| Ścieżka | Opis |
|---|---|
| `code-bronze/`, `code-silver/`, `code-gold/` | Notebooki PySpark |
| `pipeline/` | Zrzuty ekranu konfiguracji orkiestratora w Fabric |
| `silver-layer-errors/` | Dokumentacja napotkanych błędów i sposobu ich rozwiązania |
| `nbp_raport.pbix` | Raport Power BI |
| `DOKUMENTACJA_PL.md` | Pełna dokumentacja techniczna |

## Uruchomienie

Wymagania: workspace Microsoft Fabric z Lakehouse (`nbp_lakehouse`), środowisko Spark z Delta Lake, dostęp sieciowy do `api.nbp.pl`.

![Workspace w Microsoft Fabric](fabric-workspace.png)

1. Zaimportuj pliki `.py` jako notebooki Fabric i podepnij je do Lakehouse.
2. Uruchom w kolejności: **bronze → silver → gold**.
3. W pipeline Fabric ustaw orkiestrację sekwencyjną (zrzuty w `pipeline/`).
4. Podłącz `nbp_raport.pbix` do tabel Gold w trybie DirectLake.

Notebooki są idempotentne — każde uruchomienie nadpisuje tabele (`mode("overwrite")`), więc powtórny run nie duplikuje danych.

## Kluczowe decyzje projektowe

- **Jawne schematy Spark w Bronze** — tabela A nie zwraca `bid`/`ask`, a złoto nie ma pól walutowych; przy samych wartościach `None` Spark zgłasza `CANNOT_DETERMINE_TYPE`.
- **Dzielenie zapytań na okna 93-dniowe** — twardy limit API NBP na pojedyncze zapytanie.
- **Odporność na błędy HTTP** — błąd pojedynczego requestu nie przerywa całego joba (zwracana jest pusta lista).
- **Walidacje w Silver** — kursy muszą być dodatnie, dla tabeli C dodatkowo `bid < ask`.
- **`ZORDER` na tabelach faktów** — optymalizacja odczytów po `date_key` / `currency_key`.

## Znane ograniczenia

Świadomie odłożone do dalszych etapów (planowana migracja na Databricks):

- Pełny reload zamiast ładowania inkrementalnego — brak mechanizmu CDC/append.
- Bronze zapisuje przez ścieżkę (`.save`), a Silver/Gold czytają przez nazwę tabeli — stąd ręczna rejestracja `CREATE TABLE ... LOCATION`.
- W `silver_exchange_rates_merged` pola `currency_name` i `table_no_a` pochodzą wyłącznie z tabeli A; dla wiersza obecnego tylko w tabeli C będą `NULL`.
- Brak testów jednostkowych i pliku `requirements.txt`.

Szczegóły: **[DOKUMENTACJA_PL.md](DOKUMENTACJA_PL.md)**

## Źródło danych

[API NBP](https://api.nbp.pl/) — publiczne, bez uwierzytelniania. Dane udostępniane przez Narodowy Bank Polski.

## Licencja

Kod udostępniony na licencji [MIT](LICENSE) — © 2026 Klaudia Kuszczak.

Licencja obejmuje wyłącznie kod źródłowy. Dane pochodzące z API NBP podlegają warunkom korzystania określonym przez Narodowy Bank Polski.
