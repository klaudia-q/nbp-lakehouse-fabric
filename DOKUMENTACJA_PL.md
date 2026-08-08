# Dokumentacja techniczna — Pipeline danych NBP (Bronze / Silver / Gold)

Dokument opisuje implementację poszczególnych warstw pipeline'u: co robi każdy notebook, jakie decyzje projektowe za nim stoją i jakie ograniczenia są mi znane. Ogólny opis projektu i instrukcja uruchomienia znajdują się w [README.md](README.md).

## 1. Przegląd

Pipeline realizuje architekturę medalionową (Bronze → Silver → Gold) w Microsoft Fabric na PySpark i Delta Lake. Dane pochodzą z trzech endpointów publicznego API NBP:

- **Tabela A** — średnie kursy walut (`mid`)
- **Tabela C** — kursy kupna/sprzedaży (`bid`/`ask`)
- **Ceny złota** (`cenyzlota`)

Wynikiem końcowym jest model gwiazdy podłączony do Power BI w trybie DirectLake (`nbp_raport.pbix`).

| Katalog | Zawartość |
|---|---|
| `code-bronze/` | `bronze_nbp_all_sources.py` — pobieranie danych z API NBP |
| `code-silver/` | `silver_transform.py` — czyszczenie i standaryzacja danych |
| `code-gold/` | `gold_star_schema.py` — budowa modelu gwiazdy |
| `pipeline/`, `silver-layer-errors/` | Zrzuty ekranu z konfiguracji orkiestracji w Fabric i napotkanych błędów |

---

## 2. Warstwa Bronze — `bronze_nbp_all_sources.py`

Warstwa pobiera surowe dane z trzech endpointów NBP dla tego samego zakresu dat i zapisuje je do osobnych tabel Delta. Nie wykonuję tu żadnych transformacji biznesowych — jedynie spłaszczam JSON do struktury tabelarycznej.

### Budowa notebooka

`SCHEMA_EXCHANGE_RATES` i `SCHEMA_GOLD` to jawnie zdefiniowane schematy Spark. Zdecydowałam się je zapisać wprost, ponieważ Spark nie potrafi wywnioskować typu kolumny, gdy wszystkie wartości w batchu są `None` — zgłasza wtedy `CANNOT_DETERMINE_TYPE`. Dotyczy to tabeli A, która nigdy nie zwraca `bid`/`ask`, oraz danych o złocie, które nie mają pól walutowych.

`SOURCES` to słownik konfiguracyjny — każde źródło opisuję szablonem URL, ścieżką zapisu Delta, nazwą parsera i typem tabeli. Dzięki temu dodanie kolejnego endpointu NBP nie wymaga zmian w logice, tylko wpisu w słowniku.

`generate_date_ranges()` dzieli zakres `DATE_FROM`–`DATE_TO` na fragmenty po maksymalnie 93 dni. To twardy limit API NBP na pojedyncze zapytanie.

`fetch_nbp(url)` wykonuje request HTTP. Kod `404` traktuję jako „brak danych w tym zakresie" i zwracam pustą listę — API zwraca go dla okresów bez notowań. Błędy sieci i parsowania JSON przechwytuję i loguję, zwracając pustą listę zamiast przerywać całe zadanie.

`parse_exchange_rates()` i `parse_gold()` spłaszczają odpowiedzi API do list rekordów zgodnych ze schematem. Rozdzieliłam je, bo struktura odpowiedzi dla kursów i dla złota jest zupełnie inna.

Główna pętla iteruje po źródłach i zakresach dat, a po zebraniu wszystkich rekordów zapisuje je do Delta w trybie `overwrite`, dodając kolumny audytowe `_ingestion_timestamp`, `_source` i `_source_name`. Na końcu notebook odczytuje każdą tabelę Bronze i wypisuje liczbę wierszy — to prosta weryfikacja, że zapis się powiódł.

### Znane ograniczenia

Zapis w trybie `overwrite` oznacza, że każde uruchomienie nadpisuje całą historię danymi świeżo pobranymi z API. Przy tej skali danych to działa, ale nie ma mechanizmu ładowania przyrostowego ani CDC — przy dłuższej historii lub częstszym harmonogramie będzie to wymagało zmiany na `MERGE`.

Błędy pojedynczych zapytań HTTP są przechwytywane po cichu. Chroni to zadanie przed przerwaniem przy chwilowej awarii API, ale nie ma alarmowania, jeśli jakiś zakres dat systematycznie zwraca puste dane. Do dodania: zliczanie nieudanych requestów i ostrzeżenie, gdy przekroczą próg.

---

## 3. Warstwa Silver — `silver_transform.py`

Warstwa czyści dane z Bronze: rzutuje typy, waliduje wartości, usuwa duplikaty, ujednolica schematy tabel A i C oraz je łączy.

### Transformacje

**`silver_exchange_rates_a`** (z `bronze_nbp_table_a`) — rzutowanie dat przez `to_date` i `mid_rate` na `double`, normalizacja tekstu (`currency_code` na wielkie litery, `currency_name` przez `initcap`), walidacja `mid_rate > 0`, deduplikacja po parze (`effective_date`, `currency_code`). Dodaję puste kolumny `bid_rate`/`ask_rate`, żeby schemat pasował do tabeli C przy późniejszym łączeniu.

**`silver_exchange_rates_c`** (z `bronze_nbp_table_c`) — analogicznie, z dodatkową regułą biznesową: `bid_rate < ask_rate`. Spread musi być dodatni, więc wiersze łamiące ten warunek odrzucam jako błędne.

**`silver_exchange_rates_merged`** — `FULL OUTER JOIN` tabel A i C po dacie i kodzie waluty, z wyliczeniem `spread = ask_rate - bid_rate`. Złączenie pełne zewnętrzne, bo obie tabele mogą teoretycznie mieć różne zestawy dat.

Nazwa waluty i numer tabeli pobierane są w tym złączeniu wyłącznie z aliasu `a`. Jeśli wiersz istnieje tylko w tabeli C, `currency_name` będzie `NULL`. W praktyce NBP publikuje te same waluty w obu tabelach każdego dnia roboczego, więc na posiadanych danych sytuacja nie wystąpiła. Kierunek poprawki pozostaje otwarty — patrz sekcja 5.

**`silver_gold_prices`** (z `bronze_nbp_gold`) — rzutowanie typów, walidacja `gold_price_pln > 0`, deduplikacja po dacie.

Na końcu notebook wypisuje liczbę wierszy w każdej tabeli Silver.

### Kod diagnostyczny

Końcowe komórki notebooka (`In[5]`, `In[9]`, `In[13]`, `In[16]`) zawierają kod, którym diagnozowałam problem z widocznością tabel w Lakehouse — przebieg tego debugowania dokumentują zrzuty w `silver-layer-errors/`. Kod sprawdza aktualny katalog Sparka i listę zarejestrowanych tabel, a następnie rejestruje tabele Bronze jako tabele katalogowe przez `CREATE TABLE ... USING DELTA LOCATION` — najpierw ścieżką względną `Tables/...`, a po niepowodzeniu pełną ścieżką `abfss://`.

Ścieżka bazowa nie jest wpisana na sztywno — wyznacza ją funkcja `resolve_base_path()`. Sprawdza najpierw zmienną środowiskową `NBP_LAKEHOUSE_TABLES_PATH` (przydatną przy uruchomieniu z pipeline'u), a gdy jej nie ma, odczytuje ścieżkę z `notebookutils.fs.ls("Tables")` i obcina nazwę tabeli. Dzięki temu notebook działa w dowolnym workspace bez edycji kodu, a identyfikatory środowiska nie trafiają do repozytorium.

---

## 4. Warstwa Gold — `gold_star_schema.py`

Warstwa buduje model gwiazdy gotowy do podłączenia w Power BI przez DirectLake.

### Tabele wynikowe

**`gold_dim_date`** — wymiar daty zbudowany z unii unikalnych dat z `silver_exchange_rates_merged` i `silver_gold_prices`. Zawiera `date_key` w formacie `yyyyMMdd` jako liczbę całkowitą, rok, kwartał, miesiąc wraz z nazwą, tydzień roku, dzień miesiąca i tygodnia, nazwę dnia oraz flagę weekendu.

**`gold_dim_currency`** — wymiar waluty z kluczem surogatowym `currency_key` nadawanym przez `row_number()` po kodzie waluty, plus flaga `is_active`. Flaga jest zawsze `True`, bo nie implementowałam logiki dezaktywacji walut wycofanych z obrotu.

Klucz nadawany jest od nowa przy każdym uruchomieniu. Przy obecnym `overwrite` całego modelu jest to spójne, bo tabele faktów przeliczam w tym samym przebiegu. Przy przejściu na ładowanie przyrostowe przestanie być — pojawienie się nowej waluty przesunie numerację i wcześniej zapisane fakty zaczną wskazywać na inny wiersz wymiaru. Rozwiązanie tego problemu opisuję w sekcji 5.

**`gold_fact_exchange_rate`** — tabela faktów kursów walut połączona z wymiarami przez `currency_key` i `date_key`. Poza `mid_rate`, `bid_rate`, `ask_rate` i `spread` zawiera dzienną zmianę kursu (`daily_change`, `daily_change_pct`), liczoną funkcją okienkową `lag()` partycjonowaną po walucie.

**`gold_fact_gold_price`** — analogiczna tabela dla cen złota, bez partycjonowania, ponieważ to pojedynczy szereg czasowy.

Na końcu wykonuję `OPTIMIZE ... ZORDER BY` na obu tabelach faktów, po `date_key` i `currency_key`. Porządkuje to fizyczny układ plików Delta pod kątem filtrów, które Power BI wykonuje najczęściej.

### Uwagi implementacyjne

`dim_currency` dziedziczy opisane wyżej zachowanie z `currency_name` — wymiar budowany jest z tabeli scalonej, więc ewentualne `NULL`-e przechodzą dalej.

`daily_change_pct` dzieli przez poprzednią wartość kursu lub ceny. Gdyby ta była `0` lub `NULL`, wynikiem jest `NULL` — Spark nie zgłasza wyjątku dzielenia przez zero, więc nie dodawałam osobnego zabezpieczenia.

Pierwsze komórki notebooka (`In[1]`, `In[2]`) sprawdzają widoczność tabel Silver i rejestrują je w katalogu — ścieżkę wyznacza ta sama funkcja `resolve_base_path()` co w warstwie Silver.

---

## 5. Do zrobienia w kolejnych etapach

Projekt ma być przeniesiony na Databricks i przy tej okazji planuję następujące zmiany:

- zastąpienie pełnego przeładowania ładowaniem przyrostowym (`MERGE` zamiast `overwrite`),
- wyniesienie zakresu dat do konfiguracji zewnętrznej zamiast wartości w kodzie,
- ujednolicenie zapisu w Bronze — obecnie zapisuję przez ścieżkę (`.save`), a w kolejnych warstwach odczytuję przez nazwę tabeli, co wymusza ręczną rejestrację `CREATE TABLE ... LOCATION`,
- zliczanie nieudanych requestów HTTP i ostrzeganie przy przekroczeniu progu,
- testy jednostkowe dla funkcji parsujących i plik `requirements.txt`.

Dwie kwestie zostawiam świadomie jako otwarte, bo obie wychodzą dopiero przy przejściu na ładowanie przyrostowe i w obu widzę argumenty po dwóch stronach:

- **klucz surogatowy w `dim_currency`** — trwały mapping przez `MERGE` czy rezygnacja z surogatu na rzecz `currency_code` jako klucza naturalnego (wymiar jest mały i bezhistoryczny),
- **typ złączenia tabel A i C** — zostawić `FULL OUTER JOIN` i uzupełnić `coalesce()`, czy zejść do `INNER JOIN` i logować przypadki bez pary jako anomalię.
