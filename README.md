# NASA Exoplanets Data Lakehouse

### Výběr databáze
V této seminární práci budu pracovat z daty z vládního NASA archivu exoplanet.: https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=PS&constraint=default_flag%20%3E0
V tomto archivu je uloženo kolem 38 000 těles celkově, z toho je asi přibližně 5 800 těles již potvrzených exoplanet. Z této stránky jsem stáhnul část tabulky obsahují všechny objevené exoplanety a informace o nich včetně datumu publikace v csv formátu. + zkombinuji již s jednou tabulkou od Aditya Mishra ML v csv formátu, která také obsahuje informace ze NASA archivu s lepším popisem: https://www.kaggle.com/datasets/adityamishraml/nasaexoplanets.

### Výběr databázového systému
Pro vytvoření DLH systému jsem si vybral DuckDB databázový systém (https://duckdb.org/), který umožňuje vytvářet dimenze dat a datová jezera s daty. Tento databázový systém zároveň umožňuje práci v Pythonu.

Pro práci s databází nejdříve použijeme DLH_exoplanets_commit.py, přes který se nám vytvoří Warehouse databáze s hvězdicovým schématem a uložíme jí do Lakehouse storage a potom budem s DLH databází pracovat v souboru DLH_exoplanets_storages_use.py

### Nastavení DuckDB v python
Nejdřív nainstalujeme potřebný moduly.

`pip install duckdb, pandas`

Připojíme se k DuckDB databázi
```python
# import modules
import duckdb
import pandas as pd
# connect to DuckDB
con = duckdb.connect()
```



### Vytvoření hlavní tabulky
Zobrazíme si csv soubory.
```python
db = duckdb.read_csv("adityamishraml/nasaexoplanets/versions/2/cleaned_5250.csv")
duckdb.sql("SELECT * FROM 'db'").show()

db2 = duckdb.read_csv("PS_2025.04.28_06.13.44.csv")
duckdb.sql("SELECT * FROM 'db2'").show()
```

Vytvoření tabulky, kde si vezmu všechno z první tabulky a potom jí spojím s potřebný sloupci pro práci s dimenzemi z druhé tabulky.
```python
con.execute("""
        CREATE TABLE exoplanets AS
        SELECT e.*, n.pl_pubdate, n.releasedate
        FROM read_csv_auto('adityamishraml/nasaexoplanets/versions/2/cleaned_5250.csv') e
        LEFT JOIN read_csv_auto("PS_2025.04.28_06.13.44.csv") n ON LOWER(e.name) = LOWER(n.pl_name)
    """)
```

### Zobrazení tabulek
Zobrazíme si tabulku.:
```python
con.table("exoplanets").show()
```
Nová tabulka vypadá takhle.:
![image](https://github.com/user-attachments/assets/920b67f9-b038-4d97-8f65-49c39bf062d1)

Zobrazíme si popis sloupců v tabulce.:
```python
print(con.execute("DESCRIBE exoplanets").fetchdf())
```
![image](https://github.com/user-attachments/assets/fbb71914-e868-45d5-a7bd-59d4e1951605)

#### ERD diagram
Nejdřív si uděláme ERD diagram, abychom měli přehled o proměnných v tabulce.
<p align="center">
  <img src="https://github.com/user-attachments/assets/541b854c-744b-47c9-84bc-b29abcfbb388" alt="ERD diagram image"/>
</p>

Takhle prozatím vypadá má tabulka.

### Query vyhledávání
Vyzkoušíme query dotaz nad nově vytvořenou tabulkou.
```python
print(con.execute("""
        SELECT name, discovery_year
        FROM exoplanets
        WHERE discovery_year > 2010
        ORDER BY discovery_year ASC
    """).fetchdf())
```

### Vytvoření dimenzionálních tabulek
Vytvořím 9 dimenzionálních tabulek. 
- dim_planet_type, která obsahuje typy planet. 
- dim_detection_method, která obsahuje metody detekce planety.
- dim_stellar_type, která obsahuje kategorie o zářivosti hvězdy dané planety podle jeí vzdálenosti od hvězdy very bright, Bright, Moderate, Dim a Very dim.
- dim_mass_category, která obsahuje hmotnosti planet rozdělené do kategorií Very Low Mass, Low Mass, Medium Mass a High Mass.
- dim_distance_category, která obsahuje vzdálenosti planet rozdělených do kategorií Very Close (<10 ly), Close (<100 Ly), Medium (< 1000 ly) a far (>1000 ly).
- dim_orbit_category, která obsahuje planety rozdělené do kategorií podle toho, jak daleko obíhají od své hvězdy Very Short, Short, Moderate a Long.
- dim_brightness_category, která obsahuje planety rozdělené do kategorií podle jejich jasu Very Bright, Bright, Dim, Very Dim.
- dim_discovery_era, která obsahuje planety rozdělené do kategorií podle toho v jaké éře byli objeveny <2000, Early 21st Century, Kepler Era a
Modern Era
- dim_date, která obsahuje datumy zveřejnění planet, celé datum z tabulky rozdělené na rok, měsíc, den, název měsíce a název dnu.
```python
## vytvoření dim tabulek
# dim_planet_type
con.execute("""
    CREATE OR REPLACE TABLE dim_planet_type AS
    SELECT ROW_NUMBER() OVER () AS planet_type_id, planet_type
    FROM (
        SELECT DISTINCT planet_type
        FROM exoplanets
        WHERE planet_type IS NOT NULL
    ) t;
""")
# dim_detection_method
con.execute("""
    CREATE OR REPLACE TABLE dim_detection_method AS
    SELECT ROW_NUMBER() OVER () AS detection_method_id, detection_method
    FROM (
        SELECT DISTINCT detection_method
        FROM exoplanets
        WHERE detection_method IS NOT NULL
    ) t;
""")
# dim_stellar_type
con.execute("""
    CREATE OR REPLACE TABLE dim_stellar_type AS
    SELECT ROW_NUMBER() OVER () AS stellar_type_id, distance, stellar_magnitude,
        CASE
            WHEN stellar_magnitude < 0 THEN 'very bright'
            WHEN stellar_magnitude BETWEEN 0 AND 2 THEN 'bright'
            WHEN stellar_magnitude BETWEEN 2 AND 5 THEN 'moderate'
            WHEN stellar_magnitude BETWEEN 5 AND 10 THEN 'dim'
            ELSE 'very dim'
        END AS brightness_category
    FROM (
        SELECT DISTINCT distance, stellar_magnitude
        FROM exoplanets
        WHERE distance IS NOT NULL AND stellar_magnitude IS NOT NULL
    ) t;
""")
# dim_mass_category
con.execute("""
    CREATE OR REPLACE TABLE dim_mass_category AS
    SELECT ROW_NUMBER() OVER () AS mass_category_id, mass_multiplier,
        CASE
            WHEN mass_multiplier < 0.1 THEN 'Very Low Mass'
            WHEN mass_multiplier < 1 THEN 'Low Mass'
            WHEN mass_multiplier < 5 THEN 'Medium Mass'
            WHEN mass_multiplier < 20 THEN 'High Mass'
            ELSE 'Very High Mass'
        END AS mass_category
    FROM (
        SELECT DISTINCT mass_multiplier,
        FROM exoplanets
        WHERE mass_multiplier IS NOT NULL
    ) t;
""")
# dim_distance_category
con.execute("""
    CREATE OR REPLACE TABLE dim_distance_category AS
    SELECT ROW_NUMBER() OVER () AS distance_category_id, distance,
      CASE
        WHEN distance < 10 THEN 'Very Close (<10 ly)'
        WHEN distance < 100 THEN 'Close (<100 ly)'
        WHEN distance < 1000 THEN 'Medium (<1000 ly)'
        ELSE 'Far (>1000 ly)'
      END AS distance_category
    FROM (
        SELECT DISTINCT distance,
        FROM exoplanets
        WHERE distance IS NOT NULL
    ) t;
""")
# dim_orbit_category
con.execute("""
    CREATE OR REPLACE TABLE dim_orbit_category AS
    SELECT ROW_NUMBER() OVER () AS orbit_category_id, orbital_period,
        CASE
            WHEN orbital_period < 10 THEN 'Very Short'
            WHEN orbital_period < 100 THEN 'Short'
            WHEN orbital_period < 1000 THEN 'Moderate'
            ELSE 'Long'
        END AS period_class
    FROM (
        SELECT DISTINCT orbital_period,
        FROM exoplanets
        WHERE orbital_period IS NOT NULL
    ) t;
""")
# dim_brightness_category
con.execute("""
    CREATE OR REPLACE TABLE dim_brightness_category AS
    SELECT ROW_NUMBER() OVER () AS brightness_category_id, stellar_magnitude,
      CASE
        WHEN stellar_magnitude < 5 THEN 'Very Bright'
        WHEN stellar_magnitude < 10 THEN 'Bright'
        WHEN stellar_magnitude < 15 THEN 'Dim'
        ELSE 'Very Dim'
      END AS brightness_category
    FROM (
        SELECT DISTINCT stellar_magnitude,
        FROM exoplanets
        WHERE stellar_magnitude IS NOT NULL
    ) t;
""")
# dim_discovery_era
con.execute("""
    CREATE OR REPLACE TABLE dim_discovery_era AS
    SELECT ROW_NUMBER() OVER () AS discovery_era_id, discovery_year,
      CASE
        WHEN discovery_year < 2000 THEN '<2000'
        WHEN discovery_year < 2010 THEN 'Early 21st Century'
        WHEN discovery_year < 2020 THEN 'Kepler Era'
        ELSE 'Modern Era'
      END AS discovery_era
    FROM (
        SELECT DISTINCT discovery_year
        FROM exoplanets
        WHERE discovery_year IS NOT NULL
    ) t;
""")
# dim_date
con.execute("""
    CREATE OR REPLACE TABLE dim_date AS
    SELECT
        ROW_NUMBER() OVER () AS date_id,
        CAST(releasedate AS DATE) AS date,
        date_part('year', CAST(releasedate AS DATE)) AS year,
        date_part('month', CAST(releasedate AS DATE)) AS month,
        strftime(CAST(releasedate AS DATE), '%B') AS month_name,
        date_part('day', CAST(releasedate AS DATE)) AS day,
        strftime(CAST(releasedate AS DATE), '%A') AS weekday_name
    FROM (
        SELECT DISTINCT releasedate
        FROM exoplanets
        WHERE releasedate IS NOT NULL
    ) t;
""")
```

Nyní musíme propojit dimenzionální tabulky s naší tabulkou exoplanet tím, že vytvoříme vytvoříme v tabulce exoplanet id ke každé dimenzionální tabulce.
```python
# propojení dimenzionálních tabulek s tabulkou exoplanet
con.execute("""
    CREATE OR REPLACE TABLE exoplanets AS
    SELECT e.*,
        p.planet_type_id,
        d.detection_method_id,
        s.stellar_type_id,
        m.mass_category_id,
        dc.distance_category_id,
        o.orbit_category_id,
        b.brightness_category_id,
        de.discovery_era_id,
        dt.date_id
    FROM exoplanets e
    LEFT JOIN dim_planet_type p ON e.planet_type = p.planet_type
    LEFT JOIN dim_detection_method d ON e.detection_method = d.detection_method
    LEFT JOIN dim_stellar_type s ON e.distance = s.distance AND e.stellar_magnitude = s.stellar_magnitude
    LEFT JOIN dim_mass_category m ON e.mass_multiplier = m.mass_multiplier
    LEFT JOIN dim_distance_category dc ON e.distance = dc.distance
    LEFT JOIN dim_orbit_category o ON e.orbital_period = o.orbital_period
    LEFT JOIN dim_brightness_category b ON e.stellar_magnitude = b.stellar_magnitude
    LEFT JOIN dim_discovery_era de ON e.discovery_year = de.discovery_year
    LEFT JOIN dim_date dt ON CAST(e.releasedate AS DATE) = dt.date;
""")
```

#### Výsledný ERD s dimenzionálními tabulkami
![Untitled (1)](https://github.com/user-attachments/assets/88653550-f6c8-4dae-ae0e-0b37404e21ad)
Vytvořené hvězdicové schéma.

### Vytvoření Lakehouse storage
Vytvořené tabulky si uložíme Parquet(Lake) souborů, kde se nám vytvoří soubory pro Lakehouse úložiště. Všechny je uložím do nově vytvořené složky "dimensions".
```python
# vytvoření složky pro dimenze
import os
os.makedirs('dimensions', exist_ok=True)
# lakehouse storage
con.execute("""
    COPY exoplanets TO 'exoplanets.parquet' (FORMAT 'parquet');
    COPY dim_planet_type TO 'dimensions/dim_planet_type.parquet' (FORMAT 'parquet');
    COPY dim_detection_method TO 'dimensions/dim_detection_method.parquet' (FORMAT 'parquet');
    COPY dim_stellar_type TO 'dimensions/dim_stellar_type.parquet' (FORMAT 'parquet');
    COPY dim_mass_category TO 'dimensions/dim_mass_category.parquet' (FORMAT 'parquet');
    COPY dim_distance_category TO 'dimensions/dim_distance_category.parquet' (FORMAT 'parquet');
    COPY dim_orbit_category TO 'dimensions/dim_orbit_category.parquet' (FORMAT 'parquet');
    COPY dim_brightness_category TO 'dimensions/dim_brightness_category.parquet' (FORMAT 'parquet');
    COPY dim_discovery_era TO 'dimensions/dim_discovery_era.parquet' (FORMAT 'parquet');
    COPY dim_date TO 'dimensions/dim_date.parquet' (FORMAT 'parquet');
""")
```

### Práce s Lakehouse databází
```python
import duckdb
import pandas as pd
import os

con = duckdb.connect()

con.execute("""
    CREATE VIEW dim_planet_type AS SELECT * FROM 'dimensions/dim_planet_type.parquet';
    CREATE VIEW dim_detection_method AS SELECT * FROM 'dimensions/dim_detection_method.parquet';
    CREATE VIEW dim_stellar_type AS SELECT * FROM 'dimensions/dim_stellar_type.parquet';
    CREATE VIEW dim_mass_category AS SELECT * FROM 'dimensions/dim_mass_category.parquet';
    CREATE VIEW dim_distance_category AS SELECT * FROM 'dimensions/dim_distance_category.parquet';
    CREATE VIEW dim_orbit_category AS SELECT * FROM 'dimensions/dim_orbit_category.parquet';
    CREATE VIEW dim_brightness_category AS SELECT * FROM 'dimensions/dim_brightness_category.parquet';
    CREATE VIEW dim_discovery_era AS SELECT * FROM 'dimensions/dim_discovery_era.parquet';
    CREATE VIEW dim_date AS SELECT * FROM 'dimensions/dim_date.parquet';
    CREATE VIEW exoplanets AS SELECT * FROM 'exoplanets.parquet';
""")
```
### Ukázky grafů a srovnání
