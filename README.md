# NASA Exoplanets OLAP

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
Načteme si  tabulky z parquet souborů
```python
import duckdb
import pandas as pd
from os import makedirs

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
Select pro zobrazení planet s jejich kategoriemi vzdáleností
```python
#zobrazení planet se vzdálenostmi a jejich kategoriemi
print(con.execute("""
        SELECT e.name, e.distance, dc.distance_category
        FROM exoplanets e
        JOIN dim_distance_category dc ON e.distance_category_id = dc.distance_category_id
    """).df().head())
```
![image](https://github.com/user-attachments/assets/97a16e4e-3fa0-4355-b13b-88ed5638b330)

Pro kontrolu jestli se nám tabulky vytvořili si zobrazíme tabulky pomocí příkazu SHOW TABLES.
```python
print(con.execute("SHOW TABLES").fetchdf())
```

Spočítám si kolik v každé kategorii vzdálenosti je planet.
```python
#zobrazení ketegorií a počtu planet v jednotlivých kategoriích
print(con.execute("""
        SELECT dc.distance_category as category, count(dc.distance_category) as category_count
        FROM dim_distance_category as dc
        GROUP BY dc.distance_category
        ORDER BY dc.distance_category
    """).df().head())
```
![image](https://github.com/user-attachments/assets/4f75e6df-ef79-490b-90bf-7004c4d6ece0)

Zobrazení počtu planet podle ery objevení a metody detekce.
```python
# zobrazení počtu planet podle roku objevu a metody detekce
df = con.execute("""
    SELECT
        de.discovery_era,
        dm.detection_method,
        COUNT(*) AS num_planets
    FROM exoplanets e
    JOIN dim_discovery_era de ON e.discovery_year = de.discovery_year
    JOIN dim_detection_method dm ON e.detection_method_id = dm.detection_method_id
    GROUP BY de.discovery_era, dm.detection_method
    ORDER BY num_planets DESC
    """).df()
print(df)
```

Tyto výsledky se dají i uložit do parquet souboru.
```python
# výsledky si zase můžeme uložit do parquet souboru
makedirs('results', exist_ok=True)
con.execute("""
    COPY (
        SELECT
        de.discovery_era,
        dm.detection_method,
        COUNT(*) AS num_planets
    FROM exoplanets e
    JOIN dim_discovery_era de ON e.discovery_year = de.discovery_year
    JOIN dim_detection_method dm ON e.detection_method_id = dm.detection_method_id
    GROUP BY de.discovery_era, dm.detection_method
    ORDER BY num_planets DESC
    ) TO 'results/enriched_results.parquet' (FORMAT 'parquet')
""")
```
### Ukázky grafů a srovnání
Vytvoření heatmapy na počty planet podle éry objevení a jejich detekce.
```python
import seaborn as sns
import matplotlib.pyplot as plt

# Převod na kontingenční tabulku (pivot)
pivot = df.pivot(index='detection_method', columns='discovery_era', values='num_planets')
makedirs('graphs', exist_ok=True)

# Heatmapa
plt.figure(figsize=(14, 10))
sns.heatmap(pivot, annot=True, fmt=".0f", cmap="coolwarm")
plt.title("Počet exoplanet podle éry objevu a metody detekce")
plt.xlabel("Éra objevu")
plt.ylabel("Metoda detekce")
plt.tight_layout()
plt.savefig('graphs/exoplanet_era_detection_heatmap.png')
plt.close()
```
![exoplanet_era_detection_heatmap](https://github.com/user-attachments/assets/26e56bf0-fb6e-4bc4-8fd3-77b3c758c388)

Graf ukazující časovou řadu počtu objevených exoplanet podle roku.
```python
import numpy as np
#graf s časovou řadou poočtu exoplanet podle roku objevu
df = con.execute("""
    SELECT
        de.discovery_year,
        COUNT(*) AS num_planets
    FROM exoplanets e
    JOIN dim_discovery_era de ON e.discovery_year = de.discovery_year
    JOIN dim_detection_method dm ON e.detection_method_id = dm.detection_method_id
    GROUP BY de.discovery_year
    ORDER BY de.discovery_year
""").df()
plt.figure(figsize=(14, 8))
sns.lineplot(data=df, x='discovery_year', y='num_planets')
plt.scatter(data=df, x='discovery_year', y="num_planets", color='red')
for i, row in df.iterrows():
    plt.text(row['discovery_year'], row['num_planets'] + 5, str(row['num_planets']),
             ha='center', va='bottom', fontsize=14)
plt.title("Počet objevených exoplanet podle roku")
plt.xlabel("Rok objevu")
plt.ylabel("Počet planet")
plt.xticks(np.arange(1992, 2024, 1), rotation=45)
plt.tight_layout()
plt.savefig('graphs/casova_rada_poctu_objevu_exoplanet.png')
plt.close()
```

![casova_rada_poctu_objevu_exoplanet](https://github.com/user-attachments/assets/232c9e31-9486-491d-998b-964ba34c606b)

Graf ukazující typy exoplanet objevených v různých rocích.
```python
df = con.execute("""
    SELECT
        de.discovery_year,
        e.planet_type,
        COUNT(*) AS num_planets
    FROM exoplanets e
    JOIN dim_discovery_era de ON e.discovery_year = de.discovery_year
    GROUP BY de.discovery_year, e.planet_type
    ORDER BY de.discovery_year
""").df()

df_pivot = df.pivot(index='discovery_year', columns='planet_type', values='num_planets').fillna(0)

df_pivot.plot(kind='bar', stacked=True, colormap='tab20', figsize=(16, 9), width=0.9)

plt.title("Objevené exoplanety podle typu a roku objevu")
plt.xlabel("Rok objevu")
plt.ylabel("Počet exoplanet")
plt.xticks(rotation=45)
plt.legend(title='Typ planety', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_planet_type_by_year.png")
plt.close()
```
![bar_plot_planet_type_by_year](https://github.com/user-attachments/assets/c1086906-35ed-447d-b6da-aec51ada5f92)

Graf ukazující typy detekcí objevených exoplanet v různých rocích.
```python
df = con.execute("""
    SELECT
        de.discovery_year,
        dm.detection_method,
        COUNT(*) AS num_planets
    FROM exoplanets e
    JOIN dim_discovery_era de ON e.discovery_year = de.discovery_year
    JOIN dim_detection_method dm ON e.detection_method_id = dm.detection_method_id
    GROUP BY de.discovery_year, dm.detection_method
    ORDER BY de.discovery_year
""").df()

df_pivot = df.pivot(index='discovery_year', columns='detection_method', values='num_planets').fillna(0)
df_pivot.plot(kind='bar', stacked=True, colormap='tab20', figsize=(16, 9), width=0.9)

plt.title("Objevené exoplanety podle metody detekce a roku objevu")
plt.xlabel("Rok objevu")
plt.ylabel("Počet exoplanet")
plt.xticks(rotation=45)
plt.legend(title='Metoda detekce', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_detection_method_by_year.png")
plt.close()
```
![bar_plot_detection_method_by_year](https://github.com/user-attachments/assets/fc9a14dc-fead-479f-94a7-41e527f3ab05)

Objevené druhy planet podle vzdálenosti
```python
df = con.execute("""
    SELECT
        dd.distance_category,
        e.planet_type,
        COUNT(*) AS num_planets
    FROM exoplanets e
    JOIN dim_distance_category dd ON e.distance_category_id = dd.distance_category_id
    GROUP BY e.planet_type, dd.distance_category
    ORDER BY e.planet_type
""").df()
df_pivot = df.pivot(index='planet_type', columns='distance_category', values='num_planets').fillna(0)
df_pivot.plot(kind='bar', stacked=True, colormap='tab20', figsize=(16, 9), width=0.9)
plt.title("Objevené exoplanety podle kategorie vzdálenosti a typu planety")
plt.xlabel("Druh planety")
plt.ylabel("Počet exoplanet")
plt.xticks(rotation=45)
plt.legend(title='kategorie vzdálenosti', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_planet_type_distance.png")
plt.close()
```

![bar_plot_planet_type_distance](https://github.com/user-attachments/assets/916bda6c-c45f-45bf-9764-274ba0a46ec0)
Z grafu vyplývá, že na typu objevené planety a její vzdálenosti vůběc nezáleží.

```python
df = con.execute("""
    SELECT
        dd.distance_category,
        db.brightness_category,
        COUNT(*) AS num_planets
    FROM exoplanets e
    JOIN dim_distance_category dd ON e.distance_category_id = dd.distance_category_id
    JOIN dim_brightness_category db ON e.brightness_category_id = db.brightness_category_id
    GROUP BY dd.distance_category, db.brightness_category
    ORDER BY dd.distance_category
""").df()
df_pivot = df.pivot(index='distance_category', columns='brightness_category', values='num_planets').fillna(0)
df_pivot.plot(kind='bar', stacked=True, colormap='tab20', figsize=(16, 9), width=0.9)
plt.title("Objevené exoplanety podle kategorie vzdálenosti a jejich jasu.")
plt.xlabel("Vzdálenost")
plt.ylabel("Počet exoplanet")
plt.xticks(rotation=45)
plt.legend(title='Jas', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_planet_distance_brightness.png")
plt.close()
```

![bar_plot_planet_distance_brightness](https://github.com/user-attachments/assets/f3b076ea-5454-4a5e-8585-923bba1a75cc)

