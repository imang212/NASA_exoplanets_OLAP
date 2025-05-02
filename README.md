# NASA Exoplanets Data Lakehouse

### Výběr databáze
V této seminární práci budu pracovat z daty z vládního NASA archivu exoplanet.: https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=PS&constraint=default_flag%20%3E0
V tomto archivu je uloženo kolem 38 000 těles celkově, z toho je asi přibližně 5 800 těles již potvrzených exoplanet. Z této stránky jsem stáhnul část tabulky obsahují všechny objevené exoplanety a informace o nich včetně datumu publikace v csv formátu. + zkombinuji již s jednou tabulkou od Aditya Mishra ML v csv formátu, která také obsahuje informace ze NASA archivu s lepším popisem: https://www.kaggle.com/datasets/adityamishraml/nasaexoplanets.

### Výběr databázového systému
Pro vytvoření DLH systému jsem si vybral DuckDB databázový systém (https://duckdb.org/), který umožňuje vytvářet dimenze dat a datová jezera s daty. Tento databázový systém zároveň umožňuje práci v Pythonu. 

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
### ERD diagram


### Zobrazení tabulek
Zobrazíme si tabulku.:
```python
con.table("exoplanets").show()
```
Nová tabulka vypadá takhle.:
![image](https://github.com/user-attachments/assets/920b67f9-b038-4d97-8f65-49c39bf062d1)

Můžem si zobrazit popis sloupců v tabulce.:
```python
print(con.execute("DESCRIBE exoplanets").fetchdf())
```

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
- Tabulku dim_planet_type, která obsahuje typy planet. 
- Tabulku dim_detection_method, která obsahuje metody detekce planety.
- Tabulku dim_stellar_type, která obsahuje velikosti hvězd podle vzdáleností planet.
- Tabulku dim_mass_category, která obsahuje hmotnosti hvězd rozdělené do kategorií.
- Tabulku dim_distance_category, která obsahuje vzdálenosti planet rozdělených do kategorií


```python
## vytvoření dim tabulek
# dim_planet_type
con.execute("""
    CREATE TABLE dim_planet_type AS
    SELECT DISTINCT planet_type
    FROM exoplanets
    WHERE planet_type IS NOT NULL;
""")
# dim_detection_method
con.execute("""
    CREATE TABLE dim_detection_method AS
    SELECT DISTINCT  detection_method
    FROM exoplanets
    WHERE detection_method IS NOT NULL;
""")
# dim_stellar_type
con.execute("""
    CREATE TABLE dim_star AS
    SELECT DISTINCT distance, stellar_magnitude
    FROM exoplanets
    WHERE distance IS NOT NULL AND stellar_magnitude IS NOT NULL;
""")
# dim_radius_category
con.execute("""
    CREATE TABLE dim_mass_category AS
    SELECT mass_multiplier,
        CASE
            WHEN mass_multiplier < 0.1 THEN 'Very Low Mass'
            WHEN mass_multiplier < 1 THEN 'Low Mass'
            WHEN mass_multiplier < 5 THEN 'Medium Mass'
            WHEN mass_multiplier < 20 THEN 'High Mass'
            ELSE 'Very High Mass'
        END AS mass_category
    FROM exoplanets
    WHERE mass_multiplier IS NOT NULL;
""")
con.execute("""
    
    SELECT distance,
      CASE
        WHEN distance < 10 THEN 'Very Close (<10 ly)'
        WHEN distance < 100 THEN 'Close (<100 ly)'
        WHEN distance < 1000 THEN 'Medium (<1000 ly)'
        ELSE 'Far (>1000 ly)'
      END AS distance_category
    FROM exoplanets
    WHERE distance IS NOT NULL;
""")
# dim_orbit_category
con.execute("""
CREATE TABLE dim_orbit_class AS
    SELECT orbital_period,
        CASE
            WHEN orbital_period < 10 THEN 'Very Short'
            WHEN orbital_period < 100 THEN 'Short'
            WHEN orbital_period < 1000 THEN 'Moderate'
            ELSE 'Long'
        END AS period_class
    FROM exoplanets
    WHERE orbital_period IS NOT NULL;
""")
# dim_brightness_category
con.execute("""
    SELECT stellar_magnitude,
      CASE
        WHEN stellar_magnitude < 5 THEN 'Very Bright'
        WHEN stellar_magnitude < 10 THEN 'Bright'
        WHEN stellar_magnitude < 15 THEN 'Dim'
        ELSE 'Very Dim'
      END AS brightness_category
    FROM exoplanets
    WHERE stellar_magnitude IS NOT NULL;
""")
# dim_discovery_era
con.execute("""
    SELECT discovery_year,
      CASE
        WHEN discovery_year < 2000 THEN '<2000'
        WHEN discovery_year < 2010 THEN 'Early 21st Century'
        WHEN discovery_year < 2020 THEN 'Kepler Era'
        ELSE 'Modern Era'
      END AS discovery_era
    FROM exoplanets;
""")
# dim_date
con.execute("""
    CREATE TABLE dim_date AS
    SELECT DISTINCT
        CAST(releasedate AS DATE) AS date,
        date_part('year', CAST(releasedate AS DATE)) AS year,
        date_part('month', CAST(releasedate AS DATE)) AS month,
        strftime(CAST(releasedate AS DATE), '%B') AS month_name,
        date_part('day', CAST(releasedate AS DATE)) AS day,
        strftime(CAST(releasedate AS DATE), '%A') AS weekday_name
    FROM exoplanets
    WHERE releasedate IS NOT NULL;
""")
```

### Vytvoření Lakehouse storage

### Ukázka grafů a porovnání
