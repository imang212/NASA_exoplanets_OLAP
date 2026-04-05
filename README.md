[![Python: 3.13+](https://img.shields.io/badge/Python-3.13+-blue.svg?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![DuckDB: 1.x](https://img.shields.io/badge/Database-DuckDB-FFF000.svg?style=flat-square&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Storage: Parquet](https://img.shields.io/badge/Storage-Apache%20Parquet-00A6AF.svg?style=flat-square&logo=apache-parquet&logoColor=white)](https://parquet.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)](https://opensource.org/licenses/MIT)

# 🪐 NASA Exoplanets OLAP
<p align="center">
  <img src="https://github.com/user-attachments/assets/26e56bf0-fb6e-4bc4-8fd3-77b3c758c388" width="800" alt="Exoplanet Heatmap Analysis">
</p>

This repository presents a **Data Lakehouse (DLH)** implementation designed to analyze NASA's extensive exoplanet archive. By leveraging **DuckDB** for high-performance analytical processing and Python for data orchestration, this project transforms raw astronomical data into a structured **Star Schema** optimized for Online Analytical Processing (OLAP).

## Overview
The project explores the relationship between various planetary characteristics—such as mass, orbital period, and discovery era—using advanced data modeling and visualization techniques.

### Core Features
* **Hybrid Storage:** Combines raw CSV ingestion with structured Parquet (Lakehouse storage).
* **High Performance:** Uses DuckDB for lightning-fast SQL queries on millions of rows.
* **OLAP Star Schema:** Implements 9 dimensional tables for deep astronomical analysis.
* **Data Visualization:** Comprehensive insights using Seaborn and Matplotlib.

## Tech stack
| Component | Technology |
| :--- | :--- |
| **Engine** | [DuckDB](https://duckdb.org/) (In-process OLAP database) |
| **Storage** | [Apache Parquet](https://parquet.apache.org/) |
| **Language** | Python 3.13 |
| **Libraries** | Pandas, Seaborn, Matplotlib |

## Data Architecture
The pipeline transforms raw data into a multi-dimensional star schema to answer complex questions about the universe.

### Star Schema Design
We implement 9 dimensions including:
* `dim_planet_type` - Categorization of planet compositions.
* `dim_mass_category` - Mass-based grouping (Low to Very High).
* `dim_discovery_era` - Temporal grouping (Pre-2000 to Modern Era).
* `dim_distance_category` - Proximity-based classification (Light Years).
* ...

**Entity-Relationship Diagram (ERD):**
<p align="center">
  <img src="https://github.com/user-attachments/assets/88653550-f6c8-4dae-ae0e-0b37404e21ad" width="850" alt="Final Star Schema ERD"/>
</p>

### Quick start
## Installation
First, we install the necessary modules.
```bash
pip install duckdb pandas seaborn matplotlib
```

## Build the Warehouse
Run the commitment script to create the database and export Parquet files:
```bash
python DLH_exoplanets_commit.py
```

## Analyze Data
Use the storage script to perform OLAP queries and generate insights:
```bash
python DLH_exoplanets_storages_use.py
```

## Analytical Insights
Our analysis reveals fascinating trends in exoplanetary discovery:

#### Discovery Heatmap:
A comparison of discovery eras versus detection methods shows the massive impact of the Kepler Era on transit detections.

#### Temporal Trends:
The following time series illustrates the exponential growth of confirmed exoplanets over the last three decades.

## Project Structure
```plaintext
├── adityamishraml/          # Dataset source (Kaggle)
├── dimensions/              # Lakehouse Parquet storage (Dimensions)
├── graphs/                  # Generated visualization exports
├── results/                 # Parquet exports of analytical results
├── DLH_exoplanets_commit.py # Data Pipeline (Ingestion -> Warehouse -> Lake)
└── DLH_exoplanets_use.py    # Analysis & Visualization layer
```

## Acknowledgement
I worked with data from: 
- **NASA Exoplanet Archive:** [Official Site](https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=PS&constraint=default_flag%20%3E0).
- **Aditya Mishra ML:** [Kaggle dataset](https://www.kaggle.com/datasets/adityamishraml/nasaexoplanets)

## Setting up DuckDB in Python (step by step)
Connect to the DuckDB database:
```python
# import modules
import duckdb
import pandas as pd
# connect to DuckDB
con = duckdb.connect()
```

### Creating the main table
Display the CSV files.
```python
db = duckdb.read_csv("adityamishraml/nasaexoplanets/versions/2/cleaned_5250.csv")
duckdb.sql("SELECT * FROM 'db'").show()

db2 = duckdb.read_csv("PS_2025.04.28_06.13.44.csv")
duckdb.sql("SELECT * FROM 'db2'").show()
```

Create a table where I take everything from the first table and join it with the necessary columns for working with dimensions from the second table.

```python
con.execute("""
        CREATE TABLE exoplanets AS
        SELECT e.*, n.pl_pubdate, n.releasedate
        FROM read_csv_auto('adityamishraml/nasaexoplanets/versions/2/cleaned_5250.csv') e
        LEFT JOIN read_csv_auto("PS_2025.04.28_06.13.44.csv") n ON LOWER(e.name) = LOWER(n.pl_name)
    """)
```

### Viewing Tables
Display the table:
```python
con.table("exoplanets").show()
```
The new table looks like this:
![image](https://github.com/user-attachments/assets/920b67f9-b038-4d97-8f65-49c39bf062d1)

Display the column descriptions in the table:
```python
print(con.execute("DESCRIBE exoplanets").fetchdf())
```
![image](https://github.com/user-attachments/assets/fbb71914-e868-45d5-a7bd-59d4e1951605)

#### ERD diagram
First, we will create an ERD diagram to have an overview of the variables in the table.
<p align="center">
  <img src="https://github.com/user-attachments/assets/541b854c-744b-47c9-84bc-b29abcfbb388" alt="ERD diagram image"/>
</p>

This is how my table looks for now.

### Search query
Try a query on the newly created table.
```python
print(con.execute("""
        SELECT name, discovery_year
        FROM exoplanets
        WHERE discovery_year > 2010
        ORDER BY discovery_year ASC
    """).fetchdf())
```

### Creating dimensional tables
I will create 9 dimensional tables.:
- dim_planet_type, which contains types of planets.
- dim_detection_method, which contains planet detection methods.
- dim_stellar_type, which contains categories for the brightness of the planet's star given its distance: very bright, bright, moderate, dim, and very dim.
- dim_mass_category, which contains planet masses divided into the categories Very Low Mass, Low Mass, Medium Mass, and High Mass.
- dim_distance_category, which contains planet distances divided into the categories Very Close (<10 ly), Close (<100 ly), Medium (<1000 ly), and Far (>1000 ly).
- dim_orbit_category, which contains planets divided into categories based on how far they orbit their star: Very Short, Short, Moderate, and Long.
- dim_brightness_category, which contains planets divided into categories based on their brightness: Very Bright, Bright, Dim, and Very Dim.
- dim_discovery_era, which contains planets divided into categories based on the era they were discovered: <2000, Early 21st Century, Kepler Era, and Modern Era.
- dim_date, which contains the release dates of the planets, with the full date from the table divided into year, month, day, month name, and weekday name.

```python
## creating dim tables
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

Now we must link the dimensional tables with our exoplanets table by creating an ID in the exoplanets table for each dimensional table.

```python
# linking dimensional tables with the exoplanets table
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
Check if the tables have already been created.
```python
print(con.execute("SHOW TABLES").fetchdf()))
```
![image](https://github.com/user-attachments/assets/ebc300b9-feff-4c79-b8f7-2dd14c0ffcd5)

#### Final ERD with Dimensional Tables
![Untitled (1)](https://github.com/user-attachments/assets/88653550-f6c8-4dae-ae0e-0b37404e21ad)
The created star schema.

### Creating Lakehouse Storage
We will save the created tables as Parquet (Lake) files, creating the files for the Lakehouse storage. I will save them all in a newly created "dimensions" folder.
```python
# creating folder for dimensions
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

### Working with the Lakehouse Database
Load the tables from the parquet files.
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
Select to display planets with their distance categories.
```python
# display planets with distances and their categories
print(con.execute("""
        SELECT e.name, e.distance, dc.distance_category
        FROM exoplanets e
        JOIN dim_distance_category dc ON e.distance_category_id = dc.distance_category_id
    """).df().head())
```
![image](https://github.com/user-attachments/assets/97a16e4e-3fa0-4355-b13b-88ed5638b330)

Count how many planets are in each distance category.
```python
# display categories and the count of planets in individual categories
print(con.execute("""
        SELECT dc.distance_category as category, count(dc.distance_category) as category_count
        FROM dim_distance_category as dc
        GROUP BY dc.distance_category
        ORDER BY dc.distance_category
    """).df().head())
```
![image](https://github.com/user-attachments/assets/4f75e6df-ef79-490b-90bf-7004c4d6ece0)

Display the number of planets by discovery era and detection method.
```python
# display number of planets by discovery year and detection method
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

These results can also be saved to a parquet file.

```python
# we can save the results back to a parquet file
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
### Graph Examples and Comparison
Create a heatmap for the counts of planets by discovery era and their detection.
```python
import seaborn as sns
import matplotlib.pyplot as plt

# Convert to pivot table
pivot = df.pivot(index='detection_method', columns='discovery_era', values='num_planets')
makedirs('graphs', exist_ok=True)

# Heatmap
plt.figure(figsize=(14, 10))
sns.heatmap(pivot, annot=True, fmt=".0f", cmap="coolwarm")
plt.title("Number of Exoplanets by Discovery Era and Detection Method")
plt.xlabel("Discovery Era")
plt.ylabel("Detection Method")
plt.tight_layout()
plt.savefig('graphs/exoplanet_era_detection_heatmap.png')
plt.close()
```
![exoplanet_era_detection_heatmap](https://github.com/user-attachments/assets/26e56bf0-fb6e-4bc4-8fd3-77b3c758c388)

Graph showing the time series of the number of discovered exoplanets by year.
```python
import numpy as np
# graph with time series of the number of exoplanets by year of discovery
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
plt.title("Number of Discovered Exoplanets by Year")
plt.xlabel("Discovery Year")
plt.ylabel("Number of Planets")
plt.xticks(np.arange(1992, 2024, 1), rotation=45)
plt.tight_layout()
plt.savefig('graphs/casova_rada_poctu_objevu_exoplanet.png')
plt.close()
```

![casova_rada_poctu_objevu_exoplanet](https://github.com/user-attachments/assets/232c9e31-9486-491d-998b-964ba34c606b)

Graph showing the types of exoplanets discovered in different years.
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

plt.title("Discovered Exoplanets by Type and Year of Discovery")
plt.xlabel("Discovery Year")
plt.ylabel("Number of Exoplanets")
plt.xticks(rotation=45)
plt.legend(title='Planet Type', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_planet_type_by_year.png")
plt.close()
```
![bar_plot_planet_type_by_year](https://github.com/user-attachments/assets/c1086906-35ed-447d-b6da-aec51ada5f92)

Graph showing the types of detections for discovered exoplanets in different years.
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

plt.title("Discovered Exoplanets by Detection Method and Year of Discovery")
plt.xlabel("Discovery Year")
plt.ylabel("Number of Exoplanets")
plt.xticks(rotation=45)
plt.legend(title='Detection Method', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_detection_method_by_year.png")
plt.close()
```
![bar_plot_detection_method_by_year](https://github.com/user-attachments/assets/fc9a14dc-fead-479f-94a7-41e527f3ab05)

Types of planets discovered by distance.
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
plt.title("Discovered Exoplanets by Distance Category and Planet Type")
plt.xlabel("Planet Type")
plt.ylabel("Number of Exoplanets")
plt.xticks(rotation=45)
plt.legend(title='Distance Category', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_planet_type_distance.png")
plt.close()
```

![bar_plot_planet_type_distance](https://github.com/user-attachments/assets/916bda6c-c45f-45bf-9764-274ba0a46ec0)
he graph shows that the type of planet discovered and its distance are not related at all.

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
plt.title("Discovered Exoplanets by Distance Category and Their Brightness")
plt.xlabel("Distance")
plt.ylabel("Number of Exoplanets")
plt.xticks(rotation=45)
plt.legend(title='Brightness', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_planet_distance_brightness.png")
plt.close()
```

![bar_plot_planet_distance_brightness](https://github.com/user-attachments/assets/f3b076ea-5454-4a5e-8585-923bba1a75cc)

```python
con.execute("""
    COPY (
        SELECT
            dp.planet_type,
            oc.period_class,
            COUNT(*) AS num_planets
        FROM exoplanets e
        JOIN dim_planet_type dp ON e.planet_type_id = dp.planet_type_id
        JOIN dim_orbit_category oc ON e.orbit_category_id = oc.orbit_category_id
        GROUP BY dp.planet_type, oc.period_class
        ORDER BY dp.planet_type
    ) TO 'results/exoplanet_planet_type_orbit_period_count.parquet' (FORMAT 'parquet')
""")

df = con.execute("""
    SELECT * FROM 'results/exoplanet_planet_type_orbit_period_count.parquet';
""").df()

df_pivot = df.pivot(index='planet_type', columns='period_class', values='num_planets').fillna(0)

plt.figure(figsize=(16, 9))
df_pivot.plot(kind='bar', stacked=True, colormap='tab20', width=0.9)

plt.title("Discovered Exoplanets by Type and Length of Orbital Period")
plt.xlabel("Planet Type")
plt.ylabel("Number of Exoplanets")
plt.xticks(rotation=45)
plt.legend(title='Orbital Period Category', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.savefig("graphs/bar_plot_planet_type_orbit_period.png")
plt.close()
```

![bar_plot_planet_type_orbit_period](https://github.com/user-attachments/assets/a7c9d32b-44c7-42ec-836b-2f128d9c76a6)
