import duckdb
import pandas as pd
from os import makedirs
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

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


#zobrazení planet se vzdálenostmi a jejich kategoriemi
print(con.execute("""
        SELECT e.name, e.distance, dc.distance_category
        FROM exoplanets e
        JOIN dim_distance_category dc ON e.distance_category_id = dc.distance_category_id
    """).df().head())

#zobrazení ketegorií a počtu planet v jednotlivých kategoriích
print(con.execute("""
        SELECT dc.distance_category as category, count(dc.distance_category) as category_count
        FROM dim_distance_category as dc
        GROUP BY dc.distance_category
        ORDER BY dc.distance_category
    """).df().head())

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
#print(df)

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
