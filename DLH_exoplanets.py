# https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=PS&constraint=default_flag%20%3E0
# pip install duckdb
import duckdb
import pandas as pd

# connect to DuckDB
con = duckdb.connect()

db = duckdb.read_csv(r"C:\Users\imang\OneDrive\Plocha\ODM\adityamishraml\nasaexoplanets\versions\2\cleaned_5250.csv")
duckdb.sql("SELECT * FROM 'db'").show()

db2 = duckdb.read_csv("PS_2025.04.28_06.13.44.csv")
duckdb.sql("SELECT * FROM 'db'").show()

con.execute("""
        CREATE TABLE exoplanets AS
        SELECT e.*, n.pl_pubdate, n.releasedate
        FROM read_csv_auto('adityamishraml/nasaexoplanets/versions/2/cleaned_5250.csv') e
        LEFT JOIN read_csv_auto("PS_2025.04.28_06.13.44.csv") n ON LOWER(e.name) = LOWER(n.pl_name)
    """)


print(con.execute("""
        SELECT name, discovery_year
        FROM exoplanets
        WHERE discovery_year > 2010
        ORDER BY discovery_year ASC
    """).fetchdf())


con.execute("""
    CREATE TABLE dim_planet_type AS
    SELECT DISTINCT planet_type
    FROM exoplanets;
""")

# lakehouse storage
con.execute("""
    COPY exoplanets TO 'exoplanets.parquet' (FORMAT 'parquet');
""")
