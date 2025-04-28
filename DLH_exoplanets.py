# https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=PS&constraint=default_flag%20%3E0
# pip install duckdb
import duckdb
import pandas as pd

# connect to DuckDB
con = duckdb.connect()

# Load the CSV file into a DucDB table
db = duckdb.read_csv(r"C:\Users\imang\OneDrive\Plocha\ODM\adityamishraml\nasaexoplanets\versions\2\cleaned_5250.csv")


duckdb.sql("SELECT * FROM 'db'").show()

con.execute("""
        CREATE TABLE exoplanets AS
        SELECT *
        FROM read_csv_auto('adityamishraml\nasaexoplanets\versions\2\cleaned_5250.csv');
    """)

print(query = con.execute("""
        SELECT name, discovery_year, planet_type
        FROM db
        WHERE discovery_year > 2010
        ORDER BY discovery_year ASC
    """).fetchdf())