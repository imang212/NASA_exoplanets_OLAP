# NASA Exoplanets Data Lakehouse

### Zřeknutí se zodpovědnosti
V této seminární práci budu pracovat z daty z NASA archivu exoplanet.: https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=PS&constraint=default_flag%20%3E0
Z této stránky jsem stáhnul tabulku obsahují všechny objevené exoplanety a informaci o nich včetně datumu publikace.

### Výběr databázového systému
Pro vytvoření DLH systému jsem si vybral DuckDB databázový systém, který umožňuje vytvářet dimenze dat a datová jezera s daty. Tento databázový systém zároveň umožňuje práci v Pythonu. 
