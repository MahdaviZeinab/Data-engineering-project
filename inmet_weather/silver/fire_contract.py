"""
Silver contract for the fire data (INPE focos de queimadas): every rule in one place.

Plain Python only (no Spark), so it is easy to read and to test.
Rules that weather and fire share (time zones, the box around Brazil) stay in
silver/contract.py, so both sources use exactly the same ones.
"""

# The exact columns Silver expects in the table bronze.fire_foci.
# Written out here on purpose (not imported from Bronze): if Bronze changes,
# the schema check notices it.
BRONZE_COLUMNS = [
    "source_year", "source_file", "source_line_number",
    "id_bdq", "foco_id", "lat", "lon", "data_pas", "pais", "estado", "municipio", "bioma",
    "_bronze_source", "_bronze_run_id", "_bronze_ingestion_time", "_bronze_code_version",
]

KEY = ["foco_id"]                          # INPE's id: one row per fire focus
TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"        # data_pas, in UTC (e.g. 2020-06-16 17:00:00)

# INPE writes the full state name, INMET the 2-letter code (UF).
# Without this table a join on the state would find no rows, and give no error.
STATE_TO_UF = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAPÁ": "AP",
    "AMAZONAS": "AM",
    "BAHIA": "BA",
    "CEARÁ": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPÍRITO SANTO": "ES",
    "GOIÁS": "GO",
    "MARANHÃO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARÁ": "PA",
    "PARAÍBA": "PB",
    "PARANÁ": "PR",
    "PERNAMBUCO": "PE",
    "PIAUÍ": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SÃO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO",
}

# The 6 biomes of Brazil, as INPE writes them.
BIOMES = ["Amazônia", "Caatinga", "Cerrado", "Mata Atlântica", "Pampa", "Pantanal"]

# Bronze columns we do not keep in Silver: "pais" is always "Brasil".
DROP = ["pais"]

# Nearest weather station (silver/nearest_station.py).
# Size of the grid squares, in degrees (1 degree is about 111 km).
GRID_DEG = 1.0
# A station further away than this is not used: the fire gets no station.
# Keep it below about 90 km for 1 degree squares: then a closer station
# is always in the 9 squares we look at (1 degree of longitude is ~92 km in the far south).
MAX_DISTANCE_KM = 90.0
