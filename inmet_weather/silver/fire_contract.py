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

# Check that a fire is inside its state (silver/state_check.py).
# A box around every state: (lat min, lat max, lon min, lon max).
# Made from the state borders, rounded outwards. A box is bigger than the state,
# so this finds big mistakes (wrong state, lat and lon swapped), not small ones near a border.
STATE_BOX = {
    "AC": (-11.15, -7.11, -74.01, -66.63),
    "AL": (-10.51, -8.82, -38.24, -35.14),
    "AM": (-9.85, 2.24, -73.81, -56.36),
    "AP": (-1.19, 4.40, -54.79, -49.87),
    "BA": (-18.34, -8.53, -46.58, -37.41),
    "CE": (-7.86, -2.79, -41.45, -37.23),
    "DF": (-16.05, -15.48, -48.29, -47.30),
    "ES": (-21.29, -17.88, -41.87, -39.66),
    "GO": (-19.47, -12.48, -53.24, -45.90),
    "MA": (-10.33, -1.07, -48.76, -41.80),
    "MG": (-22.91, -14.23, -51.03, -39.85),
    "MS": (-24.06, -17.18, -58.17, -50.91),
    "MT": (-18.05, -7.33, -61.62, -50.22),
    "PA": (-9.85, 2.56, -58.98, -46.08),
    "PB": (-8.29, -6.01, -38.76, -34.78),
    "PE": (-9.50, -3.80, -41.38, -32.30),      # with the island Fernando de Noronha
    "PI": (-10.91, -2.76, -45.99, -40.40),
    "PR": (-26.72, -22.51, -54.63, -48.07),
    "RJ": (-23.36, -20.77, -44.87, -40.95),
    "RN": (-6.99, -4.82, -38.62, -34.96),
    "RO": (-13.69, -7.99, -66.84, -59.84),
    "RR": (-1.44, 5.28, -64.81, -58.86),
    "RS": (-33.75, -27.07, -57.59, -49.69),
    "SC": (-29.37, -25.97, -53.85, -48.36),
    "SE": (-11.59, -9.52, -38.25, -36.39),
    "SP": (-25.32, -19.77, -53.17, -44.15),
    "TO": (-13.32, -5.15, -50.75, -45.75),
}
# Extra room around every box, in degrees (0.1 is about 11 km), for fires right on a border.
STATE_BOX_MARGIN_DEG = 0.1

# The same fire seen twice (silver/fire_events.py).
# Fire points on the same local day in the same small square are one fire.
# 0.01 degree is about 1 km, the size of one satellite pixel.
EVENT_GRID_DEG = 0.01
