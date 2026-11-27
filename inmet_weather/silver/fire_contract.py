"""
Rules for the fire Silver job (INPE fire foci). Plain Python (no Spark).
Rules shared with the weather data (time zones, Brazil box) are in inmet_contract.py.
"""

# Expected columns of bronze.fire_foci. Written out (not imported)
# so a change in Bronze is caught by the schema check.
BRONZE_COLUMNS = [
    "source_year", "source_file", "source_line_number",
    "id_bdq", "foco_id", "lat", "lon", "data_pas", "pais", "estado", "municipio", "bioma",
    "_bronze_source", "_bronze_run_id", "_bronze_ingestion_time", "_bronze_code_version",
]

KEY = ["foco_id"]                          # one row per fire focus
TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"        # data_pas, UTC

# INPE uses full state names, INMET uses the UF code
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

BIOMES = ["Amazônia", "Caatinga", "Cerrado", "Mata Atlântica", "Pampa", "Pantanal"]

# "pais" is always "Brasil"
DROP = ["pais"]

# nearest station (fire_station_matching.py)
GRID_DEG = 1.0                             # 1 degree ≈ 111 km
# Stations further away are ignored. Must stay below ~90 km with 1-degree cells,
# so the closest station is always in the 3x3 cells searched.
MAX_DISTANCE_KM = 90.0

# state check (fire_state_check.py)
# (lat min, lat max, lon min, lon max), rounded outwards from the state borders.
# Catches big errors (wrong state, swapped lat/lon), not points near a border.
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
    "PE": (-9.50, -3.80, -41.38, -32.30),      # incl. Fernando de Noronha
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
STATE_BOX_MARGIN_DEG = 0.1                 # ≈ 11 km

# fire events (fire_events.py)
# points on the same local day in the same cell count as one fire
EVENT_GRID_DEG = 0.01                      # ≈ 1 km, one satellite pixel
