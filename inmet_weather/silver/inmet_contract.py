"""
Rules for the INMET Silver job, kept in one place so code and README stay in sync.
Plain Python (no Spark).
"""

# column names
# Bronze name -> Silver name. "data" and "hora_utc" are combined into observed_at_utc.
RENAME = {
    "precipitacao_total_mm": "precip_mm",
    "pressao_estacao_mb": "pressure_mb",
    "pressao_max_mb": "pressure_max_mb",
    "pressao_min_mb": "pressure_min_mb",
    "radiacao_global_kj_m2": "radiation_kj_m2",
    "temperatura_ar_c": "temp_c",
    "temperatura_orvalho_c": "dew_point_c",
    "temperatura_max_c": "temp_max_c",
    "temperatura_min_c": "temp_min_c",
    "temperatura_orvalho_max_c": "dew_point_max_c",
    "temperatura_orvalho_min_c": "dew_point_min_c",
    "umidade_max_pct": "humidity_max_pct",
    "umidade_min_pct": "humidity_min_pct",
    "umidade_relativa_pct": "humidity_pct",
    "vento_direcao_graus": "wind_dir_deg",
    "vento_rajada_ms": "wind_gust_ms",
    "vento_velocidade_ms": "wind_speed_ms",
}
MEASUREMENTS = list(RENAME.values())          # 17 double columns

KEY = ["station_code", "observed_at_utc"]     # one row per station-hour
MISSING_CODES = ["", "-9999"]                 # INMET's "no value"

# links every Silver row back to its Bronze line
LINEAGE = ["source_file", "source_line_number", "_bronze_run_id"]

# Expected columns of bronze.inmet_hourly. Written out (not imported)
# so a change in Bronze is caught by the schema check.
BRONZE_COLUMNS = [
    "source_year", "station_code", "source_file", "source_line_number",
    "data", "hora_utc", *RENAME,
    "_bronze_source", "_bronze_run_id", "_bronze_ingestion_time", "_bronze_code_version",
]

# invalid values: physically impossible, set to null
# (low, high), None = no limit
INVALID = {
    "precip_mm": (0, None),
    "wind_speed_ms": (0, None),
    "wind_gust_ms": (0, None),
    "humidity_pct": (0, 100),
    "humidity_max_pct": (0, 100),
    "humidity_min_pct": (0, 100),
    "wind_dir_deg": (0, 360),
    "pressure_mb": (1, None),
    "pressure_max_mb": (1, None),
    "pressure_min_mb": (1, None),
    "radiation_kj_m2": (None, 5000),          # max is about 4,900 kJ/m² per hour
}
# max below min in the same hour
INVALID_MAX_BELOW_MIN = [
    ("temp_max_c", "temp_min_c"),
    ("dew_point_max_c", "dew_point_min_c"),
    ("pressure_max_mb", "pressure_min_mb"),
    ("humidity_max_pct", "humidity_min_pct"),
]
DEW_POINT_TOLERANCE_C = 0.5                   # dew_point_c > temp_c + 0.5 is invalid
# small negative radiation at night is sensor noise -> 0
NEGATIVE_RADIATION_TO_ZERO = True

# unusual values: possible but rare, kept with a flag
# starting values, to be tuned with the Bronze profile percentiles
UNUSUAL = {
    "precip_mm": (None, 100),
    "temp_c": (-10, 45),
    "temp_max_c": (-10, 45),
    "temp_min_c": (-10, 45),
    "wind_gust_ms": (None, 40),
    "pressure_mb": (700, 1100),
    "pressure_max_mb": (700, 1100),
    "pressure_min_mb": (700, 1100),
}
UNUSUAL_TEMP_JUMP_C = 10                      # change from the previous hour

# time zones (IANA names, so historic DST is handled)
STATE_TIMEZONE = {
    # UTC-3
    **{uf: "America/Sao_Paulo" for uf in [
        "AL", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "PA", "PB",
        "PE", "PI", "PR", "RJ", "RN", "RS", "SC", "SE", "SP", "TO"]},
    # UTC-4
    "MT": "America/Cuiaba",
    "MS": "America/Campo_Grande",
    "RO": "America/Porto_Velho",
    "RR": "America/Boa_Vista",
    "AM": "America/Manaus",
    # UTC-5
    "AC": "America/Rio_Branco",
}
# Stations not on their state's time zone, e.g. western Amazonas ("America/Eirunepe")
# or Fernando de Noronha ("America/Noronha").
STATION_TIMEZONE_OVERRIDE = {}

# stations
# smaller moves are treated as rounding
LOCATION_TOLERANCE_DEG = 0.001                # about 100 m
ALTITUDE_TOLERANCE_M = 1.0
# Rough box around Brazil incl. islands. Catches clearly wrong coordinates
# (e.g. a missing minus sign), not stations just over the border.
BRAZIL_BOX = {"lat": (-34.0, 5.5), "lon": (-74.5, -28.5)}


def check_schema(columns, expected=BRONZE_COLUMNS):
    # also used by the fire job with expected=fire_contract.BRONZE_COLUMNS
    missing = [c for c in expected if c not in columns]
    new = [c for c in columns if c not in expected]
    if missing or new:
        raise ValueError(f"Bronze schema changed. Missing: {missing}. New: {new}.")
