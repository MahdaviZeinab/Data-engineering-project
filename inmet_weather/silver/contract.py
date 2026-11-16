"""
Silver contract: every Silver rule in one place.

The Silver job reads its rules from here, so the code and the README can't disagree.
Plain Python only (no Spark), so it is easy to read and to test.
"""

# --------------------------------------------------------------------------
# Names: Bronze (simple Portuguese names, see bronze/bronze_ingest.py) -> Silver (short English)
# "data" + "hora_utc" are not renamed: together they become observed_at_utc (S5).
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
MEASUREMENTS = list(RENAME.values())          # the 17 double columns

KEY = ["station_code", "observed_at_utc"]     # one row per station per hour
MISSING_CODES = ["", "-9999"]                 # INMET's "no value" (becomes null)

# Lineage: kept so every Silver row points back to its Bronze line.
LINEAGE = ["source_file", "source_line_number", "_bronze_run_id"]

# The exact columns Silver expects in the table bronze.inmet_hourly.
# Written out here on purpose (not imported from Bronze): if Bronze changes,
# this check notices it.
BRONZE_COLUMNS = [
    "source_year", "station_code", "source_file", "source_line_number",
    "data", "hora_utc", *RENAME,
    "_bronze_source", "_bronze_run_id", "_bronze_ingestion_time", "_bronze_code_version",
]

# --------------------------------------------------------------------------
# Invalid = physically impossible -> that one value becomes null (S8).
# (low, high): a value below low or above high is invalid. None = no limit.
INVALID = {
    "precip_mm": (0, None),                   # can't be negative
    "wind_speed_ms": (0, None),
    "wind_gust_ms": (0, None),
    "humidity_pct": (0, 100),                 # a percentage
    "humidity_max_pct": (0, 100),
    "humidity_min_pct": (0, 100),
    "wind_dir_deg": (0, 360),                 # compass direction
    "pressure_mb": (1, None),                 # pressure can't be 0 (no real value is below 1 mB)
    "pressure_max_mb": (1, None),
    "pressure_min_mb": (1, None),
    "radiation_kj_m2": (None, 5000),          # sun gives ~4,900 kJ/m² per hour at most
}
# max can't be below min in the same hour
INVALID_MAX_BELOW_MIN = [
    ("temp_max_c", "temp_min_c"),
    ("dew_point_max_c", "dew_point_min_c"),
    ("pressure_max_mb", "pressure_min_mb"),
    ("humidity_max_pct", "humidity_min_pct"),
]
DEW_POINT_TOLERANCE_C = 0.5                   # dew_point_c > temp_c + 0.5 is invalid
# Small negative radiation at night (e.g. -3) is sensor noise: set to 0 and count it.
NEGATIVE_RADIATION_TO_ZERO = True

# --------------------------------------------------------------------------
# Unusual = possible but rare -> keep the value, add a flag (S8).
# Start values; update them with the S1 percentiles.
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
UNUSUAL_TEMP_JUMP_C = 10                      # change from the hour before, same station

# --------------------------------------------------------------------------
# Time zones (S5): IANA names, so Spark also knows old summer time.
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
# Stations whose zone is not their state's zone: station_code -> zone.
# Western Amazonas (UTC-5, "America/Eirunepe") and Fernando de Noronha
# (UTC-2, "America/Noronha"). Fill in from the S1 station list.
STATION_TIMEZONE_OVERRIDE = {}

# --------------------------------------------------------------------------
# Stations (S7): bronze/inmet_files has the 8 info lines of every file
# (one file = one station, one year).
# A move smaller than this is only rounding, not a real change.
LOCATION_TOLERANCE_DEG = 0.001                # about 100 m
ALTITUDE_TOLERANCE_M = 1.0
# Simple box around Brazil, incl. the islands (Fernando de Noronha, Trindade).
# A box is not the real border: it finds clearly wrong coordinates
# (e.g. a missing minus sign), not stations a few km over the border.
BRAZIL_BOX = {"lat": (-34.0, 5.5), "lon": (-74.5, -28.5)}


def check_schema(columns):
    """Stop the Silver job if Bronze has a missing or a new column."""
    missing = [c for c in BRONZE_COLUMNS if c not in columns]
    new = [c for c in columns if c not in BRONZE_COLUMNS]
    if missing or new:
        raise ValueError(f"Bronze schema changed. Missing: {missing}. New: {new}.")
