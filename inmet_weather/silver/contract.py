"""
Silver contract: every Silver rule in one place.

The Silver job reads its rules from here, so the code and the README can't disagree.
Plain Python only (no Spark), so it is easy to read and to test.
"""

# --------------------------------------------------------------------------
# Names: Bronze (Portuguese, as INMET writes it) -> Silver (short English)
# "Data" + "Hora UTC" are not renamed: together they become observed_at_utc (S5).
RENAME = {
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "precip_mm",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressure_mb",
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)": "pressure_max_mb",
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)": "pressure_min_mb",
    "RADIACAO GLOBAL (Kj/m²)": "radiation_kj_m2",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "temp_c",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)": "dew_point_c",
    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": "temp_max_c",
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": "temp_min_c",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)": "dew_point_max_c",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)": "dew_point_min_c",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)": "humidity_max_pct",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)": "humidity_min_pct",
    "UMIDADE RELATIVA DO AR, HORARIA (%)": "humidity_pct",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "wind_dir_deg",
    "VENTO, RAJADA MAXIMA (m/s)": "wind_gust_ms",
    "VENTO, VELOCIDADE HORARIA (m/s)": "wind_speed_ms",
}
MEASUREMENTS = list(RENAME.values())          # the 17 double columns

KEY = ["station_code", "observed_at_utc"]     # one row per station per hour
MISSING_CODES = ["", "-9999"]                 # INMET's "no value" (becomes null)

# Lineage: kept so every Silver row points back to its Bronze line.
LINEAGE = ["source_file", "source_line_number", "_bronze_run_id"]

# The exact columns Silver expects in bronze/inmet_hourly.
# Written out here on purpose (not imported from Bronze): if Bronze changes,
# this check notices it.
BRONZE_COLUMNS = [
    "source_year", "station_code", "source_file", "source_line_number",
    "Data", "Hora UTC", *RENAME,
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


def check_schema(columns):
    """Stop the Silver job if Bronze has a missing or a new column."""
    missing = [c for c in BRONZE_COLUMNS if c not in columns]
    new = [c for c in columns if c not in BRONZE_COLUMNS]
    if missing or new:
        raise ValueError(f"Bronze schema changed. Missing: {missing}. New: {new}.")
