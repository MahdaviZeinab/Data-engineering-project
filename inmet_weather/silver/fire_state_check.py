"""
Check that a fire lies inside the state INPE assigns it to (STATE_BOX in fire_contract.py).
Points outside the box get dq_issue = wrong_state.
"""

from pyspark.sql import functions as F

from silver import fire_contract


def inside_state():
    # null when the state is unknown (handled as unknown_state)
    m = fire_contract.STATE_BOX_MARGIN_DEG
    check = None
    for uf, (lat_min, lat_max, lon_min, lon_max) in fire_contract.STATE_BOX.items():
        inside = (F.col("lat").between(lat_min - m, lat_max + m)
                  & F.col("lon").between(lon_min - m, lon_max + m))
        check = (F.when(F.col("state") == uf, inside) if check is None
                 else check.when(F.col("state") == uf, inside))
    return check
