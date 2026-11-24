"""
Check that a fire is inside the state INPE gives it.

Every state has a box around it (STATE_BOX in silver/fire_contract.py).
A fire whose point is outside the box of its state gets dq_issue = wrong_state.

A box is bigger than the real state, so a fire a few km over a border is not found.
We only want the big mistakes: a wrong state name, or lat and lon swapped.
"""

from pyspark.sql import functions as F

from silver import fire_contract


def inside_state():
    # True when lat/lon is inside the box of the fire's state (with a small margin).
    # Null when the state is unknown: that is a different problem (unknown_state).
    m = fire_contract.STATE_BOX_MARGIN_DEG
    check = None
    for uf, (lat_min, lat_max, lon_min, lon_max) in fire_contract.STATE_BOX.items():
        inside = (F.col("lat").between(lat_min - m, lat_max + m)
                  & F.col("lon").between(lon_min - m, lon_max + m))
        check = (F.when(F.col("state") == uf, inside) if check is None
                 else check.when(F.col("state") == uf, inside))
    return check
