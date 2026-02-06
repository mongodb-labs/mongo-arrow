import numpy as np

try:
    from pandas import __version__ as pd_version
except ImportError:
    pd_version = "2.0.0"


def base_make_data(make_datum):
    if pd_version.startswith("2."):
        return (
            [make_datum() for _ in range(8)]
            + [np.nan]
            + [make_datum() for _ in range(88)]
            + [np.nan]
            + [make_datum(), make_datum()]
        )
    return (
        [make_datum() for _ in range(4)]
        + [np.nan]
        + [make_datum(), make_datum()]
        + [np.nan]
        + [make_datum(), make_datum()]
    )
