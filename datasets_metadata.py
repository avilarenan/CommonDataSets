# 'ETTh1', 'ETTh2', 'ETTm1', 'ETTm2', 'ECL', 'TrafficL', 'Weather'
from datasetsforecast.long_horizon2 import ETTh1, ETTh2, ETTm1, ETTm2, ECL, TrafficL, Weather


ts_metadata = {
    "ETTh1": {
        "relative_path": "./raw/ETT-small/ETTh1.csv",
        "target_ts": "OT",
        "exog_list": ['HUFL', 'HULL', 'MUFL', 'MULL', "LUFL", "LULL"],
        "freq": "h",
        "test_size": 2881,
        "valid_size": 2881,
        "farm_windows": [501, 751, 1001, 1251, 1501]
    },
    "ETTh2": {
        "relative_path": "./raw/ETT-small/ETTh2.csv",
        "target_ts": "OT",
        "exog_list": ['HUFL', 'HULL', 'MUFL', 'MULL', "LUFL", "LULL"],
        "freq": "h",
        "test_size": 2881,
        "valid_size": 2881,
        "farm_windows": [501, 751, 1001, 1251, 1501]
    },
    "ETTm1": {
        "relative_path": "./raw/ETT-small/ETTm1.csv",
        "target_ts": "OT",
        "exog_list": ['HUFL', 'HULL', 'MUFL', 'MULL', "LUFL", "LULL"],
        "freq": "min",
        "test_size": 11521,
        "valid_size": 11521,
        "farm_windows": [501, 751, 1001, 1251, 1501]
    },
    "ETTm2": {
        "relative_path": "./raw/ETT-small/ETTm2.csv",
        "target_ts": "OT",
        "exog_list": ['HUFL', 'HULL', 'MUFL', 'MULL', "LUFL", "LULL"],
        "freq": "min",
        "test_size": 11521,
        "valid_size": 11521,
        "farm_windows": [501, 751, 1001, 1251, 1501]
    },
    "Weather" : {
        "relative_path": "./raw/weather/weather.csv",
        "target_ts": "OT",
        "exog_list": ['H2OC (mmol/mol)', 'PAR (�mol/m�/s)', 'SWDR (W/m�)', 'T (degC)', 'Tdew (degC)', 'Tlog (degC)', 'Tpot (K)', 'VPact (mbar)', 'VPdef (mbar)', 'VPmax (mbar)', 'max. PAR (�mol/m�/s)', 'max. wv (m/s)', 'p (mbar)', 'rain (mm)', 'raining (s)', 'rh (%)', 'rho (g/m**3)', 'sh (g/kg)', 'wd (deg)', 'wv (m/s)'],
        "freq": "10min",
        "test_size": 10540,
        "valid_size": 5271,
        "farm_windows": [501, 751, 1001, 1251, 1501]
    },
    "TrafficL" : {
        "relative_path": "./raw/traffic/traffic.csv",
        "target_ts": "OT",
        "exog_list": list(range(860+1)),
        "freq": "h",
        "test_size": 3509,
        "valid_size": 1757,
        "farm_windows": [501, 751, 1001, 1251, 1501]
    },
    "ECL" : {
        "relative_path": "./raw/electricity/electricity.csv",
        "target_ts": "OT",
        "exog_list": list(range(319+1)),
        "freq": "15min",
        "test_size": 5261,
        "valid_size": 2633,
        "farm_windows": [501, 751, 1001, 1251, 1501]
    }
}