import warnings
from datasetsforecast.long_horizon2 import LongHorizon2
# from datasetsforecast.long_horizon import LongHorizon
import pandas as pd
pd.options.plotting.backend = "plotly"
from tqdm.notebook import tqdm
from joblib import Parallel, delayed
import joblib
from datasets_metadata import ts_metadata
import contextlib
from preprocessing_utilities import pfarm, prollcorr, prollcov, pentropy, pmutual_info, pdtw
warnings.filterwarnings('ignore')

PARALLEL = True
datasets_names = [
    "ETTh1",
    # "ETTh2",
    # "ETTm1",
    # "ETTm2",
    # "Weather",
    # "ECL",
    # "TrafficL"
]

@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""
    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()

for dataset_name in tqdm(datasets_names, desc="Datasets"):

    ds_file_path = ts_metadata[dataset_name]["relative_path"]

    df_raw = pd.read_csv(ds_file_path)

    df_raw.columns = [str(col) for col in df_raw.columns]

    target_ts = ts_metadata[dataset_name]["target_ts"]
    exog_list = ts_metadata[dataset_name]["exog_list"]
    test_size = ts_metadata[dataset_name]["test_size"]
    valid_size = ts_metadata[dataset_name]["valid_size"]
    farm_windows = ts_metadata[dataset_name]["farm_windows"]

    # SHAPING
    list_of_process_fns = [
        pfarm,
        prollcorr,
        prollcov,
        pentropy,
        pmutual_info,
        # process_dtw,
    ]
    
    list_of_processed_dfs = []
    for window in tqdm(farm_windows, leave=False, desc="Iterating windows"):

        rolling_stats_params_list = []
        for feature in exog_list:
            rolling_stats_params_list += [
                {
                    "df_raw": df_raw,
                    "window": window,
                    "target_feature": target_ts,
                    "exogenous_feature": feature
                }
            ]

        for process_fn in tqdm(list_of_process_fns, leave=False, desc="Prcessing fns"):
            
            if PARALLEL:
                tqdm_it = tqdm(desc="Parallel Feature engineering processing", total=len(rolling_stats_params_list), leave=False)
                with tqdm_joblib(tqdm_it) as progress_bar:
                    results_processed = Parallel(n_jobs=-1)(delayed(process_fn)(param) for param in rolling_stats_params_list)
                tqdm_it.container.close()
            else:
                # SEQUENTIAL PROCESSING
                results_processed = []
                for param in tqdm(rolling_stats_params_list, desc="Sequential Feature engineering processing", leave=False):
                    results_processed += [process_fn(param)]
                
            df_processed = df_raw.copy()
            df_processed_inverted = df_raw.copy()

            for agg_qts_shaped, feature in results_processed:
                if feature == target_ts:
                    continue

                qts_shaped = agg_qts_shaped.get("shaped")
                if qts_shaped is None:
                    raise Exception(f"No shaped ts provided: {qts_shaped}")
                
                df_processed[str(feature)] = qts_shaped
                
                qts_shaped_inverted = agg_qts_shaped.get("inverted_shaped")

                df_processed_inverted[str(feature)] = qts_shaped_inverted


            df_processed_unique_id = f"w{window}_{process_fn.__name__}"
            df_processed.to_csv(f"./processed_data/{dataset_name}_{df_processed_unique_id}.csv", index=False)

            df_processed_unique_id_inverted = f"w{window}_i{process_fn.__name__}"
            df_processed_inverted.to_csv(f"./processed_data/{dataset_name}_{df_processed_unique_id_inverted}.csv", index=False)