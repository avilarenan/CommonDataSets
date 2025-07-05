import warnings
from tqdm.notebook import tqdm

from PyFARM import farm

from dtaidistance import dtw

from scipy.spatial.distance import euclidean

from pyinform.relativeentropy import relative_entropy
from pyinform.mutualinfo import mutual_info
from pyinform.utils.coalesce import coalesce_series

import numpy as np
import pandas as pd

from numpy.lib.stride_tricks import sliding_window_view

from scipy.stats import skewnorm

warnings.filterwarnings('once')

def pfarm(farm_params):
    '''
    FARM SHAPING
    farm_params = {
        "df_raw" : df_raw,
        "window" : window,
        "exogenous_feature": feature,
        "target_feature": target
    }
    '''
    df_raw = farm_params["df_raw"]
    window = farm_params["window"]
    exogenous_feature = str(farm_params["exogenous_feature"])
    target_feature = str(farm_params["target_feature"])
    shaping_ratio = pd.Series(farm(
        refTS=df_raw[target_feature].values,
        qryTS=df_raw[str(exogenous_feature)].values,
        ff_align=False,
        lcwin=window,
        fuzzyc=[1]
    )["rel_local_fuzz"])
    shaping_ratio_inverted = (shaping_ratio - 1).abs() # NOTE: INVERTING

    shaping_ratio = shaping_ratio.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)
    shaping_ratio_inverted = shaping_ratio_inverted.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)

    ret = df_raw[str(exogenous_feature)] * shaping_ratio
    ret_inverted = df_raw[str(exogenous_feature)] * shaping_ratio_inverted

    return {"shaped" : ret, "inverted_shaped": ret_inverted}, exogenous_feature

def prollcorr(params):
    '''
    CORRELATION SHAPING
    params = {
        "df_raw" : df_raw,
        "window" : window,
        "exogenous_feature": feature,
        "target_feature": target
    }
    '''
    df_raw = params["df_raw"]
    window = params["window"]
    exogenous_feature = str(params["exogenous_feature"])
    target_feature = str(params["target_feature"])
    saliency = df_raw[target_feature].rolling(window).corr(df_raw[str(exogenous_feature)])
    
    shaping_ratio = (saliency-saliency.min())/(saliency.max() - saliency.min()) # normalizing between 0 and 1
    shaping_ratio_inverted = (shaping_ratio - 1).abs() # NOTE: INVERTING

    shaping_ratio = shaping_ratio.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)
    shaping_ratio_inverted = shaping_ratio_inverted.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)

    ret = df_raw[str(exogenous_feature)] * shaping_ratio
    ret_inverted = df_raw[str(exogenous_feature)] * shaping_ratio_inverted

    return {"shaped" : ret, "inverted_shaped": ret_inverted}, exogenous_feature

def prollcov(params):
    '''
    COVARIANCE SHAPING
    params = {
        "df_raw" : df_raw,
        "window" : window,
        "exogenous_feature": feature,
        "target_feature": target
    }
    '''
    df_raw = params["df_raw"]
    window = params["window"]
    exogenous_feature = str(params["exogenous_feature"])
    target_feature = str(params["target_feature"])
    saliency = df_raw[target_feature].rolling(window).cov(df_raw[str(exogenous_feature)])

    shaping_ratio = (saliency-saliency.min())/(saliency.max() - saliency.min()) # normalizing between 0 and 1
    shaping_ratio_inverted = (shaping_ratio - 1).abs() # NOTE: INVERTING

    shaping_ratio = shaping_ratio.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)
    shaping_ratio_inverted = shaping_ratio_inverted.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)

    ret = df_raw[str(exogenous_feature)] * shaping_ratio
    ret_inverted = df_raw[str(exogenous_feature)] * shaping_ratio_inverted

    return {"shaped" : ret, "inverted_shaped": ret_inverted}, exogenous_feature

def pentropy(params):
    '''
    RELATIVE ENTROPY SHAPING
    params = {
        "df_raw" : df_raw,
        "window" : window,
        "exogenous_feature": feature,
        "target_feature": target
    }
    '''
    df_raw = params["df_raw"]
    window = params["window"]
    exogenous_feature = str(params["exogenous_feature"])
    target_feature = str(params["target_feature"])

    target_values, _ = coalesce_series(df_raw[target_feature].values)
    exogenous_values, _ = coalesce_series(df_raw[exogenous_feature].values)

    target_windows = sliding_window_view(target_values, window_shape=window)
    exogenous_windows = sliding_window_view(exogenous_values, window_shape=window)

    result = np.array([relative_entropy(a, b) for a, b in zip(target_windows, exogenous_windows)])
    saliency = pd.Series([np.nan]*(window-1) + list(result))

    shaping_ratio = (saliency-saliency.min())/(saliency.max() - saliency.min()) # normalizing between 0 and 1
    shaping_ratio_inverted = (shaping_ratio - 1).abs() # NOTE: INVERTING

    shaping_ratio = shaping_ratio.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)
    shaping_ratio_inverted = shaping_ratio_inverted.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)

    ret = df_raw[str(exogenous_feature)] * shaping_ratio
    ret_inverted = df_raw[str(exogenous_feature)] * shaping_ratio_inverted

    return {"shaped" : ret, "inverted_shaped": ret_inverted}, exogenous_feature

def pmutual_info(params):
    '''
    MUTUAL INFORMATION SHAPING
    params = {
        "df_raw" : df_raw,
        "window" : window,
        "exogenous_feature": feature,
        "target_feature": target
    }
    '''
    df_raw = params["df_raw"]
    window = params["window"]
    exogenous_feature = str(params["exogenous_feature"])
    target_feature = str(params["target_feature"])

    target_values, _ = coalesce_series(df_raw[target_feature].values)
    exogenous_values, _ = coalesce_series(df_raw[exogenous_feature].values)

    target_windows = sliding_window_view(target_values, window_shape=window)
    exogenous_windows = sliding_window_view(exogenous_values, window_shape=window)

    result = np.array([mutual_info(a, b) for a, b in zip(target_windows, exogenous_windows)])
    saliency = pd.Series([np.nan]*(window-1) + list(result))

    shaping_ratio = (saliency-saliency.min())/(saliency.max() - saliency.min()) # normalizing between 0 and 1
    shaping_ratio_inverted = (shaping_ratio - 1).abs() # NOTE: INVERTING

    shaping_ratio = shaping_ratio.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)
    shaping_ratio_inverted = shaping_ratio_inverted.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)

    ret = df_raw[str(exogenous_feature)] * shaping_ratio
    ret_inverted = df_raw[str(exogenous_feature)] * shaping_ratio_inverted

    return {"shaped" : ret, "inverted_shaped": ret_inverted}, exogenous_feature

def pdtw(params):
    '''
    DTW DISTANCE SHAPING
    params = {
        "df_raw" : df_raw,
        "window" : window,
        "exogenous_feature": feature,
        "target_feature": target
    }
    '''
    df_raw = params["df_raw"]
    window = params["window"]
    exogenous_feature = str(params["exogenous_feature"])
    target_feature = str(params["target_feature"])

    target_values = df_raw[target_feature].values
    exogenous_values = df_raw[exogenous_feature].values

    target_windows = sliding_window_view(target_values, window_shape=window)
    exogenous_windows = sliding_window_view(exogenous_values, window_shape=window)

    dtw_dists = []
    for a, b in zip(target_windows, exogenous_windows):
        a = np.array(a, dtype=np.double)
        b = np.array(b, dtype=np.double)
        dtw_dist = dtw.distance_fast(a, b, use_pruning=True)
        dtw_dists += [dtw_dist]

    result = np.array(dtw_dists)
    
    saliency = pd.Series([np.nan]*(window-1) + list(result))

    shaping_ratio = (saliency-saliency.min())/(saliency.max() - saliency.min()) # normalizing between 0 and 1
    shaping_ratio_inverted = (shaping_ratio - 1).abs() # NOTE: INVERTING

    shaping_ratio = shaping_ratio.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)
    shaping_ratio_inverted = shaping_ratio_inverted.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)

    ret = df_raw[str(exogenous_feature)] * shaping_ratio
    ret_inverted = df_raw[str(exogenous_feature)] * shaping_ratio_inverted

    return {"shaped" : ret, "inverted_shaped": ret_inverted}, exogenous_feature

def pnoise(params, skew=0):
    '''
    CORRELATION SHAPING
    params = {
        "df_raw" : df_raw,
        "window" : window,
        "exogenous_feature": feature,
        "target_feature": target
    }
    '''

    seed_value = 42
    np.random.seed(seed_value)

    df_raw = params["df_raw"]
    window = params["window"]
    exogenous_feature = str(params["exogenous_feature"])
    target_feature = str(params["target_feature"])

    from scipy.stats import skewnorm

    # Define parameters for the skew-normal distribution
    skew = 0  # Skewness parameter (positive for right skew, negative for left skew)
    loc = 0  # Location parameter (mean)
    scale = 1  # Scale parameter (standard deviation)
    series_size = len(target_feature)  # Number of samples

    # Generate random samples from a skew-normal distribution
    skew_normal_noise = skewnorm.rvs(skew, loc=loc, scale=scale, size=series_size)

    # Create a Pandas Series from the generated noise
    skew_noise_series = pd.Series(skew_normal_noise)
    saliency = pd.Series(skew_noise_series).rolling(window).mean()
    shaping_ratio = (saliency-saliency.min())/(saliency.max() - saliency.min()) # normalizing between 0 and 1

    shaping_ratio_inverted = (shaping_ratio - 1).abs() # NOTE: INVERTING

    shaping_ratio = shaping_ratio.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)
    shaping_ratio_inverted = shaping_ratio_inverted.fillna(1) # NOTE: keep as it is if we can't calculate a ratio (NaN case)

    ret = df_raw[str(exogenous_feature)] * shaping_ratio
    ret_inverted = df_raw[str(exogenous_feature)] * shaping_ratio_inverted

    return {"shaped" : ret, "inverted_shaped": ret_inverted}, exogenous_feature

def pnoiseskew10(params):
    return pnoise(params, skew=10)