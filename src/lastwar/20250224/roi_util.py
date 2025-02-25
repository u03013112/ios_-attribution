#!/usr/bin/env python
# coding: utf-8
import argparse
import copy
import random
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import roi_data
import roi_data_country
import roi_date_util
from dateutil.relativedelta import relativedelta
from roi_constant import *
from roi_data import *
from scipy.optimize import curve_fit
from sklearn import metrics

lock = threading.Lock()


class TrainParams:

    def __init__(
        self,
        app,
        data,
        type,
        media,
        country,
        predict_type,
        cost_currency,
        revenue_currency,
        start_date,
        end_date,
    ):
        self.app = app
        self.data = data
        self.type = type
        self.media = media
        self.country = country
        self.predict_type = predict_type
        self.cost_currency = cost_currency
        self.revenue_currency = revenue_currency
        self.start_date = start_date
        self.end_date = end_date
        self.organic = 0

    def __dict(self):
        return {
            "app": self.app,
            "data": self.data,
            "type": self.type,
            "media": self.media,
            "country": self.country,
            "predict_type": self.predict_type,
            "cost_currency": self.cost_currency,
            "revenue_currency": self.revenue_currency,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "organic": self.organic,
        }

    def __repr__(self):
        return str(self.__dict())

    def __str__(self):
        return str(self.__dict())


def power_function(x, *args):
    feature_count = len(x)
    params_count = len(args)
    result = args[0] * np.exp(-args[1] * x[0])
    for index in range(1, feature_count):
        result += args[index + 1] * x[index]
    return result + args[params_count - 1]


def get_p0(feature_count):
    return [0.1] * (feature_count + 2)


def get_p0_v1(feature_count):
    return [0.0] * (feature_count + 3)


def get_sigams(argparses, df_train, y_train):
    if argparses.weights and len(argparses.weights) > 0:
        weights = df_train[argparses.weights] / df_train[argparses.weights].max()
        weights = weights.to_numpy()
        sigma_length = int(len(y_train) / df_train.shape[0])
        sigmas = []
        for index in range(len(weights)):
            sigmas = np.append(sigmas, [weights[index]] * sigma_length)
    else:
        sigmas = [1.0] * len(y_train)
    return sigmas


def curve_fit_roi(argparses, df_train, x_train, y_train, feature_count):
    try:
        popt, pcov = curve_fit(
            power_function,
            x_train,
            y_train,
            p0=get_p0(feature_count),
            sigma=get_sigams(argparses, df_train, y_train),
            absolute_sigma=False,
        )
        return popt, pcov
    except Exception as e:
        if "Optimal parameters not found" in str(e):
            print(argparses)
            print(e)
            popt, pcov = curve_fit(
                power_function,
                x_train,
                y_train,
                p0=get_p0_v1(feature_count),
                sigma=get_sigams(argparses, df_train, y_train),
                absolute_sigma=False,
            )
            return popt, pcov
        else:
            raise e


def custom_curve_fit_roi(
    custom_power_function, argparses, df_train, x_train, y_train, feature_count
):
    return curve_fit(
        custom_power_function,
        x_train,
        y_train,
        p0=get_p0(feature_count),
        sigma=get_sigams(argparses, df_train, y_train),
        absolute_sigma=False,
    )


def get_app(argparses):
    return APPS[argparses.app]


def get_data(argparses):
    return DATAS[argparses.data]


def get_type(argparses):
    return TYPES[argparses.type]


def get_media(argparses):
    return MEDIAS[argparses.media]


def get_country(argparses):
    if is_last_war(argparses):
        return roi_data_country.get_country(argparses)
    return COUNTRIES[argparses.country]


def get_predict_type(argparses):
    return PREDICT_TYPES[PREDICT_TYPE_ROI]


def get_cost_currency(argparses):
    if argparses.data in COST_CURRENCYS.keys():
        return COST_CURRENCYS[argparses.data]
    return COST_CURRENCY_US


def get_revenue_currency(argparses):
    if argparses.data in REVENUE_CURRENCYS.keys():
        return REVENUE_CURRENCYS[argparses.data]
    return REVENUE_CURRENCY_US


def get_linear_roi_360_key(argparses):
    return "linear_sum"


def get_linear_roi_030_key(argparses):
    return "linear_sum"


def is_data_raw(argparses):
    return argparses.data in [
        TOP_WAR_RAW,
        TOP_WAR_WX_RAW,
        TOP_WAR_ARPU_MONTHLY,
        TOP_WAR_ARPU_WEEKLY,
        TOP_WAR_ARPU_WX_MONTHLY,
        TOP_WAR_ARPU_WX_WEEKLY,
        LAST_WAR_RAW,
        LAST_WAR_RAW_WEEKLY,
        LAST_WAR_RAW_GPIR,
        LAST_WAR_ARPU_MONTHLY,
        LAST_WAR_ARPU_WEEKLY,
        TOP_HERO_RAW,
        TOP_HERO_RAW_WEEKLY,
        TOP_HERO_RAW_GPIR,
        TOP_HERO_ARPU_MONTHLY,
        TOP_HERO_ARPU_WEEKLY,
        TOP_HERO_CN_RAW,
        TOP_HERO_CN_RAW_WEEKLY,
        TOP_HERO_WX_RAW,
        TOP_HERO_WX_RAW_WEEKLY,
    ]


def is_data_profit(argparses):
    return argparses.data in [
        TOP_WAR_PROFIT,
        TOP_WAR_WX_PROFIT,
        LAST_WAR_PROFIT,
        LAST_WAR_PROFIT_GPIR,
        TOP_HERO_PROFIT,
        TOP_HERO_PROFIT_GPIR,
        TOP_HERO_PROFIT_WEEKLY,
        TOP_HERO_CN_PROFIT,
        TOP_HERO_WX_PROFIT,
    ]


def to_short_data(argparses):
    if is_data_raw(argparses):
        return RAW
    if is_data_profit(argparses):
        return PROFIT
    return argparses.data


DATA_RAWS = {
    TOP_WAR_RAW: TOP_WAR_RAW,
    TOP_WAR_PROFIT: TOP_WAR_RAW,
    TOP_WAR_RAW_WEEKLY: TOP_WAR_RAW_WEEKLY,
    TOP_WAR_WX_RAW: TOP_WAR_WX_RAW,
    TOP_WAR_WX_PROFIT: TOP_WAR_WX_RAW,
    LAST_WAR_RAW: LAST_WAR_RAW,
    LAST_WAR_PROFIT: LAST_WAR_RAW,
    LAST_WAR_RAW_GPIR: LAST_WAR_RAW_GPIR,
    LAST_WAR_PROFIT_GPIR: LAST_WAR_RAW_GPIR,
    TOP_HERO_RAW: TOP_HERO_RAW,
    TOP_HERO_PROFIT: TOP_HERO_RAW,
    TOP_HERO_RAW_GPIR: TOP_HERO_RAW_GPIR,
    TOP_HERO_PROFIT_GPIR: TOP_HERO_RAW_GPIR,
    TOP_HERO_CN_RAW: TOP_HERO_CN_RAW,
    TOP_HERO_CN_PROFIT: TOP_HERO_CN_RAW,
    TOP_HERO_WX_RAW: TOP_HERO_WX_RAW,
    TOP_HERO_WX_PROFIT: TOP_HERO_WX_RAW,
}


def to_data_raw(argparses):
    return DATA_RAWS[argparses.data]


DAILY_DATA_RAWS = {
    LAST_WAR_RAW_WEEKLY: LAST_WAR_RAW,
    TOP_HERO_RAW_WEEKLY: TOP_HERO_RAW,
}


def to_daily_data_raw(argparses):
    return DAILY_DATA_RAWS[argparses.data]


def to_raw_table(params):
    return DATAS_TO_RAW_TABLE[params.data]


DATA_PROFITS = {
    TOP_WAR_RAW: TOP_WAR_PROFIT,
    TOP_WAR_PROFIT: TOP_WAR_PROFIT,
    TOP_WAR_WX_RAW: TOP_WAR_WX_PROFIT,
    TOP_WAR_WX_PROFIT: TOP_WAR_WX_PROFIT,
    LAST_WAR_RAW: LAST_WAR_PROFIT,
    LAST_WAR_PROFIT: LAST_WAR_PROFIT,
    LAST_WAR_RAW_GPIR: LAST_WAR_PROFIT_GPIR,
    LAST_WAR_PROFIT_GPIR: LAST_WAR_PROFIT_GPIR,
    TOP_HERO_RAW: TOP_HERO_PROFIT,
    TOP_HERO_PROFIT: TOP_HERO_PROFIT,
    TOP_HERO_CN_RAW: TOP_HERO_CN_PROFIT,
    TOP_HERO_CN_PROFIT: TOP_HERO_CN_PROFIT,
    TOP_HERO_WX_RAW: TOP_HERO_WX_PROFIT,
    TOP_HERO_WX_PROFIT: TOP_HERO_WX_PROFIT,
}


def to_data_profit(argparses):
    return DATA_PROFITS[argparses.data]


DAILY_DATA_PROFITS = {
    LAST_WAR_RAW_WEEKLY: LAST_WAR_PROFIT,
    TOP_HERO_RAW_WEEKLY: TOP_HERO_PROFIT,
}


def to_daily_data_profit(argparses):
    return DAILY_DATA_PROFITS[argparses.data]


DATA_WEEKLY = {
    TOP_WAR_RAW: TOP_WAR_RAW_WEEKLY,
    TOP_WAR_PROFIT: TOP_WAR_RAW_WEEKLY,
    TOP_WAR_WX_RAW: TOP_WAR_WX_RAW_WEEKLY,
    TOP_WAR_WX_PROFIT: TOP_WAR_WX_RAW_WEEKLY,
    LAST_WAR_RAW: LAST_WAR_RAW_WEEKLY,
    LAST_WAR_PROFIT: LAST_WAR_RAW_WEEKLY,
    TOP_HERO_RAW: TOP_HERO_RAW_WEEKLY,
    TOP_HERO_PROFIT: TOP_HERO_PROFIT_WEEKLY,
    TOP_HERO_CN_RAW: TOP_HERO_CN_RAW_WEEKLY,
    TOP_HERO_CN_PROFIT: TOP_HERO_CN_RAW_WEEKLY,
    TOP_HERO_WX_RAW: TOP_HERO_WX_RAW_WEEKLY,
    TOP_HERO_WX_PROFIT: TOP_HERO_WX_RAW_WEEKLY,
}


def to_weekly_data(argparses):
    return DATA_WEEKLY[argparses.data]


TYPE_WEEKLY = {
    TOP_WAR_AOS: TOP_WAR_AOS_WEEKLY,
    TOP_WAR_IOS: TOP_WAR_IOS_WEEKLY,
    TOP_WAR_WX_AOS: TOP_WAR_WX_AOS_WEEKLY,
    TOP_WAR_WX_IOS: TOP_WAR_WX_IOS_WEEKLY,
    LAST_WAR_AOS: LAST_WAR_AOS_WEEKLY,
    LAST_WAR_IOS: LAST_WAR_IOS_WEEKLY,
    TOP_HERO_AOS: TOP_HERO_AOS_WEEKLY,
    TOP_HERO_IOS: TOP_HERO_IOS_WEEKLY,
    TOP_HERO_CN_AOS: TOP_HERO_CN_AOS_WEEKLY,
    TOP_HERO_CN_IOS: TOP_HERO_CN_IOS_WEEKLY,
    TOP_HERO_WX_AOS: TOP_HERO_WX_AOS_WEEKLY,
    TOP_HERO_WX_IOS: TOP_HERO_WX_IOS_WEEKLY,
}


def to_weekly_type(argparses):
    return TYPE_WEEKLY[argparses.type]


def to_weekly_argparses(argparses, total_days, feature_count):
    argparses_weekly = copy.deepcopy(argparses)
    setattr(argparses_weekly, "data", to_weekly_data(argparses))
    setattr(argparses_weekly, "type", to_weekly_type(argparses))
    setattr(argparses_weekly, "total_days", total_days)
    setattr(argparses_weekly, "features", feature_count)
    return argparses_weekly


def is_top_war(argparses):
    return argparses.data in [
        TOP_WAR_RAW,
        TOP_WAR_PROFIT,
        TOP_WAR_RAW_WEEKLY,
        TOP_WAR_ARPU_MONTHLY,
        TOP_WAR_ARPU_WEEKLY,
        TOP_WAR_WX_RAW,
        TOP_WAR_WX_PROFIT,
        TOP_WAR_WX_RAW_WEEKLY,
        TOP_WAR_ARPU_WX_MONTHLY,
        TOP_WAR_ARPU_WX_WEEKLY,
    ]


def is_top_war_wx(argparses):
    return argparses.data in [
        TOP_WAR_ARPU_WX_MONTHLY,
        TOP_WAR_ARPU_WX_WEEKLY,
    ]


def is_top_heroes(argparses):
    return argparses.app in [
        TOP_HERO,
    ]


def is_last_war(argparses):
    return argparses.app in [
        LAST_WAR,
    ]


PREDICT_ROI_360_KEYS = {
    "WX_AOS": "linear_sum",
    "WX_IOS": "linear_sum",
}


def get_predict_roi_360_key(argparses):
    return "predict_sum"


PREDICT_REVENUE_360_KEYS = {
    "WX_AOS": "linear_revenue",
    "WX_IOS": "linear_revenue",
}


def get_predict_revenue_360_key(argparses):
    return "predict_revenue"


def get_roi_007_key(argparses):
    return "d00007_roi"


ROI_030_KEYS = {
    "WX_AOS": "predict_roi_d00030",
    "WX_IOS": "predict_roi_d00030",
}


def get_roi_030_key(argparses):
    return "roi_d00030"


def get_roi_base_030(argparses):
    return 0.10


def get_roi_base_007(argparses):
    return 0.01


def remove_last_war_info(string):
    return string.replace("LAST_WAR_", "").replace("TOP_HERO_", "")


def use_top_war_info(argparses, train_param):
    setattr(train_param, "app", APP_TOP_WAR)
    setattr(train_param, "data", DATAS[remove_last_war_info(argparses.data)])
    setattr(train_param, "type", TYPES[remove_last_war_info(argparses.type)])
    setattr(
        train_param,
        "start_date",
        _get_train_start_date(
            argparses,
            remove_last_war_info(argparses.data),
            remove_last_war_info(argparses.type),
        ),
    )
    setattr(train_param, "country", [])
    setattr(train_param, "media", [])
    setattr(train_param, "organic", 0)
    return train_param


def use_top_war_train_data(argparses, train_params):
    new_train_params = copy.deepcopy(train_params)
    new_train_params[0] = use_top_war_info(argparses, new_train_params[0])
    new_train_params[1] = use_top_war_info(argparses, new_train_params[1])
    return new_train_params


def get_train_params(argparses):
    train_params = [
        TrainParams(
            get_app(argparses),
            get_data(argparses),
            get_type(argparses),
            get_media(argparses),
            get_country(argparses),
            get_predict_type(argparses),
            get_cost_currency(argparses),
            get_revenue_currency(argparses),
            argparses.train_start_date,
            argparses.train_end_date,
        ),
        TrainParams(
            get_app(argparses),
            get_data(argparses),
            get_type(argparses),
            get_media(argparses),
            get_country(argparses),
            get_predict_type(argparses),
            get_cost_currency(argparses),
            get_revenue_currency(argparses),
            argparses.test_start_date,
            argparses.test_end_date,
        ),
        TrainParams(
            get_app(argparses),
            get_data(argparses),
            get_type(argparses),
            get_media(argparses),
            get_country(argparses),
            get_predict_type(argparses),
            get_cost_currency(argparses),
            get_revenue_currency(argparses),
            argparses.validate_start_date,
            argparses.validate_end_date,
        ),
    ]
    return train_params[0], train_params[1], train_params[2]


def get_media_train_params(argparses):
    train_param, test_param, validate_param = get_train_params(argparses)
    setattr(train_param, "organic", 1)
    setattr(test_param, "organic", 1)
    setattr(validate_param, "organic", 1)
    return train_param, test_param, validate_param


def init_argparses(args, today, month_from, month_to, total_days):
    """
    Initialize argument parsing for a specific date range.

    Args:
        args: The arguments to be initialized.
        today: The current date.
        month_from: The start month for the date range.
        month_to: The end month for the date range.
        total_days: The total number of days in the date range.

    Returns:
        The initialized arguments for the specified date range.
    """
    return init_argparses_by_date(
        args, today, f"{month_from}01", f"{month_to}31", total_days
    )


def init_argparses_daily(args, today, month_from, month_to, total_days):
    """
    Initialize argument parsing for a specific daily date range.

    Args:
        args: The arguments to be initialized.
        today: The current date.
        month_from: The start month for the date range.
        month_to: The end month for the date range.
        total_days: The total number of days in the date range.

    Returns:
        The initialized arguments for the specified daily date range.
    """
    return init_argparses_by_date_daily(
        args, today, f"{month_from}01", f"{month_to}31", total_days
    )


def _get_train_start_date(argparses, _data, _type):
    if (
        _type
        in [
            TOP_WAR_AOS,
            TOP_WAR_AOS_WEEKLY,
        ]
        and argparses.media == MEDIA_SOURCE_TIKTOK
    ):
        return "20220101"
    if _type in [
        LAST_WAR_AOS,
        LAST_WAR_AOS_WEEKLY,
        LAST_WAR_IOS,
        LAST_WAR_IOS_WEEKLY,
    ] and argparses.country in [
        "JP",
        "GCC",
    ]:
        return "20240101"
    if (
        _type
        in [
            LAST_WAR_AOS,
            LAST_WAR_AOS_GPIR,
            LAST_WAR_AOS_WEEKLY,
        ]
        and argparses.media == MEDIA_SOURCE_APPLOVIN
    ):
        return "20240301"
    if _type in TYPE_TRAIN_START_DATES.keys():
        return TYPE_TRAIN_START_DATES[_type]
    return DATA_TRAIN_START_DATES[_data]


def _get_step_one_train_start_date(argparses, _data, _type):
    if _type in TYPE_STEP_ONE_TRAIN_START_DATES.keys():
        return TYPE_STEP_ONE_TRAIN_START_DATES[_type]
    return DATA_STEP_ONE_TRAIN_START_DATES[_data]


def get_train_start_date(argparses):
    return _get_train_start_date(argparses, argparses.data, argparses.type)


def get_step_one_train_start_date(argparses):
    return _get_step_one_train_start_date(argparses, argparses.data, argparses.type)


def get_validate_start_date(argparses):
    return _get_train_start_date(argparses, argparses.data, argparses.type)


def init_argparses_by_date(args, today, date_from, date_to, total_days):
    argparses = copy.deepcopy(args)
    train_start_date = get_train_start_date(argparses)
    setattr(argparses, "train_start_date", train_start_date)
    train_end_date = today - relativedelta(months=roi_data.get_step(total_days) + 1)
    train_end_date = f"{train_end_date.strftime('%Y%m')}31"
    if train_end_date < train_start_date:
        train_end_date = train_start_date
    setattr(argparses, "train_end_date", train_end_date)
    setattr(argparses, "test_start_date", train_start_date)
    setattr(argparses, "test_end_date", train_end_date)
    validate_start_date = get_validate_start_date(argparses)
    setattr(argparses, "validate_start_date", max(date_from, validate_start_date))
    setattr(argparses, "validate_end_date", max(date_to, validate_start_date))
    return argparses


def init_argparses_by_date_daily(args, today, date_from, date_to, total_days):
    argparses = copy.deepcopy(args)
    train_start_date = get_train_start_date(argparses)
    setattr(argparses, "train_start_date", train_start_date)
    train_end_date = today - relativedelta(days=total_days + 1)
    train_end_date = get_datetime_str(train_end_date.date())
    train_end_date = max(train_end_date, train_start_date)
    ((train_start_date, train_end_date), (test_start_date, test_end_date)) = (
        roi_date_util.split_train_test_dates(train_start_date, train_end_date)
    )
    setattr(argparses, "train_start_date", train_start_date)
    setattr(argparses, "train_end_date", train_end_date)
    setattr(argparses, "test_start_date", test_start_date)
    setattr(argparses, "test_end_date", test_end_date)
    validate_start_date = get_validate_start_date(argparses)
    setattr(argparses, "validate_start_date", max(date_from, validate_start_date))
    setattr(argparses, "validate_end_date", max(date_to, validate_start_date))
    return argparses


def init_argparses_by_date_step_one(args, today, date_from, date_to, step_one_days):
    argparses = copy.deepcopy(args)
    setattr(argparses, "total_days", step_one_days)
    setattr(argparses, "extra_days", step_one_days)
    setattr(argparses, "break_even_days", step_one_days)
    train_start_date = get_step_one_train_start_date(argparses)
    setattr(argparses, "train_start_date", train_start_date)
    train_end_date = today - relativedelta(days=int(step_one_days + 1))
    train_end_date = get_datetime_str(train_end_date.date())
    train_end_date = max(train_end_date, train_start_date)
    ((train_start_date, train_end_date), (test_start_date, test_end_date)) = (
        roi_date_util.split_train_test_dates(train_start_date, train_end_date)
    )
    setattr(argparses, "train_start_date", train_start_date)
    setattr(argparses, "train_end_date", train_end_date)
    setattr(argparses, "test_start_date", test_start_date)
    setattr(argparses, "test_end_date", test_end_date)
    validate_start_date = get_step_one_train_start_date(argparses)
    setattr(argparses, "validate_start_date", max(date_from, validate_start_date))
    setattr(argparses, "validate_end_date", max(date_to, validate_start_date))
    return argparses


def get_file(argparses, pathname, keyword):
    path = os.path.dirname(os.path.abspath(__file__))
    if hasattr(argparses, "online"):
        filename = (
            f"{path}/csv/{pathname}/{pathname}_{keyword}_"
            f"{argparses.app}_{argparses.data}_{argparses.type}_"
            f"{argparses.media}_{argparses.country}_"
            f"{argparses.total_days}_{get_break_even_days(argparses)}_online.csv"
        )
    else:
        filename = (
            f"{path}/csv/{pathname}/{pathname}_{keyword}_"
            f"{argparses.app}_{argparses.data}_{argparses.type}_"
            f"{argparses.media}_{argparses.country}_"
            f"{argparses.total_days}_{get_break_even_days(argparses)}_"
            f"{argparses.validate_start_date}_{argparses.validate_end_date}.csv"
        )
    return filename.lower()


def get_monthly_files():
    return [
        roi_constant.ROI_STR_TRAIN,
        roi_constant.ROI_STR_TEST,
        roi_constant.ROI_STR_VALIDATE,
        roi_constant.ROI_STR_TRAIN_SUM,
        roi_constant.ROI_STR_TEST_SUM,
        roi_constant.ROI_STR_VALIDATE_SUM,
        roi_constant.ROI_STR_MODEL,
        "validate_train",
        "validate_roi_d00030",
        "validate_d00007_roi",
        roi_constant.ROI_STR_VALIDATE_ROI,
        ROI_STR_BREAK_EVEN,
        "share_ratio_train",
        "share_ratio_test",
        "share_ratio_validate",
        ROI_STR_ROI,
        "001_train",
        "001_test",
        "001_validate",
        "001_validate_train",
        "001_roi",
        "007_train",
        "007_test",
        "007_validate",
        "007_validate_train",
        "007_roi",
        "validate_w00004_total",
    ]


def get_daily_files():
    return np.concatenate(
        (
            get_monthly_files(),
            [
                "validate_sum_v0",
                "validate_result",
                "validate_result_v1",
                "validate_result_predict_roi_d00030",
                "validate_result_d00007_roi",
                "validate_sum_predict_roi_d00030",
                "validate_sum_d00007_roi",
                "share_ratio_train_sum",
                "share_ratio_test_sum",
                "share_ratio_validate_sum",
                ROI_STR_ROI_SUM,
                "roi_sum_v1",
                "001_train",
                "001_train_sum",
                "001_test",
                "001_test_sum",
                "001_validate",
                "001_validate_sum",
                "001_validate_train_sum",
                "001_roi_sum",
                "007_train",
                "007_train_sum",
                "007_test",
                "007_test_sum",
                "007_validate",
                "007_validate_sum",
                "007_validate_train_sum",
                "007_roi_sum",
                "028_test",
                "028_test_sum",
                "028_validate",
                "028_validate_sum",
                TW_ROI_STR_TRAIN,
                TW_ROI_STR_TEST,
                TW_ROI_STR_VALIDATE,
                TW_ROI_STR_TRAIN_SUM,
                TW_ROI_STR_TEST_SUM,
                TW_ROI_STR_VALIDATE_SUM,
                "extra_train",
                "extra_train_sum",
                "extra_test",
                "extra_test_sum",
                "extra_validate",
                "extra_validate_sum",
            ],
        )
    )


def save_file(df, file, is_append=True):
    df = df.reindex(sorted(df.columns), axis=1)
    p = Path(file).parent
    if not os.path.exists(p):
        os.makedirs(p)
    with lock:
        if is_append and os.path.exists(file):
            df.to_csv(file, mode="a", header=False, index=False)
        else:
            df.to_csv(file, index=False)


def clear_files(argparses, pathname, files):
    for key in files:
        if os.path.exists(get_file(argparses, pathname, key)):
            os.remove(get_file(argparses, pathname, key))


def get_df_from_file(argparses, key, pathname, result_prefix):
    df = pd.read_csv(get_file(argparses, pathname, result_prefix))
    df = df.astype({key: "string"})
    df = df.set_index(key, drop=False)
    df = df.sort_index(inplace=False)
    return df


def get_datetime(date_str):
    return datetime.strptime(date_str, "%Y%m%d")


def get_datetime_str(date_obj):
    return date_obj.strftime("%Y%m%d")


def get_datetime64(date_str):
    return np.datetime64(datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d"))


def get_online_months(today, argparses):
    first_date = datetime.strptime(get_validate_start_date(argparses), "%Y%m%d")
    start_date = today - relativedelta(years=1)
    start_date = max(start_date, first_date)
    end_date = today - relativedelta(months=2) + relativedelta(days=1)
    dates = pd.DatetimeIndex(
        np.arange(
            np.datetime64(start_date.strftime("%Y-%m-%d")),
            np.datetime64(end_date.strftime("%Y-%m-%d")),
        )
    )
    return dates.strftime("%Y%m").drop_duplicates(keep="first")


def get_online_months_between(start_date, end_date):
    dates = pd.DatetimeIndex(
        np.arange(
            np.datetime64(get_datetime64(start_date)),
            np.datetime64(get_datetime64(end_date)),
        )
    )
    return dates.strftime("%Y%m").drop_duplicates(keep="first")


def get_monday_of_week(date):
    # Calculate the weekday (Monday is 0 and Sunday is 6)
    weekday = date.weekday()
    # Calculate the most recent Monday
    return date - timedelta(days=weekday)


def get_sunday_of_week(date):
    # Calculate the weekday (Monday is 0 and Sunday is 6)
    weekday = date.weekday()
    # Calculate the upcoming Sunday
    sunday = date + timedelta(days=(6 - weekday))
    if sunday > date:
        sunday -= timedelta(days=7)
    return sunday


def get_first_day_of_month(date):
    # 使用 relativedelta 设置日期为该月的第一天
    return date + relativedelta(day=1)


def get_online_weeks(today, argparses):
    first_date = datetime.strptime(get_validate_start_date(argparses), "%Y%m%d")
    start_date = today - relativedelta(days=argparses.total_days)
    start_date = get_monday_of_week(start_date)
    start_date = max(start_date, first_date)
    end_date = today - relativedelta(days=4 * DAYS_OF_WEEK + 1)
    end_date = get_sunday_of_week(end_date)
    mondays = pd.DatetimeIndex(
        np.arange(
            np.datetime64(start_date.strftime("%Y-%m-%d")),
            np.datetime64(end_date.strftime("%Y-%m-%d")),
            np.timedelta64(DAYS_OF_WEEK, "D"),
        )
    )
    return mondays, start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def get_online_weeks_001(today, argparses):
    first_date = datetime.strptime(get_validate_start_date(argparses), "%Y%m%d")
    start_date = today - relativedelta(days=4 * DAYS_OF_WEEK + 1)
    start_date = get_monday_of_week(start_date)
    start_date = max(start_date, first_date)
    end_date = today - relativedelta(days=roi_constant.DAYS_OF_WEEK + 1)
    end_date = get_sunday_of_week(end_date)
    mondays = pd.DatetimeIndex(
        np.arange(
            np.datetime64(start_date.strftime("%Y-%m-%d")),
            np.datetime64(end_date.strftime("%Y-%m-%d")),
            np.timedelta64(DAYS_OF_WEEK, "D"),
        )
    )
    return mondays, start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def _add_sum(df, key, prefix, total_days):
    df[key] = 0
    step = roi_data.get_step(total_days)
    for index in np.arange(1, step + 1, 1):
        if f"{prefix}{roi_data.to_days_str(index * 30)}" in df.columns:
            df[key] += df[f"{prefix}{roi_data.to_days_str(index * 30)}"]
    return df


def add_real_sum(df, key, total_days):
    return _add_sum(df, key, "roi_d", total_days)


def add_predict_sum(df, key, total_days):
    return _add_sum(df, key, "predict_roi_d", total_days)


def add_predict_and_extra_sum(df, total_days, extra_days):
    df = add_predict_sum(df, "predict_sum", total_days)
    df["predict_revenue"] = df["costs"] * df["predict_sum"]
    df = add_predict_sum(df, "predict_extra_sum", extra_days)
    df["predict_extra_revenue"] = df["costs"] * df["predict_extra_sum"]
    return df


def _init_result(df, y_predict, total_days, feature_count):
    step = roi_data.get_step(total_days)
    y_validate_predict_sum = np.sum(y_predict.reshape(-1, step), axis=1)
    df = df.assign(predict_sum=y_validate_predict_sum)
    for index in np.arange(1, step + 1, 1):
        df[f"predict_roi_d{roi_data.to_days_str(index * 30)}"] = y_predict[
            index - 1 :: step
        ]
    for index in np.arange(1, feature_count + 1, 1):
        df[
            f"predict_roi_d{roi_data.to_days_str(roi_constant.DAYS_OF_MONTH * index)}"
        ] = df[f"roi_d{roi_data.to_days_str(roi_constant.DAYS_OF_MONTH * index)}"]
    for index in np.arange(1, step + 1, 1):
        df[f"predict_revenue_d{roi_data.to_days_str(index * 30)}"] = (
            df["costs"] * df[f"predict_roi_d{roi_data.to_days_str(index * 30)}"]
        )
    df = add_predict_sum(df, "predict_sum", total_days)
    df["predict_revenue"] = df["costs"] * df["predict_sum"]
    return df


def _init_validate_result(df, y_predict, total_days, extra_days, feature_count):
    extra_step = roi_data.get_step(extra_days)
    y_validate_predict_sum = np.sum(y_predict.reshape(-1, extra_step), axis=1)
    df = df.assign(predict_sum=y_validate_predict_sum)
    for index in np.arange(1, extra_step + 1, 1):
        df[f"predict_roi_d{roi_data.to_days_str(index * 30)}"] = y_predict[
            index - 1 :: extra_step
        ]
    for index in np.arange(1, feature_count + 1, 1):
        df[
            f"predict_roi_d{roi_data.to_days_str(roi_constant.DAYS_OF_MONTH * index)}"
        ] = df[f"roi_d{roi_data.to_days_str(roi_constant.DAYS_OF_MONTH * index)}"]
    for index in np.arange(1, extra_step + 1, 1):
        df[f"predict_revenue_d{roi_data.to_days_str(index * 30)}"] = (
            df["costs"] * df[f"predict_roi_d{roi_data.to_days_str(index * 30)}"]
        )
    df = add_predict_and_extra_sum(df, total_days, extra_days)
    return df


def get_train_result(df, y, y_predict, total_days, feature_count):
    df = _init_result(df, y_predict, total_days, feature_count)

    step = roi_data.get_step(total_days)
    y_sum = np.sum(y.reshape(-1, step), axis=1)
    df = df.assign(real_sum=y_sum)
    df["real_revenue"] = df["costs"] * df["real_sum"]
    df = df.reindex(sorted(df.columns), axis=1)
    df = df.sort_index(inplace=False)
    return df


def _init_result_between(df, y_predict, start_days, end_days, feature_count):
    step = get_step_between(start_days, end_days)
    y_validate_predict_sum = np.sum(y_predict.reshape(-1, step), axis=1)
    df = df.assign(predict_sum=y_validate_predict_sum)
    for index in np.arange(1, step + 1, 1):
        days_str = roi_data.to_days_str(index * 30 + start_days - 30)
        df[f"predict_roi_d{days_str}"] = y_predict[index - 1 :: step]
    for index in np.arange(1, feature_count + 1, 1):
        days_str = roi_data.to_days_str(index * 30 + start_days - 30)
        df[f"predict_roi_d{days_str}"] = df[f"roi_d{days_str}"]
    for index in np.arange(1, step + 1, 1):
        days_str = roi_data.to_days_str(index * 30 + start_days - 30)
        df[f"predict_revenue_d{days_str}"] = (
            df["costs"] * df[f"predict_roi_d{days_str}"]
        )
    df = add_predict_sum(df, "predict_sum", end_days)
    df["predict_revenue"] = df["costs"] * df["predict_sum"]
    return df


def get_train_result_between(df, y, y_predict, start_days, end_days, feature_count):
    df = _init_result_between(df, y_predict, start_days, end_days, feature_count)

    step = get_step_between(start_days, end_days)
    y_sum = np.sum(y.reshape(-1, step), axis=1)
    df = df.assign(real_sum=y_sum)
    df["real_revenue"] = df["costs"] * df["real_sum"]
    df = df.reindex(sorted(df.columns), axis=1)
    df = df.sort_index(inplace=False)
    return df


def get_validate_result(df, y_predict, total_days, extra_days, feature_count):
    df = _init_validate_result(df, y_predict, total_days, extra_days, feature_count)
    df = df.reindex(sorted(df.columns), axis=1)
    df = df.sort_index(inplace=False)
    return df


def _init_validate_result_between(
    df, y_predict, start_days, total_days, extra_days, feature_count
):
    extra_step = get_step_between(start_days, extra_days)
    y_validate_predict_sum = np.sum(y_predict.reshape(-1, extra_step), axis=1)
    df = df.assign(predict_sum=y_validate_predict_sum)
    for index in np.arange(1, extra_step + 1, 1):
        days_str = roi_data.to_days_str(index * 30 + start_days - 30)
        df[f"predict_roi_d{days_str}"] = y_predict[index - 1 :: extra_step]
    for index in np.arange(1, feature_count + 1, 1):
        days_str = roi_data.to_days_str(index * 30 + start_days - 30)
        df[f"predict_roi_d{days_str}"] = df[f"roi_d{days_str}"]
    for index in np.arange(1, extra_step + 1, 1):
        days_str = roi_data.to_days_str(index * 30 + start_days - 30)
        df[f"predict_revenue_d{days_str}"] = (
            df["costs"] * df[f"predict_roi_d{days_str}"]
        )
    df = add_predict_and_extra_sum(df, total_days, extra_days)
    return df


def get_validate_result_between(
    df, y_predict, start_days, total_days, extra_days, feature_count
):
    df = _init_validate_result_between(
        df, y_predict, start_days, total_days, extra_days, feature_count
    )
    df = df.reindex(sorted(df.columns), axis=1)
    df = df.sort_index(inplace=False)
    return df


def add_media_roi_columns(df):
    columns = df.columns[df.columns.str.startswith("media_revenue_")].to_numpy()
    columns = np.append(columns, "media_d00007_revenue")
    for column in columns:
        if column in df.columns:
            roi_column = column.replace("revenue", "roi")
            df[roi_column] = np.where(df["costs"] > 0, df[column] / df["costs"], 0)
    return df


def get_media_validate_result(df, y_predict, total_days, feature_count):
    df = _init_result(df, y_predict, total_days, feature_count)
    df = add_media_roi_columns(df)
    df = df.reindex(sorted(df.columns), axis=1)
    df = df.sort_index(inplace=False)
    return df


def _get_result_sum_dict(total_days):
    d = {"costs": "sum", "predict_revenue": "sum", "d00007_revenue": "sum"}
    for days in roi_data.get_days(total_days):
        days_str = roi_data.to_days_str(days)
        d[f"revenue_d{days_str}"] = "sum"
        d[f"predict_revenue_d{days_str}"] = "sum"
    return d


def _get_result_sum_dict_between(start_days, end_days):
    d = {"costs": "sum", "predict_revenue": "sum", "d00007_revenue": "sum"}
    for days in get_days_between(start_days - 30, end_days):
        days_str = roi_data.to_days_str(days)
        d[f"revenue_d{days_str}"] = "sum"
        d[f"predict_revenue_d{days_str}"] = "sum"
    return d


def _get_validate_result_sum_dict(total_days):
    d = _get_result_sum_dict(total_days)
    d["predict_extra_revenue"] = "sum"
    return d


def _get_validate_result_sum_dict_between(start_days, end_days):
    d = _get_result_sum_dict_between(start_days, end_days)
    d["predict_extra_revenue"] = "sum"
    return d


def _get_result_sum(file_or_df, d, total_days, extra_day=0):
    extra_day = extra_day if extra_day > 0 else total_days
    extra_step = roi_data.get_step(extra_day)
    if isinstance(file_or_df, str):
        df = pd.read_csv(file_or_df)
        df = df.astype({"dt": "string"})
        df = df.set_index("dt", drop=False)
    else:
        df = file_or_df.copy()
    if "media_d00007_revenue" in df.columns:
        d["media_d00007_revenue"] = "sum"
        for index in np.arange(1, extra_step + 1, 1):
            if f"media_revenue_d{roi_data.to_days_str(index * 30)}" in df.columns:
                d[f"media_revenue_d{roi_data.to_days_str(index * 30)}"] = "sum"
    if "organic_d00007_revenue" in df.columns:
        d["organic_d00007_revenue"] = "sum"
        for index in np.arange(1, extra_step + 1, 1):
            if f"organic_revenue_d{roi_data.to_days_str(index * 30)}" in df.columns:
                d[f"organic_revenue_d{roi_data.to_days_str(index * 30)}"] = "sum"
    df["monthly"] = df["dt"].str.slice(0, 6)
    df_sum = df.groupby(["monthly"]).agg(d).reset_index()
    df_sum = df_sum.astype({"monthly": "string"})
    df_sum = df_sum.set_index("monthly", drop=False)
    for index in np.arange(1, extra_step + 1, 1):
        if f"revenue_d{roi_data.to_days_str(index * 30)}" in df_sum.keys():
            df_sum[f"roi_d{roi_data.to_days_str(index * 30)}"] = (
                df_sum[f"revenue_d{roi_data.to_days_str(index * 30)}"] / df_sum["costs"]
            )
        if f"predict_revenue_d{roi_data.to_days_str(index * 30)}" in df_sum.keys():
            df_sum[f"predict_roi_d{roi_data.to_days_str(index * 30)}"] = (
                df_sum[f"predict_revenue_d{roi_data.to_days_str(index * 30)}"]
                / df_sum["costs"]
            )
    if "real_revenue" in df_sum.columns:
        df_sum["real_sum"] = df_sum["real_revenue"] / df_sum["costs"]
    df_sum["d00007_roi"] = df_sum["d00007_revenue"] / df_sum["costs"]
    df_sum = add_predict_and_extra_sum(df_sum, total_days, extra_day)
    df_sum = df_sum.reindex(sorted(df_sum.columns), axis=1)
    df_sum = df_sum.sort_index(inplace=False)
    return df_sum


def get_train_result_sum(file_or_df, total_days):
    d = _get_result_sum_dict(total_days)
    d["real_revenue"] = "sum"
    return _get_result_sum(file_or_df, d, total_days)


def get_train_result_sum_between(file_or_df, start_days, end_days):
    d = _get_result_sum_dict_between(start_days, end_days)
    d["real_revenue"] = "sum"
    return _get_result_sum(file_or_df, d, end_days)


def get_validate_result_sum(file_or_df, total_days, extra_days):
    d = _get_validate_result_sum_dict(extra_days)
    df = _get_result_sum(file_or_df, d, total_days, extra_days)
    df = add_media_roi_columns(df)
    return df


def get_validate_result_sum_between(file_or_df, start_days, total_days, extra_days):
    d = _get_validate_result_sum_dict_between(start_days, extra_days)
    df = _get_result_sum(file_or_df, d, total_days, extra_days)
    df = add_media_roi_columns(df)
    return df


def _get_attr(obj, name, default_value):
    return (
        getattr(obj, name, default_value)
        if hasattr(obj, name) and getattr(obj, name) is not None
        else default_value
    )


def get_break_even_days(argparses):
    return int(_get_attr(argparses, "break_even_days", argparses.total_days))


def get_extra_days(argparses):
    return int(_get_attr(argparses, "extra_days", argparses.total_days))


def get_online(argparses):
    return int(_get_attr(argparses, "online", 0))


def is_online(argparses):
    return get_online(argparses) > 0


def get_test(argparses):
    return int(_get_attr(argparses, "test", 0))


def is_test(argparses):
    return get_test(argparses) > 0


def _add_arguments(parser):
    parser.add_argument("-app", "--app", help="app", type=str, required=True)
    parser.add_argument("-data", "--data", help="data", type=str, required=True)
    parser.add_argument("-type", "--type", help="type", type=str, required=True)
    parser.add_argument("-media", "--media", help="media", type=str, required=True)
    parser.add_argument(
        "-country", "--country", help="country", type=str, required=True
    )
    parser.add_argument(
        "-total_days", "--total_days", help="total_days", type=int, required=True
    )
    parser.add_argument(
        "-extra_days", "--extra_days", help="extra_days", type=int, required=False
    )
    parser.add_argument(
        "-wx_days", "--wx_days", help="wx_days", type=int, required=False
    )
    parser.add_argument(
        "-break_even_days",
        "--break_even_days",
        help="break_even_days",
        type=int,
        required=False,
    )
    parser.add_argument(
        "-top_war_start_days",
        "--top_war_start_days",
        help="top_war_start_days",
        type=int,
        required=False,
    )
    parser.add_argument(
        "-top_war_end_days",
        "--top_war_end_days",
        help="top_war_end_days",
        type=int,
        required=False,
    )
    parser.add_argument(
        "-share_ratio_days",
        "--share_ratio_days",
        help="share_ratio_days",
        type=int,
        required=False,
    )
    parser.add_argument(
        "-features", "--features", help="features", type=int, required=False
    )
    parser.add_argument(
        "-features_007", "--features_007", help="features_007", type=int, required=False
    )
    parser.add_argument(
        "-weights", "--weights", help="weights", type=str, required=False
    )
    parser.add_argument(
        "-validate_total_days",
        "--validate_total_days",
        help="validate_total_days",
        type=int,
        required=False,
    )
    parser.add_argument(
        "-validate_start_date",
        "--validate_start_date",
        help="validate start date",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-validate_end_date",
        "--validate_end_date",
        help="validate end date",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-predict_start_date",
        "--predict_start_date",
        help="predict start date",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-predict_end_date",
        "--predict_end_date",
        help="predict end date",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-test",
        "--test",
        help="test",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-datas",
        "--datas",
        help="datas",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-types",
        "--types",
        help="types",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-window_size",
        "--window_size",
        help="window_size",
        type=int,
        required=False,
    )
    parser.add_argument(
        "-report",
        "--report",
        help="report",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-gpu",
        "--gpu",
        help="gpu",
        type=int,
        required=False,
    )
    return parser


def get_argparses():
    parser = argparse.ArgumentParser(description="roi predict")
    parser = _add_arguments(parser)
    parser.add_argument(
        "-train_start_date",
        "--train_start_date",
        help="train start date",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-train_end_date",
        "--train_end_date",
        help="train end date",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-test_start_date",
        "--test_start_date",
        help="test start date",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-test_end_date",
        "--test_end_date",
        help="test end date",
        type=str,
        required=True,
    )
    return parser.parse_args()


def get_online_argparses():
    parser = argparse.ArgumentParser(description="roi predict")
    parser = _add_arguments(parser)
    parser.add_argument(
        "-online", "--online", default=0, help="online", type=int, required=False
    )
    return parser.parse_args()


def get_dates_by_feature(dates, argparses, today, total_days):
    dates_by_feature = {}
    for predict_date in dates:
        start_date = predict_date.date()
        end_date = start_date + relativedelta(days=roi_constant.DAYS_DIFF_OF_MONTH)
        validate_end_date = get_datetime(argparses.validate_end_date).date()
        last_date = date(today.year, today.month, today.day) - relativedelta(
            days=roi_constant.DAYS_OF_MONTH
        )
        if end_date > validate_end_date or end_date >= last_date:
            continue

        days_diff = (
            last_date - end_date + relativedelta(days=roi_constant.DAYS_OF_MONTH)
        )
        feature_count = int(
            min(
                math.floor(days_diff.days / DAYS_OF_MONTH),
                math.floor(total_days / DAYS_OF_MONTH),
            )
        )
        if feature_count not in dates_by_feature:
            dates_by_feature[feature_count] = []
        dates_by_feature[feature_count].append(start_date)
    return dates_by_feature


def get_dates_by_feature_007(dates, argparses, today, total_days):
    dates_by_feature = {}
    for predict_date in dates:
        start_date = predict_date.date()
        end_date = start_date + relativedelta(days=roi_constant.DAYS_DIFF_OF_MONTH)
        validate_end_date = get_datetime(argparses.validate_end_date).date()
        last_date = today.date() - relativedelta(days=roi_constant.DAYS_OF_WEEK)
        if end_date > validate_end_date or end_date >= last_date:
            continue

        days_diff = last_date - end_date + relativedelta(days=DAYS_DIFF_OF_WEEK)
        feature_count = int(
            min(
                math.floor(days_diff.days / DAYS_OF_WEEK),
                math.floor(total_days / DAYS_OF_WEEK),
            )
        )
        if feature_count not in dates_by_feature:
            dates_by_feature[feature_count] = []
        dates_by_feature[feature_count].append(start_date)
    return dates_by_feature


def get_roi_report_days(argparses):
    return [90, 120, 150, 180]


LAST_WAR_ROI_REPORT_TOTAL_DAYS = {
    90: 90,
    120: 90,
    150: 90,
    180: 90,
}


TOP_HEROES_ROI_REPORT_TOTAL_DAYS = {
    90: 90,
    120: 90,
    150: 90,
    180: 90,
}


TOP_WAR_ROI_REPORT_TOTAL_DAYS = {
    90: 360,
    120: 360,
    150: 360,
    180: 360,
}


def get_roi_report_total_days(argparses, days):
    if is_last_war(argparses):
        return LAST_WAR_ROI_REPORT_TOTAL_DAYS[days]
    if is_top_heroes(argparses):
        return TOP_HEROES_ROI_REPORT_TOTAL_DAYS[days]
    if is_top_war(argparses):
        return TOP_WAR_ROI_REPORT_TOTAL_DAYS[days]
    return 90


LAST_WAR_ROI_REPORT_BREAK_EVEN_DAYS = {
    90: 90,
    120: 120,
    150: 150,
    180: 180,
}


TOP_HEROES_ROI_REPORT_BREAK_EVEN_DAYS = {
    90: 90,
    120: 120,
    150: 150,
    180: 180,
}


TOP_WAR_ROI_REPORT_BREAK_EVEN_DAYS = {
    90: 360,
    120: 450,
    150: 540,
    180: 630,
}


def get_roi_report_break_even_days(argparses, days):
    if is_last_war(argparses):
        return LAST_WAR_ROI_REPORT_BREAK_EVEN_DAYS[days]
    if is_top_heroes(argparses):
        return TOP_HEROES_ROI_REPORT_BREAK_EVEN_DAYS[days]
    if is_top_war(argparses):
        return TOP_WAR_ROI_REPORT_BREAK_EVEN_DAYS[days]
    return 90


def format_df(df, index):
    df.columns = df.columns.map(str)
    df = df.reindex(sorted(df.columns), axis=1)
    df = df.astype({index: "string"})
    df = df.set_index(index, drop=False)
    df = df.sort_index(inplace=False)
    return df


def is_organic(train_param):
    return train_param.organic == 1


def sleep(max_seconds=10):
    wait_time = random.uniform(0, max_seconds)
    print(f"Waiting for {wait_time:.2f} seconds")
    time.sleep(wait_time)
