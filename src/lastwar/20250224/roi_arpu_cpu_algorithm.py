import copy
import warnings

import numpy as np
import pandas as pd
# import roi_util
from scipy.interpolate import UnivariateSpline
from scipy.optimize import curve_fit
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

warnings.filterwarnings("ignore")


def exponential_func(x, a, b, c):
    return a * np.exp(b * x[0]) + c


def exponential_and_linear_func(x, *args):
    x_count = len(x)
    params_count = len(args)
    result = args[0] * np.exp(args[1] * x[0])
    for index in range(1, x_count):
        result += args[index + 1] * x[index]
    return result + args[params_count - 1]


def negative_exponential_func(x, a, b, c):
    return a * np.exp(-b * x[0]) + c


def negative_exponential_and_linear_func(x, *args):
    x_count = len(x)
    params_count = len(args)
    result = args[0] * np.exp(-args[1] * x[0])
    for index in range(1, x_count):
        result += args[index + 1] * x[index]
    return result + args[params_count - 1]


def power_func(x, a, b, c):
    return a * np.power(x[0], b) + c


def power_and_linear_func(x, *args):
    x_count = len(x)
    params_count = len(args)
    result = args[0] * np.power(x[0], args[1])
    for index in range(1, x_count):
        result += args[index + 1] * x[index]
    return result + args[params_count - 1]


def negative_power_func(x, a, b, c):
    return a * np.power(x[0], -b) + c


def negative_power_and_linear_func(x, *args):
    x_count = len(x)
    params_count = len(args)
    result = args[0] * np.power(x[0], -args[1])
    for index in range(1, x_count):
        result += args[index + 1] * x[index]
    return result + args[params_count - 1]


def logistic_func(x, L, k, x0):
    return L / (1 + np.exp(-k * (x[0] - x0)))


def logistic_and_linear_func(x, L, k, x0, a, b):
    return L / (1 + np.exp(-k * (x[0] - x0))) + a * x[1] + b


def four_parameter_func(x, bottom, top, ec50, hill_slope):
    return bottom + (top - bottom) / (1 + (x[0] / ec50) ** hill_slope)


def four_parameter_linear_func(x, *args):
    x_count = len(x)
    params_count = len(args)
    bottom = args[0]
    top = args[1]
    ec50 = args[2]
    hill_slope = args[3]
    result = bottom + (top - bottom) / (1 + (x[0] / ec50) ** hill_slope)
    for index in range(1, x_count):
        result += args[3 + index] * x[index]
    return result + args[params_count - 1]


def five_parameter_func(x, bottom, top, ec50, hill_slope, asymmetry):
    return bottom + (top - bottom) / (1 + (x[0] / ec50) ** hill_slope) ** asymmetry


def five_parameter_linear_func(x, *args):
    x_count = len(x)
    params_count = len(args)
    bottom = args[0]
    top = args[1]
    ec50 = args[2]
    hill_slope = args[3]
    asymmetry = args[4]
    result = bottom + (top - bottom) / (1 + (x[0] / ec50) ** hill_slope) ** asymmetry
    for index in range(1, x_count):
        result += args[4 + index] * x[index]
    return result + args[params_count - 1]


MODEL_FUNCS = {
    "exponential": exponential_func,
    "exponential_and_linear": exponential_and_linear_func,
    "negative_exponential": negative_exponential_func,
    "negative_exponential_and_linear": negative_exponential_and_linear_func,
    "power": power_func,
    "power_and_linear": power_and_linear_func,
    "negative_power": negative_power_func,
    "negative_power_and_linear": negative_power_and_linear_func,
    "logistic": logistic_func,
    "logistic_and_linear": logistic_and_linear_func,
}


def build_curve_fit_model(name, x, y):
    func = MODEL_FUNCS[name]

    def model(x, y):
        p0 = [0.1] * (len(x) + 1)
        params, _ = curve_fit(func, x, y, maxfev=1000000, p0=p0)
        return lambda x: (func(x, *params), params)

    return model


def build_logarithmic_model(name, x, y):
    # y = a + b * np.log(x[0])
    def model(x, y):
        x_transformed = np.log(x[0]).reshape(-1, 1)
        print('x_transformed:')
        print(x_transformed)
        lr = LinearRegression()
        lr.fit(x_transformed, y)
        return lambda x: (lr.predict(np.log(x[0]).reshape(-1, 1)), lr)

    return model


def get_logarithmic_x(x):
    length = len(x)
    result = np.log(x[0])
    for i in range(1, length):
        result = result + (x[i],)
    return np.column_stack(result)


def build_logarithmic_and_linear_model(name, x, y):
    # y = a + b * np.log(x[0]) + c * x[1]
    def model(x, y):
        xs = get_logarithmic_x(x)
        lr = LinearRegression()
        lr.fit(xs, y)
        return lambda x: (
            lr.predict(get_logarithmic_x(x)),
            lr,
        )

    return model


def build_polynomial_model(name, x, y, degree=3):
    def model(x, y):
        poly = PolynomialFeatures(degree=degree)
        x_poly = poly.fit_transform(x[0].reshape(-1, 1))
        lr = LinearRegression()
        lr.fit(x_poly, y)
        return lambda x: (lr.predict(poly.transform(x[0].reshape(-1, 1))), lr)

    return model


def build_polynomial_and_linear_model(name, x, y, degree=3):
    def model(x, y):
        poly = PolynomialFeatures(degree=degree)
        x_poly = poly.fit_transform(np.column_stack((x[0], x[1])))
        lr = LinearRegression()
        lr.fit(x_poly, y)
        return lambda x: (lr.predict(poly.transform(np.column_stack((x[0], x[1])))), lr)

    return model


def build_spline_model(name, x, y, s=0):
    def model(x, y):
        sorted_indices = np.argsort(x[0])
        x_sorted = x[0][sorted_indices]
        y_sorted = y[sorted_indices]
        spline = UnivariateSpline(x_sorted, y_sorted, s=s)
        return lambda x: (spline(x[0]), spline)

    return model


def build_moving_average_model(name, x, y, window_size=3):
    def model(x, y):
        y_ma = pd.Series(y).rolling(window=window_size, min_periods=1).mean().values

        def predict(x):
            y_pred = y_ma[-window_size:]
            if len(y_pred) < window_size:
                y_pred = np.pad(
                    y_pred,
                    (window_size - len(y_pred), 0),
                    "constant",
                    constant_values=(y_ma[0],),
                )
            y_pred = np.tile(y_ma[-1], len(x[0]))
            return (y_pred, {"window_size": window_size, "y_ma": y_ma})

        return predict

    return model


def _build_four_parameter_model(func, initial_guess_func, bounds):
    def model(x, y):
        initial_guess = initial_guess_func(x, y)
        params, covariance = curve_fit(
            func, x, y, p0=initial_guess, bounds=bounds, maxfev=1000000
        )
        return lambda x: (func(x, *params), params)

    return model


def build_four_parameter_model(name, x, y):
    func = four_parameter_func

    def dynamic_initial_guess(x, y):
        return [
            min(y),  # bottom
            max(y),  # top
            np.median(x[0]),  # ec50
            1.0,  # hill_slope
        ]

    bounds = (
        [-np.inf, -np.inf, 0, 0],  # 下界
        [np.inf, np.inf, np.inf, np.inf],  # 上界
    )
    return _build_four_parameter_model(func, dynamic_initial_guess, bounds)


def build_four_parameter_and_linear_model(name, x, y):
    func = four_parameter_linear_func

    def dynamic_initial_guess(x, y):
        guess = [
            min(y),  # bottom
            max(y),  # top
            np.median(x[0]),  # ec50
            1.0,  # hill_slope
        ]
        guess.extend([0.0] * len(x))
        return guess

    lower = [-np.inf, -np.inf, 0, 0]
    lower.extend([0] * len(x))
    upper = [np.inf, np.inf, np.inf, np.inf]
    upper.extend([np.inf] * len(x))
    bounds = (
        lower,  # 下界
        upper,  # 上界
    )
    return _build_four_parameter_model(func, dynamic_initial_guess, bounds)


def build_five_parameter_model(name, x, y):
    func = five_parameter_func

    def dynamic_initial_guess(x, y):
        return [
            min(y),  # bottom
            max(y),  # top
            np.median(x[0]),  # ec50
            1.0,  # hill_slope
            0.0,  # asymmetry
            0.0,  # a
            0.0,  # b
        ]

    bounds = (
        [-np.inf, -np.inf, 0, 0, 0],  # 下界
        [np.inf, np.inf, np.inf, np.inf, np.inf],  # 上界
    )
    return _build_four_parameter_model(func, dynamic_initial_guess, bounds)


def build_five_parameter_and_linear_model(name, x, y):
    func = five_parameter_linear_func

    def dynamic_initial_guess(x, y):
        guess = [
            min(y),  # bottom
            max(y),  # top
            np.median(x[0]),  # ec50
            1.0,  # hill_slope
            0.0,  # asymmetry
        ]
        guess.extend([0.0] * len(x))
        return guess

    lower = [-np.inf, -np.inf, 0, 0, 0]
    lower.extend([0] * len(x))
    upper = [np.inf, np.inf, np.inf, np.inf, np.inf]
    upper.extend([np.inf] * len(x))
    bounds = (
        lower,  # 下界
        upper,  # 上界
    )
    return _build_four_parameter_model(func, dynamic_initial_guess, bounds)


def _init_models():
    models = {
        "logarithmic": build_logarithmic_model,
        # "logarithmic_and_linear": build_logarithmic_and_linear_model,
        # "spline": build_spline_model,
        "four_parameter": build_four_parameter_model,
        "four_parameter_and_linear": build_four_parameter_and_linear_model,
        # "five_parameter": build_five_parameter_model,
        "five_parameter_and_linear": build_five_parameter_and_linear_model,
    }
    # for dimension in range(1, 10):
    #     models[f"polynomial_{dimension}"] = build_polynomial_model(dimension)
    # for dimension in range(1, 10):
    #     models[f"polynomial_and_linear_{dimension}"] = (
    #         build_polynomial_and_linear_model(dimension)
    #     )
    # for window_size in [3, 5, 7, 9]:
    #     models[f"moving_average_{window_size}"] = build_moving_average_model(
    #         window_size
    #     )
    for name in [
        # "exponential",
        "exponential_and_linear",
        # "negative_exponential",
        "negative_exponential_and_linear",
        # "power",
        "power_and_linear",
        # "negative_power",
        "negative_power_and_linear",
        # "logistic",
        # "logistic_and_linear",
    ]:
        models[name] = build_curve_fit_model
    return models


MODELS = _init_models()


def get_models(argparses, feature_count):
    # if roi_util.is_last_war(argparses) and feature_count < 3:
    #     return MODELS
    # if roi_util.is_top_heroes(argparses) and feature_count < 3:
    #     return MODELS
    models = copy.deepcopy(MODELS)
    models.pop("four_parameter_and_linear")
    models.pop("five_parameter_and_linear")

    return models


def get_model(name, x, y):
    model_builder = MODELS[name]
    model_func = model_builder(name, x, y)
    model = model_func(x, y)
    return model
