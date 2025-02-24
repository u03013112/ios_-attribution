import time
import warnings

import roi_arpu_cpu_algorithm
import roi_arpu_util
from sklearn import metrics

warnings.filterwarnings("ignore")


def evaluate_models(
    df_train,
    df_test,
    name_func,
    start,
    total_months,
    feature_count,
    argparses,
    prefix,
    file_name,
):
    start_time = time.perf_counter()
    datas = {}

    _, x_train, y_train = roi_arpu_util.get_x_and_y(
        df_train,
        name_func,
        total_months,
        start,
        total_months,
        feature_count,
    )
    _, x_test, y_test = roi_arpu_util.get_x_and_y(
        df_test,
        name_func,
        total_months,
        start,
        total_months,
        feature_count,
    )

    for name, _ in roi_arpu_cpu_algorithm.get_models(argparses, feature_count).items():
        model = roi_arpu_cpu_algorithm.get_model(name, x_train, y_train)
        y_pred, info = model(x_test)
        y_pred, y_test = roi_arpu_util.unflatten_y_pred(
            y_pred, y_test, start, total_months, feature_count
        )
        y_pred = y_pred.flatten()
        y_test = y_test.flatten()
        mape = metrics.mean_absolute_percentage_error(y_test, y_pred)
        r2 = metrics.r2_score(y_test, y_pred)

        datas[name] = {
            "model": name,
            "mape": mape,
            "r2": r2,
            "info": info,
            "type": "model_1",
            "func": name_func(1),
            "feature_count": feature_count,
        }

        roi_arpu_util.save_figure(
            x_test[0],
            y_test,
            y_pred,
            total_months,
            name,
            argparses,
            prefix,
            file_name,
        )

    roi_arpu_util.save_model_file(datas, argparses, prefix, file_name, True)

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print(
        f"Total execution time: {int(hours)} hours, {int(minutes)} minutes, {seconds:.2f} seconds"
    )


def predict(
    df_train,
    df_test,
    row,
    name_func,
    start,
    total_months,
    feature_count,
):
    _, x_train, y_train = roi_arpu_util.get_x_and_y(
        df_train,
        name_func,
        total_months,
        start,
        total_months,
        feature_count,
    )
    _, x_test, y_test = roi_arpu_util.get_x_and_y(
        df_test,
        name_func,
        total_months,
        start,
        total_months,
        feature_count,
    )
    model = roi_arpu_cpu_algorithm.get_model(row["model"], x_train, y_train)
    y_pred, _ = model(x_test)
    y_pred, y_test = roi_arpu_util.unflatten_y_pred(
        y_pred, y_test, start, total_months, feature_count
    )
    df_test = roi_arpu_util.add_y_pred(
        df_test,
        y_pred,
        start,
        total_months,
        name_func,
    )
    return df_test
