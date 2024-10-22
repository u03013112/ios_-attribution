import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 将 /src 目录添加到系统路径，以便导入自定义模块
sys.path.append('/src')
from src.maxCompute import execSql

# 配置绘图风格
sns.set(style="whitegrid")
plt.rcParams.update({'figure.max_open_warning': 0})  # 取消图形过多的警告

def getHistoricalData():
    """
    使用 execSql 函数从数据库中获取历史数据，并返回一个 Pandas DataFrame。
    """
    sql = '''
    SELECT
        media,
        country,
        mape_week,
        install_day
    FROM
        lastwar_predict_revenue_day1_by_spend_verification
    WHERE
        day >= '20240101'
        AND install_day BETWEEN '20240101' AND '20241022'
        AND app = 'com.fun.lastwar.gp';
    '''
    try:
        print("Fetching data using execSql...")
        data = execSql(sql)
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)
        print(f"Data fetched: {len(data)} rows")
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()


def preprocess_data(df):
    """
    预处理数据，包括转换日期格式、添加周信息等。
    """
    if df.empty:
        return df

    # 检查必需的列是否存在
    required_columns = {'media', 'country', 'mape_week', 'install_day'}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        print(f"Missing required columns: {missing}")
        return pd.DataFrame()

    # 转换 'install_day' 为 datetime 类型
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d', errors='coerce')

    # 移除转换失败的行
    df = df.dropna(subset=['install_day'])

    # 添加 'week' 列，格式为 'YYYY-WW'（年份-周数）
    df['week'] = df['install_day'].dt.strftime('%Y-%U')

    # mape_week 大于1.0 改为1.0
    df.loc[df['mape_week'] > 1.0, 'mape_week'] = 1.0

    return df


def save_csv(dataframe, filename, output_dir):
    """
    保存 DataFrame 为 CSV 文件。
    """
    save_path = os.path.join(output_dir, filename)
    try:
        dataframe.to_csv(save_path, index=False)
        print(f"Saved CSV: {save_path}")
    except Exception as e:
        print(f"Error saving CSV {filename}: {e}")


def plot_all_all(df, output_dir):
    """
    绘制 ALL media + ALL country 的图表，并保存为 lw_all.png，同时保存数据为 lw_all.csv
    """
    # 过滤出 media = 'ALL' 和 country = 'ALL' 的数据
    df_all_all = df[(df['media'] == 'ALL') & (df['country'] == 'ALL')]

    if df_all_all.empty:
        print("No data for media='ALL' and country='ALL'. Skipping lw_all.png and lw_all.csv.")
        return

    # 按周分组并计算平均 mape_week
    df_all_all_weekly = df_all_all.groupby('week')['mape_week'].mean().reset_index()

    # 保存 CSV
    save_csv(df_all_all_weekly, 'lw_all.csv', output_dir)

    # 绘图
    plt.figure(figsize=(18, 6))
    sns.lineplot(data=df_all_all_weekly, x='week', y='mape_week', marker='o')
    plt.title('ALL Media + ALL Country - Average MAPE Week')
    plt.xlabel('Week')
    plt.ylabel('Average MAPE Week')
    plt.xticks(rotation=45)
    plt.tight_layout()
    save_path = os.path.join(output_dir, 'lw_all.png')
    plt.savefig(save_path)
    plt.close()
    print(f"Saved plot: {save_path}")


def plot_media_all(df, output_dir):
    """
    绘制 各种 media + ALL country 的图表，并保存为 lw_media.png，同时保存数据为 lw_media.csv
    """
    # 过滤出 country = 'ALL' 和 media != 'ALL' 的数据
    df_media_all = df[(df['country'] == 'ALL') & (df['media'] != 'ALL')]

    if df_media_all.empty:
        print("No data for country='ALL' and media!='ALL'. Skipping lw_media.png and lw_media.csv.")
        return

    # 按 week 和 media 分组并计算平均 mape_week
    df_media_all_weekly = df_media_all.groupby(['week', 'media'])['mape_week'].mean().reset_index()

    # 保存 CSV
    save_csv(df_media_all_weekly, 'lw_media.csv', output_dir)

    # 绘图
    plt.figure(figsize=(18, 7))
    sns.lineplot(data=df_media_all_weekly, x='week', y='mape_week', hue='media', marker='o')
    plt.title('Various Media + ALL Country - Average MAPE Week')
    plt.xlabel('Week')
    plt.ylabel('Average MAPE Week')
    plt.xticks(rotation=45)
    plt.legend(title='Media', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    save_path = os.path.join(output_dir, 'lw_media.png')
    plt.savefig(save_path)
    plt.close()
    print(f"Saved plot: {save_path}")


def plot_all_country(df, output_dir):
    """
    绘制 ALL media + 各种 country 的图表，并保存为 lw_country.png，同时保存数据为 lw_country.csv
    """
    # 过滤出 media = 'ALL' 和 country != 'ALL' 的数据
    df_all_country = df[(df['media'] == 'ALL') & (df['country'] != 'ALL')]

    if df_all_country.empty:
        print("No data for media='ALL' and country!='ALL'. Skipping lw_country.png and lw_country.csv.")
        return

    # 按 week 和 country 分组并计算平均 mape_week
    df_all_country_weekly = df_all_country.groupby(['week', 'country'])['mape_week'].mean().reset_index()

    # 保存 CSV
    save_csv(df_all_country_weekly, 'lw_country.csv', output_dir)

    # 绘图
    plt.figure(figsize=(18, 7))
    sns.lineplot(data=df_all_country_weekly, x='week', y='mape_week', hue='country', marker='o')
    plt.title('ALL Media + Various Country - Average MAPE Week')
    plt.xlabel('Week')
    plt.ylabel('Average MAPE Week')
    plt.xticks(rotation=45)
    plt.legend(title='Country', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    save_path = os.path.join(output_dir, 'lw_country.png')
    plt.savefig(save_path)
    plt.close()
    print(f"Saved plot: {save_path}")


def plot_media_country(df, output_dir):
    """
    绘制 各种 media + 各种 country 的图表，并为每个 media 生成一张图，展示该 media 下各国的 mape_week 变化。
    每张图保存为 lw_media_country_<MEDIA_NAME>.png，同时保存数据为 lw_media_country_<MEDIA_NAME>.csv
    """
    # 过滤出 media != 'ALL' 和 country != 'ALL' 的数据
    df_media_country = df[(df['media'] != 'ALL') & (df['country'] != 'ALL')]

    if df_media_country.empty:
        print("No data for various media and countries. Skipping plot_media_country.")
        return

    # 获取所有独特的 media
    media_list = df_media_country['media'].unique()

    print(f"Found {len(media_list)} unique media.")

    for media in media_list:
        df_current_media = df_media_country[df_media_country['media'] == media]

        if df_current_media.empty:
            print(f"No data for media='{media}'. Skipping.")
            continue

        # 按 week 和 country 分组并计算平均 mape_week
        df_current_media_weekly = df_current_media.groupby(['week', 'country'])['mape_week'].mean().reset_index()

        # 保存 CSV
        safe_media = "".join([c if c.isalnum() or c in (' ', '-', '_') else '_' for c in media])
        csv_filename = f'lw_media_country_{safe_media}.csv'
        save_csv(df_current_media_weekly, csv_filename, output_dir)

        # 绘图
        plt.figure(figsize=(14, 7))
        sns.lineplot(
            data=df_current_media_weekly,
            x='week',
            y='mape_week',
            hue='country',
            marker='o'
        )
        plt.title(f"{media} + Various Countries - Average MAPE Week")
        plt.xlabel('Week')
        plt.ylabel('Average MAPE Week')
        plt.xticks(rotation=45)
        plt.legend(title='Country', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()

        # 生成文件名，移除可能的特殊字符
        png_filename = f'lw_media_country_{safe_media}.png'
        save_path = os.path.join(output_dir, png_filename)
        plt.savefig(save_path)
        plt.close()
        print(f"Saved plot for media '{media}': {save_path}")


def main():
    # 检查保存图表和 CSV 的目录是否存在，不存在则创建
    output_dir = '/src/data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    # 数据获取
    df = getHistoricalData()

    if df.empty:
        print("No data fetched. Exiting script.")
        return

    # 数据预处理
    df = preprocess_data(df)

    if df.empty:
        print("No valid data after preprocessing. Exiting script.")
        return

    # 数据可视化
    plot_all_all(df, output_dir)
    plot_media_all(df, output_dir)
    plot_all_country(df, output_dir)
    plot_media_country(df, output_dir)

    print("All plots and CSVs have been generated and saved.")


if __name__ == "__main__":
    main()
