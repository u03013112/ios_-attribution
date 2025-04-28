import os
import sys
import pandas as pd
import datetime
import cv2

sys.path.append('/src')
from src.maxCompute import execSql

def getDataFromMaxCompute(installDayStartStr, installDayEndStr, earliestDayStartStr, earliestDayEndStr):
    mediasourceList = [
        # 'Applovin',
        # 'Facebook',
        'Google',
        # 'Mintegral',
        # 'Moloco',
        # 'Snapchat',
        # 'Twitter',
        # 'tiktok',
    ]
    sql = f'''
select
    material_name,
    video_url,
    sum(cost_value_usd) as cost
from rg_bi.dws_material_overseas_data_public
where
    app = '502'
    and install_day between {installDayStartStr} and {installDayEndStr}
    and earliest_day between {earliestDayStartStr} and {earliestDayEndStr}
    and mediasource in ({','.join(f"'{source}'" for source in mediasourceList)})
    and country in ('US')
group by
    material_name,
    video_url
order by
    cost desc
;
    '''
    print(sql)
    data = execSql(sql)
    return data


# 从数据库获取数据
def trainDataPrepare1():
    df = pd.DataFrame()

    startDate = datetime.datetime(2025, 1, 6)
    endDate = datetime.datetime(2025, 4, 7)
    mondayList = []

    # 计算每周一的日期
    currentDate = startDate
    while currentDate <= endDate:
        if currentDate.weekday() == 0:  # 0表示周一
            mondayList.append(currentDate.strftime('%Y%m%d'))
        currentDate += datetime.timedelta(days=1)

    for monday in mondayList:
        sunday = (datetime.datetime.strptime(monday, '%Y%m%d') + datetime.timedelta(days=6)).strftime('%Y%m%d')
        lastMonday = (datetime.datetime.strptime(monday, '%Y%m%d') - datetime.timedelta(days=7)).strftime('%Y%m%d')
        lastSunday = (datetime.datetime.strptime(monday, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
        # print('当前周一：', monday)
        # print('当前周日：', sunday)
        # print('上周一：', lastMonday)
        # print('上周日：', lastSunday)

        weekDf = getDataFromMaxCompute(monday, sunday, lastMonday, lastSunday)
        print(weekDf)

        df = pd.concat([df, weekDf], axis=0)

    # 重置索引
    df.reset_index(drop=True, inplace=True)

    return df

# 下载视频并制作帧
def trainDataPrepare2(df):
    trainDf = pd.DataFrame()

    for i in range(len(df)):
        index = i + 1
        filename = f'{index}.mp4'
        name = df.iloc[i]['material_name']
        cost = df.iloc[i]['cost']
        video_url = df.iloc[i]['video_url']

        # 下载视频到本地 videos目录下
        video_dir = 'videos'
        if not os.path.exists(video_dir):
            os.makedirs(video_dir)

        video_path = os.path.join(video_dir, filename)
        video_frames_dir = os.path.join(video_dir, f'{index}_frames')
        # if not os.path.exists(video_path):
        # 改为如果帧目录不存在，则下载视频，重新制作帧
        if not os.path.exists(video_frames_dir):
            # 下载视频
            os.system(f'curl -o {video_path} {video_url}')
            os.makedirs(video_frames_dir)
            cap = cv2.VideoCapture(video_path)
            fps = cap.get(cv2.CAP_PROP_FPS)
            fps = int(fps)
            print('fps:',fps)
            frame_count = 0
            frame_name_count = 1
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                if frame_count % fps == 0:
                    frame_filename = os.path.join(video_frames_dir, f'frame_{frame_name_count}.jpg')
                    frame_name_count += 1
                    cv2.imwrite(frame_filename, frame,[cv2.IMWRITE_JPEG_QUALITY, 60])
                    # 最多保存30帧
                    if frame_name_count > 30:
                        break

                frame_count += 1
            cap.release()
            # 删除视频文件
            os.remove(video_path)

        # 创建一个新的 DataFrame行
        new_row = pd.DataFrame([{
            'filename': filename,
            'frames_dir': video_frames_dir,
            'video_url': video_url,
            'name': name,
            'cost': cost,
        }])

        # 使用 pd.concat() 将新行添加到现有 DataFrame
        trainDf = pd.concat([trainDf, new_row], ignore_index=True)

    return trainDf


def trainDataPrepare():
    trainDataFilename1 = '/src/data/videosTag_train1.csv'
    if os.path.exists(trainDataFilename1):
        trainDf1 = pd.read_csv(trainDataFilename1)
    else:
        trainDf1 = trainDataPrepare1()
        trainDf1.to_csv(trainDataFilename1, index=False)

    print('trainDf1:')
    print(trainDf1)

    # trainDataFilename2 = '/src/data/videosTag_train2.csv'
    # if os.path.exists(trainDataFilename2):
    #     trainDf2 = pd.read_csv(trainDataFilename2)
    # else:
    #     trainDf2 = trainDataPrepare2(trainDf1)
    #     trainDf2.to_csv(trainDataFilename2, index=False)
    # print('trainDf2:')

    # return trainDf2



if __name__ == '__main__':
    trainDataPrepare()
    


