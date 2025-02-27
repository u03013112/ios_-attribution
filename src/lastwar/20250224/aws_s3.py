import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

import sys
sys.path.append('/src')

from src.config import aws_access_key_id, aws_secret_access_key

class S3Manager:
    def __init__(self, region_name='us-east-1'):
        self.s3_client = boto3.client(
            's3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

    def check_directory(self, bucket_name, directory_name):
        """
        检查 S3 中的目录是否存在
        :param bucket_name: S3 桶名称
        :param directory_name: 目录名称（以斜杠结尾）
        :return: 如果目录存在返回 True，否则返回 False
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_name)
            return 'Contents' in response
        except Exception as e:
            print(f"检查目录时出错：{e}")
            return False

    def create_directory(self, bucket_name, directory_name):
        """
        创建 S3 目录（通过上传一个空对象模拟）
        :param bucket_name: S3 桶名称
        :param directory_name: 目录名称（以斜杠结尾）
        """
        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=directory_name)
            print(f"目录 {directory_name} 已创建")
        except Exception as e:
            print(f"创建目录失败：{e}")

    def upload_file_to_s3(self, file_name, bucket_name, directory_name, object_name=None):
        """
        将文件上传到指定的 S3 目录
        :param file_name: 本地文件路径
        :param bucket_name: S3 桶名称
        :param directory_name: 目录名称（以斜杠结尾）
        :param object_name: 在 S3 中存储的文件名（可选）
        """
        if object_name is None:
            object_name = file_name.split('/')[-1]
        s3_key = f"{directory_name}{object_name}"

        try:
            self.s3_client.upload_file(file_name, bucket_name, s3_key)
            print(f"文件 {file_name} 已成功上传到 {bucket_name}/{s3_key}")
        except FileNotFoundError:
            print(f"错误：未找到文件 {file_name}")
        except NoCredentialsError:
            print("错误：AWS 凭证缺失")
        except PartialCredentialsError:
            print("错误：AWS 凭证不完整")
        except Exception as e:
            print(f"上传文件失败：{e}")

# 示例使用
if __name__ == "__main__":
    bucket_name = "lastwardata"
    directory_name = "datascience/szj/lastwarPredictServer3To36SumRevenue20250227/"
    file_name = "/src/data/lastwarPredictRevenue3_36_sum_2025-02-26_14.csv"

    s3_manager = S3Manager()

    # 检查目录是否存在，如果不存在则创建
    if not s3_manager.check_directory(bucket_name, directory_name):
        print(f"目录 {directory_name} 不存在，正在创建...")
        s3_manager.create_directory(bucket_name, directory_name)
    else:
        print(f"目录 {directory_name} 已存在")

    # 上传文件到指定目录
    s3_manager.upload_file_to_s3(file_name, bucket_name, directory_name)
