# -*- coding: utf-8 -*-
# This file is auto-generated, don't edit it. Thanks.
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from config import AccessKeyId,AccessKeySecret


from typing import List

from alibabacloud_videoenhan20200320.client import Client as videoenhan20200320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_videoenhan20200320 import models as videoenhan_20200320_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient


class Sample:
    def __init__(self):
        pass

    @staticmethod
    def create_client() -> videoenhan20200320Client:
        config = open_api_models.Config(
            access_key_id=AccessKeyId,
            access_key_secret=AccessKeySecret
        )
        config.endpoint = f'videoenhan.cn-shanghai.aliyuncs.com'
        return videoenhan20200320Client(config)

    @staticmethod
    def main(
        args: List[str],
    ) -> None:
        client = Sample.create_client()
        merge_infos_0 = videoenhan_20200320_models.MergeVideoModelFaceRequestMergeInfos(
            template_face_id='f75af9de-d425-4ac7-93a1-73fe8ec77689-V1_0',
            image_url='https://rivergame-aigc-test.oss-cn-shanghai.aliyuncs.com/%E7%99%BD%E5%A5%B3.png'
        )
        merge_video_model_face_request = videoenhan_20200320_models.MergeVideoModelFaceRequest(
            template_id='f75af9de-d425-4ac7-93a1-73fe8ec77689-V1',
            merge_infos=[
                merge_infos_0
            ]
        )
        runtime = util_models.RuntimeOptions()
        try:
            # 复制代码运行请自行打印 API 的返回值
            ret = client.merge_video_model_face_with_options(merge_video_model_face_request, runtime)
            print(ret)
            # retEx = {'headers': {'date': 'Wed, 10 Apr 2024 10:38:16 GMT', 'content-type': 'application/json;charset=utf-8', 'content-length': '231', 'connection': 'keep-alive', 'keep-alive': 'timeout=25', 'access-control-allow-origin': '*', 'access-control-expose-headers': '*', 'x-acs-request-id': '564D1085-5813-54BC-B021-EAC89F27FEC5', 'x-acs-trace-id': '44523e0ec13721c293b8bee75deec60e', 'etag': '11hxNaiuEUpe6LTMocxjU8g1'}, 'statusCode': 200, 'body': {'Message': '该调用为异步调用，任务已提交成功，请以requestId的值作为jobId参数调用同类目下GetAsyncJobResult接口查询任务执行状态和结果。', 'RequestId': '564D1085-5813-54BC-B021-EAC89F27FEC5'}}

            if ret['statusCode'] == 200:
                requestId = ret['body']['RequestId']
                print('成功，request id：',requestId)
                return requestId

        except Exception as error:
            # 此处仅做打印展示，请谨慎对待异常处理，在工程项目中切勿直接忽略异常。
            # 错误 message
            print(error.message)
            # 诊断地址
            print(error.data.get("Recommend"))
            UtilClient.assert_as_string(error.message)

        return ''


if __name__ == '__main__':
    Sample.main(sys.argv[1:])