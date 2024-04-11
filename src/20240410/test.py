import oss2
from oss2.credentials import EnvironmentVariableCredentialsProvider,Credentials

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from config import AccessKeyId,AccessKeySecret

class JCredentialsProvider():
    def get_credentials(self):
        return Credentials(AccessKeyId, AccessKeySecret,'')

auth = oss2.ProviderAuth(EnvironmentVariableCredentialsProvider())
    
auth = oss2.ProviderAuth(JCredentialsProvider())

# yourEndpoint填写Bucket所在地域对应的Endpoint。以华东1（杭州）为例，Endpoint填写为https://oss-cn-hangzhou.aliyuncs.com。
# 填写Bucket名称。
bucket = oss2.Bucket(auth, 'https://oss-cn-shanghai.aliyuncs.com', 'rivergame-aigc-test')

bucket.put_object_from_file('jap1.csv', 'jap1.csv')

from itertools import islice
# 列举Bucket下的10个文件。
for b in islice(oss2.ObjectIterator(bucket), 10):
    print(b.key)


# bucket.delete_object('jap1.csv')
