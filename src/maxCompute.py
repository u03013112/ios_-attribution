from odps import ODPS
import sys
sys.path.append('/src')

from src.config import accessId,secretAccessKey,defaultProject,endPoint

# 执行odps sql，返回pandas的dataFrame
def execSql(sql):
    o = ODPS(accessId, secretAccessKey, defaultProject,
            endpoint=endPoint)
    with o.execute_sql(sql).open_reader() as reader:
        pd_df = reader.to_pandas()
        return pd_df
