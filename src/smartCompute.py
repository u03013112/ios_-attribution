from odps import ODPS
from config import accessId,secretAccessKey,defaultProject,endPoint
import pandas as pd
class SmartCompute:
    # 执行odps sql，返回pandas的dataFrame
    def execSql(self,sql):
        o = ODPS(accessId, secretAccessKey, defaultProject,
                endpoint=endPoint)
        
        with o.execute_sql(sql).open_reader() as reader:
            pd_df = reader.to_pandas()
            return pd_df

    # 为了测试的时候方便，可以先把sql获得的数据先存起来
    def writeCsv(self,pd_df,csvFilename):
        pd_df.to_csv(csvFilename)
    def readCsv(self,csvFilename):
        pd_df = pd.read_csv(csvFilename)
        return pd_df
    
    def getO(self):
        o = ODPS(accessId, secretAccessKey, defaultProject,
                endpoint=endPoint)
        return o