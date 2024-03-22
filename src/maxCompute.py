from odps import ODPS
from odps import options
import sys
sys.path.append('/src')

from src.config import accessId,secretAccessKey,defaultProject,bjProject,endPoint,bjEndPoint

# ODPS.options.tunnel.use_instance_tunnel =True
options.sql.settings = {
    'odps.sql.timezone':'Africa/Accra',
    "odps.sql.submit.mode" : "script"
}
# 执行odps sql，返回pandas的dataFrame
def execSql(sql):
    o = ODPS(accessId, secretAccessKey, defaultProject,
            endpoint=endPoint)
    with o.execute_sql(sql).open_reader(tunnel=True) as reader:
        pd_df = reader.to_pandas()
        return pd_df

# 不读取内容
def execSql2(sql):
    o = ODPS(accessId, secretAccessKey, defaultProject,
            endpoint=endPoint)
    o.execute_sql(sql)
    return

def execSqlBj(sql):
    o = ODPS(accessId, secretAccessKey, bjProject,
            endpoint=bjEndPoint)
    with o.execute_sql(sql).open_reader(tunnel=True) as reader:
        pd_df = reader.to_pandas()
        return pd_df

from odps.models import Schema, Column, Partition
def createTableBjTmp():
    o = ODPS(accessId, secretAccessKey, bjProject,
            endpoint=bjEndPoint)
    columns = [
        Column(name='customer_user_id', type='string', comment='uid like 813957863587'),
    ]
    schema = Schema(columns=columns)
    table = o.create_table('tmp_uid_by_j', schema, if_not_exists=True)
    return table

def writeTableBjTmp(df):
    o = ODPS(accessId, secretAccessKey, bjProject,
        endpoint=bjEndPoint)
    t = o.get_table('tmp_uid_by_j')
    
    with t.open_writer(arrow=True) as writer:
        # batch = pa.RecordBatch.from_pandas(df)
        # writer.write(batch)
        # print(df)
        writer.write(df)

if __name__ == '__main__':
    createTableBjTmp()