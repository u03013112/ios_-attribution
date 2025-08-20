from databricks import sql
import pandas as pd
import sys
sys.path.append('/src')

from src.config import databricksServer_hostname, databricksHttp_path, databricksAccess_token

# 针对select查询，返回一个DataFrame
def execSql(sql_query):
	connection = sql.connect(
		server_hostname = databricksServer_hostname,
		http_path = databricksHttp_path,
		access_token = databricksAccess_token
	)
	cursor = connection.cursor()
	use_database_query = "USE data_science.default;"
	cursor.execute(use_database_query)
	cursor.close()  # 关闭 cursor，释放资源

	df = pd.read_sql(sql_query, connection)
	connection.close()

	return df

# 针对类似建立视图或者插入数据的操作，返回None
def execSql2(sql_query):
	connection = sql.connect(
		server_hostname = databricksServer_hostname,
		http_path = databricksHttp_path,
		access_token = databricksAccess_token
	)
	cursor = connection.cursor()

	use_database_query = "USE data_science.default;"
	cursor.execute(use_database_query)

	cursor.execute(sql_query)
	connection.commit()
	cursor.close()
	connection.close()

	return None

if __name__ == "__main__":
	# Example usage
	sql_query = "SELECT * FROM `marketing`.`attribution`.`dwd_overseas_revenue_allproject` LIMIT 10"
	result_df = execSql(sql_query)
	print(result_df)