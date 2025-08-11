from databricks import sql
import pandas as pd
import sys
sys.path.append('/src')

from src.config import databricksServer_hostname, databricksHttp_path, databricksAccess_token

def execSql(sql_query):
	connection = sql.connect(
		server_hostname = databricksServer_hostname,
		http_path = databricksHttp_path,
		access_token = databricksAccess_token
	)
	df = pd.read_sql(sql_query, connection)
	connection.close()

	return df


if __name__ == "__main__":
	# Example usage
	sql_query = "SELECT * FROM `marketing`.`attribution`.`dwd_overseas_revenue_allproject` LIMIT 10"
	result_df = execSql(sql_query)
	print(result_df)