from datetime import datetime, timedelta

dayStr = '20240209'
days = '30'
days = int(days)
uploadDateStartStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=(days - 14))).strftime('%Y%m%d')
dayBeforeStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')

print('day:',dayStr)
print('days:',days)
print('uploadDateStart:',uploadDateStartStr)
print('dayBefore:',dayBeforeStr)
