import gspread

# 将log文件定期的同步到google sheet

filename = 'ios predict'
print(filename)
gc = gspread.service_account()
sh = gc.open(filename)

# 
for i in range(64):
#     sh.add_worksheet(title='%d'%(i), rows="1000", cols="20")
    sheet = sh.worksheet('%d'%(i))
    logFilename = '/src/src/predSkan/log/log%d.log'%(i)
    lines = [['mod','mape','val_mape']]
    for line in open(logFilename,'r'): 
        print (line)
        l = line.split(' ')
        lines.append(l)
    
    sheet.clear()
    sheet.update('A1',lines)