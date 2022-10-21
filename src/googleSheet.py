import gspread

class GSheet:
    def __init__(self):
        pass
    
    def clearSheet(self,filename,sheetName):
        gc = gspread.service_account()
        sheet = gc.open(filename).worksheet(sheetName)
        sheet.clear()
    
    def updateSheet(self,filename,sheetName,rangeName,data):
        gc = gspread.service_account()
        sheet = gc.open(filename).worksheet(sheetName)
        sheet.update(rangeName,data)

    def getAllValuesFromSheet(self,filename,sheetName):
        gc = gspread.service_account()
        sheet = gc.open(filename).worksheet(sheetName)
        list_of_lists = sheet.get_values()
        return 
    def addWorksheet(self,filename,sheetName,rows="1000", cols="20"):
        gc = gspread.service_account()
        sh = gc.open(filename)
        worksheet = sh.add_worksheet(title=sheetName, rows=rows, cols=cols)

if __name__ == '__main__':
    # GSheet().updateSheet('Where is the money Lebowski?','Sheet1','A1',[[1,2,3],[4,5,6]])
    # print(GSheet().getAllValuesFromSheet('auto video test','Data'))
    gc = gspread.service_account()
    sh = gc.open('ios predict')
    for i in range(64):
        sh.add_worksheet(title='%d'%(i), rows="1000", cols="20")