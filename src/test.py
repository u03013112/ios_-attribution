from odps import ODPS
from config import accessId,secretAccessKey,defaultProject,endPoint
print(accessId,secretAccessKey,defaultProject,endPoint)
o = ODPS(accessId, secretAccessKey, defaultProject,
            endpoint=endPoint)