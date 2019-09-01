import csv
import sys
import sqlparse

class Query():
    def __init__(self, query):
        self.tableList = []
        self.tableDict = {}
        self.readMetaData()
        self.identifiers = []
        self.parseQuery(query)

    def readMetaData(self):
        f = open('./files/metadata.txt', 'r')
        sw = '<begin_table>'
        ew = '<end_table>'
        fields = []
        flag = 0
        for line in f:
            if sw in line:
                flag += 1
            elif ew in line:
                self.tableDict[self.tableList[-1]] = fields
                fields = []
                flag = 0
            elif flag == 1:
                self.tableList.append(line.strip())
                flag += 1
            else:
                fields.append(line.strip())
            
    def parseQuery(self, query):
        sep = ' '
        self.query = sqlparse.parse(sep.join(query))[0].tokens
        self.qType = sqlparse.sql.Statement(self.query).get_type()
        l = sqlparse.sql.IdentifierList(self.query).get_identifiers()
        self.identifiers = [str(i) for i in l]
        print(self.query)
        print(self.qType)
        print(l)
        print(self.identifiers)

if len(sys.argv) > 1:
    q = Query(sys.argv[1:])
else:
    print("Invalid Query")
