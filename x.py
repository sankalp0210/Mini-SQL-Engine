import csv
import sys
import sqlparse

class Query():
    def __init__(self, query):
        self.tableList = []
        self.tableDict = {}
        self.tables = {}
        self.readMetaData()
        self.identifiers = []
        self.parseQuery(query)
        self.convertToLower()
        self.processQuery()

    def readMetaData(self):
        f = open('./files/metadata.txt', 'r')
        fields = []
        flag = 0
        for line in f:
            if '<begin_table>' in line:
                flag += 1
            elif '<end_table>' in line:
                self.tableDict[self.tableList[-1]] = fields
                self.tables[self.tableList[-1]] = {}
                for j in fields:
                    self.tables[self.tableList[-1]][j] = []
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
    
    def convertToLower(self):
        lst = ['select', 'distinct', 'from']
        for i in range(len(self.identifiers)):
            if self.identifiers[i].lower() in lst:
                self.identifiers[i] = self.identifiers[i].lower()

    def getTables(self, tableNames):
        for i in tableNames:
            with open('./files/' + i + '.csv') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    for ind, j in enumerate(self.tableDict[i]):
                        self.tables[i][j].append(row[ind])

    def processQuery(self):
        if self.qType != 'SELECT' or len(self.identifiers) < 4 or len(self.identifiers) > 6:
            print("INVALID QUERY !!")
            return

        pos = 4 if self.identifiers[1] == 'distinct' else 3
        self.distinct = True if self.identifiers[1] == 'distinct' else False

        tableNames = []
        for i in self.identifiers[pos].split(','):
            if i.strip() in self.tableList:
                tableNames.append(i.strip())
            else:
                print("INVALID TABLE NAME")
                return
        self.getTables(tableNames)
        
        if len(self.identifiers) == 4:
            print("")

if len(sys.argv) > 1:
    Query(sys.argv[1:])
else:
    print("INVALID QUERY !!")
