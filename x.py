import csv
import sys
from copy import deepcopy
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
                self.tableDict[self.tableList[-1]] = [self.tableList[-1] + '.' + s for s in fields]
                self.tables[self.tableList[-1]] = []
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

    def getTables(self):
        for i in self.tableNames:
            with open('./files/' + i + '.csv') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    self.tables[i].append(row)

    def getAllAttributes(self):
        lst = []
        for i in self.tableNames:
            for j in self.tableDict[i]:
                lst.append(j)
        return lst
    
    def getAttributes(self, posA):
        self.attributes = []
        self.allAttributes = self.getAllAttributes()
        self.flagAttributes = []
        for i in self.identifiers[posA].split(','):
            if i.strip() == '*':
                self.attributes = deepcopy(self.allAttributes)
                break
            for j in self.tableNames:
                for idx, k in enumerate(self.tableDict[j]):
                    if i.strip() in k:
                        self.attributes.append(self.tableDict[j][idx])

        for i, j in enumerate(self.allAttributes):
            self.flagAttributes.append(True) if j in self.attributes else self.flagAttributes.append(False)


    def getTableNames(self, posT):
        self.tableNames = []
        for i in self.identifiers[posT].split(','):
            if i.strip() in self.tableNames:
                print("Table name cannot be repeated after FROM Clause !!")
                return
            if i.strip() in self.tableList:
                self.tableNames.append(i.strip())
            else:
                print("INVALID TABLE NAME")
                return
        if(len(self.tableNames)) < 1:
            print("No Table Names were given.")
            return

    def joinTables(self):
        self.finalTable = self.tables[self.tableNames[0]]
        for i in range(1,len(self.tableNames)):
            table1 = deepcopy(self.finalTable)
            table2 = self.tables[self.tableNames[i]]
            self.finalTable = []
            for j in table1:
                for k in table2:
                    self.finalTable.append(j + k)

    def selectAttributes(self):
        curTable = deepcopy(self.finalTable)
        self.finalTable = []
        for i in curTable:
            lst = []
            for j in range(len(self.flagAttributes)):
                if self.flagAttributes[j]:
                    lst.append(i[j])
            self.finalTable.append(lst)

    def distinctTable(self):
        pass

    def processQuery(self):
        if self.qType != 'SELECT' or len(self.identifiers) < 4 or len(self.identifiers) > 6:
            print("INVALID QUERY !!")
            return

        self.where = False
        self.distinct = True if self.identifiers[1] == 'distinct' else False
        posT = 4 if self.distinct else 3
        posF = 3 if self.distinct else 2
        posA = 2 if self.distinct else 1

        if 'where' in self.identifiers[-1]:
            self.where = True
        if self.identifiers[posF] != 'from':
            print("INVALID QUERY SYNTAX !")
            return
        
        self.getTableNames(posT)
        self.getTables()
        self.getAttributes(posA)

        # Joining all the tables
        self.joinTables()
        
        # Where Condition
        if self.where:
            pass

        # Selecting given attributes
        self.selectAttributes()

        # Distict Clause
        if self.distinct:
            self.distinctTable()
        
        # printing the table
        sep = ','
        print(sep.join(self.attributes))
        for i in self.finalTable:
            print(sep.join(i))


if len(sys.argv) > 1:
    Query(sys.argv[1:])
else:
    print("INVALID QUERY !!")
