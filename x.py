import csv
import sys
from copy import deepcopy
import sqlparse

class Query():
    def __init__(self, query):
        self.agg = ['max', 'min', 'avg', 'sum']
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
                    row = [int(x) for x in row]
                    self.tables[i].append(row)

    def getAllAttributes(self):
        lst = []
        for i in self.tableNames:
            for j in self.tableDict[i]:
                lst.append(j)
        return lst

    # def checkAggregate(self):
        # at = ['max', 'min', 'avg', 'sum']
        

    def getAttributes(self, posA):
        self.attributes = []
        self.attributesAgg = []
        self.aggregateList = []
        self.aggregate = False
        self.allAttributes = self.getAllAttributes()
        self.flagAttributes = []
        for i in self.identifiers[posA].split(','):
            if i.strip() == '*':
                self.attributes = deepcopy(self.allAttributes)
                break
            for j in self.tableNames:
                for idx, k in enumerate(self.tableDict[j]):
                    if len(self.attributes) > 0 and i.strip() in self.attributes[-1]:
                        continue
                    if '(' and ')' in i.strip():
                        self.aggregate = True
                        agg = (i.strip()).split('(')[0]
                        att = (i.strip()).split('(')[1]
                        att = (att).split(')')[0]
                        if att in k:
                            self.attributesAgg.append(self.tableDict[j][idx])
                            self.attributes.append(i.strip())
                        if agg.lower() in self.agg:
                            self.aggregateList.append(agg.lower())
                        else:
                            print('Invalid Aggregate Function')
                            exit(0)
                    elif '(' in i.strip() or ')' in i.strip():
                        print("Invalid Attribute")
                        exit(0)
                    if i.strip() in k and not self.aggregate:
                        self.attributes.append(self.tableDict[j][idx])
                    elif i.strip() in k and self.aggregate:
                        print("Normal column cannot be given along with aggregate function.")
                        exit(0)
            # print(self.attributes)
            fl = False
            for j in self.attributes:
                if i.strip() in j:
                    fl = True
            if not fl:
                print("Invalid Attribute ", i.strip())
                exit(0)

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

    def selectAggregates(self):
        curTable = deepcopy(self.finalTable)
        self.finalTable = []
        final = []
        print(self.attributesAgg)
        print(self.aggregateList)
        for i, att in enumerate(self.attributesAgg):
            idx = self.allAttributes.index(att)
            lst = [tab[idx] for tab in curTable]
            print(lst)
            print(self.aggregateList[i])
            if self.aggregateList[i] == 'max':
                final.append(max(lst))
            elif self.aggregateList[i] == 'min':
                final.append(min(lst))
            elif self.aggregateList[i] == 'sum':
                final.append(sum(lst))
            elif self.aggregateList[i] == 'avg':
                final.append(sum(lst)/len(lst))
            print(final)
        self.finalTable.append(final)

    def selectAttributes(self):
        if self.aggregate:
            self.selectAggregates()
            return
        curTable = deepcopy(self.finalTable)
        self.finalTable = []
        for i in curTable:
            lst = []
            for j in range(len(self.flagAttributes)):
                if self.flagAttributes[j]:
                    lst.append(i[j])
            self.finalTable.append(lst)

    def distinctTable(self):
        lst = deepcopy(self.finalTable)
        self.finalTable = []
        for i in lst:
            if i not in self.finalTable:
                self.finalTable.append(i)

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
            print(sep.join(map(str, i)))


if len(sys.argv) > 1:
    Query(sys.argv[1:])
else:
    print("INVALID QUERY !!")
