import csv
import sys
from copy import deepcopy
import sqlparse

class Query():
    def __init__(self, query):
        self.agg = ['max', 'min', 'avg', 'sum']
        self.operators = ['=', '>', '<', '>=', '<=']
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
        self.tokenNames = [type(token).__name__ for token in self.query]
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

    def getAttributes(self, posA):
        self.attributes = []
        self.attributesAgg = []
        self.aggregateList = []
        self.aggregate = False
        self.allAttributes = self.getAllAttributes()
        self.flagAttributes = []
        flagField = []
        for bt, i in enumerate(self.identifiers[posA].split(',')):
            flagField.append(False)
            att = i.strip()
            if i.strip() == '*':
                self.attributes = deepcopy(self.allAttributes)
                break
            if '(' and ')' in i.strip():
                agg = (i.strip()).split('(')[0]
                att = (i.strip()).split('(')[1]
                att = (att).split(')')[0]
                if agg.lower() in self.agg:
                    self.aggregateList.append(agg.lower())
                else:
                    print('Invalid Aggregate Function')
                    exit(0)
                if not self.aggregate and bt > 0:
                    print('Normal column cannot be given along with aggregate function.')
                    exit(0)
                self.aggregate = True
            elif '(' in i.strip() or ')' in i.strip():
                print("Invalid Syntax !!")
                exit(0)
            elif self.aggregate:
                print('Normal column cannot be given along with aggregate function.')
                exit(0)
            for j in self.tableNames:
                if flagField[-1]:
                    continue
                newAtt = att
                if '.' not in att:
                    newAtt = j + '.' + att
                if newAtt in self.tableDict[j]:
                    flagField[-1] = True
                    if self.aggregate:
                        self.attributesAgg.append(newAtt)
                        self.attributes.append(agg + '(' + newAtt + ')')
                    else:
                        self.attributes.append(newAtt)
            if not flagField[-1]:
                print("Invalid Attribute ", i.strip())
                exit(0)

        for i, j in enumerate(self.allAttributes):
            self.flagAttributes.append(True) if j in self.attributes else self.flagAttributes.append(False)

    def getTableNames(self, posT):
        self.tableNames = []
        for i in self.identifiers[posT].split(','):
            if i.strip() in self.tableNames:
                print("Table name cannot be repeated after FROM Clause !!")
                exit(0)
            if i.strip() in self.tableList:
                self.tableNames.append(i.strip())
            else:
                print("INVALID TABLE NAME")
                exit(0)
        if(len(self.tableNames)) < 1:
            print("No Table Names were given.")
            exit(0)

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
        for i, att in enumerate(self.attributesAgg):
            idx = self.allAttributes.index(att)
            lst = [tab[idx] for tab in curTable]
            if self.aggregateList[i] == 'max':
                final.append(max(lst))
            elif self.aggregateList[i] == 'min':
                final.append(min(lst))
            elif self.aggregateList[i] == 'sum':
                final.append(sum(lst))
            elif self.aggregateList[i] == 'avg':
                final.append(sum(lst)/len(lst))
        self.finalTable.append(final)

    def selectAttributes(self):
        if self.aggregate:
            self.selectAggregates()
            return
        curTable = deepcopy(self.finalTable)
        self.finalTable = []
        for i in curTable:
            lst = []
            for j in self.attributes:
                idx = self.allAttributes.index(j)
                lst.append(i[idx])
            self.finalTable.append(lst)

    def distinctTable(self):
        lst = deepcopy(self.finalTable)
        self.finalTable = []
        for i in lst:
            if i not in self.finalTable:
                self.finalTable.append(i)

    def intersection(self, lst1, lst2):
        return [value for value in lst1 if value in lst2]

    def union(self, lst1, lst2):
        return lst1 + lst2 - self.intersection(lst1, lst2)

    def applyOp(self, a1, op, a2):
        if op == '=':
            return a1 == a2
        if op == '>=':
            return a1 >= a2
        if op == '<=':
            return a1 <= a2
        if op == '>':
            return a1 > a2
        if op == '<':
            return a1 < a2
        
    def getTabCond(self, a1, op, a2):
        val1 = True
        val2 = True

        return []

    def procWhere(self):
        idx = self.tokenNames.index('Where')
        whr = self.query[idx].tokens
        if len(whr) < 3:
            print("Invalid Query Syntax !!")
            exit(0)
        self.cond = True
        self.joinOp = 'or'
        for i in range(2, len(whr), 2):
            if type(whr[i]).__name__ == 'Comparison':
                self.cond = False
                comp = []
                flag = True
                for op in self.operators:
                    if op in str(whr[i]):
                        flag = False
                        lst = str(whr[i]).split(op)
                        if len(lst) != 2:
                            print("Invalid Syntax !!")
                            exit(0)
                        comp.append(lst[0].strip())
                        comp.append(op)
                        comp.append(lst[1].strip())
                if flag:
                    print("Operator not found!!")
                    exit(0)
                # print(comp)
                table2 = self.getTabCond(comp[0], comp[1], comp[2])
                table1 = deepcopy(self.finalTable)
                if self.joinOp == 'or':
                    self.finalTable = self.union(table1, table2)
                else:
                    self.finalTable = self.intersection(table1, table2)
            elif str(whr[i]).lower() == 'and' or whr[i].lower() == 'or':
                self.cond = True
                self.joinOp = str(whr[i]).lower()
            else:
                print("Invalid Query Syntax !!")
                exit(0)

        if self.cond == True:
            print("Invalid Query Syntax !!")
            exit(0)

    def processQuery(self):
        if self.qType != 'SELECT' or len(self.identifiers) < 4 or len(self.identifiers) > 6:
            print("INVALID QUERY !!")
            exit(0)

        self.where = True if 'where' in self.identifiers[-1] else False
        self.distinct = True if self.identifiers[1] == 'distinct' else False
        posT = 4 if self.distinct else 3
        posF = 3 if self.distinct else 2
        posA = 2 if self.distinct else 1

        if self.identifiers[posF] != 'from':
            print("INVALID QUERY SYNTAX !")
            exit(0)

        self.getTableNames(posT)
        self.getTables()
        self.getAttributes(posA)

        # Joining all the tables
        self.joinTables()

        # Where Condition
        # if self.where:
        #     self.procWhere()

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
