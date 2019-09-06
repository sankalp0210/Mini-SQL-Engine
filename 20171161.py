import csv
import sys
from copy import deepcopy
import sqlparse

class Query():
    def __init__(self, query):
        if len(query[0]) < 3:
            self.prError("No query given.")
        query[0] = ' '.join(query[0].split())
        self.agg = ['max', 'min', 'avg', 'sum']
        self.operators = ['>=', '<=', '>', '<', '=']
        self.tableList = []
        self.tableDict = {}
        self.tables = {}
        self.readMetaData()
        self.identifiers = []
        self.parseQuery(query)
        self.convertToLower()
        self.processQuery()

    def prError(self, msg = "Invalid Query Syntax"):
        print(msg)
        exit(0)

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
                    self.prError('Invalid Aggregate Function')
                if not self.aggregate and bt > 0:
                    self.prError('Normal column cannot be given along with aggregate function.')
                self.aggregate = True
            elif '(' in i.strip() or ')' in i.strip():
                self.prError("Invalid Syntax !")
            elif self.aggregate:
                self.prError('Normal column cannot be given along with aggregate function.')
            for j in self.tableNames:
                newAtt = att
                if '.' not in att:
                    newAtt = j + '.' + att
                if newAtt in self.tableDict[j] and not flagField[-1]:
                    flagField[-1] = True
                    if self.aggregate:
                        self.attributesAgg.append(newAtt)
                        self.attributes.append(agg + '(' + newAtt + ')')
                    else:
                        self.attributes.append(newAtt)
                elif newAtt in self.tableDict[j]:
                    self.prError("Ambiguous Column name : " + str(i.strip()))
            if not flagField[-1]:
                self.prError("Invalid Attribute " + str(i.strip()))

        for i, j in enumerate(self.allAttributes):
            self.flagAttributes.append(True) if j in self.attributes else self.flagAttributes.append(False)

    def getTableNames(self, posT):
        self.tableNames = []
        for i in self.identifiers[posT].split(','):
            if i.strip() in self.tableNames:
                self.prError("Table name cannot be repeated after FROM Clause !")
            if i.strip() in self.tableList:
                self.tableNames.append(i.strip())
            else:
                self.prError("INVALID TABLE NAME")
        if(len(self.tableNames)) < 1:
            self.prError("No Table Names were given.")

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
            try:
                if self.aggregateList[i] == 'max':
                    final.append(max(lst))
                elif self.aggregateList[i] == 'min':
                    final.append(min(lst))
                elif self.aggregateList[i] == 'sum':
                    final.append(sum(lst))
                elif self.aggregateList[i] == 'avg':
                    final.append(sum(lst)/len(lst))
            except:
                return
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
        lst = lst1 + lst2
        lst3 = self.intersection(lst1, lst2)
        fin = []
        for value in lst:
            fin.append(value)
            for k in lst3:
                if value == k:
                    lst3.remove(k)
                    fin.remove(value)
                    break
        return fin

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
        tab1 = ''
        tab2 = ''
        b1 = deepcopy(a1)
        b2 = deepcopy(a2)
        for i in self.tableNames:
            newAtt1 = a1
            newAtt2 = a2
            if '.' not in newAtt1:
                newAtt1 = i + '.' + newAtt1
            if '.' not in newAtt2:
                newAtt2 = i + '.' + newAtt2
            if newAtt1 in self.tableDict[i] and val1:
                b1 = newAtt1
                val1 = False
                tab1 = i
            elif newAtt1 in self.tableDict[i]:
                self.prError('Ambiguous Column name ' + str(a1))
            if newAtt2 in self.tableDict[i] and val2:
                b2 = newAtt2
                val2 = False
                tab2 = i
            elif newAtt2 in self.tableDict[i]:
                self.prError('Ambiguous Column name ' + str(a1))
        a1 = deepcopy(b1)
        a2 = deepcopy(b2)
        lst = []
        for i in self.finalTable:
            x1 = 0
            x2 = 0
            if val1:
                try:
                    x1 = int(a1)
                except:
                    self.prError('Invalid attribute ' + str(a1))
            else:
                idx = self.allAttributes.index(a1)
                x1 = i[idx]
            if val2:
                try:
                    x2 = int(a2)
                except:
                    self.prError('Invalid attribute ' + str(a2))
            else:
                idx = self.allAttributes.index(a2)
                x2 = i[idx]
            if self.applyOp(x1, op, x2):
                lst.append(i)
        if (tab1 != tab2) and (op == '=') and tab1 != '' and tab2 != '' and\
                len(self.attributes) == len(self.allAttributes):
            idx = self.allAttributes.index(a2)
            self.flagAttributes[idx] = False
            try:
                self.attributes.remove(a2)
            except:
                pass
        return lst

    def procWhere(self):
        idx = self.tokenNames.index('Where')
        whr = self.query[idx].tokens
        if len(whr) < 3:
            self.prError("No condition after where !")
        self.cond = True
        self.joinOp = 'and'
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
                            self.prError("Invalid Condition!")
                        comp.append(lst[0].strip())
                        comp.append(op)
                        comp.append(lst[1].strip())
                if flag:
                    self.prError("Operator not found!")
                table2 = self.getTabCond(comp[0], comp[1], comp[2])
                table1 = deepcopy(self.finalTable)
                if self.joinOp == 'or':
                    self.finalTable = self.union(table1, table2)
                else:
                    self.finalTable = self.intersection(table1, table2)
            elif str(whr[i]).lower() == 'and' or str(whr[i]).lower() == 'or':
                self.cond = True
                self.joinOp = str(whr[i]).lower()
            else:
                self.prError("Only AND & OR conditions are allowed !")
        if self.cond == True:
            self.prError("No condition given at the end of the query!")

    def processQuery(self):
        if self.qType != 'SELECT':
            self.prError("Only Select Queries are allowed.")
        if len(self.identifiers) < 4 or len(self.identifiers) > 6:
            self.prError("INVALID QUERY !")
        if self.identifiers[-1][-1] != ';':
            self.prError("No semicolon present")
        self.where = True if 'where' in self.identifiers[-1] else False
        self.distinct = True if self.identifiers[1] == 'distinct' else False
        posT = 4 if self.distinct else 3
        posF = 3 if self.distinct else 2
        posA = 2 if self.distinct else 1

        if self.identifiers[posF] != 'from':
            self.prError("Invalid Query!")

        self.getTableNames(posT)
        self.getTables()
        self.getAttributes(posA)
        # Joining all the tables
        self.joinTables()
        # Where Condition
        if self.where:
            self.procWhere()
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
    print("NO QUERY GIVEN !")