import csv
import sys
import re
import sqlparse
import os
import numbers

metadataDictionary = {}

def main():
	readMetadata()
	parseQuery(query)

def readMetadata():
	f = open('sampleData/metadata.txt','r')
	check = 0
	for line in f:
		if line.strip() == "<begin_table>":
			check = 1
			continue
		if check == 1:
			tableName = line.strip()
			metadataDictionary[tableName] = []
			check = 0
			continue
		if not line.strip() == '<end_table>':
			metadataDictionary[tableName].append(line.strip())

	for i in metadataDictionary:
		metadataDictionary[i] = filter(None, metadataDictionary[i])
	
def parseQuery(query):
    parsedQuery = sqlparse.parse(query)[0].tokens
    queryType = sqlparse.sql.Statement(parsedQuery).get_type()
    identifierList = []
    l = sqlparse.sql.IdentifierList(parsedQuery).get_identifiers()
    
    for i in l:
        identifierList.append(str(i))
    if (queryType == 'SELECT'):
        processSelect(parsedQuery, identifierList)\
    else:
        print("invalid")

def processSelect(parsedQuery, identifierList):
	if (len(identifierList) == 4):
		filePath = 'sampleData/'+ identifierList[3] + '.csv'
		function = re.sub(ur"[\(\)]",' ',identifierList[1]).split()
		#print function

		if (os.path.exists(filePath)):
			f = open(filePath,'rb')
			reader = csv.reader(f)

			#select * from <table>
			#printing the entire table
			if (function[0] == '*'):
				for i in metadataDictionary[identifierList[3]]:
					print i+'\t',
				print '\n'

				for row in reader:
					for i in row:
						print i+'\t',
					print '\n'

			#aggregate functions : min, max, avg, sum
			#distinct projections
			elif(function[0] == 'max' or function[0] == 'min' or function[0] == 'sum' or function[0] == 'avg' or function[0] =='distinct'):
				attribute = function[1]
				m = open('Data/metadata.txt')
				columnNumber = 0
				flag =0
				found = 0
				for line in m:
					if (line.strip() == identifierList[3]):
						flag =1
					if(flag == 1):
						if(line.strip() == '<end_table>'):
							#checking if attribute exists
							if(found != 1):
								print "ERROR: attribute does not exist"
								return
						if(line.strip()!=attribute):
							columnNumber += 1
						if(line.strip() == attribute):
							found = 1;
							break
				#print count
				#print columnNumber
				m.close()
				values=[]
				for row in reader:
					values.append(int(row[columnNumber-1]))

				print identifierList[1]
				if(function[0] == 'max'):
					print max(values)
				elif(function[0] == 'min'):
					print min(values)
				elif(function[0] == 'sum'):
					print reduce(lambda x,y:x+y,values)
				elif(function[0] == 'avg'):
					print reduce(lambda x,y:x+y,values)/float(len(values))
				elif(function[0] == 'distinct'):
					newValues = list(set(values))
					for i in newValues:
						print i
				else:
					pass

			else:
				attributes = re.sub(ur"[\,]",' ',identifierList[1]).split()
				#print attributes
				tables = re.sub(ur"[\,]",' ',identifierList[3]).split()
				#print tables

				#for a single table
				if (len(tables)==1):
					table = tables[0]
					columnNumbers = []
					for i in attributes:
						print i+'\t',
						if i in metadataDictionary[table]:
							columnNumbers.append(metadataDictionary[table].index(i))
						else:
							print"ERROR: attribute doesn't exist"
							return
					print '\n'
					for row in reader:
						for i in columnNumbers:
							print row[i]+'\t',
						print '\n'
				# else:
				# 	columnNumbers=[]
				# 	d={}
				# 	for i in attributes:
				# 		count = 0;
				# 		for j in tables:
				# 			if i in metadataDictionary[j]:
				# 				d[i] = []
				# 				d[i].append(j,metadataDictionary[j].index(i))
				# 				count+=1
				# 			if (count > 1):
				# 				print "ERROR:ambiguous attribute"
				# 				return
				# 	print d
			f.close()
			return
		else:
			print "ERROR: table does not exist"
	
	else:
		print"ERROR:invalid"
	

if __name__ == "__main__":
	main()

