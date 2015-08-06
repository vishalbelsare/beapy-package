import datetime
import requests
import numpy as np
import pandas as pd
import sys


# August 3, 2015: Updated the getNipa() method to accomodate possible differences in data availability for series in tables. 
#                 Cleaned up and organized the code substantially.

class initialize:

    def __init__(self,apiKey=None):
        ''' Saves the API key.'''

        self.apiKey = apiKey

    # 1. Methods for getting information about the available datasets, parameters, and parameter values.

    def getDataSetList(self):

        '''Method returns a list of describing the datasets available through the BEA API. No arguments'''

        r = requests.get('http://www.bea.gov/api/data?&UserID='+self.apiKey+'&method=GETDATASETLIST&ResultFormat=JSON&')
        rJson = r.json()
        lines='Datasets available through the BEA API:\n\n'
        n=1

        dataSetList = []
        for element in rJson['BEAAPI']['Results']['Dataset']:
            if np.mod(n,5)==0:
                lines = lines+element['DatasetName'].ljust(20,' ') +': '+element['DatasetDescription']+'\n\n'
                dataSetList.append(element['DatasetName'])
            else:
                lines = lines+element['DatasetName'].ljust(20,' ') +': '+element['DatasetDescription']+'\n'
                dataSetList.append(element['DatasetName'])
            n+=1
        print(lines)
        self.dataSets = lines
        self.dataSetList = dataSetList

    def getParameterList(self,dataSetName):

        '''Method returns a list of the parameters for a given dataset. Argument: one of the dataset names returned by getDataSetList().'''

        r = requests.get('http://www.bea.gov/api/data?&UserID='+self.apiKey+'&method=GETPARAMETERLIST&datasetname='+dataSetName+'&ResultFormat=JSON&')
        rJson = r.json()
        lines = 'Parameters for the '+dataSetName+' dataset.\n\n'

        strWidth  = 25
        parameterList = []
        for element in rJson['BEAAPI']['Results']['Parameter']:

            elementKeys = list(element.keys())

            lines = lines+'Parameter name'.ljust(strWidth,' ')  +'  '+element['ParameterName']+'\n'
            lines = lines+'Description'.ljust(strWidth,' ')  + '  '+element['ParameterDescription']+'\n'

            parameterList.append(element['ParameterName'])

            if element['ParameterIsRequiredFlag']==0:
                lines = lines+'Required?'.ljust(strWidth,' ')  + '  No'+'\n'
            else:
                lines = lines+'Required?'.ljust(strWidth,' ')  + '  Yes'+'\n'
            
            if 'AllValue' in elementKeys:
                if element['AllValue']=='':
                    lines = lines+'\"All\" Value'.ljust(strWidth,' ')  + '  N/A'+'\n'
                else:
                    lines = lines+'\"All\" Value'.ljust(strWidth,' ') +'  '+element['AllValue']+'\n'
            if element['MultipleAcceptedFlag']==0:
                lines = lines+'Multiple (list) accepted?'.ljust(strWidth,' ')  + '  No'+'\n'
            else:
                lines = lines+'Multiple (list) accepted?'.ljust(strWidth,' ')  + '  Yes'+'\n'
            lines = lines+'Data type'.ljust(strWidth,' ')  + '  '+element['ParameterDataType']+'\n'
            if 'ParameterDefaultValue' in elementKeys:
                if element['ParameterDefaultValue']=='':
                    lines = lines+'Default value'.ljust(strWidth,' ')  + '  N/A'+'\n\n\n'
                else:
                    lines = lines+'Default value'.ljust(strWidth,' ')  + '  '+element['ParameterDefaultValue']+'\n\n\n'
            else:
                lines = lines+'\n\n'

        print(lines)
        self.parameters = lines
        self.parameterList = parameterList

    def getParameterValues(self,dataSetName, parameterName):

        '''Method returns a list of the  values accepted for a given parameter of a dataset.
        Arguments: one of the dataset names returned by getDataSetList() and a parameter returned by getParameterList().'''

        r = requests.get('http://bea.gov/api/data?&UserID='+self.apiKey+'&method=GetParameterValues&datasetname='+dataSetName+'&ParameterName='+parameterName+'&')
        rJson = r.json()
        rJson

        lines='Values accepted for '+parameterName+' in dataset '+dataSetName+':\n\n'
        for element in rJson['BEAAPI']['Results']['ParamValue']:
            for key,value in element.items():
                lines+=key+':  '+value+'\n'
            lines+='\n'
        
        print(lines)
        self.parameterValues = lines

    # 2. Methods for retreiving data.

    # 2.1 Regional Data (statistics by state, county, and MSA)

    def getRegionalData(self,KeyCode=None,GeoFips='STATE',Year='ALL'):
        '''Retrieve state and regional data.

        Name        Type    Required?   Multiple values?    "All" Value                     Default

        KeyCode     int     yes         no                  N/A                             
        GeoFips     str     no          yes                 'STATE' or 'COUNTY' or 'MSA'    STATE
        Year        int     no          yes                 "ALL"                           ALL
        '''

        if type(KeyCode)==list:
            KeyCode = ','.join(KeyCode)

        if type(Year)==list:
            Year = [str(y) for y in Year]
            Year = ','.join(Year)

        if type(GeoFips)==list:
            GeoFips = ','.join(GeoFips)

        uri = 'http://bea.gov/api/data/?UserID='+self.apiKey+'&method=GetData&datasetname=RegionalData&KeyCode='+str(KeyCode)+'&Year='+str(Year)+'&GeoFips='+str(GeoFips)+'&ResultFormat=JSON&'
        r = requests.get(uri)
        rJson = r.json()

        dataDict = {}
        # dates = []
        # YearList = []
        # name =''

        columnNames = []
        dates = []

        try: 
            for element in rJson['BEAAPI']['Results']['Data']:
                if element['GeoName'] not in columnNames:
                    columnNames.append(element['GeoName'])

                date = convertDate(element['TimePeriod'],'A')
                if date not in dates:
                    dates.append(date)

            data = np.zeros([len(dates),len(columnNames)])
            data[:] = np.nan
            frame = pd.DataFrame(data,columns = columnNames, index = dates)

            for element in rJson['BEAAPI']['Results']['Data']:
                date = convertDate(element['TimePeriod'],'A')
                if 'DataValue' in element.keys():
                    frame.loc[date,element['GeoName']] = float(element['DataValue'].replace(',',''))
            return frame
        except:
            print('Invalid input.',sys.exc_info()[0])

    
    # 2.2 NIPA (National Income and Product Accounts)

    def getNipa(self,TableID=None,Frequency=None,Year='X',ShowMillions='N'):

        '''Retrieve data from a NIPA table.

        Name            Type    Required?   "All" Value     Default

        TableID         int     yes         N/A             None
        Frequency(A/Q)  str     yes         N/A             None
        Year            int     yes         "X"             "X"
        ShowMillions    str     no          N/A             'N'   
        '''

        if Frequency=='M':
            print('Error: monthly Frequency available for NIPA tables.')

        if type(Year)==list:
            Year = [str(y) for y in Year]
            Year = ','.join(Year)

        uri = 'http://bea.gov/api/data/?UserID='+self.apiKey+'&method=GetData&datasetname=NIPA&TableID='+str(TableID)+'&Frequency='+Frequency+'&Year='+str(Year)+'&ShowMillions='+ShowMillions+'&ResultFormat=JSON&'
        r = requests.get(uri)
        rJson = r.json()

        columnNames = []
        dates = []
        try:
            for element in rJson['BEAAPI']['Results']['Data']:
                if element['LineDescription'] not in columnNames:
                    columnNames.append(element['LineDescription'])
                
                date = convertDate(element['TimePeriod'],Frequency)
                if date not in dates:
                    dates.append(date)

            data = np.zeros([len(dates),len(columnNames)])
            data[:] = np.nan
            frame = pd.DataFrame(data,columns = columnNames, index = dates)

            for element in rJson['BEAAPI']['Results']['Data']:
                date = convertDate(element['TimePeriod'],Frequency)
                frame.loc[date,element['LineDescription']] = float(element['DataValue'].replace(',',''))
            return frame
        except:
            print('Error: invalid input.')

    # # 3.3 NIUnderlyingDetail (National Income and Product Accounts)

    # def getNIUnderlyingDetail(self,TableID,Frequency='A',Year='X'):

    #     if type(Year)==list:
    #         Year = [str(y) for y in Year]
    #         Year = ','.join(Year)

    #     uri = 'http://bea.gov/api/data/?UserID='+apiKey+'&method=GetData&datasetname=NIUnderlyingDetail&TableID='+str(TableID)+'&Year='+str(Year)+'&Frequency='+str(Frequency)+'&ResultFormat=JSON&'
    #     r = requests.get(uri)
    #     rJson = r.json()

    #     columnNames = []
    #     dates = []
    #     try:

    # 3.4 Fixed Assets

    def getFixedAssets(self,TableID=None,Year='X'):

        uri = 'http://bea.gov/api/data/?UserID='+self.apiKey+'&method=GetData&datasetname=FixedAssets&TableID='+str(TableID)+'&Year='+str(Year)+'&ResultFormat=JSON&'
        r = requests.get(uri)
        rJson = r.json()

        columnNames = []
        dates = []
        try:
            for element in rJson['BEAAPI']['Results']['Data']:
                if element['LineDescription'] not in columnNames:
                    columnNames.append(element['LineDescription'])
                
                date = convertDate(element['TimePeriod'],'A')
                if date not in dates:
                    dates.append(date)

            data = np.zeros([len(dates),len(columnNames)])
            data[:] = np.nan
            frame = pd.DataFrame(data,columns = columnNames, index = dates)

            for element in rJson['BEAAPI']['Results']['Data']:
                date = convertDate(element['TimePeriod'],'A')
                frame.loc[date,element['LineDescription']] = float(element['DataValue'].replace(',',''))
            return frame
        except:
            print('Error: invalid input.')


    

# Auxiliary function.

        
def convertDate(dateString,Frequency):

    '''Function for converting the date strings from BEA with quarter indicators into datetime format'''

    if Frequency=='A':
        month='01'
    elif Frequency=='Q':
        if dateString[-1]=='1':
            month='01'
        elif dateString[-1]=='2':
            month='04'
        elif dateString[-1]=='3':
            month='07'
        else:
            month='10'
    return datetime.datetime.strptime(dateString[0:4]+'-'+month+'-01','%Y-%m-%d')