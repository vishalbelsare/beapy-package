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
        for element in rJson['BEAAPI']['Results']['Dataset']:
            if np.mod(n,5)==0:
                lines = lines+element['DatasetName']+': '+element['DatasetDescription']+'\n\n'
            else:
                lines = lines+element['DatasetName']+': '+element['DatasetDescription']+'\n'
            n+=1
        print(lines)
        self.dataSetList = lines


    def getParameterList(self,dataSetName):

        '''Method returns a list of the parameters for a given dataset. Argument: one of the dataset names returned by getDataSetList().'''

        r = requests.get('http://www.bea.gov/api/data?&UserID='+self.apiKey+'&method=GETPARAMETERLIST&datasetname='+dataSetName+'&ResultFormat=JSON&')
        rJson = r.json()
        lines = 'Parameters for the '+dataSetName+' dataset.'

        for element in rJson['BEAAPI']['Results']['Parameter']:

            lines = lines+'Parameter name:  '+element['ParameterName']+'\n'
            lines = lines+'Description:  '+element['ParameterDescription']+'\n'

            if element['ParameterIsRequiredFlag']==0:
                lines = lines+'Required?  No'+'\n'
            else:
                lines = lines+'Required?  Yes'+'\n'
            if element['AllValue']=='':
                lines = lines+'\"All\" Value:  N/A'+'\n'
            else:
                lines = lines+'\"All\" Value:  '+element['AllValue']+'\n'
            if element['MultipleAcceptedFlag']==0:
                lines = lines+'Multiple (list) accepted?  No'+'\n'
            else:
                lines = lines+'Multiple (list) accepted?  Yes'+'\n'
            lines = lines+'Data type:  '+element['ParameterDataType']+'\n'
            if element['ParameterDefaultValue']=='':
                lines = lines+'Default value:  N/A'+'\n\n\n'
            else:
                lines = lines+'Default value:  '+element['ParameterDefaultValue']+'\n\n\n'

        print(lines)
        self.parameterList = lines

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

    # 2.1 Reguiional Data (statistics by state, county, and MSA)

    def getRegionalData(self,keyCode=None,geoFips='STATE',year='ALL'):
        '''Retrieve state and regional data.

        Name        Type    Required?   Multiple values?    "All" Value                     Default

        keyCode     int     yes         no                  N/A                             
        geoFips     str     no          yes                 'STATE' or 'COUNTY' or 'MSA'    STATE
        year        int     no          yes                 "ALL"                           ALL
        '''

        if type(year)==list:
            year = [str(y) for y in year]
            year = ','.join(year)

        if type(geoFips)==list:
            geoFips = ','.join(geoFips)

        uri = 'http://bea.gov/api/data/?UserID='+self.apiKey+'&method=GetData&datasetname=RegionalData&KeyCode='+str(keyCode)+'&Year='+str(year)+'&GeoFips='+str(geoFips)+'&ResultFormat=JSON&'
        r = requests.get(uri)
        rJson = r.json()

        dataDict = {}
        dates = []
        yearList = []
        name =''
        numSeries=0

        if type(year)==list:
            year = [str(y) for y in year]
            year = ','.join(year)
        try: 
            for element in rJson['BEAAPI']['Results']['Data']:

                if element['GeoName'] != name:
                    try:
                        dataDict[element['GeoName']+' - '+element['GeoFips']]= np.array([float(element['DataValue'])])
                    except:
                        dataDict[element['GeoName']+' - '+element['GeoFips']] = np.nan
                    name = element['GeoName']                
                    numSeries+=1
                else:
                    try:
                        dataDict[element['GeoName']+' - '+element['GeoFips']]= np.append(dataDict[element['GeoName']+' - '+element['GeoFips']],float(element['DataValue']))
                    except:
                        dataDict[element['GeoName']+' - '+element['GeoFips']]= np.append(dataDict[element['GeoName']+' - '+element['GeoFips']],np.nan)
                if element['TimePeriod'] not in yearList:
                    dates.append(convertDate(element['TimePeriod'],frequency='A'))
                    yearList.append(element['TimePeriod'])

            frame = pd.DataFrame(dataDict,index = dates)
            return frame
        except:
            print('Invalid input.',sys.exc_info()[0])

    
    # 2.2 NIPA (National Income and Product Accounts)

    def getNipa(self,tableId=None,frequency=None,year='X',showMillions='N'):

        '''Retrieve data from a NIPA table.

        Name            Type    Required?   "All" Value     Default

        tableId         int     yes         N/A             None
        frequency(A/Q)  str     yes         N/A             None
        year            int     yes         "X"             "X"
        showMillions    str     no          N/A             'N'   
        '''

        if frequency=='M':
            print('Error: monthly frequency available for NIPA tables.')

        uri = 'http://bea.gov/api/data/?UserID='+self.apiKey+'&method=GetData&datasetname=NIPA&TableID='+str(tableId)+'&Frequency='+frequency+'&Year='+str(year)+'&Showmillions='+showMillions+'&ResultFormat=JSON&'
        r = requests.get(uri)
        rJson = r.json()

        columnNames = []
        dates = []
        try:
            for element in rJson['BEAAPI']['Results']['Data']:
                if element['LineDescription'] not in columnNames:
                    columnNames.append(element['LineDescription'])
                
                date = convertDate(element['TimePeriod'],frequency)
                if date not in dates:
                    dates.append(date)

            data = np.zeros([len(dates),len(columnNames)])
            data[:] = np.nan
            frame = pd.DataFrame(data,columns = columnNames, index = dates)

            for element in rJson['BEAAPI']['Results']['Data']:
                date = convertDate(element['TimePeriod'],frequency)
                frame.loc[date,element['LineDescription']] = float(element['DataValue'].replace(',',''))
            return frame
        except:
            print('Error: invalid input.')

    

# Auxiliary function.

        
def convertDate(dateString,frequency):

    '''Function for converting the date strings from BEA with quarter indicators into datetime format'''

    if frequency=='A':
        month='01'
    elif frequency=='Q':
        if dateString[-1]=='1':
            month='01'
        elif dateString[-1]=='2':
            month='04'
        elif dateString[-1]=='3':
            month='07'
        else:
            month='10'
    return datetime.datetime.strptime(dateString[0:4]+'-'+month+'-01','%Y-%m-%d')