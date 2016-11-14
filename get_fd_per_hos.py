#!/usr/bin/env python
#coding:utf-8



import pandas as pd
import feather as fd
import os


class CsvData:
    
    
    
    def __init__(self,year,season,
                 path,ncol,ixcol):
        self.year = year
        self.season = season
        self.path = path
        self.ncol = int(ncol)
        self.ixcol = ixcol
    
    def dfvars(self):
        dfvars=''
        for var in range(1,self.ncol+1):
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars
        
        
    def writedf(self,chunkSize,write_path):
        loop = True
        looptimes=0
        reader = pd.read_csv(self.path,
                             iterator = True)
        while loop:
            try:
                looptimes += 1
                df = reader.get_chunk(chunkSize)
                df.columns = self.dfvars().split(',')
                ixcode = df[[self.ixcol]]
                ixcode = ixcode.ix[:,self.ixcol]
                ixcode = ixcode.value_counts().index
                for code in ixcode:
                    try:
                        df_hos = df.ix[df.x5 == code,:]
                        df_hos = df_hos.astype('str')
                        fd.write_dataframe(df_hos,
                                writepath+str(code)+'_'+
                                self.year+'_'+self.season+
                                '.pyr'+str(looptimes))
                    except ValueError:
                        continue   
            except StopIteration:
                loop =False
                print"Iteration is stopped."
    
    @classmethod
    def combinehos(clx,year,outputpath):
        cfiles = os.listdir(writepath)
    #   cfiles.index('CHN') len(hoscodes); len(cfiles); len(hoscodes)
        hoscodes = [cfile[0:12] for cfile in cfiles]
        hoscodes = pd.Series(hoscodes)
        hoscodes = hoscodes.value_counts().index
        for code in hoscodes:
            hosfiles=[filename for filename in 
                  cfiles if code in filename]
            hosfiles.sort()
            hosfiles.sort(key=len)
            chos = pd.DataFrame() 
            for hosfile in hosfiles:
                hos = fd.read_dataframe(writepath+hosfile)
                chos = pd.concat([chos,hos],axis = 0)
            fd.write_dataframe(chos,outputpath+str(code)+
                           '_'+year+'.pyr')  
   
if __name__ == '__main__':
    
    
    writepath = '/mnt/e/pyr/data/2015perhosproc/'
    
    '''
    data1501 = CsvData('2015','01',
                       '/mnt/e/data/2015/1501.CSV',
                       261,'x5')    
    
    data1501.writedf(50000,writepath)
  

    data1502 = CsvData('2015','02',
                       '/mnt/e/data/2015/1502.CSV',
                       261,'x5')    
    
    data1502.writedf(50000,writepath)
    
    
    data1503 = CsvData('2015','03',
                       '/mnt/e/data/2015/1503.CSV',
                       261,'x5')    
    
    data1503.writedf(50000,writepath)
     
    data1504 = CsvData('2015','04',
                       '/mnt/e/data/2015/1504.CSV',
                       261,'x5')    
    
    data1504.writedf(50000,writepath)

    '''
    CsvData.combinehos('2015','/mnt/e/pyr/data/2015perhos/')
    
    print 'mission accomplished!!!'
    
 
    '''
    
    writepath = '/mnt/e/pyr/data/2014/'
    selvar14 = [1,2,3,12,14,15,16,40,
            45,50,51,52,53]+range(210,240)
            
    selvar15 = [1,5,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)
    
    data1401 = CsvData('2014','01',
                '/mnt/e/data/2014/1401.csv',
                239,selvar14,'x3')    
    
    data1401.writedf(50000,writepath)
  
    data1402 = CsvData('2014','02',
                '/mnt/e/data/2014/1402.csv',
                239,selvar14,'x3')    
    
    data1402.writedf(50000,writepath)
    
    
    data1403 = CsvData('2014','03',
                '/mnt/e/data/2014/1403.csv',
                239,selvar14,'x3')    
    
    data1403.writedf(50000,writepath)
     
    data1404 = CsvData('2014','04',
                '/mnt/e/data/2014/1404.csv',
                239,selvar14,'x3')    
    
    data1404.writedf(50000,writepath)
    
    selvar = [1,2,12,14,15,16,40,
            45,50,51,52,53]+range(210,240)
    CsvData.combinecol(selvar,'2014',
                       '/mnt/e/pyr/data/2014x/')
    
    print 'mission accomplished!!!' 
    ''' 
    '''
    writepath = '/mnt/e/pyr/data/2013/'
    selvar13 = [1,5,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)
    
    data13 = CsvData('2013','01',
                '/mnt/e/data/2013/1301.csv',
                261,selvar13,'x5')    
    
    data13.writedf(50000,writepath)
    
    selvar = [1,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)
    CsvData.combinecol(selvar,'2013',
                       '/mnt/e/pyr/data/2013x/')
    print 'mission accomplished!!!' 
    '''
    
    '''
    reader = pd.read_csv('/mnt/e/data/2015/1502.CSV',
                             iterator = True)
    chunkSize = 50000
    
    df = reader.get_chunk(chunkSize)
    df.columns = s_dfvars(261).split(',')
    
def yield_df_hos():
    ixcol = 'x5'
    ixcode = df[[ixcol]]
    ixcode = ixcode.ix[:,ixcol]
    ixcode = ixcode.value_counts().index
    for code in ixcode:
        try:
            df_hos = df.ix[df.x5 == ixcode[0],:]
            df_hos = pd.DataFrame(df_hos)
            df_hos = df_hos.astype('str')
            fd.write_dataframe(df_hos,
                    writepath+str(ixcode[0])+'_'+
                    '2015'+'_'+'02'+
                    '.pyr'+'1')
        except ValueError:
            continue   
    
    df_rep = yield_df_hos()
    
    for data in df_rep:
        print data 
        
################################################################        
        cfiles = os.listdir(writepath)
    #   
        hoscodes = [cfile[0:12] for cfile in cfiles]
        hoscodes = pd.Series(hoscodes)
        hoscodes = hoscodes.value_counts().index
        for code in hoscodes:
            hosfiles=[filename for filename in 
                  cfiles if hoscodes[0] in filename]
            hosfiles.sort()
            hosfiles.sort(key=len)
            chos = pd.DataFrame() 
            for hosfile in hosfiles:
                hos = fd.read_dataframe(writepath+hosfile)
                chos = pd.concat([chos,hos],axis = 0)
            fd.write_dataframe(chos,outputpath+str(code)+
                           '_'+year+'.pyr')  
    
    
     '''
        
        