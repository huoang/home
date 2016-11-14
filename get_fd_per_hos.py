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
                try:
                    ixcode = df[[self.ixcol]]
                    ixcode.count_values
                    df_rep = df[[self.ixcol,var]]
                    df_cols = pd.DataFrame(df_cols)
                    fd.write_dataframe(df_cols,
                    writepath+self.year+'_'+
                    self.season+'_'+var+'.pyr'+
                    str(looptimes))
                except ValueError:
                        continue   
            except StopIteration:
                loop =False
                print"Iteration is stopped."
    
    @classmethod
    def combinecol(clx,selvar,year,outputpath):
        cfiles = os.listdir(writepath)
        selvars = clx.s_selvars(selvar).split(',')
        single_var=['_'+var+'.' for var in 
                   selvars]
        for var in single_var:
            cols=[filename for filename in 
                  cfiles if var in filename]
            cols.sort()
            ccol = pd.DataFrame() 
            for col in cols:
                col_rep = fd.read_dataframe(writepath+col)
                ccol = pd.concat([ccol,col_rep],axis = 0)
            fd.write_dataframe(ccol,outputpath+year
                               +var+'pyr')  
     
        
if __name__ == '__main__':
    
    
    writepath = '/mnt/e/pyr/data/2015/'
    
    init_selvar_15= [1,5,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)+[262]
    fee_vars = range(229,259)
    drop_vars = 'x229,x230,x240,x242,x243,x247'
    data1501 = CsvData('2015','01',
                       '/mnt/f/data/2015/1501.CSV',
                       261,'x5',init_selvar_15,
                       drop_vars,fee_vars,
                       'x262')    
    
    data1501.writedf(50000,writepath)
  
    data1502 = CsvData('2015','02',
                       '/mnt/f/data/2015/1502.CSV',
                       261,'x5',init_selvar_15,
                       drop_vars,fee_vars,
                       'x262')    
    
    data1502.writedf(50000,writepath)
    
    
    data1503 = CsvData('2015','03',
                       '/mnt/f/data/2015/1503.CSV',
                       261,'x5',init_selvar_15,
                       drop_vars,fee_vars,
                       'x262')    
    
    data1503.writedf(50000,writepath)
     
    data1504 = CsvData('2015','04',
                       '/mnt/f/data/2015/1504.CSV',
                       261,'x5',init_selvar_15,
                       drop_vars,fee_vars,
                       'x262')    
    
    data1504.writedf(50000,writepath)
    
    combine_var_15 = [1,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)+[262]
    CsvData.combinecol(combine_var_15,'2015',
                       '/mnt/e/pyr/data/2015x/')
    
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
    reader = pd.read_csv('/mnt/f/data/2015/1502.CSV',
                             iterator = True)
    chunkSize = 50000
    
    df = reader.get_chunk(chunkSize)
    df.columns = s_dfvars(261).split(',')
    
    ixcol = 'x5'
    ixcode = df[[ixcol]]
    ixcode = ixcode.ix[:,ixcol]
    ixcode.value_counts()
    type(ixcode)

    import shelve
    
    f = shelve.open('/mnt/f/data/2015/1502.CSV')
        
        