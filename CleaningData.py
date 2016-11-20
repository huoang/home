#!/usr/bin/env python
#coding:utf-8



import pandas as pd
import numpy as np
import tables as tm
import h5py as h5
import os


hdffile = 'E:/pyr/data/hdf5/R_fee_15.h5'

fee_15 = pd.read_hdf(hdffile,'fee_15')

fee_15.ttlfee = fee_15.ttlfee.astype('float')

fee15 = fee_15[fee_15.ttlfee>0]

fee_15.ttlfee.median()

fee_15.ttlfee.mean()

fin_rep15 = pd.read_hdf(hdffile,'fin_fee_15')

fin_rep15.ix[fin_rep15.X1 == '',:]

hos_dic = pd.read_hdf("e:/pyr/data/hdf5/R_fee_15.h5",
                      'hos_dic')

hos_dic.columns =['hoscode','hosname'] 

hosdic = pd.Series(hos_dic.hosname)

hosdic.index = hos_dic['hoscode']

hosdic['410000002236']

cfiles = os.listdir('e:/pyr/data/y2015/2015perhos/')
#   cfiles.index('CHN') len(code15); len(cfiles); len(code15)
code15 = [cfile[0:12] for cfile in cfiles]

code15 = pd.Series(code15)

code15 = code15.value_counts().index

hosname15 = [hosdic[code] for code in code15
            if code in hosdic.index]

hoscode15 = [code for code in code15
            if code in hosdic.index]

hos15 = pd.Series(hosname15,index = hoscode15)

hosdic15 = dict(zip(hoscode15,hosname15))

hosname15.index('')

hos15.index[237]



for X in fin_rep15.columns[1:]:
    fin_rep15.ix[:,X] = fin_rep15.ix[:,X].str.replace(',','')

pyh5file = 'E:/pyr/data/hdf5/py_fee_15.h5'

pyfee15 = pd.HDFStore(pyh5file)

pyfee15.append('py_fee_15', fin_rep15, 
                    data_columns=fin_rep15.columns)

pyfee15.close()

pyfee15.open()


fin_rep15.to_hdf('e:/pyr/data/hdf5/py_fee_15.h5',
                 'fin_rep15',format='table',
                 data_columns=True)

h5file = 'e:/pyr/data/hdf5/r_perhos.h5'

nyyy = pd.read_hdf(h5file,'410000002820')

xmyy = pd.read_hdf(h5file,'')


fin_rep15.to_hdf('e:/pyr/data/hdf5/py_fee_15.h5',
                 'fin_rep15',format='table',
                 data_columns=True)


store_perhos_15 = pd.HDFStore(
    'e:/pyr/data/hdf5/py_perhos_15.h5')

py_perhos_15.append('nyyy', store_perhos_15, 
                    data_columns=nyyy.columns,
                    dtype = 'table')


nyyy.to_hdf('e:/pyr/data/hdf5/py_perhos_15',
                 '410000002820',mode = 'w',
                 data_columns = True,
                 format = 'table',
                 complevel = 9L,
                 complib = 'zlib')



py_perhos_15.close()

py_perhos_15.open()

help(nyyy.to_hdf)

h5file = h5.File('e:/pyr/data/hdf5/h5_fee.h5','w')

h5file.create_dataset('410000002820', data = nyyy)






