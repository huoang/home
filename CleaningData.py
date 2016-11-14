#!/usr/bin/env python
#coding:utf-8



import pandas as pd
import feather as fd
import os


path = '/mnt/e/pyr/data/2015x/'
xfiles = os.listdir(path)
xfiles.sort()
xfiles.sort(key=len)
len(xfiles)
col229 =  fd.read_dataframe(path+xfiles[12])
col229[col229['x5'] == '410000000624']
col229.iloc[col229.vericol == True,:]


col262 = fd.read_dataframe(path+xfiles[42])
col229['vericol'] = col262.x262
col229[col229['vericol'] == 1]

col229.vericol.value_counts()

veri_true=col229.vericol.isin(['True'])

fee_mis_hoslist = col229[veri_true].x5.value_counts()
hosnames = pd.read_csv('/mnt/e/R/rdoc/Hosnames.csv')

hosnames.columns = ['id','name']

hosnames = hosnames.set_index('id')

hosnames.ix[410000207738]

fee_mis_hoslist = pd.DataFrame(fee_mis_hoslist)



fee_mis_ix = hosnames.index.isin(
    fee_mis_hoslist.index.astype('int'))

fee_mis_hosnames = hosnames[fee_mis_ix]

fee_mis_hoslist.index = fee_mis_hoslist.index.astype(int)
fee_mis_hos = pd.merge(fee_mis_hosnames,fee_mis_hoslist,
         left_index = True,right_index = True
         )

fee_mis_hos.sort(key=len(fee_mis_hos.name))

fee_mis_hosnames.index






col229.vericol.count(None)
col229['vericol'] == True
def colsums(path,xfiles):
    rlts = list()
    for xfile in xfiles:
        col_rep =  fd.read_dataframe(path+xfile)
        col = col_rep.iloc[:,1]
        col = col.astype('float')
        col[col > 410000000000] = 0
        colsum = col.sum()
        rlts.append(colsum)
    return rlts

def s_selvars(selvar):
        dfvars=''
        for var in selvar:
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars    
selvar = s_selvars(range(229,259))

colsum = colsums(path,xfiles[12:])


colsum = pd.Series(colsum)


[round(x,2) for x in colsum]
colsum.index = selvar.split(',')
colsum2 = colsum.drop(['x229','x230','x240',
             'x242','x243','x247'])
colsum2.sum()
colsum3 = colsum.drop(['x229','x230'])
colsum3.sum()
diff = colsum['x229'] - colsum2.sum()
round(diff,4)

varlist = pd.read_csv('/mnt/e/pyr/data/pyvarlist.csv')
varlist = varlist.chnnames
varlist = pd.Series(varlist)
varlist.index = selvar.split(',')


col_rep = fd.read_dataframe(path+xfiles[])
col = col_rep.iloc[:,1]
col = col.astype('float')
len(col[col.isnull()])

col.sum()

col_rep.ix[5288017]
col_rep[col_rep.x229 == "nan"]
col = col_rep.x229.astype('float')
col[col == 0].count()
rlts.append(0)
col[col.isnull()]
col[col == 0]

#c(-1,-2,-12,-14,-15,-19),]
col.isnull

