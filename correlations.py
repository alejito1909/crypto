#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 18:13:43 2017

@author: steel
"""


#####  loading required packages ####

from string import ascii_letters
import numpy 
import pandas as pd
import json
import urllib
import urllib.request
import datetime
from time import mktime
import mysql.connector
from sqlalchemy import create_engine


#####  defining functions ####

# convert an array of values into a dataset matrix
def create_dataset(dataset, look_back=1):
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back)]
        dataX.append(a)
        dataY.append(dataset[i + look_back][0])
    return numpy.array(dataX), numpy.array(dataY)


# putting data into dataframes:
def filldf(dataframe,column,source):
    for i in range(len(dataframe)):
        dataframe[column][i] = source[i][column]
    return dataframe

    


########################################
# connecting to cmc table in enroyd db #
########################################

engine = create_engine('mysql+mysqlconnector://', echo=False)

    
df = pd.read_sql( '''select distinct 
                                    c.Symbol
                                   
                     from c c
                    ''',engine)
print (df.head())
print (df.tail())

shitcoins = list(df['Symbol'])    
    





#####  Loading data  ####

df_concat = pd.DataFrame(numpy.zeros(720))


#####for loop starts here

columnnames = []


########################################
# connecting to cmc table in enroyd db #
########################################
df = pd.read_sql( '''select distinct c.Coin_Id
                                   , c.Symbol
                                   , c.Price_USD
                                   , c.24h_Volume_USD
                                   , c.Create_Date
                     from c c
                     inner join (select count(distinct Coin_Id) as 'Counts'
                                      , Symbol
                                 from c
                                 group by Symbol)a on c.Symbol = a.Symbol
                     inner join (select distinct Symbol
                                 from c
                                 order by Create_Date desc limit 5000)b on c.Symbol = b.Symbol
                     inner join cd cd on cd.Symbol = c.Symbol
                     where a.Counts >= 4320
                     and c.Create_Date >= date_add(CURRENT_DATE(), interval - 30 day)
                     order by c.Symbol, c.Coin_Id''',engine)
print (df.head())
print (df.tail())



#####  Filling correlations dataframe  ####

for i in range(len(shitcoins)):
    print(shitcoins[i])
    try:
        query = 'Symbol == "'+ shitcoins[i] + '"'
        interim_DF = df.query(query)

        interim_DF = interim_DF.reset_index()
        interim_DF = interim_DF[interim_DF.index % 6 == 0]
        interim_DF = interim_DF.iloc[-720:,3]
        interim_DF = interim_DF.reset_index()
        df_concat = pd.concat([df_concat.reset_index(drop=True),interim_DF['Price_USD']],axis=1, )
        
        columnnames.append(shitcoins[i])
    
    except:
        pass
    
 





df_concat = df_concat.iloc[:,1:]

df_concat.columns = columnnames


#eliminate columns with na

df_concat = df_concat.dropna(axis=1)    




#####  Compute correlation Matrix  ####
date = datetime.datetime.now()

corr = df_concat.corr()
#print(corr)

new_df = pd.DataFrame(numpy.zeros((len(df_concat.columns)*len(df_concat.columns),7)))

new_df.columns = ['symbol','correlator','correlation','Create_Date','Create_User','Update_Date','Update_User']

new_df['correlator'] = list(df_concat.columns)*len(df_concat.columns)

new_df['correlation'] = corr.stack().reset_index( drop=True)



for i in range(len(new_df)):
    print(i/len(new_df))
    new_df['symbol'][i] = corr.stack().index[i][0]

new_df['Create_Date'] = date
new_df['Create_User'] = '2'
new_df['Update_User'] = '2'
new_df['Update_Date'] = date

new_df = new_df.query('symbol != correlator')



#rank loop for each shitcoin

new_df['min_max_correlators'] = numpy.zeros(len(new_df))

for shitcoin in df_concat.columns:
    print(shitcoin)

    subset= new_df[new_df.symbol == shitcoin]
    

    #looping through max correlators
    correlators_max = subset.nlargest(3, 'correlation')['correlator']
    max_index=1    
    for correlators in correlators_max:
        #print(correlators)
        new_df['min_max_correlators'].loc[(new_df.symbol == shitcoin) & (new_df.correlator == correlators)] = max_index
        max_index +=1


    #looping through minn correlators
    correlators_min = subset.nsmallest(3, 'correlation')['correlator']
    min_index=-3    
    for correlators in correlators_min:
        #print(correlators)
        new_df['min_max_correlators'].loc[(new_df.symbol == shitcoin) & (new_df.correlator == correlators)] = min_index
        min_index +=1        
        
 
    


#loading data into database
#engine = create_engine('mysql+mysqlconnector://', echo=False)
engine = create_engine('mysql+mysqlconnector://', echo=False)

new_df.to_sql(name='correlations', con=engine, if_exists = 'append', index=False)


print('Done!')
