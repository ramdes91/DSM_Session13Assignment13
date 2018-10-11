
# coding: utf-8

# Read the following data set:
# https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data

# In[1]:


import numpy as np
import pandas as pd
import sqlite3 
import pandasql 
import sqlalchemy
from sqlalchemy import orm  


# In[2]:


adult_data= pd.read_csv("https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data",header=None,index_col=False)
lst_col=['AGE', 
        'WORKCLASS', 
        'FNLWGT', 
        'EDUCATION', 
        'EDUCATION_NUM', 
        'MARITAL_STATUS', 
        'OCCUPATION', 
        'RELATIONSHIP', 
        'RACE', 
        'SEX', 
        'CAPITAL_GAIN', 
        'CAPITAL_LOSS', 
        'HOURS_PER_WEEK', 
        'NATIVE_COUNTRY', 
        'GT50_OR_LT50K']

adult_data.columns=lst_col
adult_data.head()

Task1. Create an sqlalchemy engine using a sample from the data set
# In[3]:


engine = sqlalchemy.create_engine('sqlite:///:memory:',echo=False)
conn = engine.connect()

engine.execute('''
    CREATE TABLE IF NOT EXISTS ADULTS (
         AGE             INTEGER,
         WORKCLASS       VARCHAR(100),
         FNLWGT          INTEGER,
         EDUCATION       VARCHAR(100),
         EDUCATION_NUM   INTEGER,         
         MARITAL_STATUS  VARCHAR(100),         
         OCCUPATION      VARCHAR(100),
         RELATIONSHIP    VARCHAR(100),
         RACE            VARCHAR(100),
         SEX             VARCHAR(20),
         CAPITAL_GAIN    INTEGER,
         CAPITAL_LOSS    INTEGER,
         HOURS_PER_WEEK  INTEGER,
         NATIVE_COUNTRY  VARCHAR(100),
         GT50_OR_LT50K   VARCHAR(20))
''')

adult_sample= adult_data
dict_adult = adult_sample.to_dict(orient='records')

metadata = sqlalchemy.schema.MetaData(bind=engine)
ADULT_TAB = sqlalchemy.Table('ADULTS', metadata, autoload=True)


Session = orm.sessionmaker(bind=engine)
session = Session()
conn.execute(ADULT_TAB.insert(), dict_adult)
session.commit()
session.close()

sql_select="SELECT * FROM ADULTS LIMIT 5;"
conn=engine
result_adult_data=pd.read_sql_query(sql_select, conn) 
result_adult_data

Task2. Write two basic update queries
# In[4]:


sql_update1 = " UPDATE ADULTS SET MARITAL_STATUS='Married' where MARITAL_STATUS like ' Married%';"
engine.execute(sql_update1)

sql_update2 = " UPDATE ADULTS SET NATIVE_COUNTRY='Not Known' where NATIVE_COUNTRY =' ?';"
engine.execute(sql_update2)

sql_select="SELECT * FROM ADULTS WHERE (NATIVE_COUNTRY='Not Known') OR (MARITAL_STATUS='Married') LIMIT 10;"
conn=engine
result_adult_data=pd.read_sql_query(sql_select, conn) 
result_adult_data

Task3. Write two delete queries
# In[5]:


## For Marital_Status

sql_delete= "DELETE FROM ADULTS WHERE MARITAL_STATUS =' ?' ;"
engine.execute(sql_delete)

sql_select="SELECT * FROM ADULTS WHERE MARITAL_STATUS =' ?' ;"
conn=engine
result_adult_data=pd.read_sql_query(sql_select, conn) 
result_adult_data


# In[6]:


## For OCCUPATION
sql_delete= "DELETE FROM ADULTS WHERE OCCUPATION =' ?' ;"
engine.execute(sql_delete)

sql_select="SELECT * FROM ADULTS WHERE OCCUPATION =' ?' ;"
conn=engine
result_adult_data=pd.read_sql_query(sql_select, conn) 
result_adult_data

Task4. Write two filter queries
# In[7]:


sql_select="SELECT * FROM ADULTS where AGE <=18 AND SEX=' Male';"
conn=engine
result_adult_data=pd.read_sql_query(sql_select, conn) 
result_adult_data.head()


# In[8]:


sql_select="SELECT * FROM ADULTS"
sql_select=sql_select+ " WHERE SEX=' Female' "
sql_select=sql_select+ " AND MARITAL_STATUS=' Never-married'"

conn=engine
result_adult_data=pd.read_sql_query(sql_select, conn) 
result_adult_data.head()


# In[ ]:


Task5. Write two function queries


# In[9]:


sql_select="SELECT MARITAL_STATUS, COUNT(*) AS COUNT_MARRIED FROM ADULTS GROUP BY MARITAL_STATUS;"
conn=engine
result_adult_data=pd.read_sql_query(sql_select, conn) 
result_adult_data


# In[10]:


sql_select="SELECT SEX, COUNT(*) AS COUNT_GENDER FROM ADULTS GROUP BY SEX;"
conn=engine
result_adult_data=pd.read_sql_query(sql_select, conn) 
result_adult_data

