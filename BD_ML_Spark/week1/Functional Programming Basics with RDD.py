#!/usr/bin/env python
# coding: utf-8

# This notebook is designed to run in a IBM Watson Studio default runtime (NOT the Watson Studio Apache Spark Runtime as the default runtime with 1 vCPU is free of charge). Therefore, we install Apache Spark in local mode for test purposes only. Please don't use it in production.
# 
# In case you are facing issues, please read the following two documents first:
# 
# https://github.com/IBM/skillsnetwork/wiki/Environment-Setup
# 
# https://github.com/IBM/skillsnetwork/wiki/FAQ
# 
# Then, please feel free to ask:
# 
# https://coursera.org/learn/machine-learning-big-data-apache-spark/discussions/all
# 
# Please make sure to follow the guidelines before asking a question:
# 
# https://github.com/IBM/skillsnetwork/wiki/FAQ#im-feeling-lost-and-confused-please-help-me
# 
# 
# If running outside Watson Studio, this should work as well. In case you are running in an Apache Spark context outside Watson Studio, please remove the Apache Spark setup in the first notebook cells.

# In[1]:


from IPython.display import Markdown, display
def printmd(string):
    display(Markdown('# <span style="color:red">'+string+'</span>'))


if ('sc' in locals() or 'sc' in globals()):
    printmd('<<<<<!!!!! It seems that you are running in a IBM Watson Studio Apache Spark Notebook. Please run it in an IBM Watson Studio Default Runtime (without Apache Spark) !!!!!>>>>>')


# In[2]:


get_ipython().system('pip install pyspark==2.4.5')


# In[3]:


try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    printmd('<<<<<!!!!! Please restart your kernel after installing Apache Spark !!!!!>>>>>')


# In[4]:


sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession     .builder     .getOrCreate()


# Welcome to exercise two of “Apache Spark for Scalable Machine Learning on BigData”. In this exercise you’ll apply the basics of functional and parallel programming. 
# 
# Again, please use the following two links for your reference:
# https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD
# https://spark.apache.org/docs/latest/rdd-programming-guide.html
# 
# Let’s actually create a python function which decides whether a value is greater than 50 (True) or not (False).

# In[4]:


def gt50(i):
    if i > 50:
        return True
    else:
        return False


# In[5]:


print(gt50(4))
print(gt50(51))


# Let’s simplify this function

# In[6]:


def gt50(i):
    return i > 50


# In[7]:


print(gt50(4))
print(gt50(51))


# Now let’s use the lambda notation to define the function.

# In[7]:


gt50 = lambda i: i > 50


# In[9]:


print(gt50(4))
print(gt50(51))


# In[8]:


#let's shuffle our list to make it a bit more interesting
from random import shuffle
l = list(range(100))
shuffle(l)
rdd = sc.parallelize(l)


# Let’s filter values from our list which are equals or less than 50 by applying our “gt50” function to the list using the “filter” function. Note that by calling the “collect” function, all elements are returned to the Apache Spark Driver. This is not a good idea for BigData, please use “.sample(10,0.1).collect()” or “take(n)” instead.

# In[9]:


rdd.filter(gt50).sample(10,0.1).collect()


# We can also use the lambda function directly.

# In[10]:


rdd.filter(lambda i: i > 50).take(10)


# Let’s consider the same list of integers. Now we want to compute the sum for elements in that list which are greater than 50 but less than 75. Please implement the missing parts. 

# In[13]:


rdd.filter(lambda x: x > 50).filter(lambda x: x < 75).reduce(lambda a,b : a+b)


# You should see "1500" as answer. Now we want to know the sum of all elements. Please again, have a look at the API documentation and complete the code below in order to get the sum.

# In[12]:


rdd.reduce(lambda a,b : a+b)


# In[16]:


rdd.map(gt50).take(10)


# In[17]:


rdd.filter(gt50).take(10)

