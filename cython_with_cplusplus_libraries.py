# Databricks notebook source
# MAGIC %load_ext cython

# COMMAND ----------

# MAGIC %sh apt install -y libgsl-dev 

# COMMAND ----------

# MAGIC %%cython -lgsl -lgslcblas
# MAGIC #!python
# MAGIC #cython: language_level=3
# MAGIC #distutils: language=c++
# MAGIC from numpy cimport ndarray,import_array
# MAGIC from libc.math cimport log
# MAGIC from cython cimport boundscheck
# MAGIC import_array()
# MAGIC 
# MAGIC cdef extern from "/usr/include/gsl/gsl_randist.h":
# MAGIC     double gsl_ran_weibull_pdf(const double, const double, const double)
# MAGIC 
# MAGIC @boundscheck(False)
# MAGIC def weibull_pdf(ndarray[double] x,double shape,double scale):
# MAGIC   cdef double return_var = 0
# MAGIC   cdef Py_ssize_t i,X_MAX = x.shape[0]
# MAGIC   for i in range(X_MAX):
# MAGIC     return_var += log(gsl_ran_weibull_pdf(x[i],shape,scale))
# MAGIC   return(return_var)

# COMMAND ----------

# MAGIC %%cython --lib gsl --lib gslcblas  --compile-args=-fopenmp --link-args=-fopenmp 
# MAGIC #!python
# MAGIC #cython: language_level=3
# MAGIC #distutils: language=c++
# MAGIC 
# MAGIC from libc.math cimport log
# MAGIC from cython.parallel import prange
# MAGIC from cython cimport boundscheck,wraparound
# MAGIC 
# MAGIC 
# MAGIC cdef extern from "/usr/include/gsl/gsl_randist.h":
# MAGIC     double gsl_ran_weibull_pdf(double, double, double) nogil
# MAGIC 
# MAGIC @boundscheck(False)
# MAGIC @wraparound(False)
# MAGIC def pweibull_pdf(double[:] x,double shape,double scale):
# MAGIC   cdef double return_var = 0
# MAGIC   cdef Py_ssize_t i,X_MAX = x.shape[0]
# MAGIC   with nogil:
# MAGIC     for i in prange(X_MAX):
# MAGIC       return_var += log(gsl_ran_weibull_pdf(x[i],shape,scale))
# MAGIC   return(return_var)

# COMMAND ----------

from numpy.random import rand
from scipy.stats import weibull_min
my_array = rand(int(1e7))

# COMMAND ----------

# MAGIC %timeit weibull_min.logpdf(my_array,4.1,scale=0.6).sum() # best pythonic I can think of

# COMMAND ----------

# MAGIC %timeit weibull_pdf(my_array,4.1,0.6) # basic cython without parallelization

# COMMAND ----------

# MAGIC %timeit pweibull_pdf(my_array,4.1,0.6) # using nogil parallelization
