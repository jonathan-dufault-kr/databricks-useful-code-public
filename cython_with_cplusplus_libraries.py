# Databricks notebook source
# MAGIC %load_ext cython

# COMMAND ----------

# MAGIC %sh apt install -y libgsl-dev 

# COMMAND ----------

# MAGIC %%cython -lgsl -lgslcblas -a -n my_output_module
# MAGIC #!python
# MAGIC #cython: language_level=3
# MAGIC #distutils: language=c++
# MAGIC from numpy cimport ndarray,import_array
# MAGIC from libc.math cimport log
# MAGIC import_array()
# MAGIC 
# MAGIC cdef extern from "/usr/include/gsl/gsl_randist.h":
# MAGIC     double gsl_ran_weibull_pdf(const double, const double, const double)
# MAGIC 
# MAGIC 
# MAGIC def weibull_pdf(double[:] x,float a,float b):
# MAGIC   cdef double return_var = 0
# MAGIC   cdef Py_ssize_t i,X_MAX = x.shape[0]
# MAGIC   for i in range(X_MAX):
# MAGIC     return_var += log(gsl_ran_weibull_pdf(x[i],a,b))
# MAGIC   return(return_var)

# COMMAND ----------

from numpy.random import rand
from scipy.stats import weibull_min
my_array = rand(500000)

# COMMAND ----------

# MAGIC %timeit weibull_min.logpdf(my_array,1,scale=0.8).sum()

# COMMAND ----------

# MAGIC %timeit weibull_pdf(my_array,0.8,1)
