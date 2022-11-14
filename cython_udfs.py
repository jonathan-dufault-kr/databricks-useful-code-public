# Databricks notebook source
# MAGIC %md 
# MAGIC # Creating Cython Functions for UDFs in DataBricks
# MAGIC 
# MAGIC ## Intro
# MAGIC 
# MAGIC For cython you will end up with a file *.so that you can make spark aware of, and then import on the cluster nodes. You cannot change the name of the file or else python will not be able to import it.
# MAGIC 
# MAGIC The area below is for development. 
# MAGIC 
# MAGIC When you're done with development and the library is ready to be deployed in production, the notebooks that need
# MAGIC the udf function will look possibly like:
# MAGIC 
# MAGIC ```python
# MAGIC sc.addFile(path_to_the_so_file_name.so)
# MAGIC import so_file_name 
# MAGIC spark.udf.register("whatICallIt",udf_function_from_so_library)
# MAGIC ```

# COMMAND ----------

# MAGIC %load_ext cython

# COMMAND ----------

# MAGIC %%cython 
# MAGIC # can use -n name to make a human readable name for the final module
# MAGIC # you can include external libraries not packaged with cython
# MAGIC # but you're responsible to making those available on the cluster nodes
# MAGIC # either through `apt install lib-whatever` or just 
# MAGIC # copying those dependent libraries over too
# MAGIC 
# MAGIC def fib_mapper_cython(n):
# MAGIC     '''
# MAGIC     Return the first fibonnaci number > n.
# MAGIC     '''
# MAGIC     cdef int a = 0
# MAGIC     cdef int b = 1
# MAGIC     cdef int j = int(n)
# MAGIC     while b<j:
# MAGIC         a, b  = b, a+b
# MAGIC     return b

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are a couple ways to register the udf. this one first puts the file in dbfs and registers it to the cython udf. the second method just puts the file from the local node into the spark context.
# MAGIC 
# MAGIC I believe the second should always work since I believe our notebook will always be running on the master. 

# COMMAND ----------

# method 1

from shutil import copy as shutil_copy
from inspect import getmodule

# fill these in as needed
udf_function = fib_mapper_cython #your function
udf_module_storage_folder = "FileStore/compiled_python_modules" # where you want the module in dbfs

# extract information about the compiled module
udf_module_file = getmodule(udf_function).__file__
udf_module_file_name = udf_function.__module__

# copy the file from the ipythondir cached directory to the dbfs so the cluster nodes can access it
dbfs_file_location = f"/dbfs/{udf_module_storage_folder}/{udf_module_file_name}.so" # you can't change the name of the library
shutil_copy(udf_module_file,dbfs_file_location)
sc.addFile(f"dbfs:/{udf_module_storage_folder}/{udf_module_file_name}.so")

# register the udf
spark.udf.register("my_udf_method_1",udf_function)

# COMMAND ----------

#method 2
from inspect import getmodule

sc.addFile(getmodule(fib_mapper_cython).__file__)
spark.udf.register("my_udf_method_2",fib_mapper_cython)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- example that shows how to use the new udf
# MAGIC select my_udf_method_1(100) as method_1 ,my_udf_method_2(100) as method_2

# COMMAND ----------


