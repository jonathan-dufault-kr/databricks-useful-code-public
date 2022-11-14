# Databricks notebook source
# temporarily update environment variables

from os import environ

def set_environment_variables(*args,**kwargs):
  # record the environment variables to update when running
  environment_variables = kwargs
  backup_environment_variables = {key:environ.get(key) for key in environment_variables if key in environ}
  
  def decorator(func,*args,**kwargs):
    def inner(*args,**kwargs):
      # update environment variables
      environ.update(environment_variables)
      
      # run the function
      result = func(*args,**kwargs)

      # return environment variables to normal
      [environ.pop(key) for key in environment_variables]
      environ.update(backup_environment_variables)
      return(result)
    return(inner)  

  # the decorator function returned after setting parameters
  return(decorator)


# example

@set_environment_variables(http_proxy="http://proxy...")
def my_func():
  #my_func_code

# COMMAND ----------

my_func()
