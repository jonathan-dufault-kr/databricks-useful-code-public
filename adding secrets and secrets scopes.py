# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Initial Data

# COMMAND ----------

from requests import get
from requests import post

# parameters you can find within the url above
api_base = "https://adb[redacted].net/api/2.0"
databricks_service = "redacted" # the numbers from the o=redacted part of the url
databricks_token = dbutils.secrets.get("secret_store_name","your_developer_token_name")

# what bucket to store the secretw in
scope = "my_secrets_bucket"

# secrets to be added
secrets_to_add_or_modify = {
  "key1" : "value1",
  # "key2": "value2",

}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a Scope (Run if Needed)

# COMMAND ----------

headers_for_listing_scopes = {
  "Authorization" : f"Bearer {databricks_token}",
}

headers_for_creating_scope = {
  "Content-Type" : "application/json",
  "Authorization" : f"Bearer {databricks_token}",
}


# create data for request
data = {
  "scope" : scope,
  "initial_manage_principal" : "users",
}


# create urls for checking if scope exists
# or to create
get_scopes_url = f"{api_base}/secrets/scopes/list?o=databricks_service"

get_scopes_response = get(get_scopes_url,headers=headers_for_listing_scopes)

# parse the response to identify which scopes exist
list_of_scopes = get_scopes_response.json()["scopes"]
scopes = [existing_scope["name"] for existing_scope in list_of_scopes]

# either add new scope or replace
# not strictly necessary since the create request will work either way
if scope not in scopes:
  post_url = f"{api_base}/api/2.0/secrets/scope/create?o={databricks_service}"
  scope_creation_request = post(post_url,headers=headers_for_creating_scope,data=data)
else:
  print(f"Scope {scope} already exists!")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Secrets

# COMMAND ----------


# create headers
headers = {
  "Content-Type" : "application/json",
  "Authorization" : f"Bearer {databricks_token}",
}

# construct url
post_url = f"{api_base}/secrets/put?o={databricks_service}"

# iterate through the secrets to add dictionary defined above
for key in secrets_to_add_or_modify:
  data = {
    "scope" : scope,
    "key" : key,
    "string_value" : secrets_to_add_or_modify[key]
  }
  secrets_modify_response = post(post_url,headers=headers,json=data)
  print(f"Secret: {key}, Status Code: {secrets_modify_response.status_code}")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Methods Available

# COMMAND ----------

# list the scopes in the workspace
scopes = dbutils.secrets.listScopes()
print("The scopes in this workspace are:")
[print(f"    {scope_name[0]}") for scope_name in scopes]
print()

# list secrets within a scope
sample_scope = scopes[0].name
secrets = dbutils.secrets.list(sample_scope)
print(f"The secrets in {sample_scope} are:")
[print(f"    {key_name[0]}") for key_name in secrets]
print()

# get scope for using in a script
sample_key = secrets[0][0]

sample_key_value =  dbutils.secrets.get(sample_scope,sample_key)

print(f"The command `dbutils.secrets.get('{sample_scope}','{sample_key}')` will return the key's value corresponding with {sample_key}")
print(f"The value for the key {sample_key} will show as {sample_key_value} when you print it")
print(f"But you can work with it like it is a string. For instance testing")
print(f"does the value for {sample_key} == 1? {sample_key_value=='1'}")


# COMMAND ----------

dbutils.secrets.help()
