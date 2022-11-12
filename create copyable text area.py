# Databricks notebook source
sql_text="sample_text"

displayHTML(f"""
<p><textarea class='sql-query'>{sql_text}</textarea></p>
<p><button class='button'>click here to copy</button></p> <script>
   var button = document.querySelector('.button');
   var sqlQuery = document.querySelector('.sql-query')
   console.log("button")
   button.addEventListener('click', function(event) {{
   sqlQuery.select();
   document.execCommand('copy');
   }} )
</script>
""")

# COMMAND ----------


