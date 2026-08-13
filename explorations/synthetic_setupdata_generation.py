# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC catalog = "dac_test_dev"
# MAGIC schema = "schemaadb360dev"
# MAGIC db = "schemaadb360dev"
# MAGIC
# MAGIC spark.sql(f'USE CATALOG `{catalog}`')
# MAGIC spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{schema}`')
# MAGIC spark.sql(f'USE SCHEMA `{schema}`')
# MAGIC spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`data_streaming_files`')
# MAGIC volume_folder =  f"/Volumes/{catalog}/{schema}/data_streaming_files"
# MAGIC
# MAGIC try:
# MAGIC   dbutils.fs.ls(volume_folder+"/customers")
# MAGIC except:
# MAGIC   from pyspark.sql import functions as F
# MAGIC   from faker import Faker
# MAGIC   from collections import OrderedDict
# MAGIC   import uuid
# MAGIC   fake = Faker()
# MAGIC   import random
# MAGIC
# MAGIC   fake_firstname = F.udf(lambda _: fake.first_name())
# MAGIC   fake_lastname = F.udf(lambda _: fake.last_name())
# MAGIC   fake_email = F.udf(lambda _: fake.ascii_company_email())
# MAGIC   fake_date = F.udf(lambda _: fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
# MAGIC   fake_address = F.udf(lambda _: fake.address())
# MAGIC   operations = OrderedDict([("APPEND", 0.5),("DELETE", 0.1),("UPDATE", 0.3),(None, 0.01)])
# MAGIC   fake_operation = F.udf(lambda _: fake.random_elements(elements=operations, length=1)[0])
# MAGIC   fake_id = F.udf(lambda _: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)
# MAGIC
# MAGIC   df = spark.range(0, 100000).repartition(100)
# MAGIC   df = df.withColumn("id", fake_id(F.col("id")))
# MAGIC   df = df.withColumn("firstname", fake_firstname(F.col("id")))
# MAGIC   df = df.withColumn("lastname", fake_lastname(F.col("id")))
# MAGIC   df = df.withColumn("email", fake_email(F.col("id")))
# MAGIC   df = df.withColumn("address", fake_address(F.col("id")))
# MAGIC   df = df.withColumn("operation", fake_operation(F.col("id")))
# MAGIC   df_customers = df.withColumn("operation_date", fake_date(F.col("id")))
# MAGIC   df_customers.repartition(100).write.format("json").mode("overwrite").save(volume_folder+"/customers")

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC catalog = "dac_test_dev"
# MAGIC schema = "schemaadb360dev"
# MAGIC db = "schemaadb360dev"
# MAGIC
# MAGIC spark.sql(f'USE CATALOG `{catalog}`')
# MAGIC spark.sql(f'USE SCHEMA `{schema}`')
# MAGIC spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`data_streaming_files`')
# MAGIC volume_folder =  f"/Volumes/{catalog}/{schema}/data_streaming_files"
# MAGIC try:
# MAGIC   dbutils.fs.ls(volume_folder+"/orders")
# MAGIC except:
# MAGIC   from pyspark.sql import functions as F
# MAGIC   from faker import Faker
# MAGIC   from collections import OrderedDict
# MAGIC   import uuid
# MAGIC   fake = Faker()
# MAGIC   import random
# MAGIC
# MAGIC   fake_order_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC   fake_customer_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC   fake_order_date = F.udf(lambda _: fake.date_time_this_year().strftime("%m-%d-%Y %H:%M:%S"))
# MAGIC   fake_status = F.udf(lambda _: random.choice(["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]))
# MAGIC   fake_total = F.udf(lambda _: round(random.uniform(20, 5000), 2))
# MAGIC   fake_comment = F.udf(lambda _: fake.sentence())
# MAGIC
# MAGIC   df_orders = spark.range(0, 100000).repartition(100)
# MAGIC   df_orders = df_orders.withColumn("order_id", fake_order_id(F.col("id")))
# MAGIC   df_orders = df_orders.withColumn("customer_id", fake_customer_id(F.col("id")))
# MAGIC   df_orders = df_orders.withColumn("order_date", fake_order_date(F.col("id")))
# MAGIC   df_orders = df_orders.withColumn("status", fake_status(F.col("id")))
# MAGIC   df_orders = df_orders.withColumn("total_amount", fake_total(F.col("id")))
# MAGIC   df_orders = df_orders.withColumn("comment", fake_comment(F.col("id")))
# MAGIC   df_orders.repartition(100).write.format("json").mode("overwrite").save(volume_folder+"/orders")

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC catalog = "dac_test_dev"
# MAGIC schema = "schemaadb360dev"
# MAGIC db = "schemaadb360dev"
# MAGIC
# MAGIC
# MAGIC spark.sql(f'USE CATALOG `{catalog}`')
# MAGIC spark.sql(f'USE SCHEMA `{schema}`')
# MAGIC spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`data_streaming_files`')
# MAGIC volume_folder =  f"/Volumes/{catalog}/{schema}/data_streaming_files"
# MAGIC try:
# MAGIC   dbutils.fs.ls(volume_folder+"/products")
# MAGIC except:
# MAGIC   from pyspark.sql import functions as F
# MAGIC   from faker import Faker
# MAGIC   import uuid
# MAGIC   fake = Faker()
# MAGIC   import random
# MAGIC
# MAGIC   fake_product_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC   fake_name = F.udf(lambda _: fake.word().capitalize() + " " + fake.word().capitalize())
# MAGIC   fake_category = F.udf(lambda _: random.choice(["Electronics", "Clothing", "Home", "Sports", "Books", "Toys"]))
# MAGIC   fake_price = F.udf(lambda _: round(random.uniform(5, 2000), 2))
# MAGIC   fake_stock = F.udf(lambda _: random.randint(0, 1000))
# MAGIC   fake_description = F.udf(lambda _: fake.sentence())
# MAGIC   fake_brand = F.udf(lambda _: fake.company())
# MAGIC   fake_created_at = F.udf(lambda _: fake.date_time_this_year().strftime("%m-%d-%Y %H:%M:%S"))
# MAGIC   fake_updated_at = F.udf(lambda _: fake.date_time_this_year().strftime("%m-%d-%Y %H:%M:%S"))
# MAGIC
# MAGIC   df_products = spark.range(0, 50000).repartition(50)
# MAGIC   df_products = df_products.withColumn("product_id", fake_product_id(F.col("id")))
# MAGIC   df_products = df_products.withColumn("name", fake_name(F.col("id")))
# MAGIC   df_products = df_products.withColumn("category", fake_category(F.col("id")))
# MAGIC   df_products = df_products.withColumn("brand", fake_brand(F.col("id")))
# MAGIC   df_products = df_products.withColumn("price", fake_price(F.col("id")))
# MAGIC   df_products = df_products.withColumn("stock", fake_stock(F.col("id")))
# MAGIC   df_products = df_products.withColumn("description", fake_description(F.col("id")))
# MAGIC   df_products = df_products.withColumn("created_at", fake_created_at(F.col("id")))
# MAGIC   df_products = df_products.withColumn("updated_at", fake_updated_at(F.col("id")))
# MAGIC   df_products.repartition(50).write.format("json").mode("overwrite").save(volume_folder+"/products")

# COMMAND ----------

# MAGIC %pip install faker 
# MAGIC catalog = "dac_test_dev"
# MAGIC schema = "hrdata"
# MAGIC db = "hrdata"
# MAGIC
# MAGIC spark.sql(f'USE CATALOG `{catalog}`')
# MAGIC spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{schema}`')
# MAGIC spark.sql(f'USE SCHEMA `{schema}`')
# MAGIC spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`data_streaming_files`')
# MAGIC volume_folder =  f"/Volumes/{catalog}/{schema}/data_streaming_files"
# MAGIC try:
# MAGIC   dbutils.fs.ls(volume_folder+"/employees")
# MAGIC except:
# MAGIC   from pyspark.sql import functions as F
# MAGIC   from faker import Faker
# MAGIC   import uuid
# MAGIC   fake = Faker()
# MAGIC   import random
# MAGIC
# MAGIC   fake_employee_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC   fake_firstname = F.udf(lambda _: fake.first_name())
# MAGIC   fake_lastname = F.udf(lambda _: fake.last_name())
# MAGIC   fake_email = F.udf(lambda _: fake.ascii_company_email())
# MAGIC   fake_phone = F.udf(lambda _: fake.numerify(text="###-###-####"))
# MAGIC   fake_hire_date = F.udf(lambda _: fake.date_time_this_decade().strftime("%m-%d-%Y %H:%M:%S"))
# MAGIC   fake_salary = F.udf(lambda _: round(random.uniform(30000, 200000), 2))
# MAGIC   fake_department_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC   fake_workplace_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC
# MAGIC   # Mask PII fields: email, phone, firstname, lastname
# MAGIC   mask_udf = F.udf(lambda x: "MASKED" if x else None)
# MAGIC
# MAGIC   df_employees = spark.range(0, 20000).repartition(20)
# MAGIC   df_employees = df_employees.withColumn("employee_id", fake_employee_id(F.col("id")))
# MAGIC   df_employees = df_employees.withColumn("firstname", mask_udf(fake_firstname(F.col("id"))))
# MAGIC   df_employees = df_employees.withColumn("lastname", mask_udf(fake_lastname(F.col("id"))))
# MAGIC   df_employees = df_employees.withColumn("email", mask_udf(fake_email(F.col("id"))))
# MAGIC   df_employees = df_employees.withColumn("phone", mask_udf(fake_phone(F.col("id"))))
# MAGIC   df_employees = df_employees.withColumn("hire_date", fake_hire_date(F.col("id")))
# MAGIC   df_employees = df_employees.withColumn("salary", fake_salary(F.col("id")))
# MAGIC   df_employees = df_employees.withColumn("department_id", fake_department_id(F.col("id")))
# MAGIC   df_employees = df_employees.withColumn("workplace_id", fake_workplace_id(F.col("id")))
# MAGIC   df_employees.repartition(20).write.format("json").mode("overwrite").save(volume_folder+"/employees")
# MAGIC
# MAGIC try:
# MAGIC   dbutils.fs.ls(volume_folder+"/departments")
# MAGIC except:
# MAGIC   from pyspark.sql import functions as F
# MAGIC   from faker import Faker
# MAGIC   import uuid
# MAGIC   fake = Faker()
# MAGIC   import random
# MAGIC
# MAGIC   fake_department_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC   fake_department_name = F.udf(lambda _: fake.word().capitalize() + " Department")
# MAGIC   fake_manager_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC
# MAGIC   df_departments = spark.range(0, 100).repartition(5)
# MAGIC   df_departments = df_departments.withColumn("department_id", fake_department_id(F.col("id")))
# MAGIC   df_departments = df_departments.withColumn("department_name", fake_department_name(F.col("id")))
# MAGIC   df_departments = df_departments.withColumn("manager_id", fake_manager_id(F.col("id")))
# MAGIC   df_departments.repartition(5).write.format("json").mode("overwrite").save(volume_folder+"/departments")
# MAGIC
# MAGIC try:
# MAGIC   dbutils.fs.ls(volume_folder+"/workplaces")
# MAGIC except:
# MAGIC   from pyspark.sql import functions as F
# MAGIC   from faker import Faker
# MAGIC   import uuid
# MAGIC   fake = Faker()
# MAGIC   import random
# MAGIC
# MAGIC   fake_workplace_id = F.udf(lambda _: str(uuid.uuid4()))
# MAGIC   fake_location = F.udf(lambda _: fake.city() + ", " + fake.state_abbr())
# MAGIC   fake_address = F.udf(lambda _: fake.address())
# MAGIC   fake_capacity = F.udf(lambda _: random.randint(10, 1000))
# MAGIC
# MAGIC   df_workplaces = spark.range(0, 50).repartition(2)
# MAGIC   df_workplaces = df_workplaces.withColumn("workplace_id", fake_workplace_id(F.col("id")))
# MAGIC   df_workplaces = df_workplaces.withColumn("location", fake_location(F.col("id")))
# MAGIC   df_workplaces = df_workplaces.withColumn("address", fake_address(F.col("id")))
# MAGIC   df_workplaces = df_workplaces.withColumn("capacity", fake_capacity(F.col("id")))
# MAGIC   df_workplaces.repartition(2).write.format("json").mode("overwrite").save(volume_folder+"/workplaces")