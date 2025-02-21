# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`default`.`vgchartz_2024`;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Este es un Analisis General de Ventas
# MAGIC
# MAGIC

# COMMAND ----------

# Cargar el DataFrame de Spark en una variable
df_spark = spark.sql("SELECT * FROM `workspace`.`default`.`vgchartz_2024`")

# Convertir el DataFrame de Spark a pandas
df = df_spark.toPandas()


# COMMAND ----------

import pandas as pd

# Obtener el juego con mayor ventas
top_game = df.loc[df['total_sales'].idxmax()]
print("The top game is: \n" + str(top_game[["title", "console", "genre", "total_sales"]]))

# COMMAND ----------

# Obtener los 10 juegos con mayor ventas
top_10_games = df.nlargest(10, "total_sales")[["title", "total_sales", "console", "genre"]]
print("The top 10 games are:")
print(top_10_games)


# COMMAND ----------

# Obtener las ventas por región
region_sales = df[["na_sales", "jp_sales", "pal_sales", "other_sales"]].sum()
print("Regional sales are:")
print(region_sales)

# COMMAND ----------

# Porcentaje de ventas por región
total_global_sales = df["total_sales"].sum()
region_sales_percentage = (region_sales / total_global_sales) * 100
print("Regional sales percentage are:")
print(region_sales_percentage)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Comparación entre Consolas

# COMMAND ----------

# Consola con más juegos
console_count = df["console"].value_counts()
most_games_console = console_count.idxmax()
print(f"The most games console is: {most_games_console} ({console_count.max():.2f} milion units)")

# COMMAND ----------

# Consola con mas ventas
console_sales = df.groupby("console")["total_sales"].sum().sort_values(ascending=False)
top_sales_console = console_sales.idxmax()
print(f"The console with the highest total sales is: {top_sales_console} ({console_sales.max():.2f} millions)")

# COMMAND ----------

# Consola con mejor calificación (Se basa en la calificación de los videojuegos)
console_critic_score = df.groupby("console")["critic_score"].mean().dropna().sort_values(ascending=False)
best_reviewed_console = console_critic_score.idxmax()
print(f"The console with the highest average critic score is: {best_reviewed_console} ({console_critic_score.max():.2f})")

# COMMAND ----------

import matplotlib.pyplot as plt

df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce") 
df["year"] = df["release_date"].dt.year

sales_by_year_console = df.groupby(["year", "console"])["total_sales"].sum().unstack()

print("Evolution of game sales by console over time:")
print(sales_by_year_console)

sales_by_year_console.plot(kind="line", figsize=(12, 6), marker="o")
plt.title("Evolution of game sales by console over time")
plt.xlabel("Year")
plt.ylabel("Total sales (millions)")
plt.legend(title="Console")
plt.grid(True)
plt.show()

# COMMAND ----------


