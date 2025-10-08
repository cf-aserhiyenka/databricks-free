from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg

# 1. Inicjalizacja SparkSession
spark = SparkSession.builder.appName("ComplexExplainDemo").getOrCreate()

# 2. Tworzenie pierwszego DataFrame'u (Dane Klientów)
data_a = [
    (1, "Anna", "Warszawa", 50000),
    (2, "Piotr", "Kraków", 60000),
    (3, "Marta", "Wrocław", 75000),
    (4, "Krzysztof", "Warszawa", 50000),
    (5, "Ewa", "Gdańsk", 80000)
]
df_klienci = spark.createDataFrame(data_a, ["ID_Klienta", "Imię", "Miasto", "Zarobki"])

# 3. Tworzenie drugiego DataFrame'u (Dane Zamówień)
data_b = [
    (1, "Produkt A", 1000, 2),
    (2, "Produkt B", 1500, 1),
    (1, "Produkt C", 200, 5),
    (4, "Produkt D", 800, 3),
    (5, "Produkt A", 1000, 1)
]
df_zamowienia = spark.createDataFrame(data_b, ["ID_Klienta", "Nazwa_Produktu", "Cena_Jednostkowa", "Ilość"])

# 4. Łańcuch Złożonych Operacji (Complex DataFrame)
df_wynik = df_klienci.alias("k") \
    .join(df_zamowienia.alias("z"), col("k.ID_Klienta") == col("z.ID_Klienta"), "inner") \
    .withColumn("Wartość_Zamówienia", col("z.Cena_Jednostkowa") * col("z.Ilość")) \
    .filter(col("k.Miasto") == "Warszawa") \
    .groupBy("k.Imię", "k.Miasto") \
    .agg(
        avg("Wartość_Zamówienia").alias("Średnia_Wartość_Zamówienia"),
        avg("k.Zarobki").alias("Średnie_Zarobki")
    )