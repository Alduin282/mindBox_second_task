from pyspark.sql  import SparkSession ,DataFrame

#возвращает датафрейм с набором всех пар "имя продукта - Имя категории"
def get_names_and_categories(df_products:DataFrame,
                             df_categories:DataFrame,
                             df_products_catergories:DataFrame) -> DataFrame:
    return df_products_catergories \
    .join(df_categories, df_categories["id"] == df_products_catergories["categoriesId"], "inner") \
    .join(df_products, df_products["id"] == df_products_catergories["productsId"], "right") \
    .select(df_products["name"].alias("Имя продукта"), df_categories["name"].alias("Имя категории"))

products = [
    (0,"N-2543",5156),
    (1,"K-1232",5321),
    (2,"K-2343",456),
    (3,"N-2333",3234),
    (4,"F-8844",29),
    (5,"NK-4522",9000)
]
products_columns = ["id", "name", "cost"]

categories = [
    (0,"K"),
    (1,"N")
]
categories_colums = ["id","name"]

products_categories = [
    (0,0,1),
    (1,1,0),
    (2,2,0),
    (3,3,1),
    (4,4,None),
    (5,5,1),
    (6,5,0)
]
products_categories_colums = ["id","productsId","categoriesId"]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Task").getOrCreate()

    df_products = spark.createDataFrame(products, products_columns)
    df_categories = spark.createDataFrame(categories,categories_colums)
    df_products_catergories = spark.createDataFrame(products_categories, products_categories_colums)

    df_result = get_names_and_categories(df_products,df_categories,df_products_catergories)
    df_result.show()
    # Вывод
    # +------------+-------------+
    # |Имя продукта|Имя категории|
    # +------------+-------------+
    # |      N-2543|            N|
    # |      K-1232|            K|
    # |      K-2343|            K|
    # |      N-2333|            N|
    # |      F-8844|         null|
    # |     NK-4522|            N|
    # |     NK-4522|            K|
    # +------------+-------------+