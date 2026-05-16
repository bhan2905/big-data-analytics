from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, year, month, \
avg, datediff, max, min, when, months_between, sum as _sum, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# load data and preprocess
def load_data():
    customers = spark.read.csv("hdfs://localhost:9000/bvan2925/data/Customer_List.csv", header=True, inferSchema=True, sep=";")
    orders = spark.read.csv("hdfs://localhost:9000/bvan2925/data/Orders.csv", header=True, inferSchema=True, sep=";")
    order_items = spark.read.csv("hdfs://localhost:9000/bvan2925/data/Order_Items.csv", header=True, inferSchema=True, sep=";")
    reviews = spark.read.csv("hdfs://localhost:9000/bvan2925/data/Order_Reviews.csv", header=True, inferSchema=True, sep=";")
    products = spark.read.csv("hdfs://localhost:9000/bvan2925/data/Products.csv", header=True, inferSchema=True, sep=";")

    # filter out NULLs and outliers for Review_Score
    reviews = reviews.filter(
        col("Review_Score").isNotNull() & (col("Review_Score") >= 1) & (col("Review_Score") <= 5)
    )
    
    return customers, orders, order_items, reviews, products

def get_statistics(orders, customers, order_items):
    '''Get overall statistics for total orders, customers annd sellers'''
    
    print("\n- Ex2: Overall statistics for total orders, customers and sellers -")

    # total orders and customers from Orders table
    orders_summary = orders.select(
        count("Order_ID").alias("Total_Orders"),
        countDistinct("Customer_Trx_ID").alias("Unique_Customers")
    )
    
    # total sellers from Order_Items table
    sellers_summary = order_items.select(countDistinct("Seller_ID").alias("Unique_Sellers"))

    orders_summary.crossJoin(sellers_summary).show()

def analyze_orders_by_country(orders, customers):
    '''Analyze order count by customer country and sort in descending order'''

    print("\n- Ex3: Order count by Country (Descending) -")
    
    # join Orders with Customer_List
    orders.join(customers, on="Customer_Trx_ID", how="inner") \
             .groupBy("Customer_Country") \
             .agg(count("Order_ID").alias("Order_Count")) \
             .orderBy(col("Order_Count").desc()) \
             .show()

def analyze_orders_by_time(orders):
    '''Analyze order count by month and year (Year Asc, Month Desc)'''

    print("\n- Ex4: Order count by Time (Year Asc, Month Desc) -")

    orders.withColumn("Year", year("Order_Purchase_Timestamp")) \
             .withColumn("Month", month("Order_Purchase_Timestamp")) \
             .groupBy("Year", "Month") \
             .agg(count("Order_ID").alias("Order_Count")) \
             .orderBy(col("Year").asc(), col("Month").desc()) \
             .show()

def analyze_reviews(reviews):
    '''Analyze average review scores and distribution of review scores (1-5)'''
    print("\n- Ex5: Review score statistics -")
    
    # count the number of reviews for each score level (1-5)
    print("\n Review Distribution (1 to 5 Stars):")
    reviews.groupBy("Review_Score") \
              .agg(count("Review_ID").alias("Review_Count")) \
              .orderBy("Review_Score") \
              .show()
              
    # calculate overall average review score
    avg_score = reviews.select(avg("Review_Score").alias("Average_Score")).collect()[0]["Average_Score"]
    print("\n Overall Average Score:", avg_score)

def analyze_revenue(orders, order_items, products):
    '''Analyze total revenue in 2024 by product category, sorted in descending order'''
    print("\n- Ex6: Total revenue in 2024 by Product Category -")
    
    # filter orders in 2024
    orders_2024 = orders.filter(year(col("Order_Purchase_Timestamp")) == 2024)
    
    # join the 2024 orders with Order_Items and Products tables
    joined = orders_2024 \
        .join(order_items, on="Order_ID", how="inner") \
        .join(products, on="Product_ID", how="inner")
    
    # calculate revenue per item
    revenue = joined.withColumn("Item_Revenue", col("Price") + col("Freight_Value"))
    
    # aggregate total revenue by product category name
    result = revenue.groupBy("Product_Category_Name") \
        .agg(_sum("Item_Revenue").alias("Total_Revenue")) \
        .orderBy(col("Total_Revenue").desc())

    result.show(truncate=False)

def analyze_products(df_order_items, df_reviews, df_products):
    '''Analyze the top-selling product and the average review scores for all products'''
    print("\n- Ex7: Top selling product and product average review scores -")
    
    # identify the top-selling product
    sales_count = order_items.groupBy("Product_ID") \
                                   .agg(count("Order_Item_ID").alias("Total_Items"))
    
    top_product = sales_count.orderBy(col("Total_Items").desc()).first()
    
    if top_product:
        print(f"Top selling product: Product ID = {top_product['Product_ID']} | Total Items = {top_product['Total_Items']}")
    
    # calculate the average review score for products
    product_reviews = order_items \
        .join(df_reviews, on="Order_ID", how="inner") \
        .groupBy("Product_ID") \
        .agg(avg("Review_Score").alias("Average_Review_Score"))
    
    result = sales_count \
        .join(product_reviews, on="Product_ID", how="left") \
        .select(
            "Product_ID",
            "Total_Items", 
            "Average_Review_Score"
        ) \
        .orderBy(col("Average_Review_Score").desc())
    
    print("\nProduct leaderboard:")
    result.show(truncate=False)

def analyze_delivery(orders):
    '''Analyze the actual delivery date and the expected delivery date'''
    print("\n- Ex8: Delivery Performance -")
    
    # calculate the difference between actual delivery date and expected delivery date
    delivery = orders.filter(
        col("Order_Delivered_Customer_Date").isNotNull() & 
        col("Order_Estimated_Delivery_Date").isNotNull()
    )

    performance = delivery.withColumn(
        "Delivery_Delay_Days", 
        datediff(col("Order_Delivered_Customer_Date"), col("Order_Estimated_Delivery_Date"))
    )

    # classify deliveries based on the delay days
    performance = performance.withColumn(
        "Shipping_Status",
        when(col("Delivery_Delay_Days") > 0, "Delayed")
        .otherwise("On-Time")
    )

    performance.groupBy("Shipping_Status") \
             .agg(
                 count("Order_ID").alias("Order_Count"),
                 avg("Delivery_Delay_Days").alias("Average_Delay_Days"),
                 max("Delivery_Delay_Days").alias("Max_Delay_Days"),
                 min("Delivery_Delay_Days").alias("Min_Delay_Days")
             ).show(truncate=False)

def analyze_customers(orders, customers, order_items):
    '''Analyze the customer behavior on orders, value of orders and frequency of orders'''
    print("\n- Ex9: Customer Behavior -")

    order_items = order_items.withColumn("Total_Value", col("Price") + col("Freight_Value")) \
        .groupBy("Order_ID") \
        .agg(
            _sum("Total_Value").alias("Order_Value")
        )

    customer_orders = orders.join(customers, on="Customer_Trx_ID", how="inner") \
        .join(order_items, on="Order_ID", how="inner") \
        .groupBy("Customer_Trx_ID") \
        .agg(
            countDistinct("Order_ID").alias("Total_Orders"),
            avg("Order_Value").alias("Average_Order_Value"),
            min("Order_Purchase_Timestamp").alias("First_Order_Date"),
            max("Order_Purchase_Timestamp").alias("Last_Order_Date")
        )
    
    customer_orders = customer_orders.withColumn(
        "Active_Months",
        months_between(col("Last_Order_Date"), col("First_Order_Date")) + 1
    )

    # frequency by active months
    customer_orders = customer_orders.withColumn(
        "Purchase_Frequency",
        (col("Total_Orders") / col("Active_Months"))
    )

    customer_orders.select(
        "Customer_Trx_ID",
        "Total_Orders",
        "Average_Order_Value",
        "Active_Months",
        "Purchase_Frequency"
    ).show(truncate=False)


def rank_sellers(order_items):
    '''Analyze the seller performance based on total revenue and unique order count'''
    print("\n- Ex10: Seller Performance Ranking -")
    
    # calculate total revenue and order count for each seller
    seller = order_items.withColumn("Item_Revenue", col("Price") + col("Freight_Value")) \
        .groupBy("Seller_ID") \
        .agg(
            _sum("Item_Revenue").alias("Total_Revenue"),
            countDistinct("Order_ID").alias("Total_Orders")
        )
        
    # rank sellers by Total_Revenue descending
    seller_window = Window.orderBy(col("Total_Revenue").desc())
    seller_ranked = seller.withColumn("Seller_Rank", dense_rank().over(seller_window))

    seller_ranked.select(
        "Seller_Rank",
        "Seller_ID",
        "Total_Revenue",
        "Total_Orders"
    ).orderBy("Seller_Rank").show(truncate=False)

if __name__ == "__main__":

    customers, orders, order_items, reviews, products = load_data()

    print("\n- Ex1: Schema information-")
    customers.printSchema()
    orders.printSchema()
    order_items.printSchema()
    reviews.printSchema()
    products.printSchema()

    get_statistics(orders, customers, order_items)

    analyze_orders_by_country(orders, customers)

    analyze_orders_by_time(orders)
    
    analyze_reviews(reviews)

    analyze_revenue(orders, order_items, products)

    analyze_products(order_items, reviews, products)

    analyze_delivery(orders)

    analyze_customers(orders, customers, order_items)

    rank_sellers(order_items)