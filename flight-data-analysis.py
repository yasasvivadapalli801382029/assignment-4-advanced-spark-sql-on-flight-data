from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("Advanced Flight Data Analysis").getOrCreate()

# Load datasets
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)
airports_df = spark.read.csv("airports.csv", header=True, inferSchema=True)
carriers_df = spark.read.csv("carriers.csv", header=True, inferSchema=True)

# Define output paths
output_dir = "output/"
task1_output = output_dir + "task1_largest_discrepancy.csv"
task2_output = output_dir + "task2_consistent_airlines.csv"
task3_output = output_dir + "task3_canceled_routes.csv"
task4_output = output_dir + "task4_carrier_performance_time_of_day.csv"

# ------------------------
# Task 1: Flights with the Largest Discrepancy Between Scheduled and Actual Travel Time
# ------------------------
def task1_largest_discrepancy(flights_df):
    # Calculate scheduled and actual travel times
    flights_df = flights_df.withColumn(
        "scheduled_travel_time", 
        (F.unix_timestamp("ScheduledArrival") - F.unix_timestamp("ScheduledDeparture")) / 60
    ).withColumn(
        "actual_travel_time", 
        (F.unix_timestamp("ActualArrival") - F.unix_timestamp("ActualDeparture")) / 60
    )
    
    # Calculate discrepancy
    flights_df = flights_df.withColumn(
        "discrepancy", 
        F.abs(flights_df["scheduled_travel_time"] - flights_df["actual_travel_time"])
    )
    
    # Rank by largest discrepancy
    window = Window.orderBy(F.desc("discrepancy"))
    largest_discrepancy = flights_df.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") <= 10)
    
    # Save the result
    largest_discrepancy.select("FlightNum", "discrepancy").write.csv(task1_output, header=True)
    print(f"Task 1 output written to {task1_output}")

# ------------------------
# Task 2: Most Consistently On-Time Airlines Using Standard Deviation
# ------------------------
def task2_consistent_airlines(flights_df, carriers_df):
    # Calculate departure delay
    flights_df = flights_df.withColumn(
        "departure_delay", 
        (F.unix_timestamp("ActualDeparture") - F.unix_timestamp("ScheduledDeparture")) / 60
    )
    
    # Calculate standard deviation of delay by carrier
    delay_stddev = flights_df.groupBy("CarrierCode").agg(
        F.count("FlightNum").alias("flight_count"),
        F.stddev("departure_delay").alias("stddev_delay")
    ).filter(F.col("flight_count") > 100).orderBy("stddev_delay")
    
    # Join with carriers to get carrier names
    result = delay_stddev.join(carriers_df, "CarrierCode")
    
    # Save the result
    result.select("CarrierName", "stddev_delay").write.csv(task2_output, header=True)
    print(f"Task 2 output written to {task2_output}")

# ------------------------
# Task 3: Origin-Destination Pairs with the Highest Percentage of Canceled Flights
# ------------------------
def task3_canceled_routes(flights_df, airports_df):
    # Mark cancellations
    flights_df = flights_df.withColumn("is_canceled", F.when(F.col("ActualDeparture").isNull(), 1).otherwise(0))
    
    # Calculate cancellation rate per origin-destination pair
    cancel_rate = flights_df.groupBy("Origin", "Destination").agg(
        (F.sum("is_canceled") / F.count("FlightNum") * 100).alias("cancel_rate")
    ).orderBy(F.desc("cancel_rate"))
    
    # Join with airports to get airport names
    cancel_rate = cancel_rate \
        .join(airports_df.withColumnRenamed("AirportCode", "Origin"), "Origin") \
        .join(airports_df.withColumnRenamed("AirportCode", "Destination"), "Destination") \
        .select("Origin", "Destination", "cancel_rate")
    
    # Save the result
    cancel_rate.write.csv(task3_output, header=True)
    print(f"Task 3 output written to {task3_output}")

# ------------------------
# Task 4: Carrier Performance Based on Time of Day
# ------------------------
def task4_carrier_performance_time_of_day(flights_df, carriers_df):
    # Define time of day categories
    flights_df = flights_df.withColumn(
        "time_of_day", 
        F.when((F.hour("ScheduledDeparture") >= 5) & (F.hour("ScheduledDeparture") < 12), "morning")
        .when((F.hour("ScheduledDeparture") >= 12) & (F.hour("ScheduledDeparture") < 17), "afternoon")
        .when((F.hour("ScheduledDeparture") >= 17) & (F.hour("ScheduledDeparture") < 21), "evening")
        .otherwise("night")
    )
    
    # Calculate average delay per carrier and time of day
    avg_delay = flights_df.groupBy("CarrierCode", "time_of_day").agg(
        F.avg((F.unix_timestamp("ActualDeparture") - F.unix_timestamp("ScheduledDeparture")) / 60).alias("avg_delay")
    )
    
    # Join with carriers to get carrier names
    result = avg_delay.join(carriers_df, "CarrierCode").orderBy("time_of_day", "avg_delay")
    
    # Save the result
    result.select("CarrierName", "time_of_day", "avg_delay").write.csv(task4_output, header=True)
    print(f"Task 4 output written to {task4_output}")

# ------------------------
# Call the functions for each task
# ------------------------
task1_largest_discrepancy(flights_df)
task2_consistent_airlines(flights_df, carriers_df)
task3_canceled_routes(flights_df, airports_df)
task4_carrier_performance_time_of_day(flights_df, carriers_df)

# Stop the Spark session
spark.stop()
