import os
import pandas as pd
import yfinance as yf
import pandas_ta as ta

import pyspark
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *

from confluent_kafka.schema_registry import SchemaRegistryClient

from stocks_etl.operations.cassandra_operations import insert_stocks_info, insert_etfs_info
from stocks_etl.utils.helper_functions import configured_logger
from stocks_etl.utils.config import constants

# Load constants from config.py file
BOOTSTRAP_SERVERS = constants["kafka"]["bootstrap_servers"]
SCHEMA_REGISTRY_URL = constants["kafka"]["schema_registry_url"]
ALPHAV_API_KEY = constants["alphavantag"]["alpha_vantage_api_key"]
CASSANDRA_HOSTNAME = constants["cassandra"]["hostname"]

logger = configured_logger("spark-operations")

def create_spark_connection():
    """Function used to create a connection to the Spark cluster
    Returns
    -------
        spark (SparkSession): SparkSession instance.
    """
    
    spark = None

    try:
        java_jars = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                    "org.apache.spark:spark-avro_2.12:3.5.0",
                    "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.4.1",
                    "com.github.jnr:jnr-posix:3.1.18",
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
                    "com.datastax.spark:spark-cassandra-connector-driver_2.12:3.4.1"
                    ]
        
        spark = (SparkSession.builder
                              .appName('Spark transformations')
                              .master("spark://spark-master:7077")
                              .config('spark.jars.packages', ",".join(java_jars))
                              .config("spark.cassandra.connection.host", CASSANDRA_HOSTNAME)
                              .config('spark.cassandra.connection.port', '9042')
                              .getOrCreate())

        #spark.sparkContext.setLogLevel('ERROR')
        logger.info("Spark connection created successfully")

    except Exception as e:
        logger.error(f"Couldn't create the Spark connection due to exception: {e}")
    
    return spark

def get_reddit_df(spark, topic: str, starting_offsets: str) -> "Dataframe":
    """Function that gets the data from kafka and then use the Schema Registry to decode the message
    Parameters
    ----------
        spark (SparkSession): SparkSession instance.
        topic (str): Kafka topic from where the stream is read.
        starting_offsets (str): From which offset to start reading. Options: "earliest", "latest".
    Returns
    -------
        reddit_df (DataFrame): Spark DataFrame.
    """

    try:
        # Connect to Kafka with the Spark Session
        spark_df = connect_to_kafka(spark, BOOTSTRAP_SERVERS, topic, starting_offsets)

        # Get the schema from the Schema Registry
        reddit_schema = get_schema_from_schema_registry(SCHEMA_REGISTRY_URL, f"{topic}-value")

        # Decode the avro message. A substring is used and starting from the 6th byte as Confluent
        # avro schema has 1 magic byte + 4 schema id bytes at the start of the data
        reddit_df = (spark_df.selectExpr("substring(value, 6) as avro_value") 
                            .select(from_avro(F.col("avro_value"), reddit_schema).alias("data")) 
                            .select("data.*"))
        # reddit_df.printSchema()
        logger.info("Returned Reddit Dataframe")
        
        return reddit_df
    
    except Exception as e:
        logger.error(f"Couldn't get the reddit_df with the kafka info due to exception: {e}")

def connect_to_kafka(spark, bootstrap_servers: str, topic: str, starting_offsets: str) -> "Dataframe":
    """Function used to read a stream of messages from a Kafka Topic
    Parameters
    ----------
        spark (SparkSession): SparkSession instance.
        bootstrap_servers (str): "Host:Port" used to connect to the Kafka Cluster. "kafka-broker1:9092,kafka-broker2:9092".
        topic (str): Kafka topic from where the stream is read.
        starting_offsets (str): From which offset to start reading. Options: "earliest", "latest".
    Returns
    -------
        spark_df (DataFrame): Spark DataFrame.
    """

    try:
        spark_df = (spark.read 
                         .format('kafka') 
                         .option('kafka.bootstrap.servers', bootstrap_servers) 
                         .option('subscribe', topic) 
                         .option('startingOffsets', starting_offsets) 
                         .load())
        logger.info("Kafka dataframe created successfully")
        
        return spark_df

    except Exception as e:
        logger.error(f"Kafka dataframe could not be created due to exception: {e}")

def get_schema_from_schema_registry(schema_registry_url: str, schema_registry_subject: str):
    """Function used get the schema from the Schema Registry
    Parameters
    ----------
        schema_registry_url (str): url to connect to the Schema Registry.
        schema_registry_subject (str): Schema registry subject: "topic-value" or "topic-key".
    Returns
    -------
        schema (str): Schema of the topic in json format
    """

    try:
        sr = SchemaRegistryClient({'url': schema_registry_url})
        latest_version_schema = sr.get_latest_version(schema_registry_subject)
        schema = latest_version_schema.schema.schema_str
        logger.info("Returned Schema from the Schema Registry")
    
        return schema
    
    except Exception as e:
        logger.error(f"Couldn't get the Schema Registry due to exception: {e}")

def cast_types_and_get_subreddit(reddit_df) -> "Dataframe":
    """Function cast the df column to correct types and get the subreddit where the post was written
    Parameters
    ----------
        reddit_df (SparkSession): Spark DataFrame.
    Returns
    -------
        reddit_df (DataFrame): Transformed Spark DataFrame.
    """

    # Cast columns
    reddit_df = reddit_df.withColumn("score", reddit_df.score.cast(IntegerType()))
    reddit_df = reddit_df.withColumn("num_comments", reddit_df.num_comments.cast(IntegerType()))
    reddit_df = reddit_df.withColumn("created_utc", F.from_unixtime(F.col("created_utc")).cast("timestamp"))
    # Get subreddit and create a column for it
    reddit_df = reddit_df.withColumn("subreddit", F.split(F.col("permalink"), "/")[2])
    logger.info("Casted columns and obtained the Subreddit")
    
    return reddit_df

def get_cashtags(spark, reddit_df, alpha_v_api_key=ALPHAV_API_KEY) -> "Dataframe":
    """Function cast the df column to correct types and get the subreddit where the post was written
    Parameters
    ----------
        reddit_df (SparkSession): Spark DataFrame.
    Returns
    -------
        reddit_df (DataFrame): Transformed Spark DataFrame.
    """

    # Get the list of stocks and ETFs that are present in USA
    stocks_list_df = get_stocks_list(spark, alpha_v_api_key)
    # Create a string with the stocks and ETFs symbols and "|" to be used in the regex expression
    list_stocks_str = stocks_list_df.select(F.concat_ws("|", F.collect_list("symbol"))).collect()[0][0]

    # Pattern to be used to recognize the symbols
    cashtag_pattern = F.lit("(?:^|(?<= )|\$)(" + list_stocks_str + ")(?:(?= )|$)")

    # Get cashtags of "title", then append the cashtags of "selftext" and finally save the distinct values
    reddit_df = reddit_df.withColumn('cashtags', F.regexp_extract_all('title', cashtag_pattern))
    reddit_df = reddit_df.withColumn("cashtags", F.concat(reddit_df.cashtags, F.regexp_extract_all("selftext", F.lit(cashtag_pattern))))
    reddit_df = reddit_df.withColumn("cashtags", F.array_distinct(reddit_df.cashtags))
    logger.info("Cashtags Processed")

    return reddit_df

def get_stocks_list(spark, alpha_v_api_key: str) -> "Dataframe":
    """Function that returns a DataFrame with data about the stocks and ETFs that are present in USA market
    Parameters
    ----------
        spark (SparkSession): SparkSession instance.
        alpha_v_api_key (str): API key of AlphaVantage website.
    Returns
    -------
        stocks_data_df (DataFrame): Spark Dataframe with stocks and ETFs data.
    """

    try: 
        logger.info(f"Downloading csv with stocks list...")

        url = f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={ALPHAV_API_KEY}"
        stocks_data_df = spark.createDataFrame(pd.read_csv(url))

        return stocks_data_df
    
    except Exception as e:
        logger.error(f"Couldn't get the Stocks List due to exception: {e}")

def get_set_cashtags_database(spark, keyspace_name:str, table_name: str) -> set:
    """Function that returns set with the cashtags from the Reddit table
    Parameters
    ----------
        spark (SparkSession): SparkSession instance.
        keyspace_name (str): Keyspace Name.
        table_name (str): Table Name.
    Returns
    -------
        flatten_set (set): Set of the cashtags that are present in the Reddit table.
    """

    # Read from cassandra
    df = (spark.read.format("org.apache.spark.sql.cassandra")\
                   .options(table=table_name, keyspace=keyspace_name)
                   .load()
                   .select("cashtags"))
    # Filter for the non empty arrays
    df_ct = df.filter(F.size(df.cashtags)>=1)
    # Get a list with lists of cashtags
    lists_ct = df_ct.select(F.collect_list(df_ct.cashtags)).collect()[0][0]
    # Get 1 set with the unique cashtags
    flatten_set = set([item for sublist in lists_ct for item in sublist])
    
    return flatten_set

def load_ticker_info_to_cassandra(session, ticker, stocks_table_name, etfs_table_name):
    """Function loads the ticker information to the Cassandra database
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        ticker (str): Ticker Symbol.
        stocks_table_name (str): Name of the stocks_info table.
        etfs_table_name (str): Name of the etfs_info table.
    """
    
    try:
        # Get the ticker from yfinance
        ticker = yf.Ticker(ticker)
        info_dict = ticker.info

        # Check the quoteType and load accordingly
        if info_dict["quoteType"] == "EQUITY":
            insert_stocks_info(session, stocks_table_name, info_dict)
        elif info_dict["quoteType"] == "ETF":
            insert_etfs_info(session, etfs_table_name, info_dict)
        else:
            raise Exception("quoteType is not in the options")
            logger.info(f"The quoteType: {info_dict['quoteType']} is not in the options")
    except Exception as e:
        logger.error(f"Couldn't load the ticker information to Cassandra due to exception: {e}")

def get_ticker_price_hist(spark, tickers_list: list, period: str) -> "DataFrame":
    """Function that gets the price history of the tickers_list
    Parameters
    ----------
        spark (SparkSession): SparkSession instance.
        tickers_list (list): List of tickers to download the price history.
        period (str): Period of time to download. Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    Returns
    -------
        price_hist (DataFrame): Spark DataFrame with the tickers price history.
    """
    
    # Create a Spark DataFrame with the list of tickers
    tickers_df = spark.createDataFrame(list(map(lambda x: Row(ticker=x), tickers_list)))

    # Create a Schema
    schema = StructType([
                        StructField('ticker', StringType(), True), 
                        StructField('date', DateType(), True),
                        StructField('open', DoubleType(), True),
                        StructField('high', DoubleType(), True),
                        StructField('low', DoubleType(), True),
                        StructField('close', DoubleType(), True),
                        StructField('volume', DoubleType(), True),
                        ])
    
    def fetch_tick(groupby_keys, pdf):
        """Function used with applyInPandas to fetch the ticker price history
        Parameters
        ----------
            groupby_keys (tuple): Tuple with the keys used for the groupBy operation (in this case the ticker).
            pdf (Pandas DataFrame): Pandas Dataframe after the groupBy operation.
        Returns
        -------
            output_df (DataFrame): Spark DataFrame with the tickers price history.
        """
        
        tick = groupby_keys[0]
        
        try:
            ticker = yf.Ticker(tick)
            raw = ticker.history(period=period)[['Open', 'High', 'Low', 'Close', 'Volume']]
            # Fill the missing business days
            idx = pd.date_range(raw.index.min(), raw.index.max(), freq='B')
            # Use the last observation carried forward for missing value
            output_df = raw.reindex(idx, method='pad')
            # Pandas does not keep index (date) when converted into spark dataframe
            output_df['date'] = output_df.index
            output_df['ticker'] = tick   
            output_df = output_df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Volume": "volume", "Close": "close"})
        
            return output_df
        
        except:
            return pd.DataFrame(columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'])

    price_hist = tickers_df.groupBy("ticker").applyInPandas(fetch_tick, schema)

    return price_hist

def calculate_stocks_indicators(prices_df) -> "DataFrame":
    """Function that calculates some indicators using pandas_ta library
    Parameters
    ----------
        prices_df (DataFrame): Spark DataFrame.
    Returns
    -------
        prices_df (DataFrame): Spark DataFrame with the indicators.
    """
    
    # Column names that are of DoubleType()
    col_names_double = ["open", "high", "low", "close", "volume", "rsi", "macd_hist", "ema_21",
            "ema_50", "ema_200", "atr", "bb_lower", "bb_mid", "bb_upper"]

    # Create a schema to be used in the pandas_udf
    schema = StructType([
                    StructField("ticker", StringType(), True), 
                    StructField("date", DateType(), True)] +
                    [StructField(col_name, DoubleType(), True) for col_name in col_names_double])

    def calculate_indicators(pdf):
        """Function used to calculate the indicators
        Parameters
        ----------
            pdf (DataFrame): Pandas DataFrame.
        Returns
        -------
            pdf (DataFrame): Pandas DataFrame with the indicators.
        """
        try:
            pdf["rsi"] = ta.rsi(pdf.close, length=14)
            pdf["macd_hist"] = ta.macd(pdf.close).iloc[:,1]
            pdf["ema_21"] = ta.ema(pdf.close, length=21)
            pdf["ema_50"] = ta.ema(pdf.close, length=50)
            pdf["ema_200"] = ta.ema(pdf.close, length=200)
            pdf["atr"] = ta.atr(pdf.high, pdf.low, pdf.close, length=14)
            pdf["bb_lower"] = ta.bbands(pdf.close, length=20).iloc[:,0]
            pdf["bb_mid"] = ta.bbands(pdf.close, length=20).iloc[:,1]
            pdf["bb_upper"] = ta.bbands(pdf.close, length=20).iloc[:,2]
            
            return pdf
        
        except:
            return pd.DataFrame(columns = ["ticker", "date", "open", "high", "low", "close", "volume", "rsi", "macd_hist", "ema_21",
                                            "ema_50", "ema_200", "atr", "bb_lower", "bb_mid", "bb_upper"])
        return pdf
    
    # Groupby by ticker and calculate the indicators with the function
    prices_df = prices_df.groupby("ticker").applyInPandas(calculate_indicators, schema)

    return prices_df

def calculate_stocks_statistics(prices_df):
    """Function that calculates some stocks statistics
    Parameters
    ----------
        prices_df (DataFrame): Spark DataFrame.
    Returns
    -------
        prices_df (DataFrame): Spark DataFrame with the indicators.
    """
    
    # Add to the DataFrame columns of the year, month, day and week
    prices_df = (prices_df.withColumn("year", F.year(prices_df.date))
                        .withColumn("month", F.month(prices_df.date))
                        .withColumn("day", F.dayofmonth(prices_df.date))
                        .withColumn("week", F.weekofyear(prices_df.date))
                    )

    # Create dataframes with the max and min price for the year, month and week
    year_max_min = (prices_df.groupBy(['ticker', 'year']).agg(F.max("close").alias("year_high"),
                                                             F.min("close").alias("year_low"))
                                                         .orderBy("ticker", "year"))
    month_max_min = (prices_df.groupBy(['ticker', 'year', 'month']).agg(F.max("close").alias("month_high"),
                                                                       F.min("close").alias("month_low"))
                                                                   .orderBy("ticker", "year", "month"))
    week_max_min = (prices_df.groupBy(['ticker', 'year', 'week']).agg(F.max("close").alias("week_high"),
                                                                      F.min("close").alias("week_low"))
                                                                 .orderBy("ticker", "year", "week"))

    # 

def get_reddit_posts_sentiment_analysis(spark, keyspace_name:str, table_name: str, ticker:str):
    df_reddit_posts = (spark.read.format("org.apache.spark.sql.cassandra")\
                                 .options(table=table_name, keyspace=keyspace_name)
                                 .load()
                                 .select("*")
                                 .filter(F.array_contains("cashtags", ticker)))
    
    list_posts = list(map(lambda row: row.asDict(), df_reddit_posts.collect()))

    return list_posts