from stocks_etl.operations.spark_operations import create_spark_connection, get_set_cashtags_database, \
    load_ticker_info_to_cassandra, get_ticker_price_hist, calculate_stocks_indicators
from stocks_etl.operations.cassandra_operations import create_cassandra_connection_to_keyspace, \
    create_table_stocks_indicators

from stocks_etl.utils.helper_functions import configured_logger

logger = configured_logger("stocks-data-pipeline")

def stocks_data_pipeline():
    # Create a spark and cassandra connection
    spark = create_spark_connection()
    session = create_cassandra_connection_to_keyspace(keyspace_name="stocks_data")
    
    # Query the cassandra database for cashtags and get a set of them
    cashtags_set = get_set_cashtags_database(spark, keyspace_name="stocks_data", table_name="reddit_posts")

    # With cashtags list then insert the ticker info into cassandra
    logger.info(f"Ticker: {cashtags_set}")
    for ticker in cashtags_set:
        try:
            load_ticker_info_to_cassandra(session, ticker, "stocks_info", "etfs_info")
            logger.info(f"Loaded to cassandra: {ticker}")
        except Exception as e:
            logger.error(f"There was a problem to save {ticker} to cassandra: {e}")

    # Get price history
    tickers_hist = get_ticker_price_hist(spark, list(cashtags_set), "1y")

    # Calculate some indicators
    tickers_indicators = calculate_stocks_indicators(tickers_hist)

    # Create stocks indicator table in cassandra if it does not exist
    create_table_stocks_indicators(session, "stocks_indicators", tickers_indicators)

    # Save the stocks indicator dataframe to cassandra
    (tickers_indicators.write
                       .format("org.apache.spark.sql.cassandra")
                       .mode('append')
                       .options(table="stocks_indicators", keyspace="stocks_data")
                       .save())

    # Close spark and cassandra sessions
    session.shutdown()
    spark.stop()

if __name__ == "__main__":
    stocks_data_pipeline()

# spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.18 pipelines/stocks_data_pipeline.py