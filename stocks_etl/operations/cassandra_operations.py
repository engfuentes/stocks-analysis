import sys
from datetime import datetime
import yfinance as yf

from cassandra.cluster import Cluster

from stocks_etl.utils.helper_functions import configured_logger
from stocks_etl.utils.config import constants

# Load constants from config.py file
CASSANDRA_HOSTNAME = constants["cassandra"]["hostname"]

logger = configured_logger("cassandra-operations")

def save_posts_to_cassandra(df_to_save, cass_keyspace_name: str, cass_reddit_table: str):
    """Function that saves a Spark DataFrame to Cassandra
    Parameters
    ----------
        df_to_save (DataFrame): Spark DataFrame to be saved to Cassandra.
        cass_keyspace_name (str): Cassandra Keyspace Name.
        cass_reddit_table (str): Table name.
    """

    try:
        (df_to_save.write
                   .format("org.apache.spark.sql.cassandra")
                   .mode('append')
                   .options(table=cass_reddit_table, keyspace=cass_keyspace_name)
                   .save())
        # (df_to_save.write
        #                 .format("org.apache.spark.sql.cassandra")
        #                 .mode("append")
        #                 .option("confirm.truncate","true")
        #                 .option("checkpointLocation", '/tmp/check_point/')
        #                 .option("keyspace", cass_keyspace_name)
        #                 .option("table", cass_reddit_table)
        #                 .save())
        logger.info("Reddit posts were saved in the Cassandra Database")
    except Exception as e:
        logger.error(f"Couldn't insert the data due to: {e}")
        raise e

    # Unblock if it would be a streaming 
    # try:
    #     streaming_query = (df_to_save.writeStream
    #                                         .format("org.apache.spark.sql.cassandra")
    #                                         .option("checkpointLocation", '/tmp/check_point/')
    #                                         .option("keyspace", cass_keyspace_name)
    #                                         .option("table", cass_reddit_table)
    #                                         .start())
    #     streaming_query.awaitTermination()
    # except Exception as e:
    #     logger.error(f"Couldn't insert the data due to {e}")

def create_cassandra_connection(contact_point=CASSANDRA_HOSTNAME) -> Cluster:
    """Function used to connect to a Cassandra Cluster
    Parameters
    ----------
        contact_point (str): Contact Point to the database. default=CASSANDRA_HOSTNAME.
    Returns
    -------
        cassandra_session (Cluster): Cassandra Session.
    """
    try:
        # Connect to a Cassandra cluster
        cluster = Cluster([contact_point])
        cassandra_session = cluster.connect()
        logger.info(f"Connected to a Cassandra Cluster on contact_point: {contact_point}")
        return cassandra_session

    except Exception as e:
        logger.error(f"Couldn't create a Cassandra connection due to {e}")
        return None

def create_cassandra_connection_to_keyspace(keyspace_name: str, contact_point=CASSANDRA_HOSTNAME) -> Cluster:
    """Function used to connect to a Cassandra Cluster with a keyspace
    Parameters
    ----------
        keyspace_name (str): Keyspace Name.
        contact_point (str): Contact Point to the database. default=CASSANDRA_HOSTNAME.
    Returns
    -------
        cassandra_session (Cluster): Cassandra Session.
    """
    try:
        # Connect to a Cassandra cluster
        cluster = Cluster([contact_point])
        cassandra_session = cluster.connect(keyspace_name)
        logger.info(f"Connected to a Cassandra Cluster with keyspace: {keyspace_name}")
        return cassandra_session

    except Exception as e:
        logger.error(f"Couldn't create a Cassandra connection due to {e}")
        sys.exit(1)
        return None

def create_keyspace(session, keyspace_name: str):
    """Function to create a keyspace in Cassandra. It is like a schema to put the tables. 
    It creates the keyspace and shutdowns the session
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        keyspace_name (str): Keyspace Name.
    """
    
    session.execute(f"""CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};""")
    
    session.shutdown()

    logger.info(f"Keyspace: '{keyspace_name}' created successfully")

def create_table_reddit_posts(session, keyspace_name: str, table_name: str):
    """Function to create a table in Cassandra for the Reddit Posts
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        keyspace_name (str): Keyspace Name.
        table_name (str): Table Name.
    """
    
    try:
        session.execute(f"""CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
                                    id TEXT PRIMARY KEY,
                                    title TEXT,
                                    selftext TEXT,
                                    score INT,
                                    num_comments INT,
                                    author TEXT,
                                    created_utc TIMESTAMP,
                                    url TEXT,
                                    permalink TEXT,
                                    subreddit TEXT,
                                    cashtags SET<TEXT>,
                                    title_sentiment SET<TEXT>,
                                    selftext_sentiment SET<TEXT>
        );""")
    
    except Exception as e:
        logger.error(f"There was an error while creating the '{table_name}' table: {e}")

    logger.info(f"Table: '{table_name}' created successfully in Keyspace: '{keyspace_name}'")

def create_table_stocks_info(session, table_name: str):
    """Function to create a table in Cassandra for the stocks info
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        table_name (str): Table Name.
    """

    try:
        # Get the MSFT ticker from yfinance. This is used to create the table
        ticker = yf.Ticker("MSFT")
        info_dict = ticker.info

        # Change 52 order to be able to use the column name in cassandra (it cannot start with a number)
        if "52WeekChange" in info_dict.keys():
            info_dict["WeekChange52"] = info_dict.pop("52WeekChange")
        # Delete the companyOfficers data as it is a dictionary
        if "companyOfficers" in info_dict.keys():
            del info_dict["companyOfficers"]
        
        # Delete some extra keys
        for extra_info in ["address2", "fax"]:
            if extra_info in info_dict.keys():
                del info_dict[extra_info]

        # Create table query str
        table_query = create_table_query_str_stocks_etfs(info_dict, table_name)
        
        # Add grossprofits if not in the string
        if "grossprofits" not in table_query:
            table_query += f"grossProfits DOUBLE, "
        
        # Select the uuid as PRIMARY KEY
        table_query += "PRIMARY KEY (symbol));"

        # Execute the CREATE TABLE query
        session.execute(table_query)
    
    except Exception as e:
        logger.error(f"There was an error while creating the '{table_name}' table: {e}")

    logger.info(f"Table: '{table_name}' created successfully")

def create_table_etfs_info(session, table_name: str):
    """Function to create a table in Cassandra for the etfs info
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        table_name (str): Table Name.
    """

    try:
        # Get the SPY ticker from yfinance. This is used to create the table
        ticker = yf.Ticker("SPY")
        info_dict = ticker.info
        
        # Delete some extra keys
        for extra_info in ["phone", "trailingPegRatio"]:
            if extra_info in info_dict.keys():
                del info_dict[extra_info]

        # Create table query str
        table_query = create_table_query_str_stocks_etfs(info_dict, table_name)
        
        # Select the uuid as PRIMARY KEY
        table_query += "PRIMARY KEY (symbol));"

        # Execute the CREATE TABLE query
        session.execute(table_query)
    
    except Exception as e:
        logger.error(f"There was an error while creating the '{table_name}' table: {e}")

    logger.info(f"Table: '{table_name}' created successfully")

def create_table_query_str_stocks_etfs(info_dict: dict, table_name: str) -> str:
    """Function that returns a string with part of the query to create a table
    Parameters
    ----------
        info_dict (dict): Dictionary with the ticker info.
        table_name (str): Table Name.
    Returns
    -------
        table_query (str): Part of the string used to create a table.
    """

    # Start of the string
    table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("

    # Add column names and types depending the dict types
    for key, value in info_dict.items():
        if isinstance(value, int):
            if key == "exDividendDate":
                logger.info("exDividendDate")
                column_type = 'TIMESTAMP'
            # check if number doesnt fit in a 32 bits integer -> choose type double
            elif value/2147483647 > 1:
                column_type = 'DOUBLE'
            else:
                column_type = 'INT'
        elif isinstance(value, float):
            column_type = 'FLOAT'
        elif isinstance(value, str):
            column_type = 'TEXT'
        else:
            raise ValueError(f"Unsupported data type: {type(value)} for key: {key}")

        table_query += f"{key} {column_type}, "
    
    # The end of the string is added in the other function, as the end depends if is a stock or etf

    return table_query

def create_table_stocks_indicators(session, table_name: str, df):
    """Function to create a table in Cassandra for the stocks indicators table
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        table_name (str): Table Name.
        df (DataFrame): Spark Dataframe with the indicators
    """
    
    try:
        # Get the column names from the DataFrame
        column_names = df.columns

        # Create table query str
        table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("

        for column in column_names:
            if column == "ticker":
                column_type = 'TEXT'
            elif column == "date":
                column_type = 'DATE'
            else:
                column_type = 'DOUBLE'

            table_query += f"{column} {column_type}, "

        # Select the ticker and date as PRIMARY KEY
        table_query += "PRIMARY KEY (ticker, date));"

        # Execute the CREATE TABLE query
        session.execute(table_query)
    
    except Exception as e:
        logger.error(f"There was an error while creating the '{table_name}' table: {e}")

    logger.info(f"Table: '{table_name}' created successfully")                                                                                                      

def query_stocks_cashtags(session, keyspace_name: str, table_name: str):
    """Function use to query Cassandra database and get the cashtag column
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        keyspace_name (str): Keyspace Name.
        table_name (str): Table Name
    """
    try:        
        session.execute(f"""SELECT cashtags FROM {keyspace_name}.{table_name};""")
    except Exception as e:
        logger.error(f"There was an error while querying the table {table_name}: {e}")

    logger.info(f"Table: '{table_name}' query completed")

def insert_stocks_info(session, table_name: str, info_dict: dict):
    """Function to insert the stocks info to the Cassandra table
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        table_name (str): Table Name.
        info_dict(dict): Dictionary with the ticker info.
    """

    try:
        # Change 52 order to be able to use the column name in cassandra (it cannot start with a number)
        if "52WeekChange" in info_dict.keys():
            info_dict["WeekChange52"] = info_dict.pop("52WeekChange")
        # Delete the companyOfficers data as it is a dictionary
        if "companyOfficers" in info_dict.keys():
            del info_dict["companyOfficers"]
        
        # Delete some extra keys
        for extra_info in ["address2", "fax"]:
            if extra_info in info_dict.keys():
                del info_dict[extra_info]

        # Format the float numbers to 3 decimals
        formatted_dict = format_dict(info_dict)

        # Insert query
        column_names = ', '.join(formatted_dict.keys())
        values = ', '.join(['%s' if value is not None else 'NULL' for value in formatted_dict.values()])
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values})"

        # Execute the INSERT query
        session.execute(insert_query, tuple(value for value in formatted_dict.values() if value is not None)) 
    
    except Exception as e:
        logger.error(f"There was an error while inserting a record in the table '{table_name}'. Error: {e}")

def insert_etfs_info(session, table_name: str, info_dict: dict):
    """Function to insert the etfs info to the Cassandra table
    Parameters
    ----------
        session (Cluster): Cassandra Session.
        table_name (str): Table Name.
        info_dict(dict): Dictionary with the ticker info.
    """
    
    try:
        # Delete some extra keys
        for extra_info in ["phone", "trailingPegRatio"]:
            if extra_info in info_dict.keys():
                del info_dict[extra_info]
        
        # Format the float numbers to 3 decimals
        formatted_dict = format_dict(info_dict)

        # Delete keys with None values
        list_none_keys = []
        for k,v in formatted_dict.items():
            if v == None:
                list_none_keys.append(k)
        
        if list_none_keys:
            for key in list_none_keys:
                del formatted_dict[key]

        # Insert query
        column_names = ', '.join(formatted_dict.keys())
        values = ', '.join(['%s' for value in formatted_dict.values()])
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values})"

        # Execute the INSERT query
        session.execute(insert_query, tuple(value for value in formatted_dict.values()))  

    except Exception as e:
        logger.error(f"There was an error while inserting a record in the table '{table_name}'. Error: {e}")

def format_dict(dict_to_format: dict) -> dict:
    """Function used format a dictionary with stock information
    Parameters
    ----------
        dict_to_format (dict): Dictionary to format with the stocks info.
    Returns
    -------
        formatted_dict (dict): Formatted dictionary.
    """

    formatted_dict = dict()
    
    # Apply some transformations to some of the dictionary values
    try:
        for key, value in dict_to_format.items():
            if isinstance(value, int):
                if key == "exDividendDate":
                    formatted_dict[key] = datetime.fromtimestamp(value).date()
                else:
                    formatted_dict[key] = value
            elif isinstance(value, float):
                formatted_dict[key] = round(value, 3)
            elif isinstance(value, str):
                if value == "Infinity":
                    formatted_dict[key] = None
                else:
                    formatted_dict[key] = value
            elif value is None:
                formatted_dict[key] = None
            else:
                raise ValueError(f"Unsupported data type: {type(value)} for key: {key}")

        return formatted_dict

    except Exception as e:
        logger.error(f"There was an error while formatting the dictionary")