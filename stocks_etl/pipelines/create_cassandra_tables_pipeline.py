from stocks_etl.operations.cassandra_operations import create_cassandra_connection, create_keyspace, \
    create_cassandra_connection_to_keyspace, create_table_reddit_posts, create_table_stocks_info, \
    create_table_etfs_info

def create_cassandra_tables_pipeline():
    """This pipeline creates all the tables except: stocks indicator table"""
    # Create a Cassandra Connection
    session = create_cassandra_connection()

    # Define keyspace name
    keyspace_name = "stocks_data"
    
    # Create a Keyspace
    create_keyspace(session, keyspace_name)
    session = create_cassandra_connection_to_keyspace(keyspace_name)

    # Create Reddit Posts Table
    create_table_reddit_posts(session, keyspace_name, "reddit_posts")

    # Create Stocks Info Table
    create_table_stocks_info(session, "stocks_info")

    # Create ETFs Info Table
    create_table_etfs_info(session, "etfs_info")

    # Close Cassandra Session
    session.shutdown()

# if __name__ == "__main__":
#     create_cassandra_tables_pipeline()