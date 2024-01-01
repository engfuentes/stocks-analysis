import json, sys
from datetime import datetime
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from cassandra.cluster import Cluster

from app_modules.configure_logger import configured_logger
from app_modules.config import constants

# Load constants from config.py file
CASSANDRA_HOSTNAME = constants["cassandra"]["hostname"]

logger = configured_logger("stocks-dash-operations")

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

def get_list_stocks(session) -> list:
    """Function used to query the Cassandra db to get the list of stocks
    Parameters
    ----------
        cassandra_session (Cluster): Cassandra Session.
    Returns
    -------
        flat_list (list): List of stocks that are present in the cashtags column of the reddit_posts table.
    """
    
    # Execute query
    rows = session.execute("SELECT cashtags FROM stocks_data.reddit_posts")

    # Get a list of lists with the cashtags
    stocks_lists = []
    for row in rows:
        if row.cashtags is not None:
            stocks_lists.append(list(row.cashtags))

    # Flat the list to only one list
    flat_list = [item for sublist in stocks_lists for item in sublist]
    
    return sorted(flat_list)

def get_reddit_posts(session, ticker: str) -> list:
    """Function used to query the Cassandra db to get Reddit posts of ticker
    Parameters
    ----------
        cassandra_session (Cluster): Cassandra Session.
        ticker (str): Ticker for what the posts are requested.
    Returns
    -------
        ticker_posts_list (list): List of dictionaries with the Reddit posts of the ticker.
    """

    # Create a pandas df with all the reddit posts from the database
    reddit_posts_df = pd.DataFrame(session.execute("SELECT * FROM stocks_data.reddit_posts"))

    # Transform all non empty rows to list type
    reddit_posts_df["cashtags"] = reddit_posts_df.cashtags.apply(lambda x: list(x) if x is not None else None)

    def check_if_ticker(x):
        """Function that returns True if the ticker is in the list, otherwise False"""
        if x is not None:
            return ticker in x
        else:
            return False
    
    # Create a mask that checks if the desired ticker is in the cashtags list
    mask = reddit_posts_df.cashtags.apply(check_if_ticker)
    
    # Use the mask to filter the df for the desired ticker posts
    ticker_posts = reddit_posts_df[mask]
    
    # Transform 2 columns rows to the list type
    ticker_posts.loc[:, "selftext_sentiment"] = ticker_posts.selftext_sentiment.apply(lambda x: list(x) if x is not None else None)
    ticker_posts.loc[:, "title_sentiment"] = ticker_posts.title_sentiment.apply(lambda x: list(x) if x is not None else None)
    
    # Transform the df to a list of dictionaries
    ticker_posts_list = ticker_posts.to_dict(orient="records")

    return ticker_posts_list

def get_table_ticker_info(session, ticker: str):
    """Function used to query the Cassandra db to get Reddit posts of ticker
    Parameters
    ----------
        cassandra_session (Cluster): Cassandra Session.
        ticker (str): Ticker for what the table information is requested.
    Returns
    -------
        ticker_table_dict (dict): Dict with information of the ticker.
        ticker_summary_dict (dict): Dict with a summary of the ticker company.
        stock_bool (bool): True if the ticker is a stock.
        etf_bool (bool): True id the ticker is an ETF.
    """

    logger.info(f"Querying for the info of {ticker}")

    etf_bool = False
    stock_bool = False
    
    try:
        # Query for the stocks_info of the ticker
        ticker_info_df = pd.DataFrame(session.execute(f"SELECT * FROM stocks_data.stocks_info WHERE symbol = '{ticker}'"))
        
        # Make some formatting to the df
        ticker_table_df, ticker_summary_df = format_stocks_df(ticker_info_df)
        
        # Transform dfs to dict
        ticker_table_dict = ticker_table_df.to_dict(orient="records")[0]
        ticker_summary_dict = ticker_summary_df.to_dict(orient="records")[0]

        stock_bool = True
    
    except:
        # If there is an error in the try is because the ticker is an ETF so the information is in etfs_info table
        logger.info(f"{ticker} is an ETF")

        # Query for the stocks_info
        ticker_info_df = pd.DataFrame(session.execute(f"SELECT * FROM stocks_data.etfs_info WHERE symbol = '{ticker}'"))
        
        # Make some formatting to the df
        ticker_table_df, ticker_summary_df = format_etfs_df(ticker_info_df)

        # Transform dfs to dict
        ticker_table_dict = ticker_table_df.to_dict(orient="records")[0]
        ticker_summary_dict = ticker_summary_df.to_dict(orient="records")[0]

        etf_bool = True

    return ticker_table_dict, ticker_summary_dict, stock_bool, etf_bool

def format_stocks_df(ticker_info_df):
    """Function used to format the stocks_df
    Parameters
    ----------
        ticker_info_df (Pandas DataFrame): Dataframe with the stock information.
    Returns
    -------
        ticker_table_df (Pandas DataFrame): DataFrame with information of the stock.
        ticker_summary_df (Pandas DataFrame): Dataframe with a summary of the stock company.
    """

    # Round float numbrers to 2 decimals
    ticker_info_df = ticker_info_df.round(2)
    
    # Change format of "exdividenddate" to '%d/%m/%Y'
    if ticker_info_df.loc[0,"exdividenddate"] != None:
        ticker_info_df["exdividenddate"] = ticker_info_df["exdividenddate"].dt.strftime('%d/%m/%Y')

    # Put thousands separator (",") to the columns that have big numbers
    for col in ["fulltimeemployees", "averagedailyvolume10day", "marketcap", "sharesoutstanding", "sharesshort"]:
        ticker_info_df[col] = ticker_info_df[col].apply(lambda x : "{:,}".format(int(x)) if pd.notnull(x) else "")

    # Get column names with more readable names
    cols_ticker_table, cols_ticker_table_names = get_columns_and_names("./app_modules/json_dicts/stock_table_col_names.json")
    
    # Columnes used for the summary
    cols_ticker_summary = ["shortname", "industry", "sector", "website", "longbusinesssummary"]

    # Filter for the columns for the ticker table
    ticker_table_df = ticker_info_df[cols_ticker_table]
    # Rename columns
    ticker_table_df.columns = cols_ticker_table_names

    # Filter for the columns for the info table
    ticker_summary_df = ticker_info_df[cols_ticker_summary]
    
    return ticker_table_df, ticker_summary_df

def format_etfs_df(ticker_info_df):
    """Function used to format the stocks_df
    Parameters
    ----------
        ticker_info_df (Pandas DataFrame): Dataframe with the ETF information.
    Returns
    -------
        ticker_table_df (Pandas DataFrame): DataFrame with information of the ETF.
        ticker_summary_df (Pandas DataFrame): Dataframe with a summary of the ETF.
    """

    # Round float numbrers to 2 decimals
    ticker_info_df = ticker_info_df.round(2)

    # Put thousands separator (",") to the columns that have big numbers
    for col in ["averagedailyvolume10day", "averagevolume", "totalassets"]:
        ticker_info_df[col] = ticker_info_df[col].apply(lambda x : "{:,}".format(int(x)) if pd.notnull(x) else "")

    # Get column names with more readable names
    cols_ticker_table, cols_ticker_table_names = get_columns_and_names("./app_modules/json_dicts/etf_table_col_names.json")

    # Columnes used for the summary
    cols_ticker_summary = ['shortname', 'legaltype', 'fundfamily', 'category', 'longbusinesssummary']

    # Filter for the columns for the ticker table
    ticker_table_df = ticker_info_df[cols_ticker_table]
    # Rename columns
    ticker_table_df.columns = cols_ticker_table_names
    
    # Filter for the columns for the info table
    ticker_summary_df = ticker_info_df[cols_ticker_summary]
    
    return ticker_table_df, ticker_summary_df

def get_columns_and_names(json_file_path: str):
    """Function used to get the columns for the information table and its formatted column names
    Parameters
    ----------
        json_file_path (str): Path to the json file.
    Returns
    -------
        columns_list (list): List of columns.
        columns_names (list): List of formatted column names.
    """

    columns_list = []
    columns_names = []

    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)
        
        for k,v in data.items():
            columns_list.append(k)
            columns_names.append(v)
    
    return columns_list, columns_names

def get_ticker_indicators(session, ticker:str):
    """Function used to query the Cassandra db to get the ticker price history and indicators
    Parameters
    ----------
        cassandra_session (Cluster): Cassandra Session.
        ticker (str): Ticker for what the DataFrame is requested.
    Returns
    -------
        indicators_df (Pandas DataFrame): DataFrame with the ticker price history and indicators.
    """

    # Query the Cassandra db to get the ticker price history and indicators
    indicators_df = pd.DataFrame(session.execute(f"SELECT * FROM stocks_data.stocks_indicators WHERE ticker='{ticker}'"))

    # Round float numbers to 3 decimals
    indicators_df = indicators_df.round(3)

    # Change format to the date column to '%Y-%m-%d'
    indicators_df.loc[:,"date"] = indicators_df["date"].astype(str).apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))

    return indicators_df

def create_interactive_graph(df):
    """Function used to create the plotly figure with a candlestick graph and some indicators
    Parameters
    ----------
        df (Pandas DataFrame): DataFrame with the ticker price history and indicators.
    Returns
    -------
        fig (Plotly Figure): Plotly Figure with the Candlestick chart and indicators.
    """
    
    # Load indicators options from a json file
    with open("./app_modules/json_dicts/indicators_options.json", 'r') as json_file:
        indicators_options_dict = json.load(json_file)
    
    # Create a figure with Subplots
    fig = make_subplots(rows=5, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.02,
                        row_width=[0.105, 0.105, 0.105, 0.105, 0.5])

    # Add Candlestick to the figure
    fig.add_trace(go.Candlestick(x=df['date'],
                             open=df['open'],
                             high=df['high'],
                             low=df['low'],
                             close=df['close'],
                             name=""),
                  row=1, col=1)

    # Add indicators to the Candlestick Figure using the indicators_options dictionary
    for ind, ind_options in indicators_options_dict.items():
        fig.add_trace(go.Scatter(x=df['date'],
                                 y=df[ind],
                                 line=ind_options["line"],
                                 name=ind_options["name"],
                                 opacity=0.5,
                                 hoverinfo=ind_options["hoverinfo"]),
                       row=1, col=1)

    # Add Volume Subplot
    colors = ['green' if row['open'] - row['close'] <= 0 else 'red' for index, row in df.iterrows()]

    fig.add_trace(go.Bar(x=df['date'], y=df['volume'], name="Volume", marker_color=colors),
                  row=2, col=1)

    # Add RSI Subplot
    fig.add_trace(go.Scatter(x=df['date'], y=df['rsi'], name="RSI"),
                  row=3, col=1)

    # Add MACD Hist Subplot
    colors = ['green' if val >= 0 else 'red' for val in df.macd_hist]

    fig.add_trace(go.Bar(x=df['date'], y=df['macd_hist'], name="MACD", marker_color=colors),
                  row=4, col=1)

    # Add ATR Subplot
    fig.add_trace(go.Scatter(x=df['date'], y=df['atr'], name="ATR"),
                  row=5, col=1)

    # Update yaxis properties
    fig.update_yaxes(title_text="Stock Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_yaxes(title_text="RSI", range=[0, 100], tickvals=[30, 70], gridcolor='red', row=3, col=1)
    fig.update_yaxes(title_text="MACD", row=4, col=1)
    fig.update_yaxes(title_text="ATR", row=5, col=1)
    fig.update_yaxes(showspikes=True, # Show Horizontal spike line
                     spikecolor="black",
                     spikesnap="cursor",
                     spikemode="across",
                     spikedash="solid",
                     spikethickness = 1)

    # Update xaxis properties
    fig.update_xaxes(
                    rangebreaks=[dict(bounds=["sat", "mon"]), # Hide weekends
                                dict(values=["2022-12-25", "2023-01-01"])],  # Hide Christmas and New Year's
                    rangeselector=dict( # Create Range Selector to display different time scales
                        buttons=list([
                            dict(count=1, label="1m", step="month", stepmode="backward"),
                            dict(count=6, label="6m", step="month", stepmode="backward"),
                            dict(count=1, label="YTD", step="year", stepmode="todate"),              
                            dict(count=1, label="1y", step="year", stepmode="backward"),
                            dict(step="all")])),
                    dtick="M2", # Big grid lines every 2 months
                    tickformat="%b\n%Y",
                    showspikes=True, # Show Vertical spike line
                    spikecolor="black",
                    spikesnap="cursor",
                    spikemode="across",
                    spikedash="solid",
                    spikethickness = 1)

    # Update figure layout
    fig.update_layout(
        height = 1000,
        showlegend=False,
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis_rangeslider_visible=False,
        hovermode="x unified")

    # Spike line hover extended to all subplots
    fig.update_traces(xaxis="x1")    
    
    return fig