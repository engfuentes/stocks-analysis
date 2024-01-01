import pandas as pd
import yfinance as yf
import pandas as pd

from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app_modules.dash_operations import create_cassandra_connection_to_keyspace, get_list_stocks, \
    get_reddit_posts, get_table_ticker_info, get_ticker_indicators, create_interactive_graph

# Create app
app = Dash(__name__, external_stylesheets=[dbc.themes.ZEPHYR], suppress_callback_exceptions=True)

# Create Cassandra Session
session = create_cassandra_connection_to_keyspace(keyspace_name="stocks_data")

# Get a list of tickers that have Reddit Posts
list_stocks = get_list_stocks(session)

# Reddit Posts page layout
reddit_posts_layout = dbc.Container(children=[
     
    dbc.Row([dbc.Col([dcc.Dropdown(list_stocks, list_stocks[0], id="reddit-stocks-dropdown"),
                      html.Div(id="stock-accordion", className="stock-accordion"),
                      html.Div(id="stock-data-table")],
                      width=3),
             dbc.Col([html.Div(id="reddit-posts-accordion"),
                      dcc.Graph(id="candlestick-fig")],
                      width=9)])
             ])  

# Stocks Analysis page layout
stocks_analysis_layout = html.Div(
    children=[html.H1(children="This is going to be the stocks analysis page")]
)

# Navbar item
navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Reddit Posts", href="/")),
        #dbc.NavItem(dbc.NavLink("Stocks Analysis", href="/stocks_analysis")),
    ],
    brand="Stocks Analysis App",
    color="dark",
    dark=True,
    className="mb-2",
)

# App layout with the navbar and the page content
app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        navbar,
        dbc.Container(id="page-content", className="mb-4", fluid=True),
    ]
)

@callback(
    Output("reddit-posts-accordion", "children"),
    Input("reddit-stocks-dropdown", "value"))
def update_reddit_posts(value):
    """Function to update the accordion with the Reddit Posts of the chosen ticker"""
    
    # Get list of ticker's posts 
    posts_list = get_reddit_posts(session, value)

    # Create list of accordion items
    list_accordion_items = []
    for post in posts_list:
        if len(post['selftext_sentiment']) == 2:
            text_sentiment_str = f"**Text Sentiment Analysis:** {post['selftext_sentiment'][1]}, {post['selftext_sentiment'][0]} %"
        else:
            text_sentiment_str = ""

        accordion_item = dbc.AccordionItem([
                                dcc.Markdown(f"""
                                **Title Sentiment Analysis:** {post['title_sentiment'][1]}, {post['title_sentiment'][0]} %  
                                {text_sentiment_str}  
                                **Datetime (UTC):** {post['created_utc']}  
                                **Score:** {post['score']}  
                                **Number of comments:** {post['num_comments']}  
                                **Author:** {post['author']}  
                                """),
                                html.Hr(),
                                html.P(post['selftext'])
                            ],
                            title=post["title"],
                            class_name="reddit-accordion")
        
        list_accordion_items.append(accordion_item)

    # Create accordion with the posts
        accordion = html.Div(dbc.Accordion(list_accordion_items, start_collapsed=True))

    return accordion

@callback(
    Output("candlestick-fig", "figure"),
    Input("reddit-stocks-dropdown", "value"))
def update_candlestick_graph(value):
    """Function to update the Candlestick and indicators figure of the chosen ticker"""
    
    # Get ticker DataFrame
    ticker_indicators_df = get_ticker_indicators(session, value)

    # Create Figure
    fig = create_interactive_graph(ticker_indicators_df)

    return fig

@callback(
    Output("stock-data-table", "children"),
    Output("stock-accordion", "children"),
    Input("reddit-stocks-dropdown", "value"))
def update_stock_info(value):
    """Function to update the accordion and table information of the chosen ticker"""
    
    # Get the ticker table, summary and booleans
    ticker_table_dict, ticker_summary_dict, stock_bool, etf_bool = get_table_ticker_info(session, value)

    # If the ticker is a stock
    if stock_bool == True:
        # Create table body
        table_body = [html.Tbody([html.Tr([html.Td(k), html.Td(v)]) for (k,v) in ticker_table_dict.items()])]

        # Create Dash Bootstrap table
        table = dbc.Table(table_body, bordered=True, hover=True, class_name="stocks-table")

        # Create Accordion with the stock summary
        accordion = html.Div(
                    dbc.Accordion(
                        [
                        dbc.AccordionItem(
                            [html.H6(f"Industry: {ticker_summary_dict['industry']}"),
                            html.H6(f"Sector: {ticker_summary_dict['sector']}"),
                            html.A(ticker_summary_dict["website"], href=ticker_summary_dict["website"], target="_blank"),
                            html.Hr(),
                            html.P(ticker_summary_dict["longbusinesssummary"])
                            ],
                            title=ticker_summary_dict["shortname"],
                            class_name="stocks-accordion"
                        )
                        ], start_collapsed=True))
    
    # If the ticker is an ETF
    if etf_bool == True:
        # Create table body
        table_body = [html.Tbody([html.Tr([html.Td(k), html.Td(v)]) for (k,v) in ticker_table_dict.items()])]

        # Create Dash Bootstrap table
        table = dbc.Table(table_body, bordered=True, hover=True, class_name="stocks-table")

        # Create Accordion with the ETF summary
        accordion = html.Div(
                    dbc.Accordion(
                        [
                        dbc.AccordionItem(
                            [html.H6(f"Legal Type: {ticker_summary_dict['legaltype']}"),
                            html.H6(f"Fund Family: {ticker_summary_dict['fundfamily']}"),
                            html.H6(f"Category: {ticker_summary_dict['category']}"),
                            html.Hr(),
                            html.P(ticker_summary_dict["longbusinesssummary"])
                            ],
                            title=ticker_summary_dict["shortname"],
                            class_name="stocks-accordion"
                        )
                        ], start_collapsed=True))

    return table, accordion

@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    """Function used to display the different pages of the app"""
    if pathname == "/":
        return reddit_posts_layout
    elif pathname == "/stocks_analysis":
        return stocks_analysis_layout
    else:
        return dbc.Jumbotron(
            [
                html.H1("404: Not found", className="text-danger"),
                html.Hr(),
                html.P(f"The pathname {pathname} was not recognized..."),
            ]
        )

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8050)