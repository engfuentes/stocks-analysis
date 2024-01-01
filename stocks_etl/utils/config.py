
constants = {
 "reddit_api_key": {
    "reddit_secret_key": "REDDIT_SECRET_KEY",
    "reddit_client_id": "REDDIT_CLIENT_ID"
 },  
 "kafka": {
    "bootstrap_servers": "broker:29092",
    "schema_registry_url": "http://schema-registry:8081"
 },
 "alphavantag": {
    "alpha_vantage_api_key": "ALPHAVANTAGE_API_KEY"
 },
 "cassandra": {
    "hostname": "dse"
 },
 "reddit_post_fields": (
                        "id",
                        "title",
                        "selftext",
                        "score",
                        "num_comments",
                        "author",
                        "created_utc",
                        "url",
                        "permalink"
                     )
}