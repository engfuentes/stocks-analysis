import os, sys
import pandas as pd

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *
import pyspark.sql.functions as F

from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

from stocks_etl.operations.cassandra_operations import save_posts_to_cassandra
from stocks_etl.operations.spark_operations import create_spark_connection, get_reddit_df, \
    cast_types_and_get_subreddit, get_cashtags
from stocks_etl.utils.helper_functions import configured_logger

logger = configured_logger("reddit-pipeline")

# spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.18 pipelines/reddit_pipeline.py

def sentiment_analysis(df, column_analyze: str, column_results: str) -> "Dataframe":
    """Function that returns a DataFrame with the Sentiment Analysis of a column text.
    The text sentiment can be Positive, Neutral or Negative.
    The result for each row contains an Array with ["text sentiment", "score"] or ["Empty Text"] if text is empty
    Parameters
    ----------
        df (DataFrame): Spark Dataframe that contains the column to analyze.
        column_analyze (str): Name of the column that has to be analyzed.
        column_results (str): Name of the column that is going to be added to the DataFrame with the results.
    Returns
    -------
        df (DataFrame): Spark Dataframe that contains the new column.
    """

    def classifier_function(text, classifier):
        """Function that performs the Sentiment Analysis of a text.
        Parameters
        ----------
            text (str): Text to be analyzed.
            classifier (Pipeline): Hugging Face Pipeline used to classify the text.
        Returns
        -------
            (list): List with ["text sentiment", "score"] or ["Empty Text"].
        """

        if len(text) == 0:
            return ["Empty Text"]
        else:
            result = classifier(text)
            label_result = result[0]["label"].capitalize()
            score = str(round(result[0]["score"]*100,2))

            return [label_result, score]

    @pandas_udf(ArrayType(StringType()))
    def column_sentiment_analysis(serie: pd.Series) -> pd.Series:
        """pandas_udf that performs the sentiment analysis.
        Parameters
        ----------
            serie (pd.Series): Pandas Series of the column to analyze
        Returns
        -------
                  (pd.Series): pd.Series of Array with the result of the Sentiment Analysis.
        """
        # The following link recommends to use pandas_udf with hugging face
        # https://www.databricks.com/blog/2023/02/06/getting-started-nlp-using-hugging-face-transformers-pipelines.html
        
        # Create Hugging Face Pipeline
        classifier = create_hugging_face_pipeline()
        # Perform the Sentiment Analysis
        sentiment = serie.apply(classifier_function, args=(classifier,))
        
        return sentiment
    
    try:
        df = df.withColumn(column_results, column_sentiment_analysis(F.col(column_analyze)))
        logger.info("Performed the Sentiment Analysis")
        return df

    except Exception as e:
        logger.error(f"There was a error performing the Sentiment Analysis, error: {e}")

def create_hugging_face_pipeline() -> "Pipeline":
    """Function that creates a text classifier instance. First check if the model files are downloaded and if
    they are not it downloads them
    Returns
    -------
        classifier (Pipeline): Hugging Face Pipeline used to classify the text.
    """
    try:
        hugging_face_model = "mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
        model_path = "./data/hugging_face_models/distilroberta-finetuned-financial-news"

        # Check if the model files are downloaded
        if not os.path.exists(os.path.join(os.getcwd(), "data/hugging_face_models/distilroberta-finetuned-financial-news")):
            logger.info("Model is not saved in disk")
            # Download and load the model
            model = AutoModelForSequenceClassification.from_pretrained(hugging_face_model)
            tokenizer = AutoTokenizer.from_pretrained(hugging_face_model)
            
            # Save the model to the disk
            model.save_pretrained(model_path)
            tokenizer.save_pretrained(model_path)
            logger.info("Saving the model to the disk")

        # Load the model
        logger.info("Loading Hugging Face Model...")
        model = AutoModelForSequenceClassification.from_pretrained(model_path)
        tokenizer = AutoTokenizer.from_pretrained(model_path)
        
        # Create a Pipeline Classifier
        classifier = pipeline("text-classification", model=model, tokenizer=tokenizer, max_length=512, truncation=True)
        logger.info("Created the Hugging Face Pipeline")
        
        return classifier
    
    except Exception as e:
        logger.error(f"There was a error creating the Pipeline, error: {e}")

def reddit_pipeline():
    # Create a Spark Connection
    spark = create_spark_connection()

    if spark is not None:
        #  Kafka Topic
        kafka_topic = "reddit_posts"

        # Get the Reddit DataFrame
        reddit_df = get_reddit_df(spark, kafka_topic, "earliest")
  
        # Apply transformations
        reddit_df = cast_types_and_get_subreddit(reddit_df)
        reddit_df = get_cashtags(spark, reddit_df)
        reddit_df = sentiment_analysis(reddit_df, "title", "title_sentiment")
        reddit_df = sentiment_analysis(reddit_df, "selftext", "selftext_sentiment")
        # reddit_df.show(50)
        # reddit_df.printSchema()

        save_posts_to_cassandra(reddit_df, "stocks_data", "reddit_posts")
        
        # Close spark session
        spark.stop()

        # Query to console
        # query = (reddit_df.writeStream.format("console").outputMode("append").start())
        # query.awaitTermination()   

if __name__ == "__main__":
   reddit_pipeline()