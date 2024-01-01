import sys, json
import praw
from praw import Reddit

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from stocks_etl.utils.config import constants
from stocks_etl.utils.helper_functions import configured_logger

logger = configured_logger("reddit_producer")

# Load Constants from the config.py file  
REDDIT_POST_FIELDS = constants["reddit_post_fields"]
REDDIT_CLIENT_ID = constants["reddit_api_key"]["reddit_client_id"]
REDDIT_SECRET_KEY = constants["reddit_api_key"]["reddit_secret_key"]
SCHEMA_REGISTRY_URL = constants["kafka"]["schema_registry_url"]
BOOTSTRAP_SERVERS = constants["kafka"]["bootstrap_servers"]

# Reddit
def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    """Function used to connect to Reddit using praw
    Parameters
    ----------
        client_id (str): Client Id to connect to Reddit.
        client_secret (str): Client Secret to connect to Reddit.
        user_agent (str): User Agent to connect to Reddit.
    Returns
    -------
        reddit (Reddit): Reddit instance.
    """
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        logger.info("Connected to Reddit!")
        return reddit
    
    except Exception as e:
        logger.error(f"There was an error to connect to Reddit: {e}")
        sys.exit(1)

def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None) -> list:
    """Function used to extract posts from Reddit
    Parameters
    ----------
        reddit_instance (Reddit): Reddit instance.
        subreddit (str): Subreddit to extract the posts from.
        time_filter (str): Can be one of: "all", "day", "hour", "month", "week", or "year" (default: "all").
        limit (int|None): Maximum number of posts to get (default: None).
    Returns
    -------
        post_list (list): List containing dicts with Reddit posts.
    """
    
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_list = []

    for post in posts:
        post_dict = vars(post) # The vars() method returns the __dict__ (dictionary mapping) attribute of the given object.
        post = {key: str(post_dict[key]) for key in REDDIT_POST_FIELDS}
        post_list.append(post)

    logger.info(f"Returning the post list with {len(post_list)} posts...")
    
    return post_list

# Kafka
def delivery_report(err, msg):
    """Function that reports the failure or success of a message delivery to a kafka topic.
    Parameters:
    ----------
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        logger.error(f'Delivery failed on reading for {msg.key().decode("utf8")}: {err}')
    else:
        logger.info(f'Reddit post for key: {msg.key().decode("utf8")}, produced to topic: {msg.topic()} [{msg.partition()}] at offset { msg.offset()}')

def create_kafka_topic_register_schema(topic_name):
    """Function Creates a Kafka Topic and register its Schema in the Schema Registry
    Parameters
    ----------
        topic_name (str): Kafka topic.
    """
    
    kafka_config_local = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    
    # Create Admin Instance
    admin = AdminClient(kafka_config_local)

    # Create topic and register the schema if it doesn't exist
    if not topic_exists(admin, topic_name):
        create_topic(admin, topic_name)
    
        schema_registry_subject = f"{topic_name}-value"

        # Get the schema as str    
        with open(f"./dags/stocks_etl/schemas/{topic_name}_schema.avsc") as file:
            schema_str = file.read()
        
        # Register Schema in the Schema Registry
        register_schema(SCHEMA_REGISTRY_URL, schema_registry_subject, schema_str)
    
def topic_exists(admin, topic):
    """Function that checks if a topic exists.
    Parameters
    ----------
        admin (AdminClient): AdminClient instance.
        topic (str): Kafka topic.
    Returns
    -------
        (bool): True if the topic exists.
    """
    
    metadata = admin.list_topics()
    for t in iter(metadata.topics.values()):
        if t.topic == topic:
            return True
    return False    

def create_topic(admin, topic):
    """Function that creates a Kafka topic
    Parameters
    ----------
        admin (AdminClient): AdminClient instance.
        topic (str): Kafka topic.
    """
    
    # Create NewTopic Instance
    new_topic = NewTopic(topic, num_partitions=5, replication_factor=1) 
    
    # Create the topic and get a result_dict
    result_dict = admin.create_topics([new_topic])
    
    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            logger.info(f"Kafka topic '{topic}' created")
        except Exception as e:
            logger.info(f"Failed to create Kafka topic '{topic}': {e}")

def register_schema(schema_registry_url: str, schema_registry_subject: str, schema_str: str):
    """Function that Registers an Avro Schema in the Schema Registry a Kafka topic
    Parameters
    ----------
        schema_registry_url (str): url to connect to the Schema Registry.
        schema_registry_subject (str): Schema registry subject: "topic-value" or "topic-key".
        schema_str (str): Schema to register in str format.
    """
    try:
        sr = SchemaRegistryClient({'url': schema_registry_url})
        schema = Schema(schema_str, schema_type="AVRO")
        schema_id = sr.register_schema(subject_name=schema_registry_subject, schema=schema)

        logger.info(f"Schema Registered in the Schema Registry with schema_id: {schema_id}")

    except Exception as e:
        logger.error(f"Couldnt't Register the Schema due to: {e}")

def get_schema_from_schema_registry(schema_registry_url: str, schema_registry_subject: str):
    """Function used create Schema Registry Client and to get the latest version of the Schema
    Parameters
    ----------
        schema_registry_url (str): url to connect to the Schema Registry.
        schema_registry_subject (str): Schema registry subject: "topic-value" or "topic-key".
    Returns
    -------
        sr (SchemaRegistryClient): Instance of SchemaRegistryClient.
        latest_version (RegisteredSchema): Instance of RegisteredSchema.
    """
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version

def get_avro_serializer(topic: str) -> AvroSerializer:
    """Function that returns an AvroSerializer instance. It uses a Registered Schema in the Kafka cluster
    Parameters
    ----------
        topic (str): Kafka topic.
    Returns
    -------
        avro_serializer (AvroSerializer): Instance of AvroSerializer.
    """
    
    schema_registry_subject = f"{topic}-value"

    sr, latest_version = get_schema_from_schema_registry(SCHEMA_REGISTRY_URL, schema_registry_subject)

    # Create an AvroSerializer instance with the Schema that was created in the Kafka CLuster
    avro_serializer = AvroSerializer(schema_registry_client=sr,
                                     schema_str=latest_version.schema.schema_str,
                                     conf={'auto.register.schemas': False})
    
    return avro_serializer

def stream_to_kafka(subreddit: str, time_filter: str, limit, topic: str):
    """Function that Extract posts from Reddit and produce messages to a Kafka Cluster
    Parameters
    ----------
        subreddit (str): Subreddit to extract the posts from.
        time_filter (str): Can be one of: "all", "day", "hour", "month", "week", or "year" (default: "all").
        limit (int|None): Maximum number of posts to get (default: None).
        topic (str): Kafka topic.
    """
    # Get an AvroSerializer
    avro_serializer = get_avro_serializer(topic)

    kafka_config_local = {'bootstrap.servers': BOOTSTRAP_SERVERS}

    producer = Producer(kafka_config_local)

    try:
        # Connect to Reddit
        instance = connect_reddit(REDDIT_CLIENT_ID, REDDIT_SECRET_KEY, 'rfuentes')

        # Extraction (List of posts dictionaries)
        list_posts = extract_posts(instance, subreddit, time_filter, limit)

        for post in list_posts:
            #print(json.dumps(post,indent=4))
            producer.produce(topic=topic,
                             key=post["author"],
                             value=avro_serializer(post, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

        producer.flush()
    except Exception as e:
        logger.error(f"An error ocurred: {e}")