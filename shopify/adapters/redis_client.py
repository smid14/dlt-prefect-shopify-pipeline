from shopify.settings import get_redis_secrets
import redis
import json
from dataclasses import dataclass, field
from datetime import datetime
import backoff 


class RedisClient:
    """
    A class representing a Redis client.

    Attributes:
        redis: The Redis connection object.

    Methods:
        __init__: Initializes the Redis client and establishes a connection.
        connect: Connects to the Redis server.
        save_data: Saves data to Redis.
        load_data: Loads data from Redis.
        close: Closes the Redis connection.
    """

    def __init__(self):
        """
        Initializes the Redis client and establishes a connection.
        """
        self.redis = None
        self.connect(get_redis_secrets()['REDIS_HOST'], get_redis_secrets()['REDIS_PORT'], get_redis_secrets()['REDIS_PASSWORD'])

    def connect(self, host, port, password):
        """
        Connects to the Redis server.

        Args:
            host: The Redis server host.
            port: The Redis server port.
            password: The Redis server password.
        """
        try:
            self.redis = redis.Redis(host=host, port=port, password=password, decode_responses=True)
            self.redis.ping()
            print("Connected to Redis successfully!")
        except Exception as e:
            print(f"Error connecting to Redis: {e}")

    @backoff.on_exception(backoff.expo,
                          (ConnectionError, TimeoutError),
                          max_tries=8,
                          jitter=backoff.full_jitter)
    def save_data(self, key, value):
        """
        Saves data to Redis.

        Args:
            key: The key to store the data.
            value: The value to be stored.
        """
        try:
            self.redis.set(key, json.dumps(value))
            print(f"Data saved: {key} = {value}")
        except Exception as e:
            print(f"Error saving data to Redis: {e}")

    @backoff.on_exception(backoff.expo,
                          (ConnectionError, TimeoutError),
                          max_tries=8,
                          jitter=backoff.full_jitter)
    def load_data(self, key):
        """
        Loads data from Redis.

        Args:
            key: The key to retrieve the data.

        Returns:
            The loaded data, or None if an error occurred.
        """
        try:
            value = self.redis.get(key)
            print(f"Data loaded: {key} = {value}")
            return json.loads(value)
        except Exception as e:
            print(f"Error loading data from Redis: {e}")
            return None
        
    def close(self):
        """
        Closes the Redis connection.
        """
        if self.redis:
            self.redis.close()

@dataclass
class Run:
    initial_value: str 
    last_value: str 
    unique_hashes: str 
    run_created_at: str = field(default_factory=datetime.now)

    def __post_init__(self):
        # Convert the datetime object to a string
        self.initial_value = self.initial_value.strftime('%Y-%m-%d %H:%M:%S')
        self.last_value = self.last_value.strftime('%Y-%m-%d %H:%M:%S')
        self.run_created_at = self.run_created_at.strftime('%Y-%m-%d %H:%M:%S')