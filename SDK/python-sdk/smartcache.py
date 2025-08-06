import redis
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

class SmartCache:
    def __init__(self, redis_host, redis_port, redis_password,
                 kafka_broker="localhost:9092", topic="cache_access_logs", default_ttl=60):
        """
        Initialize SmartCache SDK.
        """
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True
        )
        self.default_ttl = default_ttl

        # Kafka Producer
        self.kafka_topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def set(self, key, value):
        self.redis_client.set(key, value, ex=self.default_ttl)
        event = {
            "resource_id": key,
            "action": "SET",
            "hit": False,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }
        self.producer.send(self.kafka_topic, event)
        print(f"✅ Cache SET: {key} -> TTL {self.default_ttl}s")

    def get(self, key):
        value = self.redis_client.get(key)
        hit = value is not None
        event = {
            "resource_id": key,
            "action": "GET",
            "hit": hit,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }
        self.producer.send(self.kafka_topic, event)

        if hit:
            print(f"✅ Cache HIT: {key}")
        else:
            print(f"❌ Cache MISS: {key}")
        return value
