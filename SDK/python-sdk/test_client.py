import random
import time
from smartcache import SmartCache

# ---------------- Initialize SmartCache SDK ----------------
cache = SmartCache(
    redis_host="localhost",     # Adjust if using Docker network
    redis_port=6379,
    redis_password=None,
    kafka_broker="localhost:9092"
)

# ---------------- Test Function ----------------
def stress_test(requests_count=200):
    keys = [f"resource:{i}" for i in range(1, 21)]  # 20 resource keys
    print(f"🚀 Starting stress test with {requests_count} requests...")

    for i in range(requests_count):
        action = random.choice(["SET", "GET"])

        key = random.choice(keys)
        if action == "SET":
            value = f"data-{random.randint(1000, 9999)}"
            cache.set(key, value)
        else:
            cache.get(key)

        # Small delay to simulate real traffic
        time.sleep(0.05)

    print("✅ Stress test completed!")

# ---------------- Run Test ----------------
if __name__ == "__main__":
    stress_test(200)
