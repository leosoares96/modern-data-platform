import json
import random
import time
from datetime import datetime, UTC



while True:
    event = {
        "flight_id": random.randint(1000, 9999),
        "airline": random.choice(["LATAM", "GOL", "AZUL"]),
        "altitude": random.randint(1000, 40000),
        "speed": random.randint(200, 900),
        "timestamp": datetime.now(UTC).isoformat()
    }

    print(json.dumps(event))

    time.sleep(1)