import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import asyncpg
import paho.mqtt.client as mqtt
from aiokafka import AIOKafkaConsumer

# ─── KONFIGURACIJA ────────────────────────────────────────────
MQTT_BROKER     = os.getenv("MQTT_BROKER", "localhost").replace("mqtt://", "")
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
DATABASE_URL    = os.getenv("DATABASE_URL", "postgresql://iot:iot123@localhost:5432/iotdb")
KAFKA_TOPIC     = "sensor-data"
MQTT_TOPIC      = "iot/sensors"
BATCH_SIZE      = 500
BATCH_TIMEOUT   = 5.0  # sekunde

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("storage")

# ─── GLOBALNI BUFFER ──────────────────────────────────────────
# Sve poruke (i MQTT i Kafka) idu u isti buffer
buffer: list[dict] = []
buffer_lock = asyncio.Lock()
db_pool = None

# ─── UPIS U BAZU ─────────────────────────────────────────────
async def flush_buffer(records: list[dict]):
    """Batch insert svih rekorda iz buffera u PostgreSQL."""
    if not records:
        return

    async with db_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO sensor_readings
                (device_id, timestamp, broker, temperature, humidity,
                 water_level, n_value, p_value, k_value,
                 fan_actuator, watering_plant_pump, water_pump_actuator)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            [
                (
                    r["device_id"],
                    datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")),
                    r["broker"],
                    r.get("temperature"),
                    r.get("humidity"),
                    r.get("water_level"),
                    r.get("n_value"),
                    r.get("p_value"),
                    r.get("k_value"),
                    r.get("fan_actuator"),
                    r.get("watering_plant_pump"),
                    r.get("water_pump_actuator"),
                )
                for r in records
            ],
        )
    log.info(f"Upisano {len(records)} rekorda u bazu")

# ─── BUFFER MENADŽER ──────────────────────────────────────────
async def add_to_buffer(message: dict, broker: str):
    """Dodaj poruku u buffer, upiši kad dostignemo BATCH_SIZE."""
    global buffer
    message["broker"] = broker

    async with buffer_lock:
        buffer.append(message)
        if len(buffer) >= BATCH_SIZE:
            to_flush = buffer.copy()
            buffer = []

    if len(to_flush if 'to_flush' in dir() else []) >= BATCH_SIZE:
        await flush_buffer(to_flush)

# ─── TIMEOUT FLUSH ────────────────────────────────────────────
async def periodic_flush():
    """Svakih BATCH_TIMEOUT sekundi upiši šta god ima u bufferu."""
    global buffer
    while True:
        await asyncio.sleep(BATCH_TIMEOUT)
        async with buffer_lock:
            if buffer:
                to_flush = buffer.copy()
                buffer = []
                log.info(f"Timeout flush: {len(to_flush)} rekorda")
                await flush_buffer(to_flush)

# ─── KAFKA CONSUMER ───────────────────────────────────────────
async def kafka_consumer_loop():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="storage-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    log.info("Kafka consumer pokrenut")
    try:
        async for msg in consumer:
            await add_to_buffer(msg.value, "kafka")
    finally:
        await consumer.stop()

# ─── MQTT CONSUMER ────────────────────────────────────────────
def start_mqtt(loop: asyncio.AbstractEventLoop):
    """MQTT je sinhroni — pokrecemo ga u posebnom threadu."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(client, userdata, flags, reason_code, properties):
        log.info(f"MQTT povezan, reason: {reason_code}")
        client.subscribe(MQTT_TOPIC, qos=1)

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8"))
            # Prebaci u async event loop
            asyncio.run_coroutine_threadsafe(
                add_to_buffer(data, "mqtt"), loop
            )
        except Exception as e:
            log.error(f"MQTT greška: {e}")

    client.on_connect = on_connect
    client.on_message = on_message
    mqtt_host = MQTT_BROKER.replace("mqtt://", "").replace("mosquitto", "mosquitto")
    client.connect("mosquitto", 1883, 60)
    client.loop_forever()

# ─── MAIN ─────────────────────────────────────────────────────
async def main():
    global db_pool

    log.info("Povezujem se na PostgreSQL...")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    log.info("PostgreSQL pool kreiran")

    # Pokreni MQTT u posebnom threadu
    import threading
    loop = asyncio.get_event_loop()
    mqtt_thread = threading.Thread(target=start_mqtt, args=(loop,), daemon=True)
    mqtt_thread.start()

    # Pokreni Kafka consumer i timeout flush konkurentno
    await asyncio.gather(
        kafka_consumer_loop(),
        periodic_flush(),
    )

if __name__ == "__main__":
    asyncio.run(main())