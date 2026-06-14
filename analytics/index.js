const mqtt = require("mqtt");
const { Kafka } = require("kafkajs");

// ─── KONFIGURACIJA ────────────────────────────────────────────
const MQTT_BROKER = process.env.MQTT_BROKER || "mqtt://localhost:1883";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const KAFKA_TOPIC = "sensor-data";
const MQTT_TOPIC = "iot/sensors";
const WINDOW_MS = 10000; // 10 sekundi
const ALERT_THRESHOLD = parseFloat(process.env.ALERT_THRESHOLD || "50");

// ─── TUMBLING WINDOW STANJE ───────────────────────────────────
// Dva nezavisna prozora — jedan za MQTT, jedan za Kafka
const windows = {
  mqtt: { readings: [], windowStart: Date.now() },
  kafka: { readings: [], windowStart: Date.now() },
};

// ─── MERENJE LATENCIJE ────────────────────────────────────────
function calcLatency(readings, filterFn) {
  const now = Date.now();
  const latencies = readings
    .filter((r) => filterFn(r) && r.sent_at && now - r.sent_at < 30000)
    .map((r) => now - r.sent_at);

  if (latencies.length === 0) return null;

  return {
    avg: (latencies.reduce((a, b) => a + b, 0) / latencies.length).toFixed(0),
    min: Math.min(...latencies),
    max: Math.max(...latencies),
  };
}

// ─── OBRADA PROZORA ───────────────────────────────────────────
function processWindow(broker) {
  const win = windows[broker];
  const now = Date.now();

  // Koliko vremena je prošlo od početka prozora
  if (now - win.windowStart < WINDOW_MS) return;

  const count = win.readings.length;

  if (count === 0) {
    console.log(`[${broker.toUpperCase()}] Prozor završen — nema poruka`);
    win.windowStart = now;
    return;
  }

  // Izračunaj proseke za sve senzore
  const avgTemp = win.readings.reduce((s, r) => s + r.temperature, 0) / count;
  const avgHumidity = win.readings.reduce((s, r) => s + r.humidity, 0) / count;
  const avgWater = win.readings.reduce((s, r) => s + r.water_level, 0) / count;

  const windowEnd = new Date().toISOString();

  console.log(
    `[${broker.toUpperCase()}] Prozor ${new Date(win.windowStart).toISOString()} → ${windowEnd} | ` +
      `Poruka: ${count} | ` +
      `Avg temp: ${avgTemp.toFixed(2)}°C | ` +
      `Avg humidity: ${avgHumidity.toFixed(2)}% | ` +
      `Avg water: ${avgWater.toFixed(2)}`,
  );

  // Proveri prag — ALARM
  // Temperatura
  if (avgTemp > ALERT_THRESHOLD) {
    const lat = calcLatency(
      win.readings,
      (r) => r.temperature > ALERT_THRESHOLD,
    );
    const latStr = lat
      ? `Latencija: avg=${lat.avg}ms min=${lat.min}ms max=${lat.max}ms`
      : "";
    console.log(
      `🚨 ALARM [${broker.toUpperCase()}] Visoka temperatura ${avgTemp.toFixed(2)}°C | ${latStr}`,
    );
  }

  // Voda
  if (avgWater < 20) {
    const lat = calcLatency(win.readings, (r) => r.water_level < 20);
    const latStr = lat
      ? `Latencija: avg=${lat.avg}ms min=${lat.min}ms max=${lat.max}ms`
      : "";
    console.log(
      `🚨 ALARM [${broker.toUpperCase()}] Nizak nivo vode: ${avgWater.toFixed(2)} | ${latStr}`,
    );
  }

  // Vlažnost
  if (avgHumidity < 30) {
    const lat = calcLatency(win.readings, (r) => r.humidity < 30);
    const latStr = lat
      ? `Latencija: avg=${lat.avg}ms min=${lat.min}ms max=${lat.max}ms`
      : "";
    console.log(
      `🚨 ALARM [${broker.toUpperCase()}] Niska vlažnost: ${avgHumidity.toFixed(2)}% | ${latStr}`,
    );
  }

  // Hraniva
  const avgN = win.readings.reduce((s, r) => s + r.n_value, 0) / count;
  const avgP = win.readings.reduce((s, r) => s + r.p_value, 0) / count;
  const avgK = win.readings.reduce((s, r) => s + r.k_value, 0) / count;

  if (avgN < 50) {
    const lat = calcLatency(win.readings, (r) => r.n_value < 50);
    const latStr = lat
      ? `Latencija: avg=${lat.avg}ms min=${lat.min}ms max=${lat.max}ms`
      : "";
    console.log(
      `🚨 ALARM [${broker.toUpperCase()}] Nizak azot (N): ${avgN.toFixed(2)} | ${latStr}`,
    );
  }
  if (avgP < 50) {
    const lat = calcLatency(win.readings, (r) => r.p_value < 50);
    const latStr = lat
      ? `Latencija: avg=${lat.avg}ms min=${lat.min}ms max=${lat.max}ms`
      : "";
    console.log(
      `🚨 ALARM [${broker.toUpperCase()}] Nizak fosfor (P): ${avgP.toFixed(2)} | ${latStr}`,
    );
  }
  if (avgK < 50) {
    const lat = calcLatency(win.readings, (r) => r.k_value < 50);
    const latStr = lat
      ? `Latencija: avg=${lat.avg}ms min=${lat.min}ms max=${lat.max}ms`
      : "";
    console.log(
      `🚨 ALARM [${broker.toUpperCase()}] Nizak kalijum (K): ${avgK.toFixed(2)} | ${latStr}`,
    );
  }

  // Reset prozora
  win.readings = [];
  win.windowStart = now;
}

// ─── DODAJ PORUKU U PROZOR ────────────────────────────────────
function addReading(data, broker) {
  if (typeof data.temperature !== "number") return;
  windows[broker].readings.push(data);
  processWindow(broker);
}

// ─── KAFKA CONSUMER ───────────────────────────────────────────
async function startKafka() {
  const kafka = new Kafka({
    clientId: "analytics-service",
    brokers: KAFKA_BROKER.split(","),
    retry: { retries: 10, initialRetryTime: 1000 },
  });

  const consumer = kafka.consumer({ groupId: "analytics-group" });
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  console.log("Kafka consumer pokrenut");

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        addReading(data, "kafka");
      } catch (e) {
        console.error("Kafka greška:", e.message);
      }
    },
  });
}

// ─── MQTT CONSUMER ────────────────────────────────────────────
function startMqtt() {
  const client = mqtt.connect(MQTT_BROKER, {
    reconnectPeriod: 1000,
  });

  client.on("connect", () => {
    console.log("MQTT consumer pokrenut");
    client.subscribe(MQTT_TOPIC, { qos: 1 });
  });

  client.on("message", (topic, payload) => {
    try {
      const data = JSON.parse(payload.toString());
      addReading(data, "mqtt");
    } catch (e) {
      console.error("MQTT greška:", e.message);
    }
  });
}

// ─── TIMER — proveri prozore svake sekunde ────────────────────
function startWindowTimer() {
  setInterval(() => {
    processWindow("mqtt");
    processWindow("kafka");
  }, 1000);
}

// ─── MAIN ─────────────────────────────────────────────────────
async function main() {
  console.log(
    `Analytics pokrenut | Window: ${WINDOW_MS}ms | Prag: ${ALERT_THRESHOLD}°C`,
  );
  startMqtt();
  startWindowTimer();
  await startKafka();
}

main().catch((err) => {
  console.error("Greška:", err);
  process.exit(1);
});
