const fs = require("fs");
const { parse } = require("csv-parse/sync");
const mqtt = require("mqtt");
const { Kafka } = require("kafkajs");

// ─── KONFIGURACIJA ────────────────────────────────────────────
const MQTT_BROKER = process.env.MQTT_BROKER || "mqtt://localhost:1883";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const KAFKA_TOPIC = "sensor-data";
const MQTT_TOPIC = "iot/sensors";
const NUM_DEVICES = parseInt(process.env.NUM_DEVICES || "10");
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS || "100");

// ─── UČITAJ CSV ───────────────────────────────────────────────
function loadDataset() {
  const raw = fs.readFileSync("./data.csv", "utf8");
  const records = parse(raw, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });

  // Transformiši svaki red u naš format
  return records.map((r) => ({
    temperature: parseFloat(r["tempreature"]), // typo je u datasetu
    humidity: parseFloat(r["humidity"]),
    water_level: parseFloat(r["water_level"]),
    n_value: parseFloat(r["N"]),
    p_value: parseFloat(r["P"]),
    k_value: parseFloat(r["K"]),
    fan_actuator: r["Fan_actuator_ON"] === "1",
    watering_plant_pump: r["Watering_plant_pump_ON"] === "1",
    water_pump_actuator: r["Water_pump_actuator_ON"] === "1",
  }));
}

// ─── GLAVNI PROGRAM ───────────────────────────────────────────
async function main() {
  console.log("Učitavam dataset...");
  const dataset = loadDataset();
  console.log(`Dataset učitan: ${dataset.length} redova`);

  // Kafka setup
  const kafka = new Kafka({
    clientId: "ingestion-service",
    brokers: [KAFKA_BROKER],
    retry: { retries: 10, initialRetryTime: 1000 },
  });
  const producer = kafka.producer();
  await producer.connect();
  console.log("Kafka producer povezan");

  // MQTT setup
  const mqttClient = mqtt.connect(MQTT_BROKER, {
    reconnectPeriod: 1000,
    connectTimeout: 10000,
  });
  await new Promise((resolve, reject) => {
    mqttClient.on("connect", () => {
      console.log("MQTT klijent povezan");
      resolve();
    });
    mqttClient.on("error", reject);
  });

  // Napravi N uređaja i pokreni svaki
  console.log(`Pokrećem ${NUM_DEVICES} uređaja, interval: ${INTERVAL_MS}ms`);

  for (let i = 0; i < NUM_DEVICES; i++) {
    const deviceId = `device_${String(i).padStart(4, "0")}`;

    // Svaki uređaj počinje od svoje pozicije u datasetu
    let rowIndex = i % dataset.length;

    setInterval(
      async () => {
        const row = dataset[rowIndex];
        rowIndex = (rowIndex + 1) % dataset.length;

        // Svakih 50 poruka simuliraj kritičnu temperaturu za merenje latencije
        const isCritical = Math.random() < 0.02; // 2% poruka je kritično

        const message = {
          device_id: deviceId,
          timestamp: new Date().toISOString(),
          sent_at: Date.now(), // za merenje latencije
          ...row,
          temperature: isCritical ? 55 : row.temperature,
        };

        const payload = JSON.stringify(message);

        // Pošalji na MQTT
        mqttClient.publish(MQTT_TOPIC, payload, { qos: 1 });

        // Pošalji na Kafka
        await producer.send({
          topic: KAFKA_TOPIC,
          messages: [{ key: deviceId, value: payload }],
        });
      },
      Math.floor(INTERVAL_MS + Math.random() * 50),
    ); // mali jitter da ne šalju svi tačno u isto vreme
  }

  console.log("Svi uređaji aktivni. Ctrl+C za stop.");
}

main().catch((err) => {
  console.error("Greška:", err);
  process.exit(1);
});
