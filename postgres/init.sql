CREATE TABLE IF NOT EXISTS sensor_readings (
    id                      SERIAL PRIMARY KEY,
    device_id               VARCHAR(50)  NOT NULL,
    timestamp               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    broker                  VARCHAR(10)  NOT NULL,  -- 'mqtt' ili 'kafka'

    -- senzorske vrednosti
    temperature             FLOAT,
    humidity                FLOAT,
    water_level             FLOAT,
    n_value                 FLOAT,        -- azot (N je rezervisana rec u SQL)
    p_value                 FLOAT,        -- fosfor
    k_value                 FLOAT,        -- kalijum

    -- aktuatori (true = ON, false = OFF)
    fan_actuator            BOOLEAN,
    watering_plant_pump     BOOLEAN,
    water_pump_actuator     BOOLEAN
);

CREATE INDEX idx_device_id  ON sensor_readings(device_id);
CREATE INDEX idx_timestamp  ON sensor_readings(timestamp);
CREATE INDEX idx_broker     ON sensor_readings(broker);