import type { Metrics } from "@/lib/oee-sim";

export const BROKER_URL =
  "wss://e6fc009f759842338bc9b4a61b9ca0a7.s1.eu.hivemq.cloud:8884/mqtt";
export const MQTT_USER = "mqtt-fall24";
export const MQTT_PASSWORD = "w@terHouse5";

type Publisher = {
  publish: (
    topic: string,
    message: string,
    options: { retain: boolean; qos: 0 },
  ) => void;
};

function num(value: number, digits: number): string {
  return value.toFixed(digits);
}

export function publishMetrics(client: Publisher, base: string, m: Metrics) {
  const send = (leaf: string, message: string) => {
    client.publish(base + leaf, message, { retain: true, qos: 0 });
  };

  send(
    "json",
    JSON.stringify({
      availability: Number(num(m.availability, 2)),
      performance: Number(num(m.performance, 2)),
      quality: Number(num(m.quality, 2)),
      oee: Number(num(m.oee, 2)),
      speedPercent: Number(num(m.speed, 1)),
      goodCount: m.good,
      badCount: m.bad,
      totalCount: m.total,
      running: m.running,
      scrapQueued: m.scrapQueued,
      runtimeSec: Number(num(m.runtimeSec, 1)),
      downtimeSec: Number(num(m.downtimeSec, 1)),
    }),
  );
  send("availability", num(m.availability, 2));
  send("performance", num(m.performance, 2));
  send("quality", num(m.quality, 2));
  send("oee", num(m.oee, 2));
  send("speedPercent", num(m.speed, 1));
  send("goodCount", String(m.good));
  send("badCount", String(m.bad));
  send("totalCount", String(m.total));
  send("running", m.running ? "true" : "false");
  send("scrapQueued", String(m.scrapQueued));
  send("status", "Online");
}
