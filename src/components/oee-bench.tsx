import { useEffect, useRef, useState } from "react";
import {
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Download, RotateCcw } from "lucide-react";
import type { MqttClient } from "mqtt";
import {
  TOPIC_LEAVES,
  blueLit,
  createSim,
  metrics,
  pressDefect,
  pressStart,
  pressStop,
  step,
  topicPath,
  DEFAULT_TOPIC,
  normalizeTopic,
  type Metrics,
  type Sample,
  type Sim,
} from "@/lib/oee-sim";
import { BROKER_URL, MQTT_PASSWORD, MQTT_USER, publishMetrics } from "@/lib/oee-broker";
import { WidgetLine } from "@/components/widget-line";

type View = {
  metrics: Metrics;
  running: boolean;
  blue: boolean;
  log: { id: number; text: string }[];
  samples: Sample[];
  balanced: boolean;
};

function snapshot(sim: Sim, now: number): View {
  const m = metrics(sim, now);
  return {
    metrics: m,
    running: sim.running,
    blue: blueLit(sim, now),
    log: sim.log.map((line) => ({ ...line })),
    samples: sim.samples.map((sample) => ({ ...sample })),
    balanced: sim.good + sim.bad === sim.total,
  };
}

function pct(value: number): string {
  return value.toFixed(1);
}

function clock(seconds: number): string {
  const whole = Math.max(0, Math.floor(seconds));
  const minutes = Math.floor(whole / 60);
  const remain = whole % 60;
  return `${minutes}:${remain.toString().padStart(2, "0")}`;
}

function Meter({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone: "fg" | "run" | "stop" | "scrap";
}) {
  const width = Math.max(0, Math.min(100, (value / 130) * 100));
  const bar =
    tone === "run"
      ? "bg-run"
      : tone === "stop"
        ? "bg-stop"
        : tone === "scrap"
          ? "bg-scrap"
          : "bg-fg";
  return (
    <div className="grid grid-cols-[5.5rem_1fr_3.5rem] items-center gap-3">
      <span className="text-sm text-muted">{label}</span>
      <span className="relative h-2 overflow-hidden rounded-full bg-surface-2">
        <span className="absolute inset-y-0 left-[76.9%] w-px bg-border" />
        <span className={`absolute inset-y-0 left-0 rounded-full ${bar}`} style={{ width: `${width}%` }} />
      </span>
      <span className="text-right font-display text-xl leading-none tabular-nums text-fg">
        {pct(value)}
      </span>
    </div>
  );
}

function Led({ on, tone }: { on: boolean; tone: "run" | "stop" | "scrap" }) {
  const onClass = tone === "run" ? "bg-run" : tone === "stop" ? "bg-stop" : "bg-scrap";
  return (
    <span
      className={`mx-auto block size-3.5 rounded-full border border-border ${on ? onClass : "bg-bg"}`}
      aria-hidden="true"
    />
  );
}

export function OeeBench() {
  const simRef = useRef<Sim | null>(null);
  const simEpochRef = useRef(-1);
  const epochRef = useRef(0);
  const speedRef = useRef(100);
  const topicRef = useRef(DEFAULT_TOPIC);
  const clientRef = useRef<MqttClient | null>(null);
  const [epoch, setEpoch] = useState(0);
  const [mounted, setMounted] = useState(false);
  const [speed, setSpeed] = useState(100);
  const [topic, setTopic] = useState(DEFAULT_TOPIC);
  const [copied, setCopied] = useState(false);
  const [broker, setBroker] = useState<"connecting" | "live" | "offline">("connecting");
  const [echo, setEcho] = useState("");
  const [view, setView] = useState<View>(() => snapshot(createSim(0), 0));

  epochRef.current = epoch;
  topicRef.current = topic;

  useEffect(() => {
    const saved = localStorage.getItem("eet124-oee-student");
    if (saved) setTopic(saved.includes("/") ? saved : topicPath(saved));
    setMounted(true);
  }, []);

  useEffect(() => {
    const now = performance.now();
    if (!simRef.current || simEpochRef.current !== epoch) {
      const sim = createSim(now);
      sim.speed = speedRef.current;
      simRef.current = sim;
      simEpochRef.current = epoch;
      setView(snapshot(sim, now));
    }
    const id = window.setInterval(() => {
      const sim = simRef.current;
      if (!sim) return;
      const tick = performance.now();
      step(sim, tick);
      setView(snapshot(sim, tick));
    }, 100);
    return () => window.clearInterval(id);
  }, [epoch]);

  useEffect(() => {
    let stopped = false;
    let client: MqttClient | null = null;
    let subscribed = "";

    const push = () => {
      const live = clientRef.current;
      if (!live?.connected) return;
      const sim = simRef.current;
      // A freshly opened page sits at 100 OEE and zero counts. Publishing that
      // retained snapshot overwrites a live run. Only the started shift talks.
      if (!sim?.shiftStarted) return;
      const base = normalizeTopic(topicRef.current);
      const oeeTopic = `${base}oee`;
      if (subscribed !== oeeTopic) {
        if (subscribed) client?.unsubscribe(subscribed);
        subscribed = oeeTopic;
        live.subscribe(oeeTopic);
      }
      publishMetrics(live, base, metrics(sim, performance.now()));
    };

    void import("mqtt").then((mod) => {
      if (stopped) return;
      const mqtt = mod.default;
      const connected = mqtt.connect(BROKER_URL, {
        username: MQTT_USER,
        password: MQTT_PASSWORD,
        clientId: `OEE-web-${Math.random().toString(16).slice(2, 10)}`,
        reconnectPeriod: 4000,
        connectTimeout: 10000,
        protocolVersion: 4,
      });
      client = connected;
      clientRef.current = connected;
      connected.on("connect", () => {
        if (stopped) return;
        setBroker("live");
        push();
      });
      connected.on("reconnect", () => {
        if (!stopped) setBroker("connecting");
      });
      connected.on("close", () => {
        if (!stopped) setBroker("offline");
      });
      connected.on("error", () => {
        if (!stopped) setBroker("offline");
      });
      connected.on("message", (topic, payload) => {
        if (topic.endsWith("/oee")) setEcho(payload.toString());
      });
    });

    const id = window.setInterval(push, 1000);
    return () => {
      stopped = true;
      window.clearInterval(id);
      clientRef.current = null;
      client?.end(true);
    };
  }, []);

  function liveSim(): Sim {
    const now = performance.now();
    if (!simRef.current || simEpochRef.current !== epochRef.current) {
      const sim = createSim(now);
      sim.speed = speedRef.current;
      simRef.current = sim;
      simEpochRef.current = epochRef.current;
    }
    return simRef.current;
  }

  function onSpeed(next: number) {
    speedRef.current = next;
    setSpeed(next);
    if (simRef.current) simRef.current.speed = next;
  }

  function onTopic(value: string) {
    setTopic(value);
    localStorage.setItem("eet124-oee-student", value);
  }

  function act(fn: (sim: Sim, now: number) => void) {
    const sim = liveSim();
    const now = performance.now();
    fn(sim, now);
    setView(snapshot(sim, now));
  }

  const path = normalizeTopic(topic);
  const m = view.metrics;
  const line = `const char* TOPIC_PATH = "${path}";`;

  async function copyLine() {
    try {
      await navigator.clipboard.writeText(line);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1400);
    } catch {
      setCopied(false);
    }
  }

  return (
    <main className="mx-auto flex w-full max-w-5xl flex-col gap-5 px-4 py-5 sm:px-6 sm:py-8">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-display text-sm tracking-widest text-muted uppercase">EET 124</p>
          <h1 className="font-display text-4xl leading-none text-fg sm:text-5xl">Line OEE</h1>
          <p className="mt-2 max-w-xl text-sm text-muted">
            Pot at 100, no Stop, and no scrap is 100 OEE. The shift clock starts
            when you press Start, so waiting on a stopped line is not downtime.
          </p>
          <p className="mt-2 text-sm">
            <span className={broker === "live" ? "text-run" : broker === "offline" ? "text-stop" : "text-muted"}>
              {broker === "live"
                ? view.running || view.metrics.plannedSec > 0
                  ? "Publishing this run to HiveMQ"
                  : "Connected — press Start to publish"
                : broker === "connecting"
                  ? "Connecting to HiveMQ"
                  : "HiveMQ offline"}
            </span>
            {broker === "live" && echo ? <span className="text-muted"> · broker echoed OEE {echo}</span> : null}
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => setEpoch((n) => n + 1)}
            className="inline-flex h-11 items-center gap-2 rounded-full border border-border bg-surface px-4 text-sm text-fg"
          >
            <RotateCcw className="size-4" aria-hidden="true" />
            New shift
          </button>
          <a
            href="/XIAO_ESP32C3_OEE.ino"
            download
            className="inline-flex h-11 items-center gap-2 rounded-full bg-fg px-4 text-sm font-medium text-ink"
          >
            <Download className="size-4" aria-hidden="true" />
            Sketch
          </a>
        </div>
      </header>

      <WidgetLine
        running={view.running}
        speed={speed}
        total={m.total}
        bad={m.bad}
        scrapQueued={m.scrapQueued}
      />

      <section className="grid gap-4 lg:grid-cols-[18rem_1fr]">
        <div className="rounded-2xl border border-border bg-surface p-4">
          <div className="mb-4 flex items-baseline justify-between">
            <h2 className="font-display text-2xl leading-none">Station</h2>
            <span className={`font-display text-xl leading-none ${m.running ? "text-run" : "text-stop"}`}>
              {m.running ? "RUNNING" : "STOPPED"}
            </span>
          </div>

          <div className="grid grid-cols-3 gap-2">
            <div className="flex flex-col gap-2">
              <Led on={view.blue} tone="scrap" />
              <button
                type="button"
                onClick={() => act(pressDefect)}
                className="h-16 rounded-xl bg-scrap text-sm font-semibold text-ink"
              >
                Defect
              </button>
              <span className="text-center text-xs text-muted">D4 blue</span>
            </div>
            <div className="flex flex-col gap-2">
              <Led on={view.running} tone="run" />
              <button
                type="button"
                onClick={() => act(pressStart)}
                aria-pressed={view.running}
                className={`h-16 rounded-xl bg-run text-sm font-semibold text-ink ${view.running ? "ring-2 ring-fg ring-offset-2 ring-offset-surface" : ""}`}
              >
                Start
              </button>
              <span className="text-center text-xs text-muted">D5 green</span>
            </div>
            <div className="flex flex-col gap-2">
              <Led on={!view.running} tone="stop" />
              <button
                type="button"
                onClick={() => act(pressStop)}
                aria-pressed={!view.running}
                className={`h-16 rounded-xl bg-stop text-sm font-semibold text-ink ${!view.running ? "ring-2 ring-fg ring-offset-2 ring-offset-surface" : ""}`}
              >
                Stop
              </button>
              <span className="text-center text-xs text-muted">D6 red</span>
            </div>
          </div>

          <label className="mt-5 block">
            <span className="flex items-baseline justify-between">
              <span className="text-sm text-muted">Speed pot · A0</span>
              <span className="font-display text-3xl leading-none tabular-nums">{pct(speed)}%</span>
            </span>
            <input
              type="range"
              min={80}
              max={120}
              step={0.5}
              value={speed}
              onChange={(event) => onSpeed(Number(event.target.value))}
              aria-label="Machine speed percent"
              className="mt-3 w-full accent-run"
            />
            <span className="mt-1 flex justify-between text-xs text-muted">
              <span>80%</span>
              <span>nameplate 100</span>
              <span>120%</span>
            </span>
          </label>

          <dl className="mt-4 grid grid-cols-2 gap-x-3 gap-y-1 text-sm">
            <dt className="text-muted">This cycle</dt>
            <dd className="text-right tabular-nums">
              {m.running ? `${Math.round(m.cycleMs)} ms` : `${Math.round(1000 / (speed / 100))} ms`}
            </dd>
            <dt className="text-muted">Scrap armed</dt>
            <dd className="text-right tabular-nums">{m.scrapQueued}</dd>
            <dt className="text-muted">Shift / run / down</dt>
            <dd className="text-right tabular-nums">
              {clock(m.plannedSec)} · {clock(m.runtimeSec)} · {clock(m.downtimeSec)}
            </dd>
          </dl>
        </div>

        <div className="flex flex-col gap-4 rounded-2xl border border-border bg-surface p-4">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div>
              <p className="text-sm text-muted">OEE</p>
              <p className="font-display text-6xl leading-none tabular-nums sm:text-7xl">{pct(m.oee)}</p>
            </div>
            <p className="max-w-xs text-sm text-muted">
              Availability × performance × quality. Performance is the pot.
              At 100% with no stop and no scrap, OEE stays at 100.
            </p>
          </div>

          <div className="flex flex-col gap-3">
            <Meter label="Availability" value={m.availability} tone="fg" />
            <Meter label="Performance" value={m.performance} tone="run" />
            <Meter label="Quality" value={m.quality} tone="scrap" />
          </div>

          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Count label="Good" value={m.good} />
            <Count label="Bad" value={m.bad} />
            <Count label="Total" value={m.total} />
            <Count label="Per minute" value={m.actualPerMin} digits={1} hint="ideal 60" />
          </div>

          <p className={`text-xs ${view.balanced ? "text-muted" : "text-stop"}`}>
            {view.balanced
              ? "Good + bad = total. Scrap does not add a cycle."
              : "Count mismatch — good + bad should equal total."}
          </p>

          <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
            <span className="text-fg">OEE</span>
            <span className="text-muted">Availability</span>
            <span className="text-run">Performance</span>
            <span className="text-scrap">Quality</span>
          </div>

          <div className="h-36">
            {mounted && view.samples.length > 1 ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={view.samples}>
                  <XAxis dataKey="t" hide />
                  <YAxis domain={[0, 130]} hide />
                  <Tooltip
                    formatter={(value) => (typeof value === "number" ? value.toFixed(1) : value)}
                    labelFormatter={(label) => `${Number(label).toFixed(0)} s`}
                    contentStyle={{
                      background: "var(--color-surface-2)",
                      border: "1px solid var(--color-border)",
                      borderRadius: "0.6rem",
                      color: "var(--color-fg)",
                    }}
                  />
                  <Line type="monotone" dataKey="oee" name="OEE" stroke="var(--color-fg)" strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="availability" name="Availability" stroke="var(--color-muted)" strokeWidth={1.5} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="performance" name="Performance" stroke="var(--color-run)" strokeWidth={1.5} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="quality" name="Quality" stroke="var(--color-scrap)" strokeWidth={1.5} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex h-full items-center justify-center rounded-xl border border-dashed border-border text-sm text-muted">
                Trend starts when you press Start.
              </div>
            )}
          </div>
        </div>
      </section>

      <section className="rounded-2xl border border-border bg-surface p-4">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <h2 className="font-display text-2xl leading-none">MQTT topic</h2>
            <p className="mt-1 text-sm text-muted">
              Default is EET124/OEE/DrJeff/. Change the whole path. The sketch uses the same string.
            </p>
          </div>
        </div>

        <label className="mt-4 block">
          <span className="text-sm text-muted">Topic</span>
          <input
            value={topic}
            onChange={(event) => onTopic(event.target.value)}
            spellCheck={false}
            autoCapitalize="none"
            aria-label="MQTT topic"
            className="mt-2 h-12 w-full rounded-xl border border-border bg-bg px-3 font-display text-2xl text-fg outline-none"
          />
        </label>
        {path !== topic && (
          <p className="mt-2 text-sm text-stop">
            {topic.trim()
              ? `Publishing uses ${path}. Spaces are removed and a trailing slash is added.`
              : `Empty topic uses ${DEFAULT_TOPIC}.`}
          </p>
        )}

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <code className="block overflow-x-auto rounded-xl bg-bg px-3 py-3 text-sm text-fg">{line}</code>
          <button
            type="button"
            onClick={copyLine}
            className="h-11 shrink-0 rounded-full border border-border px-4 text-sm text-fg"
          >
            {copied ? "Copied" : "Copy line"}
          </button>
        </div>

        <ul className="mt-4 grid gap-1 sm:grid-cols-2">
          {TOPIC_LEAVES.map((leaf) => (
            <li key={leaf} className="truncate rounded-lg bg-bg px-3 py-2 text-sm text-muted">
              <span className="text-fg">{path}</span>
              {leaf}
            </li>
          ))}
        </ul>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-2xl border border-border bg-surface p-4">
          <h2 className="font-display text-2xl leading-none">What changed on the chip</h2>
          <ul className="mt-3 flex flex-col gap-3 text-sm text-muted">
            <li>
              <span className="text-fg">100 OEE is the clean run.</span> The shift starts on Start. Waiting while stopped is not downtime. Only a slower pot, a Stop, or a scrap pulls OEE down.
            </li>
            <li>
              <span className="text-fg">Defects do not mint parts.</span> A press turns one good widget into scrap. Performance stays with the pot. Quality falls.
            </li>
            <li>
              <span className="text-fg">Performance is the pot.</span> 100% is 60 widgets a minute. Above nameplate, OEE can go over 100. This page publishes those topics to the same HiveMQ broker as the sketch.
            </li>
            <li>
              <span className="text-fg">The broker can die.</span> The station keeps making parts. Status is a retained last-will on that student’s own topic.
            </li>
          </ul>
        </div>
        <div className="rounded-2xl border border-border bg-surface p-4">
          <h2 className="font-display text-2xl leading-none">Station log</h2>
          <ol className="mt-3 flex max-h-56 flex-col gap-1 overflow-auto">
            {view.log.map((entry) => (
              <li key={entry.id} className="rounded-lg bg-bg px-3 py-2 text-sm text-fg">
                {entry.text}
              </li>
            ))}
          </ol>
        </div>
      </section>
    </main>
  );
}

function Count({
  label,
  value,
  digits = 0,
  hint,
}: {
  label: string;
  value: number;
  digits?: number;
  hint?: string;
}) {
  return (
    <div className="rounded-xl bg-bg px-3 py-2">
      <p className="text-xs text-muted">{label}</p>
      <p className="font-display text-3xl leading-none tabular-nums">
        {digits > 0 ? value.toFixed(digits) : Math.round(value)}
      </p>
      {hint ? <p className="mt-1 text-xs text-muted">{hint}</p> : null}
    </div>
  );
}
