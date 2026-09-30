export const TOPIC_PREFIX = "EET124/OEE/";
export const DEFAULT_TOPIC = "EET124/OEE/DrJeff/";

export const TOPIC_LEAVES = [
  "json",
  "availability",
  "performance",
  "quality",
  "oee",
  "goodCount",
  "badCount",
  "totalCount",
  "speedPercent",
  "running",
  "scrapQueued",
  "status",
] as const;

export const IDEAL_RATE_PPS = 1;
export const MAX_SCRAP = 25;

export type LogLine = { id: number; text: string };

export type Sample = {
  t: number;
  availability: number;
  performance: number;
  quality: number;
  oee: number;
};

export type Metrics = {
  availability: number;
  performance: number;
  quality: number;
  oee: number;
  runtimeSec: number;
  downtimeSec: number;
  plannedSec: number;
  speed: number;
  good: number;
  bad: number;
  total: number;
  running: boolean;
  scrapQueued: number;
  cycleMs: number;
  actualPerMin: number;
};

export type Sim = {
  shiftStart: number;
  shiftStarted: boolean;
  running: boolean;
  downtimeActive: boolean;
  accumulatedDowntime: number;
  downtimeStart: number;
  lastWidget: number;
  cycleJitter: number;
  total: number;
  good: number;
  bad: number;
  scrapQueued: number;
  speed: number;
  flashUntil: number;
  nextLogId: number;
  log: LogLine[];
  samples: Sample[];
  lastSample: number;
};

export function studentSlug(raw: string): string {
  const cleaned = raw.replace(/[^A-Za-z0-9]/g, "");
  return cleaned.slice(0, 24);
}

export function topicPath(raw: string): string {
  const name = studentSlug(raw) || "DrJeff";
  return `${TOPIC_PREFIX}${name}/`;
}

export function normalizeTopic(raw: string): string {
  const cleaned = raw
    .trim()
    .replace(/\s+/g, "")
    .replace(/[#+\u0000]/g, "")
    .replace(/^\/+/, "");
  if (!cleaned) return DEFAULT_TOPIC;
  return cleaned.endsWith("/") ? cleaned : `${cleaned}/`;
}

export function createSim(now: number): Sim {
  return {
    shiftStart: now,
    shiftStarted: false,
    running: false,
    downtimeActive: false,
    accumulatedDowntime: 0,
    downtimeStart: now,
    lastWidget: now,
    cycleJitter: 1,
    total: 0,
    good: 0,
    bad: 0,
    scrapQueued: 0,
    speed: 100,
    flashUntil: 0,
    nextLogId: 1,
    log: [{ id: 0, text: "Ready — a clean 100% run is 100 OEE" }],
    samples: [],
    lastSample: now,
  };
}

function pushLog(sim: Sim, text: string) {
  sim.log.unshift({ id: sim.nextLogId++, text });
  if (sim.log.length > 14) sim.log.length = 14;
}

function armCycle(sim: Sim) {
  sim.cycleJitter = 1;
}

export function cycleIntervalMs(sim: Sim): number {
  const factor = Math.max(0.5, sim.speed / 100);
  return Math.max(200, (1000 / factor) * sim.cycleJitter);
}

export function pressStart(sim: Sim, now: number) {
  if (sim.running) return;
  if (!sim.shiftStarted) {
    sim.shiftStarted = true;
    sim.shiftStart = now;
    sim.accumulatedDowntime = 0;
    sim.downtimeActive = false;
    sim.lastSample = now;
    sim.samples = [];
  } else if (sim.downtimeActive) {
    sim.accumulatedDowntime += now - sim.downtimeStart;
    sim.downtimeActive = false;
  }
  sim.running = true;
  sim.lastWidget = now;
  armCycle(sim);
  pushLog(sim, "START");
}

export function pressStop(sim: Sim, now: number) {
  if (!sim.running) return;
  sim.running = false;
  sim.downtimeStart = now;
  sim.downtimeActive = true;
  pushLog(sim, "STOP — downtime");
}

export function pressDefect(sim: Sim, now: number) {
  if (sim.good > 0) {
    sim.good -= 1;
    sim.bad += 1;
    sim.flashUntil = now + 400;
    pushLog(sim, "DEFECT — good part scrapped");
    return;
  }
  if (sim.scrapQueued >= MAX_SCRAP) {
    pushLog(sim, "DEFECT ignored — scrap queue full");
    return;
  }
  sim.scrapQueued += 1;
  pushLog(sim, `DEFECT — next part will be scrap (${sim.scrapQueued})`);
}

export function metrics(sim: Sim, now: number): Metrics {
  const speed = sim.speed;
  const commandedPerMin = (speed / 100) * 60;

  if (!sim.shiftStarted) {
    const oee = speed;
    return {
      availability: 100,
      performance: speed,
      quality: 100,
      oee,
      runtimeSec: 0,
      downtimeSec: 0,
      plannedSec: 0,
      speed,
      good: sim.good,
      bad: sim.bad,
      total: sim.total,
      running: false,
      scrapQueued: sim.scrapQueued,
      cycleMs: cycleIntervalMs(sim),
      actualPerMin: 0,
    };
  }

  const planned = Math.max(0, now - sim.shiftStart);
  let down = sim.accumulatedDowntime;
  if (sim.downtimeActive) down += Math.max(0, now - sim.downtimeStart);
  if (down > planned) down = planned;
  const runtime = planned - down;
  const availability = planned > 0 ? (100 * runtime) / planned : 100;
  const performance = speed;
  const quality = sim.total > 0 ? (100 * sim.good) / sim.total : 100;
  const oee = (availability / 100) * (performance / 100) * (quality / 100) * 100;

  return {
    availability,
    performance,
    quality,
    oee,
    runtimeSec: runtime / 1000,
    downtimeSec: down / 1000,
    plannedSec: planned / 1000,
    speed,
    good: sim.good,
    bad: sim.bad,
    total: sim.total,
    running: sim.running,
    scrapQueued: sim.scrapQueued,
    cycleMs: cycleIntervalMs(sim),
    actualPerMin: commandedPerMin,
  };
}

export function step(sim: Sim, now: number) {
  if (sim.running) {
    const interval = cycleIntervalMs(sim);
    if (now - sim.lastWidget >= interval) {
      sim.lastWidget += interval;
      if (now - sim.lastWidget > interval) sim.lastWidget = now;
      sim.total += 1;
      if (sim.scrapQueued > 0) {
        sim.scrapQueued -= 1;
        sim.bad += 1;
        sim.flashUntil = now + 400;
        pushLog(sim, "SCRAP PART");
      } else {
        sim.good += 1;
        pushLog(sim, "GOOD PART");
      }
      armCycle(sim);
    }
  }

  if (sim.shiftStarted && now - sim.lastSample >= 1000) {
    sim.lastSample = now;
    const m = metrics(sim, now);
    sim.samples.push({
      t: (now - sim.shiftStart) / 1000,
      availability: m.availability,
      performance: m.performance,
      quality: m.quality,
      oee: m.oee,
    });
    if (sim.samples.length > 60) sim.samples.shift();
  }
}

export function blueLit(sim: Sim, now: number): boolean {
  return sim.scrapQueued > 0 || now < sim.flashUntil;
}
