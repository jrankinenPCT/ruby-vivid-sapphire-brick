import { useEffect, useRef, useState } from "react";

const INFEED = 0.16;
const QUALITY = 0.5;
const OUTFEED = 0.88;
const BELT_X0 = 196;
const BELT_X1 = 900;
const BELT_Y = 168;

type Part = {
  id: number;
  t: number;
  scrap: boolean;
  reverse: boolean;
  phase: "belt" | "chute";
  drop: number;
  passed: boolean;
  trippedInfeed: boolean;
  trippedQuality: boolean;
  trippedOutfeed: boolean;
  trippedDefect: boolean;
};

type World = {
  parts: Part[];
  nextId: number;
  belt: number;
  motor: number;
  last: number;
  strikeUntil: number;
  eyes: { infeed: number; quality: number; defect: number; outfeed: number };
  lastQuality: "idle" | "good" | "scrap";
  goodBin: number;
  scrapBin: number;
};

type Paint = {
  parts: Part[];
  belt: number;
  motor: number;
  ramDown: boolean;
  kicker: boolean;
  infeed: boolean;
  quality: boolean;
  defect: boolean;
  outfeed: boolean;
  lastQuality: "idle" | "good" | "scrap";
  goodBin: number;
  scrapBin: number;
  running: boolean;
  armed: boolean;
};

function makePart(id: number, scrap: boolean, t: number, reverse = false): Part {
  return {
    id,
    t,
    scrap,
    reverse,
    phase: "belt",
    drop: 0,
    passed: false,
    trippedInfeed: reverse,
    trippedQuality: false,
    trippedOutfeed: reverse,
    trippedDefect: false,
  };
}

function beltX(t: number): number {
  return BELT_X0 + Math.max(0, Math.min(1, t)) * (BELT_X1 - BELT_X0);
}

function Eye({
  x,
  hot,
  label,
}: {
  x: number;
  hot: boolean;
  label: string;
}) {
  return (
    <g>
      <text
        x={x}
        y={108}
        textAnchor="middle"
        fill={hot ? "#f3f1e6" : "#a7a892"}
        fontSize={13}
        fontWeight={600}
        letterSpacing={1.2}
      >
        {label}
      </text>
      <rect x={x - 8} y={116} width={16} height={12} rx={2} fill="#262c20" stroke="#3a4030" />
      <circle cx={x} cy={122} r={3} fill={hot ? "#f3f1e6" : "#12140f"} />
      <line
        x1={x}
        y1={128}
        x2={x}
        y2={hot ? 146 : 176}
        stroke={hot ? "#f3f1e6" : "#3fba56"}
        strokeWidth={hot ? 2 : 1.25}
        strokeDasharray={hot ? undefined : "3 3"}
        opacity={hot ? 0.95 : 0.55}
      />
      {hot ? (
        <line x1={x} y1={162} x2={x} y2={176} stroke="#f3f1e6" strokeWidth={2} opacity={0.95} />
      ) : null}
      <rect x={x - 7} y={176} width={14} height={8} rx={1.5} fill="#1c2118" stroke="#3a4030" />
    </g>
  );
}

function WidgetBody({ scrap, passed }: { scrap: boolean; passed: boolean }) {
  return (
    <g>
      <rect
        x={-15}
        y={-11}
        width={30}
        height={18}
        rx={3}
        fill={scrap ? "#3a221c" : "#e6decc"}
        stroke={scrap ? "#e23d34" : passed ? "#3fba56" : "#8d8674"}
        strokeWidth={1.4}
      />
      <rect x={-9} y={-7} width={18} height={3.5} rx={1} fill={scrap ? "#e23d34" : "#c4baa6"} opacity={scrap ? 0.9 : 1} />
      {scrap ? (
        <path d="M-6 3 L6 -4" stroke="#e23d34" strokeWidth={1.6} />
      ) : passed ? (
        <path d="M-4 2 l3 3 l6 -7" fill="none" stroke="#3fba56" strokeWidth={1.6} />
      ) : null}
    </g>
  );
}

function BinStack({
  x,
  y,
  count,
  scrap,
}: {
  x: number;
  y: number;
  count: number;
  scrap: boolean;
}) {
  const n = Math.min(5, count);
  return (
    <g>
      {Array.from({ length: n }, (_, i) => (
        <rect
          key={i}
          x={x + 8 + (i % 2) * 6}
          y={y - 16 - i * 8}
          width={28}
          height={10}
          rx={2}
          fill={scrap ? "#3a221c" : "#e6decc"}
          stroke={scrap ? "#e23d34" : "#8d8674"}
        />
      ))}
    </g>
  );
}

export function WidgetLine({
  running,
  speed,
  total,
  bad,
  scrapQueued,
}: {
  running: boolean;
  speed: number;
  total: number;
  bad: number;
  scrapQueued: number;
}) {
  const worldRef = useRef<World>({
    parts: [],
    nextId: 1,
    belt: 0,
    motor: 0,
    last: 0,
    strikeUntil: 0,
    eyes: { infeed: 0, quality: 0, defect: 0, outfeed: 0 },
    lastQuality: "idle",
    goodBin: 0,
    scrapBin: 0,
  });
  const countsRef = useRef({ total, bad });
  const runningRef = useRef(running);
  const speedRef = useRef(speed);
  const armedRef = useRef(scrapQueued > 0);
  const reduceRef = useRef(false);
  const [paint, setPaint] = useState<Paint>(() => ({
    parts: [],
    belt: 0,
    motor: 0,
    ramDown: false,
    kicker: false,
    infeed: false,
    quality: false,
    defect: false,
    outfeed: false,
    lastQuality: "idle",
    goodBin: 0,
    scrapBin: 0,
    running: false,
    armed: false,
  }));

  runningRef.current = running;
  speedRef.current = speed;
  armedRef.current = scrapQueued > 0;

  useEffect(() => {
    reduceRef.current = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  }, []);

  useEffect(() => {
    const world = worldRef.current;
    const prev = countsRef.current;
    if (total < prev.total) {
      world.parts = [];
      world.goodBin = 0;
      world.scrapBin = 0;
      world.lastQuality = "idle";
      countsRef.current = { total, bad };
      return;
    }
    const born = total - prev.total;
    const newBad = Math.max(0, bad - prev.bad);
    const scrapBirths = Math.min(born, newBad);
    const reclass = newBad - scrapBirths;
    const burst = Math.min(born, 6);
    for (let i = 0; i < burst; i++) {
      world.parts.push(makePart(world.nextId++, i < scrapBirths, -0.075 * i));
    }
    for (let n = 0; n < reclass; n++) {
      const upcoming = world.parts
        .filter((part) => part.phase === "belt" && !part.scrap && !part.reverse && part.t < QUALITY)
        .sort((a, b) => b.t - a.t)[0];
      if (upcoming) {
        upcoming.scrap = true;
        continue;
      }
      const past = world.parts
        .filter((part) => part.phase === "belt" && !part.scrap && part.t >= QUALITY)
        .sort((a, b) => a.t - b.t)[0];
      if (past) {
        past.scrap = true;
        past.phase = "chute";
        past.drop = 0;
        world.eyes.defect = performance.now() + 220;
        continue;
      }
      world.goodBin = Math.max(0, world.goodBin - 1);
      world.parts.push(makePart(world.nextId++, true, OUTFEED, true));
    }
    if (born > 0) world.strikeUntil = performance.now() + 160;
    countsRef.current = { total, bad };
  }, [total, bad]);

  useEffect(() => {
    let raf = 0;
    let calmFrames = 0;
    const loop = (now: number) => {
      const world = worldRef.current;
      const dt = Math.min(80, world.last ? now - world.last : 16);
      world.last = now;
      const live = runningRef.current;
      if (live && !document.hidden) {
        const factor = Math.max(0.5, speedRef.current / 100);
        const cycle = Math.max(200, 1000 / factor);
        const travel = cycle * 4.2;
        if (!reduceRef.current) {
          world.belt = (world.belt + dt / cycle) % 1;
          world.motor = (world.motor + (dt / cycle) * 360) % 360;
        }
        for (const part of world.parts) {
          if (part.phase === "belt") {
            const before = part.t;
            part.t += (part.reverse ? -1 : 1) * (dt / travel);
            if (!part.reverse) {
              if (!part.trippedInfeed && before < INFEED && part.t >= INFEED) {
                part.trippedInfeed = true;
                world.eyes.infeed = now + 180;
              }
              if (!part.trippedQuality && before < QUALITY && part.t >= QUALITY) {
                part.trippedQuality = true;
                world.eyes.quality = now + 180;
                world.lastQuality = part.scrap ? "scrap" : "good";
                if (!part.scrap) part.passed = true;
              }
              if (part.scrap && before < QUALITY && part.t >= QUALITY) {
                part.phase = "chute";
                part.drop = 0;
              }
              if (!part.scrap && !part.trippedOutfeed && before < OUTFEED && part.t >= OUTFEED) {
                part.trippedOutfeed = true;
                world.eyes.outfeed = now + 180;
                world.goodBin = Math.min(6, world.goodBin + 1);
                part.phase = "chute";
                part.drop = 2;
              }
            } else if (part.t <= QUALITY) {
              part.phase = "chute";
              part.drop = 0;
              part.reverse = false;
              world.eyes.quality = now + 180;
              world.lastQuality = "scrap";
            }
          } else if (part.drop < 1) {
            const before = part.drop;
            part.drop += dt / 640;
            if (!part.trippedDefect && before < 0.42 && part.drop >= 0.42) {
              part.trippedDefect = true;
              world.eyes.defect = now + 220;
            }
            if (part.drop >= 1) {
              world.scrapBin = Math.min(6, world.scrapBin + 1);
            }
          }
        }
        world.parts = world.parts.filter((part) => part.phase === "belt" || part.drop < 1.05);
      }

      const kicker = world.parts.some(
        (part) =>
          part.scrap &&
          ((part.phase === "belt" && part.t > QUALITY - 0.06 && part.t < QUALITY + 0.02) ||
            (part.phase === "chute" && part.drop < 0.2)),
      );
      const blocked = (sensor: number, eye: number) =>
        now < eye ||
        world.parts.some((part) => part.phase === "belt" && !part.reverse && Math.abs(part.t - sensor) < 0.03);

      const calm =
        !live &&
        now > world.strikeUntil &&
        now > world.eyes.infeed &&
        now > world.eyes.quality &&
        now > world.eyes.defect &&
        now > world.eyes.outfeed;
      if (calm) calmFrames += 1;
      else calmFrames = 0;
      if (calmFrames <= 2) {
        setPaint({
          parts: world.parts.map((part) => ({ ...part })),
          belt: world.belt,
          motor: world.motor,
          ramDown: now < world.strikeUntil,
          kicker,
          infeed: blocked(INFEED, world.eyes.infeed),
          quality: blocked(QUALITY, world.eyes.quality),
          defect:
            now < world.eyes.defect ||
            world.parts.some((part) => part.phase === "chute" && part.drop > 0.28 && part.drop < 0.7),
          outfeed: blocked(OUTFEED, world.eyes.outfeed),
          lastQuality: world.lastQuality,
          goodBin: world.goodBin,
          scrapBin: world.scrapBin,
          running: live,
          armed: armedRef.current,
        });
      }
      raf = window.requestAnimationFrame(loop);
    };
    raf = window.requestAnimationFrame(loop);
    return () => window.cancelAnimationFrame(raf);
  }, []);

  const lamp =
    paint.lastQuality === "scrap" ? "#e23d34" : paint.lastQuality === "good" ? "#3fba56" : "#3a4030";

  return (
    <section className="rounded-2xl border border-border bg-surface p-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 className="font-display text-2xl leading-none">Widget line</h2>
          <p className="mt-1 max-w-2xl text-sm text-muted">
            The press releases one widget per cycle, same clock as the counts. Infeed sees every part.
            Quality kicks scrap down the chute through the defect eye. Only good widgets break the outfeed eye.
          </p>
        </div>
        <p className={`font-display text-xl leading-none ${paint.running ? "text-run" : "text-stop"}`}>
          {paint.running ? "BELT MOVING" : "BELT HELD"}
        </p>
      </div>

      <div className="mt-3 overflow-x-auto">
        <svg
          viewBox="0 0 1040 360"
          className="h-auto min-w-[760px] w-full"
          role="img"
          aria-label={
            paint.running
              ? "Widget line running. Parts leave the press, pass infeed and quality, and good parts exit at the outfeed."
              : "Widget line stopped. The belt is held."
          }
          style={{ fontFamily: "Barlow Condensed, sans-serif" }}
        >
          <line x1={24} y1={318} x2={1016} y2={318} stroke="#3a4030" strokeWidth={2} />

          <g>
            <rect x={36} y={86} width={16} height={168} rx={2} fill="#262c20" />
            <rect x={30} y={78} width={28} height={16} rx={3} fill={paint.running ? "#1c2118" : "#e23d34"} stroke="#3a4030" />
            <rect x={30} y={96} width={28} height={16} rx={3} fill={paint.running ? "#3fba56" : "#1c2118"} stroke="#3a4030" />
            <rect x={30} y={114} width={28} height={16} rx={3} fill={paint.armed ? "#3c8dff" : "#1c2118"} stroke="#3a4030" />
            <text x={44} y={154} textAnchor="middle" fill="#a7a892" fontSize={11} letterSpacing={0.8}>
              STACK
            </text>
          </g>

          <g>
            <path d="M86 118 h78 l14 28 h-106 z" fill="#1c2118" stroke="#3a4030" />
            <rect x={100} y={96} width={18} height={12} rx={2} fill="#e6decc" stroke="#8d8674" />
            <rect x={124} y={98} width={18} height={12} rx={2} fill="#e6decc" stroke="#8d8674" />
            <rect x={78} y={146} width={128} height={78} rx={6} fill="#262c20" stroke="#3a4030" />
            <rect x={92} y={158} width={36} height={52} rx={3} fill="#12140f" stroke="#3a4030" />
            <rect
              x={100}
              y={paint.ramDown ? 186 : 164}
              width={20}
              height={paint.ramDown ? 22 : 16}
              rx={2}
              fill="#f3f1e6"
            />
            <rect x={138} y={176} width={52} height={28} rx={3} fill="#1c2118" stroke="#3a4030" />
            <path d="M190 184 h18 v14 h-18 z" fill="#1c2118" stroke="#3a4030" />
            <text x={142} y={168} fill="#f3f1e6" fontSize={16} fontWeight={600} letterSpacing={1}>
              PRESS
            </text>
            <text x={142} y={210} fill="#a7a892" fontSize={12} letterSpacing={0.6}>
              60 / MIN
            </text>
          </g>

          <g>
            <rect x={188} y={196} width={724} height={10} rx={2} fill="#262c20" />
            <rect x={210} y={248} width={10} height={70} fill="#262c20" />
            <rect x={860} y={248} width={10} height={70} fill="#262c20" />
            <circle cx={214} cy={190} r={18} fill="#1c2118" stroke="#3a4030" strokeWidth={3} />
            <g transform={`rotate(${paint.motor} 214 190)`}>
              <line x1={202} y1={190} x2={226} y2={190} stroke="#a7a892" strokeWidth={2} />
              <line x1={214} y1={178} x2={214} y2={202} stroke="#a7a892" strokeWidth={2} />
            </g>
            <circle cx={886} cy={190} r={18} fill="#1c2118" stroke="#3a4030" strokeWidth={3} />
            <g transform={`rotate(${paint.motor} 886 190)`}>
              <line x1={874} y1={190} x2={898} y2={190} stroke="#a7a892" strokeWidth={2} />
              <line x1={886} y1={178} x2={886} y2={202} stroke="#a7a892" strokeWidth={2} />
            </g>
            <rect x={228} y={178} width={642} height={22} fill="#12140f" />
            {Array.from({ length: 18 }, (_, i) => {
              const u = (i / 18 + paint.belt) % 1;
              const x = 236 + u * 620;
              return (
                <path
                  key={i}
                  d={`M${x} 182 l7 7 l-7 7`}
                  fill="none"
                  stroke="#3a4030"
                  strokeWidth={2}
                />
              );
            })}
            <rect x={196} y={228} width={46} height={28} rx={4} fill="#262c20" stroke="#3a4030" />
            <circle cx={219} cy={242} r={7} fill="#12140f" stroke="#a7a892" />
            <line
              x1={219}
              y1={242}
              x2={219 + Math.cos((paint.motor * Math.PI) / 180) * 5}
              y2={242 + Math.sin((paint.motor * Math.PI) / 180) * 5}
              stroke="#f3f1e6"
              strokeWidth={1.5}
            />
            <text x={400} y={246} textAnchor="middle" fill="#a7a892" fontSize={12} letterSpacing={1.4}>
              CONVEYOR
            </text>
          </g>

          <Eye x={beltX(INFEED)} hot={paint.infeed} label="INFEED" />

          <g>
            <path d={`M${beltX(QUALITY) - 74} 96 h150 v78`} fill="none" stroke="#3a4030" strokeWidth={3} />
            <path d={`M${beltX(QUALITY) - 74} 96 v78`} fill="none" stroke="#3a4030" strokeWidth={3} />
            <circle cx={beltX(QUALITY)} cy={96} r={14} fill="#1c2118" stroke="#a7a892" strokeWidth={2} />
            <circle cx={beltX(QUALITY)} cy={96} r={6} fill={lamp} />
            <line
              x1={beltX(QUALITY)}
              y1={110}
              x2={beltX(QUALITY)}
              y2={paint.quality ? 148 : 174}
              stroke={lamp}
              strokeWidth={paint.quality ? 2 : 1.25}
              strokeDasharray={paint.quality ? undefined : "3 3"}
              opacity={paint.quality ? 0.95 : 0.7}
            />
            <text x={beltX(QUALITY)} y={84} textAnchor="middle" fill="#f3f1e6" fontSize={13} fontWeight={600} letterSpacing={1.1}>
              QUALITY
            </text>
            <rect x={beltX(QUALITY) - 18} y={112} width={16} height={34} rx={2} fill="#262c20" stroke="#3a4030" />
            <rect
              x={beltX(QUALITY) - 16}
              y={paint.kicker ? 140 : 126}
              width={12}
              height={paint.kicker ? 30 : 16}
              rx={1}
              fill="#f3f1e6"
            />
            <rect x={beltX(QUALITY) - 24} y={paint.kicker ? 166 : 140} width={28} height={7} rx={2} fill="#c4baa6" />
            {paint.armed ? (
              <text x={beltX(QUALITY) + 28} y={126} fill="#3c8dff" fontSize={12} fontWeight={600} letterSpacing={0.8}>
                ARMED
              </text>
            ) : null}
          </g>

          <path
            d={`M${beltX(QUALITY)} 196 C ${beltX(QUALITY) + 24} 230, ${beltX(QUALITY) + 40} 268, ${beltX(QUALITY) + 70} 292`}
            fill="none"
            stroke="#3a4030"
            strokeWidth={10}
            strokeLinecap="round"
          />
          <g>
            <text
              x={beltX(QUALITY) + 30}
              y={222}
              textAnchor="middle"
              fill={paint.defect ? "#f3f1e6" : "#a7a892"}
              fontSize={13}
              fontWeight={600}
              letterSpacing={1.2}
            >
              DEFECT
            </text>
            <rect x={beltX(QUALITY) + 4} y={230} width={12} height={14} rx={2} fill="#262c20" stroke="#3a4030" />
            <circle cx={beltX(QUALITY) + 10} cy={237} r={2.5} fill={paint.defect ? "#e23d34" : "#12140f"} />
            <line
              x1={beltX(QUALITY) + 16}
              y1={237}
              x2={beltX(QUALITY) + (paint.defect ? 26 : 44)}
              y2={237}
              stroke="#e23d34"
              strokeWidth={paint.defect ? 2 : 1.25}
              strokeDasharray={paint.defect ? undefined : "3 3"}
              opacity={paint.defect ? 1 : 0.55}
            />
            {paint.defect ? (
              <line x1={beltX(QUALITY) + 36} y1={237} x2={beltX(QUALITY) + 44} y2={237} stroke="#e23d34" strokeWidth={2} />
            ) : null}
            <rect x={beltX(QUALITY) + 44} y={230} width={12} height={14} rx={2} fill="#262c20" stroke="#3a4030" />
          </g>

          <Eye x={beltX(OUTFEED)} hot={paint.outfeed} label="OUTFEED" />

          <g>
            {paint.parts.map((part) => {
              const onChute = part.phase === "chute" && part.drop < 1;
              const x = onChute ? beltX(QUALITY) + part.drop * 70 : beltX(part.t);
              const y = onChute ? BELT_Y + 20 + part.drop * 110 : BELT_Y;
              if (part.phase === "chute" && part.drop >= 1) return null;
              return (
                <g key={part.id} transform={`translate(${x} ${y})`}>
                  <WidgetBody scrap={part.scrap} passed={part.passed && !part.scrap} />
                </g>
              );
            })}
          </g>

          <g>
            <path d="M948 236 h78 l-6 82 h-66 z" fill="#1c2118" stroke="#3fba56" />
            <text x={987} y={228} textAnchor="middle" fill="#3fba56" fontSize={13} fontWeight={600} letterSpacing={1}>
              GOOD
            </text>
            <BinStack x={962} y={308} count={paint.goodBin} scrap={false} />
          </g>
          <g>
            <path d={`M${beltX(QUALITY) + 36} 262 h72 l-6 56 h-60 z`} fill="#1c2118" stroke="#e23d34" />
            <text
              x={beltX(QUALITY) + 72}
              y={254}
              textAnchor="middle"
              fill="#e23d34"
              fontSize={13}
              fontWeight={600}
              letterSpacing={1}
            >
              SCRAP
            </text>
            <BinStack x={beltX(QUALITY) + 46} y={308} count={paint.scrapBin} scrap />
          </g>
        </svg>
      </div>
      <p className="mt-1 text-xs text-muted sm:hidden">Swipe the line to see quality and the outfeed.</p>

      <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <SensorPill label="Infeed" detail="Every widget" hot={paint.infeed} />
        <SensorPill label="Quality" detail={paint.armed ? "Scrap armed" : "Vision check"} hot={paint.quality} />
        <SensorPill label="Defect" detail="Scrap chute" hot={paint.defect} tone="stop" />
        <SensorPill label="Outfeed" detail="Good only" hot={paint.outfeed} tone="run" />
      </div>
    </section>
  );
}

function SensorPill({
  label,
  detail,
  hot,
  tone = "fg",
}: {
  label: string;
  detail: string;
  hot: boolean;
  tone?: "fg" | "run" | "stop";
}) {
  const dot = hot ? (tone === "stop" ? "bg-stop" : tone === "run" ? "bg-run" : "bg-fg") : "bg-bg";
  return (
    <div className="flex items-center gap-3 rounded-xl bg-bg px-3 py-2">
      <span className={`size-2.5 shrink-0 rounded-full border border-border ${dot}`} aria-hidden="true" />
      <span>
        <span className="block text-sm text-fg">{label}</span>
        <span className="block text-xs text-muted">{detail}</span>
      </span>
    </div>
  );
}
