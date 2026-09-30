import { o as __toESM } from "../_runtime.mjs";
import { K as require_react, b as require_jsx_runtime } from "../_libs/@tanstack/react-router+[...].mjs";
import { n as RotateCcw, r as Download } from "../_libs/lucide-react.mjs";
import { a as ResponsiveContainer, i as Line, n as YAxis, o as Tooltip, r as XAxis, t as LineChart } from "../_libs/recharts+[...].mjs";
//#region node_modules/.nitro/vite/services/ssr/assets/routes-C4cYoxym.js
var import_react = /* @__PURE__ */ __toESM(require_react());
var import_jsx_runtime = require_jsx_runtime();
var TOPIC_PREFIX = "EET124/OEE/";
var DEFAULT_TOPIC = "EET124/OEE/DrJeff/";
var TOPIC_LEAVES = [
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
	"status"
];
function studentSlug(raw) {
	return raw.replace(/[^A-Za-z0-9]/g, "").slice(0, 24);
}
function topicPath(raw) {
	return `${TOPIC_PREFIX}${studentSlug(raw) || "DrJeff"}/`;
}
function normalizeTopic(raw) {
	const cleaned = raw.trim().replace(/\s+/g, "").replace(/[#+\u0000]/g, "").replace(/^\/+/, "");
	if (!cleaned) return DEFAULT_TOPIC;
	return cleaned.endsWith("/") ? cleaned : `${cleaned}/`;
}
function createSim(now) {
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
		log: [{
			id: 0,
			text: "Ready — a clean 100% run is 100 OEE"
		}],
		samples: [],
		lastSample: now
	};
}
function pushLog(sim, text) {
	sim.log.unshift({
		id: sim.nextLogId++,
		text
	});
	if (sim.log.length > 14) sim.log.length = 14;
}
function armCycle(sim) {
	sim.cycleJitter = 1;
}
function cycleIntervalMs(sim) {
	const factor = Math.max(.5, sim.speed / 100);
	return Math.max(200, 1e3 / factor * sim.cycleJitter);
}
function pressStart(sim, now) {
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
function pressStop(sim, now) {
	if (!sim.running) return;
	sim.running = false;
	sim.downtimeStart = now;
	sim.downtimeActive = true;
	pushLog(sim, "STOP — downtime");
}
function pressDefect(sim, now) {
	if (sim.good > 0) {
		sim.good -= 1;
		sim.bad += 1;
		sim.flashUntil = now + 400;
		pushLog(sim, "DEFECT — good part scrapped");
		return;
	}
	if (sim.scrapQueued >= 25) {
		pushLog(sim, "DEFECT ignored — scrap queue full");
		return;
	}
	sim.scrapQueued += 1;
	pushLog(sim, `DEFECT — next part will be scrap (${sim.scrapQueued})`);
}
function metrics(sim, now) {
	const speed = sim.speed;
	const commandedPerMin = speed / 100 * 60;
	if (!sim.shiftStarted) return {
		availability: 100,
		performance: speed,
		quality: 100,
		oee: speed,
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
		actualPerMin: 0
	};
	const planned = Math.max(0, now - sim.shiftStart);
	let down = sim.accumulatedDowntime;
	if (sim.downtimeActive) down += Math.max(0, now - sim.downtimeStart);
	if (down > planned) down = planned;
	const runtime = planned - down;
	const availability = planned > 0 ? 100 * runtime / planned : 100;
	const performance = speed;
	const quality = sim.total > 0 ? 100 * sim.good / sim.total : 100;
	return {
		availability,
		performance,
		quality,
		oee: availability / 100 * (performance / 100) * (quality / 100) * 100,
		runtimeSec: runtime / 1e3,
		downtimeSec: down / 1e3,
		plannedSec: planned / 1e3,
		speed,
		good: sim.good,
		bad: sim.bad,
		total: sim.total,
		running: sim.running,
		scrapQueued: sim.scrapQueued,
		cycleMs: cycleIntervalMs(sim),
		actualPerMin: commandedPerMin
	};
}
function step(sim, now) {
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
	if (sim.shiftStarted && now - sim.lastSample >= 1e3) {
		sim.lastSample = now;
		const m = metrics(sim, now);
		sim.samples.push({
			t: (now - sim.shiftStart) / 1e3,
			availability: m.availability,
			performance: m.performance,
			quality: m.quality,
			oee: m.oee
		});
		if (sim.samples.length > 60) sim.samples.shift();
	}
}
function blueLit(sim, now) {
	return sim.scrapQueued > 0 || now < sim.flashUntil;
}
var BROKER_URL = "wss://e6fc009f759842338bc9b4a61b9ca0a7.s1.eu.hivemq.cloud:8884/mqtt";
var MQTT_USER = "mqtt-fall24";
var MQTT_PASSWORD = "w@terHouse5";
function num(value, digits) {
	return value.toFixed(digits);
}
function publishMetrics(client, base, m) {
	const send = (leaf, message) => {
		client.publish(base + leaf, message, {
			retain: true,
			qos: 0
		});
	};
	send("json", JSON.stringify({
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
		downtimeSec: Number(num(m.downtimeSec, 1))
	}));
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
var INFEED = .16;
var QUALITY = .5;
var OUTFEED = .88;
var BELT_X0 = 196;
var BELT_Y = 168;
function makePart(id, scrap, t, reverse = false) {
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
		trippedDefect: false
	};
}
function beltX(t) {
	return BELT_X0 + Math.max(0, Math.min(1, t)) * 704;
}
function Eye({ x, hot, label }) {
	return /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
		/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
			x,
			y: 108,
			textAnchor: "middle",
			fill: hot ? "#f3f1e6" : "#a7a892",
			fontSize: 13,
			fontWeight: 600,
			letterSpacing: 1.2,
			children: label
		}),
		/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
			x: x - 8,
			y: 116,
			width: 16,
			height: 12,
			rx: 2,
			fill: "#262c20",
			stroke: "#3a4030"
		}),
		/* @__PURE__ */ (0, import_jsx_runtime.jsx)("circle", {
			cx: x,
			cy: 122,
			r: 3,
			fill: hot ? "#f3f1e6" : "#12140f"
		}),
		/* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
			x1: x,
			y1: 128,
			x2: x,
			y2: hot ? 146 : 176,
			stroke: hot ? "#f3f1e6" : "#3fba56",
			strokeWidth: hot ? 2 : 1.25,
			strokeDasharray: hot ? void 0 : "3 3",
			opacity: hot ? .95 : .55
		}),
		hot ? /* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
			x1: x,
			y1: 162,
			x2: x,
			y2: 176,
			stroke: "#f3f1e6",
			strokeWidth: 2,
			opacity: .95
		}) : null,
		/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
			x: x - 7,
			y: 176,
			width: 14,
			height: 8,
			rx: 1.5,
			fill: "#1c2118",
			stroke: "#3a4030"
		})
	] });
}
function WidgetBody({ scrap, passed }) {
	return /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
		/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
			x: -15,
			y: -11,
			width: 30,
			height: 18,
			rx: 3,
			fill: scrap ? "#3a221c" : "#e6decc",
			stroke: scrap ? "#e23d34" : passed ? "#3fba56" : "#8d8674",
			strokeWidth: 1.4
		}),
		/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
			x: -9,
			y: -7,
			width: 18,
			height: 3.5,
			rx: 1,
			fill: scrap ? "#e23d34" : "#c4baa6",
			opacity: scrap ? .9 : 1
		}),
		scrap ? /* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
			d: "M-6 3 L6 -4",
			stroke: "#e23d34",
			strokeWidth: 1.6
		}) : passed ? /* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
			d: "M-4 2 l3 3 l6 -7",
			fill: "none",
			stroke: "#3fba56",
			strokeWidth: 1.6
		}) : null
	] });
}
function BinStack({ x, y, count, scrap }) {
	const n = Math.min(5, count);
	return /* @__PURE__ */ (0, import_jsx_runtime.jsx)("g", { children: Array.from({ length: n }, (_, i) => /* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
		x: x + 8 + i % 2 * 6,
		y: y - 16 - i * 8,
		width: 28,
		height: 10,
		rx: 2,
		fill: scrap ? "#3a221c" : "#e6decc",
		stroke: scrap ? "#e23d34" : "#8d8674"
	}, i)) });
}
function WidgetLine({ running, speed, total, bad, scrapQueued }) {
	const worldRef = (0, import_react.useRef)({
		parts: [],
		nextId: 1,
		belt: 0,
		motor: 0,
		last: 0,
		strikeUntil: 0,
		eyes: {
			infeed: 0,
			quality: 0,
			defect: 0,
			outfeed: 0
		},
		lastQuality: "idle",
		goodBin: 0,
		scrapBin: 0
	});
	const countsRef = (0, import_react.useRef)({
		total,
		bad
	});
	const runningRef = (0, import_react.useRef)(running);
	const speedRef = (0, import_react.useRef)(speed);
	const armedRef = (0, import_react.useRef)(scrapQueued > 0);
	const reduceRef = (0, import_react.useRef)(false);
	const [paint, setPaint] = (0, import_react.useState)(() => ({
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
		armed: false
	}));
	runningRef.current = running;
	speedRef.current = speed;
	armedRef.current = scrapQueued > 0;
	(0, import_react.useEffect)(() => {
		reduceRef.current = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
	}, []);
	(0, import_react.useEffect)(() => {
		const world = worldRef.current;
		const prev = countsRef.current;
		if (total < prev.total) {
			world.parts = [];
			world.goodBin = 0;
			world.scrapBin = 0;
			world.lastQuality = "idle";
			countsRef.current = {
				total,
				bad
			};
			return;
		}
		const born = total - prev.total;
		const newBad = Math.max(0, bad - prev.bad);
		const scrapBirths = Math.min(born, newBad);
		const reclass = newBad - scrapBirths;
		const burst = Math.min(born, 6);
		for (let i = 0; i < burst; i++) world.parts.push(makePart(world.nextId++, i < scrapBirths, -.075 * i));
		for (let n = 0; n < reclass; n++) {
			const upcoming = world.parts.filter((part) => part.phase === "belt" && !part.scrap && !part.reverse && part.t < QUALITY).sort((a, b) => b.t - a.t)[0];
			if (upcoming) {
				upcoming.scrap = true;
				continue;
			}
			const past = world.parts.filter((part) => part.phase === "belt" && !part.scrap && part.t >= QUALITY).sort((a, b) => a.t - b.t)[0];
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
		countsRef.current = {
			total,
			bad
		};
	}, [total, bad]);
	(0, import_react.useEffect)(() => {
		let raf = 0;
		let calmFrames = 0;
		const loop = (now) => {
			const world = worldRef.current;
			const dt = Math.min(80, world.last ? now - world.last : 16);
			world.last = now;
			const live = runningRef.current;
			if (live && !document.hidden) {
				const factor = Math.max(.5, speedRef.current / 100);
				const cycle = Math.max(200, 1e3 / factor);
				const travel = cycle * 4.2;
				if (!reduceRef.current) {
					world.belt = (world.belt + dt / cycle) % 1;
					world.motor = (world.motor + dt / cycle * 360) % 360;
				}
				for (const part of world.parts) if (part.phase === "belt") {
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
					if (!part.trippedDefect && before < .42 && part.drop >= .42) {
						part.trippedDefect = true;
						world.eyes.defect = now + 220;
					}
					if (part.drop >= 1) world.scrapBin = Math.min(6, world.scrapBin + 1);
				}
				world.parts = world.parts.filter((part) => part.phase === "belt" || part.drop < 1.05);
			}
			const kicker = world.parts.some((part) => part.scrap && (part.phase === "belt" && part.t > .44 && part.t < .52 || part.phase === "chute" && part.drop < .2));
			const blocked = (sensor, eye) => now < eye || world.parts.some((part) => part.phase === "belt" && !part.reverse && Math.abs(part.t - sensor) < .03);
			if (!live && now > world.strikeUntil && now > world.eyes.infeed && now > world.eyes.quality && now > world.eyes.defect && now > world.eyes.outfeed) calmFrames += 1;
			else calmFrames = 0;
			if (calmFrames <= 2) setPaint({
				parts: world.parts.map((part) => ({ ...part })),
				belt: world.belt,
				motor: world.motor,
				ramDown: now < world.strikeUntil,
				kicker,
				infeed: blocked(INFEED, world.eyes.infeed),
				quality: blocked(QUALITY, world.eyes.quality),
				defect: now < world.eyes.defect || world.parts.some((part) => part.phase === "chute" && part.drop > .28 && part.drop < .7),
				outfeed: blocked(OUTFEED, world.eyes.outfeed),
				lastQuality: world.lastQuality,
				goodBin: world.goodBin,
				scrapBin: world.scrapBin,
				running: live,
				armed: armedRef.current
			});
			raf = window.requestAnimationFrame(loop);
		};
		raf = window.requestAnimationFrame(loop);
		return () => window.cancelAnimationFrame(raf);
	}, []);
	const lamp = paint.lastQuality === "scrap" ? "#e23d34" : paint.lastQuality === "good" ? "#3fba56" : "#3a4030";
	return /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("section", {
		className: "rounded-2xl border border-border bg-surface p-4",
		children: [
			/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
				className: "flex flex-wrap items-end justify-between gap-3",
				children: [/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("h2", {
					className: "font-display text-2xl leading-none",
					children: "Widget line"
				}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
					className: "mt-1 max-w-2xl text-sm text-muted",
					children: "The press releases one widget per cycle, same clock as the counts. Infeed sees every part. Quality kicks scrap down the chute through the defect eye. Only good widgets break the outfeed eye."
				})] }), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
					className: `font-display text-xl leading-none ${paint.running ? "text-run" : "text-stop"}`,
					children: paint.running ? "BELT MOVING" : "BELT HELD"
				})]
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsx)("div", {
				className: "mt-3 overflow-x-auto",
				children: /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("svg", {
					viewBox: "0 0 1040 360",
					className: "h-auto min-w-[760px] w-full",
					role: "img",
					"aria-label": paint.running ? "Widget line running. Parts leave the press, pass infeed and quality, and good parts exit at the outfeed." : "Widget line stopped. The belt is held.",
					style: { fontFamily: "Barlow Condensed, sans-serif" },
					children: [
						/* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
							x1: 24,
							y1: 318,
							x2: 1016,
							y2: 318,
							stroke: "#3a4030",
							strokeWidth: 2
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 36,
								y: 86,
								width: 16,
								height: 168,
								rx: 2,
								fill: "#262c20"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 30,
								y: 78,
								width: 28,
								height: 16,
								rx: 3,
								fill: paint.running ? "#1c2118" : "#e23d34",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 30,
								y: 96,
								width: 28,
								height: 16,
								rx: 3,
								fill: paint.running ? "#3fba56" : "#1c2118",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 30,
								y: 114,
								width: 28,
								height: 16,
								rx: 3,
								fill: paint.armed ? "#3c8dff" : "#1c2118",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: 44,
								y: 154,
								textAnchor: "middle",
								fill: "#a7a892",
								fontSize: 11,
								letterSpacing: .8,
								children: "STACK"
							})
						] }),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
								d: "M86 118 h78 l14 28 h-106 z",
								fill: "#1c2118",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 100,
								y: 96,
								width: 18,
								height: 12,
								rx: 2,
								fill: "#e6decc",
								stroke: "#8d8674"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 124,
								y: 98,
								width: 18,
								height: 12,
								rx: 2,
								fill: "#e6decc",
								stroke: "#8d8674"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 78,
								y: 146,
								width: 128,
								height: 78,
								rx: 6,
								fill: "#262c20",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 92,
								y: 158,
								width: 36,
								height: 52,
								rx: 3,
								fill: "#12140f",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 100,
								y: paint.ramDown ? 186 : 164,
								width: 20,
								height: paint.ramDown ? 22 : 16,
								rx: 2,
								fill: "#f3f1e6"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 138,
								y: 176,
								width: 52,
								height: 28,
								rx: 3,
								fill: "#1c2118",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
								d: "M190 184 h18 v14 h-18 z",
								fill: "#1c2118",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: 142,
								y: 168,
								fill: "#f3f1e6",
								fontSize: 16,
								fontWeight: 600,
								letterSpacing: 1,
								children: "PRESS"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: 142,
								y: 210,
								fill: "#a7a892",
								fontSize: 12,
								letterSpacing: .6,
								children: "60 / MIN"
							})
						] }),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 188,
								y: 196,
								width: 724,
								height: 10,
								rx: 2,
								fill: "#262c20"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 210,
								y: 248,
								width: 10,
								height: 70,
								fill: "#262c20"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 860,
								y: 248,
								width: 10,
								height: 70,
								fill: "#262c20"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("circle", {
								cx: 214,
								cy: 190,
								r: 18,
								fill: "#1c2118",
								stroke: "#3a4030",
								strokeWidth: 3
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", {
								transform: `rotate(${paint.motor} 214 190)`,
								children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
									x1: 202,
									y1: 190,
									x2: 226,
									y2: 190,
									stroke: "#a7a892",
									strokeWidth: 2
								}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
									x1: 214,
									y1: 178,
									x2: 214,
									y2: 202,
									stroke: "#a7a892",
									strokeWidth: 2
								})]
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("circle", {
								cx: 886,
								cy: 190,
								r: 18,
								fill: "#1c2118",
								stroke: "#3a4030",
								strokeWidth: 3
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", {
								transform: `rotate(${paint.motor} 886 190)`,
								children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
									x1: 874,
									y1: 190,
									x2: 898,
									y2: 190,
									stroke: "#a7a892",
									strokeWidth: 2
								}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
									x1: 886,
									y1: 178,
									x2: 886,
									y2: 202,
									stroke: "#a7a892",
									strokeWidth: 2
								})]
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 228,
								y: 178,
								width: 642,
								height: 22,
								fill: "#12140f"
							}),
							Array.from({ length: 18 }, (_, i) => {
								const x = 236 + (i / 18 + paint.belt) % 1 * 620;
								return /* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
									d: `M${x} 182 l7 7 l-7 7`,
									fill: "none",
									stroke: "#3a4030",
									strokeWidth: 2
								}, i);
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: 196,
								y: 228,
								width: 46,
								height: 28,
								rx: 4,
								fill: "#262c20",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("circle", {
								cx: 219,
								cy: 242,
								r: 7,
								fill: "#12140f",
								stroke: "#a7a892"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
								x1: 219,
								y1: 242,
								x2: 219 + Math.cos(paint.motor * Math.PI / 180) * 5,
								y2: 242 + Math.sin(paint.motor * Math.PI / 180) * 5,
								stroke: "#f3f1e6",
								strokeWidth: 1.5
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: 400,
								y: 246,
								textAnchor: "middle",
								fill: "#a7a892",
								fontSize: 12,
								letterSpacing: 1.4,
								children: "CONVEYOR"
							})
						] }),
						/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Eye, {
							x: beltX(INFEED),
							hot: paint.infeed,
							label: "INFEED"
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
								d: `M${beltX(QUALITY) - 74} 96 h150 v78`,
								fill: "none",
								stroke: "#3a4030",
								strokeWidth: 3
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
								d: `M${beltX(QUALITY) - 74} 96 v78`,
								fill: "none",
								stroke: "#3a4030",
								strokeWidth: 3
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("circle", {
								cx: beltX(QUALITY),
								cy: 96,
								r: 14,
								fill: "#1c2118",
								stroke: "#a7a892",
								strokeWidth: 2
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("circle", {
								cx: beltX(QUALITY),
								cy: 96,
								r: 6,
								fill: lamp
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
								x1: beltX(QUALITY),
								y1: 110,
								x2: beltX(QUALITY),
								y2: paint.quality ? 148 : 174,
								stroke: lamp,
								strokeWidth: paint.quality ? 2 : 1.25,
								strokeDasharray: paint.quality ? void 0 : "3 3",
								opacity: paint.quality ? .95 : .7
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: beltX(QUALITY),
								y: 84,
								textAnchor: "middle",
								fill: "#f3f1e6",
								fontSize: 13,
								fontWeight: 600,
								letterSpacing: 1.1,
								children: "QUALITY"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: beltX(QUALITY) - 18,
								y: 112,
								width: 16,
								height: 34,
								rx: 2,
								fill: "#262c20",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: beltX(QUALITY) - 16,
								y: paint.kicker ? 140 : 126,
								width: 12,
								height: paint.kicker ? 30 : 16,
								rx: 1,
								fill: "#f3f1e6"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: beltX(QUALITY) - 24,
								y: paint.kicker ? 166 : 140,
								width: 28,
								height: 7,
								rx: 2,
								fill: "#c4baa6"
							}),
							paint.armed ? /* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: beltX(QUALITY) + 28,
								y: 126,
								fill: "#3c8dff",
								fontSize: 12,
								fontWeight: 600,
								letterSpacing: .8,
								children: "ARMED"
							}) : null
						] }),
						/* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
							d: `M${beltX(QUALITY)} 196 C ${beltX(QUALITY) + 24} 230, ${beltX(QUALITY) + 40} 268, ${beltX(QUALITY) + 70} 292`,
							fill: "none",
							stroke: "#3a4030",
							strokeWidth: 10,
							strokeLinecap: "round"
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: beltX(QUALITY) + 30,
								y: 222,
								textAnchor: "middle",
								fill: paint.defect ? "#f3f1e6" : "#a7a892",
								fontSize: 13,
								fontWeight: 600,
								letterSpacing: 1.2,
								children: "DEFECT"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: beltX(QUALITY) + 4,
								y: 230,
								width: 12,
								height: 14,
								rx: 2,
								fill: "#262c20",
								stroke: "#3a4030"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("circle", {
								cx: beltX(QUALITY) + 10,
								cy: 237,
								r: 2.5,
								fill: paint.defect ? "#e23d34" : "#12140f"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
								x1: beltX(QUALITY) + 16,
								y1: 237,
								x2: beltX(QUALITY) + (paint.defect ? 26 : 44),
								y2: 237,
								stroke: "#e23d34",
								strokeWidth: paint.defect ? 2 : 1.25,
								strokeDasharray: paint.defect ? void 0 : "3 3",
								opacity: paint.defect ? 1 : .55
							}),
							paint.defect ? /* @__PURE__ */ (0, import_jsx_runtime.jsx)("line", {
								x1: beltX(QUALITY) + 36,
								y1: 237,
								x2: beltX(QUALITY) + 44,
								y2: 237,
								stroke: "#e23d34",
								strokeWidth: 2
							}) : null,
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("rect", {
								x: beltX(QUALITY) + 44,
								y: 230,
								width: 12,
								height: 14,
								rx: 2,
								fill: "#262c20",
								stroke: "#3a4030"
							})
						] }),
						/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Eye, {
							x: beltX(OUTFEED),
							hot: paint.outfeed,
							label: "OUTFEED"
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsx)("g", { children: paint.parts.map((part) => {
							const onChute = part.phase === "chute" && part.drop < 1;
							const x = onChute ? beltX(QUALITY) + part.drop * 70 : beltX(part.t);
							const y = onChute ? 188 + part.drop * 110 : BELT_Y;
							if (part.phase === "chute" && part.drop >= 1) return null;
							return /* @__PURE__ */ (0, import_jsx_runtime.jsx)("g", {
								transform: `translate(${x} ${y})`,
								children: /* @__PURE__ */ (0, import_jsx_runtime.jsx)(WidgetBody, {
									scrap: part.scrap,
									passed: part.passed && !part.scrap
								})
							}, part.id);
						}) }),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
								d: "M948 236 h78 l-6 82 h-66 z",
								fill: "#1c2118",
								stroke: "#3fba56"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: 987,
								y: 228,
								textAnchor: "middle",
								fill: "#3fba56",
								fontSize: 13,
								fontWeight: 600,
								letterSpacing: 1,
								children: "GOOD"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)(BinStack, {
								x: 962,
								y: 308,
								count: paint.goodBin,
								scrap: false
							})
						] }),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("g", { children: [
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("path", {
								d: `M${beltX(QUALITY) + 36} 262 h72 l-6 56 h-60 z`,
								fill: "#1c2118",
								stroke: "#e23d34"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)("text", {
								x: beltX(QUALITY) + 72,
								y: 254,
								textAnchor: "middle",
								fill: "#e23d34",
								fontSize: 13,
								fontWeight: 600,
								letterSpacing: 1,
								children: "SCRAP"
							}),
							/* @__PURE__ */ (0, import_jsx_runtime.jsx)(BinStack, {
								x: beltX(QUALITY) + 46,
								y: 308,
								count: paint.scrapBin,
								scrap: true
							})
						] })
					]
				})
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
				className: "mt-1 text-xs text-muted sm:hidden",
				children: "Swipe the line to see quality and the outfeed."
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
				className: "mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4",
				children: [
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)(SensorPill, {
						label: "Infeed",
						detail: "Every widget",
						hot: paint.infeed
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)(SensorPill, {
						label: "Quality",
						detail: paint.armed ? "Scrap armed" : "Vision check",
						hot: paint.quality
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)(SensorPill, {
						label: "Defect",
						detail: "Scrap chute",
						hot: paint.defect,
						tone: "stop"
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)(SensorPill, {
						label: "Outfeed",
						detail: "Good only",
						hot: paint.outfeed,
						tone: "run"
					})
				]
			})
		]
	});
}
function SensorPill({ label, detail, hot, tone = "fg" }) {
	return /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
		className: "flex items-center gap-3 rounded-xl bg-bg px-3 py-2",
		children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
			className: `size-2.5 shrink-0 rounded-full border border-border ${hot ? tone === "stop" ? "bg-stop" : tone === "run" ? "bg-run" : "bg-fg" : "bg-bg"}`,
			"aria-hidden": "true"
		}), /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("span", { children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
			className: "block text-sm text-fg",
			children: label
		}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
			className: "block text-xs text-muted",
			children: detail
		})] })]
	});
}
function snapshot(sim, now) {
	return {
		metrics: metrics(sim, now),
		running: sim.running,
		blue: blueLit(sim, now),
		log: sim.log.map((line) => ({ ...line })),
		samples: sim.samples.map((sample) => ({ ...sample })),
		balanced: sim.good + sim.bad === sim.total
	};
}
function pct(value) {
	return value.toFixed(1);
}
function clock(seconds) {
	const whole = Math.max(0, Math.floor(seconds));
	return `${Math.floor(whole / 60)}:${(whole % 60).toString().padStart(2, "0")}`;
}
function Meter({ label, value, tone }) {
	const width = Math.max(0, Math.min(100, value / 130 * 100));
	return /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
		className: "grid grid-cols-[5.5rem_1fr_3.5rem] items-center gap-3",
		children: [
			/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
				className: "text-sm text-muted",
				children: label
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("span", {
				className: "relative h-2 overflow-hidden rounded-full bg-surface-2",
				children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", { className: "absolute inset-y-0 left-[76.9%] w-px bg-border" }), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
					className: `absolute inset-y-0 left-0 rounded-full ${tone === "run" ? "bg-run" : tone === "stop" ? "bg-stop" : tone === "scrap" ? "bg-scrap" : "bg-fg"}`,
					style: { width: `${width}%` }
				})]
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
				className: "text-right font-display text-xl leading-none tabular-nums text-fg",
				children: pct(value)
			})
		]
	});
}
function Led({ on, tone }) {
	return /* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
		className: `mx-auto block size-3.5 rounded-full border border-border ${on ? tone === "run" ? "bg-run" : tone === "stop" ? "bg-stop" : "bg-scrap" : "bg-bg"}`,
		"aria-hidden": "true"
	});
}
function OeeBench() {
	const simRef = (0, import_react.useRef)(null);
	const simEpochRef = (0, import_react.useRef)(-1);
	const epochRef = (0, import_react.useRef)(0);
	const speedRef = (0, import_react.useRef)(100);
	const topicRef = (0, import_react.useRef)(DEFAULT_TOPIC);
	const clientRef = (0, import_react.useRef)(null);
	const [epoch, setEpoch] = (0, import_react.useState)(0);
	const [mounted, setMounted] = (0, import_react.useState)(false);
	const [speed, setSpeed] = (0, import_react.useState)(100);
	const [topic, setTopic] = (0, import_react.useState)(DEFAULT_TOPIC);
	const [copied, setCopied] = (0, import_react.useState)(false);
	const [broker, setBroker] = (0, import_react.useState)("connecting");
	const [echo, setEcho] = (0, import_react.useState)("");
	const [view, setView] = (0, import_react.useState)(() => snapshot(createSim(0), 0));
	epochRef.current = epoch;
	topicRef.current = topic;
	(0, import_react.useEffect)(() => {
		const saved = localStorage.getItem("eet124-oee-student");
		if (saved) setTopic(saved.includes("/") ? saved : topicPath(saved));
		setMounted(true);
	}, []);
	(0, import_react.useEffect)(() => {
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
	(0, import_react.useEffect)(() => {
		let stopped = false;
		let client = null;
		let subscribed = "";
		const push = () => {
			const live = clientRef.current;
			if (!live?.connected) return;
			const sim = simRef.current;
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
		import("../_libs/mqtt.mjs").then((n) => /* @__PURE__ */ __toESM(n.t())).then((mod) => {
			if (stopped) return;
			const connected = mod.default.connect(BROKER_URL, {
				username: MQTT_USER,
				password: MQTT_PASSWORD,
				clientId: `OEE-web-${Math.random().toString(16).slice(2, 10)}`,
				reconnectPeriod: 4e3,
				connectTimeout: 1e4,
				protocolVersion: 4
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
		const id = window.setInterval(push, 1e3);
		return () => {
			stopped = true;
			window.clearInterval(id);
			clientRef.current = null;
			client?.end(true);
		};
	}, []);
	function liveSim() {
		const now = performance.now();
		if (!simRef.current || simEpochRef.current !== epochRef.current) {
			const sim = createSim(now);
			sim.speed = speedRef.current;
			simRef.current = sim;
			simEpochRef.current = epochRef.current;
		}
		return simRef.current;
	}
	function onSpeed(next) {
		speedRef.current = next;
		setSpeed(next);
		if (simRef.current) simRef.current.speed = next;
	}
	function onTopic(value) {
		setTopic(value);
		localStorage.setItem("eet124-oee-student", value);
	}
	function act(fn) {
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
	return /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("main", {
		className: "mx-auto flex w-full max-w-5xl flex-col gap-5 px-4 py-5 sm:px-6 sm:py-8",
		children: [
			/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("header", {
				className: "flex flex-wrap items-end justify-between gap-4",
				children: [/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", { children: [
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
						className: "font-display text-sm tracking-widest text-muted uppercase",
						children: "EET 124"
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)("h1", {
						className: "font-display text-4xl leading-none text-fg sm:text-5xl",
						children: "Line OEE"
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
						className: "mt-2 max-w-xl text-sm text-muted",
						children: "Pot at 100, no Stop, and no scrap is 100 OEE. The shift clock starts when you press Start, so waiting on a stopped line is not downtime."
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("p", {
						className: "mt-2 text-sm",
						children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
							className: broker === "live" ? "text-run" : broker === "offline" ? "text-stop" : "text-muted",
							children: broker === "live" ? view.running || view.metrics.plannedSec > 0 ? "Publishing this run to HiveMQ" : "Connected — press Start to publish" : broker === "connecting" ? "Connecting to HiveMQ" : "HiveMQ offline"
						}), broker === "live" && echo ? /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("span", {
							className: "text-muted",
							children: [" · broker echoed OEE ", echo]
						}) : null]
					})
				] }), /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
					className: "flex flex-wrap gap-2",
					children: [/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("button", {
						type: "button",
						onClick: () => setEpoch((n) => n + 1),
						className: "inline-flex h-11 items-center gap-2 rounded-full border border-border bg-surface px-4 text-sm text-fg",
						children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)(RotateCcw, {
							className: "size-4",
							"aria-hidden": "true"
						}), "New shift"]
					}), /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("a", {
						href: "/XIAO_ESP32C3_OEE.ino",
						download: true,
						className: "inline-flex h-11 items-center gap-2 rounded-full bg-fg px-4 text-sm font-medium text-ink",
						children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Download, {
							className: "size-4",
							"aria-hidden": "true"
						}), "Sketch"]
					})]
				})]
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsx)(WidgetLine, {
				running: view.running,
				speed,
				total: m.total,
				bad: m.bad,
				scrapQueued: m.scrapQueued
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("section", {
				className: "grid gap-4 lg:grid-cols-[18rem_1fr]",
				children: [/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
					className: "rounded-2xl border border-border bg-surface p-4",
					children: [
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
							className: "mb-4 flex items-baseline justify-between",
							children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("h2", {
								className: "font-display text-2xl leading-none",
								children: "Station"
							}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
								className: `font-display text-xl leading-none ${m.running ? "text-run" : "text-stop"}`,
								children: m.running ? "RUNNING" : "STOPPED"
							})]
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
							className: "grid grid-cols-3 gap-2",
							children: [
								/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
									className: "flex flex-col gap-2",
									children: [
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Led, {
											on: view.blue,
											tone: "scrap"
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("button", {
											type: "button",
											onClick: () => act(pressDefect),
											className: "h-16 rounded-xl bg-scrap text-sm font-semibold text-ink",
											children: "Defect"
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
											className: "text-center text-xs text-muted",
											children: "D4 blue"
										})
									]
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
									className: "flex flex-col gap-2",
									children: [
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Led, {
											on: view.running,
											tone: "run"
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("button", {
											type: "button",
											onClick: () => act(pressStart),
											"aria-pressed": view.running,
											className: `h-16 rounded-xl bg-run text-sm font-semibold text-ink ${view.running ? "ring-2 ring-fg ring-offset-2 ring-offset-surface" : ""}`,
											children: "Start"
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
											className: "text-center text-xs text-muted",
											children: "D5 green"
										})
									]
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
									className: "flex flex-col gap-2",
									children: [
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Led, {
											on: !view.running,
											tone: "stop"
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("button", {
											type: "button",
											onClick: () => act(pressStop),
											"aria-pressed": !view.running,
											className: `h-16 rounded-xl bg-stop text-sm font-semibold text-ink ${!view.running ? "ring-2 ring-fg ring-offset-2 ring-offset-surface" : ""}`,
											children: "Stop"
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
											className: "text-center text-xs text-muted",
											children: "D6 red"
										})
									]
								})
							]
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("label", {
							className: "mt-5 block",
							children: [
								/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("span", {
									className: "flex items-baseline justify-between",
									children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
										className: "text-sm text-muted",
										children: "Speed pot · A0"
									}), /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("span", {
										className: "font-display text-3xl leading-none tabular-nums",
										children: [pct(speed), "%"]
									})]
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("input", {
									type: "range",
									min: 80,
									max: 120,
									step: .5,
									value: speed,
									onChange: (event) => onSpeed(Number(event.target.value)),
									"aria-label": "Machine speed percent",
									className: "mt-3 w-full accent-run"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("span", {
									className: "mt-1 flex justify-between text-xs text-muted",
									children: [
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", { children: "80%" }),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", { children: "nameplate 100" }),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", { children: "120%" })
									]
								})
							]
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("dl", {
							className: "mt-4 grid grid-cols-2 gap-x-3 gap-y-1 text-sm",
							children: [
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("dt", {
									className: "text-muted",
									children: "This cycle"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("dd", {
									className: "text-right tabular-nums",
									children: m.running ? `${Math.round(m.cycleMs)} ms` : `${Math.round(1e3 / (speed / 100))} ms`
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("dt", {
									className: "text-muted",
									children: "Scrap armed"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("dd", {
									className: "text-right tabular-nums",
									children: m.scrapQueued
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("dt", {
									className: "text-muted",
									children: "Shift / run / down"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("dd", {
									className: "text-right tabular-nums",
									children: [
										clock(m.plannedSec),
										" · ",
										clock(m.runtimeSec),
										" · ",
										clock(m.downtimeSec)
									]
								})
							]
						})
					]
				}), /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
					className: "flex flex-col gap-4 rounded-2xl border border-border bg-surface p-4",
					children: [
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
							className: "flex flex-wrap items-end justify-between gap-3",
							children: [/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
								className: "text-sm text-muted",
								children: "OEE"
							}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
								className: "font-display text-6xl leading-none tabular-nums sm:text-7xl",
								children: pct(m.oee)
							})] }), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
								className: "max-w-xs text-sm text-muted",
								children: "Availability × performance × quality. Performance is the pot. At 100% with no stop and no scrap, OEE stays at 100."
							})]
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
							className: "flex flex-col gap-3",
							children: [
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Meter, {
									label: "Availability",
									value: m.availability,
									tone: "fg"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Meter, {
									label: "Performance",
									value: m.performance,
									tone: "run"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Meter, {
									label: "Quality",
									value: m.quality,
									tone: "scrap"
								})
							]
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
							className: "grid grid-cols-2 gap-3 sm:grid-cols-4",
							children: [
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Count, {
									label: "Good",
									value: m.good
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Count, {
									label: "Bad",
									value: m.bad
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Count, {
									label: "Total",
									value: m.total
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Count, {
									label: "Per minute",
									value: m.actualPerMin,
									digits: 1,
									hint: "ideal 60"
								})
							]
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
							className: `text-xs ${view.balanced ? "text-muted" : "text-stop"}`,
							children: view.balanced ? "Good + bad = total. Scrap does not add a cycle." : "Count mismatch — good + bad should equal total."
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
							className: "flex flex-wrap gap-x-4 gap-y-1 text-xs",
							children: [
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
									className: "text-fg",
									children: "OEE"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
									className: "text-muted",
									children: "Availability"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
									className: "text-run",
									children: "Performance"
								}),
								/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
									className: "text-scrap",
									children: "Quality"
								})
							]
						}),
						/* @__PURE__ */ (0, import_jsx_runtime.jsx)("div", {
							className: "h-36",
							children: mounted && view.samples.length > 1 ? /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ResponsiveContainer, {
								width: "100%",
								height: "100%",
								children: /* @__PURE__ */ (0, import_jsx_runtime.jsxs)(LineChart, {
									data: view.samples,
									children: [
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(XAxis, {
											dataKey: "t",
											hide: true
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(YAxis, {
											domain: [0, 130],
											hide: true
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Tooltip, {
											formatter: (value) => typeof value === "number" ? value.toFixed(1) : value,
											labelFormatter: (label) => `${Number(label).toFixed(0)} s`,
											contentStyle: {
												background: "var(--color-surface-2)",
												border: "1px solid var(--color-border)",
												borderRadius: "0.6rem",
												color: "var(--color-fg)"
											}
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Line, {
											type: "monotone",
											dataKey: "oee",
											name: "OEE",
											stroke: "var(--color-fg)",
											strokeWidth: 2,
											dot: false,
											isAnimationActive: false
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Line, {
											type: "monotone",
											dataKey: "availability",
											name: "Availability",
											stroke: "var(--color-muted)",
											strokeWidth: 1.5,
											dot: false,
											isAnimationActive: false
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Line, {
											type: "monotone",
											dataKey: "performance",
											name: "Performance",
											stroke: "var(--color-run)",
											strokeWidth: 1.5,
											dot: false,
											isAnimationActive: false
										}),
										/* @__PURE__ */ (0, import_jsx_runtime.jsx)(Line, {
											type: "monotone",
											dataKey: "quality",
											name: "Quality",
											stroke: "var(--color-scrap)",
											strokeWidth: 1.5,
											dot: false,
											isAnimationActive: false
										})
									]
								})
							}) : /* @__PURE__ */ (0, import_jsx_runtime.jsx)("div", {
								className: "flex h-full items-center justify-center rounded-xl border border-dashed border-border text-sm text-muted",
								children: "Trend starts when you press Start."
							})
						})
					]
				})]
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("section", {
				className: "rounded-2xl border border-border bg-surface p-4",
				children: [
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)("div", {
						className: "flex flex-wrap items-end justify-between gap-3",
						children: /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("h2", {
							className: "font-display text-2xl leading-none",
							children: "MQTT topic"
						}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
							className: "mt-1 text-sm text-muted",
							children: "Default is EET124/OEE/DrJeff/. Change the whole path. The sketch uses the same string."
						})] })
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("label", {
						className: "mt-4 block",
						children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
							className: "text-sm text-muted",
							children: "Topic"
						}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("input", {
							value: topic,
							onChange: (event) => onTopic(event.target.value),
							spellCheck: false,
							autoCapitalize: "none",
							"aria-label": "MQTT topic",
							className: "mt-2 h-12 w-full rounded-xl border border-border bg-bg px-3 font-display text-2xl text-fg outline-none"
						})]
					}),
					path !== topic && /* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
						className: "mt-2 text-sm text-stop",
						children: topic.trim() ? `Publishing uses ${path}. Spaces are removed and a trailing slash is added.` : `Empty topic uses EET124/OEE/DrJeff/.`
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
						className: "mt-4 flex flex-wrap items-center gap-3",
						children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("code", {
							className: "block overflow-x-auto rounded-xl bg-bg px-3 py-3 text-sm text-fg",
							children: line
						}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: copyLine,
							className: "h-11 shrink-0 rounded-full border border-border px-4 text-sm text-fg",
							children: copied ? "Copied" : "Copy line"
						})]
					}),
					/* @__PURE__ */ (0, import_jsx_runtime.jsx)("ul", {
						className: "mt-4 grid gap-1 sm:grid-cols-2",
						children: TOPIC_LEAVES.map((leaf) => /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("li", {
							className: "truncate rounded-lg bg-bg px-3 py-2 text-sm text-muted",
							children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
								className: "text-fg",
								children: path
							}), leaf]
						}, leaf))
					})
				]
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("section", {
				className: "grid gap-4 lg:grid-cols-2",
				children: [/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
					className: "rounded-2xl border border-border bg-surface p-4",
					children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("h2", {
						className: "font-display text-2xl leading-none",
						children: "What changed on the chip"
					}), /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("ul", {
						className: "mt-3 flex flex-col gap-3 text-sm text-muted",
						children: [
							/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("li", { children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
								className: "text-fg",
								children: "100 OEE is the clean run."
							}), " The shift starts on Start. Waiting while stopped is not downtime. Only a slower pot, a Stop, or a scrap pulls OEE down."] }),
							/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("li", { children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
								className: "text-fg",
								children: "Defects do not mint parts."
							}), " A press turns one good widget into scrap. Performance stays with the pot. Quality falls."] }),
							/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("li", { children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
								className: "text-fg",
								children: "Performance is the pot."
							}), " 100% is 60 widgets a minute. Above nameplate, OEE can go over 100. This page publishes those topics to the same HiveMQ broker as the sketch."] }),
							/* @__PURE__ */ (0, import_jsx_runtime.jsxs)("li", { children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("span", {
								className: "text-fg",
								children: "The broker can die."
							}), " The station keeps making parts. Status is a retained last-will on that student’s own topic."] })
						]
					})]
				}), /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
					className: "rounded-2xl border border-border bg-surface p-4",
					children: [/* @__PURE__ */ (0, import_jsx_runtime.jsx)("h2", {
						className: "font-display text-2xl leading-none",
						children: "Station log"
					}), /* @__PURE__ */ (0, import_jsx_runtime.jsx)("ol", {
						className: "mt-3 flex max-h-56 flex-col gap-1 overflow-auto",
						children: view.log.map((entry) => /* @__PURE__ */ (0, import_jsx_runtime.jsx)("li", {
							className: "rounded-lg bg-bg px-3 py-2 text-sm text-fg",
							children: entry.text
						}, entry.id))
					})]
				})]
			})
		]
	});
}
function Count({ label, value, digits = 0, hint }) {
	return /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", {
		className: "rounded-xl bg-bg px-3 py-2",
		children: [
			/* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
				className: "text-xs text-muted",
				children: label
			}),
			/* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
				className: "font-display text-3xl leading-none tabular-nums",
				children: digits > 0 ? value.toFixed(digits) : Math.round(value)
			}),
			hint ? /* @__PURE__ */ (0, import_jsx_runtime.jsx)("p", {
				className: "mt-1 text-xs text-muted",
				children: hint
			}) : null
		]
	});
}
function Home() {
	return /* @__PURE__ */ (0, import_jsx_runtime.jsx)(OeeBench, {});
}
//#endregion
export { Home as component };
