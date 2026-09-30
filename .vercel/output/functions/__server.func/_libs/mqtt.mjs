import { a as __toCommonJS, i as __require, n as __esmMin, r as __exportAll, t as __commonJSMin } from "../_runtime.mjs";
import { n as require_ours, t as require_bl } from "./bl+inherits+readable-stream.mjs";
import { t as require_src } from "./debug+[...].mjs";
import { t as require_commonjs } from "./lru-cache.mjs";
import { i as init_module$3, n as init_module$2, r as generateUniqueNumber, t as createBroker } from "./broker-factory+[...].mjs";
import { t as require_ip_address } from "./ip-address.mjs";
//#region node_modules/mqtt-packet/packet.js
var require_packet = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var Packet = class {
		constructor() {
			this.cmd = null;
			this.retain = false;
			this.qos = 0;
			this.dup = false;
			this.length = -1;
			this.topic = null;
			this.payload = null;
		}
	};
	module.exports = Packet;
}));
//#endregion
//#region node_modules/mqtt-packet/constants.js
var require_constants$2 = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var protocol = module.exports;
	var { Buffer: Buffer$4 } = __require("buffer");
	protocol.types = {
		0: "reserved",
		1: "connect",
		2: "connack",
		3: "publish",
		4: "puback",
		5: "pubrec",
		6: "pubrel",
		7: "pubcomp",
		8: "subscribe",
		9: "suback",
		10: "unsubscribe",
		11: "unsuback",
		12: "pingreq",
		13: "pingresp",
		14: "disconnect",
		15: "auth"
	};
	protocol.requiredHeaderFlags = {
		1: 0,
		2: 0,
		4: 0,
		5: 0,
		6: 2,
		7: 0,
		8: 2,
		9: 0,
		10: 2,
		11: 0,
		12: 0,
		13: 0,
		14: 0,
		15: 0
	};
	protocol.requiredHeaderFlagsErrors = {};
	for (const k in protocol.requiredHeaderFlags) {
		const v = protocol.requiredHeaderFlags[k];
		protocol.requiredHeaderFlagsErrors[k] = "Invalid header flag bits, must be 0x" + v.toString(16) + " for " + protocol.types[k] + " packet";
	}
	protocol.codes = {};
	for (const k in protocol.types) {
		const v = protocol.types[k];
		protocol.codes[v] = k;
	}
	protocol.CMD_SHIFT = 4;
	protocol.CMD_MASK = 240;
	protocol.DUP_MASK = 8;
	protocol.QOS_MASK = 3;
	protocol.QOS_SHIFT = 1;
	protocol.RETAIN_MASK = 1;
	protocol.VARBYTEINT_MASK = 127;
	protocol.VARBYTEINT_FIN_MASK = 128;
	protocol.VARBYTEINT_MAX = 268435455;
	protocol.SESSIONPRESENT_MASK = 1;
	protocol.SESSIONPRESENT_HEADER = Buffer$4.from([protocol.SESSIONPRESENT_MASK]);
	protocol.CONNACK_HEADER = Buffer$4.from([protocol.codes.connack << protocol.CMD_SHIFT]);
	protocol.USERNAME_MASK = 128;
	protocol.PASSWORD_MASK = 64;
	protocol.WILL_RETAIN_MASK = 32;
	protocol.WILL_QOS_MASK = 24;
	protocol.WILL_QOS_SHIFT = 3;
	protocol.WILL_FLAG_MASK = 4;
	protocol.CLEAN_SESSION_MASK = 2;
	protocol.CONNECT_HEADER = Buffer$4.from([protocol.codes.connect << protocol.CMD_SHIFT]);
	protocol.properties = {
		sessionExpiryInterval: 17,
		willDelayInterval: 24,
		receiveMaximum: 33,
		maximumPacketSize: 39,
		topicAliasMaximum: 34,
		requestResponseInformation: 25,
		requestProblemInformation: 23,
		userProperties: 38,
		authenticationMethod: 21,
		authenticationData: 22,
		payloadFormatIndicator: 1,
		messageExpiryInterval: 2,
		contentType: 3,
		responseTopic: 8,
		correlationData: 9,
		maximumQoS: 36,
		retainAvailable: 37,
		assignedClientIdentifier: 18,
		reasonString: 31,
		wildcardSubscriptionAvailable: 40,
		subscriptionIdentifiersAvailable: 41,
		sharedSubscriptionAvailable: 42,
		serverKeepAlive: 19,
		responseInformation: 26,
		serverReference: 28,
		topicAlias: 35,
		subscriptionIdentifier: 11
	};
	protocol.propertiesCodes = {};
	for (const prop in protocol.properties) {
		const id = protocol.properties[prop];
		protocol.propertiesCodes[id] = prop;
	}
	protocol.propertiesTypes = {
		sessionExpiryInterval: "int32",
		willDelayInterval: "int32",
		receiveMaximum: "int16",
		maximumPacketSize: "int32",
		topicAliasMaximum: "int16",
		requestResponseInformation: "byte",
		requestProblemInformation: "byte",
		userProperties: "pair",
		authenticationMethod: "string",
		authenticationData: "binary",
		payloadFormatIndicator: "byte",
		messageExpiryInterval: "int32",
		contentType: "string",
		responseTopic: "string",
		correlationData: "binary",
		maximumQoS: "int8",
		retainAvailable: "byte",
		assignedClientIdentifier: "string",
		reasonString: "string",
		wildcardSubscriptionAvailable: "byte",
		subscriptionIdentifiersAvailable: "byte",
		sharedSubscriptionAvailable: "byte",
		serverKeepAlive: "int16",
		responseInformation: "string",
		serverReference: "string",
		topicAlias: "int16",
		subscriptionIdentifier: "var"
	};
	function genHeader(type) {
		return [
			0,
			1,
			2
		].map((qos) => {
			return [0, 1].map((dup) => {
				return [0, 1].map((retain) => {
					const buf = Buffer$4.alloc(1);
					buf.writeUInt8(protocol.codes[type] << protocol.CMD_SHIFT | (dup ? protocol.DUP_MASK : 0) | qos << protocol.QOS_SHIFT | retain, 0, true);
					return buf;
				});
			});
		});
	}
	protocol.PUBLISH_HEADER = genHeader("publish");
	protocol.SUBSCRIBE_HEADER = genHeader("subscribe");
	protocol.SUBSCRIBE_OPTIONS_QOS_MASK = 3;
	protocol.SUBSCRIBE_OPTIONS_NL_MASK = 1;
	protocol.SUBSCRIBE_OPTIONS_NL_SHIFT = 2;
	protocol.SUBSCRIBE_OPTIONS_RAP_MASK = 1;
	protocol.SUBSCRIBE_OPTIONS_RAP_SHIFT = 3;
	protocol.SUBSCRIBE_OPTIONS_RH_MASK = 3;
	protocol.SUBSCRIBE_OPTIONS_RH_SHIFT = 4;
	protocol.SUBSCRIBE_OPTIONS_RH = [
		0,
		16,
		32
	];
	protocol.SUBSCRIBE_OPTIONS_NL = 4;
	protocol.SUBSCRIBE_OPTIONS_RAP = 8;
	protocol.SUBSCRIBE_OPTIONS_QOS = [
		0,
		1,
		2
	];
	protocol.UNSUBSCRIBE_HEADER = genHeader("unsubscribe");
	protocol.ACKS = {
		unsuback: genHeader("unsuback"),
		puback: genHeader("puback"),
		pubcomp: genHeader("pubcomp"),
		pubrel: genHeader("pubrel"),
		pubrec: genHeader("pubrec")
	};
	protocol.SUBACK_HEADER = Buffer$4.from([protocol.codes.suback << protocol.CMD_SHIFT]);
	protocol.VERSION3 = Buffer$4.from([3]);
	protocol.VERSION4 = Buffer$4.from([4]);
	protocol.VERSION5 = Buffer$4.from([5]);
	protocol.VERSION131 = Buffer$4.from([131]);
	protocol.VERSION132 = Buffer$4.from([132]);
	protocol.QOS = [
		0,
		1,
		2
	].map((qos) => {
		return Buffer$4.from([qos]);
	});
	protocol.EMPTY = {
		pingreq: Buffer$4.from([protocol.codes.pingreq << 4, 0]),
		pingresp: Buffer$4.from([protocol.codes.pingresp << 4, 0]),
		disconnect: Buffer$4.from([protocol.codes.disconnect << 4, 0])
	};
	protocol.MQTT5_PUBACK_PUBREC_CODES = {
		0: "Success",
		16: "No matching subscribers",
		128: "Unspecified error",
		131: "Implementation specific error",
		135: "Not authorized",
		144: "Topic Name invalid",
		145: "Packet identifier in use",
		151: "Quota exceeded",
		153: "Payload format invalid"
	};
	protocol.MQTT5_PUBREL_PUBCOMP_CODES = {
		0: "Success",
		146: "Packet Identifier not found"
	};
	protocol.MQTT5_SUBACK_CODES = {
		0: "Granted QoS 0",
		1: "Granted QoS 1",
		2: "Granted QoS 2",
		128: "Unspecified error",
		131: "Implementation specific error",
		135: "Not authorized",
		143: "Topic Filter invalid",
		145: "Packet Identifier in use",
		151: "Quota exceeded",
		158: "Shared Subscriptions not supported",
		161: "Subscription Identifiers not supported",
		162: "Wildcard Subscriptions not supported"
	};
	protocol.MQTT5_UNSUBACK_CODES = {
		0: "Success",
		17: "No subscription existed",
		128: "Unspecified error",
		131: "Implementation specific error",
		135: "Not authorized",
		143: "Topic Filter invalid",
		145: "Packet Identifier in use"
	};
	protocol.MQTT5_DISCONNECT_CODES = {
		0: "Normal disconnection",
		4: "Disconnect with Will Message",
		128: "Unspecified error",
		129: "Malformed Packet",
		130: "Protocol Error",
		131: "Implementation specific error",
		135: "Not authorized",
		137: "Server busy",
		139: "Server shutting down",
		141: "Keep Alive timeout",
		142: "Session taken over",
		143: "Topic Filter invalid",
		144: "Topic Name invalid",
		147: "Receive Maximum exceeded",
		148: "Topic Alias invalid",
		149: "Packet too large",
		150: "Message rate too high",
		151: "Quota exceeded",
		152: "Administrative action",
		153: "Payload format invalid",
		154: "Retain not supported",
		155: "QoS not supported",
		156: "Use another server",
		157: "Server moved",
		158: "Shared Subscriptions not supported",
		159: "Connection rate exceeded",
		160: "Maximum connect time",
		161: "Subscription Identifiers not supported",
		162: "Wildcard Subscriptions not supported"
	};
	protocol.MQTT5_AUTH_CODES = {
		0: "Success",
		24: "Continue authentication",
		25: "Re-authenticate"
	};
}));
//#endregion
//#region node_modules/mqtt-packet/parser.js
var require_parser = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var bl = require_bl();
	var { EventEmitter: EventEmitter$3 } = __require("events");
	var Packet = require_packet();
	var constants = require_constants$2();
	var debug = require_src()("mqtt-packet:parser");
	module.exports = class Parser extends EventEmitter$3 {
		constructor() {
			super();
			this.parser = this.constructor.parser;
		}
		static parser(opt) {
			if (!(this instanceof Parser)) return new Parser().parser(opt);
			this.settings = opt || {};
			this._states = [
				"_parseHeader",
				"_parseLength",
				"_parsePayload",
				"_newPacket"
			];
			this._resetState();
			return this;
		}
		_resetState() {
			debug("_resetState: resetting packet, error, _list, and _stateCounter");
			this.packet = new Packet();
			this.error = null;
			this._list = bl();
			this._stateCounter = 0;
		}
		parse(buf) {
			if (this.error) this._resetState();
			this._list.append(buf);
			debug("parse: current state: %s", this._states[this._stateCounter]);
			while ((this.packet.length !== -1 || this._list.length > 0) && this[this._states[this._stateCounter]]() && !this.error) {
				this._stateCounter++;
				debug("parse: state complete. _stateCounter is now: %d", this._stateCounter);
				debug("parse: packet.length: %d, buffer list length: %d", this.packet.length, this._list.length);
				if (this._stateCounter >= this._states.length) this._stateCounter = 0;
			}
			debug("parse: exited while loop. packet: %d, buffer list length: %d", this.packet.length, this._list.length);
			return this._list.length;
		}
		_parseHeader() {
			const zero = this._list.readUInt8(0);
			const cmdIndex = zero >> constants.CMD_SHIFT;
			this.packet.cmd = constants.types[cmdIndex];
			const headerFlags = zero & 15;
			const requiredHeaderFlags = constants.requiredHeaderFlags[cmdIndex];
			if (requiredHeaderFlags != null && headerFlags !== requiredHeaderFlags) return this._emitError(new Error(constants.requiredHeaderFlagsErrors[cmdIndex]));
			this.packet.retain = (zero & constants.RETAIN_MASK) !== 0;
			this.packet.qos = zero >> constants.QOS_SHIFT & constants.QOS_MASK;
			if (this.packet.qos > 2) return this._emitError(/* @__PURE__ */ new Error("Packet must not have both QoS bits set to 1"));
			this.packet.dup = (zero & constants.DUP_MASK) !== 0;
			debug("_parseHeader: packet: %o", this.packet);
			this._list.consume(1);
			return true;
		}
		_parseLength() {
			const result = this._parseVarByteNum(true);
			if (result) {
				this.packet.length = result.value;
				this._list.consume(result.bytes);
			}
			debug("_parseLength %d", result.value);
			return !!result;
		}
		_parsePayload() {
			debug("_parsePayload: payload %O", this._list);
			let result = false;
			if (this.packet.length === 0 || this._list.length >= this.packet.length) {
				this._pos = 0;
				switch (this.packet.cmd) {
					case "connect":
						this._parseConnect();
						break;
					case "connack":
						this._parseConnack();
						break;
					case "publish":
						this._parsePublish();
						break;
					case "puback":
					case "pubrec":
					case "pubrel":
					case "pubcomp":
						this._parseConfirmation();
						break;
					case "subscribe":
						this._parseSubscribe();
						break;
					case "suback":
						this._parseSuback();
						break;
					case "unsubscribe":
						this._parseUnsubscribe();
						break;
					case "unsuback":
						this._parseUnsuback();
						break;
					case "pingreq":
					case "pingresp": break;
					case "disconnect":
						this._parseDisconnect();
						break;
					case "auth":
						this._parseAuth();
						break;
					default: this._emitError(/* @__PURE__ */ new Error("Not supported"));
				}
				result = true;
			}
			debug("_parsePayload complete result: %s", result);
			return result;
		}
		_parseConnect() {
			debug("_parseConnect");
			let topic;
			let payload;
			let password;
			let username;
			const flags = {};
			const packet = this.packet;
			const protocolId = this._parseString();
			if (protocolId === null) return this._emitError(/* @__PURE__ */ new Error("Cannot parse protocolId"));
			if (protocolId !== "MQTT" && protocolId !== "MQIsdp") return this._emitError(/* @__PURE__ */ new Error("Invalid protocolId"));
			packet.protocolId = protocolId;
			if (this._pos >= this._list.length) return this._emitError(/* @__PURE__ */ new Error("Packet too short"));
			packet.protocolVersion = this._list.readUInt8(this._pos);
			if (packet.protocolVersion >= 128) {
				packet.bridgeMode = true;
				packet.protocolVersion = packet.protocolVersion - 128;
			}
			if (packet.protocolVersion !== 3 && packet.protocolVersion !== 4 && packet.protocolVersion !== 5) return this._emitError(/* @__PURE__ */ new Error("Invalid protocol version"));
			this._pos++;
			if (this._pos >= this._list.length) return this._emitError(/* @__PURE__ */ new Error("Packet too short"));
			if (this._list.readUInt8(this._pos) & 1) return this._emitError(/* @__PURE__ */ new Error("Connect flag bit 0 must be 0, but got 1"));
			flags.username = this._list.readUInt8(this._pos) & constants.USERNAME_MASK;
			flags.password = this._list.readUInt8(this._pos) & constants.PASSWORD_MASK;
			flags.will = this._list.readUInt8(this._pos) & constants.WILL_FLAG_MASK;
			const willRetain = !!(this._list.readUInt8(this._pos) & constants.WILL_RETAIN_MASK);
			const willQos = (this._list.readUInt8(this._pos) & constants.WILL_QOS_MASK) >> constants.WILL_QOS_SHIFT;
			if (flags.will) {
				packet.will = {};
				packet.will.retain = willRetain;
				packet.will.qos = willQos;
			} else {
				if (willRetain) return this._emitError(/* @__PURE__ */ new Error("Will Retain Flag must be set to zero when Will Flag is set to 0"));
				if (willQos) return this._emitError(/* @__PURE__ */ new Error("Will QoS must be set to zero when Will Flag is set to 0"));
			}
			packet.clean = (this._list.readUInt8(this._pos) & constants.CLEAN_SESSION_MASK) !== 0;
			this._pos++;
			packet.keepalive = this._parseNum();
			if (packet.keepalive === -1) return this._emitError(/* @__PURE__ */ new Error("Packet too short"));
			if (packet.protocolVersion === 5) {
				const properties = this._parseProperties();
				if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
			}
			const clientId = this._parseString();
			if (clientId === null) return this._emitError(/* @__PURE__ */ new Error("Packet too short"));
			packet.clientId = clientId;
			debug("_parseConnect: packet.clientId: %s", packet.clientId);
			if (flags.will) {
				if (packet.protocolVersion === 5) {
					const willProperties = this._parseProperties();
					if (Object.getOwnPropertyNames(willProperties).length) packet.will.properties = willProperties;
				}
				topic = this._parseString();
				if (topic === null) return this._emitError(/* @__PURE__ */ new Error("Cannot parse will topic"));
				packet.will.topic = topic;
				debug("_parseConnect: packet.will.topic: %s", packet.will.topic);
				payload = this._parseBuffer();
				if (payload === null) return this._emitError(/* @__PURE__ */ new Error("Cannot parse will payload"));
				packet.will.payload = payload;
				debug("_parseConnect: packet.will.paylaod: %s", packet.will.payload);
			}
			if (flags.username) {
				username = this._parseString();
				if (username === null) return this._emitError(/* @__PURE__ */ new Error("Cannot parse username"));
				packet.username = username;
				debug("_parseConnect: packet.username: %s", packet.username);
			}
			if (flags.password) {
				password = this._parseBuffer();
				if (password === null) return this._emitError(/* @__PURE__ */ new Error("Cannot parse password"));
				packet.password = password;
			}
			this.settings = packet;
			debug("_parseConnect: complete");
			return packet;
		}
		_parseConnack() {
			debug("_parseConnack");
			const packet = this.packet;
			if (this._list.length < 1) return null;
			const flags = this._list.readUInt8(this._pos++);
			if (flags > 1) return this._emitError(/* @__PURE__ */ new Error("Invalid connack flags, bits 7-1 must be set to 0"));
			packet.sessionPresent = !!(flags & constants.SESSIONPRESENT_MASK);
			if (this.settings.protocolVersion === 5) {
				if (this._list.length >= 2) packet.reasonCode = this._list.readUInt8(this._pos++);
				else packet.reasonCode = 0;
			} else {
				if (this._list.length < 2) return null;
				packet.returnCode = this._list.readUInt8(this._pos++);
			}
			if (packet.returnCode === -1 || packet.reasonCode === -1) return this._emitError(/* @__PURE__ */ new Error("Cannot parse return code"));
			if (this.settings.protocolVersion === 5) {
				const properties = this._parseProperties();
				if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
			}
			debug("_parseConnack: complete");
		}
		_parsePublish() {
			debug("_parsePublish");
			const packet = this.packet;
			packet.topic = this._parseString();
			if (packet.topic === null) return this._emitError(/* @__PURE__ */ new Error("Cannot parse topic"));
			if (packet.qos > 0) {
				if (!this._parseMessageId()) return;
			}
			if (this.settings.protocolVersion === 5) {
				const properties = this._parseProperties();
				if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
			}
			packet.payload = this._list.slice(this._pos, packet.length);
			debug("_parsePublish: payload from buffer list: %o", packet.payload);
		}
		_parseSubscribe() {
			debug("_parseSubscribe");
			const packet = this.packet;
			let topic;
			let options;
			let qos;
			let rh;
			let rap;
			let nl;
			let subscription;
			packet.subscriptions = [];
			if (!this._parseMessageId()) return;
			if (this.settings.protocolVersion === 5) {
				const properties = this._parseProperties();
				if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
			}
			if (packet.length <= 0) return this._emitError(/* @__PURE__ */ new Error("Malformed subscribe, no payload specified"));
			while (this._pos < packet.length) {
				topic = this._parseString();
				if (topic === null) return this._emitError(/* @__PURE__ */ new Error("Cannot parse topic"));
				if (this._pos >= packet.length) return this._emitError(/* @__PURE__ */ new Error("Malformed Subscribe Payload"));
				options = this._parseByte();
				if (this.settings.protocolVersion === 5) {
					if (options & 192) return this._emitError(/* @__PURE__ */ new Error("Invalid subscribe topic flag bits, bits 7-6 must be 0"));
				} else if (options & 252) return this._emitError(/* @__PURE__ */ new Error("Invalid subscribe topic flag bits, bits 7-2 must be 0"));
				qos = options & constants.SUBSCRIBE_OPTIONS_QOS_MASK;
				if (qos > 2) return this._emitError(/* @__PURE__ */ new Error("Invalid subscribe QoS, must be <= 2"));
				nl = (options >> constants.SUBSCRIBE_OPTIONS_NL_SHIFT & constants.SUBSCRIBE_OPTIONS_NL_MASK) !== 0;
				rap = (options >> constants.SUBSCRIBE_OPTIONS_RAP_SHIFT & constants.SUBSCRIBE_OPTIONS_RAP_MASK) !== 0;
				rh = options >> constants.SUBSCRIBE_OPTIONS_RH_SHIFT & constants.SUBSCRIBE_OPTIONS_RH_MASK;
				if (rh > 2) return this._emitError(/* @__PURE__ */ new Error("Invalid retain handling, must be <= 2"));
				subscription = {
					topic,
					qos
				};
				if (this.settings.protocolVersion === 5) {
					subscription.nl = nl;
					subscription.rap = rap;
					subscription.rh = rh;
				} else if (this.settings.bridgeMode) {
					subscription.rh = 0;
					subscription.rap = true;
					subscription.nl = true;
				}
				debug("_parseSubscribe: push subscription `%s` to subscription", subscription);
				packet.subscriptions.push(subscription);
			}
		}
		_parseSuback() {
			debug("_parseSuback");
			const packet = this.packet;
			this.packet.granted = [];
			if (!this._parseMessageId()) return;
			if (this.settings.protocolVersion === 5) {
				const properties = this._parseProperties();
				if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
			}
			if (packet.length <= 0) return this._emitError(/* @__PURE__ */ new Error("Malformed suback, no payload specified"));
			while (this._pos < this.packet.length) {
				const code = this._list.readUInt8(this._pos++);
				if (this.settings.protocolVersion === 5) {
					if (!constants.MQTT5_SUBACK_CODES[code]) return this._emitError(/* @__PURE__ */ new Error("Invalid suback code"));
				} else if (code > 2 && code !== 128) return this._emitError(/* @__PURE__ */ new Error("Invalid suback QoS, must be 0, 1, 2 or 128"));
				this.packet.granted.push(code);
			}
		}
		_parseUnsubscribe() {
			debug("_parseUnsubscribe");
			const packet = this.packet;
			packet.unsubscriptions = [];
			if (!this._parseMessageId()) return;
			if (this.settings.protocolVersion === 5) {
				const properties = this._parseProperties();
				if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
			}
			if (packet.length <= 0) return this._emitError(/* @__PURE__ */ new Error("Malformed unsubscribe, no payload specified"));
			while (this._pos < packet.length) {
				const topic = this._parseString();
				if (topic === null) return this._emitError(/* @__PURE__ */ new Error("Cannot parse topic"));
				debug("_parseUnsubscribe: push topic `%s` to unsubscriptions", topic);
				packet.unsubscriptions.push(topic);
			}
		}
		_parseUnsuback() {
			debug("_parseUnsuback");
			const packet = this.packet;
			if (!this._parseMessageId()) return this._emitError(/* @__PURE__ */ new Error("Cannot parse messageId"));
			if ((this.settings.protocolVersion === 3 || this.settings.protocolVersion === 4) && packet.length !== 2) return this._emitError(/* @__PURE__ */ new Error("Malformed unsuback, payload length must be 2"));
			if (packet.length <= 0) return this._emitError(/* @__PURE__ */ new Error("Malformed unsuback, no payload specified"));
			if (this.settings.protocolVersion === 5) {
				const properties = this._parseProperties();
				if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
				packet.granted = [];
				while (this._pos < this.packet.length) {
					const code = this._list.readUInt8(this._pos++);
					if (!constants.MQTT5_UNSUBACK_CODES[code]) return this._emitError(/* @__PURE__ */ new Error("Invalid unsuback code"));
					this.packet.granted.push(code);
				}
			}
		}
		_parseConfirmation() {
			debug("_parseConfirmation: packet.cmd: `%s`", this.packet.cmd);
			const packet = this.packet;
			this._parseMessageId();
			if (this.settings.protocolVersion === 5) {
				if (packet.length > 2) {
					packet.reasonCode = this._parseByte();
					switch (this.packet.cmd) {
						case "puback":
						case "pubrec":
							if (!constants.MQTT5_PUBACK_PUBREC_CODES[packet.reasonCode]) return this._emitError(/* @__PURE__ */ new Error("Invalid " + this.packet.cmd + " reason code"));
							break;
						case "pubrel":
						case "pubcomp": if (!constants.MQTT5_PUBREL_PUBCOMP_CODES[packet.reasonCode]) return this._emitError(/* @__PURE__ */ new Error("Invalid " + this.packet.cmd + " reason code"));
					}
					debug("_parseConfirmation: packet.reasonCode `%d`", packet.reasonCode);
				} else packet.reasonCode = 0;
				if (packet.length > 3) {
					const properties = this._parseProperties();
					if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
				}
			}
			return true;
		}
		_parseDisconnect() {
			const packet = this.packet;
			debug("_parseDisconnect");
			if (this.settings.protocolVersion === 5) {
				if (this._list.length > 0) {
					packet.reasonCode = this._parseByte();
					if (!constants.MQTT5_DISCONNECT_CODES[packet.reasonCode]) this._emitError(/* @__PURE__ */ new Error("Invalid disconnect reason code"));
				} else packet.reasonCode = 0;
				const properties = this._parseProperties();
				if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
			}
			debug("_parseDisconnect result: true");
			return true;
		}
		_parseAuth() {
			debug("_parseAuth");
			const packet = this.packet;
			if (this.settings.protocolVersion !== 5) return this._emitError(/* @__PURE__ */ new Error("Not supported auth packet for this version MQTT"));
			packet.reasonCode = this._parseByte();
			if (!constants.MQTT5_AUTH_CODES[packet.reasonCode]) return this._emitError(/* @__PURE__ */ new Error("Invalid auth reason code"));
			const properties = this._parseProperties();
			if (Object.getOwnPropertyNames(properties).length) packet.properties = properties;
			debug("_parseAuth: result: true");
			return true;
		}
		_parseMessageId() {
			const packet = this.packet;
			packet.messageId = this._parseNum();
			if (packet.messageId === null) {
				this._emitError(/* @__PURE__ */ new Error("Cannot parse messageId"));
				return false;
			}
			debug("_parseMessageId: packet.messageId %d", packet.messageId);
			return true;
		}
		_parseString(maybeBuffer) {
			const length = this._parseNum();
			const end = length + this._pos;
			if (length === -1 || end > this._list.length || end > this.packet.length) return null;
			const result = this._list.toString("utf8", this._pos, end);
			this._pos += length;
			debug("_parseString: result: %s", result);
			return result;
		}
		_parseStringPair() {
			debug("_parseStringPair");
			return {
				name: this._parseString(),
				value: this._parseString()
			};
		}
		_parseBuffer() {
			const length = this._parseNum();
			const end = length + this._pos;
			if (length === -1 || end > this._list.length || end > this.packet.length) return null;
			const result = this._list.slice(this._pos, end);
			this._pos += length;
			debug("_parseBuffer: result: %o", result);
			return result;
		}
		_parseNum() {
			if (this._list.length - this._pos < 2) return -1;
			const result = this._list.readUInt16BE(this._pos);
			this._pos += 2;
			debug("_parseNum: result: %s", result);
			return result;
		}
		_parse4ByteNum() {
			if (this._list.length - this._pos < 4) return -1;
			const result = this._list.readUInt32BE(this._pos);
			this._pos += 4;
			debug("_parse4ByteNum: result: %s", result);
			return result;
		}
		_parseVarByteNum(fullInfoFlag) {
			debug("_parseVarByteNum");
			const maxBytes = 4;
			let bytes = 0;
			let mul = 1;
			let value = 0;
			let result = false;
			let current;
			const padding = this._pos ? this._pos : 0;
			while (bytes < maxBytes && padding + bytes < this._list.length) {
				current = this._list.readUInt8(padding + bytes++);
				value += mul * (current & constants.VARBYTEINT_MASK);
				mul *= 128;
				if ((current & constants.VARBYTEINT_FIN_MASK) === 0) {
					result = true;
					break;
				}
				if (this._list.length <= bytes) break;
			}
			if (!result && bytes === maxBytes && this._list.length >= bytes) this._emitError(/* @__PURE__ */ new Error("Invalid variable byte integer"));
			if (padding) this._pos += bytes;
			if (result) {
				if (fullInfoFlag) result = {
					bytes,
					value
				};
				else result = value;
			} else result = false;
			debug("_parseVarByteNum: result: %o", result);
			return result;
		}
		_parseByte() {
			let result;
			if (this._pos < this._list.length) {
				result = this._list.readUInt8(this._pos);
				this._pos++;
			}
			debug("_parseByte: result: %o", result);
			return result;
		}
		_parseByType(type) {
			debug("_parseByType: type: %s", type);
			switch (type) {
				case "byte": return this._parseByte() !== 0;
				case "int8": return this._parseByte();
				case "int16": return this._parseNum();
				case "int32": return this._parse4ByteNum();
				case "var": return this._parseVarByteNum();
				case "string": return this._parseString();
				case "pair": return this._parseStringPair();
				case "binary": return this._parseBuffer();
			}
		}
		_parseProperties() {
			debug("_parseProperties");
			const length = this._parseVarByteNum();
			const end = this._pos + length;
			const result = {};
			while (this._pos < end) {
				const type = this._parseByte();
				if (!type) {
					this._emitError(/* @__PURE__ */ new Error("Cannot parse property code type"));
					return false;
				}
				const name = constants.propertiesCodes[type];
				if (!name) {
					this._emitError(/* @__PURE__ */ new Error("Unknown property"));
					return false;
				}
				if (name === "userProperties") {
					if (!result[name]) result[name] = Object.create(null);
					const currentUserProperty = this._parseByType(constants.propertiesTypes[name]);
					if (result[name][currentUserProperty.name]) {
						if (Array.isArray(result[name][currentUserProperty.name])) result[name][currentUserProperty.name].push(currentUserProperty.value);
						else {
							const currentValue = result[name][currentUserProperty.name];
							result[name][currentUserProperty.name] = [currentValue];
							result[name][currentUserProperty.name].push(currentUserProperty.value);
						}
					} else result[name][currentUserProperty.name] = currentUserProperty.value;
					continue;
				}
				if (result[name]) {
					if (Array.isArray(result[name])) result[name].push(this._parseByType(constants.propertiesTypes[name]));
					else {
						result[name] = [result[name]];
						result[name].push(this._parseByType(constants.propertiesTypes[name]));
					}
				} else result[name] = this._parseByType(constants.propertiesTypes[name]);
			}
			return result;
		}
		_newPacket() {
			debug("_newPacket");
			if (this.packet) {
				this._list.consume(this.packet.length);
				debug("_newPacket: parser emit packet: packet.cmd: %s, packet.payload: %s, packet.length: %d", this.packet.cmd, this.packet.payload, this.packet.length);
				this.emit("packet", this.packet);
			}
			debug("_newPacket: new packet");
			this.packet = new Packet();
			this._pos = 0;
			return true;
		}
		_emitError(err) {
			debug("_emitError", err);
			this.error = err;
			this.emit("error", err);
		}
	};
}));
//#endregion
//#region node_modules/mqtt-packet/numbers.js
var require_numbers = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { Buffer: Buffer$3 } = __require("buffer");
	var max = 65536;
	var cache = {};
	var SubOk = Buffer$3.isBuffer(Buffer$3.from([1, 2]).subarray(0, 1));
	function generateBuffer(i) {
		const buffer = Buffer$3.allocUnsafe(2);
		buffer.writeUInt8(i >> 8, 0);
		buffer.writeUInt8(i & 255, 1);
		return buffer;
	}
	function generateCache() {
		for (let i = 0; i < max; i++) cache[i] = generateBuffer(i);
	}
	function genBufVariableByteInt(num) {
		const maxLength = 4;
		let digit = 0;
		let pos = 0;
		const buffer = Buffer$3.allocUnsafe(maxLength);
		do {
			digit = num % 128 | 0;
			num = num / 128 | 0;
			if (num > 0) digit = digit | 128;
			buffer.writeUInt8(digit, pos++);
		} while (num > 0 && pos < maxLength);
		if (num > 0) pos = 0;
		return SubOk ? buffer.subarray(0, pos) : buffer.slice(0, pos);
	}
	function generate4ByteBuffer(num) {
		const buffer = Buffer$3.allocUnsafe(4);
		buffer.writeUInt32BE(num, 0);
		return buffer;
	}
	module.exports = {
		cache,
		generateCache,
		generateNumber: generateBuffer,
		genBufVariableByteInt,
		generate4ByteBuffer
	};
}));
//#endregion
//#region node_modules/process-nextick-args/index.js
var require_process_nextick_args = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	if (typeof process === "undefined" || !process.version || process.version.indexOf("v0.") === 0 || process.version.indexOf("v1.") === 0 && process.version.indexOf("v1.8.") !== 0) module.exports = { nextTick };
	else module.exports = process;
	function nextTick(fn, arg1, arg2, arg3) {
		if (typeof fn !== "function") throw new TypeError("\"callback\" argument must be a function");
		var len = arguments.length;
		var args, i;
		switch (len) {
			case 0:
			case 1: return process.nextTick(fn);
			case 2: return process.nextTick(function afterTickOne() {
				fn.call(null, arg1);
			});
			case 3: return process.nextTick(function afterTickTwo() {
				fn.call(null, arg1, arg2);
			});
			case 4: return process.nextTick(function afterTickThree() {
				fn.call(null, arg1, arg2, arg3);
			});
			default:
				args = new Array(len - 1);
				i = 0;
				while (i < args.length) args[i++] = arguments[i];
				return process.nextTick(function afterTick() {
					fn.apply(null, args);
				});
		}
	}
}));
//#endregion
//#region node_modules/mqtt-packet/writeToStream.js
var require_writeToStream = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var protocol = require_constants$2();
	var { Buffer: Buffer$2 } = __require("buffer");
	var empty = Buffer$2.allocUnsafe(0);
	var zeroBuf = Buffer$2.from([0]);
	var numbers = require_numbers();
	var nextTick = require_process_nextick_args().nextTick;
	var debug = require_src()("mqtt-packet:writeToStream");
	var numCache = numbers.cache;
	var generateNumber = numbers.generateNumber;
	var generateCache = numbers.generateCache;
	var genBufVariableByteInt = numbers.genBufVariableByteInt;
	var generate4ByteBuffer = numbers.generate4ByteBuffer;
	var writeNumber = writeNumberCached;
	var toGenerate = true;
	function generate(packet, stream, opts) {
		debug("generate called");
		if (stream.cork) {
			stream.cork();
			nextTick(uncork, stream);
		}
		if (toGenerate) {
			toGenerate = false;
			generateCache();
		}
		debug("generate: packet.cmd: %s", packet.cmd);
		switch (packet.cmd) {
			case "connect": return connect(packet, stream, opts);
			case "connack": return connack(packet, stream, opts);
			case "publish": return publish(packet, stream, opts);
			case "puback":
			case "pubrec":
			case "pubrel":
			case "pubcomp": return confirmation(packet, stream, opts);
			case "subscribe": return subscribe(packet, stream, opts);
			case "suback": return suback(packet, stream, opts);
			case "unsubscribe": return unsubscribe(packet, stream, opts);
			case "unsuback": return unsuback(packet, stream, opts);
			case "pingreq":
			case "pingresp": return emptyPacket(packet, stream, opts);
			case "disconnect": return disconnect(packet, stream, opts);
			case "auth": return auth(packet, stream, opts);
			default:
				stream.destroy(/* @__PURE__ */ new Error("Unknown command"));
				return false;
		}
	}
	/**
	* Controls numbers cache.
	* Set to "false" to allocate buffers on-the-flight instead of pre-generated cache
	*/
	Object.defineProperty(generate, "cacheNumbers", {
		get() {
			return writeNumber === writeNumberCached;
		},
		set(value) {
			if (value) {
				if (!numCache || Object.keys(numCache).length === 0) toGenerate = true;
				writeNumber = writeNumberCached;
			} else {
				toGenerate = false;
				writeNumber = writeNumberGenerated;
			}
		}
	});
	function uncork(stream) {
		stream.uncork();
	}
	function connect(packet, stream, opts) {
		const settings = packet || {};
		const protocolId = settings.protocolId || "MQTT";
		let protocolVersion = settings.protocolVersion || 4;
		const will = settings.will;
		let clean = settings.clean;
		const keepalive = settings.keepalive || 0;
		const clientId = settings.clientId || "";
		const username = settings.username;
		const password = settings.password;
		const properties = settings.properties;
		if (clean === void 0) clean = true;
		let length = 0;
		if (!protocolId || typeof protocolId !== "string" && !Buffer$2.isBuffer(protocolId)) {
			stream.destroy(/* @__PURE__ */ new Error("Invalid protocolId"));
			return false;
		} else length += protocolId.length + 2;
		if (protocolVersion !== 3 && protocolVersion !== 4 && protocolVersion !== 5) {
			stream.destroy(/* @__PURE__ */ new Error("Invalid protocol version"));
			return false;
		} else length += 1;
		if ((typeof clientId === "string" || Buffer$2.isBuffer(clientId)) && (clientId || protocolVersion >= 4) && (clientId || clean)) length += Buffer$2.byteLength(clientId) + 2;
		else {
			if (protocolVersion < 4) {
				stream.destroy(/* @__PURE__ */ new Error("clientId must be supplied before 3.1.1"));
				return false;
			}
			if (clean * 1 === 0) {
				stream.destroy(/* @__PURE__ */ new Error("clientId must be given if cleanSession set to 0"));
				return false;
			}
		}
		if (typeof keepalive !== "number" || keepalive < 0 || keepalive > 65535 || keepalive % 1 !== 0) {
			stream.destroy(/* @__PURE__ */ new Error("Invalid keepalive"));
			return false;
		} else length += 2;
		length += 1;
		let propertiesData;
		let willProperties;
		if (protocolVersion === 5) {
			propertiesData = getProperties(stream, properties);
			if (!propertiesData) return false;
			length += propertiesData.length;
		}
		if (will) {
			if (typeof will !== "object") {
				stream.destroy(/* @__PURE__ */ new Error("Invalid will"));
				return false;
			}
			if (!will.topic || typeof will.topic !== "string") {
				stream.destroy(/* @__PURE__ */ new Error("Invalid will topic"));
				return false;
			} else length += Buffer$2.byteLength(will.topic) + 2;
			length += 2;
			if (will.payload) {
				if (will.payload.length >= 0) {
					if (typeof will.payload === "string") length += Buffer$2.byteLength(will.payload);
					else length += will.payload.length;
				} else {
					stream.destroy(/* @__PURE__ */ new Error("Invalid will payload"));
					return false;
				}
			}
			willProperties = {};
			if (protocolVersion === 5) {
				willProperties = getProperties(stream, will.properties);
				if (!willProperties) return false;
				length += willProperties.length;
			}
		}
		let providedUsername = false;
		if (username != null) {
			if (isStringOrBuffer(username)) {
				providedUsername = true;
				length += Buffer$2.byteLength(username) + 2;
			} else {
				stream.destroy(/* @__PURE__ */ new Error("Invalid username"));
				return false;
			}
		}
		if (password != null) {
			if (!providedUsername) {
				stream.destroy(/* @__PURE__ */ new Error("Username is required to use password"));
				return false;
			}
			if (isStringOrBuffer(password)) length += byteLength(password) + 2;
			else {
				stream.destroy(/* @__PURE__ */ new Error("Invalid password"));
				return false;
			}
		}
		stream.write(protocol.CONNECT_HEADER);
		writeVarByteInt(stream, length);
		writeStringOrBuffer(stream, protocolId);
		if (settings.bridgeMode) protocolVersion += 128;
		stream.write(protocolVersion === 131 ? protocol.VERSION131 : protocolVersion === 132 ? protocol.VERSION132 : protocolVersion === 4 ? protocol.VERSION4 : protocolVersion === 5 ? protocol.VERSION5 : protocol.VERSION3);
		let flags = 0;
		flags |= username != null ? protocol.USERNAME_MASK : 0;
		flags |= password != null ? protocol.PASSWORD_MASK : 0;
		flags |= will && will.retain ? protocol.WILL_RETAIN_MASK : 0;
		flags |= will && will.qos ? will.qos << protocol.WILL_QOS_SHIFT : 0;
		flags |= will ? protocol.WILL_FLAG_MASK : 0;
		flags |= clean ? protocol.CLEAN_SESSION_MASK : 0;
		stream.write(Buffer$2.from([flags]));
		writeNumber(stream, keepalive);
		if (protocolVersion === 5) propertiesData.write();
		writeStringOrBuffer(stream, clientId);
		if (will) {
			if (protocolVersion === 5) willProperties.write();
			writeString(stream, will.topic);
			writeStringOrBuffer(stream, will.payload);
		}
		if (username != null) writeStringOrBuffer(stream, username);
		if (password != null) writeStringOrBuffer(stream, password);
		return true;
	}
	function connack(packet, stream, opts) {
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const rc = version === 5 ? settings.reasonCode : settings.returnCode;
		const properties = settings.properties;
		let length = 2;
		if (typeof rc !== "number") {
			stream.destroy(/* @__PURE__ */ new Error("Invalid return code"));
			return false;
		}
		let propertiesData = null;
		if (version === 5) {
			propertiesData = getProperties(stream, properties);
			if (!propertiesData) return false;
			length += propertiesData.length;
		}
		stream.write(protocol.CONNACK_HEADER);
		writeVarByteInt(stream, length);
		stream.write(settings.sessionPresent ? protocol.SESSIONPRESENT_HEADER : zeroBuf);
		stream.write(Buffer$2.from([rc]));
		if (propertiesData != null) propertiesData.write();
		return true;
	}
	function publish(packet, stream, opts) {
		debug("publish: packet: %o", packet);
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const qos = settings.qos || 0;
		const retain = settings.retain ? protocol.RETAIN_MASK : 0;
		const topic = settings.topic;
		const payload = settings.payload || empty;
		const id = settings.messageId;
		const properties = settings.properties;
		let length = 0;
		if (typeof topic === "string") length += Buffer$2.byteLength(topic) + 2;
		else if (Buffer$2.isBuffer(topic)) length += topic.length + 2;
		else {
			stream.destroy(/* @__PURE__ */ new Error("Invalid topic"));
			return false;
		}
		if (!Buffer$2.isBuffer(payload)) length += Buffer$2.byteLength(payload);
		else length += payload.length;
		if (qos && typeof id !== "number") {
			stream.destroy(/* @__PURE__ */ new Error("Invalid messageId"));
			return false;
		} else if (qos) length += 2;
		let propertiesData = null;
		if (version === 5) {
			propertiesData = getProperties(stream, properties);
			if (!propertiesData) return false;
			length += propertiesData.length;
		}
		stream.write(protocol.PUBLISH_HEADER[qos][settings.dup ? 1 : 0][retain ? 1 : 0]);
		writeVarByteInt(stream, length);
		writeNumber(stream, byteLength(topic));
		stream.write(topic);
		if (qos > 0) writeNumber(stream, id);
		if (propertiesData != null) propertiesData.write();
		debug("publish: payload: %o", payload);
		return stream.write(payload);
	}
	function confirmation(packet, stream, opts) {
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const type = settings.cmd || "puback";
		const id = settings.messageId;
		const dup = settings.dup && type === "pubrel" ? protocol.DUP_MASK : 0;
		let qos = 0;
		const reasonCode = settings.reasonCode;
		const properties = settings.properties;
		let length = version === 5 ? 3 : 2;
		if (type === "pubrel") qos = 1;
		if (typeof id !== "number") {
			stream.destroy(/* @__PURE__ */ new Error("Invalid messageId"));
			return false;
		}
		let propertiesData = null;
		if (version === 5) {
			if (typeof properties === "object") {
				propertiesData = getPropertiesByMaximumPacketSize(stream, properties, opts, length);
				if (!propertiesData) return false;
				length += propertiesData.length;
			}
		}
		stream.write(protocol.ACKS[type][qos][dup][0]);
		if (length === 3) length += reasonCode !== 0 ? 1 : -1;
		writeVarByteInt(stream, length);
		writeNumber(stream, id);
		if (version === 5 && length !== 2) stream.write(Buffer$2.from([reasonCode]));
		if (propertiesData !== null) propertiesData.write();
		else if (length === 4) stream.write(Buffer$2.from([0]));
		return true;
	}
	function subscribe(packet, stream, opts) {
		debug("subscribe: packet: ");
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const dup = settings.dup ? protocol.DUP_MASK : 0;
		const id = settings.messageId;
		const subs = settings.subscriptions;
		const properties = settings.properties;
		let length = 0;
		if (typeof id !== "number") {
			stream.destroy(/* @__PURE__ */ new Error("Invalid messageId"));
			return false;
		} else length += 2;
		let propertiesData = null;
		if (version === 5) {
			propertiesData = getProperties(stream, properties);
			if (!propertiesData) return false;
			length += propertiesData.length;
		}
		if (typeof subs === "object" && subs.length) for (let i = 0; i < subs.length; i += 1) {
			const itopic = subs[i].topic;
			const iqos = subs[i].qos;
			if (typeof itopic !== "string") {
				stream.destroy(/* @__PURE__ */ new Error("Invalid subscriptions - invalid topic"));
				return false;
			}
			if (typeof iqos !== "number") {
				stream.destroy(/* @__PURE__ */ new Error("Invalid subscriptions - invalid qos"));
				return false;
			}
			if (version === 5) {
				if (typeof (subs[i].nl || false) !== "boolean") {
					stream.destroy(/* @__PURE__ */ new Error("Invalid subscriptions - invalid No Local"));
					return false;
				}
				if (typeof (subs[i].rap || false) !== "boolean") {
					stream.destroy(/* @__PURE__ */ new Error("Invalid subscriptions - invalid Retain as Published"));
					return false;
				}
				const rh = subs[i].rh || 0;
				if (typeof rh !== "number" || rh > 2) {
					stream.destroy(/* @__PURE__ */ new Error("Invalid subscriptions - invalid Retain Handling"));
					return false;
				}
			}
			length += Buffer$2.byteLength(itopic) + 2 + 1;
		}
		else {
			stream.destroy(/* @__PURE__ */ new Error("Invalid subscriptions"));
			return false;
		}
		debug("subscribe: writing to stream: %o", protocol.SUBSCRIBE_HEADER);
		stream.write(protocol.SUBSCRIBE_HEADER[1][dup ? 1 : 0][0]);
		writeVarByteInt(stream, length);
		writeNumber(stream, id);
		if (propertiesData !== null) propertiesData.write();
		let result = true;
		for (const sub of subs) {
			const jtopic = sub.topic;
			const jqos = sub.qos;
			const jnl = +sub.nl;
			const jrap = +sub.rap;
			const jrh = sub.rh;
			let joptions;
			writeString(stream, jtopic);
			joptions = protocol.SUBSCRIBE_OPTIONS_QOS[jqos];
			if (version === 5) {
				joptions |= jnl ? protocol.SUBSCRIBE_OPTIONS_NL : 0;
				joptions |= jrap ? protocol.SUBSCRIBE_OPTIONS_RAP : 0;
				joptions |= jrh ? protocol.SUBSCRIBE_OPTIONS_RH[jrh] : 0;
			}
			result = stream.write(Buffer$2.from([joptions]));
		}
		return result;
	}
	function suback(packet, stream, opts) {
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const id = settings.messageId;
		const granted = settings.granted;
		const properties = settings.properties;
		let length = 0;
		if (typeof id !== "number") {
			stream.destroy(/* @__PURE__ */ new Error("Invalid messageId"));
			return false;
		} else length += 2;
		if (typeof granted === "object" && granted.length) for (let i = 0; i < granted.length; i += 1) {
			if (typeof granted[i] !== "number") {
				stream.destroy(/* @__PURE__ */ new Error("Invalid qos vector"));
				return false;
			}
			length += 1;
		}
		else {
			stream.destroy(/* @__PURE__ */ new Error("Invalid qos vector"));
			return false;
		}
		let propertiesData = null;
		if (version === 5) {
			propertiesData = getPropertiesByMaximumPacketSize(stream, properties, opts, length);
			if (!propertiesData) return false;
			length += propertiesData.length;
		}
		stream.write(protocol.SUBACK_HEADER);
		writeVarByteInt(stream, length);
		writeNumber(stream, id);
		if (propertiesData !== null) propertiesData.write();
		return stream.write(Buffer$2.from(granted));
	}
	function unsubscribe(packet, stream, opts) {
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const id = settings.messageId;
		const dup = settings.dup ? protocol.DUP_MASK : 0;
		const unsubs = settings.unsubscriptions;
		const properties = settings.properties;
		let length = 0;
		if (typeof id !== "number") {
			stream.destroy(/* @__PURE__ */ new Error("Invalid messageId"));
			return false;
		} else length += 2;
		if (typeof unsubs === "object" && unsubs.length) for (let i = 0; i < unsubs.length; i += 1) {
			if (typeof unsubs[i] !== "string") {
				stream.destroy(/* @__PURE__ */ new Error("Invalid unsubscriptions"));
				return false;
			}
			length += Buffer$2.byteLength(unsubs[i]) + 2;
		}
		else {
			stream.destroy(/* @__PURE__ */ new Error("Invalid unsubscriptions"));
			return false;
		}
		let propertiesData = null;
		if (version === 5) {
			propertiesData = getProperties(stream, properties);
			if (!propertiesData) return false;
			length += propertiesData.length;
		}
		stream.write(protocol.UNSUBSCRIBE_HEADER[1][dup ? 1 : 0][0]);
		writeVarByteInt(stream, length);
		writeNumber(stream, id);
		if (propertiesData !== null) propertiesData.write();
		let result = true;
		for (let j = 0; j < unsubs.length; j++) result = writeString(stream, unsubs[j]);
		return result;
	}
	function unsuback(packet, stream, opts) {
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const id = settings.messageId;
		const dup = settings.dup ? protocol.DUP_MASK : 0;
		const granted = settings.granted;
		const properties = settings.properties;
		const type = settings.cmd;
		const qos = 0;
		let length = 2;
		if (typeof id !== "number") {
			stream.destroy(/* @__PURE__ */ new Error("Invalid messageId"));
			return false;
		}
		if (version === 5) {
			if (typeof granted === "object" && granted.length) for (let i = 0; i < granted.length; i += 1) {
				if (typeof granted[i] !== "number") {
					stream.destroy(/* @__PURE__ */ new Error("Invalid qos vector"));
					return false;
				}
				length += 1;
			}
			else {
				stream.destroy(/* @__PURE__ */ new Error("Invalid qos vector"));
				return false;
			}
		}
		let propertiesData = null;
		if (version === 5) {
			propertiesData = getPropertiesByMaximumPacketSize(stream, properties, opts, length);
			if (!propertiesData) return false;
			length += propertiesData.length;
		}
		stream.write(protocol.ACKS[type][qos][dup][0]);
		writeVarByteInt(stream, length);
		writeNumber(stream, id);
		if (propertiesData !== null) propertiesData.write();
		if (version === 5) stream.write(Buffer$2.from(granted));
		return true;
	}
	function emptyPacket(packet, stream, opts) {
		return stream.write(protocol.EMPTY[packet.cmd]);
	}
	function disconnect(packet, stream, opts) {
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const reasonCode = settings.reasonCode;
		const properties = settings.properties;
		let length = version === 5 ? 1 : 0;
		let propertiesData = null;
		if (version === 5) {
			propertiesData = getPropertiesByMaximumPacketSize(stream, properties, opts, length);
			if (!propertiesData) return false;
			length += propertiesData.length;
		}
		stream.write(Buffer$2.from([protocol.codes.disconnect << 4]));
		writeVarByteInt(stream, length);
		if (version === 5) stream.write(Buffer$2.from([reasonCode]));
		if (propertiesData !== null) propertiesData.write();
		return true;
	}
	function auth(packet, stream, opts) {
		const version = opts ? opts.protocolVersion : 4;
		const settings = packet || {};
		const reasonCode = settings.reasonCode;
		const properties = settings.properties;
		let length = version === 5 ? 1 : 0;
		if (version !== 5) stream.destroy(/* @__PURE__ */ new Error("Invalid mqtt version for auth packet"));
		const propertiesData = getPropertiesByMaximumPacketSize(stream, properties, opts, length);
		if (!propertiesData) return false;
		length += propertiesData.length;
		stream.write(Buffer$2.from([protocol.codes.auth << 4]));
		writeVarByteInt(stream, length);
		stream.write(Buffer$2.from([reasonCode]));
		if (propertiesData !== null) propertiesData.write();
		return true;
	}
	/**
	* writeVarByteInt - write an MQTT style variable byte integer to the buffer
	*
	* @param <Buffer> buffer - destination
	* @param <Number> pos - offset
	* @param <Number> length - length (>0)
	* @returns <Number> number of bytes written
	*
	* @api private
	*/
	var varByteIntCache = {};
	function writeVarByteInt(stream, num) {
		if (num > protocol.VARBYTEINT_MAX) {
			stream.destroy(/* @__PURE__ */ new Error(`Invalid variable byte integer: ${num}`));
			return false;
		}
		let buffer = varByteIntCache[num];
		if (!buffer) {
			buffer = genBufVariableByteInt(num);
			if (num < 16384) varByteIntCache[num] = buffer;
		}
		debug("writeVarByteInt: writing to stream: %o", buffer);
		return stream.write(buffer);
	}
	/**
	* writeString - write a utf8 string to the buffer
	*
	* @param <Buffer> buffer - destination
	* @param <Number> pos - offset
	* @param <String> string - string to write
	* @return <Number> number of bytes written
	*
	* @api private
	*/
	function writeString(stream, string) {
		const strlen = Buffer$2.byteLength(string);
		writeNumber(stream, strlen);
		debug("writeString: %s", string);
		return stream.write(string, "utf8");
	}
	/**
	* writeStringPair - write a utf8 string pairs to the buffer
	*
	* @param <Buffer> buffer - destination
	* @param <String> name - string name to write
	* @param <String> value - string value to write
	* @return <Number> number of bytes written
	*
	* @api private
	*/
	function writeStringPair(stream, name, value) {
		writeString(stream, name);
		writeString(stream, value);
	}
	/**
	* writeNumber - write a two byte number to the buffer
	*
	* @param <Buffer> buffer - destination
	* @param <Number> pos - offset
	* @param <String> number - number to write
	* @return <Number> number of bytes written
	*
	* @api private
	*/
	function writeNumberCached(stream, number) {
		debug("writeNumberCached: number: %d", number);
		debug("writeNumberCached: %o", numCache[number]);
		return stream.write(numCache[number]);
	}
	function writeNumberGenerated(stream, number) {
		const generatedNumber = generateNumber(number);
		debug("writeNumberGenerated: %o", generatedNumber);
		return stream.write(generatedNumber);
	}
	function write4ByteNumber(stream, number) {
		const generated4ByteBuffer = generate4ByteBuffer(number);
		debug("write4ByteNumber: %o", generated4ByteBuffer);
		return stream.write(generated4ByteBuffer);
	}
	/**
	* writeStringOrBuffer - write a String or Buffer with the its length prefix
	*
	* @param <Buffer> buffer - destination
	* @param <Number> pos - offset
	* @param <String> toWrite - String or Buffer
	* @return <Number> number of bytes written
	*/
	function writeStringOrBuffer(stream, toWrite) {
		if (typeof toWrite === "string") writeString(stream, toWrite);
		else if (toWrite) {
			writeNumber(stream, toWrite.length);
			stream.write(toWrite);
		} else writeNumber(stream, 0);
	}
	function getProperties(stream, properties) {
		if (typeof properties !== "object" || properties.length != null) return {
			length: 1,
			write() {
				writeProperties(stream, {}, 0);
			}
		};
		let propertiesLength = 0;
		function getLengthProperty(name, value) {
			const type = protocol.propertiesTypes[name];
			let length = 0;
			switch (type) {
				case "byte":
					if (typeof value !== "boolean") {
						stream.destroy(/* @__PURE__ */ new Error(`Invalid ${name}: ${value}`));
						return false;
					}
					length += 2;
					break;
				case "int8":
					if (typeof value !== "number" || value < 0 || value > 255) {
						stream.destroy(/* @__PURE__ */ new Error(`Invalid ${name}: ${value}`));
						return false;
					}
					length += 2;
					break;
				case "binary":
					if (value && value === null) {
						stream.destroy(/* @__PURE__ */ new Error(`Invalid ${name}: ${value}`));
						return false;
					}
					length += 1 + Buffer$2.byteLength(value) + 2;
					break;
				case "int16":
					if (typeof value !== "number" || value < 0 || value > 65535) {
						stream.destroy(/* @__PURE__ */ new Error(`Invalid ${name}: ${value}`));
						return false;
					}
					length += 3;
					break;
				case "int32":
					if (typeof value !== "number" || value < 0 || value > 4294967295) {
						stream.destroy(/* @__PURE__ */ new Error(`Invalid ${name}: ${value}`));
						return false;
					}
					length += 5;
					break;
				case "var":
					if (typeof value !== "number" || value < 0 || value > 268435455) {
						stream.destroy(/* @__PURE__ */ new Error(`Invalid ${name}: ${value}`));
						return false;
					}
					length += 1 + Buffer$2.byteLength(genBufVariableByteInt(value));
					break;
				case "string":
					if (typeof value !== "string") {
						stream.destroy(/* @__PURE__ */ new Error(`Invalid ${name}: ${value}`));
						return false;
					}
					length += 3 + Buffer$2.byteLength(value.toString());
					break;
				case "pair":
					if (typeof value !== "object") {
						stream.destroy(/* @__PURE__ */ new Error(`Invalid ${name}: ${value}`));
						return false;
					}
					length += Object.getOwnPropertyNames(value).reduce((result, name) => {
						const currentValue = value[name];
						if (Array.isArray(currentValue)) result += currentValue.reduce((currentLength, value) => {
							currentLength += 3 + Buffer$2.byteLength(name.toString()) + 2 + Buffer$2.byteLength(value.toString());
							return currentLength;
						}, 0);
						else result += 3 + Buffer$2.byteLength(name.toString()) + 2 + Buffer$2.byteLength(value[name].toString());
						return result;
					}, 0);
					break;
				default:
					stream.destroy(/* @__PURE__ */ new Error(`Invalid property ${name}: ${value}`));
					return false;
			}
			return length;
		}
		if (properties) for (const propName in properties) {
			let propLength = 0;
			let propValueLength = 0;
			const propValue = properties[propName];
			if (propValue === void 0) continue;
			else if (Array.isArray(propValue)) for (let valueIndex = 0; valueIndex < propValue.length; valueIndex++) {
				propValueLength = getLengthProperty(propName, propValue[valueIndex]);
				if (!propValueLength) return false;
				propLength += propValueLength;
			}
			else {
				propValueLength = getLengthProperty(propName, propValue);
				if (!propValueLength) return false;
				propLength = propValueLength;
			}
			if (!propLength) return false;
			propertiesLength += propLength;
		}
		return {
			length: Buffer$2.byteLength(genBufVariableByteInt(propertiesLength)) + propertiesLength,
			write() {
				writeProperties(stream, properties, propertiesLength);
			}
		};
	}
	function getPropertiesByMaximumPacketSize(stream, properties, opts, length) {
		const mayEmptyProps = ["reasonString", "userProperties"];
		const maximumPacketSize = opts && opts.properties && opts.properties.maximumPacketSize ? opts.properties.maximumPacketSize : 0;
		let propertiesData = getProperties(stream, properties);
		if (maximumPacketSize) while (length + propertiesData.length > maximumPacketSize) {
			const currentMayEmptyProp = mayEmptyProps.shift();
			if (currentMayEmptyProp && properties[currentMayEmptyProp]) {
				delete properties[currentMayEmptyProp];
				propertiesData = getProperties(stream, properties);
			} else return false;
		}
		return propertiesData;
	}
	function writeProperty(stream, propName, value) {
		switch (protocol.propertiesTypes[propName]) {
			case "byte":
				stream.write(Buffer$2.from([protocol.properties[propName]]));
				stream.write(Buffer$2.from([+value]));
				break;
			case "int8":
				stream.write(Buffer$2.from([protocol.properties[propName]]));
				stream.write(Buffer$2.from([value]));
				break;
			case "binary":
				stream.write(Buffer$2.from([protocol.properties[propName]]));
				writeStringOrBuffer(stream, value);
				break;
			case "int16":
				stream.write(Buffer$2.from([protocol.properties[propName]]));
				writeNumber(stream, value);
				break;
			case "int32":
				stream.write(Buffer$2.from([protocol.properties[propName]]));
				write4ByteNumber(stream, value);
				break;
			case "var":
				stream.write(Buffer$2.from([protocol.properties[propName]]));
				writeVarByteInt(stream, value);
				break;
			case "string":
				stream.write(Buffer$2.from([protocol.properties[propName]]));
				writeString(stream, value);
				break;
			case "pair":
				Object.getOwnPropertyNames(value).forEach((name) => {
					const currentValue = value[name];
					if (Array.isArray(currentValue)) currentValue.forEach((value) => {
						stream.write(Buffer$2.from([protocol.properties[propName]]));
						writeStringPair(stream, name.toString(), value.toString());
					});
					else {
						stream.write(Buffer$2.from([protocol.properties[propName]]));
						writeStringPair(stream, name.toString(), currentValue.toString());
					}
				});
				break;
			default:
				stream.destroy(/* @__PURE__ */ new Error(`Invalid property ${propName} value: ${value}`));
				return false;
		}
	}
	function writeProperties(stream, properties, propertiesLength) {
		writeVarByteInt(stream, propertiesLength);
		for (const propName in properties) if (Object.prototype.hasOwnProperty.call(properties, propName) && properties[propName] != null) {
			const value = properties[propName];
			if (Array.isArray(value)) for (let valueIndex = 0; valueIndex < value.length; valueIndex++) writeProperty(stream, propName, value[valueIndex]);
			else writeProperty(stream, propName, value);
		}
	}
	function byteLength(bufOrString) {
		if (!bufOrString) return 0;
		else if (bufOrString instanceof Buffer$2) return bufOrString.length;
		else return Buffer$2.byteLength(bufOrString);
	}
	function isStringOrBuffer(field) {
		return typeof field === "string" || field instanceof Buffer$2;
	}
	module.exports = generate;
}));
//#endregion
//#region node_modules/mqtt-packet/generate.js
var require_generate = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var writeToStream = require_writeToStream();
	var { EventEmitter: EventEmitter$2 } = __require("events");
	var { Buffer: Buffer$1 } = __require("buffer");
	function generate(packet, opts) {
		const stream = new Accumulator();
		writeToStream(packet, stream, opts);
		return stream.concat();
	}
	var Accumulator = class extends EventEmitter$2 {
		constructor() {
			super();
			this._array = new Array(20);
			this._i = 0;
		}
		write(chunk) {
			this._array[this._i++] = chunk;
			return true;
		}
		concat() {
			let length = 0;
			const lengths = new Array(this._array.length);
			const list = this._array;
			let pos = 0;
			let i = 0;
			for (; i < list.length && list[i] !== void 0; i++) {
				if (typeof list[i] !== "string") lengths[i] = list[i].length;
				else lengths[i] = Buffer$1.byteLength(list[i]);
				length += lengths[i];
			}
			const result = Buffer$1.allocUnsafe(length);
			for (i = 0; i < list.length && list[i] !== void 0; i++) if (typeof list[i] !== "string") {
				list[i].copy(result, pos);
				pos += lengths[i];
			} else {
				result.write(list[i], pos);
				pos += lengths[i];
			}
			return result;
		}
		destroy(err) {
			if (err) this.emit("error", err);
		}
	};
	module.exports = generate;
}));
//#endregion
//#region node_modules/mqtt-packet/mqtt.js
var require_mqtt$1 = /* @__PURE__ */ __commonJSMin(((exports) => {
	exports.parser = require_parser().parser;
	exports.generate = require_generate();
	exports.writeToStream = require_writeToStream();
}));
//#endregion
//#region node_modules/rfdc/index.js
var require_rfdc = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = rfdc;
	function copyBuffer(cur) {
		if (cur instanceof Buffer) return Buffer.from(cur);
		return new cur.constructor(cur.buffer.slice(), cur.byteOffset, cur.length);
	}
	function rfdc(opts) {
		opts = opts || {};
		if (opts.circles) return rfdcCircles(opts);
		const constructorHandlers = /* @__PURE__ */ new Map();
		constructorHandlers.set(Date, (o) => new Date(o));
		constructorHandlers.set(Map, (o, fn) => new Map(cloneArray(Array.from(o), fn)));
		constructorHandlers.set(Set, (o, fn) => new Set(cloneArray(Array.from(o), fn)));
		if (opts.constructorHandlers) for (const handler of opts.constructorHandlers) constructorHandlers.set(handler[0], handler[1]);
		let handler = null;
		return opts.proto ? cloneProto : clone;
		function cloneArray(a, fn) {
			const keys = Object.keys(a);
			const a2 = new Array(keys.length);
			for (let i = 0; i < keys.length; i++) {
				const k = keys[i];
				const cur = a[k];
				if (typeof cur !== "object" || cur === null) a2[k] = cur;
				else if (cur.constructor !== Object && (handler = constructorHandlers.get(cur.constructor))) a2[k] = handler(cur, fn);
				else if (ArrayBuffer.isView(cur)) a2[k] = copyBuffer(cur);
				else a2[k] = fn(cur);
			}
			return a2;
		}
		function clone(o) {
			if (typeof o !== "object" || o === null) return o;
			if (Array.isArray(o)) return cloneArray(o, clone);
			if (o.constructor !== Object && (handler = constructorHandlers.get(o.constructor))) return handler(o, clone);
			const o2 = {};
			for (const k in o) {
				if (Object.hasOwnProperty.call(o, k) === false) continue;
				const cur = o[k];
				if (typeof cur !== "object" || cur === null) o2[k] = cur;
				else if (cur.constructor !== Object && (handler = constructorHandlers.get(cur.constructor))) o2[k] = handler(cur, clone);
				else if (ArrayBuffer.isView(cur)) o2[k] = copyBuffer(cur);
				else o2[k] = clone(cur);
			}
			return o2;
		}
		function cloneProto(o) {
			if (typeof o !== "object" || o === null) return o;
			if (Array.isArray(o)) return cloneArray(o, cloneProto);
			if (o.constructor !== Object && (handler = constructorHandlers.get(o.constructor))) return handler(o, cloneProto);
			const o2 = {};
			for (const k in o) {
				const cur = o[k];
				if (typeof cur !== "object" || cur === null) o2[k] = cur;
				else if (cur.constructor !== Object && (handler = constructorHandlers.get(cur.constructor))) o2[k] = handler(cur, cloneProto);
				else if (ArrayBuffer.isView(cur)) o2[k] = copyBuffer(cur);
				else o2[k] = cloneProto(cur);
			}
			return o2;
		}
	}
	function rfdcCircles(opts) {
		const refs = [];
		const refsNew = [];
		const constructorHandlers = /* @__PURE__ */ new Map();
		constructorHandlers.set(Date, (o) => new Date(o));
		constructorHandlers.set(Map, (o, fn) => new Map(cloneArray(Array.from(o), fn)));
		constructorHandlers.set(Set, (o, fn) => new Set(cloneArray(Array.from(o), fn)));
		if (opts.constructorHandlers) for (const handler of opts.constructorHandlers) constructorHandlers.set(handler[0], handler[1]);
		let handler = null;
		return opts.proto ? cloneProto : clone;
		function cloneArray(a, fn) {
			const keys = Object.keys(a);
			const a2 = new Array(keys.length);
			for (let i = 0; i < keys.length; i++) {
				const k = keys[i];
				const cur = a[k];
				if (typeof cur !== "object" || cur === null) a2[k] = cur;
				else if (cur.constructor !== Object && (handler = constructorHandlers.get(cur.constructor))) a2[k] = handler(cur, fn);
				else if (ArrayBuffer.isView(cur)) a2[k] = copyBuffer(cur);
				else {
					const index = refs.indexOf(cur);
					if (index !== -1) a2[k] = refsNew[index];
					else a2[k] = fn(cur);
				}
			}
			return a2;
		}
		function clone(o) {
			if (typeof o !== "object" || o === null) return o;
			if (Array.isArray(o)) return cloneArray(o, clone);
			if (o.constructor !== Object && (handler = constructorHandlers.get(o.constructor))) return handler(o, clone);
			const o2 = {};
			refs.push(o);
			refsNew.push(o2);
			for (const k in o) {
				if (Object.hasOwnProperty.call(o, k) === false) continue;
				const cur = o[k];
				if (typeof cur !== "object" || cur === null) o2[k] = cur;
				else if (cur.constructor !== Object && (handler = constructorHandlers.get(cur.constructor))) o2[k] = handler(cur, clone);
				else if (ArrayBuffer.isView(cur)) o2[k] = copyBuffer(cur);
				else {
					const i = refs.indexOf(cur);
					if (i !== -1) o2[k] = refsNew[i];
					else o2[k] = clone(cur);
				}
			}
			refs.pop();
			refsNew.pop();
			return o2;
		}
		function cloneProto(o) {
			if (typeof o !== "object" || o === null) return o;
			if (Array.isArray(o)) return cloneArray(o, cloneProto);
			if (o.constructor !== Object && (handler = constructorHandlers.get(o.constructor))) return handler(o, cloneProto);
			const o2 = {};
			refs.push(o);
			refsNew.push(o2);
			for (const k in o) {
				const cur = o[k];
				if (typeof cur !== "object" || cur === null) o2[k] = cur;
				else if (cur.constructor !== Object && (handler = constructorHandlers.get(cur.constructor))) o2[k] = handler(cur, cloneProto);
				else if (ArrayBuffer.isView(cur)) o2[k] = copyBuffer(cur);
				else {
					const i = refs.indexOf(cur);
					if (i !== -1) o2[k] = refsNew[i];
					else o2[k] = cloneProto(cur);
				}
			}
			refs.pop();
			refsNew.pop();
			return o2;
		}
	}
}));
//#endregion
//#region node_modules/rfdc/default.js
var require_default = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = require_rfdc()();
}));
//#endregion
//#region node_modules/mqtt/build/lib/validations.js
var require_validations = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.validateTopic = validateTopic;
	exports.validateTopics = validateTopics;
	function validateTopic(topic) {
		const parts = topic.split("/");
		for (let i = 0; i < parts.length; i++) {
			if (parts[i] === "+") continue;
			if (parts[i] === "#") return i === parts.length - 1;
			if (parts[i].indexOf("+") !== -1 || parts[i].indexOf("#") !== -1) return false;
		}
		return true;
	}
	function validateTopics(topics) {
		if (topics.length === 0) return "empty_topic_list";
		for (let i = 0; i < topics.length; i++) if (!validateTopic(topics[i])) return topics[i];
		return null;
	}
}));
//#endregion
//#region node_modules/mqtt/build/lib/store.js
var require_store = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var readable_stream_1 = require_ours();
	var streamsOpts = { objectMode: true };
	var defaultStoreOptions = { clean: true };
	var Store = class {
		options;
		_inflights;
		constructor(options) {
			this.options = options || {};
			this.options = {
				...defaultStoreOptions,
				...options
			};
			this._inflights = /* @__PURE__ */ new Map();
		}
		put(packet, cb) {
			this._inflights.set(packet.messageId, packet);
			if (cb) cb();
			return this;
		}
		createStream() {
			const stream = new readable_stream_1.Readable(streamsOpts);
			const values = [];
			let destroyed = false;
			let i = 0;
			this._inflights.forEach((value, key) => {
				values.push(value);
			});
			stream._read = () => {
				if (!destroyed && i < values.length) stream.push(values[i++]);
				else stream.push(null);
			};
			stream.destroy = (err) => {
				if (destroyed) return;
				destroyed = true;
				setTimeout(() => {
					stream.emit("close");
				}, 0);
				return stream;
			};
			return stream;
		}
		del(packet, cb) {
			const toDelete = this._inflights.get(packet.messageId);
			if (toDelete) {
				this._inflights.delete(packet.messageId);
				cb(null, toDelete);
			} else if (cb) cb(/* @__PURE__ */ new Error("missing packet"));
			return this;
		}
		get(packet, cb) {
			const storedPacket = this._inflights.get(packet.messageId);
			if (storedPacket) cb(null, storedPacket);
			else if (cb) cb(/* @__PURE__ */ new Error("missing packet"));
			return this;
		}
		close(cb) {
			if (this.options.clean) this._inflights = null;
			if (cb) cb();
		}
	};
	exports.default = Store;
}));
//#endregion
//#region node_modules/mqtt/package.json
var package_exports = /* @__PURE__ */ __exportAll({
	bin: () => bin,
	browser: () => browser,
	contributors: () => contributors,
	default: () => package_default,
	dependencies: () => dependencies,
	description: () => description,
	devDependencies: () => devDependencies,
	engines: () => engines,
	exports: () => exports$1,
	files: () => files,
	keywords: () => keywords,
	license: () => "MIT",
	main: () => main,
	module: () => module$1,
	name: () => name,
	overrides: () => overrides,
	publishConfig: () => publishConfig,
	repository: () => repository,
	scripts: () => scripts,
	types: () => types,
	typesVersions: () => typesVersions,
	version: () => version
}), name, description, version, contributors, keywords, repository, main, module$1, bin, files, exports$1, types, typesVersions, scripts, publishConfig, engines, browser, dependencies, overrides, devDependencies, package_default;
var init_package = __esmMin((() => {
	name = "mqtt";
	description = "A library for the MQTT protocol";
	version = "5.16.0";
	contributors = [
		"Adam Rudd <adamvrr@gmail.com>",
		"Matteo Collina <matteo.collina@gmail.com> (https://github.com/mcollina)",
		"Siarhei Buntsevich <scarry0506@gmail.com> (https://github.com/scarry1992)",
		"Yoseph Maguire <yomaguir@microsoft.com> (https://github.com/YoDaMa)",
		"Daniel Lando <daniel.sorridi@gmail.com> (https://github.com/robertsLando)"
	];
	keywords = [
		"mqtt",
		"publish/subscribe",
		"publish",
		"subscribe"
	];
	repository = {
		"type": "git",
		"url": "git://github.com/mqttjs/MQTT.js.git"
	};
	main = "./build/index.js";
	module$1 = "./dist/mqtt.esm.js";
	bin = {
		"mqtt_pub": "./build/bin/pub.js",
		"mqtt_sub": "./build/bin/sub.js",
		"mqtt": "./build/bin/mqtt.js"
	};
	files = [
		"dist/",
		"CONTRIBUTING.md",
		"LICENSE.md",
		"help/",
		"build/"
	];
	exports$1 = {
		".": {
			"react-native": "./dist/mqtt.esm.js",
			"browser": {
				"import": "./dist/mqtt.esm.js",
				"default": "./dist/mqtt.min.js"
			},
			"default": "./build/index.js"
		},
		"./package.json": "./package.json",
		"./*.map": "./build/*.js.map",
		"./dist/*": "./dist/*.js",
		"./*": "./build/*.js"
	};
	types = "build/index.d.ts";
	typesVersions = { "*": { "*": ["./build/index.d.ts"] } };
	scripts = {
		"lint": "eslint --ext .ts .",
		"lint-fix": "eslint --fix --ext .ts .",
		"build:ts": "rimraf build/ && tsc -p tsconfig.build.json",
		"build:browser": "node esbuild.js",
		"build": "npm run build:ts && npm run build:browser",
		"prepare": "npm run build",
		"unit-test:node": "node -r esbuild-register --test-concurrency 4 --test-reporter=junit --test-reporter-destination=junit.xml --test-reporter=spec --test-reporter-destination=stdout --test test/node/*.ts ",
		"unit-test:browser": "wtr",
		"test:node": "node_modules/.bin/nyc npm run unit-test:node",
		"test:browser": "npm run build && npm run unit-test:browser",
		"test": "npm run test:node",
		"changelog": "conventional-changelog -p angular -i CHANGELOG.md -s && git add CHANGELOG.md",
		"changelog-init": "conventional-changelog -p angular -i CHANGELOG.md -s -r 0",
		"release": "read -p 'GITHUB_TOKEN: ' GITHUB_TOKEN && export GITHUB_TOKEN=$GITHUB_TOKEN && release-it"
	};
	publishConfig = {
		"registry": "https://registry.npmjs.org",
		"provenance": true
	};
	engines = { "node": ">=16.0.0" };
	browser = {
		"./mqtt.js": "./dist/mqtt.js",
		"fs": false,
		"tls": false,
		"net": false
	};
	dependencies = {
		"@types/readable-stream": "^4.0.21",
		"@types/ws": "^8.18.1",
		"commist": "^3.2.0",
		"concat-stream": "^2.0.0",
		"debug": "^4.4.1",
		"help-me": "^5.0.0",
		"lru-cache": "^10.4.3",
		"minimist": "^1.2.8",
		"mqtt-packet": "^9.0.2",
		"readable-stream": "^4.7.0",
		"rfdc": "^1.4.1",
		"socks": "^2.8.6",
		"split2": "^4.2.0",
		"worker-timers": "^8.0.23",
		"ws": "^8.18.3"
	};
	overrides = {
		"levelup": {
			"bl": "1.2.3",
			"semver": "5.7.2"
		},
		"pre-commit": { "cross-spawn": "7.0.6" },
		"release-it": { "undici": "7.24.5" }
	};
	devDependencies = {
		"@eslint/eslintrc": "^3.3.1",
		"@eslint/js": "^9.32.0",
		"@esm-bundle/chai": "^4.3.4",
		"@release-it/conventional-changelog": "^12.0.2",
		"@tsconfig/node20": "^20.1.6",
		"@types/chai": "^5.2.2",
		"@types/node": "^20.17.16",
		"@types/sinon": "^17.0.4",
		"@types/tape": "^5.8.1",
		"@typescript-eslint/eslint-plugin": "^8.38.0",
		"@typescript-eslint/parser": "^8.38.0",
		"@web/test-runner": "^0.20.2",
		"@web/test-runner-playwright": "^0.11.1",
		"aedes-cli": "^0.8.0",
		"chai": "^5.2.1",
		"chokidar": "^4.0.3",
		"conventional-changelog": "^8.1.3",
		"conventional-changelog-angular": "^9.4.0",
		"end-of-stream": "^1.4.5",
		"esbuild": "^0.25.8",
		"esbuild-plugin-polyfill-node": "^0.3.0",
		"esbuild-register": "^3.6.0",
		"eslint": "^9.32.0",
		"eslint-config-prettier": "^10.1.8",
		"eslint-plugin-import": "^2.32.0",
		"eslint-plugin-prettier": "^5.5.3",
		"global": "^4.4.0",
		"globals": "^16.3.0",
		"leaked-handles": "^5.2.0",
		"mkdirp": "^3.0.1",
		"mqtt-connection": "^4.1.0",
		"mqtt-level-store": "^3.1.0",
		"nyc": "^17.1.0",
		"pre-commit": "^1.2.2",
		"prettier": "^3.6.2",
		"release-it": "^21.0.3",
		"rimraf": "^6.0.1",
		"should": "^13.2.3",
		"sinon": "^17.0.2",
		"snazzy": "^9.0.0",
		"tape": "^5.9.0",
		"ts-node": "^10.9.2",
		"typescript": "^5.8.3"
	};
	package_default = {
		name,
		description,
		version,
		contributors,
		keywords,
		license: "MIT",
		repository,
		main,
		module: module$1,
		bin,
		files,
		exports: exports$1,
		types,
		typesVersions,
		scripts,
		publishConfig,
		"pre-commit": ["lint"],
		engines,
		browser,
		dependencies,
		overrides,
		devDependencies
	};
}));
//#endregion
//#region node_modules/mqtt/build/lib/shared.js
var require_shared = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.MQTTJS_VERSION = exports.nextTick = exports.ErrorWithSubackPacket = exports.ErrorWithReasonCode = void 0;
	exports.applyMixin = applyMixin;
	exports.ErrorWithReasonCode = class ErrorWithReasonCode extends Error {
		code;
		constructor(message, code) {
			super(message);
			this.code = code;
			Object.setPrototypeOf(this, ErrorWithReasonCode.prototype);
			Object.getPrototypeOf(this).name = "ErrorWithReasonCode";
		}
	};
	exports.ErrorWithSubackPacket = class ErrorWithSubackPacket extends Error {
		packet;
		constructor(message, packet) {
			super(message);
			this.packet = packet;
			Object.setPrototypeOf(this, ErrorWithSubackPacket.prototype);
			Object.getPrototypeOf(this).name = "ErrorWithSubackPacket";
		}
	};
	function applyMixin(target, mixin, includeConstructor = false) {
		const inheritanceChain = [mixin];
		while (true) {
			const current = inheritanceChain[0];
			const base = Object.getPrototypeOf(current);
			if (base?.prototype) inheritanceChain.unshift(base);
			else break;
		}
		for (const ctor of inheritanceChain) for (const prop of Object.getOwnPropertyNames(ctor.prototype)) if (includeConstructor || prop !== "constructor") Object.defineProperty(target.prototype, prop, Object.getOwnPropertyDescriptor(ctor.prototype, prop) ?? Object.create(null));
	}
	exports.nextTick = typeof process?.nextTick === "function" ? process.nextTick : (callback) => {
		setTimeout(callback, 0);
	};
	exports.MQTTJS_VERSION = (init_package(), __toCommonJS(package_exports).default).version;
}));
//#endregion
//#region node_modules/mqtt/build/lib/handlers/publish.js
var require_publish = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var shared_1 = require_shared();
	var validReasonCodes = [
		0,
		16,
		128,
		131,
		135,
		144,
		145,
		151,
		153
	];
	var TOPIC_ALIAS_INVALID = 148;
	var PROTOCOL_ERROR = 130;
	var rejectTopicAlias = (client, message, done, pump, reasonCode = TOPIC_ALIAS_INVALID) => {
		pump?.discardParsedPackets();
		if (!pump || pump.isCurrent()) client["_cleanUp"](true);
		done();
		client.emit("error", new shared_1.ErrorWithReasonCode(message, reasonCode));
	};
	var failCustomAck = (client, error, done) => {
		done();
		client.emit("error", error);
	};
	var handlePublish = (client, packet, done, pump) => {
		client.log("handlePublish: packet %o", packet);
		done = typeof done !== "undefined" ? done : client.noop;
		let topic = packet.topic.toString();
		const message = packet.payload;
		const { qos } = packet;
		const { messageId } = packet;
		const { options } = client;
		if (client.options.protocolVersion === 5) {
			let alias;
			if (packet.properties) alias = packet.properties.topicAlias;
			if (typeof alias !== "undefined") {
				const topicAliasRecv = client["topicAliasRecv"];
				if (!topicAliasRecv) {
					client.log("handlePublish :: received unexpected topic alias. alias: %d", alias);
					rejectTopicAlias(client, "Received a PUBLISH Topic Alias but no Topic Alias Maximum was advertised", done, pump);
					return;
				}
				if (topic.length === 0) {
					if (alias > 0 && alias <= 65535) {
						const gotTopic = topicAliasRecv.getTopicByAlias(alias);
						if (gotTopic) {
							topic = gotTopic;
							client.log("handlePublish :: topic complemented by alias. topic: %s - alias: %d", topic, alias);
						} else {
							client.log("handlePublish :: unregistered topic alias. alias: %d", alias);
							rejectTopicAlias(client, "Received unregistered Topic Alias", done, pump, PROTOCOL_ERROR);
							return;
						}
					} else {
						client.log("handlePublish :: topic alias out of range. alias: %d", alias);
						rejectTopicAlias(client, `Received Topic Alias ${alias} is outside the valid range 1-65535`, done, pump);
						return;
					}
				} else if (topicAliasRecv.put(topic, alias)) client.log("handlePublish :: registered topic: %s - alias: %d", topic, alias);
				else {
					client.log("handlePublish :: topic alias outside the advertised maximum. alias: %d - max: %d", alias, topicAliasRecv.max);
					rejectTopicAlias(client, `Received Topic Alias ${alias} is outside the advertised Topic Alias Maximum range 1-${topicAliasRecv.max}`, done, pump);
					return;
				}
			}
		}
		client.log("handlePublish: qos %d", qos);
		switch (qos) {
			case 2:
				options.customHandleAcks(topic, message, packet, (error, code) => {
					if (typeof error === "number") {
						code = error;
						error = null;
					}
					if (error) return failCustomAck(client, error, done);
					if (validReasonCodes.indexOf(code) === -1) return failCustomAck(client, /* @__PURE__ */ new Error("Wrong reason code for pubrec"), done);
					if (code) client["_sendPacket"]({
						cmd: "pubrec",
						messageId,
						reasonCode: code
					}, done);
					else {
						if (!client["_trackIncomingQoS2Publish"](messageId, done, pump)) return;
						client.incomingStore.put(packet, () => {
							client["_sendPacket"]({
								cmd: "pubrec",
								messageId
							}, done);
						});
					}
				});
				break;
			case 1:
				options.customHandleAcks(topic, message, packet, (error, code) => {
					if (typeof error === "number") {
						code = error;
						error = null;
					}
					if (error) return failCustomAck(client, error, done);
					if (validReasonCodes.indexOf(code) === -1) return failCustomAck(client, /* @__PURE__ */ new Error("Wrong reason code for puback"), done);
					if (!code) client.emit("message", topic, message, packet);
					client.handleMessage(packet, (err) => {
						if (err) return done && done(err);
						client["_sendPacket"]({
							cmd: "puback",
							messageId,
							reasonCode: code
						}, done);
					});
				});
				break;
			case 0:
				client.emit("message", topic, message, packet);
				client.handleMessage(packet, done);
				break;
			default: client.log("handlePublish: unknown QoS. Doing nothing.");
		}
	};
	exports.default = handlePublish;
}));
//#endregion
//#region node_modules/mqtt/build/lib/handlers/auth.js
var require_auth = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var shared_1 = require_shared();
	var PROTOCOL_ERROR = 130;
	var rejectAuth = (client, error, pump) => {
		client.log("handleAuth :: %s", error.message);
		pump?.discardParsedPackets();
		if (!pump || pump.isCurrent()) client["_cleanUp"](true);
		client.emit("error", error);
	};
	var handleAuth = (client, packet, pump) => {
		const { options } = client;
		const version = options.protocolVersion;
		const rc = version === 5 ? packet.reasonCode : packet.returnCode;
		if (version !== 5) {
			rejectAuth(client, new shared_1.ErrorWithReasonCode(`Protocol error: Auth packets are only supported in MQTT 5. Your version:${version}`, PROTOCOL_ERROR), pump);
			return;
		}
		const authenticationMethod = client["_authenticationMethod"];
		if (!authenticationMethod) {
			rejectAuth(client, new shared_1.ErrorWithReasonCode("Protocol error: Auth packet received but enhanced authentication was not requested", PROTOCOL_ERROR), pump);
			return;
		}
		if (packet.properties?.authenticationMethod !== authenticationMethod) {
			rejectAuth(client, new shared_1.ErrorWithReasonCode(`Protocol error: Auth packet Authentication Method does not match the "${authenticationMethod}" sent in CONNECT`, PROTOCOL_ERROR), pump);
			return;
		}
		client.handleAuth(packet, (err, packet2) => {
			if (err) {
				rejectAuth(client, err, pump);
				return;
			}
			if (rc === 0) {
				client.log("handleAuth :: authentication exchange completed successfully");
				return;
			}
			if (rc === 24) {
				if (!packet2) {
					rejectAuth(client, new shared_1.ErrorWithReasonCode("Protocol error: No auth packet to continue the authentication exchange", PROTOCOL_ERROR), pump);
					return;
				}
				client.reconnecting = false;
				client["_sendPacket"](packet2);
				return;
			}
			rejectAuth(client, new shared_1.ErrorWithReasonCode(`Protocol error: Auth packet reason code ${rc} is not one a broker may send`, PROTOCOL_ERROR), pump);
		});
	};
	exports.default = handleAuth;
}));
//#endregion
//#region node_modules/mqtt/build/lib/handlers/ack.js
var require_ack = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.ReasonCodes = void 0;
	var shared_1 = require_shared();
	exports.ReasonCodes = {
		0: "",
		1: "Unacceptable protocol version",
		2: "Identifier rejected",
		3: "Server unavailable",
		4: "Bad username or password",
		5: "Not authorized",
		16: "No matching subscribers",
		17: "No subscription existed",
		128: "Unspecified error",
		129: "Malformed Packet",
		130: "Protocol Error",
		131: "Implementation specific error",
		132: "Unsupported Protocol Version",
		133: "Client Identifier not valid",
		134: "Bad User Name or Password",
		135: "Not authorized",
		136: "Server unavailable",
		137: "Server busy",
		138: "Banned",
		139: "Server shutting down",
		140: "Bad authentication method",
		141: "Keep Alive timeout",
		142: "Session taken over",
		143: "Topic Filter invalid",
		144: "Topic Name invalid",
		145: "Packet identifier in use",
		146: "Packet Identifier not found",
		147: "Receive Maximum exceeded",
		148: "Topic Alias invalid",
		149: "Packet too large",
		150: "Message rate too high",
		151: "Quota exceeded",
		152: "Administrative action",
		153: "Payload format invalid",
		154: "Retain not supported",
		155: "QoS not supported",
		156: "Use another server",
		157: "Server moved",
		158: "Shared Subscriptions not supported",
		159: "Connection rate exceeded",
		160: "Maximum connect time",
		161: "Subscription Identifiers not supported",
		162: "Wildcard Subscriptions not supported"
	};
	var acksForPendingCmd = {
		publish: [
			"puback",
			"pubrec",
			"pubcomp"
		],
		pubrel: ["pubcomp"],
		subscribe: ["suback"],
		unsubscribe: ["unsuback"]
	};
	var handleAck = (client, packet) => {
		const { messageId } = packet;
		const type = packet.cmd;
		let response = null;
		const pending = client.outgoing[messageId];
		const cb = pending ? pending.cb : null;
		let err = null;
		if (!cb) {
			client.log("_handleAck :: Server sent an ack in error. Ignoring.");
			return;
		}
		if (!acksForPendingCmd[pending.cmd]?.includes(type)) {
			client.log("_handleAck :: %s does not answer the pending %s on message id %d. Ignoring.", type, pending.cmd, messageId);
			client.emit("error", /* @__PURE__ */ new Error(`Protocol error: ${type} does not answer the ${pending.cmd} pending on message id ${messageId}`));
			return;
		}
		client.log("_handleAck :: packet type", type);
		switch (type) {
			case "pubcomp":
			case "puback": {
				const pubackRC = packet.reasonCode;
				if (pubackRC && pubackRC > 0 && pubackRC !== 16) {
					err = new shared_1.ErrorWithReasonCode(`Publish error: ${exports.ReasonCodes[pubackRC]}`, pubackRC);
					client["_removeOutgoingAndStoreMessage"](messageId, () => {
						cb(err, packet);
					});
				} else client["_removeOutgoingAndStoreMessage"](messageId, cb);
				break;
			}
			case "pubrec": {
				response = {
					cmd: "pubrel",
					qos: 2,
					messageId
				};
				const pubrecRC = packet.reasonCode;
				if (pubrecRC && pubrecRC > 0 && pubrecRC !== 16) {
					err = new shared_1.ErrorWithReasonCode(`Publish error: ${exports.ReasonCodes[pubrecRC]}`, pubrecRC);
					client["_removeOutgoingAndStoreMessage"](messageId, () => {
						cb(err, packet);
					});
				} else client["_sendPacket"](response);
				break;
			}
			case "suback": {
				delete client.outgoing[messageId];
				client.messageIdProvider.deallocate(messageId);
				const granted = packet.granted;
				for (let grantedI = 0; grantedI < granted.length; grantedI++) {
					const subackRC = granted[grantedI];
					if ((subackRC & 128) !== 0) {
						err = /* @__PURE__ */ new Error(`Subscribe error: ${exports.ReasonCodes[subackRC]}`);
						err.code = subackRC;
						const topics = client.messageIdToTopic[messageId];
						if (topics) topics.forEach((topic) => {
							delete client["_resubscribeTopics"][topic];
						});
					}
				}
				delete client.messageIdToTopic[messageId];
				client["_invokeStoreProcessingQueue"]();
				cb(err, packet);
				break;
			}
			case "unsuback":
				delete client.outgoing[messageId];
				client.messageIdProvider.deallocate(messageId);
				client["_invokeStoreProcessingQueue"]();
				cb(null, packet);
				break;
			default: client.emit("error", /* @__PURE__ */ new Error("unrecognized packet type"));
		}
		if (client.disconnecting && Object.keys(client.outgoing).length === 0) client.emit("outgoingEmpty");
	};
	exports.default = handleAck;
}));
//#endregion
//#region node_modules/mqtt/build/lib/number-allocator.js
var require_number_allocator = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.NumberAllocator = void 0;
	var NumberAllocator = class {
		used;
		min;
		max;
		lowWaterMark;
		constructor(min, max) {
			this.min = min;
			this.max = max;
			this.used = /* @__PURE__ */ new Set();
			this.lowWaterMark = min;
		}
		firstVacant() {
			if (this.used.size === this.max - this.min + 1) return null;
			for (let i = this.lowWaterMark; i <= this.max; i++) if (!this.used.has(i)) return i;
			return null;
		}
		alloc() {
			if (this.used.size === this.max - this.min + 1) return null;
			for (let i = this.lowWaterMark; i <= this.max; i++) if (!this.used.has(i)) {
				this.used.add(i);
				this.lowWaterMark = i + 1;
				return i;
			}
			return null;
		}
		use(num) {
			if (num < this.min || num > this.max || this.used.has(num)) return false;
			this.used.add(num);
			return true;
		}
		free(num) {
			if (num < this.min || num > this.max) return;
			this.used.delete(num);
			if (num < this.lowWaterMark) this.lowWaterMark = num;
		}
		clear() {
			this.used.clear();
			this.lowWaterMark = this.min;
		}
	};
	exports.NumberAllocator = NumberAllocator;
}));
//#endregion
//#region node_modules/mqtt/build/lib/topic-alias-send.js
var require_topic_alias_send = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var lru_cache_1 = require_commonjs();
	var number_allocator_1 = require_number_allocator();
	var TopicAliasSend = class {
		aliasToTopic;
		topicToAlias;
		max;
		numberAllocator;
		length;
		constructor(max) {
			if (max > 0) {
				this.aliasToTopic = new lru_cache_1.LRUCache({ max });
				this.topicToAlias = {};
				this.numberAllocator = new number_allocator_1.NumberAllocator(1, max);
				this.max = max;
				this.length = 0;
			}
		}
		put(topic, alias) {
			if (alias === 0 || alias > this.max) return false;
			const entry = this.aliasToTopic.get(alias);
			if (entry) delete this.topicToAlias[entry];
			this.aliasToTopic.set(alias, topic);
			this.topicToAlias[topic] = alias;
			this.numberAllocator.use(alias);
			this.length = this.aliasToTopic.size;
			return true;
		}
		getTopicByAlias(alias) {
			return this.aliasToTopic.get(alias);
		}
		getAliasByTopic(topic) {
			const alias = this.topicToAlias[topic];
			if (typeof alias !== "undefined") this.aliasToTopic.get(alias);
			return alias;
		}
		clear() {
			this.aliasToTopic.clear();
			this.topicToAlias = {};
			this.numberAllocator.clear();
			this.length = 0;
		}
		getLruAlias() {
			const alias = this.numberAllocator.firstVacant();
			if (alias) return alias;
			return [...this.aliasToTopic.keys()][this.aliasToTopic.size - 1];
		}
	};
	exports.default = TopicAliasSend;
}));
//#endregion
//#region node_modules/mqtt/build/lib/handlers/connack.js
var require_connack = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	var ack_1 = require_ack();
	var topic_alias_send_1 = __importDefault(require_topic_alias_send());
	var shared_1 = require_shared();
	var handleConnack = (client, packet, pump) => {
		client.log("_handleConnack");
		const { options } = client;
		const rc = options.protocolVersion === 5 ? packet.reasonCode : packet.returnCode;
		if (client["connackReceived"]) {
			const err = new shared_1.ErrorWithReasonCode("Protocol error: duplicate CONNACK on the same network connection", 130);
			pump?.discardParsedPackets();
			client["_cleanUp"](true);
			client.emit("error", err);
			return;
		}
		client["connackReceived"] = true;
		clearTimeout(client["connackTimer"]);
		delete client["topicAliasSend"];
		client["_serverProperties"] = packet.properties;
		if (packet.properties) {
			if (packet.properties.topicAliasMaximum) {
				if (packet.properties.topicAliasMaximum > 65535) {
					pump?.discardParsedPackets();
					client["_cleanUp"](true);
					client.emit("error", /* @__PURE__ */ new Error("topicAliasMaximum from broker is out of range"));
					return;
				}
				if (packet.properties.topicAliasMaximum > 0) client["topicAliasSend"] = new topic_alias_send_1.default(packet.properties.topicAliasMaximum);
			}
		}
		if (rc === 0) {
			client.reconnecting = false;
			client["_onConnect"](packet);
		} else if (rc > 0) {
			client.log("_handleConnack :: connection refused with reason code %d", rc);
			const err = new shared_1.ErrorWithReasonCode(`Connection refused: ${ack_1.ReasonCodes[rc]}`, rc);
			pump?.discardParsedPackets();
			client["reconnectSuppressed"] = true;
			client["_cleanUp"](true);
			try {
				client.emit("error", err);
			} finally {
				if (options.reconnectOnConnackError) {
					client["reconnectSuppressed"] = false;
					client["_setupReconnect"]();
				}
			}
		}
	};
	exports.default = handleConnack;
}));
//#endregion
//#region node_modules/mqtt/build/lib/handlers/pubrel.js
var require_pubrel = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var handlePubrel = (client, packet, done) => {
		client.log("handling pubrel packet");
		const callback = typeof done !== "undefined" ? done : client.noop;
		const { messageId } = packet;
		const comp = {
			cmd: "pubcomp",
			messageId
		};
		client.incomingStore.get(packet, (err, pub) => {
			if (!err) {
				client.emit("message", pub.topic, pub.payload, pub);
				client.handleMessage(pub, (err2) => {
					if (err2) return callback(err2);
					client.incomingStore.del(pub, client.noop);
					client["_releaseIncomingQoS2Publish"](messageId);
					client["_sendPacket"](comp, callback);
				});
			} else client["_sendPacket"](comp, callback);
		});
	};
	exports.default = handlePubrel;
}));
//#endregion
//#region node_modules/mqtt/build/lib/handlers/index.js
var require_handlers = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	var publish_1 = __importDefault(require_publish());
	var auth_1 = __importDefault(require_auth());
	var connack_1 = __importDefault(require_connack());
	var ack_1 = __importDefault(require_ack());
	var pubrel_1 = __importDefault(require_pubrel());
	var shared_1 = require_shared();
	var remainingLengthBytes = (remainingLength) => {
		if (remainingLength < 128) return 1;
		if (remainingLength < 16384) return 2;
		if (remainingLength < 2097152) return 3;
		return 4;
	};
	var totalPacketSize = (remainingLength) => 1 + remainingLengthBytes(remainingLength) + remainingLength;
	var handle = (client, packet, done, pump) => {
		const { options } = client;
		const maximumPacketSize = options.properties?.maximumPacketSize;
		const packetSize = totalPacketSize(packet.length);
		if (options.protocolVersion === 5 && maximumPacketSize && maximumPacketSize < packetSize) {
			client.log("_handlePacket :: %s packet of %d bytes exceeds the advertised maximumPacketSize %d", packet.cmd, packetSize, maximumPacketSize);
			pump?.discardParsedPackets();
			client["_cleanUp"](true);
			done();
			client.emit("error", new shared_1.ErrorWithReasonCode(`exceeding packets size ${packet.cmd}: ${packetSize} bytes, maximumPacketSize is ${maximumPacketSize}`, 149));
			return client;
		}
		client.log("_handlePacket :: emitting packetreceive");
		client.emit("packetreceive", packet);
		switch (packet.cmd) {
			case "publish":
				(0, publish_1.default)(client, packet, done, pump);
				break;
			case "puback":
			case "pubrec":
			case "pubcomp":
			case "suback":
			case "unsuback":
				client.reschedulePing();
				try {
					(0, ack_1.default)(client, packet);
				} finally {
					done();
				}
				break;
			case "pubrel":
				client.reschedulePing();
				(0, pubrel_1.default)(client, packet, done);
				break;
			case "connack":
				try {
					(0, connack_1.default)(client, packet, pump);
				} finally {
					done();
				}
				break;
			case "auth":
				client.reschedulePing();
				try {
					(0, auth_1.default)(client, packet, pump);
				} finally {
					done();
				}
				break;
			case "pingresp":
				client.log("_handlePacket :: received pingresp");
				client.reschedulePing(true);
				done();
				break;
			case "disconnect":
				client.emit("disconnect", packet);
				done();
				break;
			default:
				client.log("_handlePacket :: unknown command");
				done();
		}
	};
	exports.default = handle;
}));
//#endregion
//#region node_modules/mqtt/build/lib/default-message-id-provider.js
var require_default_message_id_provider = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var DefaultMessageIdProvider = class {
		nextId;
		constructor() {
			this.nextId = Math.max(1, Math.floor(Math.random() * 65535));
		}
		allocate() {
			const id = this.nextId++;
			if (this.nextId === 65536) this.nextId = 1;
			return id;
		}
		getLastAllocated() {
			return this.nextId === 1 ? 65535 : this.nextId - 1;
		}
		register(messageId) {
			return true;
		}
		deallocate(messageId) {}
		clear() {}
	};
	exports.default = DefaultMessageIdProvider;
}));
//#endregion
//#region node_modules/mqtt/build/lib/topic-alias-recv.js
var require_topic_alias_recv = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var TopicAliasRecv = class {
		aliasToTopic;
		max;
		length;
		constructor(max) {
			this.aliasToTopic = {};
			this.max = max;
		}
		put(topic, alias) {
			if (alias === 0 || alias > this.max) return false;
			this.aliasToTopic[alias] = topic;
			this.length = Object.keys(this.aliasToTopic).length;
			return true;
		}
		getTopicByAlias(alias) {
			return this.aliasToTopic[alias];
		}
		clear() {
			this.aliasToTopic = {};
		}
	};
	exports.default = TopicAliasRecv;
}));
//#endregion
//#region node_modules/mqtt/build/lib/TypedEmitter.js
var require_TypedEmitter = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.TypedEventEmitter = void 0;
	var events_1$1 = __importDefault(__require("events"));
	var shared_1 = require_shared();
	var TypedEventEmitter = class {};
	exports.TypedEventEmitter = TypedEventEmitter;
	(0, shared_1.applyMixin)(TypedEventEmitter, events_1$1.default);
}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/factories/clear-interval-factory.js
var createClearIntervalFactory;
var init_clear_interval_factory = __esmMin((() => {
	createClearIntervalFactory = (scheduledIntervalsState) => (clear) => (timerId) => {
		if (typeof scheduledIntervalsState.get(timerId) === "symbol") {
			scheduledIntervalsState.set(timerId, null);
			clear(timerId).then(() => {
				scheduledIntervalsState.delete(timerId);
			});
		}
	};
}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/factories/clear-timeout-factory.js
var createClearTimeoutFactory;
var init_clear_timeout_factory = __esmMin((() => {
	createClearTimeoutFactory = (scheduledTimeoutsState) => (clear) => (timerId) => {
		if (typeof scheduledTimeoutsState.get(timerId) === "symbol") {
			scheduledTimeoutsState.set(timerId, null);
			clear(timerId).then(() => {
				scheduledTimeoutsState.delete(timerId);
			});
		}
	};
}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/factories/set-interval-factory.js
var createSetIntervalFactory;
var init_set_interval_factory = __esmMin((() => {
	createSetIntervalFactory = (generateUniqueNumber, scheduledIntervalsState) => (set) => (func, delay = 0, ...args) => {
		const symbol = Symbol();
		const timerId = generateUniqueNumber(scheduledIntervalsState);
		scheduledIntervalsState.set(timerId, symbol);
		const schedule = () => set(delay, timerId).then(() => {
			const state = scheduledIntervalsState.get(timerId);
			if (state === void 0) throw new Error("The timer is in an undefined state.");
			if (state === symbol) {
				func(...args);
				if (scheduledIntervalsState.get(timerId) === symbol) schedule();
			}
		});
		schedule();
		return timerId;
	};
}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/factories/set-timeout-factory.js
var createSetTimeoutFactory;
var init_set_timeout_factory = __esmMin((() => {
	createSetTimeoutFactory = (generateUniqueNumber, scheduledTimeoutsState) => (set) => (func, delay = 0, ...args) => {
		const symbol = Symbol();
		const timerId = generateUniqueNumber(scheduledTimeoutsState);
		scheduledTimeoutsState.set(timerId, symbol);
		set(delay, timerId).then(() => {
			const state = scheduledTimeoutsState.get(timerId);
			if (state === void 0) throw new Error("The timer is in an undefined state.");
			if (state === symbol) {
				scheduledTimeoutsState.delete(timerId);
				func(...args);
			}
		});
		return timerId;
	};
}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/interfaces/worker-timers-broker-definition.js
var init_worker_timers_broker_definition = __esmMin((() => {}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/interfaces/index.js
var init_interfaces = __esmMin((() => {
	init_worker_timers_broker_definition();
}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/types/worker-timers-broker-loader.js
var init_worker_timers_broker_loader = __esmMin((() => {}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/types/worker-timers-broker-wrapper.js
var init_worker_timers_broker_wrapper = __esmMin((() => {}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/types/index.js
var init_types = __esmMin((() => {
	init_worker_timers_broker_loader();
	init_worker_timers_broker_wrapper();
}));
//#endregion
//#region node_modules/worker-timers-broker/build/es2019/module.js
var scheduledIntervalsState, scheduledTimeoutsState, createClearInterval, createClearTimeout, createSetInterval, createSetTimeout, wrap, load;
var init_module$1 = __esmMin((() => {
	init_module$2();
	init_module$3();
	init_clear_interval_factory();
	init_clear_timeout_factory();
	init_set_interval_factory();
	init_set_timeout_factory();
	init_interfaces();
	init_types();
	scheduledIntervalsState = /* @__PURE__ */ new Map([[0, null]]);
	scheduledTimeoutsState = /* @__PURE__ */ new Map([[0, null]]);
	createClearInterval = createClearIntervalFactory(scheduledIntervalsState);
	createClearTimeout = createClearTimeoutFactory(scheduledTimeoutsState);
	createSetInterval = createSetIntervalFactory(generateUniqueNumber, scheduledIntervalsState);
	createSetTimeout = createSetTimeoutFactory(generateUniqueNumber, scheduledTimeoutsState);
	wrap = createBroker({
		clearInterval: ({ call }) => createClearInterval((timerId) => call("clear", {
			timerId,
			timerType: "interval"
		})),
		clearTimeout: ({ call }) => createClearTimeout((timerId) => call("clear", {
			timerId,
			timerType: "timeout"
		})),
		setInterval: ({ call }) => createSetInterval((delay, timerId) => call("set", {
			delay,
			now: performance.timeOrigin + performance.now(),
			timerId,
			timerType: "interval"
		})),
		setTimeout: ({ call }) => createSetTimeout((delay, timerId) => call("set", {
			delay,
			now: performance.timeOrigin + performance.now(),
			timerId,
			timerType: "timeout"
		}))
	});
	load = (url) => {
		const worker = new Worker(url);
		return wrap(worker);
	};
}));
//#endregion
//#region node_modules/worker-timers/build/es2019/factories/load-or-return-broker.js
var createLoadOrReturnBroker;
var init_load_or_return_broker = __esmMin((() => {
	createLoadOrReturnBroker = (loadBroker, worker) => {
		let broker = null;
		return () => {
			if (broker !== null) return broker;
			const blob = new Blob([worker], { type: "application/javascript; charset=utf-8" });
			const url = URL.createObjectURL(blob);
			broker = loadBroker(url);
			setTimeout(() => URL.revokeObjectURL(url));
			return broker;
		};
	};
}));
//#endregion
//#region node_modules/worker-timers/build/es2019/worker/worker.js
var worker;
var init_worker = __esmMin((() => {
	worker = `(()=>{var e={455(e,t){!function(e){"use strict";var t=function(e){return function(t){var r=e(t);return t.add(r),r}},r=function(e){return function(t,r){return e.set(t,r),r}},n=void 0===Number.MAX_SAFE_INTEGER?9007199254740991:Number.MAX_SAFE_INTEGER,o=536870912,s=2*o,a=function(e,t){return function(r){var a=t.get(r),i=void 0===a?r.size:a<s?a+1:0;if(!r.has(i))return e(r,i);if(r.size<o){for(;r.has(i);)i=Math.floor(Math.random()*s);return e(r,i)}if(r.size>n)throw new Error("Congratulations, you created a collection of unique numbers which uses all available integers!");for(;r.has(i);)i=Math.floor(Math.random()*n);return e(r,i)}},i=new WeakMap,u=r(i),c=a(u,i),l=t(c);e.addUniqueNumber=l,e.generateUniqueNumber=c}(t)}};const t={};function r(n){const o=t[n];if(void 0!==o)return o.exports;const s=t[n]={exports:{}};return e[n].call(s.exports,s,s.exports,r),s.exports}(()=>{"use strict";const e=-32603,t=-32602,n=-32601,o=(e,t)=>Object.assign(new Error(e),{status:t}),s=t=>o('The handler of the method called "'.concat(t,'" returned an unexpected result.'),e),a=(t,r)=>async({data:{id:a,method:i,params:u}})=>{const c=r[i];try{if(void 0===c)throw(e=>o('The requested method called "'.concat(e,'" is not supported.'),n))(i);const r=void 0===u?c():c(u);if(void 0===r)throw(t=>o('The handler of the method called "'.concat(t,'" returned no required result.'),e))(i);const l=r instanceof Promise?await r:r;if(null===a){if(void 0!==l.result)throw s(i)}else{if(void 0===l.result)throw s(i);const{result:e,transferables:r=[]}=l;t.postMessage({id:a,result:e},r)}}catch(e){const{message:r,status:n=-32603}=e;t.postMessage({error:{code:n,message:r},id:a})}};var i=r(455);const u=new Map,c=(e,r,n)=>({...r,connect:({port:t})=>{t.start();const n=e(t,r),o=(0,i.generateUniqueNumber)(u);return u.set(o,()=>{n(),t.close(),u.delete(o)}),{result:o}},disconnect:({portId:e})=>{const r=u.get(e);if(void 0===r)throw(e=>o('The specified parameter called "portId" with the given value "'.concat(e,'" does not identify a port connected to this worker.'),t))(e);return r(),{result:null}},isSupported:async()=>{if(await new Promise(e=>{const t=new ArrayBuffer(0),{port1:r,port2:n}=new MessageChannel;r.onmessage=({data:t})=>e(null!==t),n.postMessage(t,[t])})){const e=n();return{result:e instanceof Promise?await e:e}}return{result:!1}}}),l=(e,t,r=()=>!0)=>{const n=c(l,t,r),o=a(e,n);return e.addEventListener("message",o),()=>e.removeEventListener("message",o)},d=(e,t)=>r=>{const n=t.get(r);if(void 0===n)return Promise.resolve(!1);const[o,s]=n;return e(o),t.delete(r),s(!1),Promise.resolve(!0)},m=(e,t,r,n)=>(o,s,a)=>{const i=o+s-t.timeOrigin,u=i-t.now();return new Promise(t=>{e.set(a,[r(n,u,i,e,t,a),t])})},f=new Map,h=d(globalThis.clearTimeout,f),p=new Map,w=d(globalThis.clearTimeout,p),v=((e,t)=>{const r=(n,o,s,a)=>{const i=n-e.now();i>0?o.set(a,[t(r,i,n,o,s,a),s]):(o.delete(a),s(!0))};return r})(performance,globalThis.setTimeout),g=m(f,performance,globalThis.setTimeout,v),T=m(p,performance,globalThis.setTimeout,v);l(self,{clear:async({timerId:e,timerType:t})=>({result:await("interval"===t?h(e):w(e))}),set:async({delay:e,now:t,timerId:r,timerType:n})=>({result:await("interval"===n?g:T)(e,t,r)})})})()})();`;
}));
//#endregion
//#region node_modules/worker-timers/build/es2019/module.js
var module_exports = /* @__PURE__ */ __exportAll({
	clearInterval: () => clearInterval$1,
	clearTimeout: () => clearTimeout$1,
	setInterval: () => setInterval$1,
	setTimeout: () => setTimeout$1
});
var loadOrReturnBroker, clearInterval$1, clearTimeout$1, setInterval$1, setTimeout$1;
var init_module = __esmMin((() => {
	init_module$1();
	init_load_or_return_broker();
	init_worker();
	loadOrReturnBroker = createLoadOrReturnBroker(load, worker);
	clearInterval$1 = (timerId) => loadOrReturnBroker().clearInterval(timerId);
	clearTimeout$1 = (timerId) => loadOrReturnBroker().clearTimeout(timerId);
	setInterval$1 = (...args) => loadOrReturnBroker().setInterval(...args);
	setTimeout$1 = (...args) => loadOrReturnBroker().setTimeout(...args);
}));
//#endregion
//#region node_modules/mqtt/build/lib/is-browser.js
var require_is_browser = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.isReactNativeBrowser = exports.isWebWorker = void 0;
	var isStandardBrowserEnv = () => {
		if (typeof window !== "undefined") {
			if (typeof navigator !== "undefined" && navigator.userAgent?.toLowerCase().indexOf(" electron/") > -1 && process?.versions) return !Object.prototype.hasOwnProperty.call(process.versions, "electron");
			return typeof window.document !== "undefined";
		}
		return false;
	};
	var isWebWorkerEnv = () => Boolean(typeof self === "object" && self?.constructor?.name?.includes("WorkerGlobalScope") && typeof Deno === "undefined");
	var isReactNativeEnv = () => typeof navigator !== "undefined" && navigator.product === "ReactNative";
	var isBrowser = isStandardBrowserEnv() || isWebWorkerEnv() || isReactNativeEnv();
	exports.isWebWorker = isWebWorkerEnv();
	exports.isReactNativeBrowser = isReactNativeEnv();
	exports.default = isBrowser;
}));
//#endregion
//#region node_modules/mqtt/build/lib/get-timer.js
var require_get_timer = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __createBinding = exports && exports.__createBinding || (Object.create ? (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		var desc = Object.getOwnPropertyDescriptor(m, k);
		if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) desc = {
			enumerable: true,
			get: function() {
				return m[k];
			}
		};
		Object.defineProperty(o, k2, desc);
	}) : (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		o[k2] = m[k];
	}));
	var __setModuleDefault = exports && exports.__setModuleDefault || (Object.create ? (function(o, v) {
		Object.defineProperty(o, "default", {
			enumerable: true,
			value: v
		});
	}) : function(o, v) {
		o["default"] = v;
	});
	var __importStar = exports && exports.__importStar || (function() {
		var ownKeys = function(o) {
			ownKeys = Object.getOwnPropertyNames || function(o) {
				var ar = [];
				for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
				return ar;
			};
			return ownKeys(o);
		};
		return function(mod) {
			if (mod && mod.__esModule) return mod;
			var result = {};
			if (mod != null) {
				for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
			}
			__setModuleDefault(result, mod);
			return result;
		};
	})();
	Object.defineProperty(exports, "__esModule", { value: true });
	var worker_timers_1 = (init_module(), __toCommonJS(module_exports));
	var is_browser_1 = __importStar(require_is_browser());
	var workerTimer = {
		set: worker_timers_1.setInterval,
		clear: worker_timers_1.clearInterval
	};
	var nativeTimer = {
		set: (func, time) => setInterval(func, time),
		clear: (timerId) => clearInterval(timerId)
	};
	var getTimer = (variant) => {
		switch (variant) {
			case "native": return nativeTimer;
			case "worker": return workerTimer;
			default: return is_browser_1.default && !is_browser_1.isWebWorker && !is_browser_1.isReactNativeBrowser ? workerTimer : nativeTimer;
		}
	};
	exports.default = getTimer;
}));
//#endregion
//#region node_modules/mqtt/build/lib/KeepaliveManager.js
var require_KeepaliveManager = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	var get_timer_1 = __importDefault(require_get_timer());
	var KeepaliveManager = class {
		_keepalive;
		timerId;
		timer;
		destroyed = false;
		counter;
		client;
		_keepaliveTimeoutTimestamp;
		_intervalEvery;
		get keepaliveTimeoutTimestamp() {
			return this._keepaliveTimeoutTimestamp;
		}
		get intervalEvery() {
			return this._intervalEvery;
		}
		get keepalive() {
			return this._keepalive;
		}
		constructor(client, variant) {
			this.client = client;
			this.timer = typeof variant === "object" && "set" in variant && "clear" in variant ? variant : (0, get_timer_1.default)(variant);
			this.setKeepalive(client.keepalive);
		}
		clear() {
			if (this.timerId) {
				this.timer.clear(this.timerId);
				this.timerId = null;
			}
		}
		setKeepalive(value) {
			value *= 1e3;
			if (isNaN(value) || value <= 0 || value > 2147483647) throw new Error(`Keepalive value must be an integer between 0 and 2147483647. Provided value is ${value}`);
			this._keepalive = value;
			this.reschedule();
			this.client["log"](`KeepaliveManager: set keepalive to ${value}ms`);
		}
		destroy() {
			this.clear();
			this.destroyed = true;
		}
		reschedule() {
			if (this.destroyed) return;
			this.clear();
			this.counter = 0;
			const keepAliveTimeout = Math.ceil(this._keepalive * 1.5);
			this._keepaliveTimeoutTimestamp = Date.now() + keepAliveTimeout;
			this._intervalEvery = Math.ceil(this._keepalive / 2);
			this.timerId = this.timer.set(() => {
				if (this.destroyed) return;
				this.counter += 1;
				if (this.counter === 2) this.client.sendPing();
				else if (this.counter > 2) this.client.onKeepaliveTimeout();
			}, this._intervalEvery);
		}
	};
	exports.default = KeepaliveManager;
}));
//#endregion
//#region node_modules/mqtt/build/lib/client.js
var require_client = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __createBinding = exports && exports.__createBinding || (Object.create ? (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		var desc = Object.getOwnPropertyDescriptor(m, k);
		if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) desc = {
			enumerable: true,
			get: function() {
				return m[k];
			}
		};
		Object.defineProperty(o, k2, desc);
	}) : (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		o[k2] = m[k];
	}));
	var __setModuleDefault = exports && exports.__setModuleDefault || (Object.create ? (function(o, v) {
		Object.defineProperty(o, "default", {
			enumerable: true,
			value: v
		});
	}) : function(o, v) {
		o["default"] = v;
	});
	var __importStar = exports && exports.__importStar || (function() {
		var ownKeys = function(o) {
			ownKeys = Object.getOwnPropertyNames || function(o) {
				var ar = [];
				for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
				return ar;
			};
			return ownKeys(o);
		};
		return function(mod) {
			if (mod && mod.__esModule) return mod;
			var result = {};
			if (mod != null) {
				for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
			}
			__setModuleDefault(result, mod);
			return result;
		};
	})();
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	var mqtt_packet_1 = __importDefault(require_mqtt$1());
	var readable_stream_1 = require_ours();
	var default_1 = __importDefault(require_default());
	var debug_1 = __importDefault(require_src());
	var validations = __importStar(require_validations());
	var store_1 = __importDefault(require_store());
	var handlers_1 = __importDefault(require_handlers());
	var ack_1 = require_ack();
	var default_message_id_provider_1 = __importDefault(require_default_message_id_provider());
	var topic_alias_recv_1 = __importDefault(require_topic_alias_recv());
	var shared_1 = require_shared();
	var TypedEmitter_1 = require_TypedEmitter();
	var KeepaliveManager_1 = __importDefault(require_KeepaliveManager());
	var is_browser_1 = __importStar(require_is_browser());
	var setImmediate = globalThis.setImmediate || ((...args) => {
		const callback = args.shift();
		(0, shared_1.nextTick)(() => {
			callback(...args);
		});
	});
	var MAX_RECEIVE_MAXIMUM = 65535;
	var RECEIVE_MAXIMUM_EXCEEDED = 147;
	var defaultConnectOptions = {
		keepalive: 60,
		reschedulePings: true,
		protocolId: "MQTT",
		protocolVersion: 4,
		reconnectPeriod: 1e3,
		connectTimeout: 3e4,
		clean: true,
		resubscribe: true,
		subscribeBatchSize: null,
		writeCache: true,
		timerVariant: "auto"
	};
	exports.default = class MqttClient extends TypedEmitter_1.TypedEventEmitter {
		static VERSION = shared_1.MQTTJS_VERSION;
		connected;
		disconnecting;
		disconnected;
		reconnecting;
		incomingStore;
		outgoingStore;
		options;
		queueQoSZero;
		_reconnectCount;
		log;
		messageIdProvider;
		outgoing;
		messageIdToTopic;
		noop;
		keepaliveManager;
		stream;
		queue;
		streamBuilder;
		_resubscribeTopics;
		connackTimer;
		reconnectTimer;
		_storeProcessing;
		_packetIdsDuringStoreProcessing;
		_storeProcessingQueue;
		_firstConnection;
		topicAliasRecv;
		topicAliasSend;
		_incomingQoS2Ids;
		_receiveMaximum;
		_deferredReconnect;
		_currentPump;
		connackReceived;
		reconnectSuppressed;
		_authenticationMethod;
		connackPacket;
		_serverProperties;
		static defaultId() {
			return `mqttjs_${Math.random().toString(16).substr(2, 8)}`;
		}
		constructor(streamBuilder, options) {
			super();
			this.options = options || {};
			for (const k in defaultConnectOptions) if (typeof this.options[k] === "undefined") this.options[k] = defaultConnectOptions[k];
			else this.options[k] = options[k];
			this.log = this.options.log || (0, debug_1.default)("mqttjs:client");
			this.noop = this._noop.bind(this);
			this.log("MqttClient :: version:", MqttClient.VERSION);
			if (is_browser_1.isWebWorker) this.log("MqttClient :: environment", "webworker");
			else this.log("MqttClient :: environment", is_browser_1.default ? "browser" : "node");
			this.log("MqttClient :: options.protocol", options.protocol);
			this.log("MqttClient :: options.protocolVersion", options.protocolVersion);
			this.log("MqttClient :: options.username", options.username);
			this.log("MqttClient :: options.keepalive", options.keepalive);
			this.log("MqttClient :: options.reconnectPeriod", options.reconnectPeriod);
			this.log("MqttClient :: options.rejectUnauthorized", options.rejectUnauthorized);
			this.log("MqttClient :: options.properties.topicAliasMaximum", options.properties ? options.properties.topicAliasMaximum : void 0);
			this.options.clientId = typeof options.clientId === "string" ? options.clientId : MqttClient.defaultId();
			this.log("MqttClient :: clientId", this.options.clientId);
			this.options.customHandleAcks = options.protocolVersion === 5 && options.customHandleAcks ? options.customHandleAcks : (...args) => {
				args[3](null, 0);
			};
			if (!this.options.writeCache) mqtt_packet_1.default.writeToStream.cacheNumbers = false;
			this.streamBuilder = streamBuilder;
			this.messageIdProvider = typeof this.options.messageIdProvider === "undefined" ? new default_message_id_provider_1.default() : this.options.messageIdProvider;
			this.outgoingStore = options.outgoingStore || new store_1.default();
			this.incomingStore = options.incomingStore || new store_1.default();
			this._incomingQoS2Ids = /* @__PURE__ */ new Set();
			this._receiveMaximum = MAX_RECEIVE_MAXIMUM;
			const receiveMaximum = options.properties?.receiveMaximum;
			if (receiveMaximum !== void 0) {
				if (Number.isInteger(receiveMaximum) && receiveMaximum >= 1 && receiveMaximum <= MAX_RECEIVE_MAXIMUM) this._receiveMaximum = receiveMaximum;
				else this.log("MqttClient :: options.properties.receiveMaximum %o is not an integer between 1 and %d, enforcing and advertising %d instead", receiveMaximum, MAX_RECEIVE_MAXIMUM, MAX_RECEIVE_MAXIMUM);
			}
			this.queueQoSZero = options.queueQoSZero === void 0 ? true : options.queueQoSZero;
			this._resubscribeTopics = {};
			this.messageIdToTopic = {};
			this.keepaliveManager = null;
			this.connected = false;
			this.disconnecting = false;
			this.reconnecting = false;
			this.queue = [];
			this.connackTimer = null;
			this.connackReceived = false;
			this.reconnectSuppressed = false;
			this.reconnectTimer = null;
			this._storeProcessing = false;
			this._packetIdsDuringStoreProcessing = {};
			this._storeProcessingQueue = [];
			this.outgoing = {};
			this._firstConnection = true;
			if (options.properties && options.properties.topicAliasMaximum > 0) {
				if (options.properties.topicAliasMaximum > 65535) this.log("MqttClient :: options.properties.topicAliasMaximum is out of range");
				else this.topicAliasRecv = new topic_alias_recv_1.default(options.properties.topicAliasMaximum);
			}
			this.on("connect", () => {
				const { queue } = this;
				const deliver = () => {
					const entry = queue.shift();
					this.log("deliver :: entry %o", entry);
					let packet = null;
					if (!entry) {
						this._resubscribe();
						return;
					}
					packet = entry.packet;
					this.log("deliver :: call _sendPacket for %o", packet);
					let send = true;
					if (packet.messageId && packet.messageId !== 0) {
						if (!this.messageIdProvider.register(packet.messageId)) send = false;
					}
					if (send) this._sendPacket(packet, (err) => {
						if (entry.cb) entry.cb(err);
						deliver();
					});
					else {
						this.log("messageId: %d has already used. The message is skipped and removed.", packet.messageId);
						deliver();
					}
				};
				this.log("connect :: sending queued packets");
				deliver();
			});
			this.on("close", () => {
				this.log("close :: connected set to `false`");
				this.connected = false;
				this.log("close :: clearing connackTimer");
				clearTimeout(this.connackTimer);
				this._destroyKeepaliveManager();
				this._serverProperties = void 0;
				if (this.topicAliasRecv) this.topicAliasRecv.clear();
				this.log("close :: calling _setupReconnect");
				this._setupReconnect();
			});
			if (!this.options.manualConnect) {
				this.log("MqttClient :: setting up stream");
				this.connect();
			}
		}
		handleAuth(packet, callback) {
			callback();
		}
		handleMessage(packet, callback) {
			callback();
		}
		_nextId() {
			return this.messageIdProvider.allocate();
		}
		getLastMessageId() {
			return this.messageIdProvider.getLastAllocated();
		}
		_trackIncomingQoS2Publish(messageId, done, pump) {
			if (this._incomingQoS2Ids.has(messageId)) return true;
			if (this.options.protocolVersion === 5 && this._incomingQoS2Ids.size >= this._receiveMaximum) {
				this.log("_trackIncomingQoS2Publish :: inbound QoS 2 messages exceeded receiveMaximum %d", this._receiveMaximum);
				pump?.discardParsedPackets();
				if (!pump || pump.isCurrent()) this._cleanUp(true);
				done();
				this.emit("error", new shared_1.ErrorWithReasonCode(`${ack_1.ReasonCodes[RECEIVE_MAXIMUM_EXCEEDED]}: ${this._incomingQoS2Ids.size + 1} inbound QoS 2 messages in flight, receiveMaximum is ${this._receiveMaximum}`, RECEIVE_MAXIMUM_EXCEEDED));
				return false;
			}
			this._incomingQoS2Ids.add(messageId);
			return true;
		}
		_releaseIncomingQoS2Publish(messageId) {
			this._incomingQoS2Ids.delete(messageId);
		}
		_discardIncomingQoS2State() {
			if (this._incomingQoS2Ids.size === 0) return;
			this.log("_discardIncomingQoS2State :: session not resumed, dropping %d inbound QoS 2 message(s)", this._incomingQoS2Ids.size);
			for (const messageId of this._incomingQoS2Ids) this.incomingStore.del({ messageId }, this.noop);
			this._incomingQoS2Ids.clear();
		}
		connect() {
			const writable = new readable_stream_1.Writable();
			const parser = mqtt_packet_1.default.parser(this.options);
			let completeParse = null;
			const packets = [];
			const pump = {
				discardParsedPackets: () => {
					packets.length = 0;
				},
				isCurrent: () => this._currentPump === pump
			};
			this._currentPump = pump;
			this.connackReceived = false;
			this.reconnectSuppressed = false;
			this.log("connect :: calling method to clear reconnect");
			this._clearReconnect();
			this._serverProperties = void 0;
			if (this.disconnected && !this.reconnecting) {
				this.incomingStore = this.options.incomingStore || new store_1.default();
				this.outgoingStore = this.options.outgoingStore || new store_1.default();
				this._incomingQoS2Ids.clear();
				this.disconnecting = false;
				this.disconnected = false;
			}
			this.log("connect :: using streamBuilder provided to client to create stream");
			this.stream = this.streamBuilder(this);
			parser.on("packet", (packet) => {
				this.log("parser :: on packet push to packets array.");
				packets.push(packet);
			});
			const work = () => {
				this.log("work :: getting next packet in queue");
				if (this._currentPump !== pump) {
					this.log("work :: pump superseded, dropping %d queued packets", packets.length);
					packets.length = 0;
				}
				const packet = packets.shift();
				if (packet) {
					this.log("work :: packet pulled from queue");
					(0, handlers_1.default)(this, packet, nextTickWork, pump);
				} else {
					this.log("work :: no packets in queue");
					const done = completeParse;
					completeParse = null;
					this.log("work :: done flag is %s", !!done);
					if (done) done();
				}
			};
			const nextTickWork = () => {
				if (packets.length) (0, shared_1.nextTick)(work);
				else {
					const done = completeParse;
					completeParse = null;
					if (done) done();
				}
			};
			writable._write = (buf, enc, done) => {
				completeParse = done;
				this.log("writable stream :: parsing buffer");
				parser.parse(buf);
				work();
			};
			const streamErrorHandler = (error) => {
				this.log("streamErrorHandler :: error", error.message);
				if (error.code) {
					this.log("streamErrorHandler :: emitting error");
					this.emit("error", error);
				} else this.noop(error);
			};
			this.log("connect :: pipe stream to writable stream");
			this.stream.pipe(writable);
			this.stream.on("error", streamErrorHandler);
			this.stream.on("close", () => {
				this.log("(%s)stream :: on close", this.options.clientId);
				this._flushVolatile();
				this.log("stream: emit close to MqttClient");
				this.emit("close");
			});
			this.log("connect: sending packet `connect`");
			const connectPacket = {
				cmd: "connect",
				protocolId: this.options.protocolId,
				protocolVersion: this.options.protocolVersion,
				clean: this.options.clean,
				clientId: this.options.clientId,
				keepalive: this.options.keepalive,
				username: this.options.username,
				password: this.options.password,
				properties: this.options.properties ? { ...this.options.properties } : void 0
			};
			if (connectPacket.properties?.receiveMaximum !== void 0) connectPacket.properties.receiveMaximum = this._receiveMaximum;
			if (this.options.will) connectPacket.will = {
				...this.options.will,
				payload: this.options.will?.payload
			};
			if (this.topicAliasRecv) {
				if (!connectPacket.properties) connectPacket.properties = {};
				if (this.topicAliasRecv) connectPacket.properties.topicAliasMaximum = this.topicAliasRecv.max;
			}
			this._authenticationMethod = connectPacket.properties?.authenticationMethod;
			this._writePacket(connectPacket);
			parser.on("error", this.emit.bind(this, "error"));
			if (this.options.properties) {
				if (!this.options.properties.authenticationMethod && this.options.properties.authenticationData) {
					this.end(() => this.emit("error", /* @__PURE__ */ new Error("Packet has no Authentication Method")));
					return this;
				}
				if (this.options.properties.authenticationMethod && this.options.authPacket && typeof this.options.authPacket === "object") {
					const authPacket = {
						cmd: "auth",
						reasonCode: 0,
						...this.options.authPacket
					};
					this._writePacket(authPacket);
				}
			}
			this.stream.setMaxListeners(1e3);
			clearTimeout(this.connackTimer);
			this.connackTimer = setTimeout(() => this._onConnackTimeout(), this.options.connectTimeout);
			return this;
		}
		_onConnackTimeout() {
			this.log("!!connectTimeout hit!! Calling _cleanUp with force `true`");
			this._cleanUp(true);
			this.emit("error", /* @__PURE__ */ new Error("connack timeout"));
		}
		publish(topic, message, opts, callback) {
			this.log("publish :: message `%s` to topic `%s`", message, topic);
			const { options } = this;
			if (typeof opts === "function") {
				callback = opts;
				opts = null;
			}
			opts = opts || {};
			opts = {
				qos: 0,
				retain: false,
				dup: false,
				...opts
			};
			const { qos, retain, dup, properties, cbStorePut } = opts;
			if (this._checkDisconnecting(callback)) return this;
			const publishProc = () => {
				let messageId = 0;
				if (qos === 1 || qos === 2) {
					messageId = this._nextId();
					if (messageId === null) {
						this.log("No messageId left");
						return false;
					}
				}
				const packet = {
					cmd: "publish",
					topic,
					payload: message,
					qos,
					retain,
					messageId,
					dup
				};
				if (options.protocolVersion === 5) packet.properties = properties;
				this.log("publish :: qos", qos);
				switch (qos) {
					case 1:
					case 2:
						this.outgoing[packet.messageId] = {
							volatile: false,
							cmd: "publish",
							cb: callback || this.noop
						};
						this.log("MqttClient:publish: packet cmd: %s", packet.cmd);
						this._sendPacket(packet, void 0, cbStorePut);
						break;
					default:
						this.log("MqttClient:publish: packet cmd: %s", packet.cmd);
						this._sendPacket(packet, callback, cbStorePut);
				}
				return true;
			};
			if (this._storeProcessing || this._storeProcessingQueue.length > 0 || !publishProc()) this._storeProcessingQueue.push({
				invoke: publishProc,
				cbStorePut: opts.cbStorePut,
				callback
			});
			return this;
		}
		publishAsync(topic, message, opts) {
			return new Promise((resolve, reject) => {
				this.publish(topic, message, opts, (err, packet) => {
					if (err) reject(err);
					else resolve(packet);
				});
			});
		}
		subscribe(topicObject, opts, callback) {
			const version = this.options.protocolVersion;
			if (typeof opts === "function") callback = opts;
			callback = callback || this.noop;
			let resubscribe = false;
			let topicsList = [];
			if (typeof topicObject === "string") {
				topicObject = [topicObject];
				topicsList = topicObject;
			} else if (Array.isArray(topicObject)) topicsList = topicObject;
			else if (typeof topicObject === "object") {
				resubscribe = topicObject.resubscribe;
				delete topicObject.resubscribe;
				topicsList = Object.keys(topicObject);
			}
			const invalidTopic = validations.validateTopics(topicsList);
			if (invalidTopic !== null) {
				setImmediate(callback, /* @__PURE__ */ new Error(`Invalid topic ${invalidTopic}`));
				return this;
			}
			if (this._checkDisconnecting(callback)) {
				this.log("subscribe: discconecting true");
				return this;
			}
			const defaultOpts = { qos: 0 };
			if (version === 5) {
				defaultOpts.nl = false;
				defaultOpts.rap = false;
				defaultOpts.rh = 0;
			}
			opts = {
				...defaultOpts,
				...opts
			};
			const { properties } = opts;
			const subs = [];
			const parseSub = (topic, subOptions) => {
				subOptions = subOptions || opts;
				if (!Object.prototype.hasOwnProperty.call(this._resubscribeTopics, topic) || this._resubscribeTopics[topic].qos < subOptions.qos || resubscribe) {
					const currentOpts = {
						topic,
						qos: subOptions.qos
					};
					if (version === 5) {
						currentOpts.nl = subOptions.nl;
						currentOpts.rap = subOptions.rap;
						currentOpts.rh = subOptions.rh;
						currentOpts.properties = properties;
					}
					this.log("subscribe: pushing topic `%s` and qos `%s` to subs list", currentOpts.topic, currentOpts.qos);
					subs.push(currentOpts);
				}
			};
			if (Array.isArray(topicObject)) topicObject.forEach((topic) => {
				this.log("subscribe: array topic %s", topic);
				parseSub(topic);
			});
			else Object.keys(topicObject).forEach((topic) => {
				this.log("subscribe: object topic %s, %o", topic, topicObject[topic]);
				parseSub(topic, topicObject[topic]);
			});
			if (!subs.length) {
				callback(null, []);
				return this;
			}
			const subscribeChunkedSubs = (chunkedSubs, messageId) => {
				const packet = {
					cmd: "subscribe",
					subscriptions: chunkedSubs,
					messageId
				};
				if (properties) packet.properties = properties;
				if (this.options.resubscribe) {
					this.log("subscribe :: resubscribe true");
					const topics = [];
					chunkedSubs.forEach((sub) => {
						if (this.options.reconnectPeriod > 0) {
							const topic = { qos: sub.qos };
							if (version === 5) {
								topic.nl = sub.nl || false;
								topic.rap = sub.rap || false;
								topic.rh = sub.rh || 0;
								topic.properties = sub.properties;
							}
							this._resubscribeTopics[sub.topic] = topic;
							topics.push(sub.topic);
						}
					});
					this.messageIdToTopic[packet.messageId] = topics;
				}
				const promise = new Promise((resolve, reject) => {
					this.outgoing[packet.messageId] = {
						volatile: true,
						cmd: "subscribe",
						cb: (err, packet2) => {
							if (err) {
								reject(new shared_1.ErrorWithSubackPacket(err.message, packet2));
								return;
							}
							const { granted } = packet2;
							if (granted.length !== chunkedSubs.length) {
								reject(new shared_1.ErrorWithSubackPacket(`Protocol error: suback granted ${granted.length} reason code(s) for ${chunkedSubs.length} subscription(s)`, packet2));
								this._cleanUp(true);
								return;
							}
							for (let grantedI = 0; grantedI < granted.length; grantedI += 1) chunkedSubs[grantedI].qos = granted[grantedI];
							resolve(packet2);
						}
					};
				});
				this.log("subscribe :: call _sendPacket");
				this._sendPacket(packet);
				return promise;
			};
			const subscribeProc = () => {
				const batchSize = this.options.subscribeBatchSize ?? subs.length;
				const subscribePromises = [];
				for (let i = 0; i < subs.length; i += batchSize) {
					const chunkedSubs = subs.slice(i, i + batchSize);
					const messageId = this._nextId();
					if (messageId === null) {
						this.log("No messageId left");
						return false;
					}
					subscribePromises.push(subscribeChunkedSubs(chunkedSubs, messageId));
				}
				Promise.all(subscribePromises).then((packets) => {
					callback(null, subs, packets.at(-1));
				}).catch((err) => {
					callback(err, subs, err.packet);
				});
				return true;
			};
			if (this._storeProcessing || this._storeProcessingQueue.length > 0 || !subscribeProc()) this._storeProcessingQueue.push({
				invoke: subscribeProc,
				callback
			});
			return this;
		}
		subscribeAsync(topicObject, opts) {
			return new Promise((resolve, reject) => {
				this.subscribe(topicObject, opts, (err, granted) => {
					if (err) reject(err);
					else resolve(granted);
				});
			});
		}
		unsubscribe(topic, opts, callback) {
			if (typeof topic === "string") topic = [topic];
			if (typeof opts === "function") callback = opts;
			callback = callback || this.noop;
			const invalidTopic = validations.validateTopics(topic);
			if (invalidTopic !== null) {
				setImmediate(callback, /* @__PURE__ */ new Error(`Invalid topic ${invalidTopic}`));
				return this;
			}
			if (this._checkDisconnecting(callback)) return this;
			const unsubscribeProc = () => {
				const messageId = this._nextId();
				if (messageId === null) {
					this.log("No messageId left");
					return false;
				}
				const packet = {
					cmd: "unsubscribe",
					messageId,
					unsubscriptions: []
				};
				if (typeof topic === "string") packet.unsubscriptions = [topic];
				else if (Array.isArray(topic)) packet.unsubscriptions = topic;
				if (this.options.resubscribe) packet.unsubscriptions.forEach((topic2) => {
					delete this._resubscribeTopics[topic2];
				});
				if (typeof opts === "object" && opts.properties) packet.properties = opts.properties;
				this.outgoing[packet.messageId] = {
					volatile: true,
					cmd: "unsubscribe",
					cb: callback
				};
				this.log("unsubscribe: call _sendPacket");
				this._sendPacket(packet);
				return true;
			};
			if (this._storeProcessing || this._storeProcessingQueue.length > 0 || !unsubscribeProc()) this._storeProcessingQueue.push({
				invoke: unsubscribeProc,
				callback
			});
			return this;
		}
		unsubscribeAsync(topic, opts) {
			return new Promise((resolve, reject) => {
				this.unsubscribe(topic, opts, (err, packet) => {
					if (err) reject(err);
					else resolve(packet);
				});
			});
		}
		end(force, opts, cb) {
			this.log("end :: (%s)", this.options.clientId);
			if (force == null || typeof force !== "boolean") {
				cb = cb || opts;
				opts = force;
				force = false;
			}
			if (typeof opts !== "object") {
				cb = cb || opts;
				opts = null;
			}
			this.log("end :: cb? %s", !!cb);
			if (!cb || typeof cb !== "function") cb = this.noop;
			const closeStores = () => {
				this.log("end :: closeStores: closing incoming and outgoing stores");
				this.disconnected = true;
				this._incomingQoS2Ids.clear();
				this.incomingStore.close((e1) => {
					this.outgoingStore.close((e2) => {
						this.log("end :: closeStores: emitting end");
						this.emit("end");
						if (cb) {
							const err = e1 || e2;
							this.log("end :: closeStores: invoking callback with args");
							cb(err);
						}
					});
				});
				if (this._deferredReconnect) this._deferredReconnect();
				else if (this.options.reconnectPeriod === 0 || this.options.manualConnect) this.disconnecting = false;
			};
			const finish = () => {
				this.log("end :: (%s) :: finish :: calling _cleanUp with force %s", this.options.clientId, force);
				this._cleanUp(force, () => {
					this.log("end :: finish :: calling process.nextTick on closeStores");
					(0, shared_1.nextTick)(closeStores);
				}, opts);
			};
			if (this.disconnecting) {
				cb();
				return this;
			}
			this._clearReconnect();
			this.disconnecting = true;
			if (!force && Object.keys(this.outgoing).length > 0) {
				this.log("end :: (%s) :: calling finish in 10ms once outgoing is empty", this.options.clientId);
				this.once("outgoingEmpty", setTimeout.bind(null, finish, 10));
			} else {
				this.log("end :: (%s) :: immediately calling finish", this.options.clientId);
				finish();
			}
			return this;
		}
		endAsync(force, opts) {
			return new Promise((resolve, reject) => {
				this.end(force, opts, (err) => {
					if (err) reject(err);
					else resolve();
				});
			});
		}
		removeOutgoingMessage(messageId) {
			if (this.outgoing[messageId]) {
				const { cb } = this.outgoing[messageId];
				this._removeOutgoingAndStoreMessage(messageId, () => {
					cb(/* @__PURE__ */ new Error("Message removed"));
				});
			}
			return this;
		}
		reconnect(opts) {
			this.log("client reconnect");
			const f = () => {
				if (opts) {
					this.options.incomingStore = opts.incomingStore;
					this.options.outgoingStore = opts.outgoingStore;
				} else {
					this.options.incomingStore = null;
					this.options.outgoingStore = null;
				}
				this.incomingStore = this.options.incomingStore || new store_1.default();
				this.outgoingStore = this.options.outgoingStore || new store_1.default();
				this._incomingQoS2Ids.clear();
				this.disconnecting = false;
				this.disconnected = false;
				this._deferredReconnect = null;
				this._reconnect();
			};
			if (this.disconnecting && !this.disconnected) this._deferredReconnect = f;
			else f();
			return this;
		}
		_flushVolatile() {
			if (this.outgoing) {
				this.log("_flushVolatile :: deleting volatile messages from the queue and setting their callbacks as error function");
				Object.keys(this.outgoing).forEach((messageId) => {
					if (this.outgoing[messageId].volatile && typeof this.outgoing[messageId].cb === "function") {
						this.outgoing[messageId].cb(/* @__PURE__ */ new Error("Connection closed"));
						delete this.outgoing[messageId];
					}
				});
			}
		}
		_flush() {
			if (this.outgoing) {
				this.log("_flush: queue exists? %b", !!this.outgoing);
				Object.keys(this.outgoing).forEach((messageId) => {
					if (typeof this.outgoing[messageId].cb === "function") {
						this.outgoing[messageId].cb(/* @__PURE__ */ new Error("Connection closed"));
						delete this.outgoing[messageId];
					}
				});
			}
		}
		_removeTopicAliasAndRecoverTopicName(packet) {
			let alias;
			if (packet.properties) alias = packet.properties.topicAlias;
			let topic = packet.topic.toString();
			this.log("_removeTopicAliasAndRecoverTopicName :: alias %d, topic %o", alias, topic);
			if (topic.length === 0) {
				if (typeof alias === "undefined") return /* @__PURE__ */ new Error("Unregistered Topic Alias");
				topic = this.topicAliasSend.getTopicByAlias(alias);
				if (typeof topic === "undefined") return /* @__PURE__ */ new Error("Unregistered Topic Alias");
				packet.topic = topic;
			}
			if (alias) delete packet.properties.topicAlias;
		}
		_checkDisconnecting(callback) {
			if (this.disconnecting) {
				if (callback && callback !== this.noop) callback(/* @__PURE__ */ new Error("client disconnecting"));
				else this.emit("error", /* @__PURE__ */ new Error("client disconnecting"));
			}
			return this.disconnecting;
		}
		_reconnect() {
			this.log("_reconnect: emitting reconnect to client");
			this.emit("reconnect");
			if (this.connected) {
				this.end(() => {
					this.connect();
				});
				this.log("client already connected. disconnecting first.");
			} else {
				this.log("_reconnect: calling connect");
				this.connect();
			}
		}
		_setupReconnect() {
			if (!this.disconnecting && !this.reconnectSuppressed && !this.reconnectTimer && this.options.reconnectPeriod > 0) {
				if (!this.reconnecting) {
					this.log("_setupReconnect :: emit `offline` state");
					this.emit("offline");
					this.log("_setupReconnect :: set `reconnecting` to `true`");
					this.reconnecting = true;
				}
				this.log("_setupReconnect :: setting reconnectTimer for %d ms", this.options.reconnectPeriod);
				this.reconnectTimer = setInterval(() => {
					this.log("reconnectTimer :: reconnect triggered!");
					this._reconnect();
				}, this.options.reconnectPeriod);
			} else this.log("_setupReconnect :: doing nothing...");
		}
		_clearReconnect() {
			this.log("_clearReconnect : clearing reconnect timer");
			if (this.reconnectTimer) {
				clearInterval(this.reconnectTimer);
				this.reconnectTimer = null;
			}
		}
		_cleanUp(forced, done, opts = {}) {
			if (done) {
				this.log("_cleanUp :: done callback provided for on stream close");
				this.stream.on("close", done);
			}
			this.log("_cleanUp :: forced? %s", forced);
			if (forced) {
				if (this.options.reconnectPeriod === 0 && this.options.clean) this._flush();
				this.log("_cleanUp :: (%s) :: destroying stream", this.options.clientId);
				this.stream.destroy();
			} else {
				const packet = {
					cmd: "disconnect",
					...opts
				};
				this.log("_cleanUp :: (%s) :: call _sendPacket with disconnect packet", this.options.clientId);
				this._sendPacket(packet, () => {
					this.log("_cleanUp :: (%s) :: destroying stream", this.options.clientId);
					setImmediate(() => {
						this.stream.end(() => {
							this.log("_cleanUp :: (%s) :: stream destroyed", this.options.clientId);
						});
					});
				});
			}
			if (!this.disconnecting && !this.reconnecting) {
				this.log("_cleanUp :: client not disconnecting/reconnecting. Clearing and resetting reconnect.");
				this._clearReconnect();
				this._setupReconnect();
			}
			this._destroyKeepaliveManager();
			if (done && !this.connected) {
				this.log("_cleanUp :: (%s) :: removing stream `done` callback `close` listener", this.options.clientId);
				this.stream.removeListener("close", done);
				done();
			}
		}
		_storeAndSend(packet, cb, cbStorePut) {
			this.log("storeAndSend :: store packet with cmd %s to outgoingStore", packet.cmd);
			let storePacket = packet;
			let err;
			if (storePacket.cmd === "publish") {
				storePacket = (0, default_1.default)(packet);
				err = this._removeTopicAliasAndRecoverTopicName(storePacket);
				if (err) return cb && cb(err);
			}
			this.outgoingStore.put(storePacket, (err2) => {
				if (err2) return cb && cb(err2);
				cbStorePut();
				this._writePacket(packet, cb);
			});
		}
		_applyTopicAlias(packet) {
			if (this.options.protocolVersion === 5) {
				if (packet.cmd === "publish") {
					let alias;
					if (packet.properties) alias = packet.properties.topicAlias;
					const topic = packet.topic.toString();
					if (this.topicAliasSend) {
						if (alias) {
							if (topic.length !== 0) {
								this.log("applyTopicAlias :: register topic: %s - alias: %d", topic, alias);
								if (!this.topicAliasSend.put(topic, alias)) {
									this.log("applyTopicAlias :: error out of range. topic: %s - alias: %d", topic, alias);
									return /* @__PURE__ */ new Error("Sending Topic Alias out of range");
								}
							}
						} else if (topic.length !== 0) {
							if (this.options.autoAssignTopicAlias) {
								alias = this.topicAliasSend.getAliasByTopic(topic);
								if (alias) {
									packet.topic = "";
									packet.properties = {
										...packet.properties,
										topicAlias: alias
									};
									this.log("applyTopicAlias :: auto assign(use) topic: %s - alias: %d", topic, alias);
								} else {
									alias = this.topicAliasSend.getLruAlias();
									this.topicAliasSend.put(topic, alias);
									packet.properties = {
										...packet.properties,
										topicAlias: alias
									};
									this.log("applyTopicAlias :: auto assign topic: %s - alias: %d", topic, alias);
								}
							} else if (this.options.autoUseTopicAlias) {
								alias = this.topicAliasSend.getAliasByTopic(topic);
								if (alias) {
									packet.topic = "";
									packet.properties = {
										...packet.properties,
										topicAlias: alias
									};
									this.log("applyTopicAlias :: auto use topic: %s - alias: %d", topic, alias);
								}
							}
						}
					} else if (alias) {
						this.log("applyTopicAlias :: error out of range. topic: %s - alias: %d", topic, alias);
						return /* @__PURE__ */ new Error("Sending Topic Alias out of range");
					}
				}
			}
		}
		_noop(err) {
			this.log("noop ::", err);
		}
		_writePacket(packet, cb) {
			this.log("_writePacket :: packet: %O", packet);
			this.log("_writePacket :: emitting `packetsend`");
			this.emit("packetsend", packet);
			this.log("_writePacket :: writing to stream");
			const result = mqtt_packet_1.default.writeToStream(packet, this.stream, this.options);
			this.log("_writePacket :: writeToStream result %s", result);
			if (!result && cb && cb !== this.noop) {
				this.log("_writePacket :: handle events on `drain` once through callback.");
				this.stream.once("drain", cb);
			} else if (cb) {
				this.log("_writePacket :: invoking cb");
				cb();
			}
		}
		_sendPacket(packet, cb, cbStorePut, noStore) {
			this.log("_sendPacket :: (%s) ::  start", this.options.clientId);
			cbStorePut = cbStorePut || this.noop;
			cb = cb || this.noop;
			const err = this._applyTopicAlias(packet);
			if (err) {
				cb(err);
				return;
			}
			if (!this.connected) {
				if (packet.cmd === "auth") {
					this._writePacket(packet, cb);
					return;
				}
				this.log("_sendPacket :: client not connected. Storing packet offline.");
				this._storePacket(packet, cb, cbStorePut);
				return;
			}
			if (noStore) {
				this._writePacket(packet, cb);
				return;
			}
			switch (packet.cmd) {
				case "publish": break;
				case "pubrel":
					this._storeAndSend(packet, cb, cbStorePut);
					return;
				default:
					this._writePacket(packet, cb);
					return;
			}
			switch (packet.qos) {
				case 2:
				case 1:
					this._storeAndSend(packet, cb, cbStorePut);
					break;
				default: this._writePacket(packet, cb);
			}
			this.log("_sendPacket :: (%s) ::  end", this.options.clientId);
		}
		_storePacket(packet, cb, cbStorePut) {
			this.log("_storePacket :: packet: %o", packet);
			this.log("_storePacket :: cb? %s", !!cb);
			cbStorePut = cbStorePut || this.noop;
			let storePacket = packet;
			if (storePacket.cmd === "publish") {
				storePacket = (0, default_1.default)(packet);
				const err = this._removeTopicAliasAndRecoverTopicName(storePacket);
				if (err) return cb && cb(err);
			}
			const qos = storePacket.qos || 0;
			if (qos === 0 && this.queueQoSZero || storePacket.cmd !== "publish") this.queue.push({
				packet: storePacket,
				cb
			});
			else if (qos > 0) {
				cb = this.outgoing[storePacket.messageId] ? this.outgoing[storePacket.messageId].cb : null;
				this.outgoingStore.put(storePacket, (err) => {
					if (err) return cb && cb(err);
					cbStorePut();
				});
			} else if (cb) cb(/* @__PURE__ */ new Error("No connection to broker"));
		}
		get serverProperties() {
			return this._serverProperties;
		}
		get keepalive() {
			const serverKeepAlive = this.connected ? this._serverProperties?.serverKeepAlive : void 0;
			return serverKeepAlive && this.options.keepalive ? serverKeepAlive : this.options.keepalive;
		}
		_setupKeepaliveManager() {
			this.log("_setupKeepaliveManager :: keepalive %d (seconds)", this.keepalive);
			if (!this.keepaliveManager && this.keepalive) this.keepaliveManager = new KeepaliveManager_1.default(this, this.options.timerVariant);
		}
		_destroyKeepaliveManager() {
			if (this.keepaliveManager) {
				this.log("_destroyKeepaliveManager :: destroying keepalive manager");
				this.keepaliveManager.destroy();
				this.keepaliveManager = null;
			}
		}
		reschedulePing(force = false) {
			if (this.keepaliveManager && this.keepalive && (force || this.options.reschedulePings)) this._reschedulePing();
		}
		_reschedulePing() {
			this.log("_reschedulePing :: rescheduling ping");
			this.keepaliveManager.reschedule();
		}
		sendPing() {
			this.log("_sendPing :: sending pingreq");
			this._sendPacket({ cmd: "pingreq" });
		}
		onKeepaliveTimeout() {
			this.log("onKeepaliveTimeout :: calling _cleanUp with force true");
			this._cleanUp(true);
			this.emit("error", /* @__PURE__ */ new Error("Keepalive timeout"));
		}
		_resubscribe() {
			this.log("_resubscribe");
			const _resubscribeTopicsKeys = Object.keys(this._resubscribeTopics);
			if (!this._firstConnection && (this.options.clean || this.options.protocolVersion >= 4 && !this.connackPacket.sessionPresent) && _resubscribeTopicsKeys.length > 0) {
				if (this.options.resubscribe) {
					if (this.options.protocolVersion === 5) {
						this.log("_resubscribe: protocolVersion 5");
						for (let topicI = 0; topicI < _resubscribeTopicsKeys.length; topicI++) {
							const resubscribeTopic = {};
							resubscribeTopic[_resubscribeTopicsKeys[topicI]] = this._resubscribeTopics[_resubscribeTopicsKeys[topicI]];
							resubscribeTopic.resubscribe = true;
							this.subscribe(resubscribeTopic, { properties: resubscribeTopic[_resubscribeTopicsKeys[topicI]].properties });
						}
					} else {
						this._resubscribeTopics.resubscribe = true;
						this.subscribe(this._resubscribeTopics);
					}
				} else this._resubscribeTopics = {};
			}
			this._firstConnection = false;
		}
		_onConnect(packet) {
			if (this.disconnected) {
				this.emit("connect", packet);
				return;
			}
			this.connackPacket = packet;
			if (!(!this.options.clean && (this.options.protocolVersion < 4 || packet.sessionPresent === true))) this._discardIncomingQoS2State();
			this.messageIdProvider.clear();
			this.connected = true;
			this._setupKeepaliveManager();
			const startStreamProcess = () => {
				let outStore = this.outgoingStore.createStream();
				const remove = () => {
					outStore.destroy();
					outStore = null;
					this._flushStoreProcessingQueue();
					clearStoreProcessing();
				};
				const clearStoreProcessing = () => {
					this._storeProcessing = false;
					this._packetIdsDuringStoreProcessing = {};
				};
				this.once("close", remove);
				outStore.on("error", (err) => {
					clearStoreProcessing();
					this._flushStoreProcessingQueue();
					this.removeListener("close", remove);
					this.emit("error", err);
				});
				const storeDeliver = () => {
					if (!outStore) return;
					const packet2 = outStore.read(1);
					let cb;
					if (!packet2) {
						outStore.once("readable", storeDeliver);
						return;
					}
					this._storeProcessing = true;
					if (this._packetIdsDuringStoreProcessing[packet2.messageId]) {
						storeDeliver();
						return;
					}
					if (!this.disconnecting && !this.reconnectTimer) {
						cb = this.outgoing[packet2.messageId] ? this.outgoing[packet2.messageId].cb : null;
						this.outgoing[packet2.messageId] = {
							volatile: false,
							cmd: packet2.cmd,
							cb(err, status) {
								if (cb) cb(err, status);
								storeDeliver();
							}
						};
						this._packetIdsDuringStoreProcessing[packet2.messageId] = true;
						if (this.messageIdProvider.register(packet2.messageId)) this._sendPacket(packet2, void 0, void 0, true);
						else this.log("messageId: %d has already used.", packet2.messageId);
					} else if (outStore.destroy) outStore.destroy();
				};
				outStore.on("end", () => {
					let allProcessed = true;
					for (const id in this._packetIdsDuringStoreProcessing) if (!this._packetIdsDuringStoreProcessing[id]) {
						allProcessed = false;
						break;
					}
					this.removeListener("close", remove);
					if (allProcessed) {
						clearStoreProcessing();
						this._invokeAllStoreProcessingQueue();
						this.emit("connect", packet);
					} else startStreamProcess();
				});
				storeDeliver();
			};
			startStreamProcess();
		}
		_invokeStoreProcessingQueue() {
			if (!this._storeProcessing && this._storeProcessingQueue.length > 0) {
				const f = this._storeProcessingQueue[0];
				if (f && f.invoke()) {
					this._storeProcessingQueue.shift();
					return true;
				}
			}
			return false;
		}
		_invokeAllStoreProcessingQueue() {
			while (this._invokeStoreProcessingQueue());
		}
		_flushStoreProcessingQueue() {
			for (const f of this._storeProcessingQueue) {
				if (f.cbStorePut) f.cbStorePut(/* @__PURE__ */ new Error("Connection closed"));
				if (f.callback) f.callback(/* @__PURE__ */ new Error("Connection closed"));
			}
			this._storeProcessingQueue.splice(0);
		}
		_removeOutgoingAndStoreMessage(messageId, cb) {
			delete this.outgoing[messageId];
			this.outgoingStore.del({ messageId }, (err, packet) => {
				cb(err, packet);
				this.messageIdProvider.deallocate(messageId);
				this._invokeStoreProcessingQueue();
			});
		}
	};
}));
//#endregion
//#region node_modules/mqtt/build/lib/unique-message-id-provider.js
var require_unique_message_id_provider = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var number_allocator_1 = require_number_allocator();
	var UniqueMessageIdProvider = class {
		numberAllocator;
		lastId;
		constructor() {
			this.numberAllocator = new number_allocator_1.NumberAllocator(1, 65535);
		}
		allocate() {
			this.lastId = this.numberAllocator.alloc();
			return this.lastId;
		}
		getLastAllocated() {
			return this.lastId;
		}
		register(messageId) {
			return this.numberAllocator.use(messageId);
		}
		deallocate(messageId) {
			this.numberAllocator.free(messageId);
		}
		clear() {
			this.numberAllocator.clear();
		}
	};
	exports.default = UniqueMessageIdProvider;
}));
//#endregion
//#region node_modules/ws/lib/constants.js
var require_constants$1 = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var BINARY_TYPES = [
		"nodebuffer",
		"arraybuffer",
		"fragments"
	];
	var hasBlob = typeof Blob !== "undefined";
	if (hasBlob) BINARY_TYPES.push("blob");
	module.exports = {
		BINARY_TYPES,
		CLOSE_TIMEOUT: 3e4,
		EMPTY_BUFFER: Buffer.alloc(0),
		GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
		hasBlob,
		kForOnEventAttribute: Symbol("kIsForOnEventAttribute"),
		kListener: Symbol("kListener"),
		kStatusCode: Symbol("status-code"),
		kWebSocket: Symbol("websocket"),
		NOOP: () => {}
	};
}));
//#endregion
//#region __vite-optional-peer-dep:bufferutil:ws
var __vite_optional_peer_dep_bufferutil_ws_exports = /* @__PURE__ */ __exportAll({ default: () => __vite_optional_peer_dep_bufferutil_ws_default });
var __vite_optional_peer_dep_bufferutil_ws_default;
var init___vite_optional_peer_dep_bufferutil_ws = __esmMin((() => {
	__vite_optional_peer_dep_bufferutil_ws_default = {};
	throw new Error(`Could not resolve "bufferutil" imported by "ws". Is it installed?`);
}));
//#endregion
//#region node_modules/ws/lib/buffer-util.js
var require_buffer_util = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { EMPTY_BUFFER } = require_constants$1();
	var FastBuffer = Buffer[Symbol.species];
	/**
	* Merges an array of buffers into a new buffer.
	*
	* @param {Buffer[]} list The array of buffers to concat
	* @param {Number} totalLength The total length of buffers in the list
	* @return {Buffer} The resulting buffer
	* @public
	*/
	function concat(list, totalLength) {
		if (list.length === 0) return EMPTY_BUFFER;
		if (list.length === 1) return list[0];
		const target = Buffer.allocUnsafe(totalLength);
		let offset = 0;
		for (let i = 0; i < list.length; i++) {
			const buf = list[i];
			target.set(buf, offset);
			offset += buf.length;
		}
		if (offset < totalLength) return new FastBuffer(target.buffer, target.byteOffset, offset);
		return target;
	}
	/**
	* Masks a buffer using the given mask.
	*
	* @param {Buffer} source The buffer to mask
	* @param {Buffer} mask The mask to use
	* @param {Buffer} output The buffer where to store the result
	* @param {Number} offset The offset at which to start writing
	* @param {Number} length The number of bytes to mask.
	* @public
	*/
	function _mask(source, mask, output, offset, length) {
		for (let i = 0; i < length; i++) output[offset + i] = source[i] ^ mask[i & 3];
	}
	/**
	* Unmasks a buffer using the given mask.
	*
	* @param {Buffer} buffer The buffer to unmask
	* @param {Buffer} mask The mask to use
	* @public
	*/
	function _unmask(buffer, mask) {
		for (let i = 0; i < buffer.length; i++) buffer[i] ^= mask[i & 3];
	}
	/**
	* Converts a buffer to an `ArrayBuffer`.
	*
	* @param {Buffer} buf The buffer to convert
	* @return {ArrayBuffer} Converted buffer
	* @public
	*/
	function toArrayBuffer(buf) {
		if (buf.length === buf.buffer.byteLength) return buf.buffer;
		return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
	}
	/**
	* Converts `data` to a `Buffer`.
	*
	* @param {*} data The data to convert
	* @return {Buffer} The buffer
	* @throws {TypeError}
	* @public
	*/
	function toBuffer(data) {
		toBuffer.readOnly = true;
		if (Buffer.isBuffer(data)) return data;
		let buf;
		if (data instanceof ArrayBuffer) buf = new FastBuffer(data);
		else if (ArrayBuffer.isView(data)) buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
		else {
			buf = Buffer.from(data);
			toBuffer.readOnly = false;
		}
		return buf;
	}
	module.exports = {
		concat,
		mask: _mask,
		toArrayBuffer,
		toBuffer,
		unmask: _unmask
	};
	/* istanbul ignore else  */
	if (!process.env.WS_NO_BUFFER_UTIL) try {
		const bufferUtil = (init___vite_optional_peer_dep_bufferutil_ws(), __toCommonJS(__vite_optional_peer_dep_bufferutil_ws_exports));
		module.exports.mask = function(source, mask, output, offset, length) {
			if (length < 48) _mask(source, mask, output, offset, length);
			else bufferUtil.mask(source, mask, output, offset, length);
		};
		module.exports.unmask = function(buffer, mask) {
			if (buffer.length < 32) _unmask(buffer, mask);
			else bufferUtil.unmask(buffer, mask);
		};
	} catch (e) {}
}));
//#endregion
//#region node_modules/ws/lib/limiter.js
var require_limiter = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var kDone = Symbol("kDone");
	var kRun = Symbol("kRun");
	/**
	* A very simple job queue with adjustable concurrency. Adapted from
	* https://github.com/STRML/async-limiter
	*/
	var Limiter = class {
		/**
		* Creates a new `Limiter`.
		*
		* @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
		*     to run concurrently
		*/
		constructor(concurrency) {
			this[kDone] = () => {
				this.pending--;
				this[kRun]();
			};
			this.concurrency = concurrency || Infinity;
			this.jobs = [];
			this.pending = 0;
		}
		/**
		* Adds a job to the queue.
		*
		* @param {Function} job The job to run
		* @public
		*/
		add(job) {
			this.jobs.push(job);
			this[kRun]();
		}
		/**
		* Removes a job from the queue and runs it if possible.
		*
		* @private
		*/
		[kRun]() {
			if (this.pending === this.concurrency) return;
			if (this.jobs.length) {
				const job = this.jobs.shift();
				this.pending++;
				job(this[kDone]);
			}
		}
	};
	module.exports = Limiter;
}));
//#endregion
//#region node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var zlib = __require("zlib");
	var bufferUtil = require_buffer_util();
	var Limiter = require_limiter();
	var { kStatusCode } = require_constants$1();
	var FastBuffer = Buffer[Symbol.species];
	var TRAILER = Buffer.from([
		0,
		0,
		255,
		255
	]);
	var kPerMessageDeflate = Symbol("permessage-deflate");
	var kTotalLength = Symbol("total-length");
	var kCallback = Symbol("callback");
	var kBuffers = Symbol("buffers");
	var kError = Symbol("error");
	var zlibLimiter;
	/**
	* permessage-deflate implementation.
	*/
	var PerMessageDeflate = class {
		/**
		* Creates a PerMessageDeflate instance.
		*
		* @param {Object} [options] Configuration options
		* @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
		*     for, or request, a custom client window size
		* @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
		*     acknowledge disabling of client context takeover
		* @param {Number} [options.concurrencyLimit=10] The number of concurrent
		*     calls to zlib
		* @param {Boolean} [options.isServer=false] Create the instance in either
		*     server or client mode
		* @param {Number} [options.maxPayload=0] The maximum allowed message length
		* @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
		*     use of a custom server window size
		* @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
		*     disabling of server context takeover
		* @param {Number} [options.threshold=1024] Size (in bytes) below which
		*     messages should not be compressed if context takeover is disabled
		* @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
		*     deflate
		* @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
		*     inflate
		*/
		constructor(options) {
			this._options = options || {};
			this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
			this._maxPayload = this._options.maxPayload | 0;
			this._isServer = !!this._options.isServer;
			this._deflate = null;
			this._inflate = null;
			this.params = null;
			if (!zlibLimiter) zlibLimiter = new Limiter(this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10);
		}
		/**
		* @type {String}
		*/
		static get extensionName() {
			return "permessage-deflate";
		}
		/**
		* Create an extension negotiation offer.
		*
		* @return {Object} Extension parameters
		* @public
		*/
		offer() {
			const params = {};
			if (this._options.serverNoContextTakeover) params.server_no_context_takeover = true;
			if (this._options.clientNoContextTakeover) params.client_no_context_takeover = true;
			if (this._options.serverMaxWindowBits) params.server_max_window_bits = this._options.serverMaxWindowBits;
			if (this._options.clientMaxWindowBits) params.client_max_window_bits = this._options.clientMaxWindowBits;
			else if (this._options.clientMaxWindowBits == null) params.client_max_window_bits = true;
			return params;
		}
		/**
		* Accept an extension negotiation offer/response.
		*
		* @param {Array} configurations The extension negotiation offers/reponse
		* @return {Object} Accepted configuration
		* @public
		*/
		accept(configurations) {
			configurations = this.normalizeParams(configurations);
			this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
			return this.params;
		}
		/**
		* Releases all resources used by the extension.
		*
		* @public
		*/
		cleanup() {
			if (this._inflate) {
				this._inflate.close();
				this._inflate = null;
			}
			if (this._deflate) {
				const callback = this._deflate[kCallback];
				this._deflate.close();
				this._deflate = null;
				if (callback) callback(/* @__PURE__ */ new Error("The deflate stream was closed while data was being processed"));
			}
		}
		/**
		*  Accept an extension negotiation offer.
		*
		* @param {Array} offers The extension negotiation offers
		* @return {Object} Accepted configuration
		* @private
		*/
		acceptAsServer(offers) {
			const opts = this._options;
			const accepted = offers.find((params) => {
				if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && (typeof params.client_max_window_bits === "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)) return false;
				return true;
			});
			if (!accepted) throw new Error("None of the extension offers can be accepted");
			if (opts.serverNoContextTakeover) accepted.server_no_context_takeover = true;
			if (opts.clientNoContextTakeover) accepted.client_no_context_takeover = true;
			if (typeof opts.serverMaxWindowBits === "number") accepted.server_max_window_bits = opts.serverMaxWindowBits;
			if (typeof opts.clientMaxWindowBits === "number") accepted.client_max_window_bits = opts.clientMaxWindowBits;
			else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) delete accepted.client_max_window_bits;
			return accepted;
		}
		/**
		* Accept the extension negotiation response.
		*
		* @param {Array} response The extension negotiation response
		* @return {Object} Accepted configuration
		* @private
		*/
		acceptAsClient(response) {
			const params = response[0];
			if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) throw new Error("Unexpected parameter \"client_no_context_takeover\"");
			if (!params.client_max_window_bits) {
				if (typeof this._options.clientMaxWindowBits === "number") params.client_max_window_bits = this._options.clientMaxWindowBits;
			} else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) throw new Error("Unexpected or invalid parameter \"client_max_window_bits\"");
			return params;
		}
		/**
		* Normalize parameters.
		*
		* @param {Array} configurations The extension negotiation offers/reponse
		* @return {Array} The offers/response with normalized parameters
		* @private
		*/
		normalizeParams(configurations) {
			configurations.forEach((params) => {
				Object.keys(params).forEach((key) => {
					let value = params[key];
					if (value.length > 1) throw new Error(`Parameter "${key}" must have only a single value`);
					value = value[0];
					if (key === "client_max_window_bits") {
						if (value !== true) {
							const num = +value;
							if (!Number.isInteger(num) || num < 8 || num > 15) throw new TypeError(`Invalid value for parameter "${key}": ${value}`);
							value = num;
						} else if (!this._isServer) throw new TypeError(`Invalid value for parameter "${key}": ${value}`);
					} else if (key === "server_max_window_bits") {
						const num = +value;
						if (!Number.isInteger(num) || num < 8 || num > 15) throw new TypeError(`Invalid value for parameter "${key}": ${value}`);
						value = num;
					} else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
						if (value !== true) throw new TypeError(`Invalid value for parameter "${key}": ${value}`);
					} else throw new Error(`Unknown parameter "${key}"`);
					params[key] = value;
				});
			});
			return configurations;
		}
		/**
		* Decompress data. Concurrency limited.
		*
		* @param {Buffer} data Compressed data
		* @param {Boolean} fin Specifies whether or not this is the last fragment
		* @param {Function} callback Callback
		* @public
		*/
		decompress(data, fin, callback) {
			zlibLimiter.add((done) => {
				this._decompress(data, fin, (err, result) => {
					done();
					callback(err, result);
				});
			});
		}
		/**
		* Compress data. Concurrency limited.
		*
		* @param {(Buffer|String)} data Data to compress
		* @param {Boolean} fin Specifies whether or not this is the last fragment
		* @param {Function} callback Callback
		* @public
		*/
		compress(data, fin, callback) {
			zlibLimiter.add((done) => {
				this._compress(data, fin, (err, result) => {
					done();
					callback(err, result);
				});
			});
		}
		/**
		* Decompress data.
		*
		* @param {Buffer} data Compressed data
		* @param {Boolean} fin Specifies whether or not this is the last fragment
		* @param {Function} callback Callback
		* @private
		*/
		_decompress(data, fin, callback) {
			const endpoint = this._isServer ? "client" : "server";
			if (!this._inflate) {
				const key = `${endpoint}_max_window_bits`;
				const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
				this._inflate = zlib.createInflateRaw({
					...this._options.zlibInflateOptions,
					windowBits
				});
				this._inflate[kPerMessageDeflate] = this;
				this._inflate[kTotalLength] = 0;
				this._inflate[kBuffers] = [];
				this._inflate.on("error", inflateOnError);
				this._inflate.on("data", inflateOnData);
			}
			this._inflate[kCallback] = callback;
			this._inflate.write(data);
			if (fin) this._inflate.write(TRAILER);
			this._inflate.flush(() => {
				const err = this._inflate[kError];
				if (err) {
					this._inflate.close();
					this._inflate = null;
					callback(err);
					return;
				}
				const data = bufferUtil.concat(this._inflate[kBuffers], this._inflate[kTotalLength]);
				if (this._inflate._readableState.endEmitted) {
					this._inflate.close();
					this._inflate = null;
				} else {
					this._inflate[kTotalLength] = 0;
					this._inflate[kBuffers] = [];
					if (fin && this.params[`${endpoint}_no_context_takeover`]) this._inflate.reset();
				}
				callback(null, data);
			});
		}
		/**
		* Compress data.
		*
		* @param {(Buffer|String)} data Data to compress
		* @param {Boolean} fin Specifies whether or not this is the last fragment
		* @param {Function} callback Callback
		* @private
		*/
		_compress(data, fin, callback) {
			const endpoint = this._isServer ? "server" : "client";
			if (!this._deflate) {
				const key = `${endpoint}_max_window_bits`;
				const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
				this._deflate = zlib.createDeflateRaw({
					...this._options.zlibDeflateOptions,
					windowBits
				});
				this._deflate[kTotalLength] = 0;
				this._deflate[kBuffers] = [];
				this._deflate.on("data", deflateOnData);
			}
			this._deflate[kCallback] = callback;
			this._deflate.write(data);
			this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
				if (!this._deflate) return;
				let data = bufferUtil.concat(this._deflate[kBuffers], this._deflate[kTotalLength]);
				if (fin) data = new FastBuffer(data.buffer, data.byteOffset, data.length - 4);
				this._deflate[kCallback] = null;
				this._deflate[kTotalLength] = 0;
				this._deflate[kBuffers] = [];
				if (fin && this.params[`${endpoint}_no_context_takeover`]) this._deflate.reset();
				callback(null, data);
			});
		}
	};
	module.exports = PerMessageDeflate;
	/**
	* The listener of the `zlib.DeflateRaw` stream `'data'` event.
	*
	* @param {Buffer} chunk A chunk of data
	* @private
	*/
	function deflateOnData(chunk) {
		this[kBuffers].push(chunk);
		this[kTotalLength] += chunk.length;
	}
	/**
	* The listener of the `zlib.InflateRaw` stream `'data'` event.
	*
	* @param {Buffer} chunk A chunk of data
	* @private
	*/
	function inflateOnData(chunk) {
		this[kTotalLength] += chunk.length;
		if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
			this[kBuffers].push(chunk);
			return;
		}
		this[kError] = /* @__PURE__ */ new RangeError("Max payload size exceeded");
		this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
		this[kError][kStatusCode] = 1009;
		this.removeListener("data", inflateOnData);
		this.reset();
	}
	/**
	* The listener of the `zlib.InflateRaw` stream `'error'` event.
	*
	* @param {Error} err The emitted error
	* @private
	*/
	function inflateOnError(err) {
		this[kPerMessageDeflate]._inflate = null;
		if (this[kError]) {
			this[kCallback](this[kError]);
			return;
		}
		err[kStatusCode] = 1007;
		this[kCallback](err);
	}
}));
//#endregion
//#region __vite-optional-peer-dep:utf-8-validate:ws
var __vite_optional_peer_dep_utf_8_validate_ws_exports = /* @__PURE__ */ __exportAll({ default: () => __vite_optional_peer_dep_utf_8_validate_ws_default });
var __vite_optional_peer_dep_utf_8_validate_ws_default;
var init___vite_optional_peer_dep_utf_8_validate_ws = __esmMin((() => {
	__vite_optional_peer_dep_utf_8_validate_ws_default = {};
	throw new Error(`Could not resolve "utf-8-validate" imported by "ws". Is it installed?`);
}));
//#endregion
//#region node_modules/ws/lib/validation.js
var require_validation = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { isUtf8 } = __require("buffer");
	var { hasBlob } = require_constants$1();
	var tokenChars = [
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		1,
		0,
		1,
		1,
		1,
		1,
		1,
		0,
		0,
		1,
		1,
		0,
		1,
		1,
		0,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		0,
		0,
		0,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		1,
		0,
		1,
		0,
		1,
		0
	];
	/**
	* Checks if a status code is allowed in a close frame.
	*
	* @param {Number} code The status code
	* @return {Boolean} `true` if the status code is valid, else `false`
	* @public
	*/
	function isValidStatusCode(code) {
		return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
	}
	/**
	* Checks if a given buffer contains only correct UTF-8.
	* Ported from https://www.cl.cam.ac.uk/%7Emgk25/ucs/utf8_check.c by
	* Markus Kuhn.
	*
	* @param {Buffer} buf The buffer to check
	* @return {Boolean} `true` if `buf` contains only correct UTF-8, else `false`
	* @public
	*/
	function _isValidUTF8(buf) {
		const len = buf.length;
		let i = 0;
		while (i < len) if ((buf[i] & 128) === 0) i++;
		else if ((buf[i] & 224) === 192) {
			if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) return false;
			i += 2;
		} else if ((buf[i] & 240) === 224) {
			if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || buf[i] === 237 && (buf[i + 1] & 224) === 160) return false;
			i += 3;
		} else if ((buf[i] & 248) === 240) {
			if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) return false;
			i += 4;
		} else return false;
		return true;
	}
	/**
	* Determines whether a value is a `Blob`.
	*
	* @param {*} value The value to be tested
	* @return {Boolean} `true` if `value` is a `Blob`, else `false`
	* @private
	*/
	function isBlob(value) {
		return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
	}
	module.exports = {
		isBlob,
		isValidStatusCode,
		isValidUTF8: _isValidUTF8,
		tokenChars
	};
	if (isUtf8) module.exports.isValidUTF8 = function(buf) {
		return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
	};
	else if (!process.env.WS_NO_UTF_8_VALIDATE) try {
		const isValidUTF8 = (init___vite_optional_peer_dep_utf_8_validate_ws(), __toCommonJS(__vite_optional_peer_dep_utf_8_validate_ws_exports));
		module.exports.isValidUTF8 = function(buf) {
			return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
		};
	} catch (e) {}
}));
//#endregion
//#region node_modules/ws/lib/receiver.js
var require_receiver = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { Writable } = __require("stream");
	var PerMessageDeflate = require_permessage_deflate();
	var { BINARY_TYPES, EMPTY_BUFFER, kStatusCode, kWebSocket } = require_constants$1();
	var { concat, toArrayBuffer, unmask } = require_buffer_util();
	var { isValidStatusCode, isValidUTF8 } = require_validation();
	var FastBuffer = Buffer[Symbol.species];
	var GET_INFO = 0;
	var GET_PAYLOAD_LENGTH_16 = 1;
	var GET_PAYLOAD_LENGTH_64 = 2;
	var GET_MASK = 3;
	var GET_DATA = 4;
	var INFLATING = 5;
	var DEFER_EVENT = 6;
	/**
	* HyBi Receiver implementation.
	*
	* @extends Writable
	*/
	var Receiver = class extends Writable {
		/**
		* Creates a Receiver instance.
		*
		* @param {Object} [options] Options object
		* @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
		*     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
		*     multiple times in the same tick
		* @param {String} [options.binaryType=nodebuffer] The type for binary data
		* @param {Object} [options.extensions] An object containing the negotiated
		*     extensions
		* @param {Boolean} [options.isServer=false] Specifies whether to operate in
		*     client or server mode
		* @param {Number} [options.maxBufferedChunks=0] The maximum number of
		*     buffered data chunks
		* @param {Number} [options.maxFragments=0] The maximum number of message
		*     fragments
		* @param {Number} [options.maxPayload=0] The maximum allowed message length
		* @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
		*     not to skip UTF-8 validation for text and close messages
		*/
		constructor(options = {}) {
			super();
			this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
			this._binaryType = options.binaryType || BINARY_TYPES[0];
			this._extensions = options.extensions || {};
			this._isServer = !!options.isServer;
			this._maxBufferedChunks = options.maxBufferedChunks | 0;
			this._maxFragments = options.maxFragments | 0;
			this._maxPayload = options.maxPayload | 0;
			this._skipUTF8Validation = !!options.skipUTF8Validation;
			this[kWebSocket] = void 0;
			this._bufferedBytes = 0;
			this._buffers = [];
			this._compressed = false;
			this._payloadLength = 0;
			this._mask = void 0;
			this._fragmented = 0;
			this._masked = false;
			this._fin = false;
			this._opcode = 0;
			this._totalPayloadLength = 0;
			this._messageLength = 0;
			this._numFragments = 0;
			this._fragments = [];
			this._errored = false;
			this._loop = false;
			this._state = GET_INFO;
		}
		/**
		* Implements `Writable.prototype._write()`.
		*
		* @param {Buffer} chunk The chunk of data to write
		* @param {String} encoding The character encoding of `chunk`
		* @param {Function} cb Callback
		* @private
		*/
		_write(chunk, encoding, cb) {
			if (this._opcode === 8 && this._state == GET_INFO) return cb();
			if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
				cb(this.createError(RangeError, "Too many buffered chunks", false, 1008, "WS_ERR_TOO_MANY_BUFFERED_PARTS"));
				return;
			}
			this._bufferedBytes += chunk.length;
			this._buffers.push(chunk);
			this.startLoop(cb);
		}
		/**
		* Consumes `n` bytes from the buffered data.
		*
		* @param {Number} n The number of bytes to consume
		* @return {Buffer} The consumed bytes
		* @private
		*/
		consume(n) {
			this._bufferedBytes -= n;
			if (n === this._buffers[0].length) return this._buffers.shift();
			if (n < this._buffers[0].length) {
				const buf = this._buffers[0];
				this._buffers[0] = new FastBuffer(buf.buffer, buf.byteOffset + n, buf.length - n);
				return new FastBuffer(buf.buffer, buf.byteOffset, n);
			}
			const dst = Buffer.allocUnsafe(n);
			do {
				const buf = this._buffers[0];
				const offset = dst.length - n;
				if (n >= buf.length) dst.set(this._buffers.shift(), offset);
				else {
					dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
					this._buffers[0] = new FastBuffer(buf.buffer, buf.byteOffset + n, buf.length - n);
				}
				n -= buf.length;
			} while (n > 0);
			return dst;
		}
		/**
		* Starts the parsing loop.
		*
		* @param {Function} cb Callback
		* @private
		*/
		startLoop(cb) {
			this._loop = true;
			do
				switch (this._state) {
					case GET_INFO:
						this.getInfo(cb);
						break;
					case GET_PAYLOAD_LENGTH_16:
						this.getPayloadLength16(cb);
						break;
					case GET_PAYLOAD_LENGTH_64:
						this.getPayloadLength64(cb);
						break;
					case GET_MASK:
						this.getMask();
						break;
					case GET_DATA:
						this.getData(cb);
						break;
					case INFLATING:
					case DEFER_EVENT:
						this._loop = false;
						return;
				}
			while (this._loop);
			if (!this._errored) cb();
		}
		/**
		* Reads the first two bytes of a frame.
		*
		* @param {Function} cb Callback
		* @private
		*/
		getInfo(cb) {
			if (this._bufferedBytes < 2) {
				this._loop = false;
				return;
			}
			const buf = this.consume(2);
			if ((buf[0] & 48) !== 0) {
				cb(this.createError(RangeError, "RSV2 and RSV3 must be clear", true, 1002, "WS_ERR_UNEXPECTED_RSV_2_3"));
				return;
			}
			const compressed = (buf[0] & 64) === 64;
			if (compressed && !this._extensions[PerMessageDeflate.extensionName]) {
				cb(this.createError(RangeError, "RSV1 must be clear", true, 1002, "WS_ERR_UNEXPECTED_RSV_1"));
				return;
			}
			this._fin = (buf[0] & 128) === 128;
			this._opcode = buf[0] & 15;
			this._payloadLength = buf[1] & 127;
			if (this._opcode === 0) {
				if (compressed) {
					cb(this.createError(RangeError, "RSV1 must be clear", true, 1002, "WS_ERR_UNEXPECTED_RSV_1"));
					return;
				}
				if (!this._fragmented) {
					cb(this.createError(RangeError, "invalid opcode 0", true, 1002, "WS_ERR_INVALID_OPCODE"));
					return;
				}
				this._opcode = this._fragmented;
			} else if (this._opcode === 1 || this._opcode === 2) {
				if (this._fragmented) {
					cb(this.createError(RangeError, `invalid opcode ${this._opcode}`, true, 1002, "WS_ERR_INVALID_OPCODE"));
					return;
				}
				this._compressed = compressed;
			} else if (this._opcode > 7 && this._opcode < 11) {
				if (!this._fin) {
					cb(this.createError(RangeError, "FIN must be set", true, 1002, "WS_ERR_EXPECTED_FIN"));
					return;
				}
				if (compressed) {
					cb(this.createError(RangeError, "RSV1 must be clear", true, 1002, "WS_ERR_UNEXPECTED_RSV_1"));
					return;
				}
				if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
					cb(this.createError(RangeError, `invalid payload length ${this._payloadLength}`, true, 1002, "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"));
					return;
				}
			} else {
				cb(this.createError(RangeError, `invalid opcode ${this._opcode}`, true, 1002, "WS_ERR_INVALID_OPCODE"));
				return;
			}
			if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
			this._masked = (buf[1] & 128) === 128;
			if (this._isServer) {
				if (!this._masked) {
					cb(this.createError(RangeError, "MASK must be set", true, 1002, "WS_ERR_EXPECTED_MASK"));
					return;
				}
			} else if (this._masked) {
				cb(this.createError(RangeError, "MASK must be clear", true, 1002, "WS_ERR_UNEXPECTED_MASK"));
				return;
			}
			if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
			else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
			else this.haveLength(cb);
		}
		/**
		* Gets extended payload length (7+16).
		*
		* @param {Function} cb Callback
		* @private
		*/
		getPayloadLength16(cb) {
			if (this._bufferedBytes < 2) {
				this._loop = false;
				return;
			}
			this._payloadLength = this.consume(2).readUInt16BE(0);
			this.haveLength(cb);
		}
		/**
		* Gets extended payload length (7+64).
		*
		* @param {Function} cb Callback
		* @private
		*/
		getPayloadLength64(cb) {
			if (this._bufferedBytes < 8) {
				this._loop = false;
				return;
			}
			const buf = this.consume(8);
			const num = buf.readUInt32BE(0);
			if (num > Math.pow(2, 21) - 1) {
				cb(this.createError(RangeError, "Unsupported WebSocket frame: payload length > 2^53 - 1", false, 1009, "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"));
				return;
			}
			this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
			this.haveLength(cb);
		}
		/**
		* Payload length has been read.
		*
		* @param {Function} cb Callback
		* @private
		*/
		haveLength(cb) {
			if (this._payloadLength && this._opcode < 8) {
				this._totalPayloadLength += this._payloadLength;
				if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
					cb(this.createError(RangeError, "Max payload size exceeded", false, 1009, "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"));
					return;
				}
			}
			if (this._masked) this._state = GET_MASK;
			else this._state = GET_DATA;
		}
		/**
		* Reads mask bytes.
		*
		* @private
		*/
		getMask() {
			if (this._bufferedBytes < 4) {
				this._loop = false;
				return;
			}
			this._mask = this.consume(4);
			this._state = GET_DATA;
		}
		/**
		* Reads data bytes.
		*
		* @param {Function} cb Callback
		* @private
		*/
		getData(cb) {
			let data = EMPTY_BUFFER;
			if (this._payloadLength) {
				if (this._bufferedBytes < this._payloadLength) {
					this._loop = false;
					return;
				}
				data = this.consume(this._payloadLength);
				if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) unmask(data, this._mask);
			}
			if (this._opcode > 7) {
				this.controlMessage(data, cb);
				return;
			}
			if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
				cb(this.createError(RangeError, "Too many message fragments", false, 1008, "WS_ERR_TOO_MANY_BUFFERED_PARTS"));
				return;
			}
			if (this._compressed) {
				this._state = INFLATING;
				this.decompress(data, cb);
				return;
			}
			if (data.length) {
				this._messageLength = this._totalPayloadLength;
				this._fragments.push(data);
			}
			this.dataMessage(cb);
		}
		/**
		* Decompresses data.
		*
		* @param {Buffer} data Compressed data
		* @param {Function} cb Callback
		* @private
		*/
		decompress(data, cb) {
			this._extensions[PerMessageDeflate.extensionName].decompress(data, this._fin, (err, buf) => {
				if (err) return cb(err);
				if (buf.length) {
					this._messageLength += buf.length;
					if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
						cb(this.createError(RangeError, "Max payload size exceeded", false, 1009, "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"));
						return;
					}
					this._fragments.push(buf);
				}
				this.dataMessage(cb);
				if (this._state === GET_INFO) this.startLoop(cb);
			});
		}
		/**
		* Handles a data message.
		*
		* @param {Function} cb Callback
		* @private
		*/
		dataMessage(cb) {
			if (!this._fin) {
				this._state = GET_INFO;
				return;
			}
			const messageLength = this._messageLength;
			const fragments = this._fragments;
			this._totalPayloadLength = 0;
			this._messageLength = 0;
			this._fragmented = 0;
			this._numFragments = 0;
			this._fragments = [];
			if (this._opcode === 2) {
				let data;
				if (this._binaryType === "nodebuffer") data = concat(fragments, messageLength);
				else if (this._binaryType === "arraybuffer") data = toArrayBuffer(concat(fragments, messageLength));
				else if (this._binaryType === "blob") data = new Blob(fragments);
				else data = fragments;
				if (this._allowSynchronousEvents) {
					this.emit("message", data, true);
					this._state = GET_INFO;
				} else {
					this._state = DEFER_EVENT;
					setImmediate(() => {
						this.emit("message", data, true);
						this._state = GET_INFO;
						this.startLoop(cb);
					});
				}
			} else {
				const buf = concat(fragments, messageLength);
				if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
					cb(this.createError(Error, "invalid UTF-8 sequence", true, 1007, "WS_ERR_INVALID_UTF8"));
					return;
				}
				if (this._state === INFLATING || this._allowSynchronousEvents) {
					this.emit("message", buf, false);
					this._state = GET_INFO;
				} else {
					this._state = DEFER_EVENT;
					setImmediate(() => {
						this.emit("message", buf, false);
						this._state = GET_INFO;
						this.startLoop(cb);
					});
				}
			}
		}
		/**
		* Handles a control message.
		*
		* @param {Buffer} data Data to handle
		* @return {(Error|RangeError|undefined)} A possible error
		* @private
		*/
		controlMessage(data, cb) {
			if (this._opcode === 8) {
				if (data.length === 0) {
					this._loop = false;
					this.emit("conclude", 1005, EMPTY_BUFFER);
					this.end();
				} else {
					const code = data.readUInt16BE(0);
					if (!isValidStatusCode(code)) {
						cb(this.createError(RangeError, `invalid status code ${code}`, true, 1002, "WS_ERR_INVALID_CLOSE_CODE"));
						return;
					}
					const buf = new FastBuffer(data.buffer, data.byteOffset + 2, data.length - 2);
					if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
						cb(this.createError(Error, "invalid UTF-8 sequence", true, 1007, "WS_ERR_INVALID_UTF8"));
						return;
					}
					this._loop = false;
					this.emit("conclude", code, buf);
					this.end();
				}
				this._state = GET_INFO;
				return;
			}
			if (this._allowSynchronousEvents) {
				this.emit(this._opcode === 9 ? "ping" : "pong", data);
				this._state = GET_INFO;
			} else {
				this._state = DEFER_EVENT;
				setImmediate(() => {
					this.emit(this._opcode === 9 ? "ping" : "pong", data);
					this._state = GET_INFO;
					this.startLoop(cb);
				});
			}
		}
		/**
		* Builds an error object.
		*
		* @param {function(new:Error|RangeError)} ErrorCtor The error constructor
		* @param {String} message The error message
		* @param {Boolean} prefix Specifies whether or not to add a default prefix to
		*     `message`
		* @param {Number} statusCode The status code
		* @param {String} errorCode The exposed error code
		* @return {(Error|RangeError)} The error
		* @private
		*/
		createError(ErrorCtor, message, prefix, statusCode, errorCode) {
			this._loop = false;
			this._errored = true;
			const err = new ErrorCtor(prefix ? `Invalid WebSocket frame: ${message}` : message);
			Error.captureStackTrace(err, this.createError);
			err.code = errorCode;
			err[kStatusCode] = statusCode;
			return err;
		}
	};
	module.exports = Receiver;
}));
//#endregion
//#region node_modules/ws/lib/sender.js
var require_sender = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { Duplex: Duplex$3 } = __require("stream");
	var { randomFillSync } = __require("crypto");
	var { types: { isUint8Array } } = __require("util");
	var PerMessageDeflate = require_permessage_deflate();
	var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants$1();
	var { isBlob, isValidStatusCode } = require_validation();
	var { mask: applyMask, toBuffer } = require_buffer_util();
	var kByteLength = Symbol("kByteLength");
	var maskBuffer = Buffer.alloc(4);
	var RANDOM_POOL_SIZE = 8192;
	var randomPool;
	var randomPoolPointer = RANDOM_POOL_SIZE;
	var DEFAULT = 0;
	var DEFLATING = 1;
	var GET_BLOB_DATA = 2;
	module.exports = class Sender {
		/**
		* Creates a Sender instance.
		*
		* @param {Duplex} socket The connection socket
		* @param {Object} [extensions] An object containing the negotiated extensions
		* @param {Function} [generateMask] The function used to generate the masking
		*     key
		*/
		constructor(socket, extensions, generateMask) {
			this._extensions = extensions || {};
			if (generateMask) {
				this._generateMask = generateMask;
				this._maskBuffer = Buffer.alloc(4);
			}
			this._socket = socket;
			this._firstFragment = true;
			this._compress = false;
			this._bufferedBytes = 0;
			this._queue = [];
			this._state = DEFAULT;
			this.onerror = NOOP;
			this[kWebSocket] = void 0;
		}
		/**
		* Frames a piece of data according to the HyBi WebSocket protocol.
		*
		* @param {(Buffer|String)} data The data to frame
		* @param {Object} options Options object
		* @param {Boolean} [options.fin=false] Specifies whether or not to set the
		*     FIN bit
		* @param {Function} [options.generateMask] The function used to generate the
		*     masking key
		* @param {Boolean} [options.mask=false] Specifies whether or not to mask
		*     `data`
		* @param {Buffer} [options.maskBuffer] The buffer used to store the masking
		*     key
		* @param {Number} options.opcode The opcode
		* @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
		*     modified
		* @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
		*     RSV1 bit
		* @return {(Buffer|String)[]} The framed data
		* @public
		*/
		static frame(data, options) {
			let mask;
			let merge = false;
			let offset = 2;
			let skipMasking = false;
			if (options.mask) {
				mask = options.maskBuffer || maskBuffer;
				if (options.generateMask) options.generateMask(mask);
				else {
					if (randomPoolPointer === RANDOM_POOL_SIZE) {
						/* istanbul ignore else  */
						if (randomPool === void 0) randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
						randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
						randomPoolPointer = 0;
					}
					mask[0] = randomPool[randomPoolPointer++];
					mask[1] = randomPool[randomPoolPointer++];
					mask[2] = randomPool[randomPoolPointer++];
					mask[3] = randomPool[randomPoolPointer++];
				}
				skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
				offset = 6;
			}
			let dataLength;
			if (typeof data === "string") {
				if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) dataLength = options[kByteLength];
				else {
					data = Buffer.from(data);
					dataLength = data.length;
				}
			} else {
				dataLength = data.length;
				merge = options.mask && options.readOnly && !skipMasking;
			}
			let payloadLength = dataLength;
			if (dataLength >= 65536) {
				offset += 8;
				payloadLength = 127;
			} else if (dataLength > 125) {
				offset += 2;
				payloadLength = 126;
			}
			const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
			target[0] = options.fin ? options.opcode | 128 : options.opcode;
			if (options.rsv1) target[0] |= 64;
			target[1] = payloadLength;
			if (payloadLength === 126) target.writeUInt16BE(dataLength, 2);
			else if (payloadLength === 127) {
				target[2] = target[3] = 0;
				target.writeUIntBE(dataLength, 4, 6);
			}
			if (!options.mask) return [target, data];
			target[1] |= 128;
			target[offset - 4] = mask[0];
			target[offset - 3] = mask[1];
			target[offset - 2] = mask[2];
			target[offset - 1] = mask[3];
			if (skipMasking) return [target, data];
			if (merge) {
				applyMask(data, mask, target, offset, dataLength);
				return [target];
			}
			applyMask(data, mask, data, 0, dataLength);
			return [target, data];
		}
		/**
		* Sends a close message to the other peer.
		*
		* @param {Number} [code] The status code component of the body
		* @param {(String|Buffer)} [data] The message component of the body
		* @param {Boolean} [mask=false] Specifies whether or not to mask the message
		* @param {Function} [cb] Callback
		* @public
		*/
		close(code, data, mask, cb) {
			let buf;
			if (code === void 0) buf = EMPTY_BUFFER;
			else if (typeof code !== "number" || !isValidStatusCode(code)) throw new TypeError("First argument must be a valid error code number");
			else if (data === void 0 || !data.length) {
				buf = Buffer.allocUnsafe(2);
				buf.writeUInt16BE(code, 0);
			} else {
				const length = Buffer.byteLength(data);
				if (length > 123) throw new RangeError("The message must not be greater than 123 bytes");
				buf = Buffer.allocUnsafe(2 + length);
				buf.writeUInt16BE(code, 0);
				if (typeof data === "string") buf.write(data, 2);
				else if (isUint8Array(data)) buf.set(data, 2);
				else throw new TypeError("Second argument must be a string or a Uint8Array");
			}
			const options = {
				[kByteLength]: buf.length,
				fin: true,
				generateMask: this._generateMask,
				mask,
				maskBuffer: this._maskBuffer,
				opcode: 8,
				readOnly: false,
				rsv1: false
			};
			if (this._state !== DEFAULT) this.enqueue([
				this.dispatch,
				buf,
				false,
				options,
				cb
			]);
			else this.sendFrame(Sender.frame(buf, options), cb);
		}
		/**
		* Sends a ping message to the other peer.
		*
		* @param {*} data The message to send
		* @param {Boolean} [mask=false] Specifies whether or not to mask `data`
		* @param {Function} [cb] Callback
		* @public
		*/
		ping(data, mask, cb) {
			let byteLength;
			let readOnly;
			if (typeof data === "string") {
				byteLength = Buffer.byteLength(data);
				readOnly = false;
			} else if (isBlob(data)) {
				byteLength = data.size;
				readOnly = false;
			} else {
				data = toBuffer(data);
				byteLength = data.length;
				readOnly = toBuffer.readOnly;
			}
			if (byteLength > 125) throw new RangeError("The data size must not be greater than 125 bytes");
			const options = {
				[kByteLength]: byteLength,
				fin: true,
				generateMask: this._generateMask,
				mask,
				maskBuffer: this._maskBuffer,
				opcode: 9,
				readOnly,
				rsv1: false
			};
			if (isBlob(data)) {
				if (this._state !== DEFAULT) this.enqueue([
					this.getBlobData,
					data,
					false,
					options,
					cb
				]);
				else this.getBlobData(data, false, options, cb);
			} else if (this._state !== DEFAULT) this.enqueue([
				this.dispatch,
				data,
				false,
				options,
				cb
			]);
			else this.sendFrame(Sender.frame(data, options), cb);
		}
		/**
		* Sends a pong message to the other peer.
		*
		* @param {*} data The message to send
		* @param {Boolean} [mask=false] Specifies whether or not to mask `data`
		* @param {Function} [cb] Callback
		* @public
		*/
		pong(data, mask, cb) {
			let byteLength;
			let readOnly;
			if (typeof data === "string") {
				byteLength = Buffer.byteLength(data);
				readOnly = false;
			} else if (isBlob(data)) {
				byteLength = data.size;
				readOnly = false;
			} else {
				data = toBuffer(data);
				byteLength = data.length;
				readOnly = toBuffer.readOnly;
			}
			if (byteLength > 125) throw new RangeError("The data size must not be greater than 125 bytes");
			const options = {
				[kByteLength]: byteLength,
				fin: true,
				generateMask: this._generateMask,
				mask,
				maskBuffer: this._maskBuffer,
				opcode: 10,
				readOnly,
				rsv1: false
			};
			if (isBlob(data)) {
				if (this._state !== DEFAULT) this.enqueue([
					this.getBlobData,
					data,
					false,
					options,
					cb
				]);
				else this.getBlobData(data, false, options, cb);
			} else if (this._state !== DEFAULT) this.enqueue([
				this.dispatch,
				data,
				false,
				options,
				cb
			]);
			else this.sendFrame(Sender.frame(data, options), cb);
		}
		/**
		* Sends a data message to the other peer.
		*
		* @param {*} data The message to send
		* @param {Object} options Options object
		* @param {Boolean} [options.binary=false] Specifies whether `data` is binary
		*     or text
		* @param {Boolean} [options.compress=false] Specifies whether or not to
		*     compress `data`
		* @param {Boolean} [options.fin=false] Specifies whether the fragment is the
		*     last one
		* @param {Boolean} [options.mask=false] Specifies whether or not to mask
		*     `data`
		* @param {Function} [cb] Callback
		* @public
		*/
		send(data, options, cb) {
			const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
			let opcode = options.binary ? 2 : 1;
			let rsv1 = options.compress;
			let byteLength;
			let readOnly;
			if (typeof data === "string") {
				byteLength = Buffer.byteLength(data);
				readOnly = false;
			} else if (isBlob(data)) {
				byteLength = data.size;
				readOnly = false;
			} else {
				data = toBuffer(data);
				byteLength = data.length;
				readOnly = toBuffer.readOnly;
			}
			if (this._firstFragment) {
				this._firstFragment = false;
				if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) rsv1 = byteLength >= perMessageDeflate._threshold;
				this._compress = rsv1;
			} else {
				rsv1 = false;
				opcode = 0;
			}
			if (options.fin) this._firstFragment = true;
			const opts = {
				[kByteLength]: byteLength,
				fin: options.fin,
				generateMask: this._generateMask,
				mask: options.mask,
				maskBuffer: this._maskBuffer,
				opcode,
				readOnly,
				rsv1
			};
			if (isBlob(data)) {
				if (this._state !== DEFAULT) this.enqueue([
					this.getBlobData,
					data,
					this._compress,
					opts,
					cb
				]);
				else this.getBlobData(data, this._compress, opts, cb);
			} else if (this._state !== DEFAULT) this.enqueue([
				this.dispatch,
				data,
				this._compress,
				opts,
				cb
			]);
			else this.dispatch(data, this._compress, opts, cb);
		}
		/**
		* Gets the contents of a blob as binary data.
		*
		* @param {Blob} blob The blob
		* @param {Boolean} [compress=false] Specifies whether or not to compress
		*     the data
		* @param {Object} options Options object
		* @param {Boolean} [options.fin=false] Specifies whether or not to set the
		*     FIN bit
		* @param {Function} [options.generateMask] The function used to generate the
		*     masking key
		* @param {Boolean} [options.mask=false] Specifies whether or not to mask
		*     `data`
		* @param {Buffer} [options.maskBuffer] The buffer used to store the masking
		*     key
		* @param {Number} options.opcode The opcode
		* @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
		*     modified
		* @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
		*     RSV1 bit
		* @param {Function} [cb] Callback
		* @private
		*/
		getBlobData(blob, compress, options, cb) {
			this._bufferedBytes += options[kByteLength];
			this._state = GET_BLOB_DATA;
			blob.arrayBuffer().then((arrayBuffer) => {
				if (this._socket.destroyed) {
					const err = /* @__PURE__ */ new Error("The socket was closed while the blob was being read");
					process.nextTick(callCallbacks, this, err, cb);
					return;
				}
				this._bufferedBytes -= options[kByteLength];
				const data = toBuffer(arrayBuffer);
				if (!compress) {
					this._state = DEFAULT;
					this.sendFrame(Sender.frame(data, options), cb);
					this.dequeue();
				} else this.dispatch(data, compress, options, cb);
			}).catch((err) => {
				process.nextTick(onError, this, err, cb);
			});
		}
		/**
		* Dispatches a message.
		*
		* @param {(Buffer|String)} data The message to send
		* @param {Boolean} [compress=false] Specifies whether or not to compress
		*     `data`
		* @param {Object} options Options object
		* @param {Boolean} [options.fin=false] Specifies whether or not to set the
		*     FIN bit
		* @param {Function} [options.generateMask] The function used to generate the
		*     masking key
		* @param {Boolean} [options.mask=false] Specifies whether or not to mask
		*     `data`
		* @param {Buffer} [options.maskBuffer] The buffer used to store the masking
		*     key
		* @param {Number} options.opcode The opcode
		* @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
		*     modified
		* @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
		*     RSV1 bit
		* @param {Function} [cb] Callback
		* @private
		*/
		dispatch(data, compress, options, cb) {
			if (!compress) {
				this.sendFrame(Sender.frame(data, options), cb);
				return;
			}
			const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
			this._bufferedBytes += options[kByteLength];
			this._state = DEFLATING;
			perMessageDeflate.compress(data, options.fin, (_, buf) => {
				if (this._socket.destroyed) {
					const err = /* @__PURE__ */ new Error("The socket was closed while data was being compressed");
					callCallbacks(this, err, cb);
					return;
				}
				this._bufferedBytes -= options[kByteLength];
				this._state = DEFAULT;
				options.readOnly = false;
				this.sendFrame(Sender.frame(buf, options), cb);
				this.dequeue();
			});
		}
		/**
		* Executes queued send operations.
		*
		* @private
		*/
		dequeue() {
			while (this._state === DEFAULT && this._queue.length) {
				const params = this._queue.shift();
				this._bufferedBytes -= params[3][kByteLength];
				Reflect.apply(params[0], this, params.slice(1));
			}
		}
		/**
		* Enqueues a send operation.
		*
		* @param {Array} params Send operation parameters.
		* @private
		*/
		enqueue(params) {
			this._bufferedBytes += params[3][kByteLength];
			this._queue.push(params);
		}
		/**
		* Sends a frame.
		*
		* @param {(Buffer | String)[]} list The frame to send
		* @param {Function} [cb] Callback
		* @private
		*/
		sendFrame(list, cb) {
			if (list.length === 2) {
				this._socket.cork();
				this._socket.write(list[0]);
				this._socket.write(list[1], cb);
				this._socket.uncork();
			} else this._socket.write(list[0], cb);
		}
	};
	/**
	* Calls queued callbacks with an error.
	*
	* @param {Sender} sender The `Sender` instance
	* @param {Error} err The error to call the callbacks with
	* @param {Function} [cb] The first callback
	* @private
	*/
	function callCallbacks(sender, err, cb) {
		if (typeof cb === "function") cb(err);
		for (let i = 0; i < sender._queue.length; i++) {
			const params = sender._queue[i];
			const callback = params[params.length - 1];
			if (typeof callback === "function") callback(err);
		}
	}
	/**
	* Handles a `Sender` error.
	*
	* @param {Sender} sender The `Sender` instance
	* @param {Error} err The error
	* @param {Function} [cb] The first pending callback
	* @private
	*/
	function onError(sender, err, cb) {
		callCallbacks(sender, err, cb);
		sender.onerror(err);
	}
}));
//#endregion
//#region node_modules/ws/lib/event-target.js
var require_event_target = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { kForOnEventAttribute, kListener } = require_constants$1();
	var kCode = Symbol("kCode");
	var kData = Symbol("kData");
	var kError = Symbol("kError");
	var kMessage = Symbol("kMessage");
	var kReason = Symbol("kReason");
	var kTarget = Symbol("kTarget");
	var kType = Symbol("kType");
	var kWasClean = Symbol("kWasClean");
	/**
	* Class representing an event.
	*/
	var Event = class {
		/**
		* Create a new `Event`.
		*
		* @param {String} type The name of the event
		* @throws {TypeError} If the `type` argument is not specified
		*/
		constructor(type) {
			this[kTarget] = null;
			this[kType] = type;
		}
		/**
		* @type {*}
		*/
		get target() {
			return this[kTarget];
		}
		/**
		* @type {String}
		*/
		get type() {
			return this[kType];
		}
	};
	Object.defineProperty(Event.prototype, "target", { enumerable: true });
	Object.defineProperty(Event.prototype, "type", { enumerable: true });
	/**
	* Class representing a close event.
	*
	* @extends Event
	*/
	var CloseEvent = class extends Event {
		/**
		* Create a new `CloseEvent`.
		*
		* @param {String} type The name of the event
		* @param {Object} [options] A dictionary object that allows for setting
		*     attributes via object members of the same name
		* @param {Number} [options.code=0] The status code explaining why the
		*     connection was closed
		* @param {String} [options.reason=''] A human-readable string explaining why
		*     the connection was closed
		* @param {Boolean} [options.wasClean=false] Indicates whether or not the
		*     connection was cleanly closed
		*/
		constructor(type, options = {}) {
			super(type);
			this[kCode] = options.code === void 0 ? 0 : options.code;
			this[kReason] = options.reason === void 0 ? "" : options.reason;
			this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
		}
		/**
		* @type {Number}
		*/
		get code() {
			return this[kCode];
		}
		/**
		* @type {String}
		*/
		get reason() {
			return this[kReason];
		}
		/**
		* @type {Boolean}
		*/
		get wasClean() {
			return this[kWasClean];
		}
	};
	Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
	Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
	Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
	/**
	* Class representing an error event.
	*
	* @extends Event
	*/
	var ErrorEvent = class extends Event {
		/**
		* Create a new `ErrorEvent`.
		*
		* @param {String} type The name of the event
		* @param {Object} [options] A dictionary object that allows for setting
		*     attributes via object members of the same name
		* @param {*} [options.error=null] The error that generated this event
		* @param {String} [options.message=''] The error message
		*/
		constructor(type, options = {}) {
			super(type);
			this[kError] = options.error === void 0 ? null : options.error;
			this[kMessage] = options.message === void 0 ? "" : options.message;
		}
		/**
		* @type {*}
		*/
		get error() {
			return this[kError];
		}
		/**
		* @type {String}
		*/
		get message() {
			return this[kMessage];
		}
	};
	Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
	Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
	/**
	* Class representing a message event.
	*
	* @extends Event
	*/
	var MessageEvent = class extends Event {
		/**
		* Create a new `MessageEvent`.
		*
		* @param {String} type The name of the event
		* @param {Object} [options] A dictionary object that allows for setting
		*     attributes via object members of the same name
		* @param {*} [options.data=null] The message content
		*/
		constructor(type, options = {}) {
			super(type);
			this[kData] = options.data === void 0 ? null : options.data;
		}
		/**
		* @type {*}
		*/
		get data() {
			return this[kData];
		}
	};
	Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
	module.exports = {
		CloseEvent,
		ErrorEvent,
		Event,
		EventTarget: {
			/**
			* Register an event listener.
			*
			* @param {String} type A string representing the event type to listen for
			* @param {(Function|Object)} handler The listener to add
			* @param {Object} [options] An options object specifies characteristics about
			*     the event listener
			* @param {Boolean} [options.once=false] A `Boolean` indicating that the
			*     listener should be invoked at most once after being added. If `true`,
			*     the listener would be automatically removed when invoked.
			* @public
			*/
			addEventListener(type, handler, options = {}) {
				for (const listener of this.listeners(type)) if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) return;
				let wrapper;
				if (type === "message") wrapper = function onMessage(data, isBinary) {
					const event = new MessageEvent("message", { data: isBinary ? data : data.toString() });
					event[kTarget] = this;
					callListener(handler, this, event);
				};
				else if (type === "close") wrapper = function onClose(code, message) {
					const event = new CloseEvent("close", {
						code,
						reason: message.toString(),
						wasClean: this._closeFrameReceived && this._closeFrameSent
					});
					event[kTarget] = this;
					callListener(handler, this, event);
				};
				else if (type === "error") wrapper = function onError(error) {
					const event = new ErrorEvent("error", {
						error,
						message: error.message
					});
					event[kTarget] = this;
					callListener(handler, this, event);
				};
				else if (type === "open") wrapper = function onOpen() {
					const event = new Event("open");
					event[kTarget] = this;
					callListener(handler, this, event);
				};
				else return;
				wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
				wrapper[kListener] = handler;
				if (options.once) this.once(type, wrapper);
				else this.on(type, wrapper);
			},
			/**
			* Remove an event listener.
			*
			* @param {String} type A string representing the event type to remove
			* @param {(Function|Object)} handler The listener to remove
			* @public
			*/
			removeEventListener(type, handler) {
				for (const listener of this.listeners(type)) if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
					this.removeListener(type, listener);
					break;
				}
			}
		},
		MessageEvent
	};
	/**
	* Call an event listener
	*
	* @param {(Function|Object)} listener The listener to call
	* @param {*} thisArg The value to use as `this`` when calling the listener
	* @param {Event} event The event to pass to the listener
	* @private
	*/
	function callListener(listener, thisArg, event) {
		if (typeof listener === "object" && listener.handleEvent) listener.handleEvent.call(listener, event);
		else listener.call(thisArg, event);
	}
}));
//#endregion
//#region node_modules/ws/lib/extension.js
var require_extension = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { tokenChars } = require_validation();
	/**
	* Adds an offer to the map of extension offers or a parameter to the map of
	* parameters.
	*
	* @param {Object} dest The map of extension offers or parameters
	* @param {String} name The extension or parameter name
	* @param {(Object|Boolean|String)} elem The extension parameters or the
	*     parameter value
	* @private
	*/
	function push(dest, name, elem) {
		if (dest[name] === void 0) dest[name] = [elem];
		else dest[name].push(elem);
	}
	/**
	* Parses the `Sec-WebSocket-Extensions` header into an object.
	*
	* @param {String} header The field value of the header
	* @return {Object} The parsed object
	* @public
	*/
	function parse(header) {
		const offers = Object.create(null);
		let params = Object.create(null);
		let mustUnescape = false;
		let isEscaping = false;
		let inQuotes = false;
		let extensionName;
		let paramName;
		let start = -1;
		let code = -1;
		let end = -1;
		let i = 0;
		for (; i < header.length; i++) {
			code = header.charCodeAt(i);
			if (extensionName === void 0) {
				if (end === -1 && tokenChars[code] === 1) {
					if (start === -1) start = i;
				} else if (i !== 0 && (code === 32 || code === 9)) {
					if (end === -1 && start !== -1) end = i;
				} else if (code === 59 || code === 44) {
					if (start === -1) throw new SyntaxError(`Unexpected character at index ${i}`);
					if (end === -1) end = i;
					const name = header.slice(start, end);
					if (code === 44) {
						push(offers, name, params);
						params = Object.create(null);
					} else extensionName = name;
					start = end = -1;
				} else throw new SyntaxError(`Unexpected character at index ${i}`);
			} else if (paramName === void 0) {
				if (end === -1 && tokenChars[code] === 1) {
					if (start === -1) start = i;
				} else if (code === 32 || code === 9) {
					if (end === -1 && start !== -1) end = i;
				} else if (code === 59 || code === 44) {
					if (start === -1) throw new SyntaxError(`Unexpected character at index ${i}`);
					if (end === -1) end = i;
					push(params, header.slice(start, end), true);
					if (code === 44) {
						push(offers, extensionName, params);
						params = Object.create(null);
						extensionName = void 0;
					}
					start = end = -1;
				} else if (code === 61 && start !== -1 && end === -1) {
					paramName = header.slice(start, i);
					start = end = -1;
				} else throw new SyntaxError(`Unexpected character at index ${i}`);
			} else if (isEscaping) {
				if (tokenChars[code] !== 1) throw new SyntaxError(`Unexpected character at index ${i}`);
				if (start === -1) start = i;
				else if (!mustUnescape) mustUnescape = true;
				isEscaping = false;
			} else if (inQuotes) {
				if (tokenChars[code] === 1) {
					if (start === -1) start = i;
				} else if (code === 34 && start !== -1) {
					inQuotes = false;
					end = i;
				} else if (code === 92) isEscaping = true;
				else throw new SyntaxError(`Unexpected character at index ${i}`);
			} else if (code === 34 && header.charCodeAt(i - 1) === 61) inQuotes = true;
			else if (end === -1 && tokenChars[code] === 1) {
				if (start === -1) start = i;
			} else if (start !== -1 && (code === 32 || code === 9)) {
				if (end === -1) end = i;
			} else if (code === 59 || code === 44) {
				if (start === -1) throw new SyntaxError(`Unexpected character at index ${i}`);
				if (end === -1) end = i;
				let value = header.slice(start, end);
				if (mustUnescape) {
					value = value.replace(/\\/g, "");
					mustUnescape = false;
				}
				push(params, paramName, value);
				if (code === 44) {
					push(offers, extensionName, params);
					params = Object.create(null);
					extensionName = void 0;
				}
				paramName = void 0;
				start = end = -1;
			} else throw new SyntaxError(`Unexpected character at index ${i}`);
		}
		if (start === -1 || inQuotes || code === 32 || code === 9) throw new SyntaxError("Unexpected end of input");
		if (end === -1) end = i;
		const token = header.slice(start, end);
		if (extensionName === void 0) push(offers, token, params);
		else {
			if (paramName === void 0) push(params, token, true);
			else if (mustUnescape) push(params, paramName, token.replace(/\\/g, ""));
			else push(params, paramName, token);
			push(offers, extensionName, params);
		}
		return offers;
	}
	/**
	* Builds the `Sec-WebSocket-Extensions` header field value.
	*
	* @param {Object} extensions The map of extensions and parameters to format
	* @return {String} A string representing the given object
	* @public
	*/
	function format(extensions) {
		return Object.keys(extensions).map((extension) => {
			let configurations = extensions[extension];
			if (!Array.isArray(configurations)) configurations = [configurations];
			return configurations.map((params) => {
				return [extension].concat(Object.keys(params).map((k) => {
					let values = params[k];
					if (!Array.isArray(values)) values = [values];
					return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
				})).join("; ");
			}).join(", ");
		}).join(", ");
	}
	module.exports = {
		format,
		parse
	};
}));
//#endregion
//#region node_modules/ws/lib/websocket.js
var require_websocket = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var EventEmitter$1 = __require("events");
	var https = __require("https");
	var http$1 = __require("http");
	var net$2 = __require("net");
	var tls = __require("tls");
	var { randomBytes, createHash: createHash$1 } = __require("crypto");
	var { Duplex: Duplex$2, Readable } = __require("stream");
	var { URL: URL$1 } = __require("url");
	var PerMessageDeflate = require_permessage_deflate();
	var Receiver = require_receiver();
	var Sender = require_sender();
	var { isBlob } = require_validation();
	var { BINARY_TYPES, CLOSE_TIMEOUT, EMPTY_BUFFER, GUID, kForOnEventAttribute, kListener, kStatusCode, kWebSocket, NOOP } = require_constants$1();
	var { EventTarget: { addEventListener, removeEventListener } } = require_event_target();
	var { format, parse } = require_extension();
	var { toBuffer } = require_buffer_util();
	var kAborted = Symbol("kAborted");
	var protocolVersions = [8, 13];
	var readyStates = [
		"CONNECTING",
		"OPEN",
		"CLOSING",
		"CLOSED"
	];
	var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
	/**
	* Class representing a WebSocket.
	*
	* @extends EventEmitter
	*/
	var WebSocket = class WebSocket extends EventEmitter$1 {
		/**
		* Create a new `WebSocket`.
		*
		* @param {(String|URL)} address The URL to which to connect
		* @param {(String|String[])} [protocols] The subprotocols
		* @param {Object} [options] Connection options
		*/
		constructor(address, protocols, options) {
			super();
			this._binaryType = BINARY_TYPES[0];
			this._closeCode = 1006;
			this._closeFrameReceived = false;
			this._closeFrameSent = false;
			this._closeMessage = EMPTY_BUFFER;
			this._closeTimer = null;
			this._errorEmitted = false;
			this._extensions = {};
			this._paused = false;
			this._protocol = "";
			this._readyState = WebSocket.CONNECTING;
			this._receiver = null;
			this._sender = null;
			this._socket = null;
			if (address !== null) {
				this._bufferedAmount = 0;
				this._isServer = false;
				this._redirects = 0;
				if (protocols === void 0) {
					if (!options || options.protocols === void 0) protocols = [];
					else if (Array.isArray(options.protocols)) protocols = options.protocols;
					else protocols = [options.protocols];
				} else if (!Array.isArray(protocols)) {
					if (typeof protocols === "object" && protocols !== null) {
						options = protocols;
						if (options.protocols === void 0) protocols = [];
						else if (Array.isArray(options.protocols)) protocols = options.protocols;
						else protocols = [options.protocols];
					} else protocols = [protocols];
				}
				initAsClient(this, address, protocols, options);
			} else {
				this._autoPong = options.autoPong;
				this._closeTimeout = options.closeTimeout;
				this._isServer = true;
			}
		}
		/**
		* For historical reasons, the custom "nodebuffer" type is used by the default
		* instead of "blob".
		*
		* @type {String}
		*/
		get binaryType() {
			return this._binaryType;
		}
		set binaryType(type) {
			if (!BINARY_TYPES.includes(type)) return;
			this._binaryType = type;
			if (this._receiver) this._receiver._binaryType = type;
		}
		/**
		* @type {Number}
		*/
		get bufferedAmount() {
			if (!this._socket) return this._bufferedAmount;
			return this._socket._writableState.length + this._sender._bufferedBytes;
		}
		/**
		* @type {String}
		*/
		get extensions() {
			return Object.keys(this._extensions).join();
		}
		/**
		* @type {Boolean}
		*/
		get isPaused() {
			return this._paused;
		}
		/**
		* @type {Function}
		*/
		/* istanbul ignore next */
		get onclose() {
			return null;
		}
		/**
		* @type {Function}
		*/
		/* istanbul ignore next */
		get onerror() {
			return null;
		}
		/**
		* @type {Function}
		*/
		/* istanbul ignore next */
		get onopen() {
			return null;
		}
		/**
		* @type {Function}
		*/
		/* istanbul ignore next */
		get onmessage() {
			return null;
		}
		/**
		* @type {String}
		*/
		get protocol() {
			return this._protocol;
		}
		/**
		* @type {Number}
		*/
		get readyState() {
			return this._readyState;
		}
		/**
		* @type {String}
		*/
		get url() {
			return this._url;
		}
		/**
		* Set up the socket and the internal resources.
		*
		* @param {Duplex} socket The network socket between the server and client
		* @param {Buffer} head The first packet of the upgraded stream
		* @param {Object} options Options object
		* @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
		*     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
		*     multiple times in the same tick
		* @param {Function} [options.generateMask] The function used to generate the
		*     masking key
		* @param {Number} [options.maxBufferedChunks=0] The maximum number of
		*     buffered data chunks
		* @param {Number} [options.maxFragments=0] The maximum number of message
		*     fragments
		* @param {Number} [options.maxPayload=0] The maximum allowed message size
		* @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
		*     not to skip UTF-8 validation for text and close messages
		* @private
		*/
		setSocket(socket, head, options) {
			const receiver = new Receiver({
				allowSynchronousEvents: options.allowSynchronousEvents,
				binaryType: this.binaryType,
				extensions: this._extensions,
				isServer: this._isServer,
				maxBufferedChunks: options.maxBufferedChunks,
				maxFragments: options.maxFragments,
				maxPayload: options.maxPayload,
				skipUTF8Validation: options.skipUTF8Validation
			});
			const sender = new Sender(socket, this._extensions, options.generateMask);
			this._receiver = receiver;
			this._sender = sender;
			this._socket = socket;
			receiver[kWebSocket] = this;
			sender[kWebSocket] = this;
			socket[kWebSocket] = this;
			receiver.on("conclude", receiverOnConclude);
			receiver.on("drain", receiverOnDrain);
			receiver.on("error", receiverOnError);
			receiver.on("message", receiverOnMessage);
			receiver.on("ping", receiverOnPing);
			receiver.on("pong", receiverOnPong);
			sender.onerror = senderOnError;
			if (socket.setTimeout) socket.setTimeout(0);
			if (socket.setNoDelay) socket.setNoDelay();
			if (head.length > 0) socket.unshift(head);
			socket.on("close", socketOnClose);
			socket.on("data", socketOnData);
			socket.on("end", socketOnEnd);
			socket.on("error", socketOnError);
			this._readyState = WebSocket.OPEN;
			this.emit("open");
		}
		/**
		* Emit the `'close'` event.
		*
		* @private
		*/
		emitClose() {
			if (!this._socket) {
				this._readyState = WebSocket.CLOSED;
				this.emit("close", this._closeCode, this._closeMessage);
				return;
			}
			if (this._extensions[PerMessageDeflate.extensionName]) this._extensions[PerMessageDeflate.extensionName].cleanup();
			this._receiver.removeAllListeners();
			this._readyState = WebSocket.CLOSED;
			this.emit("close", this._closeCode, this._closeMessage);
		}
		/**
		* Start a closing handshake.
		*
		*          +----------+   +-----------+   +----------+
		*     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
		*    |     +----------+   +-----------+   +----------+     |
		*          +----------+   +-----------+         |
		* CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
		*          +----------+   +-----------+   |
		*    |           |                        |   +---+        |
		*                +------------------------+-->|fin| - - - -
		*    |         +---+                      |   +---+
		*     - - - - -|fin|<---------------------+
		*              +---+
		*
		* @param {Number} [code] Status code explaining why the connection is closing
		* @param {(String|Buffer)} [data] The reason why the connection is
		*     closing
		* @public
		*/
		close(code, data) {
			if (this.readyState === WebSocket.CLOSED) return;
			if (this.readyState === WebSocket.CONNECTING) {
				abortHandshake(this, this._req, "WebSocket was closed before the connection was established");
				return;
			}
			if (this.readyState === WebSocket.CLOSING) {
				if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) this._socket.end();
				return;
			}
			this._sender.close(code, data, !this._isServer, (err) => {
				if (err) return;
				this._closeFrameSent = true;
				if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) this._socket.end();
			});
			this._readyState = WebSocket.CLOSING;
			setCloseTimer(this);
		}
		/**
		* Pause the socket.
		*
		* @public
		*/
		pause() {
			if (this.readyState === WebSocket.CONNECTING || this.readyState === WebSocket.CLOSED) return;
			this._paused = true;
			this._socket.pause();
		}
		/**
		* Send a ping.
		*
		* @param {*} [data] The data to send
		* @param {Boolean} [mask] Indicates whether or not to mask `data`
		* @param {Function} [cb] Callback which is executed when the ping is sent
		* @public
		*/
		ping(data, mask, cb) {
			if (this.readyState === WebSocket.CONNECTING) throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
			if (typeof data === "function") {
				cb = data;
				data = mask = void 0;
			} else if (typeof mask === "function") {
				cb = mask;
				mask = void 0;
			}
			if (typeof data === "number") data = data.toString();
			if (this.readyState !== WebSocket.OPEN) {
				sendAfterClose(this, data, cb);
				return;
			}
			if (mask === void 0) mask = !this._isServer;
			this._sender.ping(data || EMPTY_BUFFER, mask, cb);
		}
		/**
		* Send a pong.
		*
		* @param {*} [data] The data to send
		* @param {Boolean} [mask] Indicates whether or not to mask `data`
		* @param {Function} [cb] Callback which is executed when the pong is sent
		* @public
		*/
		pong(data, mask, cb) {
			if (this.readyState === WebSocket.CONNECTING) throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
			if (typeof data === "function") {
				cb = data;
				data = mask = void 0;
			} else if (typeof mask === "function") {
				cb = mask;
				mask = void 0;
			}
			if (typeof data === "number") data = data.toString();
			if (this.readyState !== WebSocket.OPEN) {
				sendAfterClose(this, data, cb);
				return;
			}
			if (mask === void 0) mask = !this._isServer;
			this._sender.pong(data || EMPTY_BUFFER, mask, cb);
		}
		/**
		* Resume the socket.
		*
		* @public
		*/
		resume() {
			if (this.readyState === WebSocket.CONNECTING || this.readyState === WebSocket.CLOSED) return;
			this._paused = false;
			if (!this._receiver._writableState.needDrain) this._socket.resume();
		}
		/**
		* Send a data message.
		*
		* @param {*} data The message to send
		* @param {Object} [options] Options object
		* @param {Boolean} [options.binary] Specifies whether `data` is binary or
		*     text
		* @param {Boolean} [options.compress] Specifies whether or not to compress
		*     `data`
		* @param {Boolean} [options.fin=true] Specifies whether the fragment is the
		*     last one
		* @param {Boolean} [options.mask] Specifies whether or not to mask `data`
		* @param {Function} [cb] Callback which is executed when data is written out
		* @public
		*/
		send(data, options, cb) {
			if (this.readyState === WebSocket.CONNECTING) throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
			if (typeof options === "function") {
				cb = options;
				options = {};
			}
			if (typeof data === "number") data = data.toString();
			if (this.readyState !== WebSocket.OPEN) {
				sendAfterClose(this, data, cb);
				return;
			}
			const opts = {
				binary: typeof data !== "string",
				mask: !this._isServer,
				compress: true,
				fin: true,
				...options
			};
			if (!this._extensions[PerMessageDeflate.extensionName]) opts.compress = false;
			this._sender.send(data || EMPTY_BUFFER, opts, cb);
		}
		/**
		* Forcibly close the connection.
		*
		* @public
		*/
		terminate() {
			if (this.readyState === WebSocket.CLOSED) return;
			if (this.readyState === WebSocket.CONNECTING) {
				abortHandshake(this, this._req, "WebSocket was closed before the connection was established");
				return;
			}
			if (this._socket) {
				this._readyState = WebSocket.CLOSING;
				this._socket.destroy();
			}
		}
	};
	/**
	* @constant {Number} CONNECTING
	* @memberof WebSocket
	*/
	Object.defineProperty(WebSocket, "CONNECTING", {
		enumerable: true,
		value: readyStates.indexOf("CONNECTING")
	});
	/**
	* @constant {Number} CONNECTING
	* @memberof WebSocket.prototype
	*/
	Object.defineProperty(WebSocket.prototype, "CONNECTING", {
		enumerable: true,
		value: readyStates.indexOf("CONNECTING")
	});
	/**
	* @constant {Number} OPEN
	* @memberof WebSocket
	*/
	Object.defineProperty(WebSocket, "OPEN", {
		enumerable: true,
		value: readyStates.indexOf("OPEN")
	});
	/**
	* @constant {Number} OPEN
	* @memberof WebSocket.prototype
	*/
	Object.defineProperty(WebSocket.prototype, "OPEN", {
		enumerable: true,
		value: readyStates.indexOf("OPEN")
	});
	/**
	* @constant {Number} CLOSING
	* @memberof WebSocket
	*/
	Object.defineProperty(WebSocket, "CLOSING", {
		enumerable: true,
		value: readyStates.indexOf("CLOSING")
	});
	/**
	* @constant {Number} CLOSING
	* @memberof WebSocket.prototype
	*/
	Object.defineProperty(WebSocket.prototype, "CLOSING", {
		enumerable: true,
		value: readyStates.indexOf("CLOSING")
	});
	/**
	* @constant {Number} CLOSED
	* @memberof WebSocket
	*/
	Object.defineProperty(WebSocket, "CLOSED", {
		enumerable: true,
		value: readyStates.indexOf("CLOSED")
	});
	/**
	* @constant {Number} CLOSED
	* @memberof WebSocket.prototype
	*/
	Object.defineProperty(WebSocket.prototype, "CLOSED", {
		enumerable: true,
		value: readyStates.indexOf("CLOSED")
	});
	[
		"binaryType",
		"bufferedAmount",
		"extensions",
		"isPaused",
		"protocol",
		"readyState",
		"url"
	].forEach((property) => {
		Object.defineProperty(WebSocket.prototype, property, { enumerable: true });
	});
	[
		"open",
		"error",
		"close",
		"message"
	].forEach((method) => {
		Object.defineProperty(WebSocket.prototype, `on${method}`, {
			enumerable: true,
			get() {
				for (const listener of this.listeners(method)) if (listener[kForOnEventAttribute]) return listener[kListener];
				return null;
			},
			set(handler) {
				for (const listener of this.listeners(method)) if (listener[kForOnEventAttribute]) {
					this.removeListener(method, listener);
					break;
				}
				if (typeof handler !== "function") return;
				this.addEventListener(method, handler, { [kForOnEventAttribute]: true });
			}
		});
	});
	WebSocket.prototype.addEventListener = addEventListener;
	WebSocket.prototype.removeEventListener = removeEventListener;
	module.exports = WebSocket;
	/**
	* Initialize a WebSocket client.
	*
	* @param {WebSocket} websocket The client to initialize
	* @param {(String|URL)} address The URL to which to connect
	* @param {Array} protocols The subprotocols
	* @param {Object} [options] Connection options
	* @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether any
	*     of the `'message'`, `'ping'`, and `'pong'` events can be emitted multiple
	*     times in the same tick
	* @param {Boolean} [options.autoPong=true] Specifies whether or not to
	*     automatically send a pong in response to a ping
	* @param {Number} [options.closeTimeout=30000] Duration in milliseconds to wait
	*     for the closing handshake to finish after `websocket.close()` is called
	* @param {Function} [options.finishRequest] A function which can be used to
	*     customize the headers of each http request before it is sent
	* @param {Boolean} [options.followRedirects=false] Whether or not to follow
	*     redirects
	* @param {Function} [options.generateMask] The function used to generate the
	*     masking key
	* @param {Number} [options.handshakeTimeout] Timeout in milliseconds for the
	*     handshake request
	* @param {Number} [options.maxBufferedChunks=262144] The maximum number of
	*     buffered data chunks
	* @param {Number} [options.maxFragments=16384] The maximum number of message
	*     fragments
	* @param {Number} [options.maxPayload=104857600] The maximum allowed message
	*     size
	* @param {Number} [options.maxRedirects=10] The maximum number of redirects
	*     allowed
	* @param {String} [options.origin] Value of the `Origin` or
	*     `Sec-WebSocket-Origin` header
	* @param {(Boolean|Object)} [options.perMessageDeflate=true] Enable/disable
	*     permessage-deflate
	* @param {Number} [options.protocolVersion=13] Value of the
	*     `Sec-WebSocket-Version` header
	* @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
	*     not to skip UTF-8 validation for text and close messages
	* @private
	*/
	function initAsClient(websocket, address, protocols, options) {
		const opts = {
			allowSynchronousEvents: true,
			autoPong: true,
			closeTimeout: CLOSE_TIMEOUT,
			protocolVersion: protocolVersions[1],
			maxBufferedChunks: 262144,
			maxFragments: 16384,
			maxPayload: 104857600,
			skipUTF8Validation: false,
			perMessageDeflate: true,
			followRedirects: false,
			maxRedirects: 10,
			...options,
			socketPath: void 0,
			hostname: void 0,
			protocol: void 0,
			protocols: void 0,
			timeout: void 0,
			method: "GET",
			host: void 0,
			path: void 0,
			port: void 0
		};
		websocket._autoPong = opts.autoPong;
		websocket._closeTimeout = opts.closeTimeout;
		if (!protocolVersions.includes(opts.protocolVersion)) throw new RangeError(`Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`);
		let parsedUrl;
		if (address instanceof URL$1) parsedUrl = address;
		else try {
			parsedUrl = new URL$1(address);
		} catch {
			throw new SyntaxError(`Invalid URL: ${address}`);
		}
		if (parsedUrl.protocol === "http:") parsedUrl.protocol = "ws:";
		else if (parsedUrl.protocol === "https:") parsedUrl.protocol = "wss:";
		websocket._url = parsedUrl.href;
		const isSecure = parsedUrl.protocol === "wss:";
		const isIpcUrl = parsedUrl.protocol === "ws+unix:";
		let invalidUrlMessage;
		if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) invalidUrlMessage = "The URL's protocol must be one of \"ws:\", \"wss:\", \"http:\", \"https:\", or \"ws+unix:\"";
		else if (isIpcUrl && !parsedUrl.pathname) invalidUrlMessage = "The URL's pathname is empty";
		else if (parsedUrl.hash) invalidUrlMessage = "The URL contains a fragment identifier";
		if (invalidUrlMessage) {
			const err = new SyntaxError(invalidUrlMessage);
			if (websocket._redirects === 0) throw err;
			else {
				emitErrorAndClose(websocket, err);
				return;
			}
		}
		const defaultPort = isSecure ? 443 : 80;
		const key = randomBytes(16).toString("base64");
		const request = isSecure ? https.request : http$1.request;
		const protocolSet = /* @__PURE__ */ new Set();
		let perMessageDeflate;
		opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
		opts.defaultPort = opts.defaultPort || defaultPort;
		opts.port = parsedUrl.port || defaultPort;
		opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
		opts.headers = {
			...opts.headers,
			"Sec-WebSocket-Version": opts.protocolVersion,
			"Sec-WebSocket-Key": key,
			Connection: "Upgrade",
			Upgrade: "websocket"
		};
		opts.path = parsedUrl.pathname + parsedUrl.search;
		opts.timeout = opts.handshakeTimeout;
		if (opts.perMessageDeflate) {
			perMessageDeflate = new PerMessageDeflate({
				...opts.perMessageDeflate,
				isServer: false,
				maxPayload: opts.maxPayload
			});
			opts.headers["Sec-WebSocket-Extensions"] = format({ [PerMessageDeflate.extensionName]: perMessageDeflate.offer() });
		}
		if (protocols.length) {
			for (const protocol of protocols) {
				if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) throw new SyntaxError("An invalid or duplicated subprotocol was specified");
				protocolSet.add(protocol);
			}
			opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
		}
		if (opts.origin) {
			if (opts.protocolVersion < 13) opts.headers["Sec-WebSocket-Origin"] = opts.origin;
			else opts.headers.Origin = opts.origin;
		}
		if (parsedUrl.username || parsedUrl.password) opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
		if (isIpcUrl) {
			const parts = opts.path.split(":");
			opts.socketPath = parts[0];
			opts.path = parts[1];
		}
		let req;
		if (opts.followRedirects) {
			if (websocket._redirects === 0) {
				websocket._originalIpc = isIpcUrl;
				websocket._originalSecure = isSecure;
				websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
				const headers = options && options.headers;
				options = {
					...options,
					headers: {}
				};
				if (headers) for (const [key, value] of Object.entries(headers)) options.headers[key.toLowerCase()] = value;
			} else if (websocket.listenerCount("redirect") === 0) {
				const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
				if (!isSameHost || websocket._originalSecure && !isSecure) {
					delete opts.headers.authorization;
					delete opts.headers.cookie;
					if (!isSameHost) delete opts.headers.host;
					opts.auth = void 0;
				}
			}
			if (opts.auth && !options.headers.authorization) options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
			req = websocket._req = request(opts);
			if (websocket._redirects) websocket.emit("redirect", websocket.url, req);
		} else req = websocket._req = request(opts);
		if (opts.timeout) req.on("timeout", () => {
			abortHandshake(websocket, req, "Opening handshake has timed out");
		});
		req.on("error", (err) => {
			if (req === null || req[kAborted]) return;
			req = websocket._req = null;
			emitErrorAndClose(websocket, err);
		});
		req.on("response", (res) => {
			const location = res.headers.location;
			const statusCode = res.statusCode;
			if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
				if (++websocket._redirects > opts.maxRedirects) {
					abortHandshake(websocket, req, "Maximum redirects exceeded");
					return;
				}
				req.abort();
				let addr;
				try {
					addr = new URL$1(location, address);
				} catch (e) {
					emitErrorAndClose(websocket, /* @__PURE__ */ new SyntaxError(`Invalid URL: ${location}`));
					return;
				}
				initAsClient(websocket, addr, protocols, options);
			} else if (!websocket.emit("unexpected-response", req, res)) abortHandshake(websocket, req, `Unexpected server response: ${res.statusCode}`);
		});
		req.on("upgrade", (res, socket, head) => {
			websocket.emit("upgrade", res);
			if (websocket.readyState !== WebSocket.CONNECTING) return;
			req = websocket._req = null;
			const upgrade = res.headers.upgrade;
			if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
				abortHandshake(websocket, socket, "Invalid Upgrade header");
				return;
			}
			const digest = createHash$1("sha1").update(key + GUID).digest("base64");
			if (res.headers["sec-websocket-accept"] !== digest) {
				abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
				return;
			}
			const serverProt = res.headers["sec-websocket-protocol"];
			let protError;
			if (serverProt !== void 0) {
				if (!protocolSet.size) protError = "Server sent a subprotocol but none was requested";
				else if (!protocolSet.has(serverProt)) protError = "Server sent an invalid subprotocol";
			} else if (protocolSet.size) protError = "Server sent no subprotocol";
			if (protError) {
				abortHandshake(websocket, socket, protError);
				return;
			}
			if (serverProt) websocket._protocol = serverProt;
			const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
			if (secWebSocketExtensions !== void 0) {
				if (!perMessageDeflate) {
					abortHandshake(websocket, socket, "Server sent a Sec-WebSocket-Extensions header but no extension was requested");
					return;
				}
				let extensions;
				try {
					extensions = parse(secWebSocketExtensions);
				} catch (err) {
					abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Extensions header");
					return;
				}
				const extensionNames = Object.keys(extensions);
				if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate.extensionName) {
					abortHandshake(websocket, socket, "Server indicated an extension that was not requested");
					return;
				}
				try {
					perMessageDeflate.accept(extensions[PerMessageDeflate.extensionName]);
				} catch (err) {
					abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Extensions header");
					return;
				}
				websocket._extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
			}
			websocket.setSocket(socket, head, {
				allowSynchronousEvents: opts.allowSynchronousEvents,
				generateMask: opts.generateMask,
				maxBufferedChunks: opts.maxBufferedChunks,
				maxFragments: opts.maxFragments,
				maxPayload: opts.maxPayload,
				skipUTF8Validation: opts.skipUTF8Validation
			});
		});
		if (opts.finishRequest) opts.finishRequest(req, websocket);
		else req.end();
	}
	/**
	* Emit the `'error'` and `'close'` events.
	*
	* @param {WebSocket} websocket The WebSocket instance
	* @param {Error} The error to emit
	* @private
	*/
	function emitErrorAndClose(websocket, err) {
		websocket._readyState = WebSocket.CLOSING;
		websocket._errorEmitted = true;
		websocket.emit("error", err);
		websocket.emitClose();
	}
	/**
	* Create a `net.Socket` and initiate a connection.
	*
	* @param {Object} options Connection options
	* @return {net.Socket} The newly created socket used to start the connection
	* @private
	*/
	function netConnect(options) {
		options.path = options.socketPath;
		return net$2.connect(options);
	}
	/**
	* Create a `tls.TLSSocket` and initiate a connection.
	*
	* @param {Object} options Connection options
	* @return {tls.TLSSocket} The newly created socket used to start the connection
	* @private
	*/
	function tlsConnect(options) {
		options.path = void 0;
		if (!options.servername && options.servername !== "") options.servername = net$2.isIP(options.host) ? "" : options.host;
		return tls.connect(options);
	}
	/**
	* Abort the handshake and emit an error.
	*
	* @param {WebSocket} websocket The WebSocket instance
	* @param {(http.ClientRequest|net.Socket|tls.Socket)} stream The request to
	*     abort or the socket to destroy
	* @param {String} message The error message
	* @private
	*/
	function abortHandshake(websocket, stream, message) {
		websocket._readyState = WebSocket.CLOSING;
		const err = new Error(message);
		Error.captureStackTrace(err, abortHandshake);
		if (stream.setHeader) {
			stream[kAborted] = true;
			stream.abort();
			if (stream.socket && !stream.socket.destroyed) stream.socket.destroy();
			process.nextTick(emitErrorAndClose, websocket, err);
		} else {
			stream.destroy(err);
			stream.once("error", websocket.emit.bind(websocket, "error"));
			stream.once("close", websocket.emitClose.bind(websocket));
		}
	}
	/**
	* Handle cases where the `ping()`, `pong()`, or `send()` methods are called
	* when the `readyState` attribute is `CLOSING` or `CLOSED`.
	*
	* @param {WebSocket} websocket The WebSocket instance
	* @param {*} [data] The data to send
	* @param {Function} [cb] Callback
	* @private
	*/
	function sendAfterClose(websocket, data, cb) {
		if (data) {
			const length = isBlob(data) ? data.size : toBuffer(data).length;
			if (websocket._socket) websocket._sender._bufferedBytes += length;
			else websocket._bufferedAmount += length;
		}
		if (cb) {
			const err = /* @__PURE__ */ new Error(`WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`);
			process.nextTick(cb, err);
		}
	}
	/**
	* The listener of the `Receiver` `'conclude'` event.
	*
	* @param {Number} code The status code
	* @param {Buffer} reason The reason for closing
	* @private
	*/
	function receiverOnConclude(code, reason) {
		const websocket = this[kWebSocket];
		websocket._closeFrameReceived = true;
		websocket._closeMessage = reason;
		websocket._closeCode = code;
		if (websocket._socket[kWebSocket] === void 0) return;
		websocket._socket.removeListener("data", socketOnData);
		process.nextTick(resume, websocket._socket);
		if (code === 1005) websocket.close();
		else websocket.close(code, reason);
	}
	/**
	* The listener of the `Receiver` `'drain'` event.
	*
	* @private
	*/
	function receiverOnDrain() {
		const websocket = this[kWebSocket];
		if (!websocket.isPaused) websocket._socket.resume();
	}
	/**
	* The listener of the `Receiver` `'error'` event.
	*
	* @param {(RangeError|Error)} err The emitted error
	* @private
	*/
	function receiverOnError(err) {
		const websocket = this[kWebSocket];
		if (websocket._socket[kWebSocket] !== void 0) {
			websocket._socket.removeListener("data", socketOnData);
			process.nextTick(resume, websocket._socket);
			websocket.close(err[kStatusCode]);
		}
		if (!websocket._errorEmitted) {
			websocket._errorEmitted = true;
			websocket.emit("error", err);
		}
	}
	/**
	* The listener of the `Receiver` `'finish'` event.
	*
	* @private
	*/
	function receiverOnFinish() {
		this[kWebSocket].emitClose();
	}
	/**
	* The listener of the `Receiver` `'message'` event.
	*
	* @param {Buffer|ArrayBuffer|Buffer[])} data The message
	* @param {Boolean} isBinary Specifies whether the message is binary or not
	* @private
	*/
	function receiverOnMessage(data, isBinary) {
		this[kWebSocket].emit("message", data, isBinary);
	}
	/**
	* The listener of the `Receiver` `'ping'` event.
	*
	* @param {Buffer} data The data included in the ping frame
	* @private
	*/
	function receiverOnPing(data) {
		const websocket = this[kWebSocket];
		if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
		websocket.emit("ping", data);
	}
	/**
	* The listener of the `Receiver` `'pong'` event.
	*
	* @param {Buffer} data The data included in the pong frame
	* @private
	*/
	function receiverOnPong(data) {
		this[kWebSocket].emit("pong", data);
	}
	/**
	* Resume a readable stream
	*
	* @param {Readable} stream The readable stream
	* @private
	*/
	function resume(stream) {
		stream.resume();
	}
	/**
	* The `Sender` error event handler.
	*
	* @param {Error} The error
	* @private
	*/
	function senderOnError(err) {
		const websocket = this[kWebSocket];
		if (websocket.readyState === WebSocket.CLOSED) return;
		if (websocket.readyState === WebSocket.OPEN) {
			websocket._readyState = WebSocket.CLOSING;
			setCloseTimer(websocket);
		}
		this._socket.end();
		if (!websocket._errorEmitted) {
			websocket._errorEmitted = true;
			websocket.emit("error", err);
		}
	}
	/**
	* Set a timer to destroy the underlying raw socket of a WebSocket.
	*
	* @param {WebSocket} websocket The WebSocket instance
	* @private
	*/
	function setCloseTimer(websocket) {
		websocket._closeTimer = setTimeout(websocket._socket.destroy.bind(websocket._socket), websocket._closeTimeout);
	}
	/**
	* The listener of the socket `'close'` event.
	*
	* @private
	*/
	function socketOnClose() {
		const websocket = this[kWebSocket];
		this.removeListener("close", socketOnClose);
		this.removeListener("data", socketOnData);
		this.removeListener("end", socketOnEnd);
		websocket._readyState = WebSocket.CLOSING;
		if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
			const chunk = this.read(this._readableState.length);
			websocket._receiver.write(chunk);
		}
		websocket._receiver.end();
		this[kWebSocket] = void 0;
		clearTimeout(websocket._closeTimer);
		if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) websocket.emitClose();
		else {
			websocket._receiver.on("error", receiverOnFinish);
			websocket._receiver.on("finish", receiverOnFinish);
		}
	}
	/**
	* The listener of the socket `'data'` event.
	*
	* @param {Buffer} chunk A chunk of data
	* @private
	*/
	function socketOnData(chunk) {
		if (!this[kWebSocket]._receiver.write(chunk)) this.pause();
	}
	/**
	* The listener of the socket `'end'` event.
	*
	* @private
	*/
	function socketOnEnd() {
		const websocket = this[kWebSocket];
		websocket._readyState = WebSocket.CLOSING;
		websocket._receiver.end();
		this.end();
	}
	/**
	* The listener of the socket `'error'` event.
	*
	* @private
	*/
	function socketOnError() {
		const websocket = this[kWebSocket];
		this.removeListener("error", socketOnError);
		this.on("error", NOOP);
		if (websocket) {
			websocket._readyState = WebSocket.CLOSING;
			this.destroy();
		}
	}
}));
//#endregion
//#region node_modules/ws/lib/stream.js
var require_stream = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	require_websocket();
	var { Duplex: Duplex$1 } = __require("stream");
	/**
	* Emits the `'close'` event on a stream.
	*
	* @param {Duplex} stream The stream.
	* @private
	*/
	function emitClose(stream) {
		stream.emit("close");
	}
	/**
	* The listener of the `'end'` event.
	*
	* @private
	*/
	function duplexOnEnd() {
		if (!this.destroyed && this._writableState.finished) this.destroy();
	}
	/**
	* The listener of the `'error'` event.
	*
	* @param {Error} err The error
	* @private
	*/
	function duplexOnError(err) {
		this.removeListener("error", duplexOnError);
		this.destroy();
		if (this.listenerCount("error") === 0) this.emit("error", err);
	}
	/**
	* Wraps a `WebSocket` in a duplex stream.
	*
	* @param {WebSocket} ws The `WebSocket` to wrap
	* @param {Object} [options] The options for the `Duplex` constructor
	* @return {Duplex} The duplex stream
	* @public
	*/
	function createWebSocketStream(ws, options) {
		let terminateOnDestroy = true;
		const duplex = new Duplex$1({
			...options,
			autoDestroy: false,
			emitClose: false,
			objectMode: false,
			writableObjectMode: false
		});
		ws.on("message", function message(msg, isBinary) {
			const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
			if (!duplex.push(data)) ws.pause();
		});
		ws.once("error", function error(err) {
			if (duplex.destroyed) return;
			terminateOnDestroy = false;
			duplex.destroy(err);
		});
		ws.once("close", function close() {
			if (duplex.destroyed) return;
			duplex.push(null);
		});
		duplex._destroy = function(err, callback) {
			if (ws.readyState === ws.CLOSED) {
				callback(err);
				process.nextTick(emitClose, duplex);
				return;
			}
			let called = false;
			ws.once("error", function error(err) {
				called = true;
				callback(err);
			});
			ws.once("close", function close() {
				if (!called) callback(err);
				process.nextTick(emitClose, duplex);
			});
			if (terminateOnDestroy) ws.terminate();
		};
		duplex._final = function(callback) {
			if (ws.readyState === ws.CONNECTING) {
				ws.once("open", function open() {
					duplex._final(callback);
				});
				return;
			}
			if (ws._socket === null) return;
			if (ws._socket._writableState.finished) {
				callback();
				if (duplex._readableState.endEmitted) duplex.destroy();
			} else {
				ws._socket.once("finish", function finish() {
					callback();
				});
				ws.close();
			}
		};
		duplex._read = function() {
			if (ws.isPaused) ws.resume();
		};
		duplex._write = function(chunk, encoding, callback) {
			if (ws.readyState === ws.CONNECTING) {
				ws.once("open", function open() {
					duplex._write(chunk, encoding, callback);
				});
				return;
			}
			ws.send(chunk, callback);
		};
		duplex.on("end", duplexOnEnd);
		duplex.on("error", duplexOnError);
		return duplex;
	}
	module.exports = createWebSocketStream;
}));
//#endregion
//#region node_modules/ws/lib/subprotocol.js
var require_subprotocol = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { tokenChars } = require_validation();
	/**
	* Parses the `Sec-WebSocket-Protocol` header into a set of subprotocol names.
	*
	* @param {String} header The field value of the header
	* @return {Set} The subprotocol names
	* @public
	*/
	function parse(header) {
		const protocols = /* @__PURE__ */ new Set();
		let start = -1;
		let end = -1;
		let i = 0;
		for (; i < header.length; i++) {
			const code = header.charCodeAt(i);
			if (end === -1 && tokenChars[code] === 1) {
				if (start === -1) start = i;
			} else if (i !== 0 && (code === 32 || code === 9)) {
				if (end === -1 && start !== -1) end = i;
			} else if (code === 44) {
				if (start === -1) throw new SyntaxError(`Unexpected character at index ${i}`);
				if (end === -1) end = i;
				const protocol = header.slice(start, end);
				if (protocols.has(protocol)) throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
				protocols.add(protocol);
				start = end = -1;
			} else throw new SyntaxError(`Unexpected character at index ${i}`);
		}
		if (start === -1 || end !== -1) throw new SyntaxError("Unexpected end of input");
		const protocol = header.slice(start, i);
		if (protocols.has(protocol)) throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
		protocols.add(protocol);
		return protocols;
	}
	module.exports = { parse };
}));
//#endregion
//#region node_modules/ws/lib/websocket-server.js
var require_websocket_server = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var EventEmitter = __require("events");
	var http = __require("http");
	var { Duplex } = __require("stream");
	var { createHash } = __require("crypto");
	var extension = require_extension();
	var PerMessageDeflate = require_permessage_deflate();
	var subprotocol = require_subprotocol();
	var WebSocket = require_websocket();
	var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants$1();
	var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
	var RUNNING = 0;
	var CLOSING = 1;
	var CLOSED = 2;
	/**
	* Class representing a WebSocket server.
	*
	* @extends EventEmitter
	*/
	var WebSocketServer = class extends EventEmitter {
		/**
		* Create a `WebSocketServer` instance.
		*
		* @param {Object} options Configuration options
		* @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
		*     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
		*     multiple times in the same tick
		* @param {Boolean} [options.autoPong=true] Specifies whether or not to
		*     automatically send a pong in response to a ping
		* @param {Number} [options.backlog=511] The maximum length of the queue of
		*     pending connections
		* @param {Boolean} [options.clientTracking=true] Specifies whether or not to
		*     track clients
		* @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
		*     wait for the closing handshake to finish after `websocket.close()` is
		*     called
		* @param {Function} [options.handleProtocols] A hook to handle protocols
		* @param {String} [options.host] The hostname where to bind the server
		* @param {Number} [options.maxBufferedChunks=262144] The maximum number of
		*     buffered data chunks
		* @param {Number} [options.maxFragments=16384] The maximum number of message
		*     fragments
		* @param {Number} [options.maxPayload=104857600] The maximum allowed message
		*     size
		* @param {Boolean} [options.noServer=false] Enable no server mode
		* @param {String} [options.path] Accept only connections matching this path
		* @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
		*     permessage-deflate
		* @param {Number} [options.port] The port where to bind the server
		* @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
		*     server to use
		* @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
		*     not to skip UTF-8 validation for text and close messages
		* @param {Function} [options.verifyClient] A hook to reject connections
		* @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
		*     class to use. It must be the `WebSocket` class or class that extends it
		* @param {Function} [callback] A listener for the `listening` event
		*/
		constructor(options, callback) {
			super();
			options = {
				allowSynchronousEvents: true,
				autoPong: true,
				maxBufferedChunks: 262144,
				maxFragments: 16384,
				maxPayload: 104857600,
				skipUTF8Validation: false,
				perMessageDeflate: false,
				handleProtocols: null,
				clientTracking: true,
				closeTimeout: CLOSE_TIMEOUT,
				verifyClient: null,
				noServer: false,
				backlog: null,
				server: null,
				host: null,
				path: null,
				port: null,
				WebSocket,
				...options
			};
			if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) throw new TypeError("One and only one of the \"port\", \"server\", or \"noServer\" options must be specified");
			if (options.port != null) {
				this._server = http.createServer((req, res) => {
					const body = http.STATUS_CODES[426];
					res.writeHead(426, {
						"Content-Length": body.length,
						"Content-Type": "text/plain"
					});
					res.end(body);
				});
				this._server.listen(options.port, options.host, options.backlog, callback);
			} else if (options.server) this._server = options.server;
			if (this._server) {
				const emitConnection = this.emit.bind(this, "connection");
				this._removeListeners = addListeners(this._server, {
					listening: this.emit.bind(this, "listening"),
					error: this.emit.bind(this, "error"),
					upgrade: (req, socket, head) => {
						this.handleUpgrade(req, socket, head, emitConnection);
					}
				});
			}
			if (options.perMessageDeflate === true) options.perMessageDeflate = {};
			if (options.clientTracking) {
				this.clients = /* @__PURE__ */ new Set();
				this._shouldEmitClose = false;
			}
			this.options = options;
			this._state = RUNNING;
		}
		/**
		* Returns the bound address, the address family name, and port of the server
		* as reported by the operating system if listening on an IP socket.
		* If the server is listening on a pipe or UNIX domain socket, the name is
		* returned as a string.
		*
		* @return {(Object|String|null)} The address of the server
		* @public
		*/
		address() {
			if (this.options.noServer) throw new Error("The server is operating in \"noServer\" mode");
			if (!this._server) return null;
			return this._server.address();
		}
		/**
		* Stop the server from accepting new connections and emit the `'close'` event
		* when all existing connections are closed.
		*
		* @param {Function} [cb] A one-time listener for the `'close'` event
		* @public
		*/
		close(cb) {
			if (this._state === CLOSED) {
				if (cb) this.once("close", () => {
					cb(/* @__PURE__ */ new Error("The server is not running"));
				});
				process.nextTick(emitClose, this);
				return;
			}
			if (cb) this.once("close", cb);
			if (this._state === CLOSING) return;
			this._state = CLOSING;
			if (this.options.noServer || this.options.server) {
				if (this._server) {
					this._removeListeners();
					this._removeListeners = this._server = null;
				}
				if (this.clients) {
					if (!this.clients.size) process.nextTick(emitClose, this);
					else this._shouldEmitClose = true;
				} else process.nextTick(emitClose, this);
			} else {
				const server = this._server;
				this._removeListeners();
				this._removeListeners = this._server = null;
				server.close(() => {
					emitClose(this);
				});
			}
		}
		/**
		* See if a given request should be handled by this server instance.
		*
		* @param {http.IncomingMessage} req Request object to inspect
		* @return {Boolean} `true` if the request is valid, else `false`
		* @public
		*/
		shouldHandle(req) {
			if (this.options.path) {
				const index = req.url.indexOf("?");
				if ((index !== -1 ? req.url.slice(0, index) : req.url) !== this.options.path) return false;
			}
			return true;
		}
		/**
		* Handle a HTTP Upgrade request.
		*
		* @param {http.IncomingMessage} req The request object
		* @param {Duplex} socket The network socket between the server and client
		* @param {Buffer} head The first packet of the upgraded stream
		* @param {Function} cb Callback
		* @public
		*/
		handleUpgrade(req, socket, head, cb) {
			socket.on("error", socketOnError);
			const key = req.headers["sec-websocket-key"];
			const upgrade = req.headers.upgrade;
			const version = +req.headers["sec-websocket-version"];
			if (req.method !== "GET") {
				abortHandshakeOrEmitwsClientError(this, req, socket, 405, "Invalid HTTP method");
				return;
			}
			if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
				abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Invalid Upgrade header");
				return;
			}
			if (key === void 0 || !keyRegex.test(key)) {
				abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Missing or invalid Sec-WebSocket-Key header");
				return;
			}
			if (version !== 13 && version !== 8) {
				abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Missing or invalid Sec-WebSocket-Version header", { "Sec-WebSocket-Version": "13, 8" });
				return;
			}
			if (!this.shouldHandle(req)) {
				abortHandshake(socket, 400);
				return;
			}
			const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
			let protocols = /* @__PURE__ */ new Set();
			if (secWebSocketProtocol !== void 0) try {
				protocols = subprotocol.parse(secWebSocketProtocol);
			} catch (err) {
				abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Invalid Sec-WebSocket-Protocol header");
				return;
			}
			const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
			const extensions = {};
			if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
				const perMessageDeflate = new PerMessageDeflate({
					...this.options.perMessageDeflate,
					isServer: true,
					maxPayload: this.options.maxPayload
				});
				try {
					const offers = extension.parse(secWebSocketExtensions);
					if (offers[PerMessageDeflate.extensionName]) {
						perMessageDeflate.accept(offers[PerMessageDeflate.extensionName]);
						extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
					}
				} catch (err) {
					abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Invalid or unacceptable Sec-WebSocket-Extensions header");
					return;
				}
			}
			if (this.options.verifyClient) {
				const info = {
					origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
					secure: !!(req.socket.authorized || req.socket.encrypted),
					req
				};
				if (this.options.verifyClient.length === 2) {
					this.options.verifyClient(info, (verified, code, message, headers) => {
						if (!verified) return abortHandshake(socket, code || 401, message, headers);
						this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
					});
					return;
				}
				if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
			}
			this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
		}
		/**
		* Upgrade the connection to WebSocket.
		*
		* @param {Object} extensions The accepted extensions
		* @param {String} key The value of the `Sec-WebSocket-Key` header
		* @param {Set} protocols The subprotocols
		* @param {http.IncomingMessage} req The request object
		* @param {Duplex} socket The network socket between the server and client
		* @param {Buffer} head The first packet of the upgraded stream
		* @param {Function} cb Callback
		* @throws {Error} If called more than once with the same socket
		* @private
		*/
		completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
			if (!socket.readable || !socket.writable) return socket.destroy();
			if (socket[kWebSocket]) throw new Error("server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration");
			if (this._state > RUNNING) return abortHandshake(socket, 503);
			const headers = [
				"HTTP/1.1 101 Switching Protocols",
				"Upgrade: websocket",
				"Connection: Upgrade",
				`Sec-WebSocket-Accept: ${createHash("sha1").update(key + GUID).digest("base64")}`
			];
			const ws = new this.options.WebSocket(null, void 0, this.options);
			if (protocols.size) {
				const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
				if (protocol) {
					headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
					ws._protocol = protocol;
				}
			}
			if (extensions[PerMessageDeflate.extensionName]) {
				const params = extensions[PerMessageDeflate.extensionName].params;
				const value = extension.format({ [PerMessageDeflate.extensionName]: [params] });
				headers.push(`Sec-WebSocket-Extensions: ${value}`);
				ws._extensions = extensions;
			}
			this.emit("headers", headers, req);
			socket.write(headers.concat("\r\n").join("\r\n"));
			socket.removeListener("error", socketOnError);
			ws.setSocket(socket, head, {
				allowSynchronousEvents: this.options.allowSynchronousEvents,
				maxBufferedChunks: this.options.maxBufferedChunks,
				maxFragments: this.options.maxFragments,
				maxPayload: this.options.maxPayload,
				skipUTF8Validation: this.options.skipUTF8Validation
			});
			if (this.clients) {
				this.clients.add(ws);
				ws.on("close", () => {
					this.clients.delete(ws);
					if (this._shouldEmitClose && !this.clients.size) process.nextTick(emitClose, this);
				});
			}
			cb(ws, req);
		}
	};
	module.exports = WebSocketServer;
	/**
	* Add event listeners on an `EventEmitter` using a map of <event, listener>
	* pairs.
	*
	* @param {EventEmitter} server The event emitter
	* @param {Object.<String, Function>} map The listeners to add
	* @return {Function} A function that will remove the added listeners when
	*     called
	* @private
	*/
	function addListeners(server, map) {
		for (const event of Object.keys(map)) server.on(event, map[event]);
		return function removeListeners() {
			for (const event of Object.keys(map)) server.removeListener(event, map[event]);
		};
	}
	/**
	* Emit a `'close'` event on an `EventEmitter`.
	*
	* @param {EventEmitter} server The event emitter
	* @private
	*/
	function emitClose(server) {
		server._state = CLOSED;
		server.emit("close");
	}
	/**
	* Handle socket errors.
	*
	* @private
	*/
	function socketOnError() {
		this.destroy();
	}
	/**
	* Close the connection when preconditions are not fulfilled.
	*
	* @param {Duplex} socket The socket of the upgrade request
	* @param {Number} code The HTTP response status code
	* @param {String} [message] The HTTP response body
	* @param {Object} [headers] Additional HTTP response headers
	* @private
	*/
	function abortHandshake(socket, code, message, headers) {
		message = message || http.STATUS_CODES[code];
		headers = {
			Connection: "close",
			"Content-Type": "text/html",
			"Content-Length": Buffer.byteLength(message),
			...headers
		};
		socket.once("finish", socket.destroy);
		socket.end(`HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r\n` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message);
	}
	/**
	* Emit a `'wsClientError'` event on a `WebSocketServer` if there is at least
	* one listener for it, otherwise call `abortHandshake()`.
	*
	* @param {WebSocketServer} server The WebSocket server
	* @param {http.IncomingMessage} req The request object
	* @param {Duplex} socket The socket of the upgrade request
	* @param {Number} code The HTTP response status code
	* @param {String} message The HTTP response body
	* @param {Object} [headers] The HTTP response headers
	* @private
	*/
	function abortHandshakeOrEmitwsClientError(server, req, socket, code, message, headers) {
		if (server.listenerCount("wsClientError")) {
			const err = new Error(message);
			Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
			server.emit("wsClientError", err, socket, req);
		} else abortHandshake(socket, code, message, headers);
	}
}));
//#endregion
//#region node_modules/ws/index.js
var require_ws$1 = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var createWebSocketStream = require_stream();
	var extension = require_extension();
	var PerMessageDeflate = require_permessage_deflate();
	var Receiver = require_receiver();
	var Sender = require_sender();
	var subprotocol = require_subprotocol();
	var WebSocket = require_websocket();
	var WebSocketServer = require_websocket_server();
	WebSocket.createWebSocketStream = createWebSocketStream;
	WebSocket.extension = extension;
	WebSocket.PerMessageDeflate = PerMessageDeflate;
	WebSocket.Receiver = Receiver;
	WebSocket.Sender = Sender;
	WebSocket.Server = WebSocketServer;
	WebSocket.subprotocol = subprotocol;
	WebSocket.WebSocket = WebSocket;
	WebSocket.WebSocketServer = WebSocketServer;
	module.exports = WebSocket;
}));
//#endregion
//#region node_modules/mqtt/build/lib/BufferedDuplex.js
var require_BufferedDuplex = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.BufferedDuplex = void 0;
	exports.writev = writev;
	var readable_stream_1 = require_ours();
	var buffer_1$4 = __require("buffer");
	function writev(chunks, cb) {
		const buffers = new Array(chunks.length);
		for (let i = 0; i < chunks.length; i++) if (typeof chunks[i].chunk === "string") buffers[i] = buffer_1$4.Buffer.from(chunks[i].chunk, "utf8");
		else buffers[i] = chunks[i].chunk;
		this._write(buffer_1$4.Buffer.concat(buffers), "binary", cb);
	}
	var BufferedDuplex = class extends readable_stream_1.Duplex {
		socket;
		proxy;
		isSocketOpen;
		writeQueue;
		constructor(opts, proxy, socket) {
			super({ objectMode: true });
			this.proxy = proxy;
			this.socket = socket;
			this.writeQueue = [];
			if (!opts.objectMode) this._writev = writev.bind(this);
			this.isSocketOpen = false;
			this.proxy.on("data", (chunk) => {
				if (!this.destroyed && this.readable) this.push(chunk);
			});
		}
		_read(size) {
			this.proxy.read(size);
		}
		_write(chunk, encoding, cb) {
			if (!this.isSocketOpen) this.writeQueue.push({
				chunk,
				encoding,
				cb
			});
			else this.writeToProxy(chunk, encoding, cb);
		}
		_final(callback) {
			this.writeQueue = [];
			this.proxy.end(callback);
		}
		_destroy(err, callback) {
			this.writeQueue = [];
			this.proxy.destroy();
			callback(err);
		}
		socketReady() {
			this.emit("connect");
			this.isSocketOpen = true;
			this.processWriteQueue();
		}
		writeToProxy(chunk, encoding, cb) {
			if (this.proxy.write(chunk, encoding) === false) this.proxy.once("drain", cb);
			else cb();
		}
		processWriteQueue() {
			while (this.writeQueue.length > 0) {
				const { chunk, encoding, cb } = this.writeQueue.shift();
				this.writeToProxy(chunk, encoding, cb);
			}
		}
	};
	exports.BufferedDuplex = BufferedDuplex;
}));
//#endregion
//#region node_modules/mqtt/build/lib/connect/ws.js
var require_ws = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.streamBuilder = exports.browserStreamBuilder = void 0;
	var buffer_1$3 = __require("buffer");
	var ws_1 = __importDefault(require_ws$1());
	var debug_1 = __importDefault(require_src());
	var readable_stream_1 = require_ours();
	var is_browser_1 = __importDefault(require_is_browser());
	var BufferedDuplex_1 = require_BufferedDuplex();
	var debug = (0, debug_1.default)("mqttjs:ws");
	var WSS_OPTIONS = [
		"rejectUnauthorized",
		"ca",
		"cert",
		"key",
		"pfx",
		"passphrase"
	];
	function buildUrl(opts, client) {
		let url = `${opts.protocol}://${opts.hostname}:${opts.port}${opts.path}`;
		if (typeof opts.transformWsUrl === "function") url = opts.transformWsUrl(url, opts, client);
		return url;
	}
	function setDefaultOpts(opts) {
		const options = opts;
		if (!opts.port) {
			if (opts.protocol === "wss") options.port = 443;
			else options.port = 80;
		}
		if (!opts.path) options.path = "/";
		if (!opts.wsOptions) options.wsOptions = {};
		if (!is_browser_1.default && !opts.forceNativeWebSocket && opts.protocol === "wss") WSS_OPTIONS.forEach((prop) => {
			if (Object.prototype.hasOwnProperty.call(opts, prop) && !Object.prototype.hasOwnProperty.call(opts.wsOptions, prop)) options.wsOptions[prop] = opts[prop];
		});
		return options;
	}
	function setDefaultBrowserOpts(opts) {
		const options = setDefaultOpts(opts);
		if (!options.hostname) options.hostname = options.host;
		if (!options.hostname) {
			if (typeof document === "undefined") throw new Error("Could not determine host. Specify host manually.");
			const parsed = new URL(document.URL);
			options.hostname = parsed.hostname;
			if (!options.port) options.port = Number(parsed.port);
		}
		if (options.objectMode === void 0) options.objectMode = !(options.binary === true || options.binary === void 0);
		return options;
	}
	function createWebSocket(client, url, opts) {
		debug("createWebSocket");
		debug(`protocol: ${opts.protocolId} ${opts.protocolVersion}`);
		const websocketSubProtocol = opts.protocolId === "MQIsdp" && opts.protocolVersion === 3 ? "mqttv3.1" : "mqtt";
		debug(`creating new Websocket for url: ${url} and protocol: ${websocketSubProtocol}`);
		let socket;
		if (opts.createWebsocket) socket = opts.createWebsocket(url, [websocketSubProtocol], opts);
		else socket = new ws_1.default(url, [websocketSubProtocol], opts.wsOptions);
		return socket;
	}
	function createBrowserWebSocket(client, opts) {
		const websocketSubProtocol = opts.protocolId === "MQIsdp" && opts.protocolVersion === 3 ? "mqttv3.1" : "mqtt";
		const url = buildUrl(opts, client);
		let socket;
		if (opts.createWebsocket) socket = opts.createWebsocket(url, [websocketSubProtocol], opts);
		else socket = new WebSocket(url, [websocketSubProtocol]);
		socket.binaryType = "arraybuffer";
		return socket;
	}
	var streamBuilder = (client, opts) => {
		debug("streamBuilder");
		const options = setDefaultOpts(opts);
		options.hostname = options.hostname || options.host || "localhost";
		const url = buildUrl(options, client);
		const socket = createWebSocket(client, url, options);
		const webSocketStream = ws_1.default.createWebSocketStream(socket, options.wsOptions);
		webSocketStream["url"] = url;
		socket.on("close", () => {
			webSocketStream.destroy();
		});
		return webSocketStream;
	};
	exports.streamBuilder = streamBuilder;
	var browserStreamBuilder = (client, opts) => {
		debug("browserStreamBuilder");
		let stream;
		const bufferSize = setDefaultBrowserOpts(opts).browserBufferSize || 524288;
		const bufferTimeout = opts.browserBufferTimeout || 1e3;
		const coerceToBuffer = !opts.objectMode;
		const socket = createBrowserWebSocket(client, opts);
		const proxy = buildProxy(opts, socketWriteBrowser, socketEndBrowser);
		if (!opts.objectMode) proxy._writev = BufferedDuplex_1.writev.bind(proxy);
		proxy.on("close", () => {
			socket.close();
		});
		const eventListenerSupport = typeof socket.addEventListener !== "undefined";
		if (socket.readyState === socket.OPEN) {
			stream = proxy;
			stream.socket = socket;
		} else {
			stream = new BufferedDuplex_1.BufferedDuplex(opts, proxy, socket);
			if (eventListenerSupport) socket.addEventListener("open", onOpen);
			else socket.onopen = onOpen;
		}
		if (eventListenerSupport) {
			socket.addEventListener("close", onClose);
			socket.addEventListener("error", onError);
			socket.addEventListener("message", onMessage);
		} else {
			socket.onclose = onClose;
			socket.onerror = onError;
			socket.onmessage = onMessage;
		}
		function buildProxy(pOptions, socketWrite, socketEnd) {
			const _proxy = new readable_stream_1.Transform({ objectMode: pOptions.objectMode });
			_proxy._write = socketWrite;
			_proxy._flush = socketEnd;
			return _proxy;
		}
		function onOpen() {
			debug("WebSocket onOpen");
			if (stream instanceof BufferedDuplex_1.BufferedDuplex) stream.socketReady();
		}
		function onClose(event) {
			debug("WebSocket onClose", event);
			stream.end();
			stream.destroy();
		}
		function onError(err) {
			debug("WebSocket onError", err);
			const error = /* @__PURE__ */ new Error("WebSocket error");
			error["event"] = err;
			stream.destroy(error);
		}
		async function onMessage(event) {
			if (!proxy || !proxy.readable || !proxy.writable) return;
			let { data } = event;
			if (data instanceof ArrayBuffer) data = buffer_1$3.Buffer.from(data);
			else if (data instanceof Blob) data = buffer_1$3.Buffer.from(await new Response(data).arrayBuffer());
			else data = buffer_1$3.Buffer.from(data, "utf8");
			proxy.push(data);
		}
		function socketWriteBrowser(chunk, enc, next) {
			if (socket.bufferedAmount > bufferSize) {
				setTimeout(socketWriteBrowser, bufferTimeout, chunk, enc, next);
				return;
			}
			if (coerceToBuffer && typeof chunk === "string") chunk = buffer_1$3.Buffer.from(chunk, "utf8");
			try {
				socket.send(chunk);
			} catch (err) {
				return next(err);
			}
			next();
		}
		function socketEndBrowser(done) {
			socket.close();
			done();
		}
		return stream;
	};
	exports.browserStreamBuilder = browserStreamBuilder;
}));
//#endregion
//#region node_modules/smart-buffer/build/utils.js
var require_utils = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var buffer_1$2 = __require("buffer");
	/**
	* Error strings
	*/
	var ERRORS = {
		INVALID_ENCODING: "Invalid encoding provided. Please specify a valid encoding the internal Node.js Buffer supports.",
		INVALID_SMARTBUFFER_SIZE: "Invalid size provided. Size must be a valid integer greater than zero.",
		INVALID_SMARTBUFFER_BUFFER: "Invalid Buffer provided in SmartBufferOptions.",
		INVALID_SMARTBUFFER_OBJECT: "Invalid SmartBufferOptions object supplied to SmartBuffer constructor or factory methods.",
		INVALID_OFFSET: "An invalid offset value was provided.",
		INVALID_OFFSET_NON_NUMBER: "An invalid offset value was provided. A numeric value is required.",
		INVALID_LENGTH: "An invalid length value was provided.",
		INVALID_LENGTH_NON_NUMBER: "An invalid length value was provived. A numeric value is required.",
		INVALID_TARGET_OFFSET: "Target offset is beyond the bounds of the internal SmartBuffer data.",
		INVALID_TARGET_LENGTH: "Specified length value moves cursor beyong the bounds of the internal SmartBuffer data.",
		INVALID_READ_BEYOND_BOUNDS: "Attempted to read beyond the bounds of the managed data.",
		INVALID_WRITE_BEYOND_BOUNDS: "Attempted to write beyond the bounds of the managed data."
	};
	exports.ERRORS = ERRORS;
	/**
	* Checks if a given encoding is a valid Buffer encoding. (Throws an exception if check fails)
	*
	* @param { String } encoding The encoding string to check.
	*/
	function checkEncoding(encoding) {
		if (!buffer_1$2.Buffer.isEncoding(encoding)) throw new Error(ERRORS.INVALID_ENCODING);
	}
	exports.checkEncoding = checkEncoding;
	/**
	* Checks if a given number is a finite integer. (Throws an exception if check fails)
	*
	* @param { Number } value The number value to check.
	*/
	function isFiniteInteger(value) {
		return typeof value === "number" && isFinite(value) && isInteger(value);
	}
	exports.isFiniteInteger = isFiniteInteger;
	/**
	* Checks if an offset/length value is valid. (Throws an exception if check fails)
	*
	* @param value The value to check.
	* @param offset True if checking an offset, false if checking a length.
	*/
	function checkOffsetOrLengthValue(value, offset) {
		if (typeof value === "number") {
			if (!isFiniteInteger(value) || value < 0) throw new Error(offset ? ERRORS.INVALID_OFFSET : ERRORS.INVALID_LENGTH);
		} else throw new Error(offset ? ERRORS.INVALID_OFFSET_NON_NUMBER : ERRORS.INVALID_LENGTH_NON_NUMBER);
	}
	/**
	* Checks if a length value is valid. (Throws an exception if check fails)
	*
	* @param { Number } length The value to check.
	*/
	function checkLengthValue(length) {
		checkOffsetOrLengthValue(length, false);
	}
	exports.checkLengthValue = checkLengthValue;
	/**
	* Checks if a offset value is valid. (Throws an exception if check fails)
	*
	* @param { Number } offset The value to check.
	*/
	function checkOffsetValue(offset) {
		checkOffsetOrLengthValue(offset, true);
	}
	exports.checkOffsetValue = checkOffsetValue;
	/**
	* Checks if a target offset value is out of bounds. (Throws an exception if check fails)
	*
	* @param { Number } offset The offset value to check.
	* @param { SmartBuffer } buff The SmartBuffer instance to check against.
	*/
	function checkTargetOffset(offset, buff) {
		if (offset < 0 || offset > buff.length) throw new Error(ERRORS.INVALID_TARGET_OFFSET);
	}
	exports.checkTargetOffset = checkTargetOffset;
	/**
	* Determines whether a given number is a integer.
	* @param value The number to check.
	*/
	function isInteger(value) {
		return typeof value === "number" && isFinite(value) && Math.floor(value) === value;
	}
	/**
	* Throws if Node.js version is too low to support bigint
	*/
	function bigIntAndBufferInt64Check(bufferMethod) {
		if (typeof BigInt === "undefined") throw new Error("Platform does not support JS BigInt type.");
		if (typeof buffer_1$2.Buffer.prototype[bufferMethod] === "undefined") throw new Error(`Platform does not support Buffer.prototype.${bufferMethod}.`);
	}
	exports.bigIntAndBufferInt64Check = bigIntAndBufferInt64Check;
}));
//#endregion
//#region node_modules/smart-buffer/build/smartbuffer.js
var require_smartbuffer = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var utils_1 = require_utils();
	var DEFAULT_SMARTBUFFER_SIZE = 4096;
	var DEFAULT_SMARTBUFFER_ENCODING = "utf8";
	exports.SmartBuffer = class SmartBuffer {
		/**
		* Creates a new SmartBuffer instance.
		*
		* @param options { SmartBufferOptions } The SmartBufferOptions to apply to this instance.
		*/
		constructor(options) {
			this.length = 0;
			this._encoding = DEFAULT_SMARTBUFFER_ENCODING;
			this._writeOffset = 0;
			this._readOffset = 0;
			if (SmartBuffer.isSmartBufferOptions(options)) {
				if (options.encoding) {
					utils_1.checkEncoding(options.encoding);
					this._encoding = options.encoding;
				}
				if (options.size) {
					if (utils_1.isFiniteInteger(options.size) && options.size > 0) this._buff = Buffer.allocUnsafe(options.size);
					else throw new Error(utils_1.ERRORS.INVALID_SMARTBUFFER_SIZE);
				} else if (options.buff) {
					if (Buffer.isBuffer(options.buff)) {
						this._buff = options.buff;
						this.length = options.buff.length;
					} else throw new Error(utils_1.ERRORS.INVALID_SMARTBUFFER_BUFFER);
				} else this._buff = Buffer.allocUnsafe(DEFAULT_SMARTBUFFER_SIZE);
			} else {
				if (typeof options !== "undefined") throw new Error(utils_1.ERRORS.INVALID_SMARTBUFFER_OBJECT);
				this._buff = Buffer.allocUnsafe(DEFAULT_SMARTBUFFER_SIZE);
			}
		}
		/**
		* Creates a new SmartBuffer instance with the provided internal Buffer size and optional encoding.
		*
		* @param size { Number } The size of the internal Buffer.
		* @param encoding { String } The BufferEncoding to use for strings.
		*
		* @return { SmartBuffer }
		*/
		static fromSize(size, encoding) {
			return new this({
				size,
				encoding
			});
		}
		/**
		* Creates a new SmartBuffer instance with the provided Buffer and optional encoding.
		*
		* @param buffer { Buffer } The Buffer to use as the internal Buffer value.
		* @param encoding { String } The BufferEncoding to use for strings.
		*
		* @return { SmartBuffer }
		*/
		static fromBuffer(buff, encoding) {
			return new this({
				buff,
				encoding
			});
		}
		/**
		* Creates a new SmartBuffer instance with the provided SmartBufferOptions options.
		*
		* @param options { SmartBufferOptions } The options to use when creating the SmartBuffer instance.
		*/
		static fromOptions(options) {
			return new this(options);
		}
		/**
		* Type checking function that determines if an object is a SmartBufferOptions object.
		*/
		static isSmartBufferOptions(options) {
			const castOptions = options;
			return castOptions && (castOptions.encoding !== void 0 || castOptions.size !== void 0 || castOptions.buff !== void 0);
		}
		/**
		* Reads an Int8 value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readInt8(offset) {
			return this._readNumberValue(Buffer.prototype.readInt8, 1, offset);
		}
		/**
		* Reads an Int16BE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readInt16BE(offset) {
			return this._readNumberValue(Buffer.prototype.readInt16BE, 2, offset);
		}
		/**
		* Reads an Int16LE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readInt16LE(offset) {
			return this._readNumberValue(Buffer.prototype.readInt16LE, 2, offset);
		}
		/**
		* Reads an Int32BE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readInt32BE(offset) {
			return this._readNumberValue(Buffer.prototype.readInt32BE, 4, offset);
		}
		/**
		* Reads an Int32LE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readInt32LE(offset) {
			return this._readNumberValue(Buffer.prototype.readInt32LE, 4, offset);
		}
		/**
		* Reads a BigInt64BE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { BigInt }
		*/
		readBigInt64BE(offset) {
			utils_1.bigIntAndBufferInt64Check("readBigInt64BE");
			return this._readNumberValue(Buffer.prototype.readBigInt64BE, 8, offset);
		}
		/**
		* Reads a BigInt64LE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { BigInt }
		*/
		readBigInt64LE(offset) {
			utils_1.bigIntAndBufferInt64Check("readBigInt64LE");
			return this._readNumberValue(Buffer.prototype.readBigInt64LE, 8, offset);
		}
		/**
		* Writes an Int8 value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeInt8(value, offset) {
			this._writeNumberValue(Buffer.prototype.writeInt8, 1, value, offset);
			return this;
		}
		/**
		* Inserts an Int8 value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertInt8(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeInt8, 1, value, offset);
		}
		/**
		* Writes an Int16BE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeInt16BE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeInt16BE, 2, value, offset);
		}
		/**
		* Inserts an Int16BE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertInt16BE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeInt16BE, 2, value, offset);
		}
		/**
		* Writes an Int16LE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeInt16LE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeInt16LE, 2, value, offset);
		}
		/**
		* Inserts an Int16LE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertInt16LE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeInt16LE, 2, value, offset);
		}
		/**
		* Writes an Int32BE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeInt32BE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeInt32BE, 4, value, offset);
		}
		/**
		* Inserts an Int32BE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertInt32BE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeInt32BE, 4, value, offset);
		}
		/**
		* Writes an Int32LE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeInt32LE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeInt32LE, 4, value, offset);
		}
		/**
		* Inserts an Int32LE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertInt32LE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeInt32LE, 4, value, offset);
		}
		/**
		* Writes a BigInt64BE value to the current write position (or at optional offset).
		*
		* @param value { BigInt } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeBigInt64BE(value, offset) {
			utils_1.bigIntAndBufferInt64Check("writeBigInt64BE");
			return this._writeNumberValue(Buffer.prototype.writeBigInt64BE, 8, value, offset);
		}
		/**
		* Inserts a BigInt64BE value at the given offset value.
		*
		* @param value { BigInt } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertBigInt64BE(value, offset) {
			utils_1.bigIntAndBufferInt64Check("writeBigInt64BE");
			return this._insertNumberValue(Buffer.prototype.writeBigInt64BE, 8, value, offset);
		}
		/**
		* Writes a BigInt64LE value to the current write position (or at optional offset).
		*
		* @param value { BigInt } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeBigInt64LE(value, offset) {
			utils_1.bigIntAndBufferInt64Check("writeBigInt64LE");
			return this._writeNumberValue(Buffer.prototype.writeBigInt64LE, 8, value, offset);
		}
		/**
		* Inserts a Int64LE value at the given offset value.
		*
		* @param value { BigInt } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertBigInt64LE(value, offset) {
			utils_1.bigIntAndBufferInt64Check("writeBigInt64LE");
			return this._insertNumberValue(Buffer.prototype.writeBigInt64LE, 8, value, offset);
		}
		/**
		* Reads an UInt8 value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readUInt8(offset) {
			return this._readNumberValue(Buffer.prototype.readUInt8, 1, offset);
		}
		/**
		* Reads an UInt16BE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readUInt16BE(offset) {
			return this._readNumberValue(Buffer.prototype.readUInt16BE, 2, offset);
		}
		/**
		* Reads an UInt16LE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readUInt16LE(offset) {
			return this._readNumberValue(Buffer.prototype.readUInt16LE, 2, offset);
		}
		/**
		* Reads an UInt32BE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readUInt32BE(offset) {
			return this._readNumberValue(Buffer.prototype.readUInt32BE, 4, offset);
		}
		/**
		* Reads an UInt32LE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readUInt32LE(offset) {
			return this._readNumberValue(Buffer.prototype.readUInt32LE, 4, offset);
		}
		/**
		* Reads a BigUInt64BE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { BigInt }
		*/
		readBigUInt64BE(offset) {
			utils_1.bigIntAndBufferInt64Check("readBigUInt64BE");
			return this._readNumberValue(Buffer.prototype.readBigUInt64BE, 8, offset);
		}
		/**
		* Reads a BigUInt64LE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { BigInt }
		*/
		readBigUInt64LE(offset) {
			utils_1.bigIntAndBufferInt64Check("readBigUInt64LE");
			return this._readNumberValue(Buffer.prototype.readBigUInt64LE, 8, offset);
		}
		/**
		* Writes an UInt8 value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeUInt8(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeUInt8, 1, value, offset);
		}
		/**
		* Inserts an UInt8 value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertUInt8(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeUInt8, 1, value, offset);
		}
		/**
		* Writes an UInt16BE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeUInt16BE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeUInt16BE, 2, value, offset);
		}
		/**
		* Inserts an UInt16BE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertUInt16BE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeUInt16BE, 2, value, offset);
		}
		/**
		* Writes an UInt16LE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeUInt16LE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeUInt16LE, 2, value, offset);
		}
		/**
		* Inserts an UInt16LE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertUInt16LE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeUInt16LE, 2, value, offset);
		}
		/**
		* Writes an UInt32BE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeUInt32BE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeUInt32BE, 4, value, offset);
		}
		/**
		* Inserts an UInt32BE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertUInt32BE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeUInt32BE, 4, value, offset);
		}
		/**
		* Writes an UInt32LE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeUInt32LE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeUInt32LE, 4, value, offset);
		}
		/**
		* Inserts an UInt32LE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertUInt32LE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeUInt32LE, 4, value, offset);
		}
		/**
		* Writes a BigUInt64BE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeBigUInt64BE(value, offset) {
			utils_1.bigIntAndBufferInt64Check("writeBigUInt64BE");
			return this._writeNumberValue(Buffer.prototype.writeBigUInt64BE, 8, value, offset);
		}
		/**
		* Inserts a BigUInt64BE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertBigUInt64BE(value, offset) {
			utils_1.bigIntAndBufferInt64Check("writeBigUInt64BE");
			return this._insertNumberValue(Buffer.prototype.writeBigUInt64BE, 8, value, offset);
		}
		/**
		* Writes a BigUInt64LE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeBigUInt64LE(value, offset) {
			utils_1.bigIntAndBufferInt64Check("writeBigUInt64LE");
			return this._writeNumberValue(Buffer.prototype.writeBigUInt64LE, 8, value, offset);
		}
		/**
		* Inserts a BigUInt64LE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertBigUInt64LE(value, offset) {
			utils_1.bigIntAndBufferInt64Check("writeBigUInt64LE");
			return this._insertNumberValue(Buffer.prototype.writeBigUInt64LE, 8, value, offset);
		}
		/**
		* Reads an FloatBE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readFloatBE(offset) {
			return this._readNumberValue(Buffer.prototype.readFloatBE, 4, offset);
		}
		/**
		* Reads an FloatLE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readFloatLE(offset) {
			return this._readNumberValue(Buffer.prototype.readFloatLE, 4, offset);
		}
		/**
		* Writes a FloatBE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeFloatBE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeFloatBE, 4, value, offset);
		}
		/**
		* Inserts a FloatBE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertFloatBE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeFloatBE, 4, value, offset);
		}
		/**
		* Writes a FloatLE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeFloatLE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeFloatLE, 4, value, offset);
		}
		/**
		* Inserts a FloatLE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertFloatLE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeFloatLE, 4, value, offset);
		}
		/**
		* Reads an DoublEBE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readDoubleBE(offset) {
			return this._readNumberValue(Buffer.prototype.readDoubleBE, 8, offset);
		}
		/**
		* Reads an DoubleLE value from the current read position or an optionally provided offset.
		*
		* @param offset { Number } The offset to read data from (optional)
		* @return { Number }
		*/
		readDoubleLE(offset) {
			return this._readNumberValue(Buffer.prototype.readDoubleLE, 8, offset);
		}
		/**
		* Writes a DoubleBE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeDoubleBE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeDoubleBE, 8, value, offset);
		}
		/**
		* Inserts a DoubleBE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertDoubleBE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeDoubleBE, 8, value, offset);
		}
		/**
		* Writes a DoubleLE value to the current write position (or at optional offset).
		*
		* @param value { Number } The value to write.
		* @param offset { Number } The offset to write the value at.
		*
		* @return this
		*/
		writeDoubleLE(value, offset) {
			return this._writeNumberValue(Buffer.prototype.writeDoubleLE, 8, value, offset);
		}
		/**
		* Inserts a DoubleLE value at the given offset value.
		*
		* @param value { Number } The value to insert.
		* @param offset { Number } The offset to insert the value at.
		*
		* @return this
		*/
		insertDoubleLE(value, offset) {
			return this._insertNumberValue(Buffer.prototype.writeDoubleLE, 8, value, offset);
		}
		/**
		* Reads a String from the current read position.
		*
		* @param arg1 { Number | String } The number of bytes to read as a String, or the BufferEncoding to use for
		*             the string (Defaults to instance level encoding).
		* @param encoding { String } The BufferEncoding to use for the string (Defaults to instance level encoding).
		*
		* @return { String }
		*/
		readString(arg1, encoding) {
			let lengthVal;
			if (typeof arg1 === "number") {
				utils_1.checkLengthValue(arg1);
				lengthVal = Math.min(arg1, this.length - this._readOffset);
			} else {
				encoding = arg1;
				lengthVal = this.length - this._readOffset;
			}
			if (typeof encoding !== "undefined") utils_1.checkEncoding(encoding);
			const value = this._buff.slice(this._readOffset, this._readOffset + lengthVal).toString(encoding || this._encoding);
			this._readOffset += lengthVal;
			return value;
		}
		/**
		* Inserts a String
		*
		* @param value { String } The String value to insert.
		* @param offset { Number } The offset to insert the string at.
		* @param encoding { String } The BufferEncoding to use for writing strings (defaults to instance encoding).
		*
		* @return this
		*/
		insertString(value, offset, encoding) {
			utils_1.checkOffsetValue(offset);
			return this._handleString(value, true, offset, encoding);
		}
		/**
		* Writes a String
		*
		* @param value { String } The String value to write.
		* @param arg2 { Number | String } The offset to write the string at, or the BufferEncoding to use.
		* @param encoding { String } The BufferEncoding to use for writing strings (defaults to instance encoding).
		*
		* @return this
		*/
		writeString(value, arg2, encoding) {
			return this._handleString(value, false, arg2, encoding);
		}
		/**
		* Reads a null-terminated String from the current read position.
		*
		* @param encoding { String } The BufferEncoding to use for the string (Defaults to instance level encoding).
		*
		* @return { String }
		*/
		readStringNT(encoding) {
			if (typeof encoding !== "undefined") utils_1.checkEncoding(encoding);
			let nullPos = this.length;
			for (let i = this._readOffset; i < this.length; i++) if (this._buff[i] === 0) {
				nullPos = i;
				break;
			}
			const value = this._buff.slice(this._readOffset, nullPos);
			this._readOffset = nullPos + 1;
			return value.toString(encoding || this._encoding);
		}
		/**
		* Inserts a null-terminated String.
		*
		* @param value { String } The String value to write.
		* @param arg2 { Number | String } The offset to write the string to, or the BufferEncoding to use.
		* @param encoding { String } The BufferEncoding to use for writing strings (defaults to instance encoding).
		*
		* @return this
		*/
		insertStringNT(value, offset, encoding) {
			utils_1.checkOffsetValue(offset);
			this.insertString(value, offset, encoding);
			this.insertUInt8(0, offset + value.length);
			return this;
		}
		/**
		* Writes a null-terminated String.
		*
		* @param value { String } The String value to write.
		* @param arg2 { Number | String } The offset to write the string to, or the BufferEncoding to use.
		* @param encoding { String } The BufferEncoding to use for writing strings (defaults to instance encoding).
		*
		* @return this
		*/
		writeStringNT(value, arg2, encoding) {
			this.writeString(value, arg2, encoding);
			this.writeUInt8(0, typeof arg2 === "number" ? arg2 + value.length : this.writeOffset);
			return this;
		}
		/**
		* Reads a Buffer from the internal read position.
		*
		* @param length { Number } The length of data to read as a Buffer.
		*
		* @return { Buffer }
		*/
		readBuffer(length) {
			if (typeof length !== "undefined") utils_1.checkLengthValue(length);
			const lengthVal = typeof length === "number" ? length : this.length;
			const endPoint = Math.min(this.length, this._readOffset + lengthVal);
			const value = this._buff.slice(this._readOffset, endPoint);
			this._readOffset = endPoint;
			return value;
		}
		/**
		* Writes a Buffer to the current write position.
		*
		* @param value { Buffer } The Buffer to write.
		* @param offset { Number } The offset to write the Buffer to.
		*
		* @return this
		*/
		insertBuffer(value, offset) {
			utils_1.checkOffsetValue(offset);
			return this._handleBuffer(value, true, offset);
		}
		/**
		* Writes a Buffer to the current write position.
		*
		* @param value { Buffer } The Buffer to write.
		* @param offset { Number } The offset to write the Buffer to.
		*
		* @return this
		*/
		writeBuffer(value, offset) {
			return this._handleBuffer(value, false, offset);
		}
		/**
		* Reads a null-terminated Buffer from the current read poisiton.
		*
		* @return { Buffer }
		*/
		readBufferNT() {
			let nullPos = this.length;
			for (let i = this._readOffset; i < this.length; i++) if (this._buff[i] === 0) {
				nullPos = i;
				break;
			}
			const value = this._buff.slice(this._readOffset, nullPos);
			this._readOffset = nullPos + 1;
			return value;
		}
		/**
		* Inserts a null-terminated Buffer.
		*
		* @param value { Buffer } The Buffer to write.
		* @param offset { Number } The offset to write the Buffer to.
		*
		* @return this
		*/
		insertBufferNT(value, offset) {
			utils_1.checkOffsetValue(offset);
			this.insertBuffer(value, offset);
			this.insertUInt8(0, offset + value.length);
			return this;
		}
		/**
		* Writes a null-terminated Buffer.
		*
		* @param value { Buffer } The Buffer to write.
		* @param offset { Number } The offset to write the Buffer to.
		*
		* @return this
		*/
		writeBufferNT(value, offset) {
			if (typeof offset !== "undefined") utils_1.checkOffsetValue(offset);
			this.writeBuffer(value, offset);
			this.writeUInt8(0, typeof offset === "number" ? offset + value.length : this._writeOffset);
			return this;
		}
		/**
		* Clears the SmartBuffer instance to its original empty state.
		*/
		clear() {
			this._writeOffset = 0;
			this._readOffset = 0;
			this.length = 0;
			return this;
		}
		/**
		* Gets the remaining data left to be read from the SmartBuffer instance.
		*
		* @return { Number }
		*/
		remaining() {
			return this.length - this._readOffset;
		}
		/**
		* Gets the current read offset value of the SmartBuffer instance.
		*
		* @return { Number }
		*/
		get readOffset() {
			return this._readOffset;
		}
		/**
		* Sets the read offset value of the SmartBuffer instance.
		*
		* @param offset { Number } - The offset value to set.
		*/
		set readOffset(offset) {
			utils_1.checkOffsetValue(offset);
			utils_1.checkTargetOffset(offset, this);
			this._readOffset = offset;
		}
		/**
		* Gets the current write offset value of the SmartBuffer instance.
		*
		* @return { Number }
		*/
		get writeOffset() {
			return this._writeOffset;
		}
		/**
		* Sets the write offset value of the SmartBuffer instance.
		*
		* @param offset { Number } - The offset value to set.
		*/
		set writeOffset(offset) {
			utils_1.checkOffsetValue(offset);
			utils_1.checkTargetOffset(offset, this);
			this._writeOffset = offset;
		}
		/**
		* Gets the currently set string encoding of the SmartBuffer instance.
		*
		* @return { BufferEncoding } The string Buffer encoding currently set.
		*/
		get encoding() {
			return this._encoding;
		}
		/**
		* Sets the string encoding of the SmartBuffer instance.
		*
		* @param encoding { BufferEncoding } The string Buffer encoding to set.
		*/
		set encoding(encoding) {
			utils_1.checkEncoding(encoding);
			this._encoding = encoding;
		}
		/**
		* Gets the underlying internal Buffer. (This includes unmanaged data in the Buffer)
		*
		* @return { Buffer } The Buffer value.
		*/
		get internalBuffer() {
			return this._buff;
		}
		/**
		* Gets the value of the internal managed Buffer (Includes managed data only)
		*
		* @param { Buffer }
		*/
		toBuffer() {
			return this._buff.slice(0, this.length);
		}
		/**
		* Gets the String value of the internal managed Buffer
		*
		* @param encoding { String } The BufferEncoding to display the Buffer as (defaults to instance level encoding).
		*/
		toString(encoding) {
			const encodingVal = typeof encoding === "string" ? encoding : this._encoding;
			utils_1.checkEncoding(encodingVal);
			return this._buff.toString(encodingVal, 0, this.length);
		}
		/**
		* Destroys the SmartBuffer instance.
		*/
		destroy() {
			this.clear();
			return this;
		}
		/**
		* Handles inserting and writing strings.
		*
		* @param value { String } The String value to insert.
		* @param isInsert { Boolean } True if inserting a string, false if writing.
		* @param arg2 { Number | String } The offset to insert the string at, or the BufferEncoding to use.
		* @param encoding { String } The BufferEncoding to use for writing strings (defaults to instance encoding).
		*/
		_handleString(value, isInsert, arg3, encoding) {
			let offsetVal = this._writeOffset;
			let encodingVal = this._encoding;
			if (typeof arg3 === "number") offsetVal = arg3;
			else if (typeof arg3 === "string") {
				utils_1.checkEncoding(arg3);
				encodingVal = arg3;
			}
			if (typeof encoding === "string") {
				utils_1.checkEncoding(encoding);
				encodingVal = encoding;
			}
			const byteLength = Buffer.byteLength(value, encodingVal);
			if (isInsert) this.ensureInsertable(byteLength, offsetVal);
			else this._ensureWriteable(byteLength, offsetVal);
			this._buff.write(value, offsetVal, byteLength, encodingVal);
			if (isInsert) this._writeOffset += byteLength;
			else if (typeof arg3 === "number") this._writeOffset = Math.max(this._writeOffset, offsetVal + byteLength);
			else this._writeOffset += byteLength;
			return this;
		}
		/**
		* Handles writing or insert of a Buffer.
		*
		* @param value { Buffer } The Buffer to write.
		* @param offset { Number } The offset to write the Buffer to.
		*/
		_handleBuffer(value, isInsert, offset) {
			const offsetVal = typeof offset === "number" ? offset : this._writeOffset;
			if (isInsert) this.ensureInsertable(value.length, offsetVal);
			else this._ensureWriteable(value.length, offsetVal);
			value.copy(this._buff, offsetVal);
			if (isInsert) this._writeOffset += value.length;
			else if (typeof offset === "number") this._writeOffset = Math.max(this._writeOffset, offsetVal + value.length);
			else this._writeOffset += value.length;
			return this;
		}
		/**
		* Ensures that the internal Buffer is large enough to read data.
		*
		* @param length { Number } The length of the data that needs to be read.
		* @param offset { Number } The offset of the data that needs to be read.
		*/
		ensureReadable(length, offset) {
			let offsetVal = this._readOffset;
			if (typeof offset !== "undefined") {
				utils_1.checkOffsetValue(offset);
				offsetVal = offset;
			}
			if (offsetVal < 0 || offsetVal + length > this.length) throw new Error(utils_1.ERRORS.INVALID_READ_BEYOND_BOUNDS);
		}
		/**
		* Ensures that the internal Buffer is large enough to insert data.
		*
		* @param dataLength { Number } The length of the data that needs to be written.
		* @param offset { Number } The offset of the data to be written.
		*/
		ensureInsertable(dataLength, offset) {
			utils_1.checkOffsetValue(offset);
			this._ensureCapacity(this.length + dataLength);
			if (offset < this.length) this._buff.copy(this._buff, offset + dataLength, offset, this._buff.length);
			if (offset + dataLength > this.length) this.length = offset + dataLength;
			else this.length += dataLength;
		}
		/**
		* Ensures that the internal Buffer is large enough to write data.
		*
		* @param dataLength { Number } The length of the data that needs to be written.
		* @param offset { Number } The offset of the data to be written (defaults to writeOffset).
		*/
		_ensureWriteable(dataLength, offset) {
			const offsetVal = typeof offset === "number" ? offset : this._writeOffset;
			this._ensureCapacity(offsetVal + dataLength);
			if (offsetVal + dataLength > this.length) this.length = offsetVal + dataLength;
		}
		/**
		* Ensures that the internal Buffer is large enough to write at least the given amount of data.
		*
		* @param minLength { Number } The minimum length of the data needs to be written.
		*/
		_ensureCapacity(minLength) {
			const oldLength = this._buff.length;
			if (minLength > oldLength) {
				let data = this._buff;
				let newLength = oldLength * 3 / 2 + 1;
				if (newLength < minLength) newLength = minLength;
				this._buff = Buffer.allocUnsafe(newLength);
				data.copy(this._buff, 0, 0, oldLength);
			}
		}
		/**
		* Reads a numeric number value using the provided function.
		*
		* @typeparam T { number | bigint } The type of the value to be read
		*
		* @param func { Function(offset: number) => number } The function to read data on the internal Buffer with.
		* @param byteSize { Number } The number of bytes read.
		* @param offset { Number } The offset to read from (optional). When this is not provided, the managed readOffset is used instead.
		*
		* @returns { T } the number value
		*/
		_readNumberValue(func, byteSize, offset) {
			this.ensureReadable(byteSize, offset);
			const value = func.call(this._buff, typeof offset === "number" ? offset : this._readOffset);
			if (typeof offset === "undefined") this._readOffset += byteSize;
			return value;
		}
		/**
		* Inserts a numeric number value based on the given offset and value.
		*
		* @typeparam T { number | bigint } The type of the value to be written
		*
		* @param func { Function(offset: T, offset?) => number} The function to write data on the internal Buffer with.
		* @param byteSize { Number } The number of bytes written.
		* @param value { T } The number value to write.
		* @param offset { Number } the offset to write the number at (REQUIRED).
		*
		* @returns SmartBuffer this buffer
		*/
		_insertNumberValue(func, byteSize, value, offset) {
			utils_1.checkOffsetValue(offset);
			this.ensureInsertable(byteSize, offset);
			func.call(this._buff, value, offset);
			this._writeOffset += byteSize;
			return this;
		}
		/**
		* Writes a numeric number value based on the given offset and value.
		*
		* @typeparam T { number | bigint } The type of the value to be written
		*
		* @param func { Function(offset: T, offset?) => number} The function to write data on the internal Buffer with.
		* @param byteSize { Number } The number of bytes written.
		* @param value { T } The number value to write.
		* @param offset { Number } the offset to write the number at (REQUIRED).
		*
		* @returns SmartBuffer this buffer
		*/
		_writeNumberValue(func, byteSize, value, offset) {
			if (typeof offset === "number") {
				if (offset < 0) throw new Error(utils_1.ERRORS.INVALID_WRITE_BEYOND_BOUNDS);
				utils_1.checkOffsetValue(offset);
			}
			const offsetVal = typeof offset === "number" ? offset : this._writeOffset;
			this._ensureWriteable(byteSize, offsetVal);
			func.call(this._buff, value, offsetVal);
			if (typeof offset === "number") this._writeOffset = Math.max(this._writeOffset, offsetVal + byteSize);
			else this._writeOffset += byteSize;
			return this;
		}
	};
}));
//#endregion
//#region node_modules/socks/build/common/constants.js
var require_constants = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.SOCKS5_NO_ACCEPTABLE_AUTH = exports.SOCKS5_CUSTOM_AUTH_END = exports.SOCKS5_CUSTOM_AUTH_START = exports.SOCKS_INCOMING_PACKET_SIZES = exports.SocksClientState = exports.Socks5Response = exports.Socks5HostType = exports.Socks5Auth = exports.Socks4Response = exports.SocksCommand = exports.ERRORS = exports.DEFAULT_TIMEOUT = void 0;
	exports.DEFAULT_TIMEOUT = 3e4;
	exports.ERRORS = {
		InvalidSocksCommand: "An invalid SOCKS command was provided. Valid options are connect, bind, and associate.",
		InvalidSocksCommandForOperation: "An invalid SOCKS command was provided. Only a subset of commands are supported for this operation.",
		InvalidSocksCommandChain: "An invalid SOCKS command was provided. Chaining currently only supports the connect command.",
		InvalidSocksClientOptionsDestination: "An invalid destination host was provided.",
		InvalidSocksClientOptionsExistingSocket: "An invalid existing socket was provided. This should be an instance of stream.Duplex.",
		InvalidSocksClientOptionsProxy: "Invalid SOCKS proxy details were provided.",
		InvalidSocksClientOptionsTimeout: "An invalid timeout value was provided. Please enter a value above 0 (in ms).",
		InvalidSocksClientOptionsProxiesLength: "At least two socks proxies must be provided for chaining.",
		InvalidSocksClientOptionsCustomAuthRange: "Custom auth must be a value between 0x80 and 0xFE.",
		InvalidSocksClientOptionsCustomAuthOptions: "When a custom_auth_method is provided, custom_auth_request_handler, custom_auth_response_size, and custom_auth_response_handler must also be provided and valid.",
		NegotiationError: "Negotiation error",
		SocketClosed: "Socket closed",
		ProxyConnectionTimedOut: "Proxy connection timed out",
		InternalError: "SocksClient internal error (this should not happen)",
		InvalidSocks4HandshakeResponse: "Received invalid Socks4 handshake response",
		Socks4ProxyRejectedConnection: "Socks4 Proxy rejected connection",
		InvalidSocks4IncomingConnectionResponse: "Socks4 invalid incoming connection response",
		Socks4ProxyRejectedIncomingBoundConnection: "Socks4 Proxy rejected incoming bound connection",
		InvalidSocks5InitialHandshakeResponse: "Received invalid Socks5 initial handshake response",
		InvalidSocks5IntiailHandshakeSocksVersion: "Received invalid Socks5 initial handshake (invalid socks version)",
		InvalidSocks5InitialHandshakeNoAcceptedAuthType: "Received invalid Socks5 initial handshake (no accepted authentication type)",
		InvalidSocks5InitialHandshakeUnknownAuthType: "Received invalid Socks5 initial handshake (unknown authentication type)",
		Socks5AuthenticationFailed: "Socks5 Authentication failed",
		InvalidSocks5FinalHandshake: "Received invalid Socks5 final handshake response",
		InvalidSocks5FinalHandshakeRejected: "Socks5 proxy rejected connection",
		InvalidSocks5IncomingConnectionResponse: "Received invalid Socks5 incoming connection response",
		Socks5ProxyRejectedIncomingBoundConnection: "Socks5 Proxy rejected incoming bound connection"
	};
	exports.SOCKS_INCOMING_PACKET_SIZES = {
		Socks5InitialHandshakeResponse: 2,
		Socks5UserPassAuthenticationResponse: 2,
		Socks5ResponseHeader: 5,
		Socks5ResponseIPv4: 10,
		Socks5ResponseIPv6: 22,
		Socks5ResponseHostname: (hostNameLength) => hostNameLength + 7,
		Socks4Response: 8
	};
	var SocksCommand;
	(function(SocksCommand) {
		SocksCommand[SocksCommand["connect"] = 1] = "connect";
		SocksCommand[SocksCommand["bind"] = 2] = "bind";
		SocksCommand[SocksCommand["associate"] = 3] = "associate";
	})(SocksCommand || (exports.SocksCommand = SocksCommand = {}));
	var Socks4Response;
	(function(Socks4Response) {
		Socks4Response[Socks4Response["Granted"] = 90] = "Granted";
		Socks4Response[Socks4Response["Failed"] = 91] = "Failed";
		Socks4Response[Socks4Response["Rejected"] = 92] = "Rejected";
		Socks4Response[Socks4Response["RejectedIdent"] = 93] = "RejectedIdent";
	})(Socks4Response || (exports.Socks4Response = Socks4Response = {}));
	var Socks5Auth;
	(function(Socks5Auth) {
		Socks5Auth[Socks5Auth["NoAuth"] = 0] = "NoAuth";
		Socks5Auth[Socks5Auth["GSSApi"] = 1] = "GSSApi";
		Socks5Auth[Socks5Auth["UserPass"] = 2] = "UserPass";
	})(Socks5Auth || (exports.Socks5Auth = Socks5Auth = {}));
	exports.SOCKS5_CUSTOM_AUTH_START = 128;
	exports.SOCKS5_CUSTOM_AUTH_END = 254;
	exports.SOCKS5_NO_ACCEPTABLE_AUTH = 255;
	var Socks5Response;
	(function(Socks5Response) {
		Socks5Response[Socks5Response["Granted"] = 0] = "Granted";
		Socks5Response[Socks5Response["Failure"] = 1] = "Failure";
		Socks5Response[Socks5Response["NotAllowed"] = 2] = "NotAllowed";
		Socks5Response[Socks5Response["NetworkUnreachable"] = 3] = "NetworkUnreachable";
		Socks5Response[Socks5Response["HostUnreachable"] = 4] = "HostUnreachable";
		Socks5Response[Socks5Response["ConnectionRefused"] = 5] = "ConnectionRefused";
		Socks5Response[Socks5Response["TTLExpired"] = 6] = "TTLExpired";
		Socks5Response[Socks5Response["CommandNotSupported"] = 7] = "CommandNotSupported";
		Socks5Response[Socks5Response["AddressNotSupported"] = 8] = "AddressNotSupported";
	})(Socks5Response || (exports.Socks5Response = Socks5Response = {}));
	var Socks5HostType;
	(function(Socks5HostType) {
		Socks5HostType[Socks5HostType["IPv4"] = 1] = "IPv4";
		Socks5HostType[Socks5HostType["Hostname"] = 3] = "Hostname";
		Socks5HostType[Socks5HostType["IPv6"] = 4] = "IPv6";
	})(Socks5HostType || (exports.Socks5HostType = Socks5HostType = {}));
	var SocksClientState;
	(function(SocksClientState) {
		SocksClientState[SocksClientState["Created"] = 0] = "Created";
		SocksClientState[SocksClientState["Connecting"] = 1] = "Connecting";
		SocksClientState[SocksClientState["Connected"] = 2] = "Connected";
		SocksClientState[SocksClientState["SentInitialHandshake"] = 3] = "SentInitialHandshake";
		SocksClientState[SocksClientState["ReceivedInitialHandshakeResponse"] = 4] = "ReceivedInitialHandshakeResponse";
		SocksClientState[SocksClientState["SentAuthentication"] = 5] = "SentAuthentication";
		SocksClientState[SocksClientState["ReceivedAuthenticationResponse"] = 6] = "ReceivedAuthenticationResponse";
		SocksClientState[SocksClientState["SentFinalHandshake"] = 7] = "SentFinalHandshake";
		SocksClientState[SocksClientState["ReceivedFinalResponse"] = 8] = "ReceivedFinalResponse";
		SocksClientState[SocksClientState["BoundWaitingForConnection"] = 9] = "BoundWaitingForConnection";
		SocksClientState[SocksClientState["Established"] = 10] = "Established";
		SocksClientState[SocksClientState["Disconnected"] = 11] = "Disconnected";
		SocksClientState[SocksClientState["Error"] = 99] = "Error";
	})(SocksClientState || (exports.SocksClientState = SocksClientState = {}));
}));
//#endregion
//#region node_modules/socks/build/common/util.js
var require_util = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.SocksClientError = void 0;
	exports.shuffleArray = shuffleArray;
	/**
	* Error wrapper for SocksClient
	*/
	var SocksClientError = class extends Error {
		constructor(message, options) {
			super(message);
			this.options = options;
		}
	};
	exports.SocksClientError = SocksClientError;
	/**
	* Shuffles a given array.
	* @param array The array to shuffle.
	*/
	function shuffleArray(array) {
		for (let i = array.length - 1; i > 0; i--) {
			const j = Math.floor(Math.random() * (i + 1));
			[array[i], array[j]] = [array[j], array[i]];
		}
	}
}));
//#endregion
//#region node_modules/socks/build/common/helpers.js
var require_helpers = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.validateSocksClientOptions = validateSocksClientOptions;
	exports.validateSocksClientChainOptions = validateSocksClientChainOptions;
	exports.ipv4ToInt32 = ipv4ToInt32;
	exports.int32ToIpv4 = int32ToIpv4;
	exports.ipToBuffer = ipToBuffer;
	var util_1 = require_util();
	var constants_1 = require_constants();
	var stream = __require("stream");
	var ip_address_1 = require_ip_address();
	var net$1 = __require("net");
	/**
	* Validates the provided SocksClientOptions
	* @param options { SocksClientOptions }
	* @param acceptedCommands { string[] } A list of accepted SocksProxy commands.
	*/
	function validateSocksClientOptions(options, acceptedCommands = [
		"connect",
		"bind",
		"associate"
	]) {
		if (!constants_1.SocksCommand[options.command]) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksCommand, options);
		if (acceptedCommands.indexOf(options.command) === -1) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksCommandForOperation, options);
		if (!isValidSocksRemoteHost(options.destination)) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsDestination, options);
		if (!isValidSocksProxy(options.proxy)) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsProxy, options);
		validateCustomProxyAuth(options.proxy, options);
		if (options.timeout && !isValidTimeoutValue(options.timeout)) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsTimeout, options);
		if (options.existing_socket && !(options.existing_socket instanceof stream.Duplex)) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsExistingSocket, options);
	}
	/**
	* Validates the SocksClientChainOptions
	* @param options { SocksClientChainOptions }
	*/
	function validateSocksClientChainOptions(options) {
		if (options.command !== "connect") throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksCommandChain, options);
		if (!isValidSocksRemoteHost(options.destination)) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsDestination, options);
		if (!(options.proxies && Array.isArray(options.proxies) && options.proxies.length >= 2)) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsProxiesLength, options);
		options.proxies.forEach((proxy) => {
			if (!isValidSocksProxy(proxy)) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsProxy, options);
			validateCustomProxyAuth(proxy, options);
		});
		if (options.timeout && !isValidTimeoutValue(options.timeout)) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsTimeout, options);
	}
	function validateCustomProxyAuth(proxy, options) {
		if (proxy.custom_auth_method !== void 0) {
			if (proxy.custom_auth_method < constants_1.SOCKS5_CUSTOM_AUTH_START || proxy.custom_auth_method > constants_1.SOCKS5_CUSTOM_AUTH_END) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsCustomAuthRange, options);
			if (proxy.custom_auth_request_handler === void 0 || typeof proxy.custom_auth_request_handler !== "function") throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsCustomAuthOptions, options);
			if (proxy.custom_auth_response_size === void 0) throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsCustomAuthOptions, options);
			if (proxy.custom_auth_response_handler === void 0 || typeof proxy.custom_auth_response_handler !== "function") throw new util_1.SocksClientError(constants_1.ERRORS.InvalidSocksClientOptionsCustomAuthOptions, options);
		}
	}
	/**
	* Validates a SocksRemoteHost
	* @param remoteHost { SocksRemoteHost }
	*/
	function isValidSocksRemoteHost(remoteHost) {
		return remoteHost && typeof remoteHost.host === "string" && Buffer.byteLength(remoteHost.host) < 256 && typeof remoteHost.port === "number" && remoteHost.port >= 0 && remoteHost.port <= 65535;
	}
	/**
	* Validates a SocksProxy
	* @param proxy { SocksProxy }
	*/
	function isValidSocksProxy(proxy) {
		return proxy && (typeof proxy.host === "string" || typeof proxy.ipaddress === "string") && typeof proxy.port === "number" && proxy.port >= 0 && proxy.port <= 65535 && (proxy.type === 4 || proxy.type === 5);
	}
	/**
	* Validates a timeout value.
	* @param value { Number }
	*/
	function isValidTimeoutValue(value) {
		return typeof value === "number" && value > 0;
	}
	function ipv4ToInt32(ip) {
		return new ip_address_1.Address4(ip).toArray().reduce((acc, part) => (acc << 8) + part, 0) >>> 0;
	}
	function int32ToIpv4(int32) {
		return [
			int32 >>> 24 & 255,
			int32 >>> 16 & 255,
			int32 >>> 8 & 255,
			int32 & 255
		].join(".");
	}
	function ipToBuffer(ip) {
		if (net$1.isIPv4(ip)) {
			const address = new ip_address_1.Address4(ip);
			return Buffer.from(address.toArray());
		} else if (net$1.isIPv6(ip)) {
			const address = new ip_address_1.Address6(ip);
			return Buffer.from(address.canonicalForm().split(":").map((segment) => segment.padStart(4, "0")).join(""), "hex");
		} else throw new Error("Invalid IP address format");
	}
}));
//#endregion
//#region node_modules/socks/build/common/receivebuffer.js
var require_receivebuffer = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.ReceiveBuffer = void 0;
	var ReceiveBuffer = class {
		constructor(size = 4096) {
			this.buffer = Buffer.allocUnsafe(size);
			this.offset = 0;
			this.originalSize = size;
		}
		get length() {
			return this.offset;
		}
		append(data) {
			if (!Buffer.isBuffer(data)) throw new Error("Attempted to append a non-buffer instance to ReceiveBuffer.");
			if (this.offset + data.length >= this.buffer.length) {
				const tmp = this.buffer;
				this.buffer = Buffer.allocUnsafe(Math.max(this.buffer.length + this.originalSize, this.buffer.length + data.length));
				tmp.copy(this.buffer);
			}
			data.copy(this.buffer, this.offset);
			return this.offset += data.length;
		}
		peek(length) {
			if (length > this.offset) throw new Error("Attempted to read beyond the bounds of the managed internal data.");
			return this.buffer.slice(0, length);
		}
		get(length) {
			if (length > this.offset) throw new Error("Attempted to read beyond the bounds of the managed internal data.");
			const value = Buffer.allocUnsafe(length);
			this.buffer.slice(0, length).copy(value);
			this.buffer.copyWithin(0, length, length + this.offset - length);
			this.offset -= length;
			return value;
		}
	};
	exports.ReceiveBuffer = ReceiveBuffer;
}));
//#endregion
//#region node_modules/socks/build/client/socksclient.js
var require_socksclient = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __awaiter = exports && exports.__awaiter || function(thisArg, _arguments, P, generator) {
		function adopt(value) {
			return value instanceof P ? value : new P(function(resolve) {
				resolve(value);
			});
		}
		return new (P || (P = Promise))(function(resolve, reject) {
			function fulfilled(value) {
				try {
					step(generator.next(value));
				} catch (e) {
					reject(e);
				}
			}
			function rejected(value) {
				try {
					step(generator["throw"](value));
				} catch (e) {
					reject(e);
				}
			}
			function step(result) {
				result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected);
			}
			step((generator = generator.apply(thisArg, _arguments || [])).next());
		});
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.SocksClientError = exports.SocksClient = void 0;
	var events_1 = __require("events");
	var net = __require("net");
	var smart_buffer_1 = require_smartbuffer();
	var constants_1 = require_constants();
	var helpers_1 = require_helpers();
	var receivebuffer_1 = require_receivebuffer();
	var util_1 = require_util();
	Object.defineProperty(exports, "SocksClientError", {
		enumerable: true,
		get: function() {
			return util_1.SocksClientError;
		}
	});
	var ip_address_1 = require_ip_address();
	exports.SocksClient = class SocksClient extends events_1.EventEmitter {
		constructor(options) {
			super();
			this.options = Object.assign({}, options);
			(0, helpers_1.validateSocksClientOptions)(options);
			this.setState(constants_1.SocksClientState.Created);
		}
		/**
		* Creates a new SOCKS connection.
		*
		* Note: Supports callbacks and promises. Only supports the connect command.
		* @param options { SocksClientOptions } Options.
		* @param callback { Function } An optional callback function.
		* @returns { Promise }
		*/
		static createConnection(options, callback) {
			return new Promise((resolve, reject) => {
				try {
					(0, helpers_1.validateSocksClientOptions)(options, ["connect"]);
				} catch (err) {
					if (typeof callback === "function") {
						callback(err);
						return resolve(err);
					} else return reject(err);
				}
				const client = new SocksClient(options);
				client.connect(options.existing_socket);
				client.once("established", (info) => {
					client.removeAllListeners();
					if (typeof callback === "function") {
						callback(null, info);
						resolve(info);
					} else resolve(info);
				});
				client.once("error", (err) => {
					client.removeAllListeners();
					if (typeof callback === "function") {
						callback(err);
						resolve(err);
					} else reject(err);
				});
			});
		}
		/**
		* Creates a new SOCKS connection chain to a destination host through 2 or more SOCKS proxies.
		*
		* Note: Supports callbacks and promises. Only supports the connect method.
		* Note: Implemented via createConnection() factory function.
		* @param options { SocksClientChainOptions } Options
		* @param callback { Function } An optional callback function.
		* @returns { Promise }
		*/
		static createConnectionChain(options, callback) {
			return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
				try {
					(0, helpers_1.validateSocksClientChainOptions)(options);
				} catch (err) {
					if (typeof callback === "function") {
						callback(err);
						return resolve(err);
					} else return reject(err);
				}
				if (options.randomizeChain) (0, util_1.shuffleArray)(options.proxies);
				try {
					let sock;
					for (let i = 0; i < options.proxies.length; i++) {
						const nextProxy = options.proxies[i];
						const nextDestination = i === options.proxies.length - 1 ? options.destination : {
							host: options.proxies[i + 1].host || options.proxies[i + 1].ipaddress,
							port: options.proxies[i + 1].port
						};
						const result = yield SocksClient.createConnection({
							command: "connect",
							proxy: nextProxy,
							destination: nextDestination,
							existing_socket: sock
						});
						sock = sock || result.socket;
					}
					if (typeof callback === "function") {
						callback(null, { socket: sock });
						resolve({ socket: sock });
					} else resolve({ socket: sock });
				} catch (err) {
					if (typeof callback === "function") {
						callback(err);
						resolve(err);
					} else reject(err);
				}
			}));
		}
		/**
		* Creates a SOCKS UDP Frame.
		* @param options
		*/
		static createUDPFrame(options) {
			const buff = new smart_buffer_1.SmartBuffer();
			buff.writeUInt16BE(0);
			buff.writeUInt8(options.frameNumber || 0);
			if (net.isIPv4(options.remoteHost.host)) {
				buff.writeUInt8(constants_1.Socks5HostType.IPv4);
				buff.writeUInt32BE((0, helpers_1.ipv4ToInt32)(options.remoteHost.host));
			} else if (net.isIPv6(options.remoteHost.host)) {
				buff.writeUInt8(constants_1.Socks5HostType.IPv6);
				buff.writeBuffer((0, helpers_1.ipToBuffer)(options.remoteHost.host));
			} else {
				buff.writeUInt8(constants_1.Socks5HostType.Hostname);
				buff.writeUInt8(Buffer.byteLength(options.remoteHost.host));
				buff.writeString(options.remoteHost.host);
			}
			buff.writeUInt16BE(options.remoteHost.port);
			buff.writeBuffer(options.data);
			return buff.toBuffer();
		}
		/**
		* Parses a SOCKS UDP frame.
		* @param data
		*/
		static parseUDPFrame(data) {
			const buff = smart_buffer_1.SmartBuffer.fromBuffer(data);
			buff.readOffset = 2;
			const frameNumber = buff.readUInt8();
			const hostType = buff.readUInt8();
			let remoteHost;
			if (hostType === constants_1.Socks5HostType.IPv4) remoteHost = (0, helpers_1.int32ToIpv4)(buff.readUInt32BE());
			else if (hostType === constants_1.Socks5HostType.IPv6) remoteHost = ip_address_1.Address6.fromByteArray(Array.from(buff.readBuffer(16))).canonicalForm();
			else remoteHost = buff.readString(buff.readUInt8());
			const remotePort = buff.readUInt16BE();
			return {
				frameNumber,
				remoteHost: {
					host: remoteHost,
					port: remotePort
				},
				data: buff.readBuffer()
			};
		}
		/**
		* Internal state setter. If the SocksClient is in an error state, it cannot be changed to a non error state.
		*/
		setState(newState) {
			if (this.state !== constants_1.SocksClientState.Error) this.state = newState;
		}
		/**
		* Starts the connection establishment to the proxy and destination.
		* @param existingSocket Connected socket to use instead of creating a new one (internal use).
		*/
		connect(existingSocket) {
			this.onDataReceived = (data) => this.onDataReceivedHandler(data);
			this.onClose = () => this.onCloseHandler();
			this.onError = (err) => this.onErrorHandler(err);
			this.onConnect = () => this.onConnectHandler();
			const timer = setTimeout(() => this.onEstablishedTimeout(), this.options.timeout || constants_1.DEFAULT_TIMEOUT);
			if (timer.unref && typeof timer.unref === "function") timer.unref();
			if (existingSocket) this.socket = existingSocket;
			else this.socket = new net.Socket();
			this.socket.once("close", this.onClose);
			this.socket.once("error", this.onError);
			this.socket.once("connect", this.onConnect);
			this.socket.on("data", this.onDataReceived);
			this.setState(constants_1.SocksClientState.Connecting);
			this.receiveBuffer = new receivebuffer_1.ReceiveBuffer();
			if (existingSocket) this.socket.emit("connect");
			else {
				this.socket.connect(this.getSocketOptions());
				if (this.options.set_tcp_nodelay !== void 0 && this.options.set_tcp_nodelay !== null) this.socket.setNoDelay(!!this.options.set_tcp_nodelay);
			}
			this.prependOnceListener("established", (info) => {
				setImmediate(() => {
					if (this.receiveBuffer.length > 0) {
						const excessData = this.receiveBuffer.get(this.receiveBuffer.length);
						info.socket.emit("data", excessData);
					}
					info.socket.resume();
				});
			});
		}
		getSocketOptions() {
			return Object.assign(Object.assign({}, this.options.socket_options), {
				host: this.options.proxy.host || this.options.proxy.ipaddress,
				port: this.options.proxy.port
			});
		}
		/**
		* Handles internal Socks timeout callback.
		* Note: If the Socks client is not BoundWaitingForConnection or Established, the connection will be closed.
		*/
		onEstablishedTimeout() {
			if (this.state !== constants_1.SocksClientState.Established && this.state !== constants_1.SocksClientState.BoundWaitingForConnection) this.closeSocket(constants_1.ERRORS.ProxyConnectionTimedOut);
		}
		/**
		* Handles Socket connect event.
		*/
		onConnectHandler() {
			this.setState(constants_1.SocksClientState.Connected);
			if (this.options.proxy.type === 4) this.sendSocks4InitialHandshake();
			else this.sendSocks5InitialHandshake();
			this.setState(constants_1.SocksClientState.SentInitialHandshake);
		}
		/**
		* Handles Socket data event.
		* @param data
		*/
		onDataReceivedHandler(data) {
			this.receiveBuffer.append(data);
			this.processData();
		}
		/**
		* Handles processing of the data we have received.
		*/
		processData() {
			while (this.state !== constants_1.SocksClientState.Established && this.state !== constants_1.SocksClientState.Error && this.receiveBuffer.length >= this.nextRequiredPacketBufferSize) if (this.state === constants_1.SocksClientState.SentInitialHandshake) {
				if (this.options.proxy.type === 4) this.handleSocks4FinalHandshakeResponse();
				else this.handleInitialSocks5HandshakeResponse();
			} else if (this.state === constants_1.SocksClientState.SentAuthentication) this.handleInitialSocks5AuthenticationHandshakeResponse();
			else if (this.state === constants_1.SocksClientState.SentFinalHandshake) this.handleSocks5FinalHandshakeResponse();
			else if (this.state === constants_1.SocksClientState.BoundWaitingForConnection) {
				if (this.options.proxy.type === 4) this.handleSocks4IncomingConnectionResponse();
				else this.handleSocks5IncomingConnectionResponse();
			} else {
				this.closeSocket(constants_1.ERRORS.InternalError);
				break;
			}
		}
		/**
		* Handles Socket close event.
		* @param had_error
		*/
		onCloseHandler() {
			this.closeSocket(constants_1.ERRORS.SocketClosed);
		}
		/**
		* Handles Socket error event.
		* @param err
		*/
		onErrorHandler(err) {
			this.closeSocket(err.message);
		}
		/**
		* Removes internal event listeners on the underlying Socket.
		*/
		removeInternalSocketHandlers() {
			this.socket.pause();
			this.socket.removeListener("data", this.onDataReceived);
			this.socket.removeListener("close", this.onClose);
			this.socket.removeListener("error", this.onError);
			this.socket.removeListener("connect", this.onConnect);
		}
		/**
		* Closes and destroys the underlying Socket. Emits an error event.
		* @param err { String } An error string to include in error event.
		*/
		closeSocket(err) {
			if (this.state !== constants_1.SocksClientState.Error) {
				this.setState(constants_1.SocksClientState.Error);
				this.socket.destroy();
				this.removeInternalSocketHandlers();
				this.emit("error", new util_1.SocksClientError(err, this.options));
			}
		}
		/**
		* Sends initial Socks v4 handshake request.
		*/
		sendSocks4InitialHandshake() {
			const userId = this.options.proxy.userId || "";
			const buff = new smart_buffer_1.SmartBuffer();
			buff.writeUInt8(4);
			buff.writeUInt8(constants_1.SocksCommand[this.options.command]);
			buff.writeUInt16BE(this.options.destination.port);
			if (net.isIPv4(this.options.destination.host)) {
				buff.writeBuffer((0, helpers_1.ipToBuffer)(this.options.destination.host));
				buff.writeStringNT(userId);
			} else {
				buff.writeUInt8(0);
				buff.writeUInt8(0);
				buff.writeUInt8(0);
				buff.writeUInt8(1);
				buff.writeStringNT(userId);
				buff.writeStringNT(this.options.destination.host);
			}
			this.nextRequiredPacketBufferSize = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks4Response;
			this.socket.write(buff.toBuffer());
		}
		/**
		* Handles Socks v4 handshake response.
		* @param data
		*/
		handleSocks4FinalHandshakeResponse() {
			const data = this.receiveBuffer.get(8);
			if (data[1] !== constants_1.Socks4Response.Granted) this.closeSocket(`${constants_1.ERRORS.Socks4ProxyRejectedConnection} - (${constants_1.Socks4Response[data[1]]})`);
			else if (constants_1.SocksCommand[this.options.command] === constants_1.SocksCommand.bind) {
				const buff = smart_buffer_1.SmartBuffer.fromBuffer(data);
				buff.readOffset = 2;
				const remoteHost = {
					port: buff.readUInt16BE(),
					host: (0, helpers_1.int32ToIpv4)(buff.readUInt32BE())
				};
				if (remoteHost.host === "0.0.0.0") remoteHost.host = this.options.proxy.ipaddress;
				this.setState(constants_1.SocksClientState.BoundWaitingForConnection);
				this.emit("bound", {
					remoteHost,
					socket: this.socket
				});
			} else {
				this.setState(constants_1.SocksClientState.Established);
				this.removeInternalSocketHandlers();
				this.emit("established", { socket: this.socket });
			}
		}
		/**
		* Handles Socks v4 incoming connection request (BIND)
		* @param data
		*/
		handleSocks4IncomingConnectionResponse() {
			const data = this.receiveBuffer.get(8);
			if (data[1] !== constants_1.Socks4Response.Granted) this.closeSocket(`${constants_1.ERRORS.Socks4ProxyRejectedIncomingBoundConnection} - (${constants_1.Socks4Response[data[1]]})`);
			else {
				const buff = smart_buffer_1.SmartBuffer.fromBuffer(data);
				buff.readOffset = 2;
				const remoteHost = {
					port: buff.readUInt16BE(),
					host: (0, helpers_1.int32ToIpv4)(buff.readUInt32BE())
				};
				this.setState(constants_1.SocksClientState.Established);
				this.removeInternalSocketHandlers();
				this.emit("established", {
					remoteHost,
					socket: this.socket
				});
			}
		}
		/**
		* Sends initial Socks v5 handshake request.
		*/
		sendSocks5InitialHandshake() {
			const buff = new smart_buffer_1.SmartBuffer();
			const supportedAuthMethods = [constants_1.Socks5Auth.NoAuth];
			if (this.options.proxy.userId || this.options.proxy.password) supportedAuthMethods.push(constants_1.Socks5Auth.UserPass);
			if (this.options.proxy.custom_auth_method !== void 0) supportedAuthMethods.push(this.options.proxy.custom_auth_method);
			buff.writeUInt8(5);
			buff.writeUInt8(supportedAuthMethods.length);
			for (const authMethod of supportedAuthMethods) buff.writeUInt8(authMethod);
			this.nextRequiredPacketBufferSize = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5InitialHandshakeResponse;
			this.socket.write(buff.toBuffer());
			this.setState(constants_1.SocksClientState.SentInitialHandshake);
		}
		/**
		* Handles initial Socks v5 handshake response.
		* @param data
		*/
		handleInitialSocks5HandshakeResponse() {
			const data = this.receiveBuffer.get(2);
			if (data[0] !== 5) this.closeSocket(constants_1.ERRORS.InvalidSocks5IntiailHandshakeSocksVersion);
			else if (data[1] === constants_1.SOCKS5_NO_ACCEPTABLE_AUTH) this.closeSocket(constants_1.ERRORS.InvalidSocks5InitialHandshakeNoAcceptedAuthType);
			else if (data[1] === constants_1.Socks5Auth.NoAuth) {
				this.socks5ChosenAuthType = constants_1.Socks5Auth.NoAuth;
				this.sendSocks5CommandRequest();
			} else if (data[1] === constants_1.Socks5Auth.UserPass) {
				this.socks5ChosenAuthType = constants_1.Socks5Auth.UserPass;
				this.sendSocks5UserPassAuthentication();
			} else if (data[1] === this.options.proxy.custom_auth_method) {
				this.socks5ChosenAuthType = this.options.proxy.custom_auth_method;
				this.sendSocks5CustomAuthentication();
			} else this.closeSocket(constants_1.ERRORS.InvalidSocks5InitialHandshakeUnknownAuthType);
		}
		/**
		* Sends Socks v5 user & password auth handshake.
		*
		* Note: No auth and user/pass are currently supported.
		*/
		sendSocks5UserPassAuthentication() {
			const userId = this.options.proxy.userId || "";
			const password = this.options.proxy.password || "";
			const buff = new smart_buffer_1.SmartBuffer();
			buff.writeUInt8(1);
			buff.writeUInt8(Buffer.byteLength(userId));
			buff.writeString(userId);
			buff.writeUInt8(Buffer.byteLength(password));
			buff.writeString(password);
			this.nextRequiredPacketBufferSize = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5UserPassAuthenticationResponse;
			this.socket.write(buff.toBuffer());
			this.setState(constants_1.SocksClientState.SentAuthentication);
		}
		sendSocks5CustomAuthentication() {
			return __awaiter(this, void 0, void 0, function* () {
				this.nextRequiredPacketBufferSize = this.options.proxy.custom_auth_response_size;
				this.socket.write(yield this.options.proxy.custom_auth_request_handler());
				this.setState(constants_1.SocksClientState.SentAuthentication);
			});
		}
		handleSocks5CustomAuthHandshakeResponse(data) {
			return __awaiter(this, void 0, void 0, function* () {
				return yield this.options.proxy.custom_auth_response_handler(data);
			});
		}
		handleSocks5AuthenticationNoAuthHandshakeResponse(data) {
			return __awaiter(this, void 0, void 0, function* () {
				return data[1] === 0;
			});
		}
		handleSocks5AuthenticationUserPassHandshakeResponse(data) {
			return __awaiter(this, void 0, void 0, function* () {
				return data[1] === 0;
			});
		}
		/**
		* Handles Socks v5 auth handshake response.
		* @param data
		*/
		handleInitialSocks5AuthenticationHandshakeResponse() {
			return __awaiter(this, void 0, void 0, function* () {
				this.setState(constants_1.SocksClientState.ReceivedAuthenticationResponse);
				let authResult = false;
				if (this.socks5ChosenAuthType === constants_1.Socks5Auth.NoAuth) authResult = yield this.handleSocks5AuthenticationNoAuthHandshakeResponse(this.receiveBuffer.get(2));
				else if (this.socks5ChosenAuthType === constants_1.Socks5Auth.UserPass) authResult = yield this.handleSocks5AuthenticationUserPassHandshakeResponse(this.receiveBuffer.get(2));
				else if (this.socks5ChosenAuthType === this.options.proxy.custom_auth_method) authResult = yield this.handleSocks5CustomAuthHandshakeResponse(this.receiveBuffer.get(this.options.proxy.custom_auth_response_size));
				if (!authResult) this.closeSocket(constants_1.ERRORS.Socks5AuthenticationFailed);
				else this.sendSocks5CommandRequest();
			});
		}
		/**
		* Sends Socks v5 final handshake request.
		*/
		sendSocks5CommandRequest() {
			const buff = new smart_buffer_1.SmartBuffer();
			buff.writeUInt8(5);
			buff.writeUInt8(constants_1.SocksCommand[this.options.command]);
			buff.writeUInt8(0);
			if (net.isIPv4(this.options.destination.host)) {
				buff.writeUInt8(constants_1.Socks5HostType.IPv4);
				buff.writeBuffer((0, helpers_1.ipToBuffer)(this.options.destination.host));
			} else if (net.isIPv6(this.options.destination.host)) {
				buff.writeUInt8(constants_1.Socks5HostType.IPv6);
				buff.writeBuffer((0, helpers_1.ipToBuffer)(this.options.destination.host));
			} else {
				buff.writeUInt8(constants_1.Socks5HostType.Hostname);
				buff.writeUInt8(this.options.destination.host.length);
				buff.writeString(this.options.destination.host);
			}
			buff.writeUInt16BE(this.options.destination.port);
			this.nextRequiredPacketBufferSize = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5ResponseHeader;
			this.socket.write(buff.toBuffer());
			this.setState(constants_1.SocksClientState.SentFinalHandshake);
		}
		/**
		* Handles Socks v5 final handshake response.
		* @param data
		*/
		handleSocks5FinalHandshakeResponse() {
			const header = this.receiveBuffer.peek(5);
			if (header[0] !== 5 || header[1] !== constants_1.Socks5Response.Granted) this.closeSocket(`${constants_1.ERRORS.InvalidSocks5FinalHandshakeRejected} - ${constants_1.Socks5Response[header[1]]}`);
			else {
				const addressType = header[3];
				let remoteHost;
				let buff;
				if (addressType === constants_1.Socks5HostType.IPv4) {
					const dataNeeded = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5ResponseIPv4;
					if (this.receiveBuffer.length < dataNeeded) {
						this.nextRequiredPacketBufferSize = dataNeeded;
						return;
					}
					buff = smart_buffer_1.SmartBuffer.fromBuffer(this.receiveBuffer.get(dataNeeded).slice(4));
					remoteHost = {
						host: (0, helpers_1.int32ToIpv4)(buff.readUInt32BE()),
						port: buff.readUInt16BE()
					};
					if (remoteHost.host === "0.0.0.0") remoteHost.host = this.options.proxy.ipaddress;
				} else if (addressType === constants_1.Socks5HostType.Hostname) {
					const hostLength = header[4];
					const dataNeeded = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5ResponseHostname(hostLength);
					if (this.receiveBuffer.length < dataNeeded) {
						this.nextRequiredPacketBufferSize = dataNeeded;
						return;
					}
					buff = smart_buffer_1.SmartBuffer.fromBuffer(this.receiveBuffer.get(dataNeeded).slice(5));
					remoteHost = {
						host: buff.readString(hostLength),
						port: buff.readUInt16BE()
					};
				} else if (addressType === constants_1.Socks5HostType.IPv6) {
					const dataNeeded = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5ResponseIPv6;
					if (this.receiveBuffer.length < dataNeeded) {
						this.nextRequiredPacketBufferSize = dataNeeded;
						return;
					}
					buff = smart_buffer_1.SmartBuffer.fromBuffer(this.receiveBuffer.get(dataNeeded).slice(4));
					remoteHost = {
						host: ip_address_1.Address6.fromByteArray(Array.from(buff.readBuffer(16))).canonicalForm(),
						port: buff.readUInt16BE()
					};
				}
				this.setState(constants_1.SocksClientState.ReceivedFinalResponse);
				if (constants_1.SocksCommand[this.options.command] === constants_1.SocksCommand.connect) {
					this.setState(constants_1.SocksClientState.Established);
					this.removeInternalSocketHandlers();
					this.emit("established", {
						remoteHost,
						socket: this.socket
					});
				} else if (constants_1.SocksCommand[this.options.command] === constants_1.SocksCommand.bind) {
					this.setState(constants_1.SocksClientState.BoundWaitingForConnection);
					this.nextRequiredPacketBufferSize = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5ResponseHeader;
					this.emit("bound", {
						remoteHost,
						socket: this.socket
					});
				} else if (constants_1.SocksCommand[this.options.command] === constants_1.SocksCommand.associate) {
					this.setState(constants_1.SocksClientState.Established);
					this.removeInternalSocketHandlers();
					this.emit("established", {
						remoteHost,
						socket: this.socket
					});
				}
			}
		}
		/**
		* Handles Socks v5 incoming connection request (BIND).
		*/
		handleSocks5IncomingConnectionResponse() {
			const header = this.receiveBuffer.peek(5);
			if (header[0] !== 5 || header[1] !== constants_1.Socks5Response.Granted) this.closeSocket(`${constants_1.ERRORS.Socks5ProxyRejectedIncomingBoundConnection} - ${constants_1.Socks5Response[header[1]]}`);
			else {
				const addressType = header[3];
				let remoteHost;
				let buff;
				if (addressType === constants_1.Socks5HostType.IPv4) {
					const dataNeeded = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5ResponseIPv4;
					if (this.receiveBuffer.length < dataNeeded) {
						this.nextRequiredPacketBufferSize = dataNeeded;
						return;
					}
					buff = smart_buffer_1.SmartBuffer.fromBuffer(this.receiveBuffer.get(dataNeeded).slice(4));
					remoteHost = {
						host: (0, helpers_1.int32ToIpv4)(buff.readUInt32BE()),
						port: buff.readUInt16BE()
					};
					if (remoteHost.host === "0.0.0.0") remoteHost.host = this.options.proxy.ipaddress;
				} else if (addressType === constants_1.Socks5HostType.Hostname) {
					const hostLength = header[4];
					const dataNeeded = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5ResponseHostname(hostLength);
					if (this.receiveBuffer.length < dataNeeded) {
						this.nextRequiredPacketBufferSize = dataNeeded;
						return;
					}
					buff = smart_buffer_1.SmartBuffer.fromBuffer(this.receiveBuffer.get(dataNeeded).slice(5));
					remoteHost = {
						host: buff.readString(hostLength),
						port: buff.readUInt16BE()
					};
				} else if (addressType === constants_1.Socks5HostType.IPv6) {
					const dataNeeded = constants_1.SOCKS_INCOMING_PACKET_SIZES.Socks5ResponseIPv6;
					if (this.receiveBuffer.length < dataNeeded) {
						this.nextRequiredPacketBufferSize = dataNeeded;
						return;
					}
					buff = smart_buffer_1.SmartBuffer.fromBuffer(this.receiveBuffer.get(dataNeeded).slice(4));
					remoteHost = {
						host: ip_address_1.Address6.fromByteArray(Array.from(buff.readBuffer(16))).canonicalForm(),
						port: buff.readUInt16BE()
					};
				}
				this.setState(constants_1.SocksClientState.Established);
				this.removeInternalSocketHandlers();
				this.emit("established", {
					remoteHost,
					socket: this.socket
				});
			}
		}
		get socksClientOptions() {
			return Object.assign({}, this.options);
		}
	};
}));
//#endregion
//#region node_modules/socks/build/index.js
var require_build$1 = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __createBinding = exports && exports.__createBinding || (Object.create ? (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		var desc = Object.getOwnPropertyDescriptor(m, k);
		if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) desc = {
			enumerable: true,
			get: function() {
				return m[k];
			}
		};
		Object.defineProperty(o, k2, desc);
	}) : (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		o[k2] = m[k];
	}));
	var __exportStar = exports && exports.__exportStar || function(m, exports$4) {
		for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports$4, p)) __createBinding(exports$4, m, p);
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	__exportStar(require_socksclient(), exports);
}));
//#endregion
//#region node_modules/mqtt/build/lib/connect/socks.js
var require_socks = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __createBinding = exports && exports.__createBinding || (Object.create ? (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		var desc = Object.getOwnPropertyDescriptor(m, k);
		if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) desc = {
			enumerable: true,
			get: function() {
				return m[k];
			}
		};
		Object.defineProperty(o, k2, desc);
	}) : (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		o[k2] = m[k];
	}));
	var __setModuleDefault = exports && exports.__setModuleDefault || (Object.create ? (function(o, v) {
		Object.defineProperty(o, "default", {
			enumerable: true,
			value: v
		});
	}) : function(o, v) {
		o["default"] = v;
	});
	var __importStar = exports && exports.__importStar || (function() {
		var ownKeys = function(o) {
			ownKeys = Object.getOwnPropertyNames || function(o) {
				var ar = [];
				for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
				return ar;
			};
			return ownKeys(o);
		};
		return function(mod) {
			if (mod && mod.__esModule) return mod;
			var result = {};
			if (mod != null) {
				for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
			}
			__setModuleDefault(result, mod);
			return result;
		};
	})();
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.default = openSocks;
	var debug_1 = __importDefault(require_src());
	var stream_1 = __require("stream");
	var socks_1 = require_build$1();
	var dns = __importStar(__require("dns"));
	var util_1 = __require("util");
	var assert_1 = __importDefault(__require("assert"));
	var debug = (0, debug_1.default)("mqttjs:socks");
	var ProxyStream = class extends stream_1.Duplex {
		_flowing = false;
		_socket;
		constructor() {
			super({ autoDestroy: false });
			this.cork();
		}
		_start(socket) {
			debug("proxy stream started");
			(0, assert_1.default)(!this._socket);
			if (this.destroyed) {
				socket.destroy(this.errored);
				return;
			}
			this._socket = socket;
			if (!this._flowing) socket.pause();
			socket.on("data", this._onData);
			socket.on("end", this._onEnd);
			socket.on("error", this._onError);
			socket.on("close", this._onClose);
			socket.emit("connect");
			this.uncork();
		}
		_write(chunk, encoding, callback) {
			(0, assert_1.default)(this._socket);
			this._socket.write(chunk, callback);
		}
		_read(size) {
			this._flowing = true;
			this._socket?.resume?.();
		}
		_destroy(error, callback) {
			this._socket?.destroy?.(error);
			callback(error);
		}
		_onData = (chunk) => {
			(0, assert_1.default)(this._socket);
			this._flowing = this.push(chunk);
			if (!this._flowing) this._socket.pause();
		};
		_onEnd = () => {
			debug("proxy stream received EOF");
			this.push(null);
		};
		_onClose = () => {
			debug("proxy stream closed");
			this.destroy();
		};
		_onError = (err) => {
			debug("proxy stream died with error %s", err);
			this.destroy(err);
		};
	};
	function fatal(e) {
		try {
			if (e.code === void 0) e.code = "SOCKS";
			return e;
		} catch {
			return e;
		}
	}
	function typeFromProtocol(proto) {
		switch (proto) {
			case "socks5h:": return [5, true];
			case "socks4a:": return [4, true];
			case "socks5:": return [5, false];
			case "socks4:": return [4, false];
			default: return [void 0, false];
		}
	}
	function parseSocksUrl(url) {
		const parsedUrl = new URL(url);
		if (parsedUrl.pathname || parsedUrl.hash || parsedUrl.search) throw fatal(/* @__PURE__ */ new Error("bad SOCKS URL"));
		const [type, resolveThroughProxy] = typeFromProtocol(parsedUrl.protocol);
		if (!type) throw fatal(/* @__PURE__ */ new Error("bad SOCKS URL: invalid protocol"));
		const port = parseInt(parsedUrl.port, 10);
		if (Number.isNaN(port)) throw fatal(/* @__PURE__ */ new Error("bad SOCKS URL: invalid port"));
		return [{
			host: parsedUrl.hostname,
			port,
			type
		}, resolveThroughProxy];
	}
	async function connectSocks(destinationHost, destinationPort, socksUrl, stream, options = {}) {
		const lookup = options.lookup ?? (0, util_1.promisify)(dns.lookup);
		const [proxy, resolveThroughProxy] = parseSocksUrl(socksUrl);
		if (!resolveThroughProxy) {
			debug("resolving %s locally", destinationHost);
			destinationHost = (await lookup(destinationHost, { family: proxy.type === 4 ? 4 : 0 })).address;
		}
		debug("establishing SOCKS%d connection to %s:%d via %s:%d", proxy.type, destinationHost, destinationPort, proxy.host, proxy.port);
		const socksClient = new socks_1.SocksClient({
			command: "connect",
			destination: {
				host: destinationHost,
				port: destinationPort
			},
			proxy: { ...proxy },
			timeout: options.timeout
		});
		socksClient.connect();
		socksClient.on("established", ({ socket }) => stream._start(socket));
		socksClient.on("error", (e) => {
			debug("SOCKS failed: %s", e);
			stream.destroy(fatal(e));
		});
	}
	function openSocks(destinationHost, destinationPort, socksUrl, options) {
		debug("SOCKS connection to %s:%d via %s", destinationHost, destinationPort, socksUrl);
		const stream = new ProxyStream();
		connectSocks(destinationHost, destinationPort, socksUrl, stream, options).catch((e) => {
			debug("SOCKS failed: %s", e);
			stream.destroy(e);
		});
		return stream;
	}
}));
//#endregion
//#region node_modules/mqtt/build/lib/connect/tcp.js
var require_tcp = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	var net_1$1 = __importDefault(__require("net"));
	var debug_1 = __importDefault(require_src());
	var socks_1 = __importDefault(require_socks());
	var debug = (0, debug_1.default)("mqttjs:tcp");
	var buildStream = (client, opts) => {
		opts.port = opts.port || 1883;
		opts.hostname = opts.hostname || opts.host || "localhost";
		if (opts.socksProxy) return (0, socks_1.default)(opts.hostname, opts.port, opts.socksProxy, { timeout: opts.socksTimeout });
		const { port, path } = opts;
		const host = opts.hostname;
		debug("port %d and host %s", port, host);
		return net_1$1.default.createConnection({
			port,
			host,
			path
		});
	};
	exports.default = buildStream;
}));
//#endregion
//#region node_modules/mqtt/build/lib/connect/tls.js
var require_tls = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	var tls_1 = __require("tls");
	var net_1 = __importDefault(__require("net"));
	var debug_1 = __importDefault(require_src());
	var socks_1 = __importDefault(require_socks());
	var debug = (0, debug_1.default)("mqttjs:tls");
	function connect(opts) {
		const { host, port, socksProxy, ...rest } = opts;
		if (socksProxy !== void 0) {
			const socket = (0, socks_1.default)(host, port, socksProxy, { timeout: opts.socksTimeout });
			return (0, tls_1.connect)({
				...rest,
				socket
			});
		}
		return (0, tls_1.connect)(opts);
	}
	var buildStream = (client, opts) => {
		opts.port = opts.port || 8883;
		opts.host = opts.hostname || opts.host || "localhost";
		if (net_1.default.isIP(opts.host) === 0 && !opts.servername) opts.servername = opts.host;
		opts.rejectUnauthorized = opts.rejectUnauthorized !== false;
		delete opts.path;
		debug("port %d host %s rejectUnauthorized %b", opts.port, opts.host, opts.rejectUnauthorized);
		const connection = connect(opts);
		connection.on("secureConnect", () => {
			if (opts.rejectUnauthorized && !connection.authorized) connection.emit("error", /* @__PURE__ */ new Error("TLS not authorized"));
			else connection.removeListener("error", handleTLSerrors);
		});
		function handleTLSerrors(err) {
			if (opts.rejectUnauthorized) client.emit("error", err);
			connection.end();
		}
		connection.on("error", handleTLSerrors);
		return connection;
	};
	exports.default = buildStream;
}));
//#endregion
//#region node_modules/mqtt/build/lib/connect/wx.js
var require_wx = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var buffer_1$1 = __require("buffer");
	var readable_stream_1 = require_ours();
	var BufferedDuplex_1 = require_BufferedDuplex();
	var socketTask;
	var proxy;
	var stream;
	function buildProxy() {
		const _proxy = new readable_stream_1.Transform();
		_proxy._write = (chunk, encoding, next) => {
			socketTask.send({
				data: chunk.buffer,
				success() {
					next();
				},
				fail(errMsg) {
					next(new Error(errMsg));
				}
			});
		};
		_proxy._flush = (done) => {
			socketTask.close({ success() {
				done();
			} });
		};
		return _proxy;
	}
	function setDefaultOpts(opts) {
		if (!opts.hostname) opts.hostname = "localhost";
		if (!opts.path) opts.path = "/";
		if (!opts.wsOptions) opts.wsOptions = {};
	}
	function buildUrl(opts, client) {
		const protocol = opts.protocol === "wxs" ? "wss" : "ws";
		let url = `${protocol}://${opts.hostname}${opts.path}`;
		if (opts.port && opts.port !== 80 && opts.port !== 443) url = `${protocol}://${opts.hostname}:${opts.port}${opts.path}`;
		if (typeof opts.transformWsUrl === "function") url = opts.transformWsUrl(url, opts, client);
		return url;
	}
	function bindEventHandler() {
		socketTask.onOpen(() => {
			stream.socketReady();
		});
		socketTask.onMessage((res) => {
			let { data } = res;
			if (data instanceof ArrayBuffer) data = buffer_1$1.Buffer.from(data);
			else data = buffer_1$1.Buffer.from(data, "utf8");
			proxy.push(data);
		});
		socketTask.onClose(() => {
			stream.emit("close");
			stream.end();
			stream.destroy();
		});
		socketTask.onError((error) => {
			const err = new Error(error.errMsg);
			stream.destroy(err);
		});
	}
	var buildStream = (client, opts) => {
		opts.hostname = opts.hostname || opts.host;
		if (!opts.hostname) throw new Error("Could not determine host. Specify host manually.");
		const websocketSubProtocol = opts.protocolId === "MQIsdp" && opts.protocolVersion === 3 ? "mqttv3.1" : "mqtt";
		setDefaultOpts(opts);
		const url = buildUrl(opts, client);
		socketTask = wx.connectSocket({
			url,
			protocols: [websocketSubProtocol]
		});
		proxy = buildProxy();
		stream = new BufferedDuplex_1.BufferedDuplex(opts, proxy, socketTask);
		stream._destroy = (err, cb) => {
			socketTask.close({ success() {
				if (cb) cb(err);
			} });
		};
		const destroyRef = stream.destroy;
		stream.destroy = (err, cb) => {
			stream.destroy = destroyRef;
			setTimeout(() => {
				socketTask.close({ fail() {
					stream._destroy(err, cb);
				} });
			}, 0);
			return stream;
		};
		bindEventHandler();
		return stream;
	};
	exports.default = buildStream;
}));
//#endregion
//#region node_modules/mqtt/build/lib/connect/ali.js
var require_ali = /* @__PURE__ */ __commonJSMin(((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	var buffer_1 = __require("buffer");
	var readable_stream_1 = require_ours();
	var BufferedDuplex_1 = require_BufferedDuplex();
	var my;
	var proxy;
	var stream;
	var isInitialized = false;
	function buildProxy() {
		const _proxy = new readable_stream_1.Transform();
		_proxy._write = (chunk, encoding, next) => {
			my.sendSocketMessage({
				data: chunk.buffer,
				success() {
					next();
				},
				fail() {
					next(/* @__PURE__ */ new Error());
				}
			});
		};
		_proxy._flush = (done) => {
			my.closeSocket({ success() {
				done();
			} });
		};
		return _proxy;
	}
	function setDefaultOpts(opts) {
		if (!opts.hostname) opts.hostname = "localhost";
		if (!opts.path) opts.path = "/";
		if (!opts.wsOptions) opts.wsOptions = {};
	}
	function buildUrl(opts, client) {
		const protocol = opts.protocol === "alis" ? "wss" : "ws";
		let url = `${protocol}://${opts.hostname}${opts.path}`;
		if (opts.port && opts.port !== 80 && opts.port !== 443) url = `${protocol}://${opts.hostname}:${opts.port}${opts.path}`;
		if (typeof opts.transformWsUrl === "function") url = opts.transformWsUrl(url, opts, client);
		return url;
	}
	function bindEventHandler() {
		if (isInitialized) return;
		isInitialized = true;
		my.onSocketOpen(() => {
			stream.socketReady();
		});
		my.onSocketMessage((res) => {
			if (typeof res.data === "string") {
				const buffer = buffer_1.Buffer.from(res.data, "base64");
				proxy.push(buffer);
			} else {
				const reader = new FileReader();
				reader.addEventListener("load", () => {
					if (reader.result instanceof ArrayBuffer) {
						proxy.push(buffer_1.Buffer.from(reader.result));
						return;
					}
					proxy.push(buffer_1.Buffer.from(reader.result, "utf-8"));
				});
				reader.readAsArrayBuffer(res.data);
			}
		});
		my.onSocketClose(() => {
			stream.end();
			stream.destroy();
		});
		my.onSocketError((err) => {
			stream.destroy(err);
		});
	}
	var buildStream = (client, opts) => {
		opts.hostname = opts.hostname || opts.host;
		if (!opts.hostname) throw new Error("Could not determine host. Specify host manually.");
		const websocketSubProtocol = opts.protocolId === "MQIsdp" && opts.protocolVersion === 3 ? "mqttv3.1" : "mqtt";
		setDefaultOpts(opts);
		const url = buildUrl(opts, client);
		my = opts.my;
		my.connectSocket({
			url,
			protocols: websocketSubProtocol
		});
		proxy = buildProxy();
		stream = new BufferedDuplex_1.BufferedDuplex(opts, proxy, my);
		bindEventHandler();
		return stream;
	};
	exports.default = buildStream;
}));
//#endregion
//#region node_modules/mqtt/build/lib/connect/index.js
var require_connect = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.connectAsync = connectAsync;
	var debug_1 = __importDefault(require_src());
	var url_1 = __importDefault(__require("url"));
	var client_1 = __importDefault(require_client());
	var is_browser_1 = __importDefault(require_is_browser());
	if (typeof process?.nextTick !== "function") process.nextTick = setImmediate;
	var debug = (0, debug_1.default)("mqttjs");
	var protocols = null;
	function getDefaultWebSocketProtocol() {
		if (typeof location !== "undefined") return location.protocol === "https:" ? "wss" : "ws";
		if (typeof document !== "undefined") return new URL(document.URL).protocol === "https:" ? "wss" : "ws";
		return "ws";
	}
	function parseAuthOptions(opts) {
		let matches;
		if (opts.auth) {
			matches = opts.auth.match(/^(.+):(.+)$/);
			if (matches) {
				const [, username, password] = matches;
				opts.username = username;
				opts.password = password;
			} else opts.username = opts.auth;
		}
	}
	function connect(brokerUrl, opts) {
		debug("connecting to an MQTT broker...");
		if (typeof brokerUrl === "object" && !opts) {
			opts = brokerUrl;
			brokerUrl = "";
		}
		opts = opts || {};
		if (brokerUrl && typeof brokerUrl === "string") {
			const parsedUrl = url_1.default.parse(brokerUrl, true);
			const parsedOptions = {};
			const isRelativeWebSocketUrl = !parsedUrl.protocol && !parsedUrl.hostname && parsedUrl.path?.startsWith("/") && (is_browser_1.default || opts.forceNativeWebSocket);
			if (parsedUrl.port != null) parsedOptions.port = Number(parsedUrl.port);
			parsedOptions.host = parsedUrl.hostname;
			parsedOptions.query = parsedUrl.query;
			parsedOptions.auth = parsedUrl.auth;
			parsedOptions.protocol = parsedUrl.protocol;
			parsedOptions.path = parsedUrl.path;
			opts = {
				...parsedOptions,
				...opts
			};
			if (!opts.protocol && !isRelativeWebSocketUrl) throw new Error("Missing protocol");
			opts.protocol = (opts.protocol || getDefaultWebSocketProtocol()).replace(/:$/, "");
		}
		opts.unixSocket = opts.unixSocket || opts.protocol?.includes("+unix");
		if (opts.unixSocket) opts.protocol = opts.protocol.replace("+unix", "");
		else if (!opts.protocol?.startsWith("ws") && !opts.protocol?.startsWith("wx")) delete opts.path;
		parseAuthOptions(opts);
		if (opts.query && typeof opts.query.clientId === "string") opts.clientId = opts.query.clientId;
		if (is_browser_1.default || opts.unixSocket) opts.socksProxy = void 0;
		else if (opts.socksProxy === void 0 && typeof process !== "undefined") opts.socksProxy = process.env["MQTTJS_SOCKS_PROXY"];
		if (opts.cert && opts.key) {
			if (opts.protocol) {
				if ([
					"mqtts",
					"wss",
					"wxs",
					"alis"
				].indexOf(opts.protocol) === -1) switch (opts.protocol) {
					case "mqtt":
						opts.protocol = "mqtts";
						break;
					case "ws":
						opts.protocol = "wss";
						break;
					case "wx":
						opts.protocol = "wxs";
						break;
					case "ali":
						opts.protocol = "alis";
						break;
					default: throw new Error(`Unknown protocol for secure connection: "${opts.protocol}"!`);
				}
			} else throw new Error("Missing secure protocol key");
		}
		if (!protocols) {
			protocols = {};
			if (!is_browser_1.default && !opts.forceNativeWebSocket) {
				protocols.ws = require_ws().streamBuilder;
				protocols.wss = require_ws().streamBuilder;
				protocols.mqtt = require_tcp().default;
				protocols.tcp = require_tcp().default;
				protocols.ssl = require_tls().default;
				protocols.tls = protocols.ssl;
				protocols.mqtts = require_tls().default;
			} else {
				protocols.ws = require_ws().browserStreamBuilder;
				protocols.wss = require_ws().browserStreamBuilder;
				protocols.wx = require_wx().default;
				protocols.wxs = require_wx().default;
				protocols.ali = require_ali().default;
				protocols.alis = require_ali().default;
			}
		}
		if (!protocols[opts.protocol]) {
			const isSecure = ["mqtts", "wss"].indexOf(opts.protocol) !== -1;
			opts.protocol = [
				"mqtt",
				"mqtts",
				"ws",
				"wss",
				"wx",
				"wxs",
				"ali",
				"alis"
			].filter((key, index) => {
				if (isSecure && index % 2 === 0) return false;
				return typeof protocols[key] === "function";
			})[0];
		}
		if (opts.clean === false && !opts.clientId) throw new Error("Missing clientId for unclean clients");
		if (opts.protocol) opts.defaultProtocol = opts.protocol;
		function wrapper(client) {
			if (opts.servers) {
				if (!client._reconnectCount || client._reconnectCount === opts.servers.length) client._reconnectCount = 0;
				opts.host = opts.servers[client._reconnectCount].host;
				opts.port = opts.servers[client._reconnectCount].port;
				opts.protocol = !opts.servers[client._reconnectCount].protocol ? opts.defaultProtocol : opts.servers[client._reconnectCount].protocol;
				opts.hostname = opts.host;
				client._reconnectCount++;
			}
			debug("calling streambuilder for", opts.protocol);
			return protocols[opts.protocol](client, opts);
		}
		const client = new client_1.default(wrapper, opts);
		client.on("error", () => {});
		return client;
	}
	function connectAsync(brokerUrl, opts, allowRetries = true) {
		return new Promise((resolve, reject) => {
			const client = connect(brokerUrl, opts);
			const promiseResolutionListeners = {
				connect: (connack) => {
					removePromiseResolutionListeners();
					resolve(client);
				},
				end: () => {
					removePromiseResolutionListeners();
					resolve(client);
				},
				error: (err) => {
					removePromiseResolutionListeners();
					client.end();
					reject(err);
				}
			};
			if (allowRetries === false) promiseResolutionListeners.close = () => {
				promiseResolutionListeners.error(/* @__PURE__ */ new Error("Couldn't connect to server"));
			};
			function removePromiseResolutionListeners() {
				Object.keys(promiseResolutionListeners).forEach((eventName) => {
					client.off(eventName, promiseResolutionListeners[eventName]);
				});
			}
			Object.keys(promiseResolutionListeners).forEach((eventName) => {
				client.on(eventName, promiseResolutionListeners[eventName]);
			});
		});
	}
	exports.default = connect;
}));
//#endregion
//#region node_modules/mqtt/build/mqtt.js
var require_mqtt = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __createBinding = exports && exports.__createBinding || (Object.create ? (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		var desc = Object.getOwnPropertyDescriptor(m, k);
		if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) desc = {
			enumerable: true,
			get: function() {
				return m[k];
			}
		};
		Object.defineProperty(o, k2, desc);
	}) : (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		o[k2] = m[k];
	}));
	var __setModuleDefault = exports && exports.__setModuleDefault || (Object.create ? (function(o, v) {
		Object.defineProperty(o, "default", {
			enumerable: true,
			value: v
		});
	}) : function(o, v) {
		o["default"] = v;
	});
	var __importStar = exports && exports.__importStar || (function() {
		var ownKeys = function(o) {
			ownKeys = Object.getOwnPropertyNames || function(o) {
				var ar = [];
				for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
				return ar;
			};
			return ownKeys(o);
		};
		return function(mod) {
			if (mod && mod.__esModule) return mod;
			var result = {};
			if (mod != null) {
				for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
			}
			__setModuleDefault(result, mod);
			return result;
		};
	})();
	var __exportStar = exports && exports.__exportStar || function(m, exports$3) {
		for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports$3, p)) __createBinding(exports$3, m, p);
	};
	var __importDefault = exports && exports.__importDefault || function(mod) {
		return mod && mod.__esModule ? mod : { "default": mod };
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.ReasonCodes = exports.KeepaliveManager = exports.UniqueMessageIdProvider = exports.DefaultMessageIdProvider = exports.Store = exports.MqttClient = exports.connectAsync = exports.connect = exports.Client = void 0;
	var client_1 = __importDefault(require_client());
	exports.MqttClient = client_1.default;
	exports.DefaultMessageIdProvider = __importDefault(require_default_message_id_provider()).default;
	exports.UniqueMessageIdProvider = __importDefault(require_unique_message_id_provider()).default;
	exports.Store = __importDefault(require_store()).default;
	var connect_1 = __importStar(require_connect());
	exports.connect = connect_1.default;
	Object.defineProperty(exports, "connectAsync", {
		enumerable: true,
		get: function() {
			return connect_1.connectAsync;
		}
	});
	exports.KeepaliveManager = __importDefault(require_KeepaliveManager()).default;
	exports.Client = client_1.default;
	__exportStar(require_client(), exports);
	__exportStar(require_shared(), exports);
	__exportStar(require_validations(), exports);
	var ack_1 = require_ack();
	Object.defineProperty(exports, "ReasonCodes", {
		enumerable: true,
		get: function() {
			return ack_1.ReasonCodes;
		}
	});
}));
//#endregion
//#region node_modules/mqtt/build/index.js
var require_build = /* @__PURE__ */ __commonJSMin(((exports) => {
	var __createBinding = exports && exports.__createBinding || (Object.create ? (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		var desc = Object.getOwnPropertyDescriptor(m, k);
		if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) desc = {
			enumerable: true,
			get: function() {
				return m[k];
			}
		};
		Object.defineProperty(o, k2, desc);
	}) : (function(o, m, k, k2) {
		if (k2 === void 0) k2 = k;
		o[k2] = m[k];
	}));
	var __setModuleDefault = exports && exports.__setModuleDefault || (Object.create ? (function(o, v) {
		Object.defineProperty(o, "default", {
			enumerable: true,
			value: v
		});
	}) : function(o, v) {
		o["default"] = v;
	});
	var __importStar = exports && exports.__importStar || (function() {
		var ownKeys = function(o) {
			ownKeys = Object.getOwnPropertyNames || function(o) {
				var ar = [];
				for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
				return ar;
			};
			return ownKeys(o);
		};
		return function(mod) {
			if (mod && mod.__esModule) return mod;
			var result = {};
			if (mod != null) {
				for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
			}
			__setModuleDefault(result, mod);
			return result;
		};
	})();
	var __exportStar = exports && exports.__exportStar || function(m, exports$2) {
		for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports$2, p)) __createBinding(exports$2, m, p);
	};
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.default = __importStar(require_mqtt());
	__exportStar(require_mqtt(), exports);
}));
//#endregion
export { require_build as t };
