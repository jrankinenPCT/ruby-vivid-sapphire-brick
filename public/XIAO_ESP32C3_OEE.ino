/*
============================================================================
XIAO ESP32-C3 OEE SIMULATOR  —  EET 124
============================================================================
STUDENTS — CHANGE ONE LINE
--------------------------
In the MQTT TOPICS section, set TOPIC_PATH to:

    EET124/OEE/<FirstName><LastInitial>/

No spaces. DrJeff is the example. Alex Rivera would use AlexR.
Every topic (json, availability, oee, counts, status, ...) is built
from that single string. Do not edit the topic names one by one.

INPUTS                         OUTPUTS
------                         -------
D4  blue button   DEFECT       D7  red LED     STOPPED
D5  green button  START        D8  green LED   RUNNING
D6  red button    STOP         D9  blue LED    scrap armed / just scrapped
A0  speed pot     80% – 120%

Buttons are active-LOW with the internal pull-ups (other side to GND).
Pot: one end 3V3, one end GND, wiper to A0.

THE LINE
--------
Boots STOPPED. The shift clock does not start until the first START.
Time spent powered up before that is not downtime.
After the shift starts, every STOP is unplanned downtime.

A clean run is 100 OEE:
  pot at 100%, no STOP, no scrap.
Performance is the pot itself (80% to 120% of nameplate).
Nameplate is 1 widget / second, 60 / minute, and the cycle time
matches that setting exactly.
Quality starts at 100. It falls only when a real widget is scrapped.
OEE falls only when you slow the pot, press STOP, or scrap a part.

The blue button is the inspector. It does NOT create a part.
  • If a good widget is on the count, that widget becomes scrap
    (good -, bad +, total unchanged).
  • If there is no good widget yet, the next cycle is born as scrap.
good + bad always equals total.

    Availability = Run Time / Planned Time     (100 before the shift)
    Performance  = speed pot
    Quality      = Good Count / Total Count    (100 until a scrap)
    OEE          = Availability × Performance × Quality

Wi-Fi or MQTT can drop. The station keeps cycling and the counts stay
on the board. Publishing resumes when the broker is back.
Offline is a retained last-will on this station's own status topic,
so one student cannot clear another student's "Online".
============================================================================
*/

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>

//============================================================================
// INPUTS / OUTPUTS
//============================================================================
#define GREEN_BTN D5
#define RED_BTN   D6
#define BLUE_BTN  D4
#define RED_LED   D7
#define GREEN_LED D8
#define BLUE_LED  D9
#define POT_PIN   A0

const unsigned long DEBOUNCE_MS     = 40;
const unsigned long PUBLISH_MS      = 1000;
const unsigned long SCRAP_FLASH_MS  = 400;
const int           MAX_SCRAP_QUEUE = 25;

//============================================================================
// WIFI NETWORKS
//============================================================================
const char* wifiNetworks[][2] =
{
  {"Wokwi-GUEST", ""},
  {"eet124", "buttercup"},
  {"PCT-IOT", "wildcats"}
};
const int NUM_NETWORKS = sizeof(wifiNetworks) / sizeof(wifiNetworks[0]);

//============================================================================
// MQTT SETTINGS
//============================================================================
#define MQTT_SERVER   "e6fc009f759842338bc9b4a61b9ca0a7.s1.eu.hivemq.cloud"
#define MQTT_PORT     8883
#define MQTT_USER     "mqtt-fall24"
#define MQTT_PASSWORD "w@terHouse5"

//============================================================================
// MQTT TOPICS
// CHANGE ONLY THIS LINE.
// First name + last initial, no spaces. Keep the slash at the end.
//     "EET124/OEE/AlexR/"
//============================================================================
const char* TOPIC_PATH = "EET124/OEE/DrJeff/";

char jsonTopic[96];
char availTopic[96];
char perfTopic[96];
char qualityTopic[96];
char oeeTopic[96];
char goodTopic[96];
char badTopic[96];
char totalTopic[96];
char speedTopic[96];
char runningTopic[96];
char scrapTopic[96];
char statusTopic[96];

//============================================================================
// MQTT OBJECTS
//============================================================================
WiFiClientSecure secureClient;
PubSubClient client(secureClient);
char mqttClientId[24] = "OEE-boot";

//============================================================================
// PRODUCTION
//============================================================================
const float IDEAL_RATE_PPS = 1.0f;   // nameplate at 100% speed
const float IDEAL_CYCLE_MS = 1000.0f;

bool machineRunning = false;         // a real station does not boot in run
bool shiftStarted = false;          // planned time starts on the first START
bool downtimeActive = false;
uint32_t shiftStartMillis = 0;
uint32_t accumulatedDowntime = 0;
uint32_t downtimeStartMillis = 0;
uint32_t lastWidgetTime = 0;
uint32_t lastPublishTime = 0;
float cycleJitter = 1.0f;
float commandSpeed = 100.0f;

uint32_t totalCount = 0;
uint32_t goodCount = 0;
uint32_t badCount = 0;
int scrapQueued = 0;                 // future cycles already condemned
uint32_t defectFlashUntil = 0;

int wifiIndex = 0;
uint32_t lastWifiAttempt = 0;
uint32_t lastMqttAttempt = 0;

//============================================================================
// BUTTONS
//============================================================================
struct DebouncedButton
{
  int pin;
  bool stable;
  bool lastReading;
  uint32_t lastChange;
};

DebouncedButton greenBtn = {GREEN_BTN, HIGH, HIGH, 0};
DebouncedButton redBtn   = {RED_BTN,   HIGH, HIGH, 0};
DebouncedButton blueBtn  = {BLUE_BTN,  HIGH, HIGH, 0};

//============================================================================
// PROTOTYPES
//============================================================================
void makeTopic(char* dest, size_t len, const char* leaf);
void buildTopics();
void connectWiFi();
void serviceWiFi();
void serviceMQTT();
void updateSpeed();
void checkButtons();
void updateLEDs();
void produceParts();
void publishData();
bool pressed(DebouncedButton& btn);
void armNextCycle();
unsigned long currentInterval();

//============================================================================
// SETUP
//============================================================================
void setup()
{
  Serial.begin(115200);
  delay(300);

  analogReadResolution(12);
  pinMode(GREEN_BTN, INPUT_PULLUP);
  pinMode(RED_BTN, INPUT_PULLUP);
  pinMode(BLUE_BTN, INPUT_PULLUP);
  pinMode(RED_LED, OUTPUT);
  pinMode(GREEN_LED, OUTPUT);
  pinMode(BLUE_LED, OUTPUT);

  machineRunning = false;
  shiftStarted = false;
  downtimeActive = false;
  digitalWrite(RED_LED, HIGH);
  digitalWrite(GREEN_LED, LOW);
  digitalWrite(BLUE_LED, LOW);

  randomSeed(analogRead(POT_PIN) + (int)millis());
  armNextCycle();

  buildTopics();

  Serial.println();
  Serial.println("=========================================");
  Serial.println("XIAO ESP32-C3 OEE SIMULATOR");
  Serial.print("TOPIC PATH  ");
  Serial.println(TOPIC_PATH);
  Serial.println("Line is STOPPED — press START");
  Serial.println("100% pot, no stop, no scrap = 100 OEE");
  Serial.println("=========================================");

  connectWiFi();

  secureClient.setInsecure();
  client.setServer(MQTT_SERVER, MQTT_PORT);
  // Stock PubSubClient packets are 256 bytes. The JSON report is larger.
  client.setBufferSize(1024);

  lastWidgetTime = millis();
  lastPublishTime = millis();
  lastWifiAttempt = millis();
}

//============================================================================
// LOOP
//============================================================================
void loop()
{
  serviceWiFi();
  serviceMQTT();
  updateSpeed();
  checkButtons();
  updateLEDs();
  produceParts();

  if (millis() - lastPublishTime >= PUBLISH_MS)
  {
    lastPublishTime = millis();
    publishData();
  }
}

//============================================================================
// TOPICS — one path, every leaf
//============================================================================
void makeTopic(char* dest, size_t len, const char* leaf)
{
  size_t n = strlen(TOPIC_PATH);
  bool hasSlash = (n > 0 && TOPIC_PATH[n - 1] == '/');
  if (hasSlash)
  {
    snprintf(dest, len, "%s%s", TOPIC_PATH, leaf);
  }
  else
  {
    snprintf(dest, len, "%s/%s", TOPIC_PATH, leaf);
  }
}

void buildTopics()
{
  makeTopic(jsonTopic,    sizeof(jsonTopic),    "json");
  makeTopic(availTopic,   sizeof(availTopic),   "availability");
  makeTopic(perfTopic,    sizeof(perfTopic),    "performance");
  makeTopic(qualityTopic, sizeof(qualityTopic), "quality");
  makeTopic(oeeTopic,     sizeof(oeeTopic),     "oee");
  makeTopic(goodTopic,    sizeof(goodTopic),    "goodCount");
  makeTopic(badTopic,     sizeof(badTopic),     "badCount");
  makeTopic(totalTopic,   sizeof(totalTopic),   "totalCount");
  makeTopic(speedTopic,   sizeof(speedTopic),   "speedPercent");
  makeTopic(runningTopic, sizeof(runningTopic), "running");
  makeTopic(scrapTopic,   sizeof(scrapTopic),   "scrapQueued");
  makeTopic(statusTopic,  sizeof(statusTopic),  "status");

  Serial.println(jsonTopic);
  Serial.println(availTopic);
  Serial.println(perfTopic);
  Serial.println(qualityTopic);
  Serial.println(oeeTopic);
  Serial.println(goodTopic);
  Serial.println(badTopic);
  Serial.println(totalTopic);
  Serial.println(speedTopic);
  Serial.println(runningTopic);
  Serial.println(scrapTopic);
  Serial.println(statusTopic);
}

//============================================================================
// WIFI
//============================================================================
void connectWiFi()
{
  for (int i = 0; i < NUM_NETWORKS; i++)
  {
    Serial.print("Trying ");
    Serial.println(wifiNetworks[i][0]);
    WiFi.begin(wifiNetworks[i][0], wifiNetworks[i][1]);

    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts < 20)
    {
      delay(500);
      Serial.print(".");
      attempts++;
    }

    if (WiFi.status() == WL_CONNECTED)
    {
      wifiIndex = (i + 1) % NUM_NETWORKS;
      Serial.println();
      Serial.println("WiFi Connected");
      Serial.print("IP: ");
      Serial.println(WiFi.localIP());

      uint8_t mac[6];
      WiFi.macAddress(mac);
      snprintf(
        mqttClientId,
        sizeof(mqttClientId),
        "OEE-%02X%02X%02X",
        mac[3], mac[4], mac[5]
      );
      Serial.print("Client ");
      Serial.println(mqttClientId);
      return;
    }

    Serial.println();
    Serial.println("No join");
  }

  Serial.println("WiFi not up yet — line will still run, retrying in loop");
}

void serviceWiFi()
{
  if (WiFi.status() == WL_CONNECTED)
  {
    return;
  }

  if (millis() - lastWifiAttempt < 15000)
  {
    return;
  }
  lastWifiAttempt = millis();

  Serial.print("Retrying WiFi ");
  Serial.println(wifiNetworks[wifiIndex][0]);
  WiFi.begin(wifiNetworks[wifiIndex][0], wifiNetworks[wifiIndex][1]);
  wifiIndex = (wifiIndex + 1) % NUM_NETWORKS;
}

//============================================================================
// MQTT  (non-blocking: a dead broker must not freeze the station)
//============================================================================
void serviceMQTT()
{
  if (WiFi.status() != WL_CONNECTED)
  {
    return;
  }

  if (client.connected())
  {
    client.loop();
    return;
  }

  if (millis() - lastMqttAttempt < 5000)
  {
    return;
  }
  lastMqttAttempt = millis();

  Serial.print("Connecting MQTT...");
  bool ok = client.connect(
    mqttClientId,
    MQTT_USER,
    MQTT_PASSWORD,
    statusTopic,
    0,
    true,
    "Offline"
  );

  if (ok)
  {
    Serial.println("Connected");
    client.publish(statusTopic, "Online", true);
  }
  else
  {
    Serial.print("Failed rc=");
    Serial.println(client.state());
  }
}

//============================================================================
// SPEED  —  12-bit pot, 80% to 120%, smoothed so ADC noise is not a VFD
//============================================================================
void updateSpeed()
{
  uint32_t sum = 0;
  for (int i = 0; i < 8; i++)
  {
    sum += analogRead(POT_PIN);
  }
  float raw = sum / 8.0f;
  if (raw < 0.0f) raw = 0.0f;
  if (raw > 4095.0f) raw = 4095.0f;

  float speed = 80.0f + (raw / 4095.0f) * 40.0f;
  commandSpeed = (commandSpeed * 0.75f) + (speed * 0.25f);
}

void armNextCycle()
{
  cycleJitter = 1.0f;
}

unsigned long currentInterval()
{
  float factor = commandSpeed / 100.0f;
  if (factor < 0.50f) factor = 0.50f;
  unsigned long interval = (unsigned long)((IDEAL_CYCLE_MS / factor) * cycleJitter);
  if (interval < 200) interval = 200;
  return interval;
}

//============================================================================
// BUTTONS
//============================================================================
bool pressed(DebouncedButton& btn)
{
  bool reading = digitalRead(btn.pin);
  if (reading != btn.lastReading)
  {
    btn.lastChange = millis();
    btn.lastReading = reading;
  }

  if ((millis() - btn.lastChange) >= DEBOUNCE_MS && reading != btn.stable)
  {
    btn.stable = reading;
    return btn.stable == LOW;
  }
  return false;
}

void checkButtons()
{
  if (pressed(greenBtn))
  {
    if (!machineRunning)
    {
      machineRunning = true;
      if (!shiftStarted)
      {
        shiftStarted = true;
        shiftStartMillis = millis();
        accumulatedDowntime = 0;
        downtimeActive = false;
      }
      else if (downtimeActive)
      {
        accumulatedDowntime += millis() - downtimeStartMillis;
        downtimeActive = false;
      }
      lastWidgetTime = millis();
      armNextCycle();
      Serial.println("START");
    }
  }

  if (pressed(redBtn))
  {
    if (machineRunning)
    {
      machineRunning = false;
      downtimeStartMillis = millis();
      downtimeActive = true;
      Serial.println("STOP");
    }
  }

  if (pressed(blueBtn))
  {
    if (goodCount > 0)
    {
      goodCount--;
      badCount++;
      defectFlashUntil = millis() + SCRAP_FLASH_MS;
      Serial.println("DEFECT — good part scrapped");
    }
    else if (scrapQueued < MAX_SCRAP_QUEUE)
    {
      scrapQueued++;
      Serial.println("DEFECT — next part will be scrap");
    }
    else
    {
      Serial.println("DEFECT ignored — scrap queue full");
    }
  }
}

//============================================================================
// LEDS
//============================================================================
void updateLEDs()
{
  digitalWrite(GREEN_LED, machineRunning ? HIGH : LOW);
  digitalWrite(RED_LED, machineRunning ? LOW : HIGH);

  bool scrapLit = (scrapQueued > 0) || (defectFlashUntil != 0 && millis() < defectFlashUntil);
  digitalWrite(BLUE_LED, scrapLit ? HIGH : LOW);
}

//============================================================================
// PRODUCTION
//============================================================================
void produceParts()
{
  if (!machineRunning)
  {
    return;
  }

  unsigned long interval = currentInterval();
  if (millis() - lastWidgetTime < interval)
  {
    return;
  }

  lastWidgetTime += interval;
  if (millis() - lastWidgetTime > interval)
  {
    lastWidgetTime = millis();
  }
  totalCount++;

  if (scrapQueued > 0)
  {
    scrapQueued--;
    badCount++;
    defectFlashUntil = millis() + SCRAP_FLASH_MS;
    Serial.println("SCRAP PART");
  }
  else
  {
    goodCount++;
    Serial.println("GOOD PART");
  }

  armNextCycle();
}

//============================================================================
// OEE + MQTT
//============================================================================
void publishNumber(const char* topic, float value, int decimals)
{
  char buffer[24];
  if (decimals <= 0)
  {
    snprintf(buffer, sizeof(buffer), "%.0f", value);
  }
  else if (decimals == 1)
  {
    snprintf(buffer, sizeof(buffer), "%.1f", value);
  }
  else
  {
    snprintf(buffer, sizeof(buffer), "%.2f", value);
  }
  client.publish(topic, buffer, true);
}

void publishData()
{
  float availability = 100.0f;
  float performance = commandSpeed;
  float quality = 100.0f;
  float runtimeSec = 0.0f;
  float downtimeSec = 0.0f;

  if (shiftStarted)
  {
    uint32_t now = millis();
    uint32_t planned = now - shiftStartMillis;
    uint32_t down = accumulatedDowntime;
    if (downtimeActive)
    {
      down += now - downtimeStartMillis;
    }
    if (down > planned)
    {
      down = planned;
    }
    uint32_t runtime = planned - down;
    if (planned > 0)
    {
      availability = (100.0f * (float)runtime) / (float)planned;
    }
    if (totalCount > 0)
    {
      quality = (100.0f * (float)goodCount) / (float)totalCount;
    }
    runtimeSec = runtime / 1000.0f;
    downtimeSec = down / 1000.0f;
  }

  float oee =
    (availability / 100.0f) *
    (performance / 100.0f) *
    (quality / 100.0f) *
    100.0f;

  char payload[512];
  snprintf(
    payload,
    sizeof(payload),
    "{\"availability\":%.2f,\"performance\":%.2f,\"quality\":%.2f,\"oee\":%.2f,"
    "\"speedPercent\":%.1f,\"goodCount\":%lu,\"badCount\":%lu,\"totalCount\":%lu,"
    "\"running\":%s,\"scrapQueued\":%d,\"runtimeSec\":%.1f,\"downtimeSec\":%.1f}",
    availability,
    performance,
    quality,
    oee,
    commandSpeed,
    (unsigned long)goodCount,
    (unsigned long)badCount,
    (unsigned long)totalCount,
    machineRunning ? "true" : "false",
    scrapQueued,
    runtimeSec,
    downtimeSec
  );

  Serial.println();
  Serial.println("============= OEE =============");
  Serial.printf("Availability : %6.2f %%\n", availability);
  Serial.printf("Performance  : %6.2f %%\n", performance);
  Serial.printf("Quality      : %6.2f %%\n", quality);
  Serial.printf("OEE          : %6.2f %%\n", oee);
  Serial.printf("Speed        : %6.1f %%\n", commandSpeed);
  Serial.printf("Cycle        : %lu ms\n", currentInterval());
  Serial.printf("Good         : %lu\n", (unsigned long)goodCount);
  Serial.printf("Bad          : %lu\n", (unsigned long)badCount);
  Serial.printf("Total        : %lu\n", (unsigned long)totalCount);
  Serial.printf("Scrap queued : %d\n", scrapQueued);
  Serial.printf("Runtime      : %.1f s\n", runtimeSec);
  Serial.printf("Downtime     : %.1f s\n", downtimeSec);
  Serial.println(payload);
  Serial.println("================================");

  if (!client.connected())
  {
    return;
  }

  client.publish(jsonTopic, payload, true);
  publishNumber(availTopic, availability, 2);
  publishNumber(perfTopic, performance, 2);
  publishNumber(qualityTopic, quality, 2);
  publishNumber(oeeTopic, oee, 2);
  publishNumber(speedTopic, commandSpeed, 1);

  char buffer[24];
  snprintf(buffer, sizeof(buffer), "%lu", (unsigned long)goodCount);
  client.publish(goodTopic, buffer, true);
  snprintf(buffer, sizeof(buffer), "%lu", (unsigned long)badCount);
  client.publish(badTopic, buffer, true);
  snprintf(buffer, sizeof(buffer), "%lu", (unsigned long)totalCount);
  client.publish(totalTopic, buffer, true);
  snprintf(buffer, sizeof(buffer), "%d", scrapQueued);
  client.publish(scrapTopic, buffer, true);
  client.publish(runningTopic, machineRunning ? "true" : "false", true);
}
