#include <Arduino.h>
#include <WiFi.h>
#include <WebServer.h>
#include <DNSServer.h>
#include <Preferences.h>
#include <ESPmDNS.h>
#include <HTTPClient.h>
#include <math.h>
#include <Wire.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Adafruit_BMP085.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_AHTX0.h>
#include <Adafruit_VEML7700.h>
#include <Adafruit_MLX90614.h>
#include "driver/i2s.h"
#include "model_data.h"
#include "tensorflow/lite/micro/all_ops_resolver.h"
#include "tensorflow/lite/micro/micro_interpreter.h"
#include "tensorflow/lite/schema/schema_generated.h"
#include "tensorflow/lite/version.h"

#define RELAY_PIN       15
#define ONE_WIRE_PIN    6
#define TDS_PIN         4
#define I2C_SDA         8
#define I2C_SCL         9
#define I2S_PORT        I2S_NUM_0
#define I2S_BCLK_PIN    18
#define I2S_LRCL_PIN    17
#define I2S_DIN_PIN     16
#define I2S_SAMPLE_RATE 16000
#define I2S_SAMPLES     2048

Preferences prefs;
WebServer  server(80);
DNSServer  dns;
bool apMode = false;
unsigned long lastConnCheck = 0, disconnectedSince = 0;
const unsigned long RECHECK_MS = 3000, DISCONNECT_GRACE_MS = 15000;
const char* MDNS_NAME = "ubiguard";
const char* AP_SSID   = "UBiGuardian-Setup";
const IPAddress AP_IP(192,168,4,1), AP_GW(192,168,4,1), AP_MASK(255,255,255,0);

String   collHost = "";
uint16_t collPort = 5001;
String   collPath = "/ingest";
bool     pushEnabled = true;

OneWire oneWire(ONE_WIRE_PIN);
DallasTemperature ds18b20(&oneWire);
Adafruit_BMP085   bmp;
Adafruit_AHTX0    aht;
Adafruit_VEML7700 veml;
Adafruit_MLX90614 mlx = Adafruit_MLX90614();

bool bmpOK=false, ahtOK=false, vemlOK=false, mlxOK=false, i2sOK=false;

volatile bool pump_active = false;
unsigned long pump_off_at = 0;
bool manual_override = false;
unsigned long pump_on_at = 0;

const uint32_t STRAT_DWELL_MS    = 60000UL;
const float    RMS_MULT          = 2.0f;
const float    DT60_COLD         = -0.5f;
const float    TDS_JUMP_ABS_MV   = 200.0f;
const float    TDS_JUMP_FRAC     = 0.25f;
const uint32_t TDS_DWELL_MS      = 10000UL;
const float    IR_DELTA_HOT      = 5.0f;
const float    IR_ABS_HOT        = 45.0f;
const float    AIR_FIRE_ABS      = 50.0f;
const float    AIR_HOT_T         = 40.0f;
const float    AIR_HOT_LOW_RH    = 15.0f;
const int      PBUF_MAX          = 190;
const float    P_DROP_HPA        = 6.0f;
const uint32_t GENTLE_SEC        = 10;
const uint32_t GENTLE_COOLDOWN   = 5UL*60UL*1000UL;
const uint32_t PUMP_SELF_MASK_MS = 8000UL;
const uint32_t MIN_ON_MS         = 5000UL;
const int      SENSOR_FAULT_SEC  = 5;
const uint32_t HOUR_WINDOW_MS    = 60UL*60UL*1000UL;
const uint32_t TAP_MIN_MS        = 700UL;
const uint32_t TAP_MAX_MS        = 2000UL;
const uint32_t DISTURB_DWELL_MS  = 5000UL;
const uint32_t LUX_WIN_SEC       = 5;
const uint32_t SUDDEN_HOLD_MS    = 3000UL

;

struct Config {
  float do_lo=5.0, do_hi=7.0;
  float night_lux=30.0, glare_lux=2000.0;
  float dt_strat=1.0, dt_inv=-0.8;
  float overheat_un=30.0;
  uint32_t cap_hour_ms  = 30UL*60UL*1000UL;
  uint32_t cap_night_ms = 3UL*60UL*60UL*1000UL;
  float    cool_on_c=30.0, cool_off_c=29.5;
  uint32_t cool_burst_ms=20000UL;
  float day_on_factor=5.0f;
  float sudden_light_factor=10.0f;
  float sudden_dark_factor =0.2f;
  uint32_t sudden_window_ms=3000UL;
  uint32_t sunrise_grace_ms=10UL*60UL*1000UL;
  bool ml_gate=false;
} cfg;

void cfgSave(){
  prefs.begin("cfg", false);
  prefs.putFloat("do_lo", cfg.do_lo);      prefs.putFloat("do_hi", cfg.do_hi);
  prefs.putFloat("night", cfg.night_lux);  prefs.putFloat("glare", cfg.glare_lux);
  prefs.putFloat("dts", cfg.dt_strat);     prefs.putFloat("dti", cfg.dt_inv);
  prefs.putFloat("oh", cfg.overheat_un);
  prefs.putULong("cap_h", cfg.cap_hour_ms); prefs.putULong("cap_n", cfg.cap_night_ms);
  prefs.putFloat("cool_on", cfg.cool_on_c); prefs.putFloat("cool_off", cfg.cool_off_c);
  prefs.putULong("cool_ms", cfg.cool_burst_ms);
  prefs.putFloat("day_fact", cfg.day_on_factor);
  prefs.putFloat("sud_l", cfg.sudden_light_factor);
  prefs.putFloat("sud_d", cfg.sudden_dark_factor);
  prefs.putULong("sud_w", cfg.sudden_window_ms);
  prefs.putULong("sunrise", cfg.sunrise_grace_ms);
  prefs.putBool("ml", cfg.ml_gate);
  prefs.end();
}
void cfgLoad(){
  prefs.begin("cfg", true);
  cfg.do_lo     = prefs.getFloat("do_lo", cfg.do_lo);
  cfg.do_hi     = prefs.getFloat("do_hi", cfg.do_hi);
  cfg.night_lux = prefs.getFloat("night", cfg.night_lux);
  cfg.glare_lux = prefs.getFloat("glare", cfg.glare_lux);
  cfg.dt_strat  = prefs.getFloat("dts", cfg.dt_strat);
  cfg.dt_inv    = prefs.getFloat("dti", cfg.dt_inv);
  cfg.overheat_un = prefs.getFloat("oh", cfg.overheat_un);
  cfg.cap_hour_ms  = prefs.getULong("cap_h", cfg.cap_hour_ms);
  cfg.cap_night_ms = prefs.getULong("cap_n", cfg.cap_night_ms);
  cfg.cool_on_c    = prefs.getFloat("cool_on", cfg.cool_on_c);
  cfg.cool_off_c   = prefs.getFloat("cool_off", cfg.cool_off_c);
  cfg.cool_burst_ms= prefs.getULong("cool_ms", cfg.cool_burst_ms);
  cfg.day_on_factor      = prefs.getFloat("day_fact", cfg.day_on_factor);
  cfg.sudden_light_factor= prefs.getFloat("sud_l", cfg.sudden_light_factor);
  cfg.sudden_dark_factor = prefs.getFloat("sud_d", cfg.sudden_dark_factor);
  cfg.sudden_window_ms   = prefs.getULong("sud_w", cfg.sudden_window_ms);
  cfg.sunrise_grace_ms   = prefs.getULong("sunrise", cfg.sunrise_grace_ms);
  cfg.ml_gate   = prefs.getBool("ml", cfg.ml_gate);
  prefs.end();
}
String cfgJSON(){
  String j="{";
  j+="\"do_lo\":"+String(cfg.do_lo)+",\"do_hi\":"+String(cfg.do_hi)
   +",\"night_lux\":"+String(cfg.night_lux)+",\"glare_lux\":"+String(cfg.glare_lux)
   +",\"dt_strat\":"+String(cfg.dt_strat)+",\"dt_inv\":"+String(cfg.dt_inv)
   +",\"overheat_un\":"+String(cfg.overheat_un)
   +",\"cap_hour_ms\":"+String(cfg.cap_hour_ms)+",\"cap_night_ms\":"+String(cfg.cap_night_ms)
   +",\"cool_on_c\":"+String(cfg.cool_on_c)+",\"cool_off_c\":"+String(cfg.cool_off_c)
   +",\"cool_burst_ms\":"+String(cfg.cool_burst_ms)
   +",\"day_on_factor\":"+String(cfg.day_on_factor)
   +",\"sudden_light_factor\":"+String(cfg.sudden_light_factor)
   +",\"sudden_dark_factor\":"+String(cfg.sudden_dark_factor)
   +",\"sudden_window_ms\":"+String(cfg.sudden_window_ms)
   +",\"sunrise_grace_ms\":"+String(cfg.sunrise_grace_ms)
   +",\"ml_gate\":"+(cfg.ml_gate?String("true"):String("false"))+"}";
  return j;
}

bool   bl_ready=false;
float  rms_base=0, lux_base=0, lux_night_base=0, tds_base=0, p_ref=1013.25f;
unsigned long bl_start_ms=0;
unsigned long hour_window_start_ms = 0;
uint32_t duty_ms_hour = 0, duty_ms_night = 0;
bool      was_dark = false;
unsigned long last_gentle_ms = 0;
const int SLOPE_WIN=60; float mid_hist[SLOPE_WIN]; int mid_idx=0, mid_cnt=0;
unsigned long strat_since=0, inv_since=0;
unsigned long disturb_since=0;
unsigned long tds_since=0;
int fault_cnt=0;
bool is_day=false;
unsigned long day_switch_ms=0;
unsigned long abrupt_dark_since=0;

struct LuxSample { unsigned long ms; float lux; };
LuxSample luxbuf[LUX_WIN_SEC+1]; int lhead=0; int lsize=0;

struct PEntry { unsigned long ms; float p; };
PEntry pbuf[PBUF_MAX]; int phead=0; int psize=0; unsigned long last_pbuf_min_mark=0;

bool   cur_alert = false;
String cur_reason = "none";
String cur_ctx = "night";

typedef uint8_t DeviceAddress[8];
DeviceAddress addrTop, addrMid, addrBot;
bool haveMap=false;

constexpr int kTensorArenaSize = 25 * 1024;
static uint8_t tensor_arena[kTensorArenaSize];
const tflite::Model* tfl_model = nullptr;
tflite::AllOpsResolver tfl_ops;
std::unique_ptr<tflite::MicroInterpreter> tfl;
TfLiteTensor* tfl_in = nullptr;
TfLiteTensor* tfl_out = nullptr;
const char* kClasses[] = {
  "calm","cold-shock","cooling-hot","disturbance","flashlight-night",
  "glare","human-tap","manual-override","other","pump-self",
  "tds-spike","uniform-overheat"
};
constexpr int kNumClasses = sizeof(kClasses)/sizeof(kClasses[0]);

String addrToHex(const DeviceAddress a){
  char hex[17]; for(int j=0;j<8;j++) sprintf(&hex[j*2], "%02X", a[j]); hex[16]=0; return String(hex);
}
bool hexToAddr(const String& s, DeviceAddress a){
  if(s.length()!=16) return false;
  for(int i=0;i<8;i++){ char b[3]={ (char)s[2*i], (char)s[2*i+1], 0 }; a[i]=strtoul(b,nullptr,16); }
  return true;
}
bool getAddrByIndex(int idx, DeviceAddress a){ return ds18b20.getAddress(a, idx); }
void saveAddr(const char* key, const DeviceAddress a){ prefs.putString(key, addrToHex(a)); }
bool loadDSMap(){
  prefs.begin("dsmap", true);
  String t=prefs.getString("top",""), m=prefs.getString("mid",""), b=prefs.getString("bot","");
  prefs.end();
  if(t.length()==16 && m.length()==16 && b.length()==16){
    haveMap = hexToAddr(t,addrTop) && hexToAddr(m,addrMid) && hexToAddr(b,addrBot);
  } else haveMap=false;
  return haveMap;
}
void persistDSMap(){
  prefs.begin("dsmap", false);
  saveAddr("top", addrTop); saveAddr("mid", addrMid); saveAddr("bot", addrBot);
  prefs.end();
}
void initOrPersistDSMap(){
  if(loadDSMap()) return;
  DeviceAddress at, am, ab;
  if(getAddrByIndex(0,at) && getAddrByIndex(1,am) && getAddrByIndex(2,ab)){
    memcpy(addrTop,at,8); memcpy(addrMid,am,8); memcpy(addrBot,ab,8);
    persistDSMap(); haveMap=true;
  } else haveMap=false;
}
float readTempRole(const DeviceAddress a, int fallbackIndex){
  if(haveMap) return ds18b20.getTempC(a);
  else return ds18b20.getTempCByIndex(fallbackIndex);
}
static inline bool i2cPresent(uint8_t addr){
  Wire.beginTransmission(addr);
  return Wire.endTransmission()==0;
}
bool i2cRecover() {
  Wire.end(); delay(5);
  Wire.begin(I2C_SDA, I2C_SCL);
  Wire.setClock(50000);
  Wire.setTimeOut(200);
  return true;
}
template<typename F>
bool i2cTry(uint8_t addr, F fn){
  if(!i2cPresent(addr)) return false;
  if(fn()) return true;
  i2cRecover(); delay(5);
  return (i2cPresent(addr) && fn());
}
bool i2sInit(){
  i2s_config_t cfgI = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX),
    .sample_rate = I2S_SAMPLE_RATE,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_32BIT,
    .channel_format = I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format = (i2s_comm_format_t)(I2S_COMM_FORMAT_I2S | I2S_COMM_FORMAT_I2S_MSB),
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 6,
    .dma_buf_len = 256,
    .use_apll = false,
    .tx_desc_auto_clear = false,
    .fixed_mclk = 0
  };
  i2s_pin_config_t pins = {
    .bck_io_num = I2S_BCLK_PIN,
    .ws_io_num  = I2S_LRCL_PIN,
    .data_out_num = I2S_PIN_NO_CHANGE,
    .data_in_num  = I2S_DIN_PIN
  };
  if (i2s_driver_install(I2S_PORT, &cfgI, 0, NULL) != ESP_OK) return false;
  if (i2s_set_pin(I2S_PORT, &pins) != ESP_OK) return false;
  return true;
}
float micRMS(){
  static int32_t buf[I2S_SAMPLES];
  size_t br=0;
  if(i2s_read(I2S_PORT,(void*)buf,sizeof(buf),&br,pdMS_TO_TICKS(50))!=ESP_OK) return NAN;
  int n=br/sizeof(int32_t); if(n<=0) return NAN;
  double acc=0; for(int i=0;i<n;i++){ int32_t s=buf[i]>>8; acc += (double)s*s; }
  return (float)(sqrt(acc/n)/2048.0);
}
int median5i(int a,int b,int c,int d,int e){
  int v[5]={a,b,c,d,e};
  for(int i=0;i<4;i++) for(int j=i+1;j<5;j++) if(v[j]<v[i]){int t=v[i];v[i]=v[j];v[j]=t;}
  return v[2];
}
float tds_mV(bool &sat){
  analogSetPinAttenuation(TDS_PIN, ADC_11db);
  int m0=analogReadMilliVolts(TDS_PIN); delay(2);
  int m1=analogReadMilliVolts(TDS_PIN); delay(2);
  int m2=analogReadMilliVolts(TDS_PIN); delay(2);
  int m3=analogReadMilliVolts(TDS_PIN); delay(2);
  int m4=analogReadMilliVolts(TDS_PIN);
  int mv=median5i(m0,m1,m2,m3,m4);
  sat = (mv>3000);
  return (float)mv;
}
static inline float o2sol_mLL_fresh(float T_C){
  const double A0=2.00856, A1=3.22400, A2=3.99063, A3=4.80299, A4=0.978188, A5=1.71069;
  const double B0=-6.24097e-3, B1=-6.93498e-3, B2=-6.90358e-3, B3=-4.29155e-3, C0=-3.11680e-7;
  double Ts = log((298.15 - (double)T_C) / (273.15 + (double)T_C));
  double A = ((((A5*Ts + A4)*Ts + A3)*Ts + A2)*Ts + A1)*Ts + A0;
  double B = ((B3*Ts + B2)*Ts + B1)*Ts + B0;
  double mLL = exp(A);
  return (float)mLL;
}
static inline float sat_vapor_kPa(float T_C){ return 0.61078f * expf(17.27f * T_C / (T_C + 237.3f)); }
static inline float o2sat_mgL_local(float T_C, float P_hPa){
  if (isnan(T_C) || isnan(P_hPa) || T_C < -1.0f || T_C > 45.0f) return NAN;
  float mgL_1atm = o2sol_mLL_fresh(T_C) * 1.429f;
  float pw_kPa = sat_vapor_kPa(T_C);
  float pdry_kPa = max(0.0f, (P_hPa*0.1f) - pw_kPa);
  float pdry_ref = 101.325f - pw_kPa;
  float factor = (pdry_ref>0.0f) ? (pdry_kPa / pdry_ref) : 1.0f;
  return mgL_1atm * factor;
}
void pumpOn(){  digitalWrite(RELAY_PIN, HIGH); }
void pumpOff(){ digitalWrite(RELAY_PIN, LOW);  }
bool pumpIsOn(){ return digitalRead(RELAY_PIN)==HIGH; }
String jnum(float v, int digits){ if (isnan(v)) return "null"; String s; s+=String(v, digits); return s; }
String jbool(bool b){ return b ? "true" : "false"; }
static inline uint16_t clamp_port(long v){ if(v<1) return 1; if(v>65535) return 65535; return (uint16_t)v; }
static inline int      clamp_nonneg(long v){ return v<0 ? 0 : (int)v; }
String htmlHeader() {
  return F("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>UBi-Guardian</title><style>body{font-family:system-ui;margin:20px}button,input,select{font-size:1rem;padding:8px} .card{border:1px solid #ddd;border-radius:12px;padding:16px;margin-bottom:12px}</style></head><body><h2>UBi-Guardian</h2>");
}
String htmlFooter() { return F("</body></html>"); }
String statusPage(float tTop,float tMid,float tBot,float P_hPa,float lux,float irObj,float irAmb,float tds,bool tdsSAT,float rms,float doCstar,float airT,float airRH){
  String s = htmlHeader();
  s += "<div class='card'><h3>Status</h3>";
  s += "Hostname: <b>"+String(MDNS_NAME)+".local</b><br>";
  s += "IP: <b>"+WiFi.localIP().toString()+"</b><br>";
  s += "Pump: <b>"+String(pumpIsOn()?"ON":"OFF")+"</b><br>";
  s += "Manual override: <b>"+String(manual_override?"ON":"OFF")+"</b><br>";
  s += "Alert: <b>"+String(cur_alert?"TRUE":"false")+"</b> ("+cur_reason+")<br>";
  s += "Context: <b>"+cur_ctx+"</b><br>";
  s += "</div>";
  s += "<div class='card'><h3>Collector</h3>";
  s += "Target: http://" + (collHost.length()?collHost:String(WiFi.gatewayIP().toString())) + ":" + String(collPort) + collPath + "<br>";
  s += "Push: " + String(pushEnabled?"ON":"OFF");
  s += "</div>";
  s += "<div class='card'><h3>Sensors</h3><pre>";
  s += "Top/Mid/Bot [C]: "+String(isnan(tTop)?0:tTop,2)+", "+String(isnan(tMid)?0:tMid,2)+", "+String(isnan(tBot)?0:tBot,2)+"\n";
  s += "P [hPa]: "+String(isnan(P_hPa)?0:P_hPa,1)+"  Lux: "+String(isnan(lux)?0:lux,1)+"  IR Obj/Amb [C]: "+String(isnan(irObj)?0:irObj,2)+"/"+String(isnan(irAmb)?0:irAmb,2)+"\n";
  s += "Air T/RH: "+String(isnan(airT)?0:airT,1)+" °C / "+String(isnan(airRH)?0:airRH,0)+" %\n";
  s += "TDS [mV]: "+String(tds,0)+(tdsSAT?" SAT":"")+"  MicRMS: "+String(isnan(rms)?0:rms,2)+"  C* DO [mg/L]: "+String(isnan(doCstar)?0:doCstar,2);
  s += "</pre></div>";
  s += htmlFooter();
  return s;
}
String makeJSON(float tTop,float tMid,float tBot,float dT,float P_hPa,float lux,float irObj,float irAmb,float tds,bool tdsSAT,float rms,float doCstar, float airT, float airRH, bool alert, const String& reason, int rec_ms, const String& ctx){
  String j="{";
  j += "\"ms\":"+String(millis())+",";
  j += "\"pump\":"+jbool(pumpIsOn())+",";
  j += "\"manual_override\":"+jbool(manual_override)+",";
  j += "\"alert\":"+jbool(alert)+",";
  j += "\"reason\":\""+reason+"\",";
  j += "\"context\":\""+ctx+"\",";
  j += "\"rec_ms\":"+String(rec_ms)+",";
  j += "\"tTop\":"+jnum(tTop,2)+",";
  j += "\"tMid\":"+jnum(tMid,2)+",";
  j += "\"tBot\":"+jnum(tBot,2)+",";
  j += "\"dT_tb\":"+jnum(dT,2)+",";
  j += "\"pressure_hPa\":"+jnum(P_hPa,1)+",";
  j += "\"lux\":"+jnum(lux,1)+",";
  j += "\"irObj\":"+jnum(irObj,2)+",";
  j += "\"irAmb\":"+jnum(irAmb,2)+",";
  j += "\"airT\":"+jnum(airT,1)+",";
  j += "\"airRH\":"+jnum(airRH,0)+",";
  j += "\"tds_mV\":"+String(tds,0)+",";
  j += "\"tds_sat\":"+jbool(tdsSAT)+",";
  j += "\"micRMS\":"+jnum(rms,2)+",";
  j += "\"DOproxy\":"+jnum(doCstar,2);
  j += "}";
  return j;
}
String portalPage() {
  int n = WiFi.scanNetworks();
  String opts;
  for (int i=0;i<n;i++){
    String ssid = WiFi.SSID(i);
    ssid.replace("\"","");
    opts += "<option value=\""+ssid+"\">"+ssid+" ("+String(WiFi.RSSI(i))+" dBm)</option>";
  }
  String s = htmlHeader();
  s += "<div class='card'><h3>Wi-Fi Setup</h3>";
  s += "<form method='POST' action='/save'>";
  s += "<label>Network: </label><select name='ssid'>"+opts+"</select><br><br>";
  s += "<label>Password: </label><input type='password' name='pass' required><br><br>";
  s += "<button type='submit'>Connect</button>";
  s += "</form></div>";
  s += "<div class='card'>Connect to this AP: <b>UBiGuardian-Setup</b></div>";
  s += htmlFooter();
  return s;
}
void startAP(){
  apMode = true;
  WiFi.mode(WIFI_AP);
  WiFi.softAPConfig(AP_IP, AP_GW, AP_MASK);
  WiFi.softAP(AP_SSID);
  delay(100);
  dns.start(53, "*", AP_IP);
  server.onNotFound([](){ server.send(200,"text/html", portalPage()); });
  server.on("/", [](){ server.send(200,"text/html", portalPage()); });
  server.on("/save", HTTP_POST, [](){
    String ssid = server.arg("ssid");
    String pass = server.arg("pass");
    prefs.begin("wifi", false);
    prefs.putString("ssid", ssid);
    prefs.putString("pass", pass);
    prefs.end();
    server.send(200,"text/html", htmlHeader()+"<div class='card'>Saved. Connecting to <b>"+ssid+"</b> …</div>"+htmlFooter());
    delay(500);
    WiFi.softAPdisconnect(true);
    dns.stop();
    server.stop();
    apMode = false;
    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid.c_str(), pass.c_str());
  });
  server.begin();
  Serial.println(F("[WiFi] AP mode started: UBiGuardian-Setup"));
}
void stopAP(){
  if (apMode){
    dns.stop();
    WiFi.softAPdisconnect(true);
    apMode = false;
    Serial.println(F("[WiFi] AP mode stopped"));
  }
}
bool connectSTA_fromPrefs(unsigned long timeoutMs=20000){
  prefs.begin("wifi", true);
  String ssid = prefs.getString("ssid", "");
  String pass = prefs.getString("pass", "");
  prefs.end();
  if (ssid.length()==0) return false;
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid.c_str(), pass.c_str());
  Serial.printf("[WiFi] Connecting to '%s' ...\n", ssid.c_str());
  unsigned long t0=millis();
  while (WiFi.status()!=WL_CONNECTED && (millis()-t0)<timeoutMs){
    delay(250);
    Serial.print(".");
  }
  Serial.println();
  if (WiFi.status()==WL_CONNECTED){
    Serial.print("[WiFi] Connected. IP: "); Serial.println(WiFi.localIP());
    if (MDNS.begin(MDNS_NAME)) {
      Serial.printf("[mDNS] http://%s.local\n", MDNS_NAME);
    } else {
      Serial.println("[mDNS] start failed");
    }
    return true;
  }
  Serial.println("[WiFi] Connect failed.");
  return false;
}
void ensurePortalIfDisconnected(){
  wl_status_t st = WiFi.status();
  if (st==WL_CONNECTED){
    disconnectedSince = 0;
    if (apMode) stopAP();
  } else {
    if (disconnectedSince==0) disconnectedSince = millis();
    if (!apMode && (millis()-disconnectedSince)>=DISCONNECT_GRACE_MS){
      startAP();
    }
  }
}
IPAddress defaultCollectorIP(){ return WiFi.gatewayIP(); }
bool postTelemetry(const String& body){
  if (WiFi.status()!=WL_CONNECTED) return false;
  IPAddress ip;
  if (collHost.length()) ip.fromString(collHost);
  else ip = defaultCollectorIP();
  String url = "http://" + ip.toString() + ":" + String(collPort) + collPath;
  HTTPClient http;
  http.begin(url);
  http.addHeader("Content-Type","application/json");
  int code = http.POST((uint8_t*)body.c_str(), body.length());
  http.end();
  return (code==200);
}
String dsMapJSON(){
  String s="{";
  s += "\"haveMap\":" + String(haveMap?"true":"false") + ",";
  s += "\"top\":\"" + (haveMap?addrToHex(addrTop):String("n/a")) + "\",";
  s += "\"mid\":\"" + (haveMap?addrToHex(addrMid):String("n/a")) + "\",";
  s += "\"bot\":\"" + (haveMap?addrToHex(addrBot):String("n/a")) + "\"";
  s += "}";
  return s;
}
void startHTTP_STA(){
  server.on("/health", [](){ server.send(200,"text/plain","OK"); });
  server.on("/sensors", [](){
    ds18b20.requestTemperatures();
    float tTop = readTempRole(addrTop,0);
    float tMid = readTempRole(addrMid,1);
    float tBot = readTempRole(addrBot,2);
    if (tTop==85.0||tTop<=-100) tTop=NAN;
    if (tMid==85.0||tMid<=-100) tMid=NAN;
    if (tBot==85.0||tBot<=-100) tBot=NAN;
    float dT_tb = (isnan(tTop)||isnan(tBot)) ? NAN : (tTop - tBot);
    float P_hPa = (bmpOK && i2cPresent(0x77)) ? (bmp.readPressure()/100.0f) : NAN;
    float lux = (vemlOK && i2cPresent(0x10)) ? veml.readLux() : NAN;
    float irObj=NAN, irAmb=NAN;
    i2cTry(0x5A, [&](){ irObj=mlx.readObjectTempC(); irAmb=mlx.readAmbientTempC(); return !(isnan(irObj)||isnan(irAmb)); });
    float airT=NAN, airRH=NAN;
    i2cTry(0x38, [&](){ sensors_event_t hum, temp; if(!aht.getEvent(&hum,&temp)) return false; airT=temp.temperature; airRH=hum.relative_humidity; return true; });
    bool tdsSAT=false; 
    float tds=tds_mV(tdsSAT);
    float rms = i2sOK ? micRMS() : NAN;
    float P_eff = isnan(P_hPa) ? 1013.25f : P_hPa;
    float doCstar = (!isnan(tMid)) ? o2sat_mgL_local(tMid, P_eff) : NAN;
    server.send(200,"application/json",
      makeJSON(tTop,tMid,tBot,dT_tb,P_hPa,lux,irObj,irAmb,tds,tdsSAT,rms,doCstar, airT, airRH,
               cur_alert, cur_reason, pump_active?(int)(pump_off_at>millis()? (pump_off_at-millis()):0):0, cur_ctx));
  });
  server.on("/collector", [](){
    if(server.hasArg("host")) collHost = server.arg("host");
    if(server.hasArg("port")) collPort = clamp_port(server.arg("port").toInt());
    if(server.hasArg("path")) collPath = server.arg("path");
    prefs.begin("collector", false);
    prefs.putString("host", collHost);
    prefs.putUShort("port", collPort);
    prefs.putString("path", collPath);
    prefs.end();
    server.send(200,"text/plain","OK");
  });
  server.on("/caps", [](){
    String j = "{";
    j += "\"duty_ms_hour\":"+String(duty_ms_hour)+",";
    j += "\"hour_window_ms\":"+String(HOUR_WINDOW_MS)+",";
    j += "\"cap_hour_ms\":"+String(cfg.cap_hour_ms)+",";
    j += "\"duty_ms_night\":"+String(duty_ms_night)+",";
    j += "\"cap_night_ms\":"+String(cfg.cap_night_ms)+",";
    j += "\"isNight\":"+String((was_dark?"true":"false"));
    j += "}";
    server.send(200,"application/json", j);
  });
  server.on("/pump", [](){
    bool on = server.hasArg("on") ? (server.arg("on")=="1") : false;
    int sec = server.hasArg("sec") ? clamp_nonneg(server.arg("sec").toInt()) : 0;
    if(on){
      pumpOn(); pump_active=true; manual_override=true;
      pump_on_at = millis();
      pump_off_at = (sec>0) ? (millis() + (unsigned long)sec*1000UL) : 0;
      server.send(200,"text/plain","ON");
    } else {
      pumpOff(); pump_active=false; pump_off_at=0; manual_override=false;
      server.send(200,"text/plain","OFF");
    }
  });
  server.on("/dsmap", [](){ server.send(200,"application/json", dsMapJSON()); });
  server.on("/dsmap/reset", [](){ prefs.begin("dsmap", false); prefs.clear(); prefs.end(); haveMap=false; server.send(200,"text/plain","OK"); });
  server.on("/dsmap/byindex", [](){
    if(!server.hasArg("top") || !server.hasArg("mid") || !server.hasArg("bot")){
      server.send(400,"text/plain","need top, mid, bot"); return;
    }
    int it = server.arg("top").toInt();
    int im = server.arg("mid").toInt();
    int ib = server.arg("bot").toInt();
    DeviceAddress at, am, ab;
    if(!getAddrByIndex(it,at) || !getAddrByIndex(im,am) || !getAddrByIndex(ib,ab)){
      server.send(400,"text/plain","bad index(es)"); return;
    }
    memcpy(addrTop,at,8); memcpy(addrMid,am,8); memcpy(addrBot,ab,8);
    persistDSMap(); haveMap=true;
    server.send(200,"application/json", dsMapJSON());
  });
  server.on("/dsmap/setaddr", [](){
    if(!server.hasArg("top") || !server.hasArg("mid") || !server.hasArg("bot")){
      server.send(400,"text/plain","need top, mid, bot"); return;
    }
    DeviceAddress at, am, ab;
    if(!hexToAddr(server.arg("top"),at) || !hexToAddr(server.arg("mid"),am) || !hexToAddr(server.arg("bot"),ab)){
      server.send(400,"text/plain","bad hex address"); return;
    }
    memcpy(addrTop,at,8); memcpy(addrMid,am,8); memcpy(addrBot,ab,8);
    persistDSMap(); haveMap=true;
    server.send(200,"application/json", dsMapJSON());
  });
  server.on("/config/get", [](){ server.send(200,"application/json", cfgJSON()); });
  server.on("/config/reset", [](){ prefs.begin("cfg", false); prefs.clear(); prefs.end(); cfg = Config(); cfgSave(); server.send(200,"text/plain","OK"); });
  server.on("/config/set", [](){
    if (server.hasArg("key") && server.hasArg("val")){
      String k=server.arg("key"), v=server.arg("val");
      if(k=="do_lo") cfg.do_lo=v.toFloat();
      else if(k=="do_hi") cfg.do_hi=v.toFloat();
      else if(k=="night_lux") cfg.night_lux=v.toFloat();
      else if(k=="glare_lux") cfg.glare_lux=v.toFloat();
      else if(k=="dt_strat") cfg.dt_strat=v.toFloat();
      else if(k=="dt_inv") cfg.dt_inv=v.toFloat();
      else if(k=="overheat_un") cfg.overheat_un=v.toFloat();
      else if(k=="cap_hour_ms") cfg.cap_hour_ms=v.toInt();
      else if(k=="cap_night_ms") cfg.cap_night_ms=v.toInt();
      else if(k=="cool_on_c") cfg.cool_on_c=v.toFloat();
      else if(k=="cool_off_c") cfg.cool_off_c=v.toFloat();
      else if(k=="cool_burst_ms") cfg.cool_burst_ms=v.toInt();
      else if(k=="ml_gate") cfg.ml_gate = (v=="1"||v=="true");
      else if(k=="day_on_factor") cfg.day_on_factor=v.toFloat();
      else if(k=="sudden_light_factor") cfg.sudden_light_factor=v.toFloat();
      else if(k=="sudden_dark_factor") cfg.sudden_dark_factor=v.toFloat();
      else if(k=="sudden_window_ms") cfg.sudden_window_ms=v.toInt();
      else if(k=="sunrise_grace_ms") cfg.sunrise_grace_ms=v.toInt();
      cfgSave();
      server.send(200,"application/json", cfgJSON());
    } else server.send(400,"text/plain","need key & val");
  });
  server.on("/", [](){
    ds18b20.requestTemperatures();
    float tTop = readTempRole(addrTop,0);
    float tMid = readTempRole(addrMid,1);
    float tBot = readTempRole(addrBot,2);
    if (tTop==85.0||tTop<=-100) tTop=NAN;
    if (tMid==85.0||tMid<=-100) tMid=NAN;
    if (tBot==85.0||tBot<=-100) tBot=NAN;
    float P_hPa = (bmpOK && i2cPresent(0x77)) ? (bmp.readPressure()/100.0f) : NAN;
    float lux   = (vemlOK && i2cPresent(0x10)) ? veml.readLux() : NAN;
    float irObj=NAN, irAmb=NAN;
    i2cTry(0x5A, [&](){ irObj=mlx.readObjectTempC(); irAmb=mlx.readAmbientTempC(); return !(isnan(irObj)||isnan(irAmb)); });
    sensors_event_t hum, temp; float airT=NAN, airRH=NAN;
    i2cTry(0x38, [&](){ if(!aht.getEvent(&hum,&temp)) return false; airT=temp.temperature; airRH=hum.relative_humidity; return true; });
    bool tdsSAT=false; float tds=tds_mV(tdsSAT);
    float rms = i2sOK ? micRMS() : NAN;
    float P_eff = isnan(P_hPa) ? 1013.25f : P_hPa;
    float doCstar = (!isnan(tMid)) ? o2sat_mgL_local(tMid, P_eff) : NAN;
    server.send(200,"text/html", statusPage(tTop,tMid,tBot,P_hPa,lux,irObj,irAmb,tds,tdsSAT,rms,doCstar, airT, airRH));
  });
  server.begin();
  Serial.println("[HTTP] Server started (STA mode).");
}
void lux_push(unsigned long now_ms, float lux){
  luxbuf[lhead] = { now_ms, lux };
  lhead = (lhead+1) % (LUX_WIN_SEC+1);
  if (lsize < (LUX_WIN_SEC+1)) lsize++;
}
bool lux_change_ratio(unsigned long window_ms, float &ratio_up, float &ratio_down){
  if (lsize < 2) { ratio_up=1.0f; ratio_down=1.0f; return false; }
  unsigned long now = millis();
  int idx = lhead;
  float ref = luxbuf[(idx-1 + LUX_WIN_SEC+1)%(LUX_WIN_SEC+1)].lux;
  float base = ref;
  for (int i=1; i<lsize; i++){
    int j = (idx-1 - i + LUX_WIN_SEC+1)%(LUX_WIN_SEC+1);
    if (now - luxbuf[j].ms >= window_ms){ base = luxbuf[j].lux; break; }
  }
  float up   = (base>0.0f)? (ref/base) : (ref>0.0f? 999.0f:1.0f);
  float down = (ref>0.0f)? (base/ref) : 1.0f;
  ratio_up = up;
  ratio_down = down;
  return true;
}
void pbuf_push(unsigned long now_ms, float p_hPa){
  unsigned long minute_mark = now_ms / 60000UL;
  if (minute_mark == last_pbuf_min_mark) return;
  last_pbuf_min_mark = minute_mark;
  pbuf[phead] = { now_ms, p_hPa };
  phead = (phead + 1) % PBUF_MAX;
  if (psize < PBUF_MAX) psize++;
}
bool pbuf_drop_over_3h(float now_p, float &drop_hPa){
  if (psize == 0) { drop_hPa = 0; return false; }
  unsigned long now = millis();
  unsigned long target = (now > 3UL*60UL*60UL*1000UL) ? (now - 3UL*60UL*60UL*1000UL) : 0;
  float oldest_p = pbuf[(phead - psize + PBUF_MAX) % PBUF_MAX].p;
  unsigned long oldest_ms = pbuf[(phead - psize + PBUF_MAX) % PBUF_MAX].ms;
  for (int i=psize-1; i>=0; --i){
    int idx = (phead - 1 - i + PBUF_MAX) % PBUF_MAX;
    if (pbuf[idx].ms <= target){ oldest_p = pbuf[idx].p; oldest_ms = pbuf[idx].ms; }
    else break;
  }
  drop_hPa = oldest_p - now_p;
  return (millis() - oldest_ms) >= (150UL*60UL*1000UL);
}

unsigned long lastTick=0;

static inline float nz(float v){ return isnan(v)?0.f:v; }
bool ml_predict(float rms, float lux, float dT_tb, float dTdt_mid, float doCstar, float tds_delta, int &pred_idx, float &pred_score){
  if (!tfl) return false;
  float* x = tfl_in->data.f;
  x[0]=nz(rms); x[1]=nz(lux); x[2]=nz(dT_tb); x[3]=nz(dTdt_mid); x[4]=nz(doCstar); x[5]=nz(tds_delta);
  if (tfl->Invoke() != kTfLiteOk) return false;
  pred_idx = 0; pred_score = tfl_out->data.f[0];
  for(int i=1;i<kNumClasses;i++){ float v=tfl_out->data.f[i]; if(v>pred_score){ pred_score=v; pred_idx=i; } }
  return true;
}

void setup(){
  Serial.begin(115200);
  cfgLoad();
  pinMode(RELAY_PIN, OUTPUT);
  pumpOff();
  Wire.begin(I2C_SDA, I2C_SCL);
  Wire.setClock(50000);
  Wire.setTimeOut(200);
  ds18b20.begin();
  prefs.begin("dsmap", true);
  String sTop = prefs.getString("top",  "");
  String sMid = prefs.getString("mid",  "");
  String sBot = prefs.getString("bot",  "");
  prefs.end();
  if (sTop.length()==16 && sMid.length()==16 && sBot.length()==16){
    DeviceAddress at, am, ab;
    haveMap = hexToAddr(sTop,at) && hexToAddr(sMid,am) && hexToAddr(sBot,ab);
    if (haveMap){ memcpy(addrTop,at,8); memcpy(addrMid,am,8); memcpy(addrBot,ab,8); persistDSMap(); }
  }
  initOrPersistDSMap();
  bmpOK  = bmp.begin();
  ahtOK  = aht.begin();
  vemlOK = veml.begin();
  mlxOK  = mlx.begin();
  i2sOK  = i2sInit();
  if (vemlOK){ veml.setGain(VEML7700_GAIN_1); veml.setIntegrationTime(VEML7700_IT_100MS); }
  prefs.begin("collector", true);
  String h=prefs.getString("host","");
  uint16_t p=prefs.getUShort("port",5001);
  String pa=prefs.getString("path","/ingest");
  prefs.end();
  if(h.length()) collHost=h; collPort=p; collPath=pa;
  Serial.println(F("UBi-Guardian"));
  Serial.println(F("Serial: 's' 3s ON, 'l' 10s ON, 'x' OFF"));
  if (!connectSTA_fromPrefs()) startAP(); else startHTTP_STA();
  bl_start_ms = millis();
  hour_window_start_ms = millis();
  tfl_model = tflite::GetModel(ripple_classifier_tflite);
  if (tfl_model->version() != TFLITE_SCHEMA_VERSION) { Serial.println("[ML] schema mismatch"); }
  tfl = std::make_unique<tflite::MicroInterpreter>(tfl_model, tfl_ops, tensor_arena, kTensorArenaSize);
  if (tfl->AllocateTensors() != kTfLiteOk) { Serial.println("[ML] alloc fail"); }
  tfl_in  = tfl->input(0);
  tfl_out = tfl->output(0);
}

void loop(){
  if (Serial.available()){
    char c = Serial.read();
    if (c=='s'){ pumpOn(); pump_active=true; manual_override=true; pump_on_at=millis(); pump_off_at=millis()+3000UL; Serial.println(F("[MANUAL] Short burst (3s)")); }
    else if (c=='l'){ pumpOn(); pump_active=true; manual_override=true; pump_on_at=millis(); pump_off_at=millis()+10000UL; Serial.println(F("[MANUAL] Long burst (10s)")); }
    else if (c=='x'){ pumpOff(); pump_active=false; manual_override=false; pump_off_at=0; Serial.println(F("[MANUAL] Pump OFF")); }
  }
  if (pump_active && pump_off_at!=0 && (long)(millis()-pump_off_at) >= 0){
    pumpOff(); pump_active=false; pump_off_at=0; manual_override=false;
    disturb_since=0; tds_since=0; abrupt_dark_since=0;
    Serial.println(F("[PUMP] Auto OFF"));
  }
  if (apMode){ dns.processNextRequest(); server.handleClient(); }
  else {
    server.handleClient();
    if (millis()-lastConnCheck >= RECHECK_MS){
      lastConnCheck = millis();
      ensurePortalIfDisconnected();
      if (apMode){ server.stop(); MDNS.end(); }
    }
  }
  if (millis()-lastTick >= 1000){
    lastTick = millis();
    ds18b20.requestTemperatures();
    float tTop = readTempRole(addrTop,0);
    float tMid = readTempRole(addrMid,1);
    float tBot = readTempRole(addrBot,2);
    if (tTop==85.0 || tTop<=-100) tTop=NAN;
    if (tMid==85.0 || tMid<=-100) tMid=NAN;
    if (tBot==85.0 || tBot<=-100) tBot=NAN;
    float dT_tb = (isnan(tTop)||isnan(tBot)) ? NAN : (tTop - tBot);
    mid_hist[mid_idx]=tMid; mid_idx=(mid_idx+1)%SLOPE_WIN; if(mid_cnt<SLOPE_WIN) mid_cnt++;
    bool cold_shock=false;
    float dTdt_mid=NAN;
    if(mid_cnt==SLOPE_WIN){
      int oldest = mid_idx;
      float d = tMid - mid_hist[oldest];
      cold_shock = (!isnan(d) && d <= DT60_COLD);
      dTdt_mid = d;
    }
    bool strat_now = (!isnan(dT_tb) && dT_tb > cfg.dt_strat);
    bool inv_now   = (!isnan(dT_tb) && dT_tb < cfg.dt_inv);
    if (strat_now){ if (strat_since==0) strat_since=millis(); } else strat_since=0;
    if (inv_now){ if (inv_since==0)   inv_since=millis(); }   else inv_since=0;
    bool stratified = (strat_since>0 && (millis()-strat_since)>=STRAT_DWELL_MS);
    bool inversion  = (inv_since>0   && (millis()-inv_since)  >=STRAT_DWELL_MS);
    float P_hPa = NAN;
    if (bmpOK && i2cPresent(0x77)) { P_hPa = bmp.readPressure()/100.0f; }
    float P_eff = isnan(P_hPa) ? 1013.25f : P_hPa;
    if (!isnan(P_eff)) pbuf_push(millis(), P_eff);
    float lux = NAN;
    if (vemlOK && i2cPresent(0x10)) { lux = veml.readLux(); }
    float irObj=NAN, irAmb=NAN;
    i2cTry(0x5A, [&](){ irObj=mlx.readObjectTempC(); irAmb=mlx.readAmbientTempC(); return !(isnan(irObj)||isnan(irAmb)); });
    float airT=NAN, airRH=NAN;
    i2cTry(0x38, [&](){ sensors_event_t hum, temp; if(!aht.getEvent(&hum,&temp)) return false; airT=temp.temperature; airRH=hum.relative_humidity; return true; });
    bool tdsSAT = false;
    float tds   = tds_mV(tdsSAT);
    float rms   = i2sOK ? micRMS() : NAN;
    if (!isnan(rms) && rms < 5.0f) rms = 0.0f;
    float doCstar = (!isnan(tMid)) ? o2sat_mgL_local(tMid, P_eff) : NAN;
    float calm_thr_cur = (bl_ready && rms_base > 0) ? (RMS_MULT * rms_base) : 5.0f;
    bool  calm_now     = (!isnan(rms) && rms < calm_thr_cur);
    if (!pump_active && calm_now){
      if (!bl_ready && millis() - bl_start_ms > 10000) bl_ready = true;
      float alpha = bl_ready ? 0.01f : 0.2f;
      if (!isnan(rms) && rms <= calm_thr_cur) rms_base = (1 - alpha) * rms_base + alpha * rms;
      if (!isnan(lux)) {
        lux_base = (1 - alpha) * lux_base + alpha * lux;
        if (!is_day) lux_night_base = (1 - alpha) * lux_night_base + alpha * lux;
      }
      if (!isnan(P_hPa)) p_ref   = (1 - 0.001f) * p_ref + 0.001f * P_hPa;
      if (!isnan(tds))   tds_base = (1 - alpha) * tds_base + alpha * tds;
    }
    float day_on_lux = cfg.night_lux * max(2.0f, cfg.day_on_factor);
    bool was_day = is_day;
    if (!is_day && !isnan(lux) && lux > day_on_lux) { is_day = true; day_switch_ms = millis(); }
    if ( is_day && !isnan(lux) && lux < cfg.night_lux) { is_day = false; day_switch_ms = millis(); }
    bool in_sunrise_grace = (!isnan(lux) && is_day && (millis()-day_switch_ms) < cfg.sunrise_grace_ms);
    lux_push(millis(), isnan(lux)?0.0f:lux);
    float ratio_up=1.0f, ratio_down=1.0f;
    lux_change_ratio(cfg.sudden_window_ms, ratio_up, ratio_down);
    bool night_flashlight = (!is_day && ratio_up >= cfg.sudden_light_factor);
    bool day_abrupt_dark_now = (is_day && ratio_down <= cfg.sudden_dark_factor);
    if (day_abrupt_dark_now){
      if (abrupt_dark_since==0) abrupt_dark_since=millis();
    } else {
      abrupt_dark_since=0;
    }
    bool day_abrupt_dark = (abrupt_dark_since>0 && (millis()-abrupt_dark_since)>=SUDDEN_HOLD_MS);
    float calm_thr = (bl_ready && rms_base>0) ? (RMS_MULT * rms_base) : 5.0f;
    bool calm = (!isnan(rms) && rms < calm_thr);
    bool dark = (!isnan(lux) && lux < cfg.night_lux);
    if ((millis() - hour_window_start_ms) >= HOUR_WINDOW_MS) {
      unsigned long over = (millis() - hour_window_start_ms) % HOUR_WINDOW_MS;
      hour_window_start_ms = millis() - over;
      duty_ms_hour = 0;
    }
    if (pump_active) {
      duty_ms_hour += 1000;
      if (dark) duty_ms_night += 1000;
    }
    static unsigned long bright_since_ms = 0;
    if (dark) { was_dark = true; bright_since_ms = 0; }
    else {
      if (was_dark) {
        if (bright_since_ms==0) bright_since_ms = millis();
        if (millis() - bright_since_ms >= 10UL*60UL*1000UL) {
          duty_ms_night = 0; was_dark = false; bright_since_ms = 0;
        }
      }
    }
    bool tds_spike_now = (bl_ready && !isnan(tds) && ( (tds - tds_base) > max(TDS_JUMP_ABS_MV, TDS_JUMP_FRAC*tds_base) ));
    if (tds_spike_now){ if (tds_since==0) tds_since=millis(); } else tds_since=0;
    bool tds_spike = (tds_since>0 && (millis()-tds_since)>=TDS_DWELL_MS);
    float drop3h=0; bool have3h = pbuf_drop_over_3h(P_eff, drop3h);
    bool baro_drop = (have3h && drop3h >= P_DROP_HPA && !isnan(tMid) && tMid >= 20.0f);
    bool heater_lamp = (!isnan(lux) && lux>cfg.glare_lux) && ( (!isnan(irObj) && !isnan(irAmb) && (irObj - irAmb) >= IR_DELTA_HOT) || (!isnan(irObj) && irObj >= IR_ABS_HOT) );
    bool flashlight_suspicious = night_flashlight && !in_sunrise_grace;
    bool abrupt_dark_hazard    = day_abrupt_dark;
    bool within_self_mask = pump_active && (millis() - pump_on_at) < PUMP_SELF_MASK_MS;
    bool ripple_now = (!isnan(rms) && !isnan(calm_thr) && (rms >= calm_thr)) && !within_self_mask;
    if (ripple_now){
      if (disturb_since==0) disturb_since = millis();
    } else {
      disturb_since = 0;
    }
    bool human_tap = false, disturbance = false;
    if (disturb_since>0){
      uint32_t dur = millis() - disturb_since;
      if (!pump_active && dur >= TAP_MIN_MS && dur <= TAP_MAX_MS) human_tap = true;
      if (dur >= DISTURB_DWELL_MS) disturbance = true;
    }
    bool overheat_un = (!isnan(tMid)  && tMid > cfg.overheat_un && (!isnan(dT_tb) && fabs(dT_tb) < 0.3f));
    bool critical_nan = (isnan(tTop) || isnan(tMid) || isnan(tBot));
    if (critical_nan) fault_cnt++; else fault_cnt=0;
    bool sensor_fault = (fault_cnt >= SENSOR_FAULT_SEC);

    int ml_cls=-1; float ml_conf=0.f; bool ml_ok=false;
    float tds_delta = (bl_ready && !isnan(tds)) ? (tds - tds_base) : 0.f;
    if (cfg.ml_gate && tfl && tfl_in && tfl_out){
      ml_ok = ml_predict(rms, lux, dT_tb, dTdt_mid, doCstar, tds_delta, ml_cls, ml_conf);
      if (ml_ok){
        const char* c = kClasses[ml_cls];
        bool is_calm = (strcmp(c,"calm")==0 && ml_conf>=0.7f);
        bool is_dist = (strcmp(c,"disturbance")==0 && ml_conf>=0.6f);
        bool is_tap  = (strcmp(c,"human-tap")==0 && ml_conf>=0.6f);
        bool is_flash= (strcmp(c,"flashlight-night")==0 && ml_conf>=0.6f);
        bool is_pump = (strcmp(c,"pump-self")==0 && ml_conf>=0.6f);
        if (is_calm){ ripple_now=false; disturb_since=0; }
        if (is_dist){ ripple_now=true; if (disturb_since==0) disturb_since=millis(); }
        if (is_tap){ human_tap=true; }
        if (is_flash){ flashlight_suspicious = flashlight_suspicious || (!is_day); }
        if (is_pump){ ripple_now=false; human_tap=false; disturbance=false; disturb_since=0; }
      }
    }

    bool alert=false; bool pump_help=false; int rec_ms=0; String reason="none";
    String ctx = String(is_day ? "day" : "night");
    bool blockers_present =
        sensor_fault || heater_lamp || flashlight_suspicious || abrupt_dark_hazard ||
        tds_spike || cold_shock || human_tap || disturbance;
    if (sensor_fault){ alert=true; reason="safe_hold_sensor"; }
    if (flashlight_suspicious){ alert=true; if(reason=="none") reason="flashlight_night"; }
    if (abrupt_dark_hazard){   alert=true; if(reason=="none") reason="abrupt_dark_day"; }
    if (heater_lamp){          alert=true; if(reason=="none") reason="heater_lamp"; }
    if (tds_spike){            alert=true; if(reason=="none") reason="tds_spike"; }
    if (cold_shock){           alert=true; if(reason=="none") reason="cold_shock"; }
    if (overheat_un){                          if(reason=="none") reason="uniform_overheat"; }
    if (reason=="none" && human_tap)     reason="human_tap";
    if (reason=="none" && disturbance)   reason="disturbance";

    if (!manual_override && !blockers_present){
      if (!isnan(tMid) && tMid >= cfg.cool_on_c){
        pump_help = true;
        rec_ms = max(rec_ms, (int)cfg.cool_burst_ms);
        if (reason=="none") reason = "cooling_hot"; else reason += "_cooling";
      }
    }
    if (!manual_override && !blockers_present){
      if (stratified || inversion){
        pump_help=true; rec_ms=max(rec_ms, 180000);
        reason = (reason=="none") ? (stratified? "strat":"inversion") : (reason + (stratified? "_strat":"_inv"));
      }
      if (!isnan(doCstar) && doCstar <= cfg.do_lo){
        pump_help=true; rec_ms=max(rec_ms, 15000); reason = (reason=="none")? "low_Cstar": (reason+"_lowC*");
      }
      if (baro_drop && (!isnan(doCstar) && doCstar <= cfg.do_hi)){
        pump_help=true; rec_ms=max(rec_ms, 10000); reason=(reason=="none")? "baro_drop":(reason+"_baro");
      }
      bool mild = (!isnan(doCstar) && doCstar > cfg.do_lo && doCstar <= cfg.do_hi);
      if (dark && calm && !pump_active && mild && (millis()-last_gentle_ms)>=GENTLE_COOLDOWN){
        pump_help=true; rec_ms=max(rec_ms, (int)(GENTLE_SEC*1000UL)); if (reason=="none") reason="night_mild";
      }
    }
    if (!isnan(tMid) && tMid <= cfg.cool_off_c){
      if (pump_help && rec_ms == (int)cfg.cool_burst_ms) { pump_help = false; rec_ms = 0; }
    }
    if (!manual_override && pump_help && rec_ms>0){
      bool hour_cap_exhausted  = (duty_ms_hour >= cfg.cap_hour_ms);
      bool night_cap_exhausted = (was_dark && (duty_ms_night >= cfg.cap_night_ms));
      if (hour_cap_exhausted || night_cap_exhausted){
        String cap_reason = hour_cap_exhausted ? "cap_hour" : "cap_night";
        reason = (reason=="none") ? cap_reason : (reason + "_" + cap_reason);
        pump_help = false; rec_ms = 0;
      }
    }
    if (!manual_override){
      if (blockers_present && pump_active){
        bool min_elapsed = ((millis()-pump_on_at) >= MIN_ON_MS);
        if (min_elapsed || sensor_fault){
          pumpOff(); pump_active=false; pump_off_at=0;
          disturb_since=0; tds_since=0; abrupt_dark_since=0;
          Serial.println(F("[AUTO] Pump OFF (blocker)"));
        }
      } else if (pump_help && !pump_active && rec_ms>0) {
        pumpOn(); pump_active=true; pump_on_at = millis(); pump_off_at = millis() + rec_ms;
        if (reason.indexOf("night_mild")>=0) last_gentle_ms = millis();
        Serial.printf("[AUTO] Pump ON %d ms (%s)\n", rec_ms, reason.c_str());
      } else if (!pump_help && pump_active){
        bool hurts_ready = ((millis()-pump_on_at) >= MIN_ON_MS) &&
                           (tds_spike || cold_shock || sensor_fault || flashlight_suspicious || abrupt_dark_hazard || heater_lamp);
        if (hurts_ready){
          pumpOff(); pump_active=false; pump_off_at=0;
          disturb_since=0; tds_since=0; abrupt_dark_since=0;
          Serial.println(F("[AUTO] Pump OFF (hazard ready)"));
        }
      }
    }
    cur_alert  = sensor_fault || heater_lamp || flashlight_suspicious || abrupt_dark_hazard || tds_spike || cold_shock || overheat_un;
    cur_reason = (cur_alert ? reason : (pump_help ? reason : (human_tap ? "human_tap" : (disturbance ? "disturbance" : "none"))));
    cur_ctx    = String(is_day ? "day" : "night");
    if(pushEnabled && WiFi.status()==WL_CONNECTED){
      int rec = pump_active ? (int)(pump_off_at>millis()? (pump_off_at-millis()):0) : (pump_help?rec_ms:0);
      String body = makeJSON(tTop,tMid,tBot,dT_tb,P_hPa,lux,irObj,irAmb,tds,tdsSAT,rms,doCstar, airT, airRH, cur_alert, cur_reason, rec, cur_ctx);
      if(!postTelemetry(body)) Serial.println(F("[PUSH] Failed"));
    }
  }
}