#include <Wire.h>
#include <LiquidCrystal_I2C.h>

LiquidCrystal_I2C lcd(0x27, 16, 2);

const int TRIG1 = 9,  ECHO1 = 10;   // right sensor (next to the LCD)
const int TRIG2 = 11, ECHO2 = 12;   // left sensor

const float SENSOR_DISTANCE_CM = 16.0;   // distance between sensor centers
const float THRESHOLD_CM = 10.0;         // closer than this = object detected
const unsigned long TIMEOUT_MS = 5000;   // give up if the second sensor is not reached

int state = 0;   // 0 = waiting, 1 = started at sensor 1, 2 = started at sensor 2
unsigned long startTime = 0;
unsigned long passCount = 0;   // pass counter

float measure(int trig, int echo) {
  digitalWrite(trig, LOW);
  delayMicroseconds(2);
  digitalWrite(trig, HIGH);
  delayMicroseconds(10);
  digitalWrite(trig, LOW);
  long duration = pulseIn(echo, HIGH, 30000);
  return duration * 0.0343 / 2;   // distance in cm
}

bool detected(float distance) {
  return distance > 0 && distance < THRESHOLD_CM;
}

void showMessage(const char* line1, const char* line2) {
  lcd.clear();
  lcd.setCursor(0, 0); lcd.print(line1);
  lcd.setCursor(0, 1); lcd.print(line2);
}

void sendJson(int startSensor, const char* direction, unsigned long elapsed, float speedKmh) {
  Serial.print("{\"event\":\"pass\"");
  Serial.print(",\"id\":");           Serial.print(passCount);
  Serial.print(",\"start_sensor\":"); Serial.print(startSensor);
  Serial.print(",\"direction\":\"");  Serial.print(direction); Serial.print("\"");
  Serial.print(",\"elapsed_ms\":");   Serial.print(elapsed);
  Serial.print(",\"distance_cm\":");  Serial.print(SENSOR_DISTANCE_CM, 1);
  Serial.print(",\"speed_kmh\":");    Serial.print(speedKmh, 2);
  Serial.println("}");
}

void reportResult(unsigned long elapsed, int startSensor, const char* arrow, const char* direction) {
  float speedCms = SENSOR_DISTANCE_CM / (elapsed / 1000.0);
  float speedKmh = speedCms * 0.036;
  passCount++;

  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("Velocidade:");
  lcd.setCursor(0, 1);
  lcd.print(speedKmh, 2); lcd.print(" km/h "); lcd.print(arrow);

  sendJson(startSensor, direction, elapsed, speedKmh);

  delay(2000);
  state = 0;
}

void setup() {
  Serial.begin(9600);
  pinMode(TRIG1, OUTPUT); pinMode(ECHO1, INPUT);
  pinMode(TRIG2, OUTPUT); pinMode(ECHO2, INPUT);

  lcd.init();
  lcd.backlight();
  showMessage("Radar pronto", "Aguardando...");
}

void loop() {
  if (state == 0) {
    if (detected(measure(TRIG1, ECHO1))) {
      startTime = millis();
      state = 1;
      showMessage("Passou sensor 1", "Medindo...");
    } else {
      delay(30);   // avoid crosstalk between the two sensors
      if (detected(measure(TRIG2, ECHO2))) {
        startTime = millis();
        state = 2;
        showMessage("Passou sensor 2", "Medindo...");
      }
    }
  } else if (state == 1) {
    if (detected(measure(TRIG2, ECHO2)))
      reportResult(millis() - startTime, 1, "<-", "right_to_left");
  } else {
    if (detected(measure(TRIG1, ECHO1)))
      reportResult(millis() - startTime, 2, "->", "left_to_right");
  }

  if (state != 0 && millis() - startTime > TIMEOUT_MS) {
    showMessage("Tempo esgotado", "Aguardando...");
    state = 0;
  }
  delay(20);
}