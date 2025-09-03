import os
import sys
import time
import json
import threading
import requests
from datetime import datetime, timedelta
import pytz
import paho.mqtt.client as mqtt

print("--- Début de l'exécution du script Linky + VictoriaMetrics ---")
time.sleep(1)
print(f"Version Python: {sys.version}")

# =======================
# CONFIG via ENV
# =======================
LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT") or 1883)

VM_HOST = os.getenv("VM_HOST", "127.0.0.1")
VM_PORT = int(os.getenv("VM_PORT") or 8428)

REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", 300))  # secondes, 300s = 5 minutes

METRIC_NAMEhpjb = "sensor.linky_tempo_index_bbrhpjb_value"
METRIC_NAMEhcjb = "sensor.linky_tempo_index_bbrhcjb_value"
METRIC_NAMEhpjw = "sensor.linky_tempo_index_bbrhpjw_value"
METRIC_NAMEhcjw = "sensor.linky_tempo_index_bbrhcjw_value"
METRIC_NAMEhpjr = "sensor.linky_tempo_index_bbrhpjr_value"
METRIC_NAMEhcjr = "sensor.linky_tempo_index_bbrhcjr_value"
METRIC_NAMEpcons = "sensor.linky_puissance_consommee_value"

MQTT_RETAIN = True

# =======================
# MQTT Topics
# =======================
LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

# =======================
# Utils : Query VM
# =======================
def query_vm_range(vm_host, vm_port, metric_name, start_ts, end_ts, step):
    url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
    params = {"query": metric_name, "start": start_ts, "end": end_ts, "step": step}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# =======================
# Consommation quotidienne
# =======================
def fetch_daily_for_calendar_days(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    results = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = tz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
        if day == today:
            end_dt = now
        else:
            end_dt = start_dt + timedelta(days=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        try:
            data = query_vm_range(vm_host, vm_port, metric_name, start_ts, end_ts, 60)
            res_list = data.get("data", {}).get("result", [])
            if not res_list:
                results.append(0.0)
                continue
            values = res_list[0].get("values", [])
            if not values:
                results.append(0.0)
                continue
            first_val = float(values[0][1])
            last_val = float(values[-1][1])
            diff = last_val - first_val
            if diff < 0:
                diff = 0.0
            results.append(round(diff, 2))
        except Exception as e:
            print(f"❌ Erreur fetch_daily_for_calendar_days '{metric_name}' pour {day}: {e}")
            results.append(0.0)

    return results

# =======================
# Puissance max par jour
# =======================
def fetch_daily_max_power(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    max_values = []
    max_times = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = tz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
        if day == today:
            end_dt = now
        else:
            end_dt = start_dt + timedelta(days=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        try:
            data = query_vm_range(vm_host, vm_port, metric_name, start_ts, end_ts, 60)
            res_list = data.get("data", {}).get("result", [])
            if not res_list or not res_list[0].get("values"):
                max_values.append(0)
                max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))
                continue
            values = res_list[0]["values"]
            max_val = -1
            max_ts = start_ts
            for ts, val in values:
                v = float(val) / 1000.0  # conversion en kVA
                if v > max_val:
                    max_val = v
                    max_ts = int(ts)
            max_values.append(round(max_val, 2))
            max_times.append(datetime.fromtimestamp(max_ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            print(f"❌ Erreur fetch_daily_max_power '{metric_name}' pour {day}: {e}")
            max_values.append(0)
            max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))

    return max_values, max_times

# =======================
# Détection couleur Tempo
# =======================
def fetch_daily_tempo_colors(vm_host, vm_port, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    colors = []

    tempo_metrics = {
        "BLUE": [METRIC_NAMEhpjb, METRIC_NAMEhcjb],
        "WHITE": [METRIC_NAMEhpjw, METRIC_NAMEhcjw],
        "RED": [METRIC_NAMEhpjr, METRIC_NAMEhcjr],
    }

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = tz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
        end_dt = start_dt + timedelta(days=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        detected_color = "UNKNOWN"
        try:
            for color, metrics in tempo_metrics.items():
                found = False
                for metric in metrics:
                    data = query_vm_range(vm_host, vm_port, metric, start_ts, end_ts, 600)
                    res_list = data.get("data", {}).get("result", [])
                    if res_list and res_list[0].get("values"):
                        first_val = float(res_list[0]["values"][0][1])
                        last_val = float(res_list[0]["values"][-1][1])
                        if last_val > first_val:
                            detected_color = color
                            found = True
                            break
                if found:
                    break
        except Exception as e:
            print(f"⚠️ Erreur fetch_daily_tempo_colors pour {day}: {e}")

        colors.append(detected_color)

    return colors

# =======================
# Consommation annuelle
# =======================
def fetch_yearly_consumption(vm_host, vm_port, metrics, year_offset=0):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    year = now.year - year_offset

    start_dt = tz.localize(datetime(year, 1, 1, 0, 0, 0))
    if year_offset == 0:
        end_dt = now
    else:
        end_dt = tz.localize(datetime(year, now.month, now.day, now.hour, now.minute, now.second))

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    total = 0.0
    for metric in metrics:
        try:
            data = query_vm_range(vm_host, vm_port, metric, start_ts, end_ts, 3600)
            res_list = data.get("data", {}).get("result", [])
            if res_list and res_list[0].get("values"):
                first_val = float(res_list[0]["values"][0][1])
                last_val = float(res_list[0]["values"][-1][1])
                diff = last_val - first_val
                if diff > 0:
                    total += diff
        except Exception as e:
            print(f"❌ Erreur fetch_yearly_consumption {metric} ({year}): {e}")

    return round(total, 2)

# =======================
# JSON principal
# =======================
def build_linky_payload_exact(dailyweek_HP, dailyweek_HC, dailyweek_MP,
                              dailyweek_MP_time, dailyweek_Tempo,
                              current_year, current_year_last_year, yearly_evolution):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()
    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    daily = [round(dailyweek_HP[i] + dailyweek_HC[i], 2) for i in range(7)]

    payload = {
        "serviceEnedis": "myElectricalData",
        "typeCompteur": "consommation",
        "unit_of_measurement": "kWh",
        "current_year": current_year,
        "current_year_last_year": current_year_last_year,
        "yearly_evolution": yearly_evolution,
        "last_month": 1200,
        "last_month_last_year": 1100,
        "monthly_evolution": 9.1,
        "current_month": 1300,
        "current_month_last_year": 1250,
        "current_month_evolution": 4,
        "current_week": 300,
        "last_week": 310,
        "current_week_evolution": -3.2,
        "yesterday": daily[1] if len(daily) > 1 else 0,
        "day_2": daily[2] if len(daily) > 2 else 0,
        "yesterday_evolution": -10,
        "daily": daily,
        "dailyweek": dailyweek_dates,
        "dailyweek_cost": [1.2,1.3,1.1,1.4,1.3,1.2,1.3],
        "dailyweek_costHC": [0.5,0.6,0.5,0.6,0.5,0.6,0.5],
        "dailyweek_costHP": [0.7,0.7,0.6,0.8,0.8,0.6,0.8],
        "dailyweek_HC": dailyweek_HC,
        "daily_cost": 0.6,
        "yesterday_HP": dailyweek_HP[1] if len(dailyweek_HP) > 1 else 0,
        "yesterday_HC": dailyweek_HC[1] if len(dailyweek_HC) > 1 else 0,
        "dailyweek_HP": dailyweek_HP,
        "dailyweek_MP": dailyweek_MP,
        "dailyweek_MP_over": [val > 7 for val in dailyweek_MP],
        "dailyweek_MP_time": dailyweek_MP_time,
        "dailyweek_Tempo": dailyweek_Tempo,
        "errorLastCall": "",
        "versionUpdateAvailable": False,
        "versionGit": "1.0.0",
        "peak_offpeak_percent": 45
    }
    return payload

# =======================
# SCRIPT PRINCIPAL
# =======================
def main():
    client = mqtt.Client(protocol=mqtt.MQTTv5)
    evt = threading.Event()

    def on_connect(c, u, flags, rc, props=None):
        if rc == 0:
            print("✅ MQTT connecté")
            evt.set()
        else:
            print(f"❌ MQTT échec (rc={rc})")

    client.on_connect = on_connect
    if LOGIN and PASSWORD:
        client.username_pw_set(LOGIN, PASSWORD)

    client.loop_start()
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        print(f"❌ Connexion MQTT impossible: {e}")
        sys.exit(1)

    if not evt.wait(timeout=10):
        print("⛔ Timeout MQTT")
        sys.exit(1)

    linky_discovery_payload = {
        "name": "Linky Test",
        "state_topic": LINKY_STATE_TOPIC,
        "value_template": "{{ value_json.current_year }}",
        "json_attributes_topic": LINKY_STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "icon": "mdi:counter",
        "unique_id": "linky_test_sensor",
        "device": {"identifiers": ["linky"], "name": "Compteur Linky", "manufacturer": "Enedis", "model": "Linky"}
    }
    client.publish(LINKY_DISCOVERY_TOPIC, json.dumps(linky_discovery_payload), qos=1, retain=True)

    print("\n--- Boucle MQTT démarrée ---")
    while True:
        print("\n--- Nouveau cycle ---")

        hpjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=7)
        hpjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=7)
        hpjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=7)
        dailyweek_HP = [round(hpjb[i] + hpjw[i] + hpjr[i], 2) for i in range(7)]

        hcjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=7)
        hcjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=7)
        hcjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=7)
        dailyweek_HC = [round(hcjb[i] + hcjw[i] + hcjr[i], 2) for i in range(7)]

        print(f"📊 dailyweek_HP = {dailyweek_HP}")
        print(f"📊 dailyweek_HC = {dailyweek_HC}")

        dailyweek_MP, dailyweek_MP_time = fetch_daily_max_power(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=7)
        print(f"⚡ dailyweek_MP = {dailyweek_MP}")
        print(f"⏰ dailyweek_MP_time = {dailyweek_MP_time}")

        dailyweek_Tempo = fetch_daily_tempo_colors(VM_HOST, VM_PORT, days=7)
        print(f"🎨 dailyweek_Tempo = {dailyweek_Tempo}")

        # consommation annuelle
        metrics_all = [METRIC_NAMEhpjb, METRIC_NAMEhcjb, METRIC_NAMEhpjw, METRIC_NAMEhcjw, METRIC_NAMEhpjr, METRIC_NAMEhcjr]
        current_year = fetch_yearly_consumption(VM_HOST, VM_PORT, metrics_all, year_offset=0)
        current_year_last_year = fetch_yearly_consumption(VM_HOST, VM_PORT, metrics_all, year_offset=1)

        yearly_evolution = 0.0
        if current_year_last_year > 0:
            yearly_evolution = round(((current_year - current_year_last_year) / current_year_last_year) * 100, 2)

        now = datetime.now().astimezone().isoformat()
        linky_payload = build_linky_payload_exact(
            dailyweek_HP, dailyweek_HC, dailyweek_MP, dailyweek_MP_time,
            dailyweek_Tempo, current_year, current_year_last_year, yearly_evolution
        )
        linky_payload["lastUpdate"] = now
        linky_payload["timeLastCall"] = now

        result2 = client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
        result2.wait_for_publish()
        print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")

        print(f"\n--- Mise en veille pour {REFRESH_INTERVAL} secondes ---")
        time.sleep(REFRESH_INTERVAL)


if __name__ == "__main__":
    main()
