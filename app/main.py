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
# VictoriaMetrics robust daily fetch (Europe/Paris day boundaries)
# =======================
def fetch_daily_for_calendar_days(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    results = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day,
                            hour=0, minute=0, second=0, tzinfo=tz)

        if day == today:
            end_dt = now
        else:
            end_dt = start_dt + timedelta(days=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
        params = {"query": metric_name, "start": start_ts, "end": end_ts, "step": 60}

        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
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
# Puissance max par jour (en kVA)
# =======================
def fetch_daily_max_power(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()

    max_values = []
    max_times = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, tzinfo=tz)
        if day == today:
            end_dt = now
        else:
            end_dt = start_dt + timedelta(days=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
        params = {"query": metric_name, "start": start_ts, "end": end_ts, "step": 60}

        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
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
# Détection couleur Tempo basée sur consommation réelle
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
        start_dt = datetime(year=day.year, month=day.month, day=day.day, hour=0, tzinfo=tz)
        if day == today:
            end_dt = now
        else:
            end_dt = start_dt + timedelta(days=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        detected_color = "UNKNOWN"
        for color, metrics in tempo_metrics.items():
            for metric in metrics:
                url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
                params = {"query": metric, "start": start_ts, "end": end_ts, "step": 900}
                try:
                    r = requests.get(url, params=params, timeout=10)
                    r.raise_for_status()
                    data = r.json()
                    res_list = data.get("data", {}).get("result", [])
                    if res_list and res_list[0].get("values"):
                        first_val = float(res_list[0]["values"][0][1])
                        last_val = float(res_list[0]["values"][-1][1])
                        if last_val > first_val:  # consommation détectée
                            detected_color = color
                            break
                except Exception as e:
                    print(f"⚠️ Erreur fetch_daily_tempo_colors {day} ({metric}): {e}")
            if detected_color != "UNKNOWN":
                break

        colors.append(detected_color)

    return colors

# =======================
# JSON principal (COMPLET)
# =======================
def build_linky_payload_exact(dailyweek_HP=None, dailyweek_HC=None,
                              dailyweek_MP=None, dailyweek_MP_time=None,
                              dailyweek_Tempo=None):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()
    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    hp = dailyweek_HP if dailyweek_HP else [0.0]*7
    hc = dailyweek_HC if dailyweek_HC else [0.0]*7
    mp = dailyweek_MP if dailyweek_MP else [0]*7
    mp_time = dailyweek_MP_time if dailyweek_MP_time else [today.strftime("%Y-%m-%d 00:00:00")]*7
    tempo = dailyweek_Tempo if dailyweek_Tempo else ["UNKNOWN"]*7

    daily = [round(hp[i] + hc[i], 2) for i in range(7)]

    payload = {
        "serviceEnedis": "myElectricalData",
        "typeCompteur": "consommation",
        "unit_of_measurement": "kWh",
        "daily": daily,
        "dailyweek": dailyweek_dates,
        "dailyweek_HC": hc,
        "dailyweek_HP": hp,
        "yesterday_HP": hp[1] if len(hp) > 1 else 0,
        "yesterday_HC": hc[1] if len(hc) > 1 else 0,
        "dailyweek_MP": mp,
        "dailyweek_MP_time": mp_time,
        "dailyweek_MP_over": [val > 7 for val in mp],
        "dailyweek_Tempo": tempo,
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

    # Discovery unique
    linky_discovery_payload = {
        "name": "Linky Test",
        "state_topic": LINKY_STATE_TOPIC,
        "value_template": "{{ value_json.daily[0] }}",
        "json_attributes_topic": LINKY_STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "icon": "mdi:counter",
        "unique_id": "linky_test_sensor",
        "device": {"identifiers": ["linky"], "name": "Compteur Linky", "manufacturer": "Enedis", "model": "Linky"}
    }
    client.publish(LINKY_DISCOVERY_TOPIC, json.dumps(linky_discovery_payload), qos=1, retain=True)

    print("\n--- Boucle MQTT démarrée ---")
    last_full_day = None

    while True:
        tz = pytz.timezone("Europe/Paris")
        today = datetime.now(tz).date()

        # Nouveau jour → recalcul complet
        if last_full_day != today:
            print("\n🌅 Nouveau jour détecté → recalcul complet")
            hpjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=7)
            hpjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=7)
            hpjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=7)
            dailyweek_HP = [round(hpjb[i] + hpjw[i] + hpjr[i], 2) for i in range(7)]

            hcjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=7)
            hcjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=7)
            hcjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=7)
            dailyweek_HC = [round(hcjb[i] + hcjw[i] + hcjr[i], 2) for i in range(7)]

            dailyweek_MP, dailyweek_MP_time = fetch_daily_max_power(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=7)
            dailyweek_Tempo = fetch_daily_tempo_colors(VM_HOST, VM_PORT, days=7)

            print(f"📊 dailyweek_HP = {dailyweek_HP}")
            print(f"📊 dailyweek_HC = {dailyweek_HC}")
            print(f"⚡ dailyweek_MP = {dailyweek_MP}")
            print(f"⏰ dailyweek_MP_time = {dailyweek_MP_time}")
            print(f"🎨 dailyweek_Tempo = {dailyweek_Tempo}")

            last_full_day = today
        else:
            # Sinon → mise à jour seulement des colonnes du jour
            print("\n🔄 Mise à jour partielle (5 min)")
            hpjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=1)
            hpjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=1)
            hpjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=1)
            dailyweek_HP[0] = round(hpjb[0] + hpjw[0] + hpjr[0], 2)

            hcjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=1)
            hcjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=1)
            hcjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=1)
            dailyweek_HC[0] = round(hcjb[0] + hcjw[0] + hcjr[0], 2)

            mp_tmp, mp_time_tmp = fetch_daily_max_power(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=1)
            dailyweek_MP[0] = mp_tmp[0]
            dailyweek_MP_time[0] = mp_time_tmp[0]

            tempo_tmp = fetch_daily_tempo_colors(VM_HOST, VM_PORT, days=1)
            dailyweek_Tempo[0] = tempo_tmp[0]

        # Publication MQTT
        now = datetime.now().astimezone().isoformat()
        linky_payload = build_linky_payload_exact(
            dailyweek_HP, dailyweek_HC, dailyweek_MP, dailyweek_MP_time, dailyweek_Tempo
        )
        linky_payload["lastUpdate"] = now
        linky_payload["timeLastCall"] = now

        client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
        print(f"📡 JSON publié sur {LINKY_STATE_TOPIC}")

        time.sleep(300)  # 5 min


if __name__ == "__main__":
    main()
