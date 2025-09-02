#!/usr/bin/env python3
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

# metrics index Linky (HP/HC pour jb, jw, jr) + puissance instantanée
METRIC_NAMEhpjb = "sensor.linky_tempo_index_bbrhpjb_value"
METRIC_NAMEhcjb = "sensor.linky_tempo_index_bbrhcjb_value"
METRIC_NAMEhpjw = "sensor.linky_tempo_index_bbrhpjw_value"
METRIC_NAMEhcjw = "sensor.linky_tempo_index_bbrhcjw_value"
METRIC_NAMEhpjr = "sensor.linky_tempo_index_bbrhpjr_value"
METRIC_NAMEhcjr = "sensor.linky_tempo_index_bbrhcjr_value"
METRIC_NAMEpcons = os.getenv("METRIC_NAMEpcons", "sensor.linky_puissance_consommee_value")

# puissance souscrite configurable (en W) - ex: 6000 pour 6 kVA
PUISSANCE_SOUSCRITE = int(os.getenv("PUISSANCE_SOUSCRITE", "6000"))

MQTT_RETAIN = True

# =======================
# MQTT Topics
# =======================
STATE_TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
DISCOVERY_TOPIC = "homeassistant/sensor/consommation_veille_linky/config"

LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

# =======================
# Helpers pour VM (jours calendaires Europe/Paris)
# =======================
def fetch_daily_for_calendar_days(vm_host, vm_port, metric_name, days=7, step=60):
    """
    Retourne liste de consommations journalières (last-first) pour chaque jour :
    ordre: [aujourd'hui, hier, ..., J-(days-1)]
    step : pas de la requête en secondes (default 60)
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    results = []

    url = f"http://{vm_host}:{vm_port}/api/v1/query_range"

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day,
                            hour=0, minute=0, second=0, tzinfo=tz)
        end_dt = now if day == today else (start_dt + timedelta(days=1))

        params = {
            "query": metric_name,
            "start": int(start_dt.timestamp()),
            "end": int(end_dt.timestamp()),
            "step": step
        }

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

            # values = [ [ts, val], ... ] trié asc
            first_val = float(values[0][1])
            last_val = float(values[-1][1])
            diff = last_val - first_val
            if diff < 0:
                diff = 0.0
            results.append(round(diff, 2))
        except Exception as e:
            print(f"❌ Erreur fetch_daily_for_calendar_days metric='{metric_name}' pour {day}: {e}")
            results.append(0.0)

    return results  # ordre: aujourd'hui, hier, ...

def fetch_daily_max_and_time(vm_host, vm_port, metric_name, days=7, step=60):
    """
    Pour chaque jour retourne (max_vals, max_times) :
      - max_vals: int (0 si pas de donnée)
      - max_times: str "YYYY-MM-DD HH:MM:SS" en Europe/Paris ("" si pas de donnée)
    ordre: [aujourd'hui, hier, ...]
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()

    max_vals = []
    max_times = []
    url = f"http://{vm_host}:{vm_port}/api/v1/query_range"

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day,
                            hour=0, minute=0, second=0, tzinfo=tz)
        end_dt = now if day == today else (start_dt + timedelta(days=1))

        params = {
            "query": metric_name,
            "start": int(start_dt.timestamp()),
            "end": int(end_dt.timestamp()),
            "step": step
        }

        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            res_list = data.get("data", {}).get("result", [])
            if not res_list:
                max_vals.append(0)
                max_times.append("")
                continue

            values = res_list[0].get("values", [])
            if not values:
                max_vals.append(0)
                max_times.append("")
                continue

            # find max and first index of max
            numeric = []
            for v in values:
                try:
                    numeric.append(float(v[1]))
                except:
                    numeric.append(float("-inf"))

            max_value = max(numeric)
            if max_value == float("-inf"):
                max_vals.append(0)
                max_times.append("")
                continue

            idx = numeric.index(max_value)
            ts = float(values[idx][0])
            dt = datetime.fromtimestamp(ts, tz)
            timestr = dt.strftime("%Y-%m-%d %H:%M:%S")

            if max_value < 0:
                max_value = 0

            max_vals.append(int(round(max_value)))
            max_times.append(timestr)
        except Exception as e:
            print(f"❌ Erreur fetch_daily_max_and_time metric='{metric_name}' pour {day}: {e}")
            max_vals.append(0)
            max_times.append("")

    return max_vals, max_times  # ordre: aujourd'hui, hier, ...

# =======================
# fetch consumption yesterday (garde si besoin)
# =======================
def fetch_first_last_yesterday(vm_host, vm_port, metric_name):
    url = f"http://{vm_host}:{vm_port}/api/v1/query"
    queries = {
        "first": f"first_over_time({metric_name}[1d] offset 1d)",
        "last":  f"last_over_time({metric_name}[1d] offset 1d)"
    }
    result = {}
    for key, query in queries.items():
        try:
            r = requests.get(url, params={'query': query}, timeout=10)
            r.raise_for_status()
            data = r.json()
            res_list = data.get("data", {}).get("result", [])
            if res_list and "value" in res_list[0]:
                result[key] = float(res_list[0]["value"][1])
                print(f"VM: {key} value = {result[key]}")
            else:
                result[key] = None
                print(f"VM: pas de valeur pour {key}")
        except Exception as e:
            print(f"❌ Erreur VM query '{query}': {e}")
            result[key] = None

    if result.get("first") is None or result.get("last") is None:
        return None
    return round(result["last"] - result["first"], 2)

# =======================
# JSON principal (COMPLET)
# =======================
def build_linky_payload_exact(dailyweek_HP=None, dailyweek_HC=None,
                              dailyweek_MP=None, dailyweek_MP_time=None):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()

    # dailyweek : aujourd'hui (index 0), puis J-1, J-2, ... J-6
    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    hp = dailyweek_HP if dailyweek_HP else [0.0]*7
    hc = dailyweek_HC if dailyweek_HC else [0.0]*7
    mp = dailyweek_MP if dailyweek_MP else [0]*7
    mp_time = dailyweek_MP_time if dailyweek_MP_time else ["" for _ in range(7)]

    # daily = HP + HC (même ordre : aujourd'hui, ...)
    daily = [round(hp[i] + hc[i], 2) for i in range(7)]

    # Calcul du dépassement en fonction de PUISSANCE_SOUSCRITE (W)
    mp_over = [(val > PUISSANCE_SOUSCRITE) for val in mp]

    payload = {
        "serviceEnedis": "myElectricalData",
        "typeCompteur": "consommation",
        "unit_of_measurement": "kWh",
        "current_year": 14560,
        "current_year_last_year": 13200,
        "yearly_evolution": 10.2,
        "last_month": 1200,
        "last_month_last_year": 1100,
        "monthly_evolution": 9.1,
        "current_month": 1300,
        "current_month_last_year": 1250,
        "current_month_evolution": 4,
        "current_week": 300,
        "last_week": 310,
        "current_week_evolution": -3.2,
        "yesterday": 45,
        "day_2": 50,
        "yesterday_evolution": -10,
        "daily": daily,
        "dailyweek": dailyweek_dates,
        "dailyweek_cost": [1.2,1.3,1.1,1.4,1.3,1.2,1.3],
        "dailyweek_costHC": [0.5,0.6,0.5,0.6,0.5,0.6,0.5],
        "dailyweek_costHP": [0.7,0.7,0.6,0.8,0.8,0.6,0.8],
        "dailyweek_HC": hc,
        "daily_cost": 0.6000000000000001,
        "yesterday_HP": hp[1] if len(hp) > 1 else 0,
        "yesterday_HC": hc[1] if len(hc) > 1 else 0,
        "dailyweek_HP": hp,
        "dailyweek_MP": mp,
        "dailyweek_MP_over": mp_over,
        "dailyweek_MP_time": mp_time,
        "dailyweek_MP_over_info": {
            "puissance_souscrite_W": PUISSANCE_SOUSCRITE
        },
        "dailyweek_MP_over_detection_method": "computed_local_script",
        "dailyweek_MP_over_note": "True si dailyweek_MP > PUISSANCE_SOUSCRITE (W).",
        "dailyweek_MP_over_by_day_example": ["true","false","true","false","false","true","false"],
        "dailyweek_MP_over_debug": [],  # placeholder utile pour debug si besoin
        "dailyweek_MP_over_threshold": PUISSANCE_SOUSCRITE,
        "dailyweek_MP_over_unit": "W",
        "dailyweek_MP_over_timestamp_tz": "Europe/Paris",
        "dailyweek_MP_over_generated_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S"),
        "dailyweek_MP_over_version": "v1",
        "dailyweek_MP_over_author": "scripted",
        "dailyweek_Tempo": ["WHITE","WHITE","RED","BLUE","WHITE","RED","BLUE"],
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

    # --- Discovery consommation veille ---
    discovery_payload = {
        "name": "Consommation veille Linky",
        "state_topic": STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "icon": "mdi:flash",
        "unique_id": "linky_veille_sensor",
        "device": {
            "identifiers": ["linky"],
            "name": "Compteur Linky",
            "manufacturer": "Enedis",
            "model": "Linky"
        }
    }
    client.publish(DISCOVERY_TOPIC, json.dumps(discovery_payload), qos=1, retain=True)
    print(f"📡 Discovery publié: {DISCOVERY_TOPIC}")

    # --- Discovery sensor.linky_test ---
    linky_discovery_payload = {
        "name": "Linky Test",
        "state_topic": LINKY_STATE_TOPIC,
        "value_template": "{{ value_json.current_year }}",
        "json_attributes_topic": LINKY_STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "icon": "mdi:counter",
        "unique_id": "linky_test_sensor",
        "device": {
            "identifiers": ["linky"],
            "name": "Compteur Linky",
            "manufacturer": "Enedis",
            "model": "Linky"
        }
    }
    client.publish(LINKY_DISCOVERY_TOPIC, json.dumps(linky_discovery_payload), qos=1, retain=True)
    print(f"📡 Discovery publié: {LINKY_DISCOVERY_TOPIC}")

    # =======================
    # Boucle principale
    # =======================
    print("\n--- Boucle MQTT démarrée ---")
    while True:
        print("\n--- Début du cycle quotidien ---")

        # --- Consommations HP (jb + jw + jr) pour chaque jour (aujourd'hui, hier, ...)
        hpjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=7)
        hpjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=7)
        hpjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=7)
        dailyweek_HP = [round((hpjb[i] or 0) + (hpjw[i] or 0) + (hpjr[i] or 0), 2) for i in range(7)]

        # --- Consommations HC (jb + jw + jr)
        hcjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=7)
        hcjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=7)
        hcjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=7)
        dailyweek_HC = [round((hcjb[i] or 0) + (hcjw[i] or 0) + (hcjr[i] or 0), 2) for i in range(7)]

        # --- Puissance max et time
        dailyweek_MP, dailyweek_MP_time = fetch_daily_max_and_time(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=7)

        # --- Calcul dailyweek_MP_over (dépassé ?)
        dailyweek_MP_over = [(val > PUISSANCE_SOUSCRITE) for val in dailyweek_MP]

        # --- Logs : affichage complet avant publication ---
        tz = pytz.timezone("Europe/Paris")
        today = datetime.now(tz).date()
        dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

        print("📊 dailyweek (dates)         =", dailyweek_dates)
        print(f"📊 dailyweek_HP             = {dailyweek_HP}")
        print(f"📊 dailyweek_HC             = {dailyweek_HC}")
        print(f"📊 daily (HP+HC)           = {[round(dailyweek_HP[i] + dailyweek_HC[i],2) for i in range(7)]}")
        print(f"📊 dailyweek_MP             = {dailyweek_MP}")
        print(f"📊 dailyweek_MP_time        = {dailyweek_MP_time}")
        print(f"📊 dailyweek_MP_over        = {dailyweek_MP_over} (threshold={PUISSANCE_SOUSCRITE} W)")

        # --- Publier JSON complet sensor.linky_test ---
        now = datetime.now().astimezone().isoformat()
        linky_payload = build_linky_payload_exact(dailyweek_HP, dailyweek_HC, dailyweek_MP, dailyweek_MP_time)
        linky_payload["lastUpdate"] = now
        linky_payload["timeLastCall"] = now

        # Log JSON complet (formaté)
        print("📡 JSON complet à publier :")
        print(json.dumps(linky_payload, indent=2, ensure_ascii=False))

        # publish
        try:
            result2 = client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
            result2.wait_for_publish()
            print(f"✅ JSON complet publié sur {LINKY_STATE_TOPIC}")
        except Exception as e:
            print(f"❌ Erreur publication MQTT: {e}")

        print("\n--- Cycle terminé. Mise en veille pour 24h ---")
        time.sleep(24 * 3600)

# =======================
# Lancement script
# =======================
if __name__ == "__main__":
    main()
