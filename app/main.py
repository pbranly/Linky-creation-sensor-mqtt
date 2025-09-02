import os
import sys
import time
import json
import threading
import requests
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
import pytz

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

MQTT_RETAIN = True

# =======================
# MQTT Topics
# =======================
STATE_TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
DISCOVERY_TOPIC = "homeassistant/sensor/consommation_veille_linky/config"

LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

# =======================
# VictoriaMetrics
# =======================
def fetch_daily_differences(vm_host, vm_port, metric_name, days=7):
    """
    Récupère la consommation journalière (diff entre last et first) pour chaque jour.
    Inclut aujourd’hui (même incomplet).
    Retourne une liste [jour0=aujourd’hui, jour-1, ..., jour-(days-1)]
    """
    url = f"http://{vm_host}:{vm_port}/api/v1/query"
    results = []
    for i in range(days):
        query_first = f"first_over_time({metric_name}[1d] offset {i}d)"
        query_last = f"last_over_time({metric_name}[1d] offset {i}d)"
        try:
            r1 = requests.get(url, params={"query": query_first}, timeout=10)
            r2 = requests.get(url, params={"query": query_last}, timeout=10)
            f = r1.json().get("data", {}).get("result", [])
            l = r2.json().get("data", {}).get("result", [])
            if f and l:
                first = float(f[0]["value"][1])
                last = float(l[0]["value"][1])
                results.append(round(last - first, 2))
            else:
                results.append(0.0)
        except Exception as e:
            print(f"❌ Erreur VM query jour-{i} '{metric_name}': {e}")
            results.append(0.0)
    return results

# =======================
# JSON principal
# =======================
def build_linky_payload_exact(dailyweek_HP=None, dailyweek_HC=None):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()

    # J=0 aujourd’hui à gauche, J-6 à droite
    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    hp = dailyweek_HP if dailyweek_HP else [0]*7
    hc = dailyweek_HC if dailyweek_HC else [0]*7

    # daily = somme HP + HC
    daily = [round(hp[i] + hc[i], 2) for i in range(7)]

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
        "daily": daily,  # <── calcul dynamique
        "dailyweek": dailyweek_dates,
        "dailyweek_cost": [1.2,1.3,1.1,1.4,1.3,1.2,1.3],
        "dailyweek_costHC": [0.5,0.6,0.5,0.6,0.5,0.6,0.5],
        "dailyweek_costHP": [0.7,0.7,0.6,0.8,0.8,0.6,0.8],
        "dailyweek_HC": hc,
        "daily_cost": 0.6000000000000001,
        "yesterday_HP": hp[1] if len(hp) > 1 else 0,
        "yesterday_HC": hc[1] if len(hc) > 1 else 0,
        "dailyweek_HP": hp,
        "dailyweek_MP": [7,6,8,7,6,7,7],
        "dailyweek_MP_over": [False, False, True, False, False, True, False],
        "dailyweek_MP_time": [
            "2025-09-01 00:00:00",
            "2025-08-31 10:55:08",
            "2025-08-30 09:04:30",
            "2025-08-29 22:08:53",
            "2025-08-28 22:03:52",
            "2025-08-27 22:15:09",
            "2025-08-26 20:45:29"
        ],
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

    # --- Discovery consommation veille
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

    # --- Discovery sensor.linky_test
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

        # HP = somme (hpjb + hpjw + hpjr)
        hpjb = fetch_daily_differences(VM_HOST, VM_PORT, METRIC_NAMEhpjb)
        hpjw = fetch_daily_differences(VM_HOST, VM_PORT, METRIC_NAMEhpjw)
        hpjr = fetch_daily_differences(VM_HOST, VM_PORT, METRIC_NAMEhpjr)
        dailyweek_HP = [round(hpjb[i] + hpjw[i] + hpjr[i], 2) for i in range(7)]

        # HC = somme (hcjb + hcjw + hcjr)
        hcjb = fetch_daily_differences(VM_HOST, VM_PORT, METRIC_NAMEhcjb)
        hcjw = fetch_daily_differences(VM_HOST, VM_PORT, METRIC_NAMEhcjw)
        hcjr = fetch_daily_differences(VM_HOST, VM_PORT, METRIC_NAMEhcjr)
        dailyweek_HC = [round(hcjb[i] + hcjw[i] + hcjr[i], 2) for i in range(7)]

        print(f"📊 dailyweek_HP = {dailyweek_HP}")
        print(f"📊 dailyweek_HC = {dailyweek_HC}")

        # --- Publier JSON complet sensor.linky_test ---
        now = datetime.now().astimezone().isoformat()
        linky_payload = build_linky_payload_exact(dailyweek_HP, dailyweek_HC)
        linky_payload["lastUpdate"] = now
        linky_payload["timeLastCall"] = now

        result2 = client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
        result2.wait_for_publish()
        print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")

        print("\n--- Cycle terminé. Mise en veille pour 24h ---")
        time.sleep(24 * 3600)

# =======================
# Lancement script
# =======================
if __name__ == "__main__":
    main()
