import os
import sys
import time
import json
import threading
import requests
from datetime import datetime
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

METRIC_NAME = "sensor.linky_tempo_index_bbrhpjb_value"

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
def fetch_first_last_yesterday(vm_host, vm_port, metric_name):
    """
    Calcule la consommation de la veille comme :
    dernière valeur de la veille - première valeur de la veille
    """
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
# JSON principal (SIMPLIFIÉ ET EXACT)
# =======================
def build_linky_payload_exact():
    return {
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
  "daily": [5.1, 4.9, 5.3, 5.0, 5.2, 4.8, 5.0],
  "dailyweek": ["2025-09-01","2025-09-02","2025-09-03","2025-09-04","2025-09-05","2025-09-06","2025-09-07"],
  "dailyweek_cost": [1.2,1.3,1.1,1.4,1.3,1.2,1.3],
  "dailyweek_costHC": [0.5,0.6,0.5,0.6,0.5,0.6,0.5],
  "dailyweek_costHP": [0.7,0.7,0.6,0.8,0.8,0.6,0.8],
  "dailyweek_HC": [2.0,2.1,1.9,2.2,2.0,2.1,2.0],
  "dailyweek_HP": [3.1,3.0,3.2,3.1,3.0,3.1,3.0],
  "dailyweek_MP": [7,6,8,7,6,7,7],
  "dailyweek_MP_over": [False, False, True, False, False, True, False],
  "dailyweek_MP_time": ["08:15","10:30","12:45","14:00","16:15","18:30","20:45"],
  "dailyweek_Tempo": ["WHITE","WHITE","RED","BLUE","WHITE","RED","BLUE"],
  "errorLastCall": "",
  "versionUpdateAvailable": false,
  "versionGit": "1.0.0",
  "peak_offpeak_percent": 45
}

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
        consommation_veille = fetch_first_last_yesterday(VM_HOST, VM_PORT, METRIC_NAME)

        if consommation_veille is None:
            print("❌ Données VM manquantes, cycle ignoré")
        else:
            # --- Publier consommation veille ---
            result = client.publish(STATE_TOPIC, str(consommation_veille), qos=1, retain=MQTT_RETAIN)
            result.wait_for_publish()
            print(f"📩 Consommation veille publiée: {consommation_veille} kWh sur {STATE_TOPIC}")

        # --- Publier JSON complet sensor.linky_test ---
        now = datetime.now().astimezone().isoformat()
        linky_payload = build_linky_payload_exact()
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
