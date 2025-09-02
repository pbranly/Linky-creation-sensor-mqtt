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
# JSON principal (EXEMPLE fixe)
# =======================
def build_linky_payload_static():
    """Construit le JSON EXACTEMENT conforme à l’exemple fourni (valeurs en dur)."""
    return {
    "typeCompteur": "consommation",
    "unit_of_measurement": "kWh",

    "yesterday": 15.3,
    "day_2": 14.7,
    "yesterday_HC": 6.8,
    "yesterday_HP": 8.5,
    "peak_offpeak_percent": 55,

    "current_week": 98.6,
    "last_week": 95.2,

    "current_month": 420.4,
    "last_month": 410.0,
    "current_month_last_year": 395.7,
    "last_month_last_year": 385.2,

    "current_year": 3520,
    "current_year_last_year": 3385,

    "daily": [15.3, 14.7, 16.1, 13.9, 15.0, 14.2, 15.8],
    "dailyweek": "2025-08-26,2025-08-27,2025-08-28,2025-08-29,2025-08-30,2025-08-31,2025-09-01",

    "dailyweek_cost": "2.73,2.62,2.85,2.48,2.70,2.55,2.82",
    "dailyweek_costHC": "1.05,0.98,1.12,0.95,1.01,0.97,1.10",
    "dailyweek_costHP": "1.68,1.64,1.73,1.53,1.69,1.58,1.72",

    "dailyweek_HC": "6.8,6.5,7.0,6.3,6.7,6.4,6.9",
    "dailyweek_HP": "8.5,8.2,9.1,7.6,8.3,7.8,8.9",

    "dailyweek_MP": "6.0,6.2,5.8,6.5,6.1,6.3,6.4",
    "dailyweek_MP_over": "false,false,false,true,false,false,false",
    "dailyweek_MP_time": "2025-08-26T19:30:00,2025-08-27T19:15:00,2025-08-28T19:10:00,2025-08-29T19:00:00,2025-08-30T18:50:00,2025-08-31T19:40:00,2025-09-01T19:20:00",

    "dailyweek_Tempo": "BLUE,BLUE,WHITE,WHITE,RED,BLUE,WHITE",

    "serviceEnedis": "myElectricalData",

    "friendly_name": "Linky - Consommation"
  
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
        linky_payload = build_linky_payload_static()
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
