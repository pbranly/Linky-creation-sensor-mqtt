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
# JSON principal (SIMPLIFIÉ)
# =======================
def build_linky_payload_exact():
    return {
        "yesterdayDate": "2025-09-01",
        "yesterday": 0,
        "serviceEnedis": "myElectricalData",
        "yesterdayLastYearDate": "2024-09-02",
        "yesterdayLastYear": 4.06,
        "daily": [0, 7.31, 4.72, 4.44, 4.36, 4.35, 4.35],
        "current_week": 29.55,
        "last_week": 38.47,
        "day_1": 0,
        "day_2": 7.31,
        "day_3": 4.72,
        "day_4": 4.44,
        "day_5": 4.36,
        "day_6": 4.35,
        "day_7": 4.35,
        "current_week_last_year": 28.54,
        "last_month": 185.13,
        "current_month": 0,
        "current_month_last_year": 4.06,
        "last_month_last_year": 168.95,
        "last_year": 286.11,
        "current_year": 1775.17,
        "current_year_last_year": 1763.92,
        "dailyweek": ["2025-09-01","2025-08-31","2025-08-30","2025-08-29","2025-08-28","2025-08-27","2025-08-26"],
        "dailyweek_cost": [0.6,1.1,0.7,0.6,0.6,0.6,0.6],
        "dailyweek_costHP": [0.4,0.9,0.5,0.4,0.4,0.4,0.4],
        "dailyweek_HP": [2.89,5.83,3.18,2.97,2.92,2.9,2.91],
        "dailyweek_costHC": [0.2,0.2,0.2,0.2,0.2,0.2,0.2],
        "dailyweek_HC": [1.36,1.46,1.55,1.47,1.43,1.44,1.43],
        "daily_cost": 0.6000000000000001,
        "yesterday_HP_cost": 0.9,
        "yesterday_HP": 2.89,
        "day_1_HP": 2898,
        "day_2_HP": 5831,
        "day_3_HP": 3187,
        "day_4_HP": 2976,
        "day_5_HP": 2920,
        "day_6_HP": 2909,
        "day_7_HP": 2916,
        "yesterday_HC_cost": 0.2,
        "yesterday_HC": 1.36,
        "day_1_HC": 1362,
        "day_2_HC": 1466,
        "day_3_HC": 1555,
        "day_4_HC": 1476,
        "day_5_HC": 1434,
        "day_6_HC": 1449,
        "day_7_HC": 1438,
        "peak_offpeak_percent": 54.84,
        "yesterdayConsumptionMaxPower": 0,
        "dailyweek_MP": [0,3.27,1.71,0.33,0.32,0.32,0.35],
        "dailyweek_MP_time": ["2025-09-01 00:00:00","2025-08-31 10:55:08","2025-08-30 09:04:30","2025-08-29 22:08:53","2025-08-28 22:03:52","2025-08-27 22:15:09","2025-08-26 20:45:29"],
        "dailyweek_MP_over": ["false","false","false","false","false","false","false"],
        "dailyweek_Tempo": ["BLUE","BLUE","BLUE","BLUE","BLUE","BLUE","BLUE"],
        "monthly_evolution": 9.58,
        "current_week_evolution": -23.18,
        "current_month_evolution": -100,
        "yesterday_evolution": -100,
        "yearly_evolution": 0.64,
        "errorLastCall": "",
        "errorLastCallInterne": "",
        "current_week_number": "36",
        "offpeak_hours_enedis": "Lundi (22H00-6H00);Mardi (22H00-6H00);Mercredi (22H00-6H00);Jeudi (22H00-6H00);Vendredi (22H00-6H00);Samedi (22H00-6H00);Dimanche (22H00-6H00);",
        "offpeak_hours": [
            [[ "22H00", "6H00" ]],
            [[ "22H00", "6H00" ]],
            [[ "22H00", "6H00" ]],
            [[ "22H00", "6H00" ]],
            [[ "22H00", "6H00" ]],
            [[ "22H00", "6H00" ]],
            [[ "22H00", "6H00" ]]
        ],
        "subscribed_power": "6 kVA",
        "version": "0.13.2",
        "activationDate": None,
        "lastUpdate": "2025-09-02 03:37:02",
        "timeLastCall": "2025-09-02 03:37:02",
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "friendly_name": "Linky 01129377636898 consumption"
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
