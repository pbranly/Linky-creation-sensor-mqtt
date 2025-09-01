import os
import sys
import time
import json
import threading
from datetime import datetime, timezone
import requests
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

MQTT_RETAIN = True

# =======================
# MQTT Topics
# =======================
# Capteur "consommation de la veille" (numérique simple)
STATE_TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
DISCOVERY_TOPIC = "homeassistant/sensor/consommation_veille_linky/config"

# Capteur principal JSON "linky_test"
LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

# =======================
# Requêtes VictoriaMetrics (à adapter si besoin)
# =======================
# Exemple: index Linky calculé avec last_over_time sur des fenêtres décalées
VM_QUERY_START = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1d)'
VM_QUERY_END   = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1h)'

def fetch_data(query: str):
    """Interroge VictoriaMetrics et renvoie un float ou None."""
    url = f"http://{VM_HOST}:{VM_PORT}/api/v1/query"
    try:
        r = requests.get(url, params={'query': query}, timeout=10)
        r.raise_for_status()
        data = r.json()
        result = data.get("data", {}).get("result", [])
        if result and "value" in result[0] and len(result[0]["value"]) >= 2:
            val = float(result[0]["value"][1])
            print(f"VM OK: {query} -> {val}")
            return val
        print(f"VM vide: {query}")
        return None
    except Exception as e:
        print(f"❌ VM erreur sur '{query}': {e}")
        return None

def build_linky_payload_static():
    """Construit le JSON EXACTEMENT conforme à l’exemple fourni (valeurs en dur)."""
    return {
        "yesterdayDate": "2025-08-31",
        "yesterday": 0,
        "serviceEnedis": "myElectricalData",
        "yesterdayLastYearDate": "2024-09-01",
        "yesterdayLastYear": 4.06,
        "daily": [0, 4.72, 4.44, 4.36, 4.35, 4.35, 4.34],
        "current_week": 26.58,
        "last_week": 41.24,
        "day_1": 0,
        "day_2": 4.72,
        "day_3": 4.44,
        "day_4": 4.36,
        "day_5": 4.35,
        "day_6": 4.35,
        "day_7": 4.34,
        "current_week_last_year": 28.51,
        "last_month": 0,
        "current_month": 0,
        "current_month_last_year": 0,
        "last_month_last_year": 0,
        "last_year": 286.11,
        "current_year": 1767.85,
        "current_year_last_year": 1759.85,
        "dailyweek": [
            "2025-08-31",
            "2025-08-30",
            "2025-08-29",
            "2025-08-28",
            "2025-08-27",
            "2025-08-26",
            "2025-08-25"
        ],
        "dailyweek_cost": [1.1, 0.7, 0.6, 0.6, 0.6, 0.6, 0.6],
        "dailyweek_costHP": [0.9, 0.5, 0.4, 0.4, 0.4, 0.4, 0.4],
        "dailyweek_HP": [5.83, 3.18, 2.97, 2.92, 2.9, 2.91, 2.9],
        "dailyweek_costHC": [0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2],
        "dailyweek_HC": [1.37, 1.55, 1.47, 1.43, 1.44, 1.43, 1.44],
        "daily_cost": 1.1,
        "yesterday_HP_cost": 0.5,
        "yesterday_HP": 5.83,
        "day_1_HP": 5831,
        "day_2_HP": 3187,
        "day_3_HP": 2976,
        "day_4_HP": 2920,
        "day_5_HP": 2909,
        "day_6_HP": 2916,
        "day_7_HP": 2904,
        "yesterday_HC_cost": 0.2,
        "yesterday_HC": 1.37,
        "day_1_HC": 1378,
        "day_2_HC": 1555,
        "day_3_HC": 1476,
        "day_4_HC": 1434,
        "day_5_HC": 1449,
        "day_6_HC": 1438,
        "day_7_HC": 1449,
        "peak_offpeak_percent": 54.84,
        "yesterdayConsumptionMaxPower": 0,
        "dailyweek_MP": [0, 1.71, 0.33, 0.32, 0.32, 0.35, 0.31],
        "dailyweek_MP_time": [
            "2025-08-31 00:00:00",
            "2025-08-30 09:04:30",
            "2025-08-29 22:08:53",
            "2025-08-28 22:03:52",
            "2025-08-27 22:15:09",
            "2025-08-26 20:45:29",
            "2025-08-25 22:13:12"
        ],
        # ⚠️ volontairement des chaînes "false" (conforme à l’exemple)
        "dailyweek_MP_over": ["false", "false", "false", "false", "false", "false", "false"],
        "dailyweek_Tempo": ["BLUE", "BLUE", "BLUE", "BLUE", "BLUE", "BLUE", "BLUE"],
        "monthly_evolution": 0,
        "current_week_evolution": -35.54,
        "current_month_evolution": 0,
        "yesterday_evolution": -100,
        "yearly_evolution": 0,
        "errorLastCall": "",
        "errorLastCallInterne": "",
        "current_week_number": "35",
        "offpeak_hours_enedis": (
            "Lundi (22H00-6H00);Mardi (22H00-6H00);Mercredi (22H00-6H00);Jeudi "
            "(22H00-6H00);Vendredi (22H00-6H00);Samedi (22H00-6H00);Dimanche (22H00-6H00);"
        ),
        # Liste de 7 jours, chaque jour = [["22H00", "6H00"]]
        "offpeak_hours": [[["22H00", "6H00"]]] * 7,
        "subscribed_power": "6 kVA",
        "version": "0.13.2",
        "activationDate": None,
        "lastUpdate": "2025-09-01 15:25:54",
        "timeLastCall": "2025-09-01 15:25:54",
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "friendly_name": ""
    }

def main():
    # =======================
    # MQTT client
    # =======================
    client = mqtt.Client(protocol=mqtt.MQTTv5)
    evt = threading.Event()

    def on_connect(c, u, flags, rc, props=None):
        if rc == 0:
            print("✅ MQTT connecté")
            evt.set()
        else:
            print(f"❌ MQTT échec connexion (rc={rc})")

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
        print("⛔ Timeout connexion MQTT")
        sys.exit(1)

    # =======================
    # Discovery Home Assistant
    # =======================
    # 1) consommation veille
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

    # 2) sensor.linky_test (état = value_json.current_year, attributs = JSON complet)
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
    # Boucle principale (quotidienne)
    # =======================
    while True:
        print("\n--- Cycle de calcul ---")

        # ---- Requêtes VictoriaMetrics pour consommation veille
        start_value = fetch_data(VM_QUERY_START)
        end_value   = fetch_data(VM_QUERY_END)

        if start_value is not None and end_value is not None and end_value >= start_value:
            daily_consumption = round(end_value - start_value, 2)
            payload_daily = str(daily_consumption)
            info = client.publish(STATE_TOPIC, payload_daily, qos=1, retain=MQTT_RETAIN)
            info.wait_for_publish()
            print(f"📩 Publié {STATE_TOPIC}: {payload_daily} kWh")
        else:
            print("⚠️ Données VM insuffisantes pour consommation veille (publication ignorée)")

        # ---- Publication du JSON principal (EXACT exemple)
        linky_payload = build_linky_payload_static()
        # (optionnel) Tu pourras plus tard mettre à jour lastUpdate/timeLastCall ici avec 'now'
        # now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # linky_payload["lastUpdate"] = now_str
        # linky_payload["timeLastCall"] = now_str

        info2 = client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
        info2.wait_for_publish()
        print(f"📩 Publié {LINKY_STATE_TOPIC}: JSON (attributs conformes)")

        # ---- Attente 24h
        print("--- Fin de cycle. Dodo 24h ---")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Arrêt demandé par l'utilisateur.")
