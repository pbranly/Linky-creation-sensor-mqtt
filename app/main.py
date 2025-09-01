import os
import requests
import paho.mqtt.client as mqtt
import time
import sys
import threading
import json
from datetime import datetime

print("--- Début de l'exécution du script de récupération des données Linky ---")
time.sleep(5)
print(f"Version Python utilisée: {sys.version}")

# --- Configuration depuis les variables d'environnement ---
LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT") or 1883)
VM_HOST = os.getenv("VM_HOST")
VM_PORT = 8428

# --- Topics MQTT ---
STATE_TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
DISCOVERY_TOPIC = "homeassistant/sensor/consommation_veille_linky/config"

LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

VM_QUERY_START = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1d)'
VM_QUERY_END   = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1h)'

MQTT_RETAIN = True

print("\n--- Chargement de la configuration ---")
print(f"  - Hôte MQTT: {MQTT_HOST}")
print(f"  - Port MQTT: {MQTT_PORT}")
print(f"  - Hôte VictoriaMetrics: {VM_HOST}")
print("--- Configuration chargée avec succès ---")

# --- Fonction pour récupérer les données depuis VictoriaMetrics ---
def fetch_data(query):
    url = f"http://{VM_HOST}:{VM_PORT}/api/v1/query"
    print(f"\nTentative de connexion à VictoriaMetrics...")
    print(f"  - URL de la requête: {url}")
    print(f"  - Requête PromQL envoyée: '{query}'")

    try:
        response = requests.get(url, params={'query': query})
        print(f"  - Réponse HTTP reçue. Code de statut: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        print(f"  - Contenu de la réponse JSON: {data}")

        if 'data' in data and 'result' in data['data'] and data['data']['result']:
            result_list = data['data']['result'][0]
            if 'value' in result_list and len(result_list['value']) > 1:
                result_value = result_list['value'][1]
                print(f"  - Valeur extraite avec succès: '{result_value}'")
                return float(result_value)
            else:
                print("  - Erreur: Le champ 'value' est absent ou incomplet.")
                return None
        else:
            print("  - Erreur: Les champs 'data' ou 'result' sont absents ou vides.")
            return None

    except Exception as e:
        print(f"❌ Erreur récupération données: {e}")
        return None

# --- MQTT ---
def main():
    client = mqtt.Client(protocol=mqtt.MQTTv5)
    evt = threading.Event()

    def on_connect(c, u, flags, rc, props=None):
        if rc == 0:
            print("✅ Connexion MQTT réussie")
            evt.set()
        else:
            print(f"❌ Connexion MQTT échouée, code {rc}")

    def on_message(c, u, msg):
        print(f"📩 Message reçu sur {msg.topic}: {msg.payload.decode()}")

    client.on_connect = on_connect
    client.on_message = on_message

    if LOGIN and PASSWORD:
        print("Authentification MQTT activée.")
        client.username_pw_set(LOGIN, PASSWORD)

    client.loop_start()
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        print(f"❌ Échec de la connexion MQTT: {e}")
        sys.exit(1)

    if not evt.wait(timeout=10):
        print("⛔ Impossible de se connecter au broker MQTT, arrêt du script")
        sys.exit(1)

    # --- Discovery HA pour consommation veille ---
    discovery_payload = {
        "name": "Consommation veille Linky",
        "state_topic": STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "icon": "mdi:flash",
        "unique_id": "linky_veille_sensor",
        "device": {
            "identifiers": ["linky"],
            "name": "Compteur Linky",
            "manufacturer": "EDF",
            "model": "Linky"
        }
    }
    client.publish(DISCOVERY_TOPIC, json.dumps(discovery_payload), retain=True)
    print(f"📡 Message Discovery publié sur {DISCOVERY_TOPIC}")

    # --- Discovery HA pour sensor.linky_test ---
    linky_discovery_payload = {
    "name": "Linky Test",
    "state_topic": LINKY_STATE_TOPIC,
    "value_template": "{{ value_json.state }}",   # dit à HA où prendre l'état
    "json_attributes_topic": LINKY_STATE_TOPIC,  # dit à HA d'utiliser le reste du JSON comme attributs
    "unit_of_measurement": "kWh",
    "icon": "mdi:counter",
    "unique_id": "linky_test_sensor",
    "device": {
        "identifiers": ["linky"],
        "name": "Compteur Linky",
        "manufacturer": "EDF",
        "model": "Linky"
    }
}
    client.publish(LINKY_DISCOVERY_TOPIC, json.dumps(linky_discovery_payload), retain=True)
    print(f"📡 Message Discovery publié sur {LINKY_DISCOVERY_TOPIC}")

    client.subscribe(STATE_TOPIC)

    print("\n--- Boucle MQTT démarrée ---")
    while True:
        print("\n--- Début du cycle de calcul quotidien ---")
        start_value = fetch_data(VM_QUERY_START)
        end_value   = fetch_data(VM_QUERY_END)

        print(f"\n--- Résultats finaux des requêtes ---")
        print(f"  - Valeur de début de veille: {start_value}")
        print(f"  - Valeur de fin de veille: {end_value}")

        if start_value is None or end_value is None:
            print("❌ Données manquantes, cycle ignoré")
        else:
            if end_value >= start_value:
                daily_consumption = round(end_value - start_value, 2)
                print(f"✅ Consommation calculée: {daily_consumption} kWh")

                # --- Publication du sensor consommation veille ---
                payload = str(daily_consumption)
                result = client.publish(STATE_TOPIC, payload, qos=1, retain=MQTT_RETAIN)
                result.wait_for_publish()
                print(f"📩 Message publié sur {STATE_TOPIC}: {payload}")

                # --- Publication du sensor principal sensor.linky_test ---
                now = datetime.now().astimezone().isoformat()
                linky_payload = {
                    "state": str(int(end_value)),   # index actuel
                    "attributes": {
                        "typeCompteur": "consommation",
                        "unit_of_measurement": "kWh",
                        "yesterday": 15.3,
                        "day_2": 14.7,
                        "yesterday_HC": 6.8,
                        "yesterday_HP": 8.5,
                        "peak_offpeak_percent": 55,
                        "current_week": 98.6,
                        "last_week": 95.2,
                        "current_week_evolution": 3.6,
                        "current_month": 420.4,
                        "current_month_last_year": 395.7,
                        "current_month_evolution": 6.2,
                        "monthly_evolution": 2.1,
                        "current_year": 3520,
                        "current_year_last_year": 3385,
                        "yearly_evolution": 4.0,
                        "daily": [15.3,14.7,16.1,13.9,15.0,14.2,15.8],
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
                        "errorLastCall": "",
                        "versionUpdateAvailable": False,
                        "versionGit": "1.2.3",
                        "serviceEnedis": "myElectricalData",
                        "friendly_name": "Linky - Consommation"
                    },
                    "last_changed": now,
                    "last_updated": now,
                    "context": {
                        "id": "01H9ZXYZABC123DEF",
                        "parent_id": None,
                        "user_id": None
                    }
                }
                client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
                print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")
            else:
                print("⚠️ Fin < début, cycle ignoré")

        print("\n--- Cycle terminé. Mise en veille pour 24 heures... ---")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    main()
