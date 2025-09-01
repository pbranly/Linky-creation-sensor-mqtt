import os
import requests
import paho.mqtt.client as mqtt
import time
import sys
import threading
import json

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

STATE_TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
DISCOVERY_TOPIC = "homeassistant/sensor/consommation_veille_linky/config"
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

    # --- Publier le message Discovery pour Home Assistant ---
    discovery_payload = {
        "name": "Consommation veille Linky",
        "state_topic": STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "icon": "mdi:flash",
        "unique_id": "linky_veille_sensor",
        "device": {
            "identifiers": ["linky_veille"],
            "name": "Compteur Linky",
            "manufacturer": "EDF",
            "model": "Linky"
        }
    }
    client.publish(DISCOVERY_TOPIC, json.dumps(discovery_payload), retain=True)
    print(f"📡 Message Discovery publié sur {DISCOVERY_TOPIC}")

    # S'abonner au topic pour recevoir les messages publiés
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

                payload = str(daily_consumption)
                result = client.publish(STATE_TOPIC, payload, qos=1, retain=MQTT_RETAIN)
                result.wait_for_publish()
                print(f"📩 Message publié sur {STATE_TOPIC}: {payload}")
            else:
                print("⚠️ Fin < début, cycle ignoré")

        print("\n--- Cycle terminé. Mise en veille pour 24 heures... ---")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    main()
