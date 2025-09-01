import os
import requests
import paho.mqtt.client as mqtt
import time
import sys

print("--- Début de l'exécution du script de récupération des données Linky ---")
time.sleep(5)
print(f"Version Python utilisée: {sys.version}")

LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT") or 1883)
VM_HOST = os.getenv("VM_HOST")
VM_PORT = 8428
TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
VM_QUERY_START = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1d)'
VM_QUERY_END = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1h)'

print("\n--- Chargement de la configuration ---")
print(f"  - Hôte MQTT: {MQTT_HOST}")
print(f"  - Port MQTT: {MQTT_PORT}")
print(f"  - Hôte VictoriaMetrics: {VM_HOST}")
print(f"  - Topic de publication MQTT: {TOPIC}")
print(f"  - Login MQTT: {'Défini' if LOGIN else 'Non défini'}")
print("--- Configuration chargée avec succès ---")

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
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur de connexion/requête HTTP: {e}")
        return None
    except Exception as e:
        print(f"❌ Erreur inattendue lors de la récupération des données: {e}")
        return None

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Connexion au broker MQTT réussie (Code {rc})")
    else:
        print(f"❌ Échec de la connexion au broker MQTT. Code de résultat: {rc}")

def main():
    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.on_connect = on_connect

    if LOGIN and PASSWORD:
        print("Authentification MQTT activée.")
        client.username_pw_set(LOGIN, PASSWORD)

    print("\n--- Démarrage de la boucle de gestion des événements MQTT ---")
    client.loop_start()

    print(f"Tentative de connexion à MQTT sur {MQTT_HOST}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        print(f"❌ Échec critique de la connexion à MQTT: {e}")
        sys.exit(1)

    while True:
        print("\n--- Début du cycle de calcul quotidien ---")
        
        try:
            print("Étape 1: Récupération de la valeur de début de veille...")
            start_value = fetch_data(VM_QUERY_START)
            
            print("\nÉtape 2: Récupération de la valeur de fin de veille...")
            end_value = fetch_data(VM_QUERY_END)
            
            print(f"\n--- Résultats finaux des requêtes ---")
            print(f"  - Valeur de début de veille: {start_value}")
            print(f"  - Valeur de fin de veille: {end_value}")

            if start_value is not None and end_value is not None:
                if end_value >= start_value:
                    daily_consumption = round(end_value - start_value, 2)
                    print(f"✅ Calcul de la consommation: {end_value} - {start_value} = {daily_consumption} kWh")
                    
                    print(f"\nÉtape 3: Publication sur MQTT...")
                    # Affiche la charge utile exacte
                    print(f"  - Publication de la charge utile: '{daily_consumption}' sur le topic '{TOPIC}'")
                    result = client.publish(TOPIC, daily_consumption, qos=1, retain=True)
                    print(f"  - Résultat de la publication: Code de retour = {result.rc} (0 = succès)")
                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                        print(f"✅ Données publiées avec succès sur le topic : {TOPIC}")
                    else:
                        print(f"❌ Échec de la publication. Le message n'a pas été mis en file d'attente.")
                else:
                    print("⚠️ Attention: La valeur de fin est inférieure à celle de début. Le calcul est ignoré.")
            else:
                print("❌ Impossible de calculer la consommation. Données manquantes.")

        except Exception as e:
            print(f"❌ Erreur inattendue lors de l'exécution du cycle: {e}")

        print("\n--- Cycle terminé. Mise en veille pour 24 heures... ---")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    main()
