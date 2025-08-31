import os
import requests
import paho.mqtt.client as mqtt
import time
import sys

print("--- Initialisation du script de récupération de données ---")
print(f"Version Python: {sys.version}")

# --- Configuration (utilisant les variables d'environnement) ---
MQTT_HOST = os.environ.get("MQTT_HOST")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
VM_HOST = os.environ.get("VM_HOST")
VM_PORT = 8428
TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
VM_QUERY_START = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1d)'
VM_QUERY_END = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1h)'

print(f"Configuration chargée:")
print(f"  - MQTT Host: {MQTT_HOST}")
print(f"  - MQTT Port: {MQTT_PORT}")
print(f"  - VictoriaMetrics Host: {VM_HOST}")
print(f"  - Topic MQTT: {TOPIC}")

def fetch_data(query):
    """
    Effectue une requête à l'API de VictoriaMetrics et retourne la valeur.
    """
    url = f"http://{VM_HOST}:{VM_PORT}/api/v1/query"
    
    print(f"\nTentative de requête vers VictoriaMetrics...")
    print(f"  - URL: {url}")
    print(f"  - Requête PromQL: {query}")
    
    try:
        response = requests.get(url, params={'query': query})
        
        print(f"  - Code de statut HTTP: {response.status_code}")
        response.raise_for_status() # Lève une exception pour les codes d'erreur HTTP
        
        data = response.json()
        print(f"  - Réponse JSON reçue: {data}")
        
        if 'data' in data and 'result' in data['data'] and data['data']['result']:
            result_value = data['data']['result'][0]['value'][1]
            print(f"  - Valeur extraite: {result_value}")
            return float(result_value)
        else:
            print("  - Erreur: Le champ 'data.result' est vide ou absent.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Erreur de connexion/requête HTTP : {e}")
        return None
    except Exception as e:
        print(f"Erreur inattendue lors de la récupération des données : {e}")
        return None

def on_connect(client, userdata, flags, rc):
    """Callback qui gère la connexion au broker MQTT."""
    if rc == 0:
        print(f"✅ Connecté au broker MQTT sur {MQTT_HOST}:{MQTT_PORT}")
    else:
        print(f"❌ Échec de la connexion. Code de résultat : {rc}")

def main():
    """
    Fonction principale.
    - Établit la connexion MQTT.
    - Entre dans une boucle infinie pour exécuter le script une fois par jour.
    - Effectue les requêtes vers VictoriaMetrics, calcule la consommation et publie le résultat.
    """
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    
    print(f"\nTentative de connexion à MQTT sur {MQTT_HOST}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        print(f"Échec de la connexion à MQTT : {e}")
        sys.exit(1) # Quitte si la connexion MQTT échoue

    client.loop_start()

    # Boucle pour s'exécuter une fois toutes les 24 heures
    while True:
        try:
            print("\n--- Exécution du cycle quotidien ---")
            
            # Récupération de la valeur du compteur à minuit la veille
            start_value = fetch_data(VM_QUERY_START)
            
            # Récupération de la valeur du compteur à 23h59 la veille
            end_value = fetch_data(VM_QUERY_END)
            
            print(f"\nRésultats des requêtes:")
            print(f"  - Valeur de début de veille: {start_value}")
            print(f"  - Valeur de fin de veille: {end_value}")

            if start_value is not None and end_value is not None:
                if end_value >= start_value:
                    daily_consumption = round(end_value - start_value, 2)
                    print(f"Calcul de la consommation: {end_value} - {start_value} = {daily_consumption} kWh")
                    
                    print(f"Tentative de publication sur MQTT...")
                    client.publish(TOPIC, daily_consumption)
                    print(f"✅ Données publiées sur le topic : {TOPIC}")
                else:
                    print("Attention: La valeur de fin de période est inférieure à celle de début. Le calcul sera ignoré.")
            else:
                print("Données non disponibles pour le calcul. Vérifiez les requêtes et la source de données.")

        except Exception as e:
            print(f"Erreur inattendue lors de l'exécution du cycle : {e}")

        print("\n--- Cycle terminé. Mise en veille pour 24 heures... ---")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    main()
