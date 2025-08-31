import os
import requests
import paho.mqtt.client as mqtt
import time

# --- Configuration (utilisant les variables d'environnement) ---
# Ces variables sont définies dans le fichier docker-compose.yml
MQTT_HOST = os.environ.get("MQTT_HOST")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
VM_HOST = os.environ.get("VM_HOST")
VM_PORT = 8428
TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
VM_QUERY_START = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1d)'
VM_QUERY_END = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1h)'

def fetch_data(query):
    """
    Effectue une requête à l'API de VictoriaMetrics pour une requête PromQL donnée.
    Retourne la valeur extraite si la requête réussit, sinon None.
    """
    url = f"http://{VM_HOST}:{VM_PORT}/api/v1/query"
    try:
        response = requests.get(url, params={'query': query})
        response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP
        data = response.json()
        # Vérifie si la requête a retourné un résultat valide
        if data['data']['result']:
            # Retourne la valeur numérique, qui se trouve dans le second élément de l'array
            return float(data['data']['result'][0]['value'][1])
    except Exception as e:
        print(f"Erreur lors de la requête vers VictoriaMetrics : {e}")
    return None

def on_connect(client, userdata, flags, rc):
    """Callback qui gère la connexion au broker MQTT."""
    print(f"Connecté à MQTT avec le code de résultat {rc}")

def main():
    """
    Fonction principale.
    - Établit la connexion MQTT.
    - Entre dans une boucle infinie pour exécuter le script une fois par jour.
    - Effectue les requêtes vers VictoriaMetrics, calcule la consommation et publie le résultat.
    """
    # 💡 Ligne corrigée : Utilisation du protocole MQTTv5
    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        print(f"Échec de la connexion à MQTT : {e}")
        return

    client.loop_start()

    # Boucle pour s'exécuter une fois toutes les 24 heures
    while True:
        try:
            print("--- Exécution du script de calcul de la consommation de la veille ---")
            
            # Récupération de la valeur du compteur à minuit la veille
            start_value = fetch_data(VM_QUERY_START)
            
            # Récupération de la valeur du compteur à 23h59 la veille
            end_value = fetch_data(VM_QUERY_END)

            if start_value is not None and end_value is not None:
                # Calcul de la différence et arrondi à 2 décimales
                daily_consumption = round(end_value - start_value, 2)
                print(f"Consommation calculée : {daily_consumption} kWh")
                
                # Publication du résultat sur le topic MQTT
                client.publish(TOPIC, daily_consumption)
            else:
                print("Données non disponibles pour le calcul. Vérifiez les requêtes et la source de données.")

        except Exception as e:
            print(f"Erreur inattendue : {e}")

        # Le script se met en veille pour 24 heures avant la prochaine exécution
        print("Mise en veille pour 24 heures...")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    main()
