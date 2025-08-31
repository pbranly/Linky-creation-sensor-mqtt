import os
import requests
import paho.mqtt.client as mqtt
import time

# --- Configuration (utilisant les variables d'environnement) ---
MQTT_HOST = os.environ.get("MQTT_HOST")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
VM_HOST = os.environ.get("VM_HOST")
VM_PORT = 8428
TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
VM_QUERY_START = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1d)'
VM_QUERY_END = 'last_over_time(sensor.linky_tempo_index_bbrhpjb_value[1d] offset 1h)'

def fetch_data(query):
    """Effectue une requête à l'API de VictoriaMetrics et retourne la valeur."""
    url = f"http://{VM_HOST}:{VM_PORT}/api/v1/query"
    try:
        response = requests.get(url, params={'query': query})
        response.raise_for_status() # Lève une exception pour les erreurs HTTP
        data = response.json()
        if data['data']['result']:
            return float(data['data']['result'][0]['value'][1])
    except Exception as e:
        print(f"Erreur lors de la requête : {e}")
    return None

def on_connect(client, userdata, flags, rc):
    """Gère la connexion MQTT."""
    print(f"Connecté à MQTT avec le code de résultat {rc}")

def main():
    """Fonction principale pour récupérer, calculer et publier les données."""
    client = mqtt.Client()
    client.on_connect = on_connect
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_start()

    while True:
        try:
            start_value = fetch_data(VM_QUERY_START)
            end_value = fetch_data(VM_QUERY_END)

            if start_value is not None and end_value is not None:
                daily_consumption = round(end_value - start_value, 2)
                print(f"Consommation de la veille : {daily_consumption} kWh")
                client.publish(TOPIC, daily_consumption)
            else:
                print("Données non disponibles pour le calcul.")

        except Exception as e:
            print(f"Erreur inattendue : {e}")

        # Le script s'exécute une fois par jour à minuit
        time.sleep(24 * 3600)

if __name__ == "__main__":
    main()

