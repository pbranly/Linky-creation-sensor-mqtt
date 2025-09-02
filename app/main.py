import os
import sys
import time
import json
import threading
import requests
from datetime import datetime, timedelta
import pytz
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

METRIC_NAMEhpjb = "sensor.linky_tempo_index_bbrhpjb_value"
METRIC_NAMEhcjb = "sensor.linky_tempo_index_bbrhcjb_value"
METRIC_NAMEhpjw = "sensor.linky_tempo_index_bbrhpjw_value"
METRIC_NAMEhcjw = "sensor.linky_tempo_index_bbrhcjw_value"
METRIC_NAMEhpjr = "sensor.linky_tempo_index_bbrhpjr_value"
METRIC_NAMEhcjr = "sensor.linky_tempo_index_bbrhcjr_value"
METRIC_NAMEpcons= "sensor.linky_puissance_consommee_value"

MQTT_RETAIN = True

# =======================
# MQTT Topics
# =======================
STATE_TOPIC = "homeassistant/sensor/consommation_veille_linky/state"
DISCOVERY_TOPIC = "homeassistant/sensor/consommation_veille_linky/config"

LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

# =======================
# VictoriaMetrics robust daily fetch (Europe/Paris day boundaries)
# =======================
def fetch_daily_for_calendar_days(vm_host, vm_port, metric_name, days=7):
    """
    Pour chaque jour i (i=0 => aujourd'hui, i=1 => hier, ...), récupère les valeurs
    entre minuit Europe/Paris du jour et minuit+1 du jour (pour aujourd'hui, end = now).
    Retourne liste [J=aujourd'hui, J-1, ..., J-(days-1)].
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    results = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, hour=0, minute=0, second=0, tzinfo=tz)
        # pour aujourd'hui, end = now ; sinon end = minuit du jour suivant
        if day == today:
            end_dt = now
        else:
            end_dt = start_dt + timedelta(days=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
        params = {
            "query": metric_name,
            "start": start_ts,
            "end": end_ts,
            "step": 60  # 1 minute : suffisant pour TIC Linky ; si tu veux moins, réduire
        }

        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            res_list = data.get("data", {}).get("result", [])
            if not res_list:
                # pas de points pour ce jour
                results.append(0.0)
                continue

            values = res_list[0].get("values", [])
            if not values:
                results.append(0.0)
                continue

            # values are [ [ts, val], ... ] sorted ascending
            first_val = float(values[0][1])
            last_val = float(values[-1][1])
            diff = last_val - first_val
            # safety: if negative due to reset/counter issue, set 0
            if diff < 0:
                diff = 0.0
            results.append(round(diff, 2))
        except Exception as e:
            print(f"❌ Erreur fetch_daily_for_calendar_days '{metric_name}' pour {day}: {e}")
            results.append(0.0)

    return results  # ordre: aujourd'hui, hier, ..., J-(days-1)


# =======================
# fetch first/last yesterday (garde la fonction si utile ailleurs)
# =======================
def fetch_first_last_yesterday(vm_host, vm_port, metric_name):
    url = f"http://{vm_host}:{vm_port}/api/v1/query"
    queries = {
        "first": f"first_over_time({metric_name}[1d] offset 1d)",
        "last": f"last_over_time({metric_name}[1d] offset 1d)"
    }
    result = {}
    for key, query in queries.items():
        try:
            r = requests.get(url, params={"query": query}, timeout=10)
            r.raise_for_status()
            data = r.json()
            res_list = data.get("data", {}).get("result", [])
            if res_list and "value" in res_list[0]:
                result[key] = float(res_list[0]["value"][1])
            else:
                result[key] = None
        except Exception as e:
            print(f"❌ Erreur VM query '{query}': {e}")
            result[key] = None

    if result.get("first") is None or result.get("last") is None:
        return None
    return round(result["last"] - result["first"], 2)


# =======================
# JSON principal (COMPLET)
# =======================
def build_linky_payload_exact(dailyweek_HP=None, dailyweek_HC=None):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()

    # dailyweek : aujourd'hui à gauche
    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    hp = dailyweek_HP if dailyweek_HP else [0.0]*7
    hc = dailyweek_HC if dailyweek_HC else [0.0]*7

    # daily = HP + HC jour par jour (même ordre : aujourd'hui, ...)
    daily = [round(hp[i] + hc[i], 2) for i in range(7)]

    payload = {
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
        "daily": daily,
        "dailyweek": dailyweek_dates,
        "dailyweek_cost": [1.2,1.3,1.1,1.4,1.3,1.2,1.3],
        "dailyweek_costHC": [0.5,0.6,0.5,0.6,0.5,0.6,0.5],
        "dailyweek_costHP": [0.7,0.7,0.6,0.8,0.8,0.6,0.8],
        "dailyweek_HC": hc,
        "daily_cost": 0.6000000000000001,
        "yesterday_HP": hp[1] if len(hp) > 1 else 0,
        "yesterday_HC": hc[1] if len(hc) > 1 else 0,
        "dailyweek_HP": hp,
        "dailyweek_MP": [7,6,8,7,6,7,7],
        "dailyweek_MP_over": [False, False, True, False, False, True, False],
        "dailyweek_MP_time": [
            "2025-09-01 00:00:00",
            "2025-08-31 10:55:08",
            "2025-08-30 09:04:30",
            "2025-08-29 22:08:53",
            "2025-08-28 22:03:52",
            "2025-08-27 22:15:09",
            "2025-08-26 20:45:29"
        ],
        "dailyweek_Tempo": ["WHITE","WHITE","RED","BLUE","WHITE","RED","BLUE"],
        "errorLastCall": "",
        "versionUpdateAvailable": False,
        "versionGit": "1.0.0",
        "peak_offpeak_percent": 45
    }
    return payload


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

    # Discovery
    discovery_payload = {
        "name": "Consommation veille Linky",
        "state_topic": STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "icon": "mdi:flash",
        "unique_id": "linky_veille_sensor",
        "device": {"identifiers": ["linky"], "name": "Compteur Linky", "manufacturer": "Enedis", "model": "Linky"}
    }
    client.publish(DISCOVERY_TOPIC, json.dumps(discovery_payload), qos=1, retain=True)

    linky_discovery_payload = {
        "name": "Linky Test",
        "state_topic": LINKY_STATE_TOPIC,
        "value_template": "{{ value_json.current_year }}",
        "json_attributes_topic": LINKY_STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "icon": "mdi:counter",
        "unique_id": "linky_test_sensor",
        "device": {"identifiers": ["linky"], "name": "Compteur Linky", "manufacturer": "Enedis", "model": "Linky"}
    }
    client.publish(LINKY_DISCOVERY_TOPIC, json.dumps(linky_discovery_payload), qos=1, retain=True)

    print("\n--- Boucle MQTT démarrée ---")
    while True:
        print("\n--- Début du cycle quotidien ---")

        # HP = somme (hpjb + hpjw + hpjr) pour chaque jour (J, J-1, ...)
        hpjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=7)
        hpjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=7)
        hpjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=7)
        dailyweek_HP = [round(hpjb[i] + hpjw[i] + hpjr[i], 2) for i in range(7)]

        # HC = somme (hcjb + hcjw + hcjr)
        hcjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=7)
        hcjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=7)
        hcjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=7)
        dailyweek_HC = [round(hcjb[i] + hcjw[i] + hcjr[i], 2) for i in range(7)]

        print(f"📊 dailyweek_HP = {dailyweek_HP}")
        print(f"📊 dailyweek_HC = {dailyweek_HC}")

        # Publier JSON complet sensor.linky_test
        now = datetime.now().astimezone().isoformat()
        linky_payload = build_linky_payload_exact(dailyweek_HP, dailyweek_HC)
        linky_payload["lastUpdate"] = now
        linky_payload["timeLastCall"] = now

        result2 = client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
        result2.wait_for_publish()
        print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")

        print("\n--- Cycle terminé. Mise en veille pour 24h ---")
        time.sleep(24 * 3600)


if __name__ == "__main__":
    main()
