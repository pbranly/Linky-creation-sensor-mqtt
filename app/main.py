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

PUBLISH_INTERVAL = int(os.getenv("PUBLISH_INTERVAL") or 300)

METRIC_NAMEhpjb = "sensor.linky_tempo_index_bbrhpjb_value"
METRIC_NAMEhcjb = "sensor.linky_tempo_index_bbrhcjb_value"
METRIC_NAMEhpjw = "sensor.linky_tempo_index_bbrhpjw_value"
METRIC_NAMEhcjw = "sensor.linky_tempo_index_bbrhcjw_value"
METRIC_NAMEhpjr = "sensor.linky_tempo_index_bbrhpjr_value"
METRIC_NAMEhcjr = "sensor.linky_tempo_index_bbrhcjr_value"
METRIC_NAMEpcons = "sensor.linky_puissance_consommee_value"

MQTT_RETAIN = True

# =======================
# MQTT Topics
# =======================
LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

# =======================
# FETCH DATA FROM VICTORIAMETRICS
# =======================
def fetch_daily_for_calendar_days(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    results = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, hour=0, minute=0, second=0, tzinfo=tz)
        end_dt = now if day == today else start_dt + timedelta(days=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
        params = {"query": metric_name, "start": start_ts, "end": end_ts, "step": 60}

        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            res_list = data.get("data", {}).get("result", [])
            if not res_list or not res_list[0].get("values"):
                results.append(0.0)
                continue

            values = res_list[0]["values"]
            first_val = float(values[0][1])
            last_val = float(values[-1][1])
            diff = last_val - first_val
            if diff < 0:
                diff = 0.0
            results.append(round(diff, 2))
        except Exception as e:
            print(f"❌ Erreur fetch_daily_for_calendar_days '{metric_name}' pour {day}: {e}")
            results.append(0.0)

    return results

# =======================
# CALCUL CONSO ANNUELLE
# =======================
def fetch_yearly_consumption(vm_host, vm_port, metric_name, year=None):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)

    if year is None:
        year = now.year
    start_dt = datetime(year=year, month=1, day=1, hour=0, tzinfo=tz)
    end_dt = now if year == now.year else datetime(year=year, month=12, day=31, hour=23, minute=59, tzinfo=tz)

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
    params = {"query": metric_name, "start": start_ts, "end": end_ts, "step": 3600}  # pas horaire
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        res_list = data.get("data", {}).get("result", [])
        if not res_list or not res_list[0].get("values"):
            return 0.0

        values = res_list[0]["values"]
        first_val = float(values[0][1])
        last_val = float(values[-1][1])
        diff = last_val - first_val
        if diff < 0:
            diff = 0.0
        return round(diff, 2)
    except Exception as e:
        print(f"❌ Erreur fetch_yearly_consumption '{metric_name}' pour l'année {year}: {e}")
        return 0.0

# =======================
# BUILD JSON PAYLOAD
# =======================
def build_linky_payload_exact(dailyweek_HP=None, dailyweek_HC=None,
                              dailyweek_MP=None, dailyweek_MP_time=None,
                              dailyweek_Tempo=None,
                              current_week=0.0, last_week=0.0, current_week_evolution=0.0,
                              current_year=0.0, current_year_last_year=0.0, yearly_evolution=0.0):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()

    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    hp = dailyweek_HP if dailyweek_HP else [0.0]*7
    hc = dailyweek_HC if dailyweek_HC else [0.0]*7
    mp = dailyweek_MP if dailyweek_MP else [0]*7
    mp_time = dailyweek_MP_time if dailyweek_MP_time else [today.strftime("%Y-%m-%d 00:00:00")]*7
    tempo = dailyweek_Tempo if dailyweek_Tempo else ["UNKNOWN"]*7
    daily = [round(hp[i] + hc[i], 2) for i in range(7)]

    payload = {
        "serviceEnedis": "myElectricalData",
        "typeCompteur": "consommation",
        "unit_of_measurement": "kWh",
        "current_year": current_year,
        "current_year_last_year": current_year_last_year,
        "yearly_evolution": yearly_evolution,
        "last_month": 1200,
        "last_month_last_year": 1100,
        "monthly_evolution": 9.1,
        "current_month": 1300,
        "current_month_last_year": 1250,
        "current_month_evolution": 4,
        "current_week": current_week,
        "last_week": last_week,
        "current_week_evolution": current_week_evolution,
        "yesterday": 45,
        "day_2": 50,
        "yesterday_evolution": -10,
        "daily": daily,
        "dailyweek": dailyweek_dates,
        "dailyweek_cost": [1.2,1.3,1.1,1.4,1.3,1.2,1.3],
        "dailyweek_costHC": [0.5,0.6,0.5,0.6,0.5,0.6,0.5],
        "dailyweek_costHP": [0.7,0.7,0.6,0.8,0.8,0.6,0.8],
        "dailyweek_HC": hc,
        "daily_cost": 0.6,
        "yesterday_HP": hp[1] if len(hp) > 1 else 0,
        "yesterday_HC": hc[1] if len(hc) > 1 else 0,
        "dailyweek_HP": hp,
        "dailyweek_MP": mp,
        "dailyweek_MP_over": [val > 7 for val in mp],
        "dailyweek_MP_time": mp_time,
        "dailyweek_Tempo": tempo,
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

    # Discovery Linky Test
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
    current_day = datetime.now(pytz.timezone("Europe/Paris")).date()

    while True:
        now_dt = datetime.now(pytz.timezone("Europe/Paris"))
        today = now_dt.date()

        if today != current_day:
            print("🔄 Changement de jour détecté, rafraîchissement complet")
            current_day = today

        # HP / HC
        hpjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=7)
        hpjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=7)
        hpjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=7)
        dailyweek_HP = [round(hpjb[i] + hpjw[i] + hpjr[i], 2) for i in range(7)]

        hcjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=7)
        hcjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=7)
        hcjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=7)
        dailyweek_HC = [round(hcjb[i] + hcjw[i] + hcjr[i], 2) for i in range(7)]

        # =======================
        # Calcul semaine
        # =======================
        current_week = round(sum(dailyweek_HP[:7] + dailyweek_HC[:7]), 2)
        last_week = round(sum(dailyweek_HP[1:8] + dailyweek_HC[1:8]), 2) if len(dailyweek_HP) > 7 else 0.0
        current_week_evolution = round((current_week - last_week) / last_week * 100, 2) if last_week > 0 else 0.0

        # Puissance max
        dailyweek_MP, dailyweek_MP_time = fetch_daily_max_power(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=7)

        # Couleur tempo
        dailyweek_Tempo = fetch_daily_tempo_colors(VM_HOST, VM_PORT, days=7)

        # =======================
        # Calcul annuel
        # =======================
        current_year = fetch_yearly_consumption(VM_HOST, VM_PORT, METRIC_NAMEpcons, year=today.year)
        current_year_last_year = fetch_yearly_consumption(VM_HOST, VM_PORT, METRIC_NAMEpcons, year=today.year-1)
        yearly_evolution = round((current_year - current_year_last_year) / current_year_last_year * 100, 2) if current_year_last_year > 0 else 0.0

        # JSON
        linky_payload = build_linky_payload_exact(
            dailyweek_HP, dailyweek_HC, dailyweek_MP, dailyweek_MP_time, dailyweek_Tempo,
            current_week=current_week, last_week=last_week, current_week_evolution=current_week_evolution,
            current_year=current_year, current_year_last_year=current_year_last_year, yearly_evolution=yearly_evolution
        )
        now_iso = now_dt.isoformat()
        linky_payload["lastUpdate"] = now_iso
        linky_payload["timeLastCall"] = now_iso

        # === LOG DES VARIABLES CALCULEES ===
        print("\n📑 Variables calculées pour ce cycle:")
        print(f"  Dates:          {linky_payload['dailyweek']}")
        print(f"  HP (7j):        {dailyweek_HP}")
        print(f"  HC (7j):        {dailyweek_HC}")
        print(f"  Daily (HP+HC):  {linky_payload['daily']}")
        print(f"  MP (7j):        {dailyweek_MP}")
        print(f"  MP times:       {dailyweek_MP_time}")
        print(f"  Tempo couleurs: {dailyweek_Tempo}")
        print(f"  Yesterday HP:   {linky_payload['yesterday_HP']}")
        print(f"  Yesterday HC:   {linky_payload['yesterday_HC']}")
        print(f"  Current week:   {current_week} kWh")
        print(f"  Last week:      {last_week} kWh")
        print(f"  Current week évolution: {current_week_evolution}%")
        print(f"  Current year:   {current_year} kWh")
        print(f"  Last year:      {current_year_last_year} kWh")
        print(f"  Yearly evolution: {yearly_evolution}%")
        print(f"  Last update:    {linky_payload['lastUpdate']}")

        # Publication
        result = client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
        result.wait_for_publish()
        print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")

        # Pause
        time.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    main()
