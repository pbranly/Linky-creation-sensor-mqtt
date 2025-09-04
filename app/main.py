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
# Fonctions pour fetch quotidien depuis VictoriaMetrics
# =======================
def fetch_daily_for_calendar_days(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    results = []
    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
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
            if not res_list:
                results.append(0.0)
                continue
            values = res_list[0].get("values", [])
            if not values:
                results.append(0.0)
                continue
            diff = float(values[-1][1]) - float(values[0][1])
            if diff < 0:
                diff = 0.0
            results.append(round(diff, 2))
        except Exception as e:
            print(f"❌ Erreur fetch_daily_for_calendar_days '{metric_name}' pour {day}: {e}")
            results.append(0.0)
    return results

def fetch_daily_max_power(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    max_values, max_times = [], []
    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(day.year, day.month, day.day, tzinfo=tz)
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
                max_values.append(0)
                max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))
                continue
            values = res_list[0]["values"]
            max_val, max_ts = -1, start_ts
            for ts, val in values:
                v = float(val)/1000.0
                if v > max_val:
                    max_val = v
                    max_ts = int(ts)
            max_values.append(round(max_val, 2))
            max_times.append(datetime.fromtimestamp(max_ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            print(f"❌ Erreur fetch_daily_max_power '{metric_name}' pour {day}: {e}")
            max_values.append(0)
            max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))
    return max_values, max_times

def fetch_daily_tempo_colors(vm_host, vm_port, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    colors = []
    tempo_metrics = {
        "BLUE": [METRIC_NAMEhpjb, METRIC_NAMEhcjb],
        "WHITE": [METRIC_NAMEhpjw, METRIC_NAMEhcjw],
        "RED": [METRIC_NAMEhpjr, METRIC_NAMEhcjr],
    }
    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(day.year, day.month, day.day, 0, tzinfo=tz)
        end_dt = start_dt + timedelta(days=1)
        detected_color = "UNKNOWN"
        for color, metrics in tempo_metrics.items():
            for metric in metrics:
                url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
                params = {"query": metric, "start": int(start_dt.timestamp()), "end": int(end_dt.timestamp()), "step": 300}
                try:
                    r = requests.get(url, params=params, timeout=10)
                    r.raise_for_status()
                    data = r.json()
                    res_list = data.get("data", {}).get("result", [])
                    if res_list and res_list[0].get("values"):
                        values = res_list[0]["values"]
                        for _, v in values:
                            if float(v) > 0:
                                detected_color = color
                                break
                        if detected_color != "UNKNOWN":
                            break
                except Exception as e:
                    print(f"⚠️ Erreur fetch_daily_tempo_colors pour {day} ({metric}): {e}")
            if detected_color != "UNKNOWN":
                break
        colors.append(detected_color)
    return colors

def compute_weekly_consumption(daily_14):
    current_week = sum(daily_14[:7])
    last_week = sum(daily_14[7:14])
    current_week_evolution = ((current_week - last_week)/last_week*100) if last_week else 0.0
    return current_week, last_week, round(current_week_evolution,2)

# =======================
# AJOUT: consommation annuelle
# =======================
def fetch_yearly_consumption(vm_host, vm_port):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    start_current = datetime(today.year,1,1,0,0,0,tzinfo=tz)
    start_last = datetime(today.year-1,1,1,0,0,0,tzinfo=tz)
    end_last = start_last + (today - start_current + timedelta(days=1))
    metric_names = [METRIC_NAMEhpjb,METRIC_NAMEhpjw,METRIC_NAMEhpjr,
                    METRIC_NAMEhcjb,METRIC_NAMEhcjw,METRIC_NAMEhcjr]

    def fetch_total(start,end):
        total=0.0
        for metric in metric_names:
            url=f"http://{vm_host}:{vm_port}/api/v1/query_range"
            params={"query":metric,"start":int(start.timestamp()),"end":int(end.timestamp()),"step":60}
            try:
                r=requests.get(url,params=params,timeout=30)
                r.raise_for_status()
                data=r.json()
                res_list=data.get("data",{}).get("result",[])
                if res_list and res_list[0].get("values"):
                    values=res_list[0]["values"]
                    total+=max(0.0,float(values[-1][1])-float(values[0][1]))
            except Exception as e:
                print(f"❌ Erreur fetch_yearly_consumption pour {metric}: {e}")
        return round(total,2)

    current_year=fetch_total(start_current, now)
    last_year=fetch_total(start_last, end_last)
    evolution=( (current_year-last_year)/last_year*100 if last_year else 0.0 )
    return current_year,last_year,round(evolution,2)

# =======================
# JSON complet
# =======================
def build_linky_payload_exact(dailyweek_HP=None, dailyweek_HC=None,
                              dailyweek_MP=None, dailyweek_MP_time=None,
                              dailyweek_Tempo=None, current_week=0, last_week=0, current_week_evolution=0,
                              current_year=0, current_year_last_year=0, yearly_evolution=0):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()
    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    hp = dailyweek_HP if dailyweek_HP else [0.0]*7
    hc = dailyweek_HC if dailyweek_HC else [0.0]*7
    mp = dailyweek_MP if dailyweek_MP else [0]*7
    mp_time = dailyweek_MP_time if dailyweek_MP_time else [today.strftime("%Y-%m-%d 00:00:00")]*7
    tempo = dailyweek_Tempo if dailyweek_Tempo else ["UNKNOWN"]*7
    daily = [round(hp[i]+hc[i],2) for i in range(7)]
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
        "yesterday_HP": hp[1] if len(hp)>1 else 0,
        "yesterday_HC": hc[1] if len(hc)>1 else 0,
        "dailyweek_HP": hp,
        "dailyweek_MP": mp,
        "dailyweek_MP_over": [val>7 for val in mp],
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
    def on_connect(c,u,flags,rc,props=None):
        if rc==0:
            print("✅ MQTT connecté")
            evt.set()
        else:
            print(f"❌ MQTT échec (rc={rc})")
    client.on_connect = on_connect
    if LOGIN and PASSWORD:
        client.username_pw_set(LOGIN,PASSWORD)
    client.loop_start()
    try:
        client.connect(MQTT_HOST,MQTT_PORT,60)
    except Exception as e:
        print(f"❌ Connexion MQTT impossible: {e}")
        sys.exit(1)
    if not evt.wait(timeout=10):
        print("⛔ Timeout MQTT")
        sys.exit(1)

    # Discovery Linky Test
    linky_discovery_payload={
        "name":"Linky Test",
        "state_topic":LINKY_STATE_TOPIC,
        "value_template":"{{ value_json.current_year }}",
        "json_attributes_topic":LINKY_STATE_TOPIC,
        "unit_of_measurement":"kWh",
        "device_class":"energy",
        "icon":"mdi:counter",
        "unique_id":"linky_test_sensor",
        "device":{"identifiers":["linky"],"name":"Compteur Linky","manufacturer":"Enedis","model":"Linky"}
    }
    client.publish(LINKY_DISCOVERY_TOPIC,json.dumps(linky_discovery_payload),qos=1,retain=True)

    print("\n--- Boucle MQTT démarrée ---")
    current_day=datetime.now(pytz.timezone("Europe/Paris")).date()

    while True:
        now_dt=datetime.now(pytz.timezone("Europe/Paris"))
        today=now_dt.date()
        if today!=current_day:
            print("🔄 Changement de jour détecté, rafraîchissement complet")
            current_day=today

        # HP/HC pour 14 derniers jours
        hpjb_14=fetch_daily_for_calendar_days(VM_HOST,VM_PORT,METRIC_NAMEhpjb,days=14)
        hpjw_14=fetch_daily_for_calendar_days(VM_HOST,VM_PORT,METRIC_NAMEhpjw,days=14)
        hpjr_14=fetch_daily_for_calendar_days(VM_HOST,VM_PORT,METRIC_NAMEhpjr,days=14)
        hcjb_14=fetch_daily_for_calendar_days(VM_HOST,VM_PORT,METRIC_NAMEhcjb,days=14)
        hcjw_14=fetch_daily_for_calendar_days(VM_HOST,VM_PORT,METRIC_NAMEhcjw,days=14)
        hcjr_14=fetch_daily_for_calendar_days(VM_HOST,VM_PORT,METRIC_NAMEhcjr,days=14)

        daily_14=[round(hpjb_14[i]+hpjw_14[i]+hpjr_14[i]+hcjb_14[i]+hcjw_14[i]+hcjr_14[i],2) for i in range(14)]
        current_week,last_week,current_week_evolution=compute_weekly_consumption(daily_14)
        dailyweek_HP=[round(hpjb_14[i]+hpjw_14[i]+hpjr_14[i],2) for i in range(7)]
        dailyweek_HC=[round(hcjb_14[i]+hcjw_14[i]+hcjr_14[i],2) for i in range(7)]
        dailyweek_MP,dailyweek_MP_time=fetch_daily_max_power(VM_HOST,VM_PORT,METRIC_NAMEpcons,days=7)
        dailyweek_Tempo=fetch_daily_tempo_colors(VM_HOST,VM_PORT,days=7)

        # === AJOUT: consommation annuelle ===
        current_year_val,last_year_val,yearly_evol=fetch_yearly_consumption(VM_HOST,VM_PORT)

        # JSON
        linky_payload=build_linky_payload_exact(
            dailyweek_HP,dailyweek_HC,dailyweek_MP,dailyweek_MP_time,dailyweek_Tempo,
            current_week,last_week,current_week_evolution,
            current_year_val,last_year_val,yearly_evol
        )
        now_iso=now_dt.isoformat()
        linky_payload["lastUpdate"]=now_iso
        linky_payload["timeLastCall"]=now_iso

        # LOG
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
        print(f"  Current week:   {linky_payload['current_week']} kWh")
        print(f"  Last week:      {linky_payload['last_week']} kWh")
        print(f"  Week evolution: {linky_payload['current_week_evolution']}%")
        print(f"  Current year:   {linky_payload['current_year']}")
        print(f"  Last year:      {linky_payload['current_year_last_year']}")
        print(f"  Year evolution: {linky_payload['yearly_evolution']}%")
        print(f"  Last update:    {linky_payload['lastUpdate']}")

        # Publication MQTT
        result=client.publish(LINKY_STATE_TOPIC,json.dumps(linky_payload),qos=1,retain=MQTT_RETAIN)
        result.wait_for_publish()
        print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")

        time.sleep(PUBLISH_INTERVAL)

if __name__=="__main__":
    main()
