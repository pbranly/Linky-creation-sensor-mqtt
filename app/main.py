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
METRIC_NAMEpcons = "sensor.linky_puissance_consommee_value"

MQTT_RETAIN = True

# =======================
# MQTT Topics
# =======================
LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

# =======================
# VictoriaMetrics robust daily fetch (Europe/Paris day boundaries)
# =======================
def fetch_daily_for_calendar_days(vm_host, vm_port, metric_name, days=7):
    """
    Retourne une liste de 'days' consommations journalières calculées par
    différentiel d'index sur chaque journée civile Europe/Paris :
    ordre = [J0 (aujourd’hui, partiel), J-1, ..., J-6]
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    results = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, hour=0, minute=0, second=0, tzinfo=tz)
        if day == today:
            end_dt = now  # aujourd’hui = partiel
        else:
            end_dt = start_dt + timedelta(days=1)  # journée complète

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

            # Différentiel index
            first_val = float(values[0][1])
            last_val = float(values[-1][1])
            diff = last_val - first_val
            if diff < 0:
                diff = 0.0
            results.append(round(diff, 2))
        except Exception as e:
            print(f"❌ Erreur fetch_daily_for_calendar_days '{metric_name}' pour {day}: {e}")
            results.append(0.0)

    return results  # [J0, J-1, ..., J-6]

# =======================
# Puissance max par jour -> MP (kVA) + heure
# =======================
def fetch_daily_max_power(vm_host, vm_port, metric_name, days=7):
    """
    Retourne (mp_kva, mp_time) sur 7 jours Europe/Paris :
    - mp_kva : liste des puissances max en kVA (valeurs du metric / 1000)
    - mp_time : liste des timestamps '%Y-%m-%d %H:%M:%S' Europe/Paris
    Ordre: [J0 (partiel), J-1, ..., J-6]
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()

    max_values = []
    max_times = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, hour=0, tzinfo=tz)
        if day == today:
            end_dt = now
        else:
            end_dt = start_dt + timedelta(days=1)

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
                max_values.append(0.0)
                max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))
                continue

            values = res_list[0]["values"]

            max_val_va = -1.0
            max_ts = start_ts
            for ts, val in values:
                try:
                    v = float(val)
                except:
                    v = 0.0
                if v > max_val_va:
                    max_val_va = v
                    max_ts = int(ts)

            # Conversion VA -> kVA
            max_val_kva = round(max_val_va / 1000.0, 2)
            max_values.append(max_val_kva)
            max_times.append(datetime.fromtimestamp(max_ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S"))

        except Exception as e:
            print(f"❌ Erreur fetch_daily_max_power '{metric_name}' pour {day}: {e}")
            max_values.append(0.0)
            max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))

    return max_values, max_times  # [J0..J-6], [J0..J-6]

# =======================
# Détection couleur Tempo via présence de points entre 08:00 et 09:00
# =======================
def fetch_daily_tempo_colors(vm_host, vm_port, days=7):
    """
    Déduit BLUE/WHITE/RED selon la présence de mesures (index HP/HC de la couleur)
    entre 08:00 et 09:00 Europe/Paris. Ordre [J0..J-6].
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    colors = []

    tempo_metrics = {
        "BLUE": ["sensor.linky_tempo_index_bbrhpjb_value", "sensor.linky_tempo_index_bbrhcjb_value"],
        "WHITE": ["sensor.linky_tempo_index_bbrhpjw_value", "sensor.linky_tempo_index_bbrhcjw_value"],
        "RED": ["sensor.linky_tempo_index_bbrhpjr_value", "sensor.linky_tempo_index_bbrhcjr_value"],
    }

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, hour=8, tzinfo=tz)
        end_dt = start_dt + timedelta(hours=1)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        detected_color = "UNKNOWN"
        for color, metrics in tempo_metrics.items():
            found = False
            for metric in metrics:
                url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
                params = {"query": metric, "start": start_ts, "end": end_ts, "step": 300}
                try:
                    r = requests.get(url, params=params, timeout=10)
                    r.raise_for_status()
                    data = r.json()
                    res_list = data.get("data", {}).get("result", [])
                    if res_list and res_list[0].get("values"):
                        detected_color = color
                        found = True
                        break
                except Exception as e:
                    print(f"⚠️ Erreur fetch_daily_tempo_colors pour {day} ({metric}): {e}")
            if found:
                break

        colors.append(detected_color)

    return colors  # [J0..J-6]

# =======================
# Helpers
# =======================
def make_dailyweek_dates(days=7):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()
    # ordre : [J0, J-1, ..., J-6]
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

def build_linky_payload_exact(dailyweek_HP=None, dailyweek_HC=None,
                              dailyweek_MP=None, dailyweek_MP_time=None,
                              dailyweek_Tempo=None):
    """
    Construit le JSON complet attendu par la carte (aucun champ supprimé).
    Les listes sont ordonnées [J0, J-1, ..., J-6] avec J0 à gauche.
    """
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()

    dailyweek_dates = make_dailyweek_dates(7)

    hp = dailyweek_HP if dailyweek_HP else [0.0]*7
    hc = dailyweek_HC if dailyweek_HC else [0.0]*7
    mp = dailyweek_MP if dailyweek_MP else [0.0]*7
    mp_time = dailyweek_MP_time if dailyweek_MP_time else [today.strftime("%Y-%m-%d 00:00:00")]*7
    tempo = dailyweek_Tempo if dailyweek_Tempo else ["UNKNOWN"]*7

    # Somme HP+HC jour par jour (J0..J-6)
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
        "daily_cost": 0.6,
        "yesterday_HP": hp[1] if len(hp) > 1 else 0,
        "yesterday_HC": hc[1] if len(hc) > 1 else 0,
        "dailyweek_HP": hp,
        "dailyweek_MP": mp,
        # Seuil 7 kVA
        "dailyweek_MP_over": [bool(val > 7.0) for val in mp],
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

    # --- Discovery sensor.linky_test (on garde tout)
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
    # Cache pour mises à jour 5 minutes
    # =======================
    cached_hp = [0.0]*7
    cached_hc = [0.0]*7
    cached_mp = [0.0]*7
    cached_mptime = [datetime.now().strftime("%Y-%m-%d 00:00:00")]*7
    cached_tempo = ["UNKNOWN"]*7
    cached_dates = make_dailyweek_dates(7)
    last_full_recalc_day = None  # 'YYYY-MM-DD' Europe/Paris

    def do_full_recalc_and_publish():
        nonlocal cached_hp, cached_hc, cached_mp, cached_mptime, cached_tempo, cached_dates, last_full_recalc_day

        # HP = somme (hpjb + hpjw + hpjr)
        hpjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=7)
        hpjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=7)
        hpjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=7)
        cached_hp = [round(hpjb[i] + hpjw[i] + hpjr[i], 2) for i in range(7)]

        # HC = somme (hcjb + hcjw + hcjr)
        hcjb = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=7)
        hcjw = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=7)
        hcjr = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=7)
        cached_hc = [round(hcjb[i] + hcjw[i] + hcjr[i], 2) for i in range(7)]

        print(f"📊 dailyweek_HP = {cached_hp}")
        print(f"📊 dailyweek_HC = {cached_hc}")

        # MP (kVA) + heure
        cached_mp, cached_mptime = fetch_daily_max_power(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=7)
        print(f"⚡ dailyweek_MP (kVA) = {cached_mp}")
        print(f"⏰ dailyweek_MP_time = {cached_mptime}")

        # Couleurs Tempo
        cached_tempo = fetch_daily_tempo_colors(VM_HOST, VM_PORT, days=7)
        print(f"🎨 dailyweek_Tempo = {cached_tempo}")

        # Dates
        cached_dates = make_dailyweek_dates(7)

        # Construction + publish
        now = datetime.now().astimezone().isoformat()
        payload = build_linky_payload_exact(
            cached_hp, cached_hc, cached_mp, cached_mptime, cached_tempo
        )
        payload["lastUpdate"] = now
        payload["timeLastCall"] = now
        # forcer les dates dynamiques rafraîchies
        payload["dailyweek"] = cached_dates

        result2 = client.publish(LINKY_STATE_TOPIC, json.dumps(payload), qos=1, retain=MQTT_RETAIN)
        result2.wait_for_publish()
        print(f"📡 JSON COMPLET publié sur {LINKY_STATE_TOPIC}")

        # mémoriser le jour du recalcul
        tz = pytz.timezone("Europe/Paris")
        last_full_recalc_day = datetime.now(tz).strftime("%Y-%m-%d")

    def publish_5min_update():
        """
        Recalcule uniquement J0 (aujourd’hui) pour HP/HC/MP/Tempo + refresh dates,
        réutilise le cache pour J-1..J-6, puis publie un JSON complet
        (mais calcul “réduit”).
        """
        nonlocal cached_hp, cached_hc, cached_mp, cached_mptime, cached_tempo, cached_dates

        # Recalcule J0 seulement
        hpjb0 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=1)[0]
        hpjw0 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=1)[0]
        hpjr0 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=1)[0]
        hcjb0 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=1)[0]
        hcjw0 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=1)[0]
        hcjr0 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=1)[0]
        j0_hp = round(hpjb0 + hpjw0 + hpjr0, 2)
        j0_hc = round(hcjb0 + hcjw0 + hcjr0, 2)

        mp0_list, mpt0_list = fetch_daily_max_power(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=1)
        j0_mp = mp0_list[0]
        j0_mptime = mpt0_list[0]

        # Couleur tempo du jour (si dispo après 8:00)
        tempo0 = fetch_daily_tempo_colors(VM_HOST, VM_PORT, days=1)[0]

        # MAJ cache index 0
        cached_hp[0] = j0_hp
        cached_hc[0] = j0_hc
        cached_mp[0] = j0_mp
        cached_mptime[0] = j0_mptime
        cached_tempo[0] = tempo0

        # Dates dynamiques (refresh toutes les 5 min)
        cached_dates = make_dailyweek_dates(7)

        # Build + publish
        now = datetime.now().astimezone().isoformat()
        payload = build_linky_payload_exact(
            cached_hp, cached_hc, cached_mp, cached_mptime, cached_tempo
        )
        payload["lastUpdate"] = now
        payload["timeLastCall"] = now
        payload["dailyweek"] = cached_dates  # forcer dates à jour

        result2 = client.publish(LINKY_STATE_TOPIC, json.dumps(payload), qos=1, retain=MQTT_RETAIN)
        result2.wait_for_publish()
        print(f"📡 JSON (mise à jour 5 min) publié sur {LINKY_STATE_TOPIC}")
        print(f"   ↳ J0 HP/HC={j0_hp}/{j0_hc} kWh, MP={j0_mp} kVA @ {j0_mptime}, Tempo={tempo0}")
        print(f"   ↳ Dates = {cached_dates}")

    # ============ Premier calcul complet au démarrage ============
    do_full_recalc_and_publish()

    # =======================
    # Boucle principale
    # =======================
    print("\n--- Boucle MQTT démarrée ---")
    while True:
        try:
            tz = pytz.timezone("Europe/Paris")
            now_local = datetime.now(tz)
            today_str = now_local.strftime("%Y-%m-%d")

            # Changement de jour -> recalcul complet
            if last_full_recalc_day != today_str:
                print("\n📅 Changement de jour détecté -> recalcul COMPLET")
                do_full_recalc_and_publish()
            else:
                # Sinon, mise à jour “réduite” (J0 + dates + MP/heure + tempo)
                publish_5min_update()

        except Exception as e:
            print(f"❌ Erreur dans la boucle principale: {e}")

        # Attente 5 minutes
        print("\n--- Pause 5 minutes avant prochaine mise à jour ---")
        time.sleep(5 * 60)


if __name__ == "__main__":
    main()
