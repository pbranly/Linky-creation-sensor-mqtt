#!/usr/bin/env python3
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

SENSOR_NAME = os.getenv("SENSOR_NAME", "linky_tic")

PUBLISH_INTERVAL = int(os.getenv("PUBLISH_INTERVAL") or 300)  # toutes les 5 min par défaut

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
LINKY_STATE_TOPIC = f"homeassistant/sensor/{SENSOR_NAME}/state"
LINKY_DISCOVERY_TOPIC = f"homeassistant/sensor/{SENSOR_NAME}/config"


# =======================
# Helper: requêtes vers VictoriaMetrics
# =======================
def vm_query_range(vm_host, vm_port, metric, start_ts, end_ts, step=3600, timeout=30):
    url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
    params = {"query": metric, "start": int(start_ts), "end": int(end_ts), "step": int(step)}
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        res_list = data.get("data", {}).get("result", [])
        if res_list and res_list[0].get("values"):
            return res_list[0]["values"]
        return []
    except Exception as e:
        print(f"❌ Erreur vm_query_range pour {metric}: {e}")
        return []


def vm_query_instant(vm_host, vm_port, metric, timeout=10):
    url = f"http://{vm_host}:{vm_port}/api/v1/query"
    params = {"query": metric}
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        res_list = data.get("data", {}).get("result", [])
        if res_list and res_list[0].get("value"):
            return res_list[0]["value"][1]
        return None
    except Exception as e:
        print(f"❌ Erreur vm_query_instant pour {metric}: {e}")
        return None


# =======================
# Calculs de conso réutilisables
# =======================
def compute_consumption_for_period(vm_host, vm_port, metrics, start_dt, end_dt, step=3600, label=""):
    """
    Pour chaque metric dans metrics:
      - récupère la série (start_dt -> end_dt)
      - calcule last - first (clamp >= 0)
      - somme sur metrics
    """
    total = 0.0
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    for metric in metrics:
        values = vm_query_range(vm_host, vm_port, metric, start_ts, end_ts, step=step)
        if not values or len(values) < 2:
            print(f"⚠️ Données insuffisantes pour {metric} ({label})")
            continue
        try:
            first_val = float(values[0][1])
            last_val = float(values[-1][1])
            consumption = max(0.0, last_val - first_val)
            total += consumption
            print(f"📊 {metric}: {first_val:.2f} → {last_val:.2f} = {consumption:.2f} kWh")
        except Exception as e:
            print(f"❌ Erreur lors du parsing pour {metric} ({label}): {e}")
            continue
    return round(total, 2)


def compute_daily_diffs(vm_host, vm_port, metric_name, days=7, step=60):
    """
    Retourne une liste [jour0, jour1, ...] avec jour0 = aujourd'hui (ou 0 si pas de données),
    chaque valeur = last - first sur la journée.
    step par défaut = 60s (comme dans le script original).
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    results = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, hour=0, minute=0, second=0, tzinfo=tz)
        end_dt = now if day == today else (start_dt + timedelta(days=1))
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        values = vm_query_range(vm_host, vm_port, metric_name, start_ts, end_ts, step=step, timeout=30)
        if not values:
            results.append(0.0)
            continue
        try:
            first_val = float(values[0][1])
            last_val = float(values[-1][1])
            diff = last_val - first_val
            if diff < 0:
                diff = 0.0
            results.append(round(diff, 2))
        except Exception as e:
            print(f"❌ Erreur compute_daily_diffs pour {metric_name} {day}: {e}")
            results.append(0.0)

    return results


# =======================
# Fonctions métier (équivalents aux versions longues)
# =======================
def fetch_yearly_consumption_data(vm_host, vm_port, metric_names):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    current_year = now.year

    current_year_start = datetime(current_year, 1, 1, tzinfo=tz)
    current_year_end = now

    last_year = current_year - 1
    last_year_start = datetime(last_year, 1, 1, tzinfo=tz)
    try:
        last_year_end = datetime(last_year, now.month, now.day, now.hour, now.minute, tzinfo=tz)
    except ValueError:
        # Cas 29/02
        last_year_end = datetime(last_year, now.month, 28, now.hour, now.minute, tzinfo=tz)

    current_year_consumption = compute_consumption_for_period(vm_host, vm_port, metric_names, current_year_start, current_year_end, step=3600, label="année en cours")
    last_year_consumption = compute_consumption_for_period(vm_host, vm_port, metric_names, last_year_start, last_year_end, step=3600, label="année précédente")

    if last_year_consumption > 0:
        yearly_evolution = ((current_year_consumption - last_year_consumption) / last_year_consumption) * 100
    else:
        yearly_evolution = 0.0

    yearly_evolution = round(yearly_evolution, 2)
    print(f"📊 Consommation année en cours ({current_year_start.date()} → {current_year_end.date()}): {current_year_consumption} kWh")
    print(f"📊 Consommation année précédente ({last_year_start.date()} → {last_year_end.date()}): {last_year_consumption} kWh")
    print(f"📊 Évolution annuelle: {yearly_evolution}%")

    return current_year_consumption, last_year_consumption, yearly_evolution


def fetch_monthly_consumption_data(vm_host, vm_port, metric_names):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)

    # mois précédent
    if now.month == 1:
        last_month_year = now.year - 1
        last_month_month = 12
    else:
        last_month_year = now.year
        last_month_month = now.month - 1

    last_month_start = datetime(last_month_year, last_month_month, 1, tzinfo=tz)
    if last_month_month == 12:
        next_month = datetime(last_month_year + 1, 1, 1, tzinfo=tz)
    else:
        next_month = datetime(last_month_year, last_month_month + 1, 1, tzinfo=tz)
    last_month_end = next_month - timedelta(seconds=1)

    # même mois année précédente
    last_year_month_year = last_month_year - 1
    last_year_month_start = datetime(last_year_month_year, last_month_month, 1, tzinfo=tz)
    if last_month_month == 12:
        next_month_last_year = datetime(last_year_month_year + 1, 1, 1, tzinfo=tz)
    else:
        next_month_last_year = datetime(last_year_month_year, last_month_month + 1, 1, tzinfo=tz)
    last_year_month_end = next_month_last_year - timedelta(seconds=1)

    def _compute(start_dt, end_dt, label):
        return compute_consumption_for_period(vm_host, vm_port, metric_names, start_dt, end_dt, step=3600, label=label)

    print("📅 Calcul mois précédent et même mois année précédente...")
    last_month_consumption = _compute(last_month_start, last_month_end, f"mois précédent ({last_month_start.strftime('%B %Y')})")
    last_month_last_year_consumption = _compute(last_year_month_start, last_year_month_end, f"même mois année précédente ({last_year_month_start.strftime('%B %Y')})")

    if last_month_last_year_consumption > 0:
        monthly_evolution = ((last_month_consumption - last_month_last_year_consumption) / last_month_last_year_consumption) * 100
    else:
        monthly_evolution = 0.0
    monthly_evolution = round(monthly_evolution, 2)

    print(f"📊 Consommation mois précédent: {last_month_consumption} kWh")
    print(f"📊 Consommation même mois année précédente: {last_month_last_year_consumption} kWh")
    print(f"📊 Évolution mensuelle: {monthly_evolution}%")

    return last_month_consumption, last_month_last_year_consumption, monthly_evolution


def fetch_current_month_consumption_data(vm_host, vm_port, metric_names):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)

    current_month_start = datetime(now.year, now.month, 1, tzinfo=tz)
    current_month_end = now

    last_year = now.year - 1
    last_year_month_start = datetime(last_year, now.month, 1, tzinfo=tz)
    try:
        last_year_month_end = datetime(last_year, now.month, now.day, now.hour, now.minute, tzinfo=tz)
    except ValueError:
        last_year_month_end = datetime(last_year, now.month, 28, now.hour, now.minute, tzinfo=tz)

    current_month_consumption = compute_consumption_for_period(vm_host, vm_port, metric_names, current_month_start, current_month_end, step=3600, label=f"mois en cours ({current_month_start.strftime('%B %Y')})")
    current_month_last_year_consumption = compute_consumption_for_period(vm_host, vm_port, metric_names, last_year_month_start, last_year_month_end, step=3600, label=f"même période année précédente ({last_year_month_start.strftime('%B %Y')})")

    if current_month_last_year_consumption > 0:
        current_month_evolution = ((current_month_consumption - current_month_last_year_consumption) / current_month_last_year_consumption) * 100
    else:
        current_month_evolution = 0.0
    current_month_evolution = round(current_month_evolution, 2)

    print(f"📊 Consommation mois en cours: {current_month_consumption} kWh")
    print(f"📊 Consommation même période année précédente: {current_month_last_year_consumption} kWh")
    print(f"📊 Évolution mois en cours: {current_month_evolution}%")

    return current_month_consumption, current_month_last_year_consumption, current_month_evolution


def fetch_daily_consumption_data(vm_host, vm_port, metric_names):
    """
    Retourne: (yesterday, day_2, yesterday_evolution)
    où yesterday = conso hier (jour complet), day_2 = avant-hier (jour complet)
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()

    yesterday = today - timedelta(days=1)
    yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=tz)
    yesterday_end = yesterday_start + timedelta(days=1) - timedelta(seconds=1)

    day_before_yesterday = today - timedelta(days=2)
    day_before_start = datetime(day_before_yesterday.year, day_before_yesterday.month, day_before_yesterday.day, tzinfo=tz)
    day_before_end = day_before_start + timedelta(days=1) - timedelta(seconds=1)

    def _compute(start_dt, end_dt, label):
        return compute_consumption_for_period(vm_host, vm_port, metric_names, start_dt, end_dt, step=3600, label=label)

    print("📅 Calcul consommation hier et avant-hier...")
    yesterday_consumption = _compute(yesterday_start, yesterday_end, f"hier ({yesterday.strftime('%d/%m/%Y')})")
    day_2_consumption = _compute(day_before_start, day_before_end, f"avant-hier ({day_before_yesterday.strftime('%d/%m/%Y')})")

    if day_2_consumption > 0:
        yesterday_evolution = ((yesterday_consumption - day_2_consumption) / day_2_consumption) * 100
    else:
        yesterday_evolution = 0.0
    yesterday_evolution = round(yesterday_evolution, 2)

    print(f"📊 Consommation hier: {yesterday_consumption} kWh")
    print(f"📊 Consommation avant-hier: {day_2_consumption} kWh")
    print(f"📊 Évolution quotidienne: {yesterday_evolution}%")

    return yesterday_consumption, day_2_consumption, yesterday_evolution


def fetch_tempo_tariffs_and_calculate_costs(vm_host, vm_port, dailyweek_HP, dailyweek_HC, dailyweek_Tempo):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()

    tariff_metrics = {
        "BLUE": {"HP": "sensor.tarif_bleu_tempo_heures_pleines_ttc_value", "HC": "sensor.tarif_bleu_tempo_heures_creuses_ttc_value"},
        "WHITE": {"HP": "sensor.tarif_blanc_tempo_heures_pleines_ttc_value", "HC": "sensor.tarif_blanc_tempo_heures_creuses_ttc_value"},
        "RED": {"HP": "sensor.tarif_rouge_tempo_heures_pleines_ttc_value", "HC": "sensor.tarif_rouge_tempo_heures_creuses_ttc_value"},
    }

    def get_current_tariff(metric_name):
        val = vm_query_instant(vm_host, vm_port, metric_name, timeout=10)
        if val is None:
            print(f"⚠️ Pas de données tarifaires pour {metric_name}")
            return 0.0
        try:
            return float(val)
        except Exception as e:
            print(f"❌ Erreur parsing tarif {metric_name}: {e}")
            return 0.0

    dailyweek_cost = []
    dailyweek_costHP = []
    dailyweek_costHC = []

    print("💰 Calcul des coûts journaliers avec tarifs Tempo...")
    for i in range(7):
        day = today - timedelta(days=i)
        color = dailyweek_Tempo[i] if i < len(dailyweek_Tempo) else "BLUE"
        hp_consumption = dailyweek_HP[i] if i < len(dailyweek_HP) else 0.0
        hc_consumption = dailyweek_HC[i] if i < len(dailyweek_HC) else 0.0

        if color in tariff_metrics:
            hp_tariff = get_current_tariff(tariff_metrics[color]["HP"])
            hc_tariff = get_current_tariff(tariff_metrics[color]["HC"])
        else:
            print(f"⚠️ Couleur inconnue {color} pour le {day.strftime('%d/%m/%Y')}, utilisation tarif BLEU par défaut")
            hp_tariff = get_current_tariff(tariff_metrics["BLUE"]["HP"])
            hc_tariff = get_current_tariff(tariff_metrics["BLUE"]["HC"])

        cost_hp = round(hp_consumption * hp_tariff, 2)
        cost_hc = round(hc_consumption * hc_tariff, 2)
        total_cost = round(cost_hp + cost_hc, 2)

        dailyweek_costHP.append(cost_hp)
        dailyweek_costHC.append(cost_hc)
        dailyweek_cost.append(total_cost)

        # Le script original affichait hp_tariff/100 pour format euro; on garde cette notation informative.
        print(f"💰 {day.strftime('%d/%m/%Y')} ({color}): HP={hp_consumption}kWh×{hp_tariff/100:.4f}€ + HC={hc_consumption}kWh×{hc_tariff/100:.4f}€ = {total_cost}€")

    print(f"💰 Coûts totaux journaliers: {dailyweek_cost}")
    print(f"💰 Coûts HP journaliers: {dailyweek_costHP}")
    print(f"💰 Coûts HC journaliers: {dailyweek_costHC}")

    return dailyweek_cost, dailyweek_costHP, dailyweek_costHC


def fetch_daily_max_power(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()

    max_values = []
    max_times = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, tzinfo=tz)
        end_dt = now if day == today else (start_dt + timedelta(days=1))

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        values = vm_query_range(vm_host, vm_port, metric_name, start_ts, end_ts, step=60, timeout=30)
        if not values:
            max_values.append(0)
            max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))
            continue

        max_val = -1.0
        max_ts = start_ts
        for ts, val in values:
            try:
                v = float(val) / 1000.0  # conversion VA → kVA
                tss = int(float(ts))
                if v > max_val:
                    max_val = v
                    max_ts = tss
            except Exception:
                continue

        if max_val < 0:
            max_values.append(0)
            max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))
        else:
            max_values.append(round(max_val, 2))
            max_times.append(datetime.fromtimestamp(max_ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S"))

    return max_values, max_times


def fetch_daily_tempo_colors(vm_host, vm_port, days=7):
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
        start_dt = datetime(year=day.year, month=day.month, day=day.day, hour=0, tzinfo=tz)
        end_dt = start_dt + timedelta(days=1)

        detected_color = "UNKNOWN"
        for color, metrics in tempo_metrics.items():
            stop = False
            for metric in metrics:
                values = vm_query_range(vm_host, vm_port, metric, int(start_dt.timestamp()), int(end_dt.timestamp()), step=300, timeout=10)
                if not values:
                    continue
                for _, v in values:
                    try:
                        if float(v) > 0:
                            detected_color = color
                            stop = True
                            break
                    except Exception:
                        continue
                if stop:
                    break
            if detected_color != "UNKNOWN":
                break
        colors.append(detected_color)

    return colors


def compute_weekly_consumption(daily_14):
    """
    daily_14 : liste des 14 derniers jours (0 = aujourd'hui, 13 = 13 jours avant)
    """
    current_week = sum(daily_14[:7])
    last_week = sum(daily_14[7:14])
    current_week_evolution = ((current_week - last_week) / last_week * 100) if last_week else 0.0

    print(f"🗓️ Current week: {current_week} kWh")
    print(f"🗓️ Last week:    {last_week} kWh")
    print(f"📊 Evolution:    {current_week_evolution:.2f} %")

    return current_week, last_week, round(current_week_evolution, 2)


# =======================
# Build payload (identique au script original)
# =======================
def build_linky_payload_exact(dailyweek_HP=None, dailyweek_HC=None,
                              dailyweek_MP=None, dailyweek_MP_time=None,
                              dailyweek_Tempo=None, current_week=0, last_week=0, current_week_evolution=0,
                              current_year=0, current_year_last_year=0, yearly_evolution=0,
                              last_month=0, last_month_last_year=0, monthly_evolution=0,
                              current_month=0, current_month_last_year=0, current_month_evolution=0,
                              yesterday=0, day_2=0, yesterday_evolution=0,
                              dailyweek_cost=None, dailyweek_costHP=None, dailyweek_costHC=None):
    tz = pytz.timezone("Europe/Paris")
    today = datetime.now(tz).date()

    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    hp = dailyweek_HP if dailyweek_HP else [0.0]*7
    hc = dailyweek_HC if dailyweek_HC else [0.0]*7
    mp = dailyweek_MP if dailyweek_MP else [0]*7
    mp_time = dailyweek_MP_time if dailyweek_MP_time else [today.strftime("%Y-%m-%d 00:00:00")]*7
    tempo = dailyweek_Tempo if dailyweek_Tempo else ["UNKNOWN"]*7
    daily = [round(hp[i] + hc[i], 2) for i in range(7)]

    # Utilisation des coûts calculés ou valeurs par défaut
    cost = dailyweek_cost if dailyweek_cost else [1.2,1.3,1.1,1.4,1.3,1.2,1.3]
    cost_hp = dailyweek_costHP if dailyweek_costHP else [0.7,0.7,0.6,0.8,0.8,0.6,0.8]
    cost_hc = dailyweek_costHC if dailyweek_costHC else [0.5,0.6,0.5,0.6,0.5,0.6,0.5]

    payload = {
        "serviceEnedis": "myElectricalData",
        "typeCompteur": "consommation",
        "unit_of_measurement": "kWh",
        "current_year": current_year,
        "current_year_last_year": current_year_last_year,
        "yearly_evolution": yearly_evolution,
        "last_month": last_month,
        "last_month_last_year": last_month_last_year,
        "monthly_evolution": monthly_evolution,
        "current_month": current_month,
        "current_month_last_year": current_month_last_year,
        "current_month_evolution": current_month_evolution,
        "current_week": current_week,
        "last_week": last_week,
        "current_week_evolution": current_week_evolution,
        "yesterday": yesterday,
        "day_2": day_2,
        "yesterday_evolution": yesterday_evolution,
        "daily": daily,
        "dailyweek": dailyweek_dates,
        "dailyweek_cost": cost,
        "dailyweek_costHC": cost_hc,
        "dailyweek_costHP": cost_hp,
        "dailyweek_HC": hc,
        "daily_cost": cost[0] if cost else 0.0,
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

    # Discovery Linky
    linky_discovery_payload = {
        "name": SENSOR_NAME.replace("_", " ").title(),
        "state_topic": LINKY_STATE_TOPIC,
        "value_template": "{{ value_json.current_year }}",
        "json_attributes_topic": LINKY_STATE_TOPIC,
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "icon": "mdi:counter",
        "unique_id": f"{SENSOR_NAME}_sensor",
        "device": {
            "identifiers": [SENSOR_NAME],
            "name": f"Compteur {SENSOR_NAME.replace('_', ' ').title()}",
            "manufacturer": "Enedis",
            "model": "Linky"
        }
    }
    client.publish(LINKY_DISCOVERY_TOPIC, json.dumps(linky_discovery_payload), qos=1, retain=True)

    print("\n--- Boucle MQTT démarrée ---")
    current_day = datetime.now(pytz.timezone("Europe/Paris")).date()

    tempo_metrics = [METRIC_NAMEhpjb, METRIC_NAMEhcjb, METRIC_NAMEhpjw,
                     METRIC_NAMEhcjw, METRIC_NAMEhpjr, METRIC_NAMEhcjr]

    while True:
        now_dt = datetime.now(pytz.timezone("Europe/Paris"))
        today = now_dt.date()
        if today != current_day:
            print("🔄 Changement de jour détecté, rafraîchissement complet")
            current_day = today

        # HP / HC pour 14 derniers jours (chaque metric séparément)
        hpjb_14 = compute_daily_diffs(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=14)
        hpjw_14 = compute_daily_diffs(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=14)
        hpjr_14 = compute_daily_diffs(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=14)
        hcjb_14 = compute_daily_diffs(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=14)
        hcjw_14 = compute_daily_diffs(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=14)
        hcjr_14 = compute_daily_diffs(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=14)

        daily_14 = [round(hpjb_14[i] + hpjw_14[i] + hpjr_14[i] + hcjb_14[i] + hcjw_14[i] + hcjr_14[i], 2) for i in range(14)]

        # Calcul des semaines
        current_week, last_week, current_week_evolution = compute_weekly_consumption(daily_14)

        # Calcul des données annuelles
        print("\n📊 Calcul des données annuelles...")
        current_year, current_year_last_year, yearly_evolution = fetch_yearly_consumption_data(VM_HOST, VM_PORT, tempo_metrics)

        # Calcul des données mensuelles
        print("\n📊 Calcul des données mensuelles...")
        last_month, last_month_last_year, monthly_evolution = fetch_monthly_consumption_data(VM_HOST, VM_PORT, tempo_metrics)

        # Calcul des données du mois en cours
        print("\n📊 Calcul des données du mois en cours...")
        current_month, current_month_last_year, current_month_evolution = fetch_current_month_consumption_data(VM_HOST, VM_PORT, tempo_metrics)

        # Calcul des données quotidiennes
        print("\n📊 Calcul des données quotidiennes...")
        yesterday, day_2, yesterday_evolution = fetch_daily_consumption_data(VM_HOST, VM_PORT, tempo_metrics)

        # HP / HC pour les 7 derniers jours
        dailyweek_HP = [round(hpjb_14[i] + hpjw_14[i] + hpjr_14[i], 2) for i in range(7)]
        dailyweek_HC = [round(hcjb_14[i] + hcjw_14[i] + hcjr_14[i], 2) for i in range(7)]

        # Puissance max
        dailyweek_MP, dailyweek_MP_time = fetch_daily_max_power(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=7)

        # Couleurs tempo
        dailyweek_Tempo = fetch_daily_tempo_colors(VM_HOST, VM_PORT, days=7)

        # Calcul des coûts avec les tarifs Tempo
        print("\n💰 Calcul des coûts journaliers...")
        dailyweek_cost, dailyweek_costHP, dailyweek_costHC = fetch_tempo_tariffs_and_calculate_costs(
            VM_HOST, VM_PORT, dailyweek_HP, dailyweek_HC, dailyweek_Tempo
        )

        # JSON
        linky_payload = build_linky_payload_exact(
            dailyweek_HP, dailyweek_HC, dailyweek_MP, dailyweek_MP_time, dailyweek_Tempo,
            current_week, last_week, current_week_evolution,
            current_year, current_year_last_year, yearly_evolution,
            last_month, last_month_last_year, monthly_evolution,
            current_month, current_month_last_year, current_month_evolution,
            yesterday, day_2, yesterday_evolution,
            dailyweek_cost, dailyweek_costHP, dailyweek_costHC
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
        print(f"  Current week:   {linky_payload['current_week']} kWh")
        print(f"  Last week:      {linky_payload['last_week']} kWh")
        print(f"  Week evolution: {linky_payload['current_week_evolution']}%")
        print(f"  Current year:   {linky_payload['current_year']} kWh")
        print(f"  Last year:      {linky_payload['current_year_last_year']} kWh")
        print(f"  Year evolution: {linky_payload['yearly_evolution']}%")
        print(f"  Last month:     {linky_payload['last_month']} kWh")
        print(f"  Last month LY:  {linky_payload['last_month_last_year']} kWh")
        print(f"  Month evolution: {linky_payload['monthly_evolution']}%")
        print(f"  Current month:  {linky_payload['current_month']} kWh")
        print(f"  Current month LY: {linky_payload['current_month_last_year']} kWh")
        print(f"  Current month evo: {linky_payload['current_month_evolution']}%")
        print(f"  Yesterday:      {linky_payload['yesterday']} kWh")
        print(f"  Day before:     {linky_payload['day_2']} kWh")
        print(f"  Daily evolution: {linky_payload['yesterday_evolution']}%")
        print(f"  Coûts journaliers: {linky_payload['dailyweek_cost']}")
        print(f"  Coûts HP:       {linky_payload['dailyweek_costHP']}")
        print(f"  Coûts HC:       {linky_payload['dailyweek_costHC']}")
        print(f"  Last update:    {linky_payload['lastUpdate']}")

        # Publication
        result = client.publish(LINKY_STATE_TOPIC, json.dumps(linky_payload), qos=1, retain=MQTT_RETAIN)
        try:
            result.wait_for_publish()
        except Exception:
            # selon implementation paho, wait_for_publish peut échouer sur certains clients; on ignore
            pass
        print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")

        # Pause
        time.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    main()
