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
# VictoriaMetrics robust daily fetch
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
            if not res_list:
                results.append(0.0)
                continue

            values = res_list[0].get("values", [])
            if not values:
                results.append(0.0)
                continue

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
# Fetch yearly consumption data
# =======================
def fetch_yearly_consumption_data(vm_host, vm_port, metric_names):
    """
    Calcule les consommations annuelles selon les spécifications :
    - current_year : du 1er janvier à maintenant
    - current_year_last_year : même période l'année précédente
    
    Pour chaque metric, calcul = max(dernier_jour) - min(1er_janvier) + somme_autres_metrics
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    current_year = now.year
    
    # Période année en cours (1er janvier à maintenant)
    current_year_start = datetime(current_year, 1, 1, tzinfo=tz)
    current_year_end = now
    
    # Période année précédente (1er janvier à date équivalente)
    last_year = current_year - 1
    last_year_start = datetime(last_year, 1, 1, tzinfo=tz)
    # Date équivalente l'année dernière
    try:
        last_year_end = datetime(last_year, now.month, now.day, now.hour, now.minute, tzinfo=tz)
    except ValueError:  # Cas du 29 février sur année non bissextile
        last_year_end = datetime(last_year, now.month, 28, now.hour, now.minute, tzinfo=tz)
    
    def get_consumption_for_period(start_dt, end_dt, metrics):
        """Calcule la consommation totale pour une période donnée"""
        total_consumption = 0.0
        
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        
        for metric in metrics:
            try:
                url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
                params = {"query": metric, "start": start_ts, "end": end_ts, "step": 3600}  # 1h step
                
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                res_list = data.get("data", {}).get("result", [])
                
                if not res_list or not res_list[0].get("values"):
                    print(f"⚠️ Pas de données pour {metric} sur la période {start_dt.date()} - {end_dt.date()}")
                    continue
                
                values = res_list[0]["values"]
                if len(values) < 2:
                    print(f"⚠️ Données insuffisantes pour {metric}")
                    continue
                
                # Calcul : max(dernier_jour) - min(1er_janvier)
                first_val = float(values[0][1])  # Valeur du 1er janvier
                last_val = float(values[-1][1])   # Valeur actuelle
                
                consumption = max(0, last_val - first_val)
                total_consumption += consumption
                
                print(f"📊 {metric}: {first_val:.2f} → {last_val:.2f} = {consumption:.2f} kWh")
                
            except Exception as e:
                print(f"❌ Erreur lors du calcul pour {metric}: {e}")
                continue
        
        return round(total_consumption, 2)
    
    # Calcul année en cours
    current_year_consumption = get_consumption_for_period(current_year_start, current_year_end, metric_names)
    print(f"📊 Consommation année en cours ({current_year_start.date()} → {current_year_end.date()}): {current_year_consumption} kWh")
    
    # Calcul année précédente (même période)
    last_year_consumption = get_consumption_for_period(last_year_start, last_year_end, metric_names)
    print(f"📊 Consommation année précédente ({last_year_start.date()} → {last_year_end.date()}): {last_year_consumption} kWh")
    
    # Calcul évolution
    if last_year_consumption > 0:
        yearly_evolution = ((current_year_consumption - last_year_consumption) / last_year_consumption) * 100
    else:
        yearly_evolution = 0.0
    
    yearly_evolution = round(yearly_evolution, 2)
    print(f"📊 Évolution annuelle: {yearly_evolution}%")
    
    return current_year_consumption, last_year_consumption, yearly_evolution

# =======================
# Fetch monthly consumption data
# =======================
def fetch_monthly_consumption_data(vm_host, vm_port, metric_names):
    """
    Calcule les consommations mensuelles selon les spécifications :
    - last_month : du 1er au dernier jour du mois précédent
    - last_month_last_year : même mois l'année précédente
    
    Pour chaque metric, calcul = max(dernier_jour) - min(1er_du_mois) + somme_autres_metrics
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    
    # Calcul du mois précédent
    if now.month == 1:
        last_month_year = now.year - 1
        last_month_month = 12
    else:
        last_month_year = now.year
        last_month_month = now.month - 1
    
    # Période mois précédent (1er au dernier jour du mois précédent)
    last_month_start = datetime(last_month_year, last_month_month, 1, tzinfo=tz)
    
    # Dernier jour du mois précédent
    if last_month_month == 12:
        next_month = datetime(last_month_year + 1, 1, 1, tzinfo=tz)
    else:
        next_month = datetime(last_month_year, last_month_month + 1, 1, tzinfo=tz)
    last_month_end = next_month - timedelta(seconds=1)
    
    # Même mois l'année précédente
    last_year_month_year = last_month_year - 1
    last_year_month_start = datetime(last_year_month_year, last_month_month, 1, tzinfo=tz)
    
    # Dernier jour du même mois l'année précédente
    if last_month_month == 12:
        next_month_last_year = datetime(last_year_month_year + 1, 1, 1, tzinfo=tz)
    else:
        next_month_last_year = datetime(last_year_month_year, last_month_month + 1, 1, tzinfo=tz)
    last_year_month_end = next_month_last_year - timedelta(seconds=1)
    
    def get_consumption_for_period(start_dt, end_dt, metrics, period_name):
        """Calcule la consommation totale pour une période donnée"""
        total_consumption = 0.0
        
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        
        print(f"📅 Calcul {period_name} ({start_dt.date()} → {end_dt.date()})")
        
        for metric in metrics:
            try:
                url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
                params = {"query": metric, "start": start_ts, "end": end_ts, "step": 3600}  # 1h step
                
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                res_list = data.get("data", {}).get("result", [])
                
                if not res_list or not res_list[0].get("values"):
                    print(f"⚠️ Pas de données pour {metric} sur la période {start_dt.date()} - {end_dt.date()}")
                    continue
                
                values = res_list[0]["values"]
                if len(values) < 2:
                    print(f"⚠️ Données insuffisantes pour {metric}")
                    continue
                
                # Calcul : max(dernier_jour) - min(1er_du_mois)
                first_val = float(values[0][1])  # Valeur du 1er du mois
                last_val = float(values[-1][1])   # Valeur du dernier jour
                
                consumption = max(0, last_val - first_val)
                total_consumption += consumption
                
                print(f"📊 {metric}: {first_val:.2f} → {last_val:.2f} = {consumption:.2f} kWh")
                
            except Exception as e:
                print(f"❌ Erreur lors du calcul mensuel pour {metric}: {e}")
                continue
        
        return round(total_consumption, 2)
    
    # Calcul mois précédent
    last_month_consumption = get_consumption_for_period(
        last_month_start, last_month_end, metric_names, 
        f"mois précédent ({last_month_start.strftime('%B %Y')})"
    )
    
    # Calcul même mois l'année précédente
    last_month_last_year_consumption = get_consumption_for_period(
        last_year_month_start, last_year_month_end, metric_names,
        f"même mois année précédente ({last_year_month_start.strftime('%B %Y')})"
    )
    
    # Calcul évolution mensuelle
    if last_month_last_year_consumption > 0:
        monthly_evolution = ((last_month_consumption - last_month_last_year_consumption) / last_month_last_year_consumption) * 100
    else:
        monthly_evolution = 0.0
    
    monthly_evolution = round(monthly_evolution, 2)
    
    print(f"📊 Consommation mois précédent: {last_month_consumption} kWh")
    print(f"📊 Consommation même mois année précédente: {last_month_last_year_consumption} kWh")
    print(f"📊 Évolution mensuelle: {monthly_evolution}%")
    
    return last_month_consumption, last_month_last_year_consumption, monthly_evolution

# =======================
# Fetch current month consumption data
# =======================
def fetch_current_month_consumption_data(vm_host, vm_port, metric_names):
    """
    Calcule les consommations du mois en cours selon les spécifications :
    - current_month : du 1er du mois actuel à maintenant
    - current_month_last_year : même période l'année précédente
    
    Pour chaque metric, calcul = max(dernier_jour) - min(1er_du_mois) + somme_autres_metrics
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    
    # Période mois en cours (1er du mois à maintenant)
    current_month_start = datetime(now.year, now.month, 1, tzinfo=tz)
    current_month_end = now
    
    # Même période l'année précédente
    last_year = now.year - 1
    last_year_month_start = datetime(last_year, now.month, 1, tzinfo=tz)
    # Date équivalente l'année dernière
    try:
        last_year_month_end = datetime(last_year, now.month, now.day, now.hour, now.minute, tzinfo=tz)
    except ValueError:  # Cas du 29 février sur année non bissextile
        last_year_month_end = datetime(last_year, now.month, 28, now.hour, now.minute, tzinfo=tz)
    
    def get_consumption_for_period(start_dt, end_dt, metrics, period_name):
        """Calcule la consommation totale pour une période donnée"""
        total_consumption = 0.0
        
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        
        print(f"📅 Calcul {period_name} ({start_dt.date()} → {end_dt.date()})")
        
        for metric in metrics:
            try:
                url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
                params = {"query": metric, "start": start_ts, "end": end_ts, "step": 3600}  # 1h step
                
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                res_list = data.get("data", {}).get("result", [])
                
                if not res_list or not res_list[0].get("values"):
                    print(f"⚠️ Pas de données pour {metric} sur la période {start_dt.date()} - {end_dt.date()}")
                    continue
                
                values = res_list[0]["values"]
                if len(values) < 2:
                    print(f"⚠️ Données insuffisantes pour {metric}")
                    continue
                
                # Calcul : max(dernier_jour) - min(1er_du_mois)
                first_val = float(values[0][1])  # Valeur du 1er du mois
                last_val = float(values[-1][1])   # Valeur actuelle
                
                consumption = max(0, last_val - first_val)
                total_consumption += consumption
                
                print(f"📊 {metric}: {first_val:.2f} → {last_val:.2f} = {consumption:.2f} kWh")
                
            except Exception as e:
                print(f"❌ Erreur lors du calcul mois en cours pour {metric}: {e}")
                continue
        
        return round(total_consumption, 2)
    
    # Calcul mois en cours
    current_month_consumption = get_consumption_for_period(
        current_month_start, current_month_end, metric_names, 
        f"mois en cours ({current_month_start.strftime('%B %Y')})"
    )
    
    # Calcul même période l'année précédente
    current_month_last_year_consumption = get_consumption_for_period(
        last_year_month_start, last_year_month_end, metric_names,
        f"même période année précédente ({last_year_month_start.strftime('%B %Y')})"
    )
    
    # Calcul évolution mois en cours
    if current_month_last_year_consumption > 0:
        current_month_evolution = ((current_month_consumption - current_month_last_year_consumption) / current_month_last_year_consumption) * 100
    else:
        current_month_evolution = 0.0
    
    current_month_evolution = round(current_month_evolution, 2)
    
    print(f"📊 Consommation mois en cours: {current_month_consumption} kWh")
    print(f"📊 Consommation même période année précédente: {current_month_last_year_consumption} kWh")
    print(f"📊 Évolution mois en cours: {current_month_evolution}%")
    
    return current_month_consumption, current_month_last_year_consumption, current_month_evolution

# =======================
# Fetch daily consumption data (yesterday and day before)
# =======================
def fetch_daily_consumption_data(vm_host, vm_port, metric_names):
    """
    Calcule les consommations quotidiennes selon les spécifications :
    - yesterday : consommation d'hier (jour complet)
    - day_2 : consommation avant-hier (jour complet)
    - yesterday_evolution : évolution entre avant-hier et hier
    
    Pour chaque metric, calcul = max(fin_jour) - min(début_jour) + somme_autres_metrics
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    
    # Hier (jour complet)
    yesterday = today - timedelta(days=1)
    yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=tz)
    yesterday_end = yesterday_start + timedelta(days=1) - timedelta(seconds=1)
    
    # Avant-hier (jour complet)
    day_before_yesterday = today - timedelta(days=2)
    day_before_start = datetime(day_before_yesterday.year, day_before_yesterday.month, day_before_yesterday.day, tzinfo=tz)
    day_before_end = day_before_start + timedelta(days=1) - timedelta(seconds=1)
    
    def get_consumption_for_day(start_dt, end_dt, metrics, day_name):
        """Calcule la consommation totale pour une journée"""
        total_consumption = 0.0
        
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        
        print(f"📅 Calcul {day_name} ({start_dt.date()})")
        
        for metric in metrics:
            try:
                url = f"http://{vm_host}:{vm_port}/api/v1/query_range"
                params = {"query": metric, "start": start_ts, "end": end_ts, "step": 3600}  # 1h step
                
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                res_list = data.get("data", {}).get("result", [])
                
                if not res_list or not res_list[0].get("values"):
                    print(f"⚠️ Pas de données pour {metric} le {start_dt.date()}")
                    continue
                
                values = res_list[0]["values"]
                if len(values) < 2:
                    print(f"⚠️ Données insuffisantes pour {metric}")
                    continue
                
                # Calcul : max(fin_jour) - min(début_jour)
                first_val = float(values[0][1])  # Valeur début jour
                last_val = float(values[-1][1])   # Valeur fin jour
                
                consumption = max(0, last_val - first_val)
                total_consumption += consumption
                
                print(f"📊 {metric}: {first_val:.2f} → {last_val:.2f} = {consumption:.2f} kWh")
                
            except Exception as e:
                print(f"❌ Erreur lors du calcul quotidien pour {metric}: {e}")
                continue
        
        return round(total_consumption, 2)
    
    # Calcul hier
    yesterday_consumption = get_consumption_for_day(
        yesterday_start, yesterday_end, metric_names, 
        f"hier ({yesterday.strftime('%d/%m/%Y')})"
    )
    
    # Calcul avant-hier
    day_2_consumption = get_consumption_for_day(
        day_before_start, day_before_end, metric_names,
        f"avant-hier ({day_before_yesterday.strftime('%d/%m/%Y')})"
    )
    
    # Calcul évolution quotidienne
    if day_2_consumption > 0:
        yesterday_evolution = ((yesterday_consumption - day_2_consumption) / day_2_consumption) * 100
    else:
        yesterday_evolution = 0.0
    
    yesterday_evolution = round(yesterday_evolution, 2)
    
    print(f"📊 Consommation hier: {yesterday_consumption} kWh")
    print(f"📊 Consommation avant-hier: {day_2_consumption} kWh")
    print(f"📊 Évolution quotidienne: {yesterday_evolution}%")
    
    return yesterday_consumption, day_2_consumption, yesterday_evolution

# =======================
# Fetch Tempo tariffs and calculate daily costs
# =======================
def fetch_tempo_tariffs_and_calculate_costs(vm_host, vm_port, dailyweek_HP, dailyweek_HC, dailyweek_Tempo):
    """
    Récupère les tarifs Tempo depuis VictoriaMetrics et calcule les coûts journaliers
    """
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()
    
    # Mapping des couleurs vers les métriques de tarifs
    tariff_metrics = {
        "BLUE": {
            "HP": "sensor.tarif_bleu_tempo_heures_pleines_ttc_value",
            "HC": "sensor.tarif_bleu_tempo_heures_creuses_ttc_value"
        },
        "WHITE": {
            "HP": "sensor.tarif_blanc_tempo_heures_pleines_ttc_value", 
            "HC": "sensor.tarif_blanc_tempo_heures_creuses_ttc_value"
        },
        "RED": {
            "HP": "sensor.tarif_rouge_tempo_heures_pleines_ttc_value",
            "HC": "sensor.tarif_rouge_tempo_heures_creuses_ttc_value"
        }
    }
    
    def get_current_tariff(metric_name):
        """Récupère le tarif actuel depuis VictoriaMetrics"""
        try:
            url = f"http://{vm_host}:{vm_port}/api/v1/query"
            params = {"query": metric_name}
            
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            res_list = data.get("data", {}).get("result", [])
            
            if res_list and res_list[0].get("value"):
                tariff = float(res_list[0]["value"][1])
                return tariff
            else:
                print(f"⚠️ Pas de données tarifaires pour {metric_name}")
                return 0.0
                
        except Exception as e:
            print(f"❌ Erreur récupération tarif {metric_name}: {e}")
            return 0.0
    
    # Calcul des coûts pour chaque jour de la semaine
    dailyweek_cost = []
    dailyweek_costHP = []
    dailyweek_costHC = []
    
    print("💰 Calcul des coûts journaliers avec tarifs Tempo...")
    
    for i in range(7):
        day = today - timedelta(days=i)
        color = dailyweek_Tempo[i]
        hp_consumption = dailyweek_HP[i]
        hc_consumption = dailyweek_HC[i]
        
        # Récupération des tarifs selon la couleur du jour
        if color in tariff_metrics:
            hp_tariff = get_current_tariff(tariff_metrics[color]["HP"])
            hc_tariff = get_current_tariff(tariff_metrics[color]["HC"])
        else:
            print(f"⚠️ Couleur inconnue {color} pour le {day.strftime('%d/%m/%Y')}, utilisation tarif BLEU par défaut")
            hp_tariff = get_current_tariff(tariff_metrics["BLUE"]["HP"])
            hc_tariff = get_current_tariff(tariff_metrics["BLUE"]["HC"])
        
        # Calcul des coûts
        cost_hp = round(hp_consumption * hp_tariff , 2)
        cost_hc = round(hc_consumption * hc_tariff , 2)
        total_cost = round(cost_hp + cost_hc, 2)
        
        dailyweek_costHP.append(cost_hp)
        dailyweek_costHC.append(cost_hc)
        dailyweek_cost.append(total_cost)
        
        print(f"💰 {day.strftime('%d/%m/%Y')} ({color}): HP={hp_consumption}kWh×{hp_tariff/100:.4f}€ + HC={hc_consumption}kWh×{hc_tariff/100:.4f}€ = {total_cost}€")
    
    print(f"💰 Coûts totaux journaliers: {dailyweek_cost}")
    print(f"💰 Coûts HP journaliers: {dailyweek_costHP}")
    print(f"💰 Coûts HC journaliers: {dailyweek_costHC}")
    
    return dailyweek_cost, dailyweek_costHP, dailyweek_costHC

# =======================
# Puissance max par jour (VA → kVA)
# =======================
def fetch_daily_max_power(vm_host, vm_port, metric_name, days=7):
    tz = pytz.timezone("Europe/Paris")
    now = datetime.now(tz)
    today = now.date()

    max_values = []
    max_times = []

    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = datetime(year=day.year, month=day.month, day=day.day, tzinfo=tz)
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
            max_val = -1
            max_ts = start_ts
            for ts, val in values:
                v = float(val)/1000.0  # conversion VA → kVA
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

# =======================
# Détection couleur Tempo par consommation
# =======================
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

# =======================
# Calcul de la consommation hebdomadaire
# =======================
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

    return current_week, last_week, round(current_week_evolution,2)

# =======================
# JSON complet
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
    "name": SENSOR_NAME.replace("_", " ").title(),  # Ex: linky_test -> "Linky Test"
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
    client.publish(LINKY_DISCOVERY_TOPIC, json.dumps(linky_discovery_payload), qos=1, retain=True)

    print("\n--- Boucle MQTT démarrée ---")
    current_day = datetime.now(pytz.timezone("Europe/Paris")).date()

    # Liste des métriques pour les calculs annuels
    tempo_metrics = [METRIC_NAMEhpjb, METRIC_NAMEhcjb, METRIC_NAMEhpjw, 
                     METRIC_NAMEhcjw, METRIC_NAMEhpjr, METRIC_NAMEhcjr]

    while True:
        now_dt = datetime.now(pytz.timezone("Europe/Paris"))
        today = now_dt.date()

        if today != current_day:
            print("🔄 Changement de jour détecté, rafraîchissement complet")
            current_day = today

        # HP / HC pour 14 derniers jours
        hpjb_14 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjb, days=14)
        hpjw_14 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjw, days=14)
        hpjr_14 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhpjr, days=14)
        hcjb_14 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjb, days=14)
        hcjw_14 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjw, days=14)
        hcjr_14 = fetch_daily_for_calendar_days(VM_HOST, VM_PORT, METRIC_NAMEhcjr, days=14)

        daily_14 = [round(hpjb_14[i]+hpjw_14[i]+hpjr_14[i]+hcjb_14[i]+hcjw_14[i]+hcjr_14[i], 2) for i in range(14)]

        # Calcul des semaines
        current_week, last_week, current_week_evolution = compute_weekly_consumption(daily_14)

        # Calcul des données annuelles
        print("\n📊 Calcul des données annuelles...")
        current_year, current_year_last_year, yearly_evolution = fetch_yearly_consumption_data(
            VM_HOST, VM_PORT, tempo_metrics
        )

        # Calcul des données mensuelles
        print("\n📊 Calcul des données mensuelles...")
        last_month, last_month_last_year, monthly_evolution = fetch_monthly_consumption_data(
            VM_HOST, VM_PORT, tempo_metrics
        )

        # Calcul des données du mois en cours
        print("\n📊 Calcul des données du mois en cours...")
        current_month, current_month_last_year, current_month_evolution = fetch_current_month_consumption_data(
            VM_HOST, VM_PORT, tempo_metrics
        )

        # Calcul des données quotidiennes
        print("\n📊 Calcul des données quotidiennes...")
        yesterday, day_2, yesterday_evolution = fetch_daily_consumption_data(
            VM_HOST, VM_PORT, tempo_metrics
        )

        # HP / HC pour les 7 derniers jours (inchangé)
        dailyweek_HP = [round(hpjb_14[i]+hpjw_14[i]+hpjr_14[i],2) for i in range(7)]
        dailyweek_HC = [round(hcjb_14[i]+hcjw_14[i]+hcjr_14[i],2) for i in range(7)]

        # Puissance max
        dailyweek_MP, dailyweek_MP_time = fetch_daily_max_power(VM_HOST, VM_PORT, METRIC_NAMEpcons, days=7)

        # Couleur tempo
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
        result.wait_for_publish()
        print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")

        # Pause
        time.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    main()
