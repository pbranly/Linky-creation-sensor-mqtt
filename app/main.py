import os
import sys
import time
import json
import threading
import requests
from datetime import datetime, timedelta
import pytz
import paho.mqtt.client as mqtt

# =======================
# CONFIG DEBUG & LOG
# =======================
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
def log_debug(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}")

print("--- Début de l'exécution du script Linky + VictoriaMetrics ---")
time.sleep(0.5)
print(f"Version Python: {sys.version}")
print(f"Mode DEBUG: {DEBUG}")

# =======================
# ENV / PARAMÈTRES
# =======================
LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT") or 1883)
MQTT_RETAIN = True

VM_HOST = os.getenv("VM_HOST", "127.0.0.1")
VM_PORT = int(os.getenv("VM_PORT") or 8428)

# Linky / VictoriaMetrics metric names
METRIC_NAMEhpjb = "sensor.linky_tempo_index_bbrhpjb_value"
METRIC_NAMEhcjb = "sensor.linky_tempo_index_bbrhcjb_value"
METRIC_NAMEhpjw = "sensor.linky_tempo_index_bbrhpjw_value"
METRIC_NAMEhcjw = "sensor.linky_tempo_index_bbrhcjw_value"
METRIC_NAMEhpjr = "sensor.linky_tempo_index_bbrhpjr_value"
METRIC_NAMEhcjr = "sensor.linky_tempo_index_bbrhcjr_value"
METRIC_NAMEpcons = "sensor.linky_puissance_consommee_value"  # VA instantané

# Options
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", 300))  # rafraîchissement (sec), défaut 5 min
MAX_POWER_LIMIT_KVA = float(os.getenv("MAX_POWER_LIMIT_KVA", 7.0))  # seuil MP_over en kVA
KWH_PRICE = os.getenv("KWH_PRICE")  # prix global au kWh (optionnel)
KWH_PRICE_HP = os.getenv("KWH_PRICE_HP")
KWH_PRICE_HC = os.getenv("KWH_PRICE_HC")

# =======================
# MQTT Topics (un seul sensor complet)
# =======================
LINKY_STATE_TOPIC = "homeassistant/sensor/linky_test/state"
LINKY_DISCOVERY_TOPIC = "homeassistant/sensor/linky_test/config"

# =======================
# OUTILS TEMPS / VM
# =======================
def tz_now():
    return datetime.now(pytz.timezone("Europe/Paris"))

def vm_query_range(metric_name, start_ts, end_ts, step=60):
    url = f"http://{VM_HOST}:{VM_PORT}/api/v1/query_range"
    params = {"query": metric_name, "start": start_ts, "end": end_ts, "step": step}
    log_debug(f"VM query_range: {metric_name}, start={start_ts}, end={end_ts}, step={step}")
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def diff_counter_for_period(metric_name, start_dt, end_dt, step=300):
    """
    Retourne (last - first) pour un 'compteur' (index) sur [start_dt, end_dt].
    Si données absentes: 0.0
    """
    try:
        data = vm_query_range(metric_name, int(start_dt.timestamp()), int(end_dt.timestamp()), step=step)
        res_list = data.get("data", {}).get("result", [])
        if not res_list:
            return 0.0
        values = res_list[0].get("values", [])
        if not values:
            return 0.0
        first_val = float(values[0][1])
        last_val = float(values[-1][1])
        diff = last_val - first_val
        return round(diff if diff > 0 else 0.0, 4)
    except Exception as e:
        print(f"❌ diff_counter_for_period '{metric_name}' : {e}")
        return 0.0

def fetch_daily_for_calendar_days(metric_name, days=7):
    """
    Retourne la conso quotidienne (kWh) sur 7 jours: [J, J-1, ..., J-6]
    En bornant chaque jour sur minuit Europe/Paris. Pour aujourd'hui, fin = now().
    """
    tz = pytz.timezone("Europe/Paris")
    now = tz_now()
    today = now.date()
    out = []
    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = tz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
        end_dt = now if (day == today) else (start_dt + timedelta(days=1))
        val = diff_counter_for_period(metric_name, start_dt, end_dt, step=60)
        out.append(round(val, 3))
        log_debug(f"[{metric_name}] {day} => {val} kWh")
    return out  # ordre: J, J-1, ...

def fetch_daily_max_power(days=7):
    """
    Puissance max journalière sur 7 jours, en kVA (VA/1000).
    """
    tz = pytz.timezone("Europe/Paris")
    now = tz_now()
    today = now.date()
    max_values, max_times = [], []
    for i in range(days):
        day = today - timedelta(days=i)
        start_dt = tz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
        end_dt = now if (day == today) else (start_dt + timedelta(days=1))
        try:
            data = vm_query_range(METRIC_NAMEpcons, int(start_dt.timestamp()), int(end_dt.timestamp()), step=60)
            res_list = data.get("data", {}).get("result", [])
            if not res_list or not res_list[0].get("values"):
                max_values.append(0.0)
                max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))
                continue
            values = res_list[0]["values"]
            mval = -1.0
            mts = int(start_dt.timestamp())
            for ts, val in values:
                v_kva = float(val) / 1000.0
                if v_kva > mval:
                    mval = v_kva
                    mts = int(ts)
            mval = round(mval if mval > 0 else 0.0, 2)
            max_values.append(mval)
            max_times.append(datetime.fromtimestamp(mts, tz=tz).strftime("%Y-%m-%d %H:%M:%S"))
            log_debug(f"[MP] {day} => {mval} kVA à {max_times[-1]}")
        except Exception as e:
            print(f"❌ fetch_daily_max_power pour {day}: {e}")
            max_values.append(0.0)
            max_times.append(start_dt.strftime("%Y-%m-%d 00:00:00"))
    return max_values, max_times

def sum_counters_period(metric_names, start_dt, end_dt, step=900):
    """
    Somme des (last-first) pour plusieurs compteurs (HP/HC toutes couleurs) sur la période.
    """
    total = 0.0
    for m in metric_names:
        total += diff_counter_for_period(m, start_dt, end_dt, step=step)
    return round(total, 3)

def fetch_year_to_date(year_offset=0):
    """
    current_year (offset=0): 1er janvier année courante -> maintenant.
    current_year_last_year (offset=1): 1er jan de l'année N-1 -> même date/heure relative (fin = aujourd'hui N, même mm-jj hh:mm).
    """
    tz = pytz.timezone("Europe/Paris")
    now = tz_now()
    year = now.year - year_offset
    start_dt = tz.localize(datetime(year, 1, 1, 0, 0, 0))
    # Fin = "même point dans l'année" (ex: 12/09 14:23) mais sur l'année year
    end_dt = tz.localize(datetime(year, now.month, now.day, now.hour, now.minute, now.second))
    return sum_counters_period(
        [METRIC_NAMEhpjb, METRIC_NAMEhpjw, METRIC_NAMEhpjr,
         METRIC_NAMEhcjb, METRIC_NAMEhcjw, METRIC_NAMEhcjr],
        start_dt, end_dt, step=1800
    )

def fetch_month_to_date(year_offset=0):
    """
    Somme depuis le 1er du mois courant (année - offset) jusqu'au 'même jour/heure' relative.
    """
    tz = pytz.timezone("Europe/Paris")
    now = tz_now()
    year = now.year - year_offset
    start_dt = tz.localize(datetime(year, now.month, 1, 0, 0, 0))
    end_dt = tz.localize(datetime(year, now.month, now.day, now.hour, now.minute, now.second))
    return sum_counters_period(
        [METRIC_NAMEhpjb, METRIC_NAMEhpjw, METRIC_NAMEhpjr,
         METRIC_NAMEhcjb, METRIC_NAMEhcjw, METRIC_NAMEhcjr],
        start_dt, end_dt, step=900
    )

def fetch_last_month(year_offset=0):
    """
    Conso du mois précédent COMPLET (année - offset).
    """
    tz = pytz.timezone("Europe/Paris")
    now = tz_now()
    # Mois précédent par rapport à 'year'
    target = now.replace(year=now.year - year_offset)
    first_this_month = tz.localize(datetime(target.year, target.month, 1, 0, 0, 0))
    first_prev_month = (first_this_month - timedelta(days=1)).replace(day=1)
    # Période = [1er mois-1, 1er mois courant)
    start_dt = first_prev_month
    end_dt = first_this_month
    return sum_counters_period(
        [METRIC_NAMEhpjb, METRIC_NAMEhpjw, METRIC_NAMEhpjr,
         METRIC_NAMEhcjb, METRIC_NAMEhcjw, METRIC_NAMEhcjr],
        start_dt, end_dt, step=1800
    )

def fetch_week_values():
    """
    current_week / last_week (ISO week, lundi->dimanche)
    - current_week: lundi de la semaine courante -> maintenant
    - last_week: lundi de la semaine précédente -> lundi (semaine courante)
    """
    tz = pytz.timezone("Europe/Paris")
    now = tz_now()
    # Lundi de cette semaine
    monday_this = (now - timedelta(days=(now.weekday()))).replace(hour=0, minute=0, second=0, microsecond=0)
    monday_this = tz.localize(datetime(monday_this.year, monday_this.month, monday_this.day, 0, 0, 0))
    # Lundi de la semaine précédente
    monday_prev = monday_this - timedelta(days=7)
    current_week = sum_counters_period(
        [METRIC_NAMEhpjb, METRIC_NAMEhpjw, METRIC_NAMEhpjr,
         METRIC_NAMEhcjb, METRIC_NAMEhcjw, METRIC_NAMEhcjr],
        monday_this, now, step=900
    )
    last_week = sum_counters_period(
        [METRIC_NAMEhpjb, METRIC_NAMEhpjw, METRIC_NAMEhpjr,
         METRIC_NAMEhcjb, METRIC_NAMEhcjw, METRIC_NAMEhcjr],
        monday_prev, monday_this, step=1800
    )
    return round(current_week, 3), round(last_week, 3)

def percent(a, b):
    if b and b != 0:
        return round((a - b) / b * 100.0, 2)
    return 0.0

# =======================
# TEMPO PAR PRÉSENCE DE CONSOMMATION
# =======================
def detect_tempo_colors_7days(hpjb, hcjb, hpjw, hcjw, hpjr, hcjr, eps=0.01):
    """
    Retourne 7 couleurs (J, J-1, ..., J-6) où la couleur du jour est
    celle pour laquelle HP+HC progresse (>eps) et les deux autres ~0.
    Si plusieurs >eps: on prend le max ; si égalité ou rien: "UNKNOWN".
    """
    colors = []
    for i in range(7):
        blue  = (hpjb[i] + hcjb[i])
        white = (hpjw[i] + hcjw[i])
        red   = (hpjr[i] + hcjr[i])
        vals = {"BLUE": blue, "WHITE": white, "RED": red}
        positives = {k: v for k, v in vals.items() if v > eps}
        if len(positives) == 1:
            color = list(positives.keys())[0]
        elif len(positives) == 0:
            color = "UNKNOWN"
        else:
            # Plusieurs couleurs > eps : choisir la dominante si elle est clairement plus grande
            color = max(positives, key=positives.get)
            # Si très proches (ambigü), on peut rester sur la dominante malgré tout.
        colors.append(color)
        log_debug(f"[TEMPO] J-{i}: BLUE={blue:.3f} WHITE={white:.3f} RED={red:.3f} => {color}")
    return colors

# =======================
# CALCUL DES COÛTS (OPTIONNEL)
# =======================
def compute_costs(daily_hp, daily_hc):
    """
    Calcule dailyweek_cost, dailyweek_costHP, dailyweek_costHC et daily_cost (J)
    si des prix sont fournis. Sinon, met -1.
    """
    n = len(daily_hp)
    costs_total  = [-1]*n
    costs_hp     = [-1]*n
    costs_hc     = [-1]*n
    daily_cost   = -1

    try:
        if KWH_PRICE_HP and KWH_PRICE_HC:
            p_hp = float(KWH_PRICE_HP)
            p_hc = float(KWH_PRICE_HC)
            for i in range(n):
                costs_hp[i] = round(daily_hp[i] * p_hp, 2)
                costs_hc[i] = round(daily_hc[i] * p_hc, 2)
                costs_total[i] = round(costs_hp[i] + costs_hc[i], 2)
            daily_cost = costs_total[0]
        elif KWH_PRICE:
            p = float(KWH_PRICE)
            totals = [round(daily_hp[i] + daily_hc[i], 3) for i in range(n)]
            for i in range(n):
                costs_total[i] = round(totals[i] * p, 2)
            daily_cost = costs_total[0]
    except Exception as e:
        print(f"⚠️ Erreur compute_costs: {e}")

    return costs_total, costs_hc, costs_hp, daily_cost

# =======================
# JSON principal
# =======================
def build_linky_payload_exact(
    dailyweek_HP, dailyweek_HC,
    dailyweek_MP, dailyweek_MP_time,
    dailyweek_Tempo,
    current_year, current_year_last_year,
    extra_stats
):
    tz = pytz.timezone("Europe/Paris")
    today = tz_now().date()
    dailyweek_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    hp = dailyweek_HP
    hc = dailyweek_HC
    mp = dailyweek_MP
    mp_time = dailyweek_MP_time
    tempo = dailyweek_Tempo

    daily = [round(hp[i] + hc[i], 3) for i in range(7)]

    # Costs (optionnels)
    dailyweek_cost, dailyweek_costHC, dailyweek_costHP, daily_cost = compute_costs(hp, hc)

    # Ratios & agrégats calculés
    yesterday = round(daily[1], 3) if len(daily) > 1 else 0
    day_2 = round(daily[2], 3) if len(daily) > 2 else 0
    yesterday_evolution = percent(yesterday, day_2)

    current_week = extra_stats["current_week"]
    last_week = extra_stats["last_week"]
    current_week_evolution = percent(current_week, last_week)

    current_month = extra_stats["current_month"]
    current_month_last_year = extra_stats["current_month_last_year"]
    current_month_evolution = percent(current_month, current_month_last_year)

    last_month = extra_stats["last_month"]
    last_month_last_year = extra_stats["last_month_last_year"]
    monthly_evolution = percent(last_month, last_month_last_year)

    # % HP (hier)
    if len(hp) > 1 and (hp[1] + hc[1]) > 0:
        peak_offpeak_percent = round(hp[1] / (hp[1] + hc[1]) * 100.0, 1)
    else:
        peak_offpeak_percent = 0

    # yearly evolution
    yearly_evolution = percent(current_year, current_year_last_year)

    payload = {
        "serviceEnedis": "myElectricalData",
        "typeCompteur": "consommation",
        "unit_of_measurement": "kWh",

        # Annuel (demandé)
        "current_year": round(current_year, 3),
        "current_year_last_year": round(current_year_last_year, 3),
        "yearly_evolution": yearly_evolution,

        # Mensuel / Hebdo / Journalier (cohérent, calculé)
        "last_month": round(last_month, 3),
        "last_month_last_year": round(last_month_last_year, 3),
        "monthly_evolution": monthly_evolution,

        "current_month": round(current_month, 3),
        "current_month_last_year": round(current_month_last_year, 3),
        "current_month_evolution": current_month_evolution,

        "current_week": round(current_week, 3),
        "last_week": round(last_week, 3),
        "current_week_evolution": current_week_evolution,

        "yesterday": yesterday,
        "day_2": day_2,
        "yesterday_evolution": yesterday_evolution,

        # Historique 7 jours
        "daily": daily,
        "dailyweek": dailyweek_dates,
        "dailyweek_cost": dailyweek_cost,
        "dailyweek_costHC": dailyweek_costHC,
        "dailyweek_costHP": dailyweek_costHP,

        "dailyweek_HC": hc,
        "dailyweek_HP": hp,

        # Coût du jour (optionnel, sinon -1)
        "daily_cost": daily_cost,

        # Détails jour J-1
        "yesterday_HP": hp[1] if len(hp) > 1 else 0.0,
        "yesterday_HC": hc[1] if len(hc) > 1 else 0.0,

        # Max puissance (kVA) + dépassement
        "dailyweek_MP": mp,
        "dailyweek_MP_over": [bool(v > MAX_POWER_LIMIT_KVA) for v in mp],
        "dailyweek_MP_time": mp_time,

        # Tempo
        "dailyweek_Tempo": tempo,

        # Admin / divers
        "errorLastCall": "",
        "versionUpdateAvailable": False,
        "versionGit": "1.0.0",
        "peak_offpeak_percent": peak_offpeak_percent
    }
    return payload

# =======================
# SCRIPT PRINCIPAL
# =======================
def main():
    # MQTT
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

    # Discovery unique
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

    print(f"⏱️ Rafraîchissement toutes les {REFRESH_INTERVAL} secondes ; seuil MP_over = {MAX_POWER_LIMIT_KVA} kVA")

    while True:
        loop_start = tz_now()
        print("\n--- Début du cycle ---")

        # 1) Récup indices quotidiens (J..J-6) pour chaque couleur & HP/HC
        hpjb = fetch_daily_for_calendar_days(METRIC_NAMEhpjb, days=7)
        hcjb = fetch_daily_for_calendar_days(METRIC_NAMEhcjb, days=7)
        hpjw = fetch_daily_for_calendar_days(METRIC_NAMEhpjw, days=7)
        hcjw = fetch_daily_for_calendar_days(METRIC_NAMEhcjw, days=7)
        hpjr = fetch_daily_for_calendar_days(METRIC_NAMEhpjr, days=7)
        hcjr = fetch_daily_for_calendar_days(METRIC_NAMEhcjr, days=7)

        # Sommes HP / HC
        dailyweek_HP = [round(hpjb[i] + hpjw[i] + hpjr[i], 3) for i in range(7)]
        dailyweek_HC = [round(hcjb[i] + hcjw[i] + hcjr[i], 3) for i in range(7)]
        log_debug(f"dailyweek_HP = {dailyweek_HP}")
        log_debug(f"dailyweek_HC = {dailyweek_HC}")

        # 2) Puissance max / jour (kVA)
        dailyweek_MP, dailyweek_MP_time = fetch_daily_max_power(days=7)
        log_debug(f"dailyweek_MP = {dailyweek_MP}")
        log_debug(f"dailyweek_MP_time = {dailyweek_MP_time}")

        # 3) Couleur Tempo par présence de conso
        dailyweek_Tempo = detect_tempo_colors_7days(hpjb, hcjb, hpjw, hcjw, hpjr, hcjr, eps=0.01)
        log_debug(f"dailyweek_Tempo = {dailyweek_Tempo}")

        # 4) Agrégats période (année/mois/semaine)
        current_year = fetch_year_to_date(0)
        current_year_last_year = fetch_year_to_date(1)
        current_month = fetch_month_to_date(0)
        current_month_last_year = fetch_month_to_date(1)
        last_month = fetch_last_month(0)
        last_month_last_year = fetch_last_month(1)
        wk_cur, wk_prev = fetch_week_values()

        extra_stats = {
            "current_month": current_month,
            "current_month_last_year": current_month_last_year,
            "last_month": last_month,
            "last_month_last_year": last_month_last_year,
            "current_week": wk_cur,
            "last_week": wk_prev
        }

        log_debug(f"current_year={current_year} ; current_year_last_year={current_year_last_year}")
        log_debug(f"current_month={current_month} ; current_month_last_year={current_month_last_year}")
        log_debug(f"last_month={last_month} ; last_month_last_year={last_month_last_year}")
        log_debug(f"current_week={wk_cur} ; last_week={wk_prev}")

        # 5) Construire payload COMPLET
        payload = build_linky_payload_exact(
            dailyweek_HP, dailyweek_HC,
            dailyweek_MP, dailyweek_MP_time,
            dailyweek_Tempo,
            current_year, current_year_last_year,
            extra_stats
        )

        now_iso = tz_now().astimezone().isoformat()
        payload["lastUpdate"] = now_iso
        payload["timeLastCall"] = now_iso

        # 6) Publier
        client.publish(LINKY_STATE_TOPIC, json.dumps(payload), qos=1, retain=MQTT_RETAIN)
        print(f"📡 JSON complet publié sur {LINKY_STATE_TOPIC}")
        if DEBUG:
            print(f"[DEBUG] dailyweek dates = {payload['dailyweek']}")
            print(f"[DEBUG] yesterday_HP={payload['yesterday_HP']} ; yesterday_HC={payload['yesterday_HC']}")
            print(f"[DEBUG] MP_over = {payload['dailyweek_MP_over']} (seuil {MAX_POWER_LIMIT_KVA} kVA)")
            print(f"[DEBUG] yearly_evolution = {payload['yearly_evolution']} %")

        # 7) Attente
        elapsed = (tz_now() - loop_start).total_seconds()
        sleep_time = max(1, REFRESH_INTERVAL - int(elapsed))
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
