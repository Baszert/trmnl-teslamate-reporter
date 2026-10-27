import os
import logging
import schedule
import time
import requests
import paho.mqtt.client as mqtt

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("trmnl-teslamate-reporter")

# Load dev environment variables if available
try:
    from dotenv import load_dotenv
    load_dotenv()  # Development mode
except ImportError:
    pass           # Production mode

# Get configuration from environment variables
FETCH_FREQUENCY = int(os.environ.get("FETCH_FREQUENCY", "15"))
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
CAR_ID = int(os.environ.get("CAR_ID", "1"))

COLLECT_SECONDS = float(os.environ.get("COLLECT_SECONDS", "3"))

# TeslaMate topics to forward (teslamate/cars/<id>/<key>). Kept to what the
# TRMNL plugin shows, so the webhook payload stays well under 5 kB.
TOPICS = {
    # car
    "display_name", "model", "trim_badging", "version", "update_available",
    "update_version",
    # state
    "state", "since", "geofence", "speed", "power",
    # battery & charging
    "battery_level", "usable_battery_level", "rated_battery_range_km",
    "est_battery_range_km", "plugged_in", "charging_state", "charge_limit_soc",
    "charger_power", "charger_voltage", "charger_actual_current",
    "charger_phases", "charge_energy_added", "time_to_full_charge",
    # climate
    "inside_temp", "outside_temp", "is_climate_on", "is_preconditioning",
    # security & body
    "locked", "sentry_mode", "doors_open", "windows_open", "trunk_open",
    "frunk_open",
    # tyres
    "tpms_pressure_fl", "tpms_pressure_fr", "tpms_pressure_rl",
    "tpms_pressure_rr", "tpms_soft_warning_fl", "tpms_soft_warning_fr",
    "tpms_soft_warning_rl", "tpms_soft_warning_rr",
    # misc
    "odometer",
}

def fetch_data_mqtt():
    """Collect the retained TeslaMate values for this car in one connection"""
    results = {}
    prefix = f"teslamate/cars/{CAR_ID}/"

    def on_connect(client, userdata, flags, reason_code, properties):
        client.subscribe(prefix + "#")

    def on_message(client, userdata, msg):
        key = msg.topic[len(prefix):]
        if key in TOPICS and msg.payload:
            results[key] = msg.payload.decode().strip()

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if MQTT_USER and MQTT_PASSWORD:
            client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
        client.loop_start()
        time.sleep(COLLECT_SECONDS)
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        logger.error(f"Error fetching data from MQTT: {e}")

    if results:
        results["reported_at"] = str(int(time.time()))

    return results

def post_to_webhook(data):
    """Post data to webhook endpoint"""
    if not data:
        logger.warning("No data to send")
        return

    payload = {
        "merge_variables": data
    }

    try:
        logger.debug(f"POSTing payload: {payload}")
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            logger.info(f"Successfully posted {len(data)} records to webhook")
        else:
            logger.error(f"Webhook post failed with status {response.status_code}: {response.text}")
    except Exception as e:
        logger.error(f"Error posting to webhook: {e}")

def report_data():
    """Main function to report data from MQTT to webhook"""
    logger.info("Starting data reporter job")
    data = fetch_data_mqtt()
    if data:
        post_to_webhook(data)
        logger.info("Data reporter job completed")
    else:
        logger.warning("Data reporter job failed")

def start_scheduler():
    """Set up and start the scheduler"""
    logger.info("Starting scheduler")
    schedule.every(FETCH_FREQUENCY).minutes.do(report_data)

    # Run once immediately on startup
    report_data()

    # Keep the script running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute for pending jobs
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user (CTRL+C). Exiting gracefully.")
        exit(0)

if __name__ == "__main__":
    if not WEBHOOK_URL:
        logger.critical("WEBHOOK_URL environment variable is not set. Exiting.")
        exit(1)

    logger.info("Reporter service starting up")
    start_scheduler()
