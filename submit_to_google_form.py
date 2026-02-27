import requests
import datetime
import logging
import time

logger = logging.getLogger(__name__)

PING_FORM_URL = "https://docs.google.com/forms/d/e/1FAIpQLSdKfiAaWkccMGS8tdpHZx6mTQglx4qyXI3FI4q9B1hbHpe-6w/formResponse"
SPEED_FORM_URL = "https://docs.google.com/forms/d/e/1FAIpQLSe0KNSxoVx1eZKQzThy9du1f5b1QXI1RBkuZHjIRxY7e74vJA/formResponse"

PING_FORM_ENTRY_IDS = {
    "site_id": "entry.340984991",
    "timestamp": "entry.1528372262",
    "ip_address": "entry.108902199",
    "location": "entry.1210248263",
    "netsuite_up": "entry.188435571",
    "cru_up": "entry.353707753",
    "okta_up": "entry.388744467",
    "google_up": "entry.1234320055",
    "apple_up": "entry.1570063945",
    "pihole_up": "entry.2027274655",
    "node_up": "entry.233669881",
    "github_up": "entry.85529884",
    "speedtest_up": "entry.755750213",
    "http_latency": "entry.267708514",
    "http_samples": "entry.653778920",
    "http_time": "entry.1194873277",
    "http_content_length": "entry.739476608",
    "http_duration": "entry.752185240",
}

SPEED_FORM_ENTRY_IDS = {
    "site_id": "entry.1089427232",
    "siteid": "entry.1089427232",
    "timestamp": "entry.17901975",
    "location": "entry.389737321",
    "download_mbps": "entry.1429602019",
    "upload_mbps": "entry.306813776",
    "ping_ms": "entry.1963382231",
    "jitter_ms": "entry.1682369155",
    "ip_addres": "entry.1588320435",
}

def format_data(metrics_data, form_url, form_entry_ids):
    """
    Submits collected metrics to the Google Form.
    """
    form_data = {}
    form_data[form_entry_ids["timestamp"]] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for metric_name, value in metrics_data.items():
        if metric_name in form_entry_ids:
            form_data[form_entry_ids[metric_name]] = str(value)
        else:
            # logger.warning(f"Metric '{metric_name}' not found in form_entry_ids. Skipping.")
    submit_to_google_form(form_data, form_url)


def submit_to_google_form(form_data, form_url, max_retries=3, delay_seconds=60):
    for attempt in range(max_retries):
        try:
            response = requests.post(form_url, data=form_data)
            response.raise_for_status()
            # logger.info(f"Successfully submitted data to Google Form. Response: {response.status_code}")
            return
        except requests.exceptions.RequestException as e:
            if response is not None and response.status_code == 429:
                logger.warning(f"Received 429 (Too Many Requests). Retrying in {delay_seconds} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay_seconds)
            else:
                logger.error(f"Failed to submit data to Google Form: {e}")
                break
    logger.error(f"Failed to submit data to Google Form after {max_retries} attempts.")


def ping(metrics_data):
    """
    Collects and submits ping-related metrics to the Google Form.
    """
    submit_to_google_form(metrics_data, PING_FORM_URL)

def speed(metrics_data):
    """
    Collects and submits speed-related metrics to the Google Form.
    """
    submit_to_google_form(metrics_data, SPEED_FORM_URL)
