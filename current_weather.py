#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import logging
import signal
import time

import requests
from kafka import KafkaProducer

# config
TOPIC = "weather_stream"
BOOTSTRAP = "kafka:9092"
INTERVAL_S = 30

API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude={latitude}&longitude={longitude}"
    "&current=temperature_2m,wind_speed_10m,weather_code&wind_speed_unit=ms"
)

CITIES = [
    {"city": "Paris",      "country": "France",               "latitude": 48.8566,  "longitude": 2.3522},
    {"city": "Seville",    "country": "Spain",                "latitude": 37.3891,  "longitude": -5.9845},
    {"city": "Dubai",      "country": "United Arab Emirates", "latitude": 25.2048,  "longitude": 55.2708},
    {"city": "Reykjavik",  "country": "Iceland",              "latitude": 64.1466,  "longitude": -21.9426},
    {"city": "Wellington", "country": "New Zealand",          "latitude": -41.2865, "longitude": 174.7762},
]
# fin config

logging.basicConfig(format="%(asctime)s %(levelname)-7s %(message)s", level=logging.INFO)
log = logging.getLogger("current_weather")


def fetch_weather(session, city):
    """appelle l'api pour une ville et renvoie la réponse + city/country"""
    url = API_URL.format(latitude=city["latitude"], longitude=city["longitude"])
    response = session.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    data["city"] = city["city"]
    data["country"] = city["country"]
    return data


def stop(signum, frame):
    """kill / pkill envoie SIGTERM : on le transforme en Ctrl+C pour sortir proprement"""
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, stop)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    session = requests.Session()
    log.info("démarré -> topic '%s', une tournée toutes les %s s", TOPIC, INTERVAL_S)

    try:
        while True:
            start = time.monotonic()

            for city in CITIES:
                try:
                    data = fetch_weather(session, city)
                    producer.send(TOPIC, key=city["city"], value=data)
                    cur = data["current"]
                    log.info("%-10s %s°C | vent %s m/s | code %s",
                             city["city"], cur["temperature_2m"], cur["wind_speed_10m"], cur["weather_code"])
                except (requests.RequestException, ValueError, KeyError) as e:
                    log.warning("%-10s échec : %s", city["city"], e)

            producer.flush()
            elapsed = time.monotonic() - start
            time.sleep(max(0, INTERVAL_S - elapsed))
    except KeyboardInterrupt:
        log.info("arrêt demandé")
    finally:
        producer.flush()
        producer.close()
        session.close()
        log.info("producer fermé proprement")


if __name__ == "__main__":
    main()


# In[ ]:




