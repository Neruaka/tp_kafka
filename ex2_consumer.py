#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
from datetime import datetime, timezone

from kafka import KafkaConsumer

# config 
TOPIC = "weather_stream"
BOOTSTRAP = "kafka:9092"
GROUP_ID = "ex2-console-reader"
# fin config


def deserialize(raw):
    """bytes reçus de kafka -> dict si c'est du json, sinon texte brut"""
    if raw is None:
        return None
    text = raw.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=deserialize,
)

print(f"en écoute sur '{TOPIC}' (groupe '{GROUP_ID}')... Ctrl+C pour arrêter")

try:
    for msg in consumer:
        heure = datetime.fromtimestamp(msg.timestamp / 1000, tz=timezone.utc)
        print(f"[{heure:%H:%M:%S} UTC] partition={msg.partition} offset={msg.offset} -> {msg.value}")
except KeyboardInterrupt:
    print("\narrêt demandé")
finally:
    consumer.close()


# In[ ]:




