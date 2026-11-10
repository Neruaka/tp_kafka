{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "bf9f6897-ec02-4573-8a17-b35dfc68a84a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "en écoute sur 'weather_stream' (groupe 'ex2-console-reader')... Ctrl+C pour arrêter\n",
      "[14:32:03 UTC] partition=0 offset=0 -> {'msg': 'Hello Kafka'}\n",
      "[15:08:03 UTC] partition=0 offset=1 -> {'msg': 'Hello Kafka'}\n",
      "[15:08:07 UTC] partition=0 offset=2 -> {'msg': 'Hello Kafka'}\n"
     ]
    }
   ],
   "source": [
    "import json\n",
    "from datetime import datetime, timezone\n",
    "\n",
    "from kafka import KafkaConsumer\n",
    "\n",
    "# config \n",
    "TOPIC = \"weather_stream\"\n",
    "BOOTSTRAP = \"kafka:9092\"\n",
    "GROUP_ID = \"ex2-console-reader\"\n",
    "# fin config\n",
    "\n",
    "\n",
    "def deserialize(raw):\n",
    "    \"\"\"bytes reçus de kafka -> dict si c'est du json, sinon texte brut\"\"\"\n",
    "    if raw is None:\n",
    "        return None\n",
    "    text = raw.decode(\"utf-8\")\n",
    "    try:\n",
    "        return json.loads(text)\n",
    "    except json.JSONDecodeError:\n",
    "        return text\n",
    "\n",
    "\n",
    "consumer = KafkaConsumer(\n",
    "    TOPIC,\n",
    "    bootstrap_servers=BOOTSTRAP,\n",
    "    group_id=GROUP_ID,\n",
    "    auto_offset_reset=\"earliest\",\n",
    "    enable_auto_commit=True,\n",
    "    value_deserializer=deserialize,\n",
    ")\n",
    "\n",
    "print(f\"en écoute sur '{TOPIC}' (groupe '{GROUP_ID}')... Ctrl+C pour arrêter\")\n",
    "\n",
    "try:\n",
    "    for msg in consumer:\n",
    "        heure = datetime.fromtimestamp(msg.timestamp / 1000, tz=timezone.utc)\n",
    "        print(f\"[{heure:%H:%M:%S} UTC] partition={msg.partition} offset={msg.offset} -> {msg.value}\")\n",
    "except KeyboardInterrupt:\n",
    "    print(\"\\narrêt demandé\")\n",
    "finally:\n",
    "    consumer.close()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "e9c77e11-cd4d-4696-bcc3-51e014fb9efe",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
