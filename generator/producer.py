"""Producteur Kafka asynchrone : envoie des transactions synthétiques en temps réel.

- clé de message = customer_id (un même client reste dans une même partition)
- envoi asynchrone avec callback de livraison
- vérité terrain écrite dans data/ground_truth/anomalies.jsonl, uniquement pour les
  anomalies dont la livraison est confirmée ; jamais dans le message Kafka.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from confluent_kafka import Producer

from generator.transactions import load_generator


def run(rate: int | None, duration: float | None, max_tx: int | None, reset_truth: bool) -> None:
    gen, cfg = load_generator()
    topic = cfg["kafka"]["topic"]
    rate = rate or cfg["generation"]["rate_tx_per_sec"]

    truth_dir = Path(cfg["paths"]["ground_truth_dir"])
    truth_dir.mkdir(parents=True, exist_ok=True)
    truth_file = open(truth_dir / "anomalies.jsonl", "w" if reset_truth else "a", encoding="utf-8")

    stats = {"sent": 0, "delivered": 0, "failed": 0, "anomalies": 0}

    def on_delivery(err, msg, label=None):
        if err is not None:
            stats["failed"] += 1
            if stats["failed"] <= 5:
                print(f"échec de livraison : {err}")
            return
        stats["delivered"] += 1
        if label is not None:
            truth_file.write(json.dumps(label) + "\n")
            stats["anomalies"] += 1

    producer = Producer({
        "bootstrap.servers": cfg["kafka"]["bootstrap_servers"],
        "enable.idempotence": True,        # implique acks=all, sans doublons dus aux retries
        "linger.ms": 20,                   # regroupe les messages en lots
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 200000,
    })

    def send(tx: dict, label: dict | None) -> None:
        payload = json.dumps(tx, ensure_ascii=False).encode("utf-8")
        while True:
            try:
                producer.produce(topic, key=tx["customer_id"], value=payload,
                                 on_delivery=partial(on_delivery, label=label))
                break
            except BufferError:            # file locale pleine : on laisse partir les lots
                producer.poll(0.1)
        stats["sent"] += 1
        producer.poll(0)                   # exécute les callbacks de livraison

    print(f"envoi vers '{topic}' : cible {rate} tx/s, "
          f"{'durée ' + str(duration) + ' s' if duration else 'jusqu à Ctrl+C'}")
    start = time.perf_counter()
    last_report = start
    try:
        while True:
            elapsed = time.perf_counter() - start
            if duration and elapsed >= duration:
                break
            if max_tx and stats["sent"] >= max_tx:
                break
            if stats["sent"] >= elapsed * rate + 1:   # en avance sur le débit cible
                producer.poll(0.001)
                continue
            for tx, label in gen.next_event(datetime.now(timezone.utc)):
                send(tx, label)
            if time.perf_counter() - last_report >= 5:
                last_report = time.perf_counter()
                print(f"  {elapsed:6.1f} s | envoyés {stats['sent']} | livrés {stats['delivered']}")
    except KeyboardInterrupt:
        print("interruption")
    finally:
        remaining = producer.flush(30)
        elapsed = time.perf_counter() - start
        truth_file.close()

    print("--- bilan ---")
    print(f"envoyés {stats['sent']} | livrés {stats['delivered']} | échecs {stats['failed']} "
          f"| non confirmés {remaining}")
    print(f"débit livré : {stats['delivered'] / elapsed:.0f} tx/s (cible {rate})")
    if stats["delivered"]:
        print(f"anomalies : {stats['anomalies']} ({100 * stats['anomalies'] / stats['delivered']:.2f} %)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Producteur de transactions vers Kafka")
    ap.add_argument("--rate", type=int, help="tx/s (défaut : config.yaml)")
    ap.add_argument("--duration", type=float, default=30, help="durée en secondes (0 = illimité)")
    ap.add_argument("--max-tx", type=int, help="s'arrêter après N transactions")
    ap.add_argument("--reset-truth", action="store_true", help="repartir d'une vérité terrain vide")
    args = ap.parse_args()
    run(args.rate, args.duration or None, args.max_tx, args.reset_truth)