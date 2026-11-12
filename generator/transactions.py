"""Générateur de transactions synthétiques (normales + anomalies étiquetées à part)."""
from __future__ import annotations

import argparse
import json
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import yaml

from generator.anomalies import ANOMALY_SIZES, INJECTORS

SEGMENT_FACTOR = {"standard": 1.0, "premium": 1.5, "vip": 3.0}


class TransactionGenerator:
    def __init__(self, cfg: dict, customers: pd.DataFrame, products: pd.DataFrame, seed: int | None = None):
        self.rng = np.random.default_rng(cfg["seed"] if seed is None else seed)
        self.cities = cfg["reference"]["cities"]

        self.cust_ids = customers["customer_id"].to_numpy()
        self.cust_home = customers["home_city"].to_numpy()
        self.prod_ids = products["product_id"].to_numpy()
        self.prod_cat = products["category"].to_numpy()
        self.price_of = dict(zip(products["product_id"], products["ref_price"]))

        # Activité des clients : longue traîne, amplifiée par le segment
        w = self.rng.lognormal(0, 1.0, len(customers)) * customers["segment"].map(SEGMENT_FACTOR).to_numpy()
        self.cust_cdf = np.cumsum(w / w.sum())
        # Popularité des produits : décroissante (type Zipf)
        ranks = self.rng.permutation(len(products)) + 1
        pw = 1.0 / ranks ** 0.8
        self.prod_cdf = np.cumsum(pw / pw.sum())

        # Calibrage : part d'anomalies exprimée en TRANSACTIONS
        gen_cfg = cfg["generation"]
        ratio = gen_cfg["anomaly_ratio"]
        weights = gen_cfg["anomaly_weights"]
        total = sum(weights.values())
        self.types = list(weights)
        d = sum((weights[t] / total) / ANOMALY_SIZES[t] for t in self.types)
        probs = np.array([(weights[t] / total) / ANOMALY_SIZES[t] / d for t in self.types])
        self.type_p = probs / probs.sum()
        self.event_anomaly_p = ratio / ((1 - ratio) / d + ratio)
        self.group_counter = 0

    # --- tirages ---
    def pick_customer(self) -> int:
        return min(int(np.searchsorted(self.cust_cdf, self.rng.random())), len(self.cust_ids) - 1)

    def pick_product(self) -> int:
        return min(int(np.searchsorted(self.prod_cdf, self.rng.random())), len(self.prod_ids) - 1)

    def new_id(self) -> str:
        return str(uuid.UUID(bytes=self.rng.bytes(16), version=4))

    # --- transaction normale ---
    def normal(self, ts: datetime, customer_idx: int | None = None, home_city: bool = False) -> dict:
        c = self.pick_customer() if customer_idx is None else customer_idx
        p = self.pick_product()
        qty = int(min(1 + self.rng.poisson(0.3), 5))
        amount = round(float(self.price_of[self.prod_ids[p]]) * qty * float(self.rng.lognormal(0, 0.1)), 2)
        city = self.cust_home[c]
        if not home_city and self.rng.random() < 0.03:   # déplacement normal dans une autre ville
            city = str(self.rng.choice(self.cities))
        return {
            "transaction_id": self.new_id(),
            "customer_id": str(self.cust_ids[c]),
            "product_id": str(self.prod_ids[p]),
            "category": str(self.prod_cat[p]),
            "amount": amount,
            "quantity": qty,
            "city": str(city),
            "timestamp": ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    # --- événement : une transaction normale, ou un groupe d'anomalies ---
    def next_event(self, ts: datetime) -> list[tuple[dict, dict | None]]:
        """Renvoie des couples (transaction, étiquette). Étiquette = None si normale.

        L'étiquette sert uniquement à écrire la vérité terrain ; elle n'est jamais envoyée à Kafka.
        """
        if self.rng.random() < self.event_anomaly_p:
            t = self.types[int(self.rng.choice(len(self.types), p=self.type_p))]
            self.group_counter += 1
            gid = f"G{self.group_counter:07d}"
            txs = INJECTORS[t](self, ts)
            return [(tx, {"transaction_id": tx["transaction_id"], "anomaly_type": t, "group_id": gid})
                    for tx in txs]
        return [(self.normal(ts), None)]


def load_generator(config_path: str = "generator/config.yaml") -> tuple[TransactionGenerator, dict]:
    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ref = cfg["paths"]["reference_dir"]
    customers = pd.read_csv(f"{ref}/customers.csv")
    products = pd.read_csv(f"{ref}/products.csv")
    return TransactionGenerator(cfg, customers, products), cfg


def _demo(n_events: int) -> None:
    """Contrôle hors Kafka : ratio réel d'anomalies et répartition par type."""
    gen, _ = load_generator()
    ts = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
    n_tx, types, amounts, sample = 0, Counter(), [], None
    for _ in range(n_events):
        ts += timedelta(milliseconds=3)
        for tx, label in gen.next_event(ts):
            n_tx += 1
            if label:
                types[label["anomaly_type"]] += 1
            else:
                amounts.append(tx["amount"])
                sample = sample or tx
    n_anom = sum(types.values())
    print(f"transactions : {n_tx}  |  anomalies : {n_anom} ({100 * n_anom / n_tx:.2f} %)")
    for t, k in types.most_common():
        print(f"  {t:<18} {k:>6}  ({100 * k / n_anom:.1f} % des anomalies)")
    print(f"montant normal : médiane {np.median(amounts):.2f}, "
          f"p99 {np.percentile(amounts, 99):.2f}, max {max(amounts):.2f}")
    print("exemple de message :", json.dumps(sample, ensure_ascii=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=20000)
    _demo(ap.parse_args().events)