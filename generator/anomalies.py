"""Injecteurs d'anomalies. Chaque fonction renvoie une liste de transactions.

Les étiquettes (type, groupe) sont attachées par TransactionGenerator.next_event,
jamais dans le message envoyé à Kafka.
"""
from __future__ import annotations

from datetime import timedelta

# Taille moyenne (en transactions) d'une occurrence de chaque type.
# Sert à calibrer le ratio global d'anomalies en transactions.
ANOMALY_SIZES = {
    "extreme_amount": 1,
    "extreme_quantity": 1,
    "customer_burst": 22.5,   # 15 à 30 transactions
    "hour_spike": 30,         # 20 à 40 transactions
    "unusual_city": 1,
}

FOREIGN_CITIES = ["Paris", "Madrid", "Dubaï", "Istanbul", "Lagos", "N'Djamena", "Montréal"]


def extreme_amount(gen, ts):
    """Montant multiplié par 8 à 30 par rapport au montant normal."""
    tx = gen.normal(ts)
    tx["amount"] = round(tx["amount"] * float(gen.rng.uniform(8, 30)), 2)
    return [tx]


def extreme_quantity(gen, ts):
    """Quantité aberrante (20 à 99) ; le montant suit la quantité."""
    tx = gen.normal(ts)
    qty = int(gen.rng.integers(20, 100))
    tx["quantity"] = qty
    tx["amount"] = round(gen.price_of[tx["product_id"]] * qty * float(gen.rng.lognormal(0, 0.1)), 2)
    return [tx]


def customer_burst(gen, ts):
    """Un même client enchaîne 15 à 30 achats en moins d'une minute."""
    cust = gen.pick_customer()
    n = int(gen.rng.integers(15, 31))
    return [
        gen.normal(ts + timedelta(seconds=float(gen.rng.uniform(0, 60))), customer_idx=cust, home_city=True)
        for _ in range(n)
    ]


def hour_spike(gen, ts):
    """20 à 40 transactions de clients différents, en 5 minutes, en pleine nuit."""
    night = ts.replace(hour=int(gen.rng.integers(1, 5)), minute=int(gen.rng.integers(0, 55)),
                       second=0, microsecond=0)
    n = int(gen.rng.integers(20, 41))
    return [gen.normal(night + timedelta(seconds=float(gen.rng.uniform(0, 300)))) for _ in range(n)]


def unusual_city(gen, ts):
    """Achat dans une ville étrangère, très éloignée de la ville habituelle du client."""
    tx = gen.normal(ts, home_city=True)
    tx["city"] = str(gen.rng.choice(FOREIGN_CITIES))
    return [tx]


INJECTORS = {
    "extreme_amount": extreme_amount,
    "extreme_quantity": extreme_quantity,
    "customer_burst": customer_burst,
    "hour_spike": hour_spike,
    "unusual_city": unusual_city,
}