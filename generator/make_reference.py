"""Génère customers.csv et products.csv (référentiels, lus par la détection)."""
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# catégorie -> (prix médian, dispersion log-normale)
CATEGORIES = {
    "electronics": (300, 0.6),
    "home": (60, 0.6),
    "sports": (50, 0.5),
    "clothing": (40, 0.5),
    "toys": (25, 0.5),
    "beauty": (20, 0.4),
    "books": (15, 0.3),
    "grocery": (8, 0.4),
}


def main(config_path: str = "generator/config.yaml") -> None:
    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    rng = np.random.default_rng(cfg["seed"])
    ref = cfg["reference"]
    out = Path(cfg["paths"]["reference_dir"])
    out.mkdir(parents=True, exist_ok=True)

    # --- clients ---
    n_c = ref["n_customers"]
    segs = list(ref["segments"].keys())
    customers = pd.DataFrame({
        "customer_id": [f"C{i:05d}" for i in range(1, n_c + 1)],
        "segment": rng.choice(segs, size=n_c, p=list(ref["segments"].values())),
        "home_city": rng.choice(ref["cities"], size=n_c),
    })
    customers.to_csv(out / "customers.csv", index=False)

    # --- produits ---
    n_p = ref["n_products"]
    cats = rng.choice(list(CATEGORIES), size=n_p)
    prices = [
        max(1.0, round(float(rng.lognormal(np.log(CATEGORIES[c][0]), CATEGORIES[c][1])), 2))
        for c in cats
    ]
    products = pd.DataFrame({
        "product_id": [f"P{i:04d}" for i in range(1, n_p + 1)],
        "category": cats,
        "product_name": [f"{c}_item_{i}" for i, c in enumerate(cats, start=1)],
        "ref_price": prices,
    })
    products.to_csv(out / "products.csv", index=False)

    print(f"{len(customers)} clients, {len(products)} produits -> {out}")


if __name__ == "__main__":
    main()