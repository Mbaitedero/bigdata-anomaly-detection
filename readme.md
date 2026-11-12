# Plateforme Big Data de détection de transactions atypiques

Pipeline reproductible : **Kafka → HDFS → traitement batch (PySpark / MapReduce) → détection → évaluation chiffrée**.

> 🚧 Projet en cours de construction. L'infrastructure (Kafka + HDFS) est en place ; l'ingestion et le traitement arrivent. Voir l'avancement ci-dessous.

## Objectif

Générer des transactions synthétiques contenant environ 5 % d'anomalies de types variés, les ingérer en continu dans HDFS, puis comparer plusieurs méthodes de détection (z-score, approche robuste, score composite) à une vérité terrain, avec précision, rappel et F1 mesurés.

Une transaction atypique est un **signal à examiner**, pas une preuve de fraude.

## Architecture

```
 Générateur Python ──► Kafka (KRaft, topic "transactions", 3 partitions)
                              │
                              ▼
                    Consommateur ──► HDFS  /transactions/year=…/month=…/day=…/
                                          │
   customers.csv, products.csv ──► HDFS /reference/
                                          │
                                          ▼
                       PySpark (stats, détection) + 1 job MapReduce Java
                                          │
                                          ▼
                           HDFS /curated/ (Parquet) ──► rapport, évaluation
```

Schéma détaillé à venir : `docs/architecture.png`.

## Stack

| Couche | Technologie |
|---|---|
| Messagerie | Apache Kafka 3.9 (mode KRaft, un nœud) |
| Stockage | HDFS (Hadoop 3.3.6, 1 namenode, 1 datanode) |
| Orchestration locale | Docker Compose |
| Génération | Python (confluent-kafka, NumPy, Pandas) |
| Traitement | PySpark, MapReduce Java |
| Rapport | Notebook Python (Pandas, Matplotlib, Seaborn) |

## Avancement

- [x] Dépôt, structure, `.gitignore`
- [x] Kafka KRaft + topic `transactions` (3 partitions)
- [x] HDFS : namenode + datanode, écriture et lecture validées
- [x] Référentiels clients et produits (générés, chargés dans HDFS `/reference/`)
- [ ] Générateur de transactions et injection des anomalies, vérité terrain
- [ ] Consommateur Kafka → HDFS (buffering, partitionnement par date)
- [ ] Historique de 30 jours (backfill)
- [ ] Statistiques et jointures (PySpark), sortie Parquet
- [ ] Détection : méthodes A, B, C et évaluation
- [ ] MapReduce Java et benchmark contre Spark
- [ ] Rapport, tests, intégration continue

## Démarrage rapide (état actuel)

Prérequis : Docker Desktop (5 Go de RAM ou plus alloués à Docker recommandés), Python 3.10 ou plus.

```bash
git clone https://github.com/Mbaitedero/bigdata-anomaly-detection.git
cd bigdata-anomaly-detection

# Infrastructure
docker compose up -d
docker compose ps        # kafka et namenode : healthy

# Environnement Python
python -m venv venv
source venv/Scripts/activate     # Windows (Git Bash) ; Linux/macOS : source venv/bin/activate
pip install -r requirements.txt

# Référentiels (déterministes, seed = 42)
python generator/make_reference.py

# Chargement dans HDFS
docker exec namenode hdfs dfs -mkdir -p /reference
docker cp data/reference/customers.csv namenode:/tmp/customers.csv
docker cp data/reference/products.csv namenode:/tmp/products.csv
docker exec namenode hdfs dfs -put -f /tmp/customers.csv /reference/
docker exec namenode hdfs dfs -put -f /tmp/products.csv /reference/
docker exec namenode hdfs dfs -ls /reference
```

Interface web du namenode : http://localhost:9870

Vérifier le topic :

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --describe --topic transactions
```

Remise à zéro complète : `docker compose down -v`.

### Windows (Git Bash)

Git Bash convertit les chemins comme `/opt/...` avant de les passer à Docker. Si `docker exec` échoue avec un chemin `C:/Program Files/Git/...` :

```bash
export MSYS_NO_PATHCONV=1
```

## Schéma des transactions

```json
{
  "transaction_id": "uuid",
  "customer_id": "C00042",
  "product_id": "P0017",
  "category": "electronics",
  "amount": 249.90,
  "quantity": 1,
  "city": "Casablanca",
  "timestamp": "2026-11-10T14:32:05Z"
}
```

Aucun champ `is_anomaly` : les anomalies injectées sont listées dans un fichier de vérité terrain séparé, jamais lu par la détection.

Types d'anomalies injectées : montant extrême, quantité aberrante, rafale d'un client, pic horaire, ville inhabituelle.

## Résultats mesurés

Aucun chiffre n'est publié avant d'avoir été mesuré.

| Indicateur | Valeur |
|---|---|
| Débit d'ingestion soutenu | à mesurer |
| Taille moyenne des fichiers HDFS | à mesurer |
| Volume traité | à mesurer |
| Rappel / précision, méthodes A, B, C | à mesurer |
| MapReduce contre Spark | à mesurer |

## Structure du dépôt

```
docker-compose.yml      Kafka (KRaft) + HDFS
hadoop-conf/            configuration HDFS (core-site.xml, hdfs-site.xml)
generator/              configuration, référentiels, générateur de transactions
ingestion/              consommateur Kafka → HDFS
processing/             PySpark et job MapReduce Java
evaluation/             précision, rappel, F1 contre la vérité terrain ; benchmark
reporting/              rapport et figures
data/reference/         customers.csv, products.csv
data/ground_truth/      vérité terrain (hors Git)
docs/                   décisions techniques, architecture
tests/                  tests unitaires
```

Les choix techniques et leurs justifications sont dans [`docs/decisions.md`](docs/decisions.md).

## Limites connues

- Traitement **batch**, pas temps réel.
- Un seul broker Kafka et un seul datanode : aucune tolérance aux pannes, réplication à 1.
- HDFS exécuté en root, permissions désactivées : configuration de démonstration locale, à ne pas utiliser en production.
- Données synthétiques : une anomalie statistique n'est pas une fraude avérée.