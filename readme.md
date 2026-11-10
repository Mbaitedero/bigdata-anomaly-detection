# Plateforme Big Data de détection de transactions atypiques

Pipeline Kafka → HDFS → traitement batch (PySpark / MapReduce) → détection → évaluation chiffrée.

> 🚧 En cours de construction. Avancement ci-dessous.

## Architecture
_(schéma à venir : docs/architecture.png)_

## Avancement
- [ ] Semaine 1 : infrastructure et ingestion
- [ ] Semaine 2 : traitement batch et statistiques
- [ ] Semaine 3 : détection, MapReduce vs Spark, évaluation
- [ ] Semaine 4 : rapport, CI, packaging

## Résultats mesurés
| Indicateur | Valeur |
|---|---|
| Débit d'ingestion | à mesurer |
| Taille moyenne des fichiers HDFS | à mesurer |

## Limites
Traitement batch (pas temps réel) ; une transaction atypique n'est pas une preuve de fraude.