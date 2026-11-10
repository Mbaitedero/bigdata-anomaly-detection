# Décisions techniques

Chaque décision : contexte, choix, justification. Ce fichier est mis à jour à chaque étape.

## 1. Infrastructure

### D1. Kafka en mode KRaft, un seul nœud
- **Choix** : image `apache/kafka:3.9.0`, rôles `broker,controller` sur un même nœud, sans ZooKeeper.
- **Pourquoi** : KRaft est le mode standard actuel et simplifie l'infrastructure (un conteneur de moins). Un seul nœud suffit pour un projet local ; la réplication est donc fixée à 1 partout.
- **Limite** : aucune tolérance aux pannes côté Kafka.

### D2. Deux listeners Kafka (INTERNAL et EXTERNAL)
- **Choix** : `INTERNAL://kafka:19092` pour les conteneurs, `EXTERNAL://localhost:9092` pour les programmes lancés sur la machine hôte (producteur Python).
- **Pourquoi** : le broker annonce une adresse aux clients. Avec une seule adresse, soit les conteneurs, soit l'hôte ne peuvent pas s'y connecter.

### D3. Topic créé explicitement, 3 partitions, auto-création désactivée
- **Choix** : service `kafka-init` qui crée `transactions` (3 partitions, réplication 1) ; `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.
- **Pourquoi** : éviter un topic créé par erreur avec les valeurs par défaut. Trois partitions permettent le parallélisme du consommateur tout en restant raisonnables pour un seul broker.
- **Clé de message prévue** : `customer_id`, pour que les transactions d'un même client restent ordonnées dans une même partition.

### D4. HDFS : 1 namenode + 1 datanode, réplication 1
- **Choix** : image `apache/hadoop:3.3.6`, `dfs.replication=1`.
- **Pourquoi** : un seul datanode ; avec le facteur par défaut (3), tous les blocs seraient signalés sous-répliqués.
- **Limite** : pas de tolérance aux pannes de stockage. Acceptable pour un environnement de démonstration.

### D5. Services HDFS exécutés en root
- **Contexte** : le formatage du namenode échouait avec `storage directory does not exist or is not accessible`. Les volumes Docker nommés appartiennent à root, alors que l'image lance HDFS avec un utilisateur non-root.
- **Choix** : `user: root` et variables `HDFS_NAMENODE_USER` / `HDFS_DATANODE_USER`.
- **Limite** : à ne pas reproduire en production. Mentionné dans le README.

### D6. Permissions HDFS désactivées, WebHDFS activé
- **Choix** : `dfs.permissions.enabled=false`, `dfs.webhdfs.enabled=true`.
- **Pourquoi** : en local, évite les erreurs de droits ; WebHDFS permettra au consommateur Python d'écrire dans HDFS par HTTP sans client Hadoop installé.
- **Limite** : l'écriture WebHDFS depuis l'hôte redirige vers le datanode par son nom d'hôte (`datanode:9864`). À traiter à l'étape du consommateur (consommateur dans Docker, ou port exposé et entrée dans le fichier hosts).

### D7. Mémoire limitée par service
- **Choix** : `HADOOP_HEAPSIZE_MAX=512m` pour le namenode et le datanode.
- **Pourquoi** : Docker Desktop disposait de 3,76 Go par défaut. Les limites permettent à Kafka, HDFS et plus tard Spark de cohabiter. Allocation WSL2 portée à 6 Go via `.wslconfig` sur la machine de développement.

### D8. Volumes nommés
- **Choix** : `kafka-data`, `namenode-data`, `datanode-data`.
- **Pourquoi** : les données survivent à `docker compose down`. Remise à zéro avec `docker compose down -v`.

## 2. Stockage et données

### D9. HDFS comme seule couche de stockage des données volumineuses
- **Choix** : transactions, historique, résultats (zone `/curated/`) dans HDFS uniquement. Pas de DVC.
- **Pourquoi** : le projet a pour objet l'utilisation de HDFS. Ajouter un second système de stockage ferait doublon et alourdirait le démarrage pour un lecteur du dépôt (« trop de technologies »).

### D10. Référentiels versionnés dans Git et copiés dans HDFS
- **Choix** : `data/reference/customers.csv` (3 000 clients, ~72 Ko) et `products.csv` (200 produits, ~7 Ko) dans Git ; copie dans HDFS `/reference/` pour les jointures Spark.
- **Pourquoi** : fichiers minuscules et déterministes ; le clone fonctionne sans étape supplémentaire. Les petits fichiers se prêtent à une jointure broadcast.

### D11. Graine aléatoire fixe (`seed: 42`)
- **Pourquoi** : données reproductibles, donc résultats d'évaluation comparables d'une exécution à l'autre.

### D12. Prix de référence log-normaux par catégorie
- **Choix** : chaque catégorie a un prix médian et une dispersion propres (électronique ~300, alimentation ~8).
- **Pourquoi** : les montants réels sont à longue traîne et varient fortement selon la catégorie. Cela justifie une comparaison honnête entre z-score classique et approche robuste (médiane et MAD).

### D13. `home_city` dans les référentiels clients
- **Pourquoi** : rend la « ville inhabituelle » détectable à partir d'une information légitime, disponible à la détection, contrairement à la vérité terrain.

### D14. Vérité terrain strictement séparée
- **Choix** : identifiants des anomalies injectées écrits dans `data/ground_truth/`, exclu de Git, jamais lu par les algorithmes de détection ; aucun champ `is_anomaly` dans les messages.
- **Pourquoi** : sinon l'évaluation (précision, rappel, F1) n'a aucune valeur.

## 3. Environnement de développement

### D15. Git Bash sous Windows : conversion des chemins
- **Contexte** : `docker exec ... /opt/...` échouait car Git Bash convertit `/opt/...` en `C:/Program Files/Git/opt/...`.
- **Choix** : `export MSYS_NO_PATHCONV=1` (dans `~/.bashrc`) ou préfixe `//` devant les chemins.

## 4. Décisions à documenter

À compléter quand elles seront prises, avec les chiffres mesurés : paramètres de buffering du consommateur (taille ou durée, objectif de fichiers de plusieurs dizaines de Mo), pourquoi Parquet, pourquoi MAD (ou IQR), choix des seuils de détection, comparaison MapReduce et Spark.