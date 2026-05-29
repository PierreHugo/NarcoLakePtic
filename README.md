# 🌿 NarcoLakePtic

> A Data Lake project analyzing global drug trends using open public data.

![Python](https://img.shields.io/badge/Python-3.11+-green)
![Prefect](https://img.shields.io/badge/Orchestration-Prefect-blue)
![MinIO](https://img.shields.io/badge/Storage-MinIO-purple)
![Docker](https://img.shields.io/badge/Infra-Docker-orange)

NarcoLakePtic is a Data Lake pipeline that ingests, transforms and exposes
drug-related data from official sources (EMCDDA, UNODC, Global Drug Survey).
Built by 3 students at EFREI Paris — Big Data & Machine Learning track.

---

## Architecture

```
Raw (MinIO BLOB) → Staging (cleaning & parsing) → Curated (analytics-ready)
Orchestration: Prefect
```

---

## Project structure

```
NarcoLakePtic/
├── data/
│   ├── raw/         # données brutes (MinIO)
│   ├── staging/     # données nettoyées
│   └── curated/     # données prêtes à l'analyse
├── ingestion/       # scripts API & téléchargement datasets
├── flows/           # Prefect flows & tasks
├── processing/      # transformations staging → curated
├── notebooks/       # exploration & visualisation
├── docs/
├── .env.example
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## Data sources

| Source | Type | Contenu |
|---|---|---|
| EMCDDA | API officielle | Données EU sur consommation, saisies, prix |
| UNODC | CSV / JSON | Données mondiales ONU sur production & trafic |
| Global Drug Survey | Dataset annuel | Habitudes de consommation par pays |

---

## Quick start

```bash
git clone https://github.com/PierreHugo/NarcoLakePtic
cd NarcoLakePtic
cp .env.example .env
docker-compose up -d
pip install -r requirements.txt
prefect server start
```

---

## Team

EFREI Paris — ING2-APP-BDML1, 2025-2026
- [Pierre-Hugo HERRAN](github.com/PierreHugo)
- [Uthum UKWATTAGE](github.com/uthumatik)
- [Aymen Alloune](github.com/AymenShe)

> Public data only. For academic purposes.
