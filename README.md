# 🎧 Rap Italian Songs – Data Mining Project  

**University of Pisa – Department of Computer Science**  
**Academic Year:** 2025/2026  

## 👥 Authors

- Abderrahmane Salmi  
- Ricardo Talarico  
- Lorenzo Allegrini  

---

# 📌 Project Overview

This repository contains the full implementation of our Data Mining project on **Italian rap songs**.  

The goal of the project is to apply data mining methodologies to analyze musical, lyrical, geographical, and temporal patterns in a dataset of over **11,000 Italian rap tracks** and **100+ artists**.

The project is structured into five main tasks:

1. **Data Understanding & Preparation**
2. **Clustering Analysis**
3. **Predictive Analysis & Explainable AI**
4. **Time Series Analysis on Audio**
5. **Ethical & Legal Implications**

All analyses are implemented in **Python**, using Jupyter notebooks and modularized utilities.

---

# 📂 Repository Structure

```text
.
├── datasets/          # Original raw datasets
├── task1/             # Data Understanding & Preparation
├── task2/             # Clustering Analysis
├── task3/             # Predictive Analysis & XAI
└── task4/             # Time Series Analysis (Audio)
```

---

# 📊 Dataset Description

The dataset is composed of two CSV files:

### 🎵 Tracks Dataset (~11,000 songs, 44+ features)

Includes:

- Track metadata (title, album, release date)
- Lyrics
- Audio features (bpm, centroid, rolloff, flux, loudness, etc.)
- Popularity metrics
- Linguistic statistics
- Explicit content indicators

### 👤 Artists Dataset (~100 artists, 14 features)

Includes:

- Birth date and birthplace
- Region & province
- Gender
- Career start
- Geographic coordinates

---

# 🧠 Task Summary

---

## 1️⃣ Data Understanding & Preparation

### ✔ Data Quality Protocol
- Structural integrity checks
- Temporal consistency validation
- Text quality filtering
- Domain-based logical constraints

### ✔ Missing Value Handling
Two-phase strategy:

1. **External Retrieval**
   - Spotify API (Spotipy)
   - MusicBrainz
   - Wikidata
2. **Hierarchical Statistical Imputation**
   - Album mean
   - Artist mean
   - Global fallback

### ✔ Feature Engineering

Examples:

- `swear_ratio`
- `aggressiveness`
- `relative_popularity`
- `artist_age_at_release`
- `release_season`
- `has_collaboration`

### ✔ Outlier Detection
- IQR-based univariate detection
- Isolation Forest (multivariate)
- Winsorization + Scaling

---

## 2️⃣ Clustering Analysis

We explored multiple clustering paradigms.

- K-Means
- DBSCAN
- Hierarchical
- X-Means

---

## 3️⃣ Predictive Analysis & XAI

### 🎯 Objective
Predict macro-region (school) of the artist.

Classes: Campania, Lazio, Lombardia, North, South

### 🏆 Final Model
**Explainable Boosting Machine (EBM)**

Performance:

| Set | Balanced Accuracy | F1 Weighted |
|------|------------------|-------------|
| Validation | 0.63 | 0.67 |
| Test | 0.62 | 0.66 |

---

## 4️⃣ Time Series Analysis

We analyzed raw MP3 audio signals.

### ✔ Embeddings Compared
- MFCC
- Wav2Vec2
- MERT
- CLAP
- Hybrid (MERT + Wav2Vec2)

### ✔ Clustering Approaches
- Static (Global Average Pooling)
- Dynamic (TimeSeriesKMeans)

### ✔ Shapelet Analysis
- Random Shapelet Transform
- Multivariate Learning Shapelets

Best Balanced Accuracy (Shapelet RF): **0.71**

---

## 5️⃣ Ethical & Legal Considerations

We discuss:

- Copyright constraints on lyrics
- API usage and scraping policies
- GDPR compliance
- Regional bias risk in predictive models
- Biometric risks in deep audio embeddings
- Privacy-by-design in shapelet abstraction
