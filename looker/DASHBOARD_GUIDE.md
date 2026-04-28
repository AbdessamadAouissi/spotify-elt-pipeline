# Spotify ELT — Looker Studio Dashboard (Guide A → Z)

> **Projet GCP** : `smart-charter-301917`
> **Dataset Gold** : `spotify_gold`
> **URL Looker Studio** : https://lookerstudio.google.com

---

## 1. Connexion BigQuery → Looker Studio

### 1.1 Créer les sources de données

1. Ouvre https://lookerstudio.google.com
2. Clic **"Créer" → "Rapport"**
3. Cherche **"BigQuery"** dans le panneau "Ajouter des données"
4. **Autorise** l'accès à ton compte Google (celui lié à GCP)

#### Source 1 — `track_streams` (principale)
| Champ | Valeur |
|---|---|
| Projet GCP | `smart-charter-301917` |
| Dataset | `spotify_gold` |
| Table | `track_streams` |
| Nom affiché | `Spotify — Streams` |

#### Source 2 — `top_artists`
| Champ | Valeur |
|---|---|
| Dataset | `spotify_gold` |
| Table | `top_artists` |
| Nom affiché | `Spotify — Artistes` |

#### Source 3 — `album_trends`
| Champ | Valeur |
|---|---|
| Dataset | `spotify_gold` |
| Table | `album_trends` |
| Nom affiché | `Spotify — Albums` |

#### Source 4 — `playlist_insights`
| Champ | Valeur |
|---|---|
| Dataset | `spotify_gold` |
| Table | `playlist_insights` |
| Nom affiché | `Spotify — Playlists` |

> 💡 **Ne pas activer "autodetect date range"** sur les sources album_trends et top_artists (pas de champ date compatible).

---

## 2. Configuration des champs (par source)

### 2.1 Source `Spotify — Streams`

Après avoir sélectionné la table, clique sur **"Modifier la source"** et configure :

| Champ | Type Looker | Agrégation par défaut |
|---|---|---|
| `chart_date` | Date | — |
| `chart_year` | Nombre | — |
| `chart_month` | Nombre | — |
| `year_month` | Texte | — |
| `country` | Texte (Géo : Pays) | — |
| `chart_type` | Texte | — |
| `rank` | Nombre | Moyenne |
| `peak_rank` | Nombre | Min |
| `weeks_on_chart` | Nombre | Max |
| `track_name` | Texte | — |
| `artist_names` | Texte | — |
| `artist_name` | Texte | — |
| `duration_min` | Nombre | Moyenne |
| `explicit` | Booléen | — |
| `streams` | Nombre | Somme |
| `cumulative_streams` | Nombre | Max |
| `pct_monthly_streams` | Pourcentage | Moyenne |

> 💡 Pour `country` : dans "Type de données", choisis **"Géo → Pays"** pour activer la carte.

### 2.2 Champs calculés à créer dans Looker Studio

Dans la source `Spotify — Streams`, clique **"Ajouter un champ"** :

#### Champ : `Streams (M)`
```
streams / 1000000
```
Type : Nombre — Décimales : 2 — Suffixe : `M`

#### Champ : `Top 10 ?`
```
CASE WHEN rank <= 10 THEN "Top 10" ELSE "11-200" END
```
Type : Texte

#### Champ : `Semaine`
```
WEEK(chart_date)
```
Type : Nombre

#### Champ : `Mois label`
```
FORMAT_DATETIME("%b %Y", chart_date)
```
Type : Texte

---

## 3. Architecture du rapport (5 pages)

```
Page 1 — Vue d'ensemble Streams
Page 2 — Top Artistes
Page 3 — Top Titres
Page 4 — Tendances Albums
Page 5 — Playlists
```

---

## 4. Page 1 — Vue d'ensemble Streams

**Source** : `Spotify — Streams`

### Filtres globaux de la page (en-tête)

Ajoute ces contrôles en haut de page :

| Widget | Champ | Position |
|---|---|---|
| Liste déroulante | `country` | Haut gauche |
| Sélecteur de date | `chart_date` | Haut centre |
| Liste déroulante | `chart_type` | Haut droite |

**Configuration du sélecteur de date :**
- Plage par défaut : **Personnalisée**
- Date début : `2026-01-01`
- Date fin : `2026-04-09` (ou "Aujourd'hui")

---

### Bloc KPIs (ligne 1)

Crée **4 tableaux de bord scorecards** côte à côte :

#### KPI 1 — Total Streams
- Widget : **Carte de résultats (Scorecard)**
- Métrique : `streams` — Agrégation : Somme
- Libellé : `Total Streams`
- Format : `#,##0` (séparateur de milliers)

#### KPI 2 — Titres uniques
- Métrique : **Champ calculé** :
  ```
  COUNT_DISTINCT(track_id)
  ```
- Libellé : `Titres uniques`

#### KPI 3 — Pays couverts
- Métrique : **Champ calculé** :
  ```
  COUNT_DISTINCT(country)
  ```
- Libellé : `Pays`

#### KPI 4 — Semaines d'historique
- Métrique : **Champ calculé** :
  ```
  COUNT_DISTINCT(chart_date)
  ```
- Libellé : `Semaines`

---

### Graphique — Évolution des streams dans le temps

- Widget : **Graphique en courbes**
- Dimension : `chart_date`
- Métrique : `streams` (Somme)
- Dimension de répartition : `country` (optionnel — activer si < 5 pays filtrés)
- Tri : `chart_date` croissant
- Style : couleur `#1DB954` (vert Spotify)

---

### Carte géographique — Streams par pays

- Widget : **Carte géographique**
- Dimension géo : `country` (type Géo : Pays)
- Métrique : `streams` (Somme)
- Palette de couleur : Blanc → `#1DB954`
- Zoom par défaut : **Europe** (vu que les data sont France)

---

### Tableau — Top 20 titres de la semaine

- Widget : **Tableau**
- Source filtrée : ajouter filtre `chart_date = MAX(chart_date)`
  *(ou utiliser le filtre de date global)*
- Dimensions : `rank`, `track_name`, `artist_names`, `country`
- Métriques : `streams`, `weeks_on_chart`, `peak_rank`
- Tri : `rank` croissant
- Lignes max : 20
- Pagination : désactivée

---

## 5. Page 2 — Top Artistes

**Source principale** : `Spotify — Streams`
**Source secondaire** : `Spotify — Artistes`

### Filtres de la page
- Liste déroulante : `country`
- Sélecteur de date : `chart_date`

---

### Bar chart horizontal — Top 15 artistes par streams

- Widget : **Graphique à barres (horizontal)**
- Dimension : `artist_name`
- Métrique : `streams` (Somme)
- Tri : `streams` décroissant
- Nombre de barres : 15
- Couleur des barres : `#1DB954`
- Afficher valeurs : Oui

---

### Tableau détaillé artistes

- Widget : **Tableau avec barres de données**
- Dimensions : `artist_name`
- Métriques :
  - `streams` (Somme) — avec barre de données
  - `COUNT_DISTINCT(track_id)` → renommer "Titres chartés"
  - `weeks_on_chart` (Max) → "Max semaines classé"
  - `peak_rank` (Min) → "Meilleur rang"
- Tri : `streams` décroissant
- Lignes : 20

---

### Graphique camembert — Part de marché artistes (Top 10)

- Widget : **Graphique circulaire**
- Dimension : `artist_name`
- Métrique : `streams` (Somme)
- Nombre max de tranches : 10
- Regrouper autres : "Autres"
- Palette : dégradé vert Spotify

---

### Graphique courbes — Évolution rang artiste dans le temps

- Widget : **Graphique en courbes**
- Dimension : `chart_date`
- Dimension de répartition : `artist_name`
- Métrique : `rank` (Moyenne)
- Axe Y inversé : Oui *(rang 1 = haut du graphique)*
- Filtre interne : `rank <= 10`
- Tri : `chart_date` croissant

---

## 6. Page 3 — Top Titres

**Source** : `Spotify — Streams`

### Filtres
- Liste déroulante : `country`
- Liste déroulante : `artist_name`
- Sélecteur de date : `chart_date`
- Boutons radio : `chart_type` (regional / viral)

---

### Tableau principal — Tous les titres

- Widget : **Tableau paginé**
- Dimensions : `track_name`, `artist_names`, `country`
- Métriques :
  - `streams` (Somme)
  - `rank` (Min) → "Meilleur rang"
  - `peak_rank` (Min)
  - `weeks_on_chart` (Max)
  - `duration_min` (Moy)
  - `explicit` (Mode)
- Tri : `streams` décroissant
- Lignes par page : 25

---

### Graphique — Évolution d'un titre

- Widget : **Graphique en courbes**
- Dimension : `chart_date`
- Métrique : `streams` (Somme)
- Dimension de répartition : `track_name`
- Filtre interne : `rank <= 5`
- Titre : "Top 5 titres — évolution hebdomadaire"

---

### Scorecard — Titre #1 toutes semaines confondues

- Widget : **Scorecard avec comparaison**
- Métrique : `streams` (Somme)
- Filtre interne : `rank = 1`
- Libellé : "Streams cumulés — #1"

---

## 7. Page 4 — Tendances Albums

**Source** : `Spotify — Albums`

### Graphique — Sorties d'albums par mois

- Widget : **Graphique à barres empilées**
- Dimension : `year_month`
- Dimension de répartition : `album_type`
- Métrique : `album_count` (Somme)
- Tri : `year_month` croissant
- Couleurs par type :
  - `album` : `#1DB954`
  - `single` : `#535353`
  - `compilation` : `#B3B3B3`

---

### Graphique — Nombre moyen de titres par album

- Widget : **Graphique en courbes**
- Dimension : `year_month`
- Métrique : `avg_tracks_per_album` (Moyenne)
- Filtre : `album_type = 'album'`
- Titre : "Longueur moyenne des albums"

---

### Tableau — Top artistes productifs

- Widget : **Tableau**
- Dimensions : `primary_artist_name`
- Métriques :
  - `album_count` (Somme) → "Nb albums"
  - `avg_tracks_per_album` (Moy)
  - `release_year` (Min) → "1ère sortie"
  - `release_year` (Max) → "Dernière sortie"
- Tri : `album_count` décroissant

---

## 8. Page 5 — Playlists

**Source** : `Spotify — Playlists`

### Donut — Répartition par taille

- Widget : **Graphique circulaire (donut)**
- Dimension : `size_bucket`
- Métrique : `playlist_count` (Somme)
- Centre : `pct_of_total` (Moy)

---

### Tableau — Analyse par segment

- Widget : **Tableau avec barres**
- Dimensions : `size_bucket`, `is_public`, `is_collaborative`
- Métriques :
  - `playlist_count`
  - `avg_tracks`
  - `median_tracks`
  - `unique_owners`
  - `pct_of_total`
- Tri : `avg_tracks` décroissant

---

### Scorecard x3 — Métriques clés playlists

| KPI | Champ calculé |
|---|---|
| Total playlists | `SUM(playlist_count)` |
| % playlists publiques | `SUM(CASE WHEN is_public THEN playlist_count ELSE 0 END) / SUM(playlist_count) * 100` |
| Taille médiane | `MEDIAN(median_tracks)` |

---

## 9. Style global du rapport

### Thème à appliquer

Dans **Thème et mise en page** (menu du rapport) :

| Paramètre | Valeur |
|---|---|
| Couleur d'arrière-plan | `#121212` (noir Spotify) |
| Police titre | **Montserrat Bold** |
| Police corps | **Roboto** |
| Couleur principale | `#1DB954` (vert Spotify) |
| Couleur secondaire | `#535353` |
| Couleur texte | `#FFFFFF` |
| Couleur texte secondaire | `#B3B3B3` |

### En-tête du rapport

Crée un rectangle en haut de chaque page :
- Hauteur : 60px
- Couleur fond : `#1DB954`
- Texte : **SPOTIFY ANALYTICS** (blanc, Montserrat Bold 24px)
- Logo : image Spotify (optionnel)

---

## 10. Rafraîchissement des données

### Fréquence recommandée
- Looker Studio rafraîchit les sources BigQuery **à la demande** (bouton "Actualiser les données")
- Pour un refresh automatique : **menu Ressources → Actualisation des données** → toutes les 4 heures

### Mise à jour hebdomadaire des charts

Chaque lundi matin :
1. Télécharge les nouveaux CSV depuis Spotify Charts
2. Dépose dans `charts/`
3. Lance le loader :
   ```bash
   cd C:/Users/abdes/Downloads/spotify-elt-pipeline
   .venv_dbt\Scripts\activate
   python loaders/charts_loader.py --charts-dir charts/
   ```
4. Relance dbt Gold :
   ```bash
   set GCP_PROJECT_ID=smart-charter-301917
   cd dbt
   dbt run --select gold.track_streams
   ```
5. Clique **"Actualiser les données"** dans Looker Studio

---

## 11. Partage du rapport

1. Bouton **"Partager"** en haut à droite
2. Option **"Obtenir le lien"** → "Lecteur"
3. Ou **"Intégrer"** pour insérer dans un site web

---

## 12. Checklist finale

- [ ] Source `track_streams` connectée et champs configurés
- [ ] Source `top_artists` connectée
- [ ] Source `album_trends` connectée
- [ ] Source `playlist_insights` connectée
- [ ] Page 1 — Streams créée avec KPIs + carte + tableau
- [ ] Page 2 — Artistes créée avec bar chart + tableau
- [ ] Page 3 — Titres créée avec tableau paginé
- [ ] Page 4 — Albums créée avec barres empilées
- [ ] Page 5 — Playlists créée avec donut
- [ ] Thème Spotify appliqué (fond noir, vert #1DB954)
- [ ] Filtres globaux (country, date) sur chaque page
- [ ] Rapport partagé / lien copié
