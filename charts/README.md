# Spotify Charts — Données de streams

Placez ici les fichiers CSV téléchargés depuis **charts.spotify.com**.

## Comment télécharger

1. Aller sur https://charts.spotify.com/charts/overview/fr
2. Se connecter avec votre compte Spotify
3. Sélectionner le pays et la semaine souhaitée
4. Cliquer sur **"Download CSV"** (icône en haut à droite)
5. Placer le fichier téléchargé dans ce dossier

## Format du nom de fichier

```
regional-fr-weekly-2024-01-04--2024-01-11.csv
regional-fr-weekly-2024-01-11--2024-01-18.csv
...
```

## Format du CSV

| Colonne | Description |
|---|---|
| `rank` | Position dans le classement |
| `uri` | URI Spotify du titre (ex: spotify:track:xxx) |
| `artist_names` | Noms des artistes |
| `track_name` | Titre de la chanson |
| `streams` | **Nombre de streams sur la semaine** |
| `peak_rank` | Meilleure position atteinte |
| `weeks_on_chart` | Nombre de semaines dans le classement |

## Charger dans BigQuery

```bash
python -m loaders.charts_loader
```

Les données seront chargées dans `spotify_silver.spotify_charts` et
disponibles dans le modèle Gold `track_streams` pour Looker Studio.

## Pays disponibles

Téléchargez les charts pour plusieurs pays pour comparaison :
- `fr` — France
- `be` — Belgique  
- `ch` — Suisse
- `global` — Mondial
