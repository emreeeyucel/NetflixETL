from pymongo import MongoClient
import pandas as pd
import numpy as np


conn = MongoClient('mongodb://localhost:27017/')
etl = conn['Netflix_ETL']

collection = etl['director_tv_shows']
collection2 = etl['country_cast_counts']
collection3 = etl['actor_director_stats']


data = pd.read_csv(filepath_or_buffer='data/netflix_titles.csv',
                   encoding='utf-8')

# region Data Kontrolleri
print(data.shape)
print(data.info())
print(data.isnull().sum())
# endregion



# region Boş değerlerimizi "Unknown" ile dolduralım
data['director'] = data['director'].fillna(value='Unkown')
data['cast'] = data['cast'].fillna(value='Unkown')
data['country'] = data['country'].fillna(value='Unkown')
data['date_added'] = data['date_added'].fillna(value='Unkown')
data['rating'] = data['rating'].fillna(value='Unkown')
data['duration'] = data['duration'].fillna(value='Unkown')

print(data.isna().sum())
# endregion




# region Hangi yönetmenin çalışmaları daha çok TV Show olarak kategorize edilmiş ve hangi yıllarda yayımlanmış?


tv_shows = data[data['type'] == 'TV Show'].copy()
tv_shows['director'] = tv_shows['director'].str.split(',').explode('director')

director_tv_show = (
    tv_shows.groupby(['director', 'release_year'])
    .size()
    .reset_index(name='show_count')
)


total_tv_shows = (
    director_tv_show.groupby('director')['show_count']
    .sum()
    .reset_index(name='total_shows')
    .sort_values(by='total_shows', ascending=False)
)

director_tv_show = director_tv_show.merge(total_tv_shows, on='director')


filtered_data = director_tv_show[director_tv_show['director'] != 'Unkown']


sorted_data = filtered_data.sort_values(
    by=['total_shows', 'release_year'], ascending=[False, True]
)

top_director_years_dict = sorted_data.to_dict(orient='records')
collection.insert_many(top_director_years_dict)
print(sorted_data.to_string())

# endregion




# region En Çok Oyuncuya Sahip Ülkeler
df = data[['cast', 'country']].copy()

df['cast'] = df['cast'].str.split(',')
df['country'] = df['country'].str.split(',')
df = df.explode('cast')
df = df.explode('country')

df = df[(df['cast'] != 'Unkown') & (df['country'] != 'Unkown')]

country_unique_cast_count = df.groupby('country')['cast'].nunique().reset_index(name='count').sort_values(by='count', ascending=False)
print(country_unique_cast_count)

# MongoDB'ye yükleme
country_unique_cast_count_dict = country_unique_cast_count.to_dict(orient='records')
collection2.insert_many(country_unique_cast_count_dict)
print(f'{len(country_unique_cast_count_dict)} records inserted into MongoDB.')
# endregion




# region  Bir Oyuncunun Yıl Bazında En Çok Çalıştığı Yönetmen

movie_data = data[['director', 'release_year', 'cast']].copy()
movie_data['cast'] = movie_data['cast'].str.split(',').explode('cast')
movie_data['director'] = movie_data['director'].str.split(',').explode('director')
movie_data = movie_data[(movie_data['cast'] != 'Unkown') & (movie_data['release_year'] != 'Unkown') & (movie_data['director'] != 'Unkown')]
actor_director_stats = movie_data.groupby(['cast', 'release_year', 'director']).size().reset_index(name='count')
print(actor_director_stats.sort_values(by='cast', ascending=True).to_string())
actor_director_stats_dict = actor_director_stats.to_dict(orient='records')
collection3.insert_many(actor_director_stats_dict)
print(f'{len(actor_director_stats_dict)} records inserted into MongoDB.')
# endregion


