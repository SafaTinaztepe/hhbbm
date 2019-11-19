#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import boto3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import re
import multiprocessing as mp
import time


# In[21]:


def request_song_info(song_title, artist_name):
    endpoint = 'https://api.genius.com'
    headers = {'Authorization': 'Bearer ' + 'zF2i3MJqmrSI0cNQB9L35hMvAAmfgvSkrmh4A2UQx8IrHQiTORrgwBkaEF4e2K9B'}
    search_url = endpoint + '/search'
    data = {'q': song_title + ' ' + artist_name}
    response = requests.get(search_url, data=data, headers=headers)
    for hit in response.json()['response']['hits']:
        hit = hit['result']
        if song_title == hit['title']:
            return hit['url']

    return None

def scrape(url):
    page = requests.get(url)
    html = BeautifulSoup(page.text, 'html.parser')
    lyrics = html.find('div', class_='lyrics').get_text()

    return lyrics

def get_basketball_players():
    players = []
    endpoint = "https://www.basketball-reference.com/players/"
    for letter in range(ord('a'), ord('z')+1):
        reference_page = requests.get(endpoint + chr(letter))
        reference_html = BeautifulSoup(reference_page.text, 'html.parser')
        table = reference_html.find('table')
        if table:
            for row in table.findAll('tr'):
                if row.find('a'):
                    player = row.find('a').text
                    players.append(player)
                    
    return players

def get_artists_or_groups():
    endpoint = "https://en.wikipedia.org/wiki/List_of_hip_hop_musicians"
    page = requests.get(endpoint)
    html = BeautifulSoup(page.text, 'html.parser')
    artists = []
    for li in html.findAll('li'):
        if li.find('a') and li.find('a').get('title'):
            text = li.find('a').get('title')
            artists.append( text )
            
    endpoint = "https://en.wikipedia.org/wiki/List_of_hip_hop_groups"
    page = requests.get(endpoint)
    html = BeautifulSoup(page.text, 'html.parser')
    for li in html.findAll('li'):
        if li.find('a') and li.find('a').get('title'):
            text = li.find('a').get('title')
            artists.append( text )
            
    return artists

def poll(queue):
    with open('queue.txt', 'w') as infile:
        while True:
            message = queue.get()
            if m == '-1':
                infile.write('killed')
                break
            else:
                infile.write(",".join(m) + "\n")
                infile.flush()

def run(artist):
    results = sp.search(q=artist, limit=50)
    for item in results['tracks']['items']:
        song = item['name']
        if song in songs:
            continue 
        else:
            songs.append(players)
        response = request_song_info(song, artist)
        if not response: continue
        lyrics_list = []
        lyrics = scrape(response).strip().replace('\n', '. ')
        if len(lyrics.encode('utf-8')) >= 5000:
            lyrics_list = [" ".join(lyrics.split()[:len(lyrics.split())//2]), " ".join(lyrics.split()[len(lyrics.split())//2:])]
        else:
            lyrics_list.append(lyrics)
        try:
            result_list = comprehend.batch_detect_entities(TextList=lyrics_list, LanguageCode='en')['ResultList']
        except:
            continue
        for result in result_list:
            entities = result['Entities']
            shoutout = list(filter(lambda entity: entity['Type'] == 'PERSON' and entity['Text'] in players, entities))
            for so in shoutout:
                if so["Text"] == "Michael Jackson":
                    continue
                shoutouts.append( (artist, song, so["Text"]) )
                queue.put((artist,so["Text"],song,mp.current_process().pid))
                print(f'{artist},{so["Text"]},{song},{mp.current_process().pid},{time.time()}')


# In[18]:


comprehend = boto3.client('comprehend')
client_credentials_manager = SpotifyClientCredentials(client_id="817e183d515b4347b9e32487d263a242", client_secret="1734045840e945a1aef748a660caa6b4")
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


# In[ ]:


artists = get_artists_or_groups()
shoutouts = []
players = get_basketball_players()
songs = []
print("artist,player,song,pid,time")
cpu_count = mp.cpu_count()
manager = mp.Manager()
queue = manager.Queue()
with mp.Pool( cpu_count + 2 ) as p:
    watcher = p.apply_async(poll, (queue,))
    p.map(run, artists)
queue.put('-1')


# In[ ]:




