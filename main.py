import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

session = requests.Session()


def get_html(url):
    with session.get(url) as response:
        response.raise_for_status()
        response.encoding = 'utf8'
        return response.text


def extract_artists_data(html):
    soup = BeautifulSoup(html, 'lxml')
    td_elements = soup.find_all('td', {'class': 'text'})

    artists_data = []
    for td in td_elements:
        artist_name = td.find('a').text.strip()
        artist_href = urljoin(url, td.find('a')['href'])
        artists_data.append({'name': artist_name, 'href': artist_href})

    return artists_data


def process_artist(artist):
    art_name = artist['name']
    art_url = artist['href']

    try:
        art_html = get_html(art_url)
        art_soup = BeautifulSoup(art_html, 'lxml')

        table = art_soup.find('table', {'class': 'addpos'})
        rows = table.find_all('tr')

        in_art_str = []
        for i in range(1, len(rows)):
            cols = rows[i].find_all('td')
            song_title = rows[i].find('a').text.strip()
            song_url = rows[i].find('a')['href']
            streams = int(cols[1].text.replace(',', ''))
            if streams > 100000000:
                res_str = f"{art_name} - {song_title}\t{streams}\t{song_url}"
                in_art_str.append(res_str)

        return in_art_str

    except requests.exceptions.HTTPError as errh:
        failed_artists.append(art_name)
        return []


# Загрузка HTML-страницы
url = 'https://kworb.net/spotify/artists.html'
html = get_html(url)

# Извлечение данных об артистах
artists_data = extract_artists_data(html)

# Использование многопоточности для параллельной обработки данных об артистах
failed_artists = []
with ThreadPoolExecutor() as executor:
    results = executor.map(process_artist, artists_data)

# Объединение результатов и вывод
with open('artists.txt', 'w', encoding='utf-8') as f:
    print("\n".join([item for sublist in results for item in sublist]), file=f)

# Вывод списка неудачных артистов
print("Failed artists:")
for art in failed_artists:
    print(art)


url = 'https://kworb.net/spotify/toplists.html'
html = get_html(url)
soup = BeautifulSoup(html, 'lxml')
tables = soup.find_all('table')
trs = tables[0].find('tbody').find_all('tr')
URLS = []
for tr in trs:
    prefURL = 'https://kworb.net'
    tds = tr.find_all('td')
    playListURL = urljoin(prefURL, tds[1].find('a')['href'])
    URLS.append(playListURL)

song_streams = []

for u in URLS:
    url = u
    html = get_html(url)
    soup = BeautifulSoup(html, 'lxml')
    trs = soup.find('tbody').find_all('tr')
    for tr in trs:
        tds = tr.find_all('td')
        song = tds[0].find('div').text.strip()
        streams = tds[1].text.replace(',', '')
        res = song + '\t' + streams
        song_streams.append(res)


with open('topListsByYear.txt', 'w', encoding='utf-8') as f:
    print("\n".join(song_streams), file=f)
