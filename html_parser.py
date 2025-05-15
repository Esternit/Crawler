from bs4 import BeautifulSoup
import re
from datetime import datetime

class Parser:
    def __init__(self, base_url):
        self.base_url = base_url

    def parse_calendar(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        
        main_section = soup.find('section', {'class': 'ipc-page-section ipc-page-section--base'})
        if not main_section:
            return list(links)
            
        for a in main_section.select("a[href^='/title/tt']"):
            href = a.get("href")
            if href:
                match = re.match(r"^/title/tt\d+", href)
                if match:
                    full_link = self.base_url + match.group(0) + '/'
                    links.add(full_link)
                    
        return list(links)

    def parse_movie_page(self, html, url):
        soup = BeautifulSoup(html, 'html.parser')
        try:
            title = soup.find('h1').text.strip()
            release_date = datetime.now().date()
            country = 'USA'
            description = soup.find('span', {'data-testid': 'plot-l'}).text.strip() if soup.find('span', {'data-testid': 'plot-l'}) else None
            type_ = 'Movie'

            cast_block = soup.find('ul', {'data-testid': 'title-pc-list'})
            cast = []

            if cast_block:
                for li in cast_block.find_all('li', {'role': 'presentation'}):
                    a_tag = li.find('a', {'href': re.compile(r"^/name/")})
                    if a_tag:
                        name = a_tag.text.strip()
                        href = a_tag.get('href')
                        imdb_id = href.split('/')[2]

                        role_tag = li.find('span', {'class': 'ipc-metadata-list-item__label'})
                        role = role_tag.text.strip() if role_tag else 'Unknown'

                        cast.append({'name': name, 'imdb_id': imdb_id, 'role': role})

            return {
                'imdb_url': url,
                'title': title,
                'release_date': release_date,
                'type': type_,
                'country': country,
                'description': description,
                'cast': cast
            }
        except Exception as e:
            raise RuntimeError(f"Parsing failed for {url}: {e}") 