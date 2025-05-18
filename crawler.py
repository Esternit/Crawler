import asyncio
import aiohttp
import logging
from datetime import datetime

from models import Database
from html_parser import Parser
from config_parser import IMDB_CALENDAR_URL, BASE_URL, DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT, INSTANCE_NAME

logging.basicConfig(level=logging.INFO)

class IMDbCrawler:
    def __init__(self, db_url, instance_name):
        self.db = Database(db_url, instance_name)
        self.parser = Parser(BASE_URL)

    async def fetch(self, session, url):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            )
        }
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.text()
                logging.error(f"Failed to fetch {url}, status {response.status}")
        except Exception as e:
            logging.error(f"Fetch error: {url} - {e}")
        return None

    async def process_movie(self, session, imdb_url):
        conn = await self.db.pool.acquire()
        try:
            await self.db.mark_task_in_progress(conn, imdb_url)
            html = await self.fetch(session, imdb_url)
            if not html:
                await self.db.mark_task_failed(conn, imdb_url, "Failed to fetch page content")
                return
                
            movie_data = self.parser.parse_movie_page(html, imdb_url)
            await self.db.save_movie(conn, movie_data)
            await self.db.save_cast(conn, movie_data.get('cast', []), imdb_url)
            await self.db.mark_task_done(conn, imdb_url)
        except Exception as e:
            logging.error(f"Error processing {imdb_url}: {e}")
            await self.db.mark_task_failed(conn, imdb_url, str(e))
        finally:
            await self.db.pool.release(conn)

    async def run(self):
        await self.db.init_db()
        parsed_at = datetime.now()

        async with self.db.pool.acquire() as conn:
            await self.db.cleanup_stale_tasks(conn)
            
            tasks_count = await conn.fetchval("""
                SELECT COUNT(*) FROM crawler_task_status;
            """)
            
            if tasks_count == 0:
                async with aiohttp.ClientSession() as session:
                    html = await self.fetch(session, IMDB_CALENDAR_URL)
                    if not html:
                        logging.error("Failed to fetch main calendar page")
                        return

                    links = self.parser.parse_calendar(html)
                    logging.info(f"Found {len(links)} movie links from calendar")

                    for link in links:
                        await conn.execute("""
                            INSERT INTO crawler_task_status (imdb_url, status)
                            VALUES ($1, 'pending')
                            ON CONFLICT (imdb_url) DO NOTHING;
                        """, link)

        async with aiohttp.ClientSession() as session:
            async with self.db.pool.acquire() as conn:
                tasks = await conn.fetch("""
                    SELECT imdb_url 
                    FROM crawler_task_status 
                    WHERE status != 'in_progress';
                """)

            if tasks:
                logging.info(f"Processing {len(tasks)} tasks")
                await asyncio.gather(*[
                    self.process_movie(session, task['imdb_url'])
                    for task in tasks
                ])
            else:
                logging.info("No tasks to process")

        async with self.db.pool.acquire() as conn:
            await self.db.log_crawler_run(conn, parsed_at)


if __name__ == "__main__":
    DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    crawler = IMDbCrawler(DB_URL, INSTANCE_NAME)
    
    async def main():
        logging.info("Waiting 10 seconds before first run...")
        await asyncio.sleep(10)
        
        while True:
            try:
                logging.info("Starting crawler run...")
                await crawler.run()
                logging.info("Crawler run completed")
            except Exception as e:
                logging.error(f"Error during crawler run: {e}")
            finally:
                if hasattr(crawler.db, 'pool'):
                    await crawler.db.pool.close()
            
            logging.info("Waiting for 1 hour before next run...")
            await asyncio.sleep(3600)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Crawler stopped by user")