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

        # Clean up stale tasks at startup
        async with self.db.pool.acquire() as conn:
            await self.db.cleanup_stale_tasks(conn)

        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, IMDB_CALENDAR_URL)
            if not html:
                logging.error("Failed to fetch main calendar page")
                return

            links = self.parser.parse_calendar(html)
            logging.info(f"Found {len(links)} movie links")

            async with self.db.pool.acquire() as conn:
                for link in links:
                    await conn.execute("""
                        INSERT INTO crawler_task_status (imdb_url, status)
                        VALUES ($1, 'pending')
                        ON CONFLICT (imdb_url) DO NOTHING;
                    """, link)

                tasks = await conn.fetch("""
                    SELECT imdb_url 
                    FROM crawler_task_status 
                    WHERE status != 'in_progress';
                """)

            await asyncio.gather(*[
                self.process_movie(session, task['imdb_url'])
                for task in tasks
            ])

        async with self.db.pool.acquire() as conn:
            await self.db.log_crawler_run(conn, parsed_at)


if __name__ == "__main__":
    DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    crawler = IMDbCrawler(DB_URL, INSTANCE_NAME)
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(crawler.run())
    finally:
        if hasattr(crawler.db, 'pool'):
            loop.run_until_complete(crawler.db.pool.close())
        
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()