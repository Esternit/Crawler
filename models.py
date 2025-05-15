import asyncpg
import logging
from datetime import datetime

class Database:
    def __init__(self, db_url, instance_name):
        self.db_url = db_url
        self.instance_name = instance_name
        self.pool = None
        self.movies_added = 0
        self.movies_updated = 0

    async def init_db(self):
        self.pool = await asyncpg.create_pool(self.db_url)

    async def mark_task_in_progress(self, conn, imdb_url):
        await conn.execute("""
            INSERT INTO crawler_task_status (imdb_url, status, started_at, assigned_instance)
            VALUES ($1, 'in_progress', NOW(), $2)
            ON CONFLICT (imdb_url) DO UPDATE
            SET status = 'in_progress', started_at = NOW(), assigned_instance = $2;
        """, imdb_url, self.instance_name)

    async def mark_task_done(self, conn, imdb_url):
        await conn.execute("""
            UPDATE crawler_task_status
            SET status = 'done', 
                finished_at = NOW(), 
                last_updated = NOW(),
                error_message = NULL
            WHERE imdb_url = $1;
        """, imdb_url)

    async def mark_task_failed(self, conn, imdb_url, error_msg):
        await conn.execute("""
            UPDATE crawler_task_status
            SET status = 'failed', error_message = $2, last_updated = NOW()
            WHERE imdb_url = $1;
        """, imdb_url, error_msg)

    async def movie_exists(self, conn, imdb_url):
        return await conn.fetchval("SELECT movie_id FROM movie WHERE imdb_url = $1;", imdb_url)

    async def save_movie(self, conn, movie_data):
        imdb_url = movie_data['imdb_url']
        movie_id = await self.movie_exists(conn, imdb_url)
        
        if movie_id:
            current_data = await conn.fetchrow("""
                SELECT title, release_date, type, country, description
                FROM movie 
                WHERE movie_id = $1;
            """, movie_id)
            
            has_changes = (
                current_data['title'] != movie_data['title'] or
                current_data['release_date'] != movie_data['release_date'] or
                current_data['type'] != movie_data['type'] or
                current_data['country'] != movie_data['country'] or
                (current_data['description'] != movie_data['description'] and movie_data['description'] is not None)
            )
            
            if has_changes:
                await conn.execute("""
                    UPDATE movie SET 
                        title = $1, 
                        release_date = $2, 
                        type = $3, 
                        country = $4,
                        description = COALESCE($5, description),
                        updated_at = NOW()
                    WHERE movie_id = $6;
                """, 
                    movie_data['title'], 
                    movie_data['release_date'], 
                    movie_data['type'],
                    movie_data['country'], 
                    movie_data['description'], 
                    movie_id
                )
                self.movies_updated += 1
                logging.info(f"Updated movie: {movie_data['title']}")
            else:
                logging.info(f"No changes detected for movie: {movie_data['title']}")
        else:
            await conn.execute("""
                INSERT INTO movie (title, release_date, imdb_url, type, country, description)
                VALUES ($1, $2, $3, $4, $5, $6);
            """, 
                movie_data['title'], 
                movie_data['release_date'], 
                imdb_url,
                movie_data['type'], 
                movie_data['country'], 
                movie_data['description']
            )
            self.movies_added += 1
            logging.info(f"Added new movie: {movie_data['title']}")

    async def save_cast(self, conn, cast, imdb_url):
        movie_id = await self.movie_exists(conn, imdb_url)
        if not movie_id:
            logging.warning(f"Movie not found for cast insertion: {imdb_url}")
            return

        for person in cast:
            person_id = await conn.fetchval("SELECT person_id FROM person WHERE imdb_id = $1", person['imdb_id'])
            if not person_id:
                person_id = await conn.fetchval("""
                    INSERT INTO person (full_name, imdb_id)
                    VALUES ($1, $2)
                    RETURNING person_id;
                """, person['name'], person['imdb_id'])

            await conn.execute("""
                INSERT INTO movie_cast (movie_id, person_id, role)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING;
            """, movie_id, person_id, person['role'])

    async def log_crawler_run(self, conn, parsed_at):
        await conn.execute("""
            INSERT INTO crawler_log (parsed_at, movies_added, movies_updated)
            VALUES ($1, $2, $3);
        """, parsed_at, self.movies_added, self.movies_updated)

    async def cleanup_stale_tasks(self, conn):
        """Reset stale in_progress tasks that haven't been updated in the last hour"""
        await conn.execute("""
            UPDATE crawler_task_status
            SET status = 'pending',
                started_at = NULL,
                assigned_instance = NULL,
                error_message = 'Task reset due to timeout'
            WHERE status = 'in_progress' 
            AND last_updated < NOW() - INTERVAL '1 hour';
        """) 