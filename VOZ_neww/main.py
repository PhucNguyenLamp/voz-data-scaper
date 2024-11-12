# api/main.py
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from underthesea import sentiment
import os
import logging
import time
from contextlib import contextmanager
from typing import Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="VOZ Analytics API")
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_CONFIG = {
    "dbname": "vozdb",
    "user": "postgres",
    "password": "postgres",
    "host": "db",
    "port": "5432"
}

def wait_for_db(max_retries=30, delay_seconds=2):
    """Wait for database to be ready"""
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            logger.info("Successfully connected to the database")
            return True
        except psycopg2.Error as e:
            retries += 1
            logger.warning(f"Attempt {retries}/{max_retries} to connect to database failed: {str(e)}")
            logger.warning("Retrying in %s seconds...", delay_seconds)
            time.sleep(delay_seconds)
    
    raise Exception("Could not connect to the database after multiple attempts")

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

def get_db():
    """Database dependency for FastAPI"""
    with get_db_connection() as conn:
        yield conn

# Analytics queries
def get_sentiment_stats(conn):
    """Get hourly sentiment statistics for the last 24 hours"""
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    DATE_TRUNC('hour', analyzed_at) as time_bucket,
                    SUM(positive_count) as positive_count,
                    SUM(negative_count) as negative_count,
                    SUM(neutral_count) as neutral_count,
                    COUNT(*) as total_messages
                FROM voz_messages
                WHERE analyzed_at >= NOW() - INTERVAL '24 hours'
                GROUP BY time_bucket
                ORDER BY time_bucket DESC
            """
            cur.execute(query)
            results = cur.fetchall()
            return list(results)
    except psycopg2.Error as e:
        logger.error(f"Error fetching sentiment stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

def get_sentiment_summary(conn):
    """Get overall sentiment summary for the last 24 hours"""
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    SUM(positive_count) as total_positive,
                    SUM(negative_count) as total_negative,
                    SUM(neutral_count) as total_neutral,
                    COUNT(*) as total_messages
                FROM voz_messages
                WHERE analyzed_at >= NOW() - INTERVAL '24 hours'
            """
            cur.execute(query)
            result = cur.fetchone()
            return result
    except psycopg2.Error as e:
        logger.error(f"Error fetching sentiment summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

def get_messages_with_sentiment(conn, limit: int = 10, offset: int = 0, thread_id: Optional[str] = None):
    """Get messages with their sentiment analysis"""
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    id,
                    thread_title,
                    thread_date,
                    message_content,
                    latest_poster,
                    latest_post_time,
                    thread_url,
                    CASE 
                        WHEN positive_count = 1 THEN 'positive'
                        WHEN negative_count = 1 THEN 'negative'
                        ELSE 'neutral'
                    END as sentiment,
                    analyzed_at
                FROM voz_messages
                WHERE 1=1
            """
            params = []
            
            if thread_id:
                query += " AND thread_id = %s"
                params.append(thread_id)
            
            query += """
                ORDER BY analyzed_at DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limit, offset])
            
            cur.execute(query, params)
            messages = cur.fetchall()
            
            # Get total count for pagination
            count_query = """
                SELECT COUNT(*) as total
                FROM voz_messages
                WHERE 1=1
            """
            if thread_id:
                count_query += " AND thread_id = %s"
                cur.execute(count_query, [thread_id] if thread_id else None)
            else:
                cur.execute(count_query)
            
            total_count = cur.fetchone()['total']
            
            return {
                "messages": messages,
                "total": total_count,
                "limit": limit,
                "offset": offset
            }
    except psycopg2.Error as e:
        logger.error(f"Error fetching messages with sentiment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

def analyze_text_sentiment(text: str):
    """Analyze sentiment of a given text using underthesea"""
    try:
        return sentiment(text)
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {str(e)}")
        return "neutral"

# API endpoints
@app.get("/stats/sentiment")
def sentiment_stats(conn = Depends(get_db)):
    """Get hourly sentiment statistics"""
    return get_sentiment_stats(conn)

@app.get("/stats/sentiment/summary")
def sentiment_summary(conn = Depends(get_db)):
    """Get overall sentiment summary"""
    return get_sentiment_summary(conn)

@app.get("/messages/sentiment")
def get_messages(
    conn = Depends(get_db),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    thread_id: Optional[str] = None
):
    """Get messages with their sentiment analysis"""
    return get_messages_with_sentiment(conn, limit, offset, thread_id)

@app.post("/analyze/text")
def analyze_text(text: str):
    """Analyze sentiment of provided text"""
    return {"sentiment": analyze_text_sentiment(text)}

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint that also verifies database connection"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return {
                    "status": "healthy",
                    "database": "connected",
                    "timestamp": datetime.now().isoformat()
                }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")