from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    "email_on_failure": True,
    "email": ["screenbondhq@gmail.com", "heisrayco@gmail.com"],
    "email_on_retry": True,
}


@dag(
    dag_id='scrape_finance_news',
    default_args=default_args,
    description='Scrapes finance news and saves to MongoDB with embeddings',
    schedule='0 9 * * *',
    start_date=datetime(2025, 11, 23),
    catchup=False,
    tags=['news', 'scraping', 'finance']
)
def scrape_finance_news():

    @task()
    def fetch_and_store_news():
        import feedparser
        import hashlib
        from pymongo import MongoClient
        from sentence_transformers import SentenceTransformer
        
        MONGO = Variable.get("MONGO")
        client = MongoClient(MONGO)
        db = client["renci_db"]
        news_collection = db["finance_news"]
        
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        
        RSS_FEEDS = [
            "https://finance.yahoo.com/rss/",
            "https://www.cnbc.com/id/100003114/device/rss/rss.html",
            "https://feeds.marketwatch.com/marketwatch/topstories",
        ]
        
        def generate_article_id(link: str) -> str:
            return hashlib.md5(link.encode()).hexdigest()
        
        stored_count = 0
        
        for feed_url in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
                if feed.bozo:
                    print(f"Failed to parse: {feed_url}")
                    continue

                for entry in feed.entries:
                    link = entry.get("link", "")
                    if not link:
                        continue
                        
                    article_id = generate_article_id(link)
                    
                    if news_collection.find_one({"_id": article_id}):
                        continue

                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    content = f"{title}. {summary}"

                    embedding = model.encode(content).tolist()

                    article = {
                        "_id": article_id,
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "content": content,
                        "source": feed_url,
                        "published": entry.get("published", entry.get("updated")),
                        "fetched_at": datetime.utcnow(),
                        "embedding": embedding,
                    }

                    news_collection.insert_one(article)
                    stored_count += 1
                    print(f"Stored: {title[:60]}...")

            except Exception as e:
                print(f"Error processing {feed_url}: {e}")
        
        client.close()
        print(f"Total new articles stored: {stored_count}")
        return stored_count

    send_email_task = EmailOperator(
        task_id='email_notification',
        to=['screenbondhq@gmail.com', 'heisrayco@gmail.com'],
        subject='Yfinance news scraped and stored',
        html_content='<h3>The news scraping task completed successfully at {{ ts }}</h3>',
    )

    fetch_and_store_news() >> send_email_task

scrape_finance_news()
# scrape_finance_news()