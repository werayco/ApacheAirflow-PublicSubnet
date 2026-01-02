import feedparser
from datetime import datetime
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
import hashlib

MONGO = "mongodb://[::1]:27018/?directConnection=true"
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

def fetch_and_store_news():
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

if __name__ == "__main__":
    fetch_and_store_news()