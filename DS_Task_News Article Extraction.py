#!/usr/bin/env python
# coding: utf-8

# # RSS News Extraction Project

# Objective: Build a system to:
# 
# Collect news articles from various RSS feeds.
# Store the articles in a database.
# Categorize the articles into four categories:
# Terrorism / Protest / Political unrest / Riot
# Positive / Uplifting
# Natural Disasters
# Others
# 
# Tools and Technologies:
# Programming language: Python
# Libraries (for Python):
# Feedparser: To fetch and parse RSS feeds.
# SQLAlchemy: To manage interaction with a relational database like PostgreSQL or MySQL.
# Celery: To handle background tasks and manage queues.
# spaCy or NLTK: For categorizing news articles using natural language processing (NLP).

# Using the feedparser library, we’ll connect to each RSS feed, extract the title, content, publication date, and URL, and ensure we handle duplicate articles.

# In[1]:


import feedparser
from datetime import datetime
feeds=[
    "http://rss.cnn.com/rss/cnn_topstories.rss",
    "http://qz.com/feed",
    "http://feeds.foxnews.com/foxnews/politics",
    "http://feeds.reuters.com/reuters/businessNews",
    "http://feeds.feedburner.com/NewshourWorld",
    "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
]
def parse_feed(feed_url):
    feed=feedparser.parse(feed_url)
    articles=[]
    for entry in feed.entries:
        article={
            "title":entry.get("title"),
            "link":entry.get("link"),
            "published":entry.get("published"),
            "summary":entry.get("summary"),
            "source":feed_url
        }
        articles.append(article)
    return articles
all_articles=[]
for feed_url in feeds:
    all_articles.extend(parse_feed(feed_url))


# In[2]:


# Database Schema and Storage
from sqlalchemy import create_engine, Column, String, Text, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# We will use SQLAlchemy to design our database schema, which consists of a table to store news articles.

# In[3]:


# define a postgresql connection URL
database_url="postgresql://postgres:4131%40spG@localhost:5432/RSS_Article"
Base=declarative_base()
class NewsArticle(Base):
    __tablename__='news_articles'
    id=Column(String,primary_key=True)
    title=Column(String,nullable=False)
    link=Column(String,nullable=False)
    summary=Column(Text)
    source=Column(String)
    published=Column(DateTime,default=func.now())
    category=Column(String,default="Uncategorized")


# In[4]:


# setup the database engine and session
engine=create_engine(database_url)
SessionLocal=sessionmaker(autocommit=False,autoflush=False,bind=engine)
Base.metadata.create_all(bind=engine)


# In[5]:


# insert data into the database
def save_articles(articles):
    session=SessionLocal()
    for article in articles:
        if not session.query(NewsArticle).filter_by(link=article['link']).first():
            new_article=NewsArticle(
            title=article['title'],
            link=article['link'],
            summary=article['summary'],
            source=article['source'],
            published=datetime.strptime(article['published'],"%a,%d %b %Y %H:%M:%S %Z")
            )
            session.add(new_article)
    session.commit()
    session.close()


# In[6]:


from celery import Celery
app=Celery('news_aggregator',broker='redis://localhost:6370/0')
@app.task
def process_article(article):
    # perform classification (placeholder for now)
    category=classify_article(article['title'],article['summary'])
    # update the article in the database with its category
    session=SessionLocal()
    article_in_db=session.query(NewsArticle).filter_by(link=article['link']).first()
    if article_in_db:
        article_in_db.category=category
        session.commit()
    session.close()
# Example usage
def process_new_articles(articles):
    for article in articles:
        process_article.delay(article)  # sends to Celery queue


# In[7]:


pip install blis


# In[8]:


# Simple function to categorize article using keyword matching
def categorize_article(article_content):
    article_content_lower = article_content.lower()  # Convert to lowercase for case-insensitive matching

    # Categorize based on keyword presence in the article content
    if any(keyword in article_content_lower for keyword in ['protest', 'riot', 'political unrest', 'terrorism']):
        return "Terrorism/Protest/Political unrest/Riot"
    elif any(keyword in article_content_lower for keyword in ['earthquake', 'flood', 'hurricane', 'tsunami', 'natural disaster']):
        return "Natural Disaster"
    elif any(keyword in article_content_lower for keyword in ['positive', 'uplifting', 'hope', 'inspiring']):
        return "Positive/Uplifting"
    else:
        return "Others"


# In[9]:


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def fetch_and_process_feeds():
    logging.info("Starting RSS feed fetching")
    try:
        for feed_url in feeds:
            articles = parse_feed(feed_url)
            save_articles(articles)
            process_new_articles(articles)
    except Exception as e:
        logging.error(f"Error processing feeds: {e}")


# In[11]:


import json
with open('categorized_news.json', 'w') as f:
    json.dump(all_articles, f, indent=4)
print("Articles have been processed and saved to categorized_news.json.")


# In[14]:


import csv
# Exporting to CSV
with open('categorized_news.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['title', 'content', 'published', 'url', 'category']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for article in all_articles:
        writer.writerow({
            'title': article.get('title', 'N/A'),
            'content': article.get('content', 'N/A'),
            'published': article.get('published', 'N/A'),
            'url': article.get('link', 'N/A'),
            'category': article.get('category', 'N/A')
        })
print("Articles have been processed and saved to categorized_news.csv.")


# In[15]:


import os
print(os.getcwd())


# In[ ]:




