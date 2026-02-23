#!/usr/bin/env python3
"""BlackRoad RSS Aggregator - RSS/Atom feed aggregator with content deduplication"""

import sqlite3
import uuid
import hashlib
import json
import re
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict
from datetime import datetime, timezone, timedelta
from enum import Enum


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FeedStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"


@dataclass
class AggregatorConfig:
    max_items_per_feed: int = 100
    dedupe_window_days: int = 30
    fetch_timeout: int = 10
    max_summary_length: int = 500


@dataclass
class Feed:
    id: str
    name: str
    url: str
    category: str
    fetch_interval_min: int = 60
    last_fetched: Optional[str] = None
    status: str = FeedStatus.ACTIVE.value
    error_message: Optional[str] = None
    item_count: int = 0
    created_at: str = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FeedItem:
    id: str
    feed_id: str
    title: str
    url: str
    summary: str
    author: str
    published_at: str
    fingerprint: str
    is_read: bool = False
    is_bookmarked: bool = False
    created_at: str = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)


class RSSAggregator:
    """RSS/Atom feed aggregator with deduplication."""

    SAMPLE_FEEDS = {
        "tech-news": [
            {"title": "AI Breakthrough in 2025", "author": "Jane Smith",
             "url": "https://example.com/ai-2025", "summary": "New AI models achieve human parity on reasoning benchmarks."},
            {"title": "Open Source Wins Big", "author": "John Doe",
             "url": "https://example.com/oss", "summary": "Open source projects dominate enterprise adoption."},
            {"title": "Cloud Costs Spiral", "author": "Alice Brown",
             "url": "https://example.com/cloud", "summary": "Enterprise cloud costs up 40% year over year."},
        ],
        "science": [
            {"title": "Mars Mission Update", "author": "NASA Team",
             "url": "https://example.com/mars", "summary": "Perseverance rover discovers ancient lake bed."},
            {"title": "Quantum Computing Milestone", "author": "MIT Lab",
             "url": "https://example.com/quantum", "summary": "100-qubit processor achieves new error correction record."},
        ],
        "default": [
            {"title": "Sample Article 1", "author": "Author A",
             "url": "https://example.com/1", "summary": "Content for article one."},
            {"title": "Sample Article 2", "author": "Author B",
             "url": "https://example.com/2", "summary": "Content for article two."},
        ]
    }

    def __init__(self, db_path: str = "rss_aggregator.db",
                 config: Optional[AggregatorConfig] = None):
        self.db_path = db_path
        self.config = config or AggregatorConfig()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS feeds (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT UNIQUE NOT NULL,
                    category TEXT NOT NULL DEFAULT 'general',
                    fetch_interval_min INTEGER DEFAULT 60,
                    last_fetched TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    error_message TEXT,
                    item_count INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS feed_items (
                    id TEXT PRIMARY KEY,
                    feed_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    summary TEXT DEFAULT '',
                    author TEXT DEFAULT '',
                    published_at TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    is_read INTEGER DEFAULT 0,
                    is_bookmarked INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (feed_id) REFERENCES feeds(id)
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS feed_items_fts
                    USING fts5(title, summary, author, content=feed_items, content_rowid=rowid);

                CREATE TRIGGER IF NOT EXISTS feed_items_ai AFTER INSERT ON feed_items BEGIN
                    INSERT INTO feed_items_fts(rowid, title, summary, author)
                    VALUES (new.rowid, new.title, new.summary, new.author);
                END;

                CREATE TRIGGER IF NOT EXISTS feed_items_ad AFTER DELETE ON feed_items BEGIN
                    INSERT INTO feed_items_fts(feed_items_fts, rowid, title, summary, author)
                    VALUES ('delete', old.rowid, old.title, old.summary, old.author);
                END;

                CREATE INDEX IF NOT EXISTS idx_items_feed ON feed_items(feed_id);
                CREATE INDEX IF NOT EXISTS idx_items_fingerprint ON feed_items(fingerprint);
                CREATE INDEX IF NOT EXISTS idx_items_published ON feed_items(published_at DESC);
            """)

    @staticmethod
    def _fingerprint(title: str, url: str) -> str:
        """Generate a deduplication fingerprint."""
        text = f"{title.lower().strip()}{url.lower().strip()}"
        return hashlib.sha256(text.encode()).hexdigest()[:16]

    def add_feed(self, name: str, url: str, category: str = "general",
                 fetch_interval_min: int = 60) -> Feed:
        """Add a new RSS/Atom feed."""
        feed_id = str(uuid.uuid4())
        feed = Feed(
            id=feed_id, name=name, url=url,
            category=category, fetch_interval_min=fetch_interval_min,
        )
        with self._connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO feeds
                (id, name, url, category, fetch_interval_min, status, created_at)
                VALUES (?, ?, ?, ?, ?, 'active', ?)
            """, (feed.id, feed.name, feed.url, feed.category,
                  feed.fetch_interval_min, feed.created_at))
        return self.get_feed_by_url(url) or feed

    def get_feed(self, feed_id: str) -> Optional[Feed]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM feeds WHERE id=?", (feed_id,)).fetchone()
        return Feed(**dict(row)) if row else None

    def get_feed_by_url(self, url: str) -> Optional[Feed]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM feeds WHERE url=?", (url,)).fetchone()
        return Feed(**dict(row)) if row else None

    def _simulate_fetch(self, feed: Feed) -> List[Dict]:
        """Simulate fetching and parsing XML feed (no network needed)."""
        key = feed.category if feed.category in self.SAMPLE_FEEDS else "default"
        items = self.SAMPLE_FEEDS[key]
        now = datetime.now(timezone.utc)
        result = []
        for i, item in enumerate(items[:self.config.max_items_per_feed]):
            pub_time = now - timedelta(hours=i * 6)
            result.append({
                "title": item["title"],
                "url": item["url"],
                "summary": item["summary"][:self.config.max_summary_length],
                "author": item["author"],
                "published_at": pub_time.isoformat(),
            })
        return result

    def refresh(self, feed_id: str) -> dict:
        """Fetch and store new items for a feed."""
        feed = self.get_feed(feed_id)
        if not feed:
            raise ValueError(f"Feed {feed_id} not found")
        if feed.status == FeedStatus.PAUSED.value:
            return {"feed_id": feed_id, "skipped": True, "reason": "paused"}

        try:
            raw_items = self._simulate_fetch(feed)
            new_count = 0
            dup_count = 0

            for raw in raw_items:
                fp = self._fingerprint(raw["title"], raw["url"])
                with self._connect() as conn:
                    existing = conn.execute(
                        "SELECT id FROM feed_items WHERE fingerprint=?", (fp,)
                    ).fetchone()
                    if existing:
                        dup_count += 1
                        continue
                    item_id = str(uuid.uuid4())
                    conn.execute("""
                        INSERT INTO feed_items
                        (id, feed_id, title, url, summary, author, published_at,
                         fingerprint, is_read, is_bookmarked, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
                    """, (item_id, feed_id, raw["title"], raw["url"],
                          raw.get("summary", ""), raw.get("author", ""),
                          raw.get("published_at", _now()), fp, _now()))
                    new_count += 1

            with self._connect() as conn:
                total = conn.execute(
                    "SELECT COUNT(*) FROM feed_items WHERE feed_id=?", (feed_id,)
                ).fetchone()[0]
                conn.execute("""
                    UPDATE feeds SET last_fetched=?, status='active', item_count=?
                    WHERE id=?
                """, (_now(), total, feed_id))

            return {
                "feed_id": feed_id,
                "new_items": new_count,
                "duplicates": dup_count,
                "total_items": total,
            }

        except Exception as e:
            with self._connect() as conn:
                conn.execute("""
                    UPDATE feeds SET status='error', error_message=? WHERE id=?
                """, (str(e), feed_id))
            raise

    def refresh_all(self) -> List[dict]:
        """Refresh all active feeds."""
        with self._connect() as conn:
            feeds = conn.execute(
                "SELECT id FROM feeds WHERE status='active'"
            ).fetchall()
        results = []
        for row in feeds:
            try:
                result = self.refresh(row["id"])
                results.append(result)
            except Exception as e:
                results.append({"feed_id": row["id"], "error": str(e)})
        return results

    def mark_read(self, item_id: str) -> bool:
        """Mark a feed item as read."""
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE feed_items SET is_read=1 WHERE id=?", (item_id,)
            )
        return result.rowcount > 0

    def mark_unread(self, item_id: str) -> bool:
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE feed_items SET is_read=0 WHERE id=?", (item_id,)
            )
        return result.rowcount > 0

    def bookmark(self, item_id: str) -> bool:
        """Bookmark a feed item."""
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE feed_items SET is_bookmarked=1 WHERE id=?", (item_id,)
            )
        return result.rowcount > 0

    def unbookmark(self, item_id: str) -> bool:
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE feed_items SET is_bookmarked=0 WHERE id=?", (item_id,)
            )
        return result.rowcount > 0

    def search(self, query: str, limit: int = 20) -> List[FeedItem]:
        """Full-text search using FTS5."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT fi.* FROM feed_items fi
                JOIN feed_items_fts fts ON fi.rowid = fts.rowid
                WHERE feed_items_fts MATCH ?
                ORDER BY fi.published_at DESC
                LIMIT ?
            """, (query, limit)).fetchall()
        return [self._row_to_item(dict(r)) for r in rows]

    def by_category(self, category: str, limit: int = 50,
                    unread_only: bool = False) -> List[FeedItem]:
        """Get items from feeds in a category."""
        with self._connect() as conn:
            if unread_only:
                rows = conn.execute("""
                    SELECT fi.* FROM feed_items fi
                    JOIN feeds f ON fi.feed_id = f.id
                    WHERE f.category=? AND fi.is_read=0
                    ORDER BY fi.published_at DESC LIMIT ?
                """, (category, limit)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT fi.* FROM feed_items fi
                    JOIN feeds f ON fi.feed_id = f.id
                    WHERE f.category=?
                    ORDER BY fi.published_at DESC LIMIT ?
                """, (category, limit)).fetchall()
        return [self._row_to_item(dict(r)) for r in rows]

    def get_items(self, feed_id: str, limit: int = 50,
                  unread_only: bool = False) -> List[FeedItem]:
        with self._connect() as conn:
            if unread_only:
                rows = conn.execute("""
                    SELECT * FROM feed_items WHERE feed_id=? AND is_read=0
                    ORDER BY published_at DESC LIMIT ?
                """, (feed_id, limit)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM feed_items WHERE feed_id=?
                    ORDER BY published_at DESC LIMIT ?
                """, (feed_id, limit)).fetchall()
        return [self._row_to_item(dict(r)) for r in rows]

    def get_bookmarks(self) -> List[FeedItem]:
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT * FROM feed_items WHERE is_bookmarked=1
                ORDER BY published_at DESC
            """).fetchall()
        return [self._row_to_item(dict(r)) for r in rows]

    @staticmethod
    def _row_to_item(d: dict) -> FeedItem:
        d["is_read"] = bool(d["is_read"])
        d["is_bookmarked"] = bool(d["is_bookmarked"])
        return FeedItem(**d)

    def deduplicate(self) -> dict:
        """Remove duplicate items based on fingerprint, keep most recent."""
        with self._connect() as conn:
            dups = conn.execute("""
                SELECT fingerprint, COUNT(*) as cnt, MIN(id) as keep_id
                FROM feed_items
                GROUP BY fingerprint
                HAVING cnt > 1
            """).fetchall()

            removed = 0
            for dup in dups:
                result = conn.execute("""
                    DELETE FROM feed_items
                    WHERE fingerprint=? AND id != ?
                """, (dup["fingerprint"], dup["keep_id"]))
                removed += result.rowcount

        return {"duplicates_removed": removed}

    def export_opml(self) -> str:
        """Export all feeds as OPML XML."""
        feeds = self.list_feeds()
        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<opml version="2.0">',
            '  <head><title>BlackRoad RSS Feeds</title></head>',
            '  <body>',
        ]
        categories: Dict[str, List[Feed]] = {}
        for feed in feeds:
            categories.setdefault(feed.category, []).append(feed)

        for cat, cat_feeds in sorted(categories.items()):
            lines.append(f'    <outline text="{cat}" title="{cat}">')
            for f in cat_feeds:
                lines.append(
                    f'      <outline type="rss" text="{f.name}" '
                    f'title="{f.name}" xmlUrl="{f.url}"/>'
                )
            lines.append('    </outline>')

        lines += ['  </body>', '</opml>']
        return "\n".join(lines)

    def list_feeds(self, status: Optional[str] = None) -> List[Feed]:
        with self._connect() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM feeds WHERE status=? ORDER BY name", (status,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM feeds ORDER BY name").fetchall()
        return [Feed(**dict(r)) for r in rows]

    def pause_feed(self, feed_id: str) -> bool:
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE feeds SET status='paused' WHERE id=?", (feed_id,)
            )
        return result.rowcount > 0

    def resume_feed(self, feed_id: str) -> bool:
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE feeds SET status='active' WHERE id=?", (feed_id,)
            )
        return result.rowcount > 0

    def get_stats(self) -> dict:
        with self._connect() as conn:
            total_feeds = conn.execute("SELECT COUNT(*) FROM feeds").fetchone()[0]
            active_feeds = conn.execute(
                "SELECT COUNT(*) FROM feeds WHERE status='active'"
            ).fetchone()[0]
            total_items = conn.execute("SELECT COUNT(*) FROM feed_items").fetchone()[0]
            unread = conn.execute(
                "SELECT COUNT(*) FROM feed_items WHERE is_read=0"
            ).fetchone()[0]
            bookmarked = conn.execute(
                "SELECT COUNT(*) FROM feed_items WHERE is_bookmarked=1"
            ).fetchone()[0]
        return {
            "total_feeds": total_feeds,
            "active_feeds": active_feeds,
            "total_items": total_items,
            "unread_items": unread,
            "bookmarked_items": bookmarked,
        }


def create_aggregator(db_path: str = "rss_aggregator.db") -> RSSAggregator:
    return RSSAggregator(db_path=db_path)


if __name__ == "__main__":
    agg = create_aggregator()
    print("BlackRoad RSS Aggregator")
    print("=" * 40)
    f1 = agg.add_feed("TechCrunch", "https://techcrunch.com/feed/", category="tech-news")
    f2 = agg.add_feed("NASA News", "https://www.nasa.gov/rss/dyn/breaking_news.rss", category="science")
    print(f"Added feeds: {f1.name}, {f2.name}")
    r1 = agg.refresh(f1.id)
    r2 = agg.refresh(f2.id)
    print(f"Fetched: {r1['new_items']} tech items, {r2['new_items']} science items")
    items = agg.get_items(f1.id)
    if items:
        agg.mark_read(items[0].id)
        agg.bookmark(items[0].id)
        print(f"Read and bookmarked: {items[0].title}")
    results = agg.search("AI")
    print(f"Search 'AI': {len(results)} results")
    opml = agg.export_opml()
    print(f"OPML ({len(opml.splitlines())} lines)")
    stats = agg.get_stats()
    print(f"Stats: {stats}")
