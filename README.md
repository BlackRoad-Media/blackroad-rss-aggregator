# BlackRoad RSS Aggregator

RSS/Atom feed aggregator with content deduplication.

[![CI](https://github.com/BlackRoad-Media/blackroad-rss-aggregator/actions/workflows/ci.yml/badge.svg)](https://github.com/BlackRoad-Media/blackroad-rss-aggregator/actions/workflows/ci.yml)

## Features
- Add and manage RSS/Atom feeds by category
- Simulated fetch and XML parsing
- SHA-256 fingerprint deduplication
- Full-text search with SQLite FTS5
- Mark read/unread, bookmark items
- OPML export
- Batch refresh all feeds

## Usage
```python
from rss_aggregator import create_aggregator
agg = create_aggregator()
feed = agg.add_feed("TechCrunch", "https://techcrunch.com/feed/", category="tech-news")
result = agg.refresh(feed.id)
items = agg.get_items(feed.id)
results = agg.search("AI")
opml = agg.export_opml()
```

## Testing
```bash
pytest tests/ -v --cov=rss_aggregator
```

## License
Proprietary - (c) BlackRoad OS, Inc.
