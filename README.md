<!-- BlackRoad SEO Enhanced -->

# ulackroad rss aggregator

> Part of **[BlackRoad OS](https://blackroad.io)** — Sovereign Computing for Everyone

[![BlackRoad OS](https://img.shields.io/badge/BlackRoad-OS-ff1d6c?style=for-the-badge)](https://blackroad.io)
[![BlackRoad-Media](https://img.shields.io/badge/Org-BlackRoad-Media-2979ff?style=for-the-badge)](https://github.com/BlackRoad-Media)

**ulackroad rss aggregator** is part of the **BlackRoad OS** ecosystem — a sovereign, distributed operating system built on edge computing, local AI, and mesh networking by **BlackRoad OS, Inc.**

### BlackRoad Ecosystem
| Org | Focus |
|---|---|
| [BlackRoad OS](https://github.com/BlackRoad-OS) | Core platform |
| [BlackRoad OS, Inc.](https://github.com/BlackRoad-OS-Inc) | Corporate |
| [BlackRoad AI](https://github.com/BlackRoad-AI) | AI/ML |
| [BlackRoad Hardware](https://github.com/BlackRoad-Hardware) | Edge hardware |
| [BlackRoad Security](https://github.com/BlackRoad-Security) | Cybersecurity |
| [BlackRoad Quantum](https://github.com/BlackRoad-Quantum) | Quantum computing |
| [BlackRoad Agents](https://github.com/BlackRoad-Agents) | AI agents |
| [BlackRoad Network](https://github.com/BlackRoad-Network) | Mesh networking |

**Website**: [blackroad.io](https://blackroad.io) | **Chat**: [chat.blackroad.io](https://chat.blackroad.io) | **Search**: [search.blackroad.io](https://search.blackroad.io)

---


> RSS/Atom feed aggregator with content deduplication

Part of the [BlackRoad OS](https://blackroad.io) ecosystem — [BlackRoad-Media](https://github.com/BlackRoad-Media)

---

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
