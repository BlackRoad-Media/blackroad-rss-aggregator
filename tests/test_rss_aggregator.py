import pytest
from rss_aggregator import (
    RSSAggregator, Feed, FeedItem, AggregatorConfig,
    FeedStatus, create_aggregator,
)


@pytest.fixture
def agg(tmp_path):
    return RSSAggregator(db_path=str(tmp_path / "test_rss.db"))


@pytest.fixture
def agg_with_feeds(agg):
    agg.add_feed("Tech Feed", "https://example.com/tech.rss", category="tech-news")
    agg.add_feed("Science Feed", "https://example.com/science.rss", category="science")
    agg.add_feed("General Feed", "https://example.com/general.rss", category="general")
    return agg


class TestFeedManagement:
    def test_add_feed(self, agg):
        feed = agg.add_feed("Test Feed", "https://example.com/feed.rss", category="tech")
        assert feed.id is not None
        assert feed.name == "Test Feed"
        assert feed.status == FeedStatus.ACTIVE.value

    def test_add_duplicate_feed_returns_existing(self, agg):
        f1 = agg.add_feed("Test", "https://example.com/feed.rss")
        f2 = agg.add_feed("Test Again", "https://example.com/feed.rss")
        assert f1.id == f2.id

    def test_get_feed(self, agg):
        f = agg.add_feed("Test", "https://example.com/feed.rss")
        fetched = agg.get_feed(f.id)
        assert fetched is not None
        assert fetched.name == "Test"

    def test_pause_resume_feed(self, agg):
        f = agg.add_feed("Test", "https://example.com/feed.rss")
        agg.pause_feed(f.id)
        fetched = agg.get_feed(f.id)
        assert fetched.status == FeedStatus.PAUSED.value
        agg.resume_feed(f.id)
        fetched = agg.get_feed(f.id)
        assert fetched.status == FeedStatus.ACTIVE.value

    def test_list_feeds(self, agg_with_feeds):
        feeds = agg_with_feeds.list_feeds()
        assert len(feeds) == 3


class TestFeedRefresh:
    def test_refresh_adds_items(self, agg_with_feeds):
        feeds = agg_with_feeds.list_feeds()
        result = agg_with_feeds.refresh(feeds[0].id)
        assert result["new_items"] > 0

    def test_refresh_no_duplicates_second_time(self, agg_with_feeds):
        feeds = agg_with_feeds.list_feeds()
        agg_with_feeds.refresh(feeds[0].id)
        result2 = agg_with_feeds.refresh(feeds[0].id)
        assert result2["duplicates"] > 0
        assert result2["new_items"] == 0

    def test_refresh_paused_feed_skips(self, agg):
        f = agg.add_feed("Paused", "https://example.com/feed.rss")
        agg.pause_feed(f.id)
        result = agg.refresh(f.id)
        assert result.get("skipped") is True

    def test_refresh_nonexistent_raises(self, agg):
        with pytest.raises(ValueError):
            agg.refresh("nonexistent-id")

    def test_refresh_all(self, agg_with_feeds):
        results = agg_with_feeds.refresh_all()
        assert len(results) == 3


class TestItemOperations:
    def test_mark_read_unread(self, agg_with_feeds):
        feeds = agg_with_feeds.list_feeds()
        agg_with_feeds.refresh(feeds[0].id)
        items = agg_with_feeds.get_items(feeds[0].id)
        assert len(items) > 0
        agg_with_feeds.mark_read(items[0].id)
        updated = agg_with_feeds.get_items(feeds[0].id, unread_only=True)
        assert all(i.id != items[0].id for i in updated)

    def test_bookmark_unbookmark(self, agg_with_feeds):
        feeds = agg_with_feeds.list_feeds()
        agg_with_feeds.refresh(feeds[0].id)
        items = agg_with_feeds.get_items(feeds[0].id)
        agg_with_feeds.bookmark(items[0].id)
        bookmarks = agg_with_feeds.get_bookmarks()
        assert any(b.id == items[0].id for b in bookmarks)
        agg_with_feeds.unbookmark(items[0].id)
        bookmarks2 = agg_with_feeds.get_bookmarks()
        assert not any(b.id == items[0].id for b in bookmarks2)

    def test_search_fts(self, agg_with_feeds):
        agg_with_feeds.refresh_all()
        results = agg_with_feeds.search("AI")
        assert isinstance(results, list)

    def test_by_category(self, agg_with_feeds):
        agg_with_feeds.refresh_all()
        items = agg_with_feeds.by_category("tech-news")
        assert isinstance(items, list)

    def test_deduplicate(self, agg_with_feeds):
        agg_with_feeds.refresh_all()
        result = agg_with_feeds.deduplicate()
        assert "duplicates_removed" in result

    def test_export_opml(self, agg_with_feeds):
        opml = agg_with_feeds.export_opml()
        assert "opml" in opml.lower()
        assert "outline" in opml

    def test_get_stats(self, agg_with_feeds):
        agg_with_feeds.refresh_all()
        stats = agg_with_feeds.get_stats()
        assert "total_feeds" in stats
        assert stats["total_feeds"] == 3
        assert stats["total_items"] > 0

    def test_create_aggregator_factory(self, tmp_path):
        a = create_aggregator(str(tmp_path / "factory.db"))
        assert a is not None
