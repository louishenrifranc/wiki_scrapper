from unittest import TestCase
from unittest.mock import patch, Mock
from parameterized import parameterized
from wiki_utils import get_all_urls_on_page, is_movie_wiki_page


class TestWikiUtils(TestCase):
    def setUp(self):
        pass

    def test_get_all_urls_on_page_error_404(self):
        base_url = "https://www.quora.com/fake_does_not_exist"
        self.assertEqual(get_all_urls_on_page(base_url), None)

    @parameterized.expand([
        ("https://en.wikipedia.org/wiki/Logan_(film)", True),
        ("https://www.facebook.com/", False),
        ("https://en.wikipedia.org/wiki/Transformers:_Age_of_Extinction", True),
        ("https://en.wikipedia.org/wiki/2017_in_film", False)
    ])
    def test_is_movie_wiki_page(self, url, is_movie_wiki_url):
        self.assertEqual(is_movie_wiki_page(url), is_movie_wiki_url)
