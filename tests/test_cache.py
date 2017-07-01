from unittest import TestCase
from unittest.mock import Mock
from cache import Cache
from copy import copy
import shutil
import os


class TestCache(TestCase):
    def setUp(self):
        if os.path.exists(".cache"):
            shutil.rmtree(".cache")

    def tearDown(self):
        if os.path.exists(".cache"):
            shutil.rmtree(".cache")

    def test_create_object(self):
        """
            Test object creation
        """
        func = Mock()
        func.return_value = 1
        func.__name__ = "function"
        cache = Cache(func)
        self.assertIsNotNone(cache)

        self.assertTrue(hasattr(cache, "_cache"))
        self.assertTrue(isinstance(cache._cache, dict))

        self.assertTrue(hasattr(cache, "_num_calls"))
        self.assertEqual(cache._num_calls, 0)

        self.assertTrue(hasattr(cache, "_save_every_iter"))
        self.assertEqual(cache._save_every_iter, 100)

        self.assertEqual(cache.func(), 1)

    def _setup_cache(self):
        func = Mock()
        func.side_effect = [1, 2, 3]
        func.__name__ = "function"
        cache = Cache(func)
        return cache, func

    def test_call_func(self):
        """
            Test function correct behaviour when calling cache
        """
        cache, func = self._setup_cache()
        self.assertEqual(cache("url"), 1)
        self.assertEqual(cache("url2"), 2)
        self.assertEqual(cache("url2"), 2)
        self.assertEqual(cache("url3"), 3)

        self.assertTrue("url" and "url2" and "url3" in cache._cache)
        self.assertEqual(cache._cache["url"], 1)
        self.assertEqual(cache._cache["url2"], 2)
        self.assertEqual(cache._cache["url3"], 3)

        self.assertEqual(len(cache._cache), 3)
        self.assertEqual(func.call_count, 3)

    def test_serialize_deserialize(self):
        """
            Test serialization and deserializtion
        """
        cache, func = self._setup_cache()
        cache("url1")
        cache("url3")
        cache("url2")
        cache("url2")
        cache("url2")
        cache._serialize()
        previous_cache = copy(cache._cache)
        del cache

        cache = Cache(func)
        self.assertEqual(cache._cache, previous_cache)
    