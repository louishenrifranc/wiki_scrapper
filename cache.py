import pickle
import os


class Cache:
    def __init__(self, func):
        self.__dir_path = ".cache"
        self.func = func

        self._cache = self._deserialize() or dict()
        self._num_calls = 0
        self._save_every_iter = 100

    def _serialize(self):
        dir_path = self.__dir_path
        func = self.func

        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, func.__name__ + "_cache.pkl")
        with open(file_path, 'wb') as f:
            pickle.dump(self._cache, f)

    def _deserialize(self):
        dir_path = self.__dir_path
        func = self.func

        file_path = os.path.join(dir_path, func.__name__ + "_cache.pkl")
        if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
            return None
        else:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            return data

    def __call__(self, url: str, **kwargs):
        self._num_calls += 1
        if self._num_calls % self._save_every_iter == 0:
            self._serialize()

        if url in self._cache:
            return self._cache[url]
        result = self.func(url, **kwargs)
        self._cache[url] = result
        return result
