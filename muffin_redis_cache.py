""" Support cache with redis backend in Muffin framework. """

import types
import inspect
import functools
import jsonpickle
import asyncio
import asyncio_redis

from muffin.plugins import PluginException
from muffin.utils import to_coroutine

from muffin_redis import Plugin as RedisPlugin

__version__ = "0.0.1"
__project__ = "muffin-redis-cache"
__author__ = "Mike Klimin <klinkin@gmail.com>"
__license__ = "MIT"


class Plugin(RedisPlugin):

    """ Connect to Redis. """

    name = 'redis_cache'

    defaults = {
        'db': 0,
        'fake': False,
        'host': '127.0.0.1',
        'password': None,
        'poolsize': 1,
        'port': 6379,
        'default_expire': 300,
        'key_prefix': 'muffin_cache_'
    }

    def __init__(self, *args, **kwargs):
        """ Initialize the Plugin. """
        super().__init__(*args, **kwargs)
        self.conn = None

    def setup(self, app):
        """ Setup self options. """
        super().setup(app)
        self.options.default_expire = int(self.options.default_expire)

    def cached(self, expire=None, key_prefix='view%s', unless=None):

        def decorator(view):
            view = to_coroutine(view)

            @asyncio.coroutine
            @functools.wraps(view)
            def decorated_function(request, *args, **kwargs):
                #: Bypass the cache entirely.
                if callable(unless) and unless() is True:
                    return (yield from view(request, *args, **kwargs))

                try:
                    cache_key = decorated_function.make_cache_key(request, *args, **kwargs)
                    rv = yield from self.conn.get(cache_key)
                except Exception:
                    if self.app.cfg.DEBUG:
                        raise PluginException('Exception possibly due to cache backend.')
                    self.app.logger.exception("Exception possibly due to cache backend.")
                    return (yield from view(request, *args, **kwargs))

                if rv is None:

                    future = asyncio.Future()
                    rv = (yield from view(request, *args, **kwargs))

                    if isinstance(rv, asyncio.futures.Future) or inspect.isgenerator(rv):
                        rv = yield from rv

                    try:
                        yield from self.conn.set(cache_key, jsonpickle.encode(rv), expire=decorated_function.cache_expire)
                    except Exception:
                        if self.app.cfg.DEBUG:
                            raise PluginException('Exception possibly due to cache backend.')
                        self.app.logger.exception("Exception possibly due to cache backend.")
                        return (yield from view(request, *args, **kwargs))
                else:
                    rv = jsonpickle.decode(rv)

                return rv

            def make_cache_key(request, *args, **kwargs):
                if callable(key_prefix):
                    cache_key = key_prefix()
                elif '%s' in key_prefix:
                    cache_key = key_prefix % request.path
                else:
                    cache_key = key_prefix

                return cache_key

            decorated_function.uncached = view
            decorated_function.cache_expire = expire
            decorated_function.make_cache_key = make_cache_key

            return decorated_function
        return decorator

try:
    import fakeredis

    class FakeRedis(fakeredis.FakeRedis):

        """ Fake connection for tests. """

        def __getattribute__(self, name):
            """ Make a coroutine. """
            method = super().__getattribute__(name)
            if not name.startswith('_'):
                @asyncio.coroutine
                def coro(*args, **kwargs):
                    return method(*args, **kwargs)
                return coro
            return method

        def close(self):
            """ Do nothing. """
            pass

    class FakeConnection(asyncio_redis.Connection):

        """ Fake Redis for tests. """

        @classmethod
        @asyncio.coroutine
        def create(cls, *args, **kwargs):
            """ Create a fake connection. """
            return FakeRedis()

except ImportError:
    FakeConnection = False
