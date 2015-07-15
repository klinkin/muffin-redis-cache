import asyncio
import jsonpickle
import muffin
import pytest


@pytest.fixture(scope='session')
def app(loop):
    app = muffin.Application(
        'redis_cache', loop=loop,

        PLUGINS=[
            'muffin_redis',
            'muffin_redis_cache'
        ],
        REDIS_FAKE=True,
    )

    @app.register('/cached')
    @app.ps.redis_cache.cached()
    def cached(request):
        return {'key': 'value'}

    @app.register('/cached_keyprefix')
    @app.ps.redis_cache.cached(key_prefix='custom_keyprefix')
    def cached_keyprefix(request):
        return {'firstname': 'Mike', 'gender': 'male'}

    return app


def test_muffin_redis_cache(loop, app, client):
    assert app.ps.redis_cache
    assert app.ps.redis_cache.conn

    response = client.get('/cached')
    assert response.status_code == 200
    assert 'key' in response.json

    response = client.get('/cached_keyprefix')
    assert response.status_code == 200
    assert 'firstname' in response.json

    @asyncio.coroutine
    def exist_key_in_redis():
        return (yield from app.ps.redis_cache.get('view/cached'))

    result = loop.run_until_complete(exist_key_in_redis())
    assert jsonpickle.decode(result) == {'key': 'value'}

    @asyncio.coroutine
    def exist_key_in_redis():
        return (yield from app.ps.redis_cache.get('custom_keyprefix'))

    result = loop.run_until_complete(exist_key_in_redis())
    assert jsonpickle.decode(result) == {'firstname': 'Mike', 'gender': 'male'}



