import asyncio
import collections

from edgedb.protocol.protocol import Tuple, NamedTuple  # NoQA
from edgedb.protocol.protocol import Set, Object, Array  # NoQA
from edgedb.protocol.protocol import Protocol


_ConnectionParameters = collections.namedtuple(
    'ConnectionParameters',
    [
        'user',
        'password',
        'database',
        'ssl',
        'connect_timeout',
        'server_settings',
    ])


class Connection:

    def __init__(self, transport, protocol):
        self._transport = transport
        self._protocol = protocol

    async def fetch(self, query, *args, **kwargs):
        st = await self._protocol.prepare('', query)
        return await self._protocol.bind_execute(st, args, kwargs)


async def connect(host, port, dbname):
    loop = asyncio.get_event_loop()

    con_param = _ConnectionParameters('yury', '', dbname, False, 1, None)

    connected_fut = loop.create_future()
    protocol_factory = lambda: Protocol((host, port), connected_fut,
                                        con_param, loop)
    tr, pr = await loop.create_connection(protocol_factory, host, port)
    await connected_fut
    con = Connection(tr, pr)
    return con
