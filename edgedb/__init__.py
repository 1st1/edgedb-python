import asyncio
import collections

from edgedb.protocol.protocol import Tuple, NamedTuple, Set, Object  # NoQA
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

    async def fetch(self, query, *args):
        st = await self._protocol.prepare('', query)
        await self._protocol.bind_execute(st, args)
        print('FETCH', st)


async def connect(host, port):
    loop = asyncio.get_event_loop()

    con_param = _ConnectionParameters('yury', '', 'yury', False, 1, None)

    connected_fut = loop.create_future()
    protocol_factory = lambda: Protocol((host, port), connected_fut,
                                        con_param, loop)
    tr, pr = await loop.create_connection(protocol_factory, host, port)
    await connected_fut
    con = Connection(tr, pr)
    return con
