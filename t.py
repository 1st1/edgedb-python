from __future__ import annotations

from typing import *  # NoQA

import collections
import functools
import hashlib
import io
import itertools
import os
import pathlib
import tempfile
import typing

import edgedb

from edgedb.protocol import dstructs

from edb.edgeql.quote import quote_literal as ql, quote_ident as qi
from edb.common import binwrapper


# This should be a good hint that EdgeDB dumps are not text files:
#
# * "\xFF" is invalid utf-8;
# * "\xD8\x00" is invalid utf-16-le
# * "\xFF\xD8\x00\x00\xD8" is also invalid utf-16/32 both le & be
#
# Presense of "\x00" is also a hint to tools like "git" that this is
# a binary file.
HEADER_TITLE = b'\xFF\xD8\x00\x00\xD8EDGEDB\x00DUMP\x00'
HEADER_TITLE_LEN = len(HEADER_TITLE)

COPY_BUFFER_SIZE = 1024 * 1024 * 10

DUMP_PROTO_VER = 1
MAX_SUPPORTED_DUMP_VER = 1


class DumpImpl:

    conn: edgedb.BlockingIOConnection

    # Mapping of `schema_object_id` to a list of data block sizes/checksums.
    blocks_datainfo: typing.Dict[str, List[Tuple[int, bytes]]]

    def __init__(self, conn: edgedb.BlockingIOConnection) -> None:
        self.conn = conn
        self.blocks_datainfo = {}

    def _data_callback(
        self,
        tmpdir: pathlib.Path,
        data: dstructs.DumpDataBlock,
    ) -> None:
        fn = tmpdir / data.schema_object_id.hex
        with open(fn, 'ba+') as f:
            f.write(data.data)

        self.blocks_datainfo.setdefault(data.schema_object_id, []).append(
            (
                len(data.data),
                hashlib.sha1(data.data).digest()
            )
        )

    def _serialize_header(self, desc: dstructs.DumpDesc) -> bytes:
        buf = io.BytesIO()
        binbuf = binwrapper.BinWrapper(buf)

        binbuf.write_ui64(desc.server_ts)
        binbuf.write_len32_prefixed_bytes(desc.server_version)
        binbuf.write_len32_prefixed_bytes(desc.schema)

        binbuf.write_ui64(len(desc.blocks))
        for block in desc.blocks:
            block_di = self.blocks_datainfo[block.schema_object_id]

            if len(block_di) != block.data_blocks_count:
                raise RuntimeError(
                    'server reported data blocks count does not match '
                    'actual received')

            binbuf.write_bytes(block.schema_object_id.bytes)
            print('DUMP', block.schema_object_id.bytes)
            binbuf.write_ui32(len(block.schema_deps))
            for dep in block.schema_deps:
                binbuf.write_bytes(dep.bytes)
            binbuf.write_len32_prefixed_bytes(block.type_desc)
            binbuf.write_ui64(block.data_size)

            binbuf.write_ui64(block.data_blocks_count)
            total_size = 0
            for data_size, data_hash in block_di:
                binbuf.write_ui64(data_size)
                binbuf.write_bytes(data_hash)
                total_size += data_size

            if total_size != block.data_size:
                raise RuntimeError(
                    'server reported data block size does not match '
                    'actual received')

        return buf.getvalue()

    def dump(self, outfn: os.PathLike) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = pathlib.Path(tmp)

            desc = self.conn._dump(
                on_data=functools.partial(self._data_callback, tmpdir))

            header = self._serialize_header(desc)
            with open(outfn, 'wb+') as outf:
                buf = binwrapper.BinWrapper(outf)

                buf.write_bytes(HEADER_TITLE)
                buf.write_ui64(DUMP_PROTO_VER)

                buf.write_bytes(hashlib.sha1(header).digest())
                buf.write_ui64(len(header))
                buf.write_bytes(header)

                for block in desc.blocks:
                    datafn = tmpdir / block.schema_object_id.hex
                    with open(datafn, 'br') as dataf:
                        while True:
                            data = dataf.read(COPY_BUFFER_SIZE)
                            if not data:
                                break
                            buf.write_bytes(data)


class DumpBlockInfo(NamedTuple):
    schema_object_id: bytes
    schema_deps: typing.List[bytes]
    type_desc: bytes

    data_offset: int
    data_size: int
    data_blocks: List[Tuple[int, bytes]]


class DumpInfo(NamedTuple):

    blocks: List[DumpBlockInfo]
    schema: bytes
    header_offset: int
    filename: str
    server_ts: int
    server_version: bytes
    dump_version: int


class RestoreImpl:

    def __init__(self, conn: edgedb.BlockingIOConnection) -> None:
        self.conn = conn

    def _parse(
        self,
        dumpfn: os.PathLike,
    ) -> DumpInfo:

        with open(dumpfn, 'rb') as f:
            buf = binwrapper.BinWrapper(f)

            header = buf.read_bytes(HEADER_TITLE_LEN)
            if header != HEADER_TITLE:
                raise RuntimeError('not an EdgeDB dump')

            dump_ver = buf.read_ui64()
            if dump_ver > MAX_SUPPORTED_DUMP_VER:
                raise RuntimeError(f'dump version {dump_ver} is not supported')

            header_hash = buf.read_bytes(20)
            header_len = buf.read_ui64()
            header_bytes = buf.read_bytes(header_len)

            if hashlib.sha1(header_bytes).digest() != header_hash:
                raise RuntimeError(
                    'dump integrity is compamised: header data does not match '
                    'the checksum')

            header_buf = binwrapper.BinWrapper(io.BytesIO(header_bytes))

            server_ts = header_buf.read_ui64()
            server_version = header_buf.read_len32_prefixed_bytes()

            schema = header_buf.read_len32_prefixed_bytes()

            blocks: List[DumpBlockInfo] = []
            blocks_num = header_buf.read_ui64()
            offset = 0
            for _ in range(blocks_num):
                schema_object_id = header_buf.read_bytes(16)
                schema_deps: List[bytes] = []

                deps_num = header_buf.read_ui32()
                for _ in range(deps_num):
                    schema_deps.append(header_buf.read_bytes(16))

                type_desc = header_buf.read_len32_prefixed_bytes()
                block_size = header_buf.read_ui64()
                data_count = header_buf.read_ui64()

                data_blocks: List[Tuple[int, bytes]] = []
                for _ in range(data_count):
                    data_blocks.append(
                        (
                            header_buf.read_ui64(),
                            header_buf.read_bytes(20),
                        )
                    )

                blocks.append(
                    DumpBlockInfo(
                        schema_object_id=schema_object_id,
                        schema_deps=schema_deps,
                        type_desc=type_desc,
                        data_offset=offset,
                        data_size=block_size,
                        data_blocks=data_blocks,
                    )
                )

                offset += block_size

            return DumpInfo(
                blocks=blocks,
                schema=schema,
                header_offset=f.tell(),
                filename=dumpfn,
                server_ts=server_ts,
                server_version=server_version,
                dump_version=dump_ver,
            )

    def _interleave(
        self,
        factor: int,
        dumpfn: os.PathLike,
        info: DumpInfo
    ) -> Iterable[Tuple[bytes, bytes]]:

        def worker(
            blocks: Deque[DumpBlockInfo]
        ) -> Iterable[Tuple[bytes, bytes]]:
            while True:
                try:
                    block = blocks.popleft()
                except IndexError:
                    break

                with open(dumpfn, 'rb') as f:
                    offset = 0
                    buf = binwrapper.BinWrapper(f)
                    for dlen, dhash in block.data_blocks:
                        f.seek(info.header_offset + block.data_offset + offset)
                        data = buf.read_bytes(dlen)
                        if hashlib.sha1(data).digest() != dhash:
                            raise RuntimeError(
                                'dump integrity is compamised: data block '
                                'does not match the checksum')
                        offset += dlen
                        yield (block.schema_object_id, data)

        if factor <= 0:
            raise ValueError('invalid interleave factor')

        blocks: Deque[DumpBlockInfo] = collections.deque(info.blocks)
        workers = itertools.cycle(worker(blocks) for _ in range(factor))

        while True:
            stopped = 0
            for _ in range(factor):
                wrk = next(workers)
                try:
                    yield next(wrk)
                except StopIteration:
                    stopped += 1
            if stopped == factor:
                # All workers are exhausted
                return

    def _restore(
        self,
        conn: edgedb.BlockingIOConnection,
        dumpfn: os.PathLike,
        info: DumpInfo,
    ) -> None:
        data_gen = self._interleave(4, dumpfn, info)
        conn._restore(
            schema=info.schema,
            blocks=[(b.schema_object_id, b.type_desc) for b in info.blocks],
            data_gen=data_gen,
        )

    def restore(self, dbname: str, dumpfn: os.PathLike) -> None:
        self.conn.execute(f'CREATE DATABASE {qi(dbname)}')

        rconn = edgedb.connect(database=dbname)
        try:
            info = self._parse(dumpfn)
            print("BEFORE")
            self._restore(rconn, dumpfn, info)
        except BaseException:
            print("ERROR")
            rconn.close()
            self.conn.execute(f'DROP DATABASE {qi(dbname)}')
            raise
        finally:
            print("FINALLY")
            rconn.close()


def main():

    for enc in {'utf-8', 'utf-16-be', 'utf-16-le', 'utf-32-be', 'utf-32-le'}:
        try:
            HEADER_TITLE.decode(enc)
        except UnicodeDecodeError:
            pass
        else:
            raise AssertionError

    conn = edgedb.connect(database='edgedb_bench')

    import time
    st = time.time()
    dumper = DumpImpl(conn)
    dumper.dump('out.dump')
    print(time.time() - st)

    rest = RestoreImpl(conn)
    # rest.restore('edgedb_bench_restored', 'out.dump')
    rest.restore('test1', 'out.dump')


main()
