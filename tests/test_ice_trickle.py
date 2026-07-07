import asyncio
import unittest
from typing import Optional

from aioice import Candidate, ice, stun

from .utils import asynctest


class IceTrickleTest(unittest.TestCase):
    def assertCandidateTypes(self, conn: ice.Connection, expected: set[str]) -> None:
        types = set([c.type for c in conn.local_candidates])
        self.assertEqual(types, expected)

    def tearDown(self) -> None:
        ice.PAC_TIMEOUT = 39.5
        stun.RETRY_MAX = 6

    @asynctest
    async def test_connect(self) -> None:
        conn_a = ice.Connection(ice_controlling=True)
        conn_b = ice.Connection(ice_controlling=False)

        # invite
        await conn_a.gather_candidates()
        conn_b.remote_username = conn_a.local_username
        conn_b.remote_password = conn_a.local_password

        # accept
        await conn_b.gather_candidates()
        conn_a.remote_username = conn_b.local_username
        conn_a.remote_password = conn_b.local_password

        # we should only have host candidates
        self.assertCandidateTypes(conn_a, set(["host"]))
        self.assertCandidateTypes(conn_b, set(["host"]))

        # there should be a default candidate for component 1
        candidate = conn_a.get_default_candidate(1)
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.type, "host")

        # there should not be a default candidate for component 2
        candidate = conn_a.get_default_candidate(2)
        self.assertIsNone(candidate)

        async def add_candidates_later(a: ice.Connection, b: ice.Connection) -> None:
            await asyncio.sleep(0.1)
            for candidate in b.local_candidates:
                await a.add_remote_candidate(candidate)
                await asyncio.sleep(0.1)
            await a.add_remote_candidate(None)

        # connect
        await asyncio.gather(
            conn_a.connect(),
            conn_b.connect(),
            add_candidates_later(conn_a, conn_b),
            add_candidates_later(conn_b, conn_a),
        )

        # send data a -> b
        await conn_a.send(b"howdee")
        data = await conn_b.recv()
        self.assertEqual(data, b"howdee")

        # send data b -> a
        await conn_b.send(b"gotcha")
        data = await conn_a.recv()
        self.assertEqual(data, b"gotcha")

        # close
        await conn_a.close()
        await conn_b.close()

    @asynctest
    async def test_gather_candidates_callback(self) -> None:
        """
        Each gathered candidate is passed to the `on_local_candidate` callback.
        """
        candidates: list[Candidate] = []
        conn = ice.Connection(
            ice_controlling=True, on_local_candidate=candidates.append
        )
        await conn.gather_candidates()
        self.assertGreater(len(candidates), 0)
        self.assertEqual(candidates, conn.local_candidates)
        await conn.close()

    @asynctest
    async def test_connect_while_gathering(self) -> None:
        """
        Full trickle: connect() is called as soon as gathering starts, and
        candidates are exchanged as they are discovered.
        """
        conn_a = ice.Connection(ice_controlling=True)
        conn_b = ice.Connection(ice_controlling=False)

        # exchange credentials, as would happen over the signaling channel
        conn_a.remote_username = conn_b.local_username
        conn_a.remote_password = conn_b.local_password
        conn_b.remote_username = conn_a.local_username
        conn_b.remote_password = conn_a.local_password

        async def gather_and_trickle(
            conn: ice.Connection, peer: ice.Connection
        ) -> None:
            # deliver candidates to the peer in order, with `None` last
            queue: asyncio.Queue[Optional[Candidate]] = asyncio.Queue()
            conn.on_local_candidate = queue.put_nowait

            async def deliver() -> None:
                while True:
                    candidate = await queue.get()
                    await peer.add_remote_candidate(candidate)
                    if candidate is None:
                        return

            deliver_task = asyncio.ensure_future(deliver())
            await conn.gather_candidates()
            queue.put_nowait(None)
            await deliver_task

        # start gathering, then connect before gathering has completed
        task_a = asyncio.ensure_future(gather_and_trickle(conn_a, conn_b))
        task_b = asyncio.ensure_future(gather_and_trickle(conn_b, conn_a))
        await asyncio.sleep(0)
        await asyncio.gather(conn_a.connect(), conn_b.connect(), task_a, task_b)

        # send data a -> b
        await conn_a.send(b"howdee")
        data = await conn_b.recv()
        self.assertEqual(data, b"howdee")

        # send data b -> a
        await conn_b.send(b"gotcha")
        data = await conn_a.recv()
        self.assertEqual(data, b"gotcha")

        # close
        await conn_a.close()
        await conn_b.close()

    @asynctest
    async def test_delayed_remote_candidates(self) -> None:
        """
        All initially-known pairs fail before usable candidates trickle in.

        The connection must not declare failure while more candidates may
        still be provided.
        """
        # lower STUN retries so the initial pair fails quickly
        stun.RETRY_MAX = 1

        conn_a = ice.Connection(ice_controlling=True)
        conn_b = ice.Connection(ice_controlling=False)

        await conn_a.gather_candidates()
        await conn_b.gather_candidates()
        conn_a.remote_username = conn_b.local_username
        conn_a.remote_password = conn_b.local_password
        conn_b.remote_username = conn_a.local_username
        conn_b.remote_password = conn_a.local_password

        # initially, conn_a only knows an unreachable candidate
        await conn_a.add_remote_candidate(
            Candidate.from_sdp(
                "6815297761 1 udp 659136 192.0.2.1 31102 typ host generation 0"
            )
        )

        async def add_candidates_later() -> None:
            # wait for the unreachable candidate's check to fail
            await asyncio.sleep(2)
            for candidate in conn_b.local_candidates:
                await conn_a.add_remote_candidate(candidate)
            await conn_a.add_remote_candidate(None)
            for candidate in conn_a.local_candidates:
                await conn_b.add_remote_candidate(candidate)
            await conn_b.add_remote_candidate(None)

        await asyncio.gather(conn_a.connect(), conn_b.connect(), add_candidates_later())

        # send data a -> b
        await conn_a.send(b"howdee")
        data = await conn_b.recv()
        self.assertEqual(data, b"howdee")

        # close
        await conn_a.close()
        await conn_b.close()

    @asynctest
    async def test_pac_timeout(self) -> None:
        """
        If all pairs fail and end-of-candidates is never signaled, the
        connection fails once the PAC timer (RFC 8863) expires.
        """
        # lower STUN retries and the PAC timeout to keep the test fast
        stun.RETRY_MAX = 1
        ice.PAC_TIMEOUT = 1

        conn = ice.Connection(ice_controlling=True)
        await conn.gather_candidates()
        conn.remote_username = "foo"
        conn.remote_password = "bar"
        await conn.add_remote_candidate(
            Candidate.from_sdp(
                "6815297761 1 udp 659136 192.0.2.1 31102 typ host generation 0"
            )
        )
        with self.assertRaises(ConnectionError) as cm:
            await conn.connect()
        self.assertEqual(str(cm.exception), "ICE negotiation failed")
        await conn.close()

    @asynctest
    async def test_pac_timeout_no_candidates(self) -> None:
        """
        If no remote candidate ever arrives and end-of-candidates is never
        signaled, the connection fails once the PAC timer expires.
        """
        ice.PAC_TIMEOUT = 1

        conn = ice.Connection(ice_controlling=True)
        await conn.gather_candidates()
        conn.remote_username = "foo"
        conn.remote_password = "bar"
        with self.assertRaises(ConnectionError) as cm:
            await conn.connect()
        self.assertEqual(str(cm.exception), "ICE negotiation failed")
        await conn.close()

    @asynctest
    async def test_early_check_before_credentials(self) -> None:
        """
        Connectivity checks arriving before the remote credentials are known
        are queued and processed once connect() is called.
        """
        conn_a = ice.Connection(ice_controlling=True)
        conn_b = ice.Connection(ice_controlling=False)

        await conn_a.gather_candidates()
        await conn_b.gather_candidates()

        # conn_b has conn_a's credentials and candidates
        conn_b.remote_username = conn_a.local_username
        conn_b.remote_password = conn_a.local_password
        for candidate in conn_a.local_candidates:
            await conn_b.add_remote_candidate(candidate)
        await conn_b.add_remote_candidate(None)

        # conn_a has a candidate pair, but neither conn_b's credentials nor
        # its real candidates yet, so conn_b's checks arrive from unknown
        # addresses and would trigger peer-reflexive checks
        await conn_a.add_remote_candidate(
            Candidate.from_sdp(
                "6815297761 1 udp 659136 192.0.2.1 31102 typ host generation 0"
            )
        )

        # conn_b starts connecting; its checks reach conn_a before conn_a
        # has learnt conn_b's credentials, and must be queued
        connect_b = asyncio.ensure_future(conn_b.connect())
        await asyncio.sleep(0.5)
        self.assertGreater(len(conn_a._early_checks), 0)

        # conn_a now learns conn_b's credentials and candidates, and connects
        conn_a.remote_username = conn_b.local_username
        conn_a.remote_password = conn_b.local_password
        for candidate in conn_b.local_candidates:
            await conn_a.add_remote_candidate(candidate)
        await conn_a.add_remote_candidate(None)
        await asyncio.gather(conn_a.connect(), connect_b)

        # send data a -> b
        await conn_a.send(b"howdee")
        data = await conn_b.recv()
        self.assertEqual(data, b"howdee")

        # close
        await conn_a.close()
        await conn_b.close()

    @asynctest
    async def test_close_during_gathering(self) -> None:
        """
        Closing the connection while gathering is in progress releases all
        sockets, including those created after close().
        """
        conn = ice.Connection(ice_controlling=True)
        gather_task = asyncio.ensure_future(conn.gather_candidates())
        await asyncio.sleep(0)
        await conn.close()
        await gather_task
        self.assertEqual(conn._protocols, [])
        self.assertEqual(conn.local_candidates, [])

    @asynctest
    async def test_gather_candidates_callback_exception(self) -> None:
        """
        An exception raised by the `on_local_candidate` callback does not
        abort candidate gathering.
        """

        def misbehaving_callback(candidate: Candidate) -> None:
            raise RuntimeError("boom")

        conn = ice.Connection(
            ice_controlling=True, on_local_candidate=misbehaving_callback
        )
        await conn.gather_candidates()
        self.assertGreater(len(conn.local_candidates), 0)
        await conn.close()
