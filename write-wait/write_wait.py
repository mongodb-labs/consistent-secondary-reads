import argparse
import bisect
import enum
import logging
import random
import statistics
import time
import uuid
from dataclasses import dataclass, field

import hydra
import dvclive
from omegaconf import DictConfig, OmegaConf

from simulate import (
    get_event_loop,
    get_current_ts,
    initiate_logging,
    sleep,
    Timestamp,
)

logger = logging.getLogger("write-wait")


class Role(enum.Enum):
    PRIMARY = enum.auto()
    SECONDARY = enum.auto()


@dataclass(order=True)
class OpTime:
    """A hybrid logical clock (HLC)."""

    ts: Timestamp
    i: int

    @classmethod
    def default(cls) -> "OpTime":
        return OpTime(-1, -1)


@dataclass(order=True)
class Write:
    key: str = field(compare=False)
    value: str = field(compare=False)
    optime: OpTime


class Node:
    def __init__(self, role: Role, cfg: DictConfig, prng: random.Random):
        self.role = role
        self.prng = prng
        # Map key to (value, last-written time).
        self.data: dict[str, tuple[str, OpTime]] = {}
        self.log: list[Write] = []
        self.committed_optime: OpTime = OpTime.default()
        self.last_applied_entry: Write | None = None
        self.nodes: list["Node"] | None = None
        # Map Node ids to their last-replicated timestamps.
        self.node_replication_positions: dict[int, OpTime] = {}
        self.one_way_latency: int = cfg.one_way_latency
        self.noop_rate: int = cfg.noop_rate
        self.write_wait: int = cfg.write_wait

    def initiate(self, nodes: list["Node"]):
        self.nodes = nodes[:]
        self.node_replication_positions = {id(n): OpTime.default() for n in nodes}
        get_event_loop().create_task("no-op writer", self.noop_writer())
        get_event_loop().create_task("replication", self.replicate())

    @property
    def last_applied(self) -> OpTime:
        return self.log[-1].optime if self.log else OpTime.default()

    async def noop_writer(self):
        while True:
            await sleep(self.noop_rate)
            if self.role is not Role.PRIMARY:
                continue

            await self.write("noop", "")

    async def replicate(self):
        peers = [n for n in self.nodes if n is not self]
        while True:
            await sleep(self.one_way_latency_value())  # Receive an entry from primary.
            if self.role is Role.PRIMARY:
                continue

            try:
                primary = next(n for n in peers if n.role is Role.PRIMARY)
            except StopIteration:
                continue  # No primary.

            if len(self.log) >= len(primary.log):
                continue

            # Find the next entry to replicate and apply.
            if len(self.log) == 0:
                i = 0
            else:
                i = bisect.bisect_right(primary.log, self.log[-1])

            entry = primary.log[i]
            self.data[entry.key] = (entry.value, entry.optime)
            self.log.append(entry)
            self.node_replication_positions[id(self)] = entry.optime
            get_event_loop().call_later(
                self.one_way_latency_value(),
                primary.update_secondary_position,
                secondary=self,
                optime=entry.optime,
            )

    def update_secondary_position(self, secondary: "Node", optime: OpTime):
        if self.role is not Role.PRIMARY:
            return

        # Handle out-of-order messages with max(), assume no rollbacks.
        self.node_replication_positions[id(secondary)] = max(
            self.node_replication_positions[id(secondary)], optime
        )
        self.committed_optime = statistics.median(
            self.node_replication_positions.values()
        )

        for n in self.nodes:
            if n is not self:
                get_event_loop().call_later(
                    self.one_way_latency_value(),
                    n.update_committed_optime,
                    self.committed_optime,
                )

    def update_committed_optime(self, optime: OpTime):
        self.committed_optime = max(self.committed_optime, optime)

    async def write(self, key: str, value: str):
        """Update a key."""
        write_start = get_current_ts()
        optime = OpTime(write_start, 0)
        if len(self.log) > 0 and self.log[-1].optime.ts == optime.ts:
            optime.i = self.log[-1].optime.i + 1

        w = Write(key=key, value=value, optime=optime)
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        self.data[w.key] = (value, w.optime)
        # TODO: Try a realistic oplog, reserve a future slot and eventually fill it.
        self.log.append(w)
        self.node_replication_positions[id(self)] = optime
        assert write_start == get_current_ts()  # Assume no time since the write began.
        await sleep(self.write_wait)
        while self.committed_optime < optime:
            await sleep(1)

    async def read(self, key: str) -> str | None:
        """Return a key's latest value."""
        query_start = get_current_ts()
        if self.role is Role.SECONDARY:
            # Wait until replication catches up, aka "barrier".
            while self.last_applied.ts + self.write_wait < query_start:
                await sleep(1)

        # Wait for item's last-written OpTime (last applied entry if no item) to commit,
        # aka "rinse".
        value, last_written_optime = self.data.get(key, (None, self.last_applied))
        while self.committed_optime < last_written_optime:
            await sleep(1)

        return value

    def one_way_latency_value(self) -> int:
        return round(self.prng.expovariate(1 / self.one_way_latency))


@dataclass
class ClientLogEntry:
    class OpType(enum.Enum):
        Write = enum.auto()
        Read = enum.auto()

    client_id: int
    op_type: OpType
    start_ts: Timestamp
    end_ts: Timestamp
    key: str
    value: str | None = None

    @property
    def duration(self) -> int:
        assert self.end_ts >= self.start_ts
        return self.end_ts - self.start_ts


async def reader(
    client_id: int,
    start_ts: Timestamp,
    nodes: list[Node],
    client_log: list[ClientLogEntry],
    prng: random.Random,
):
    await sleep(start_ts)
    assert get_current_ts() == start_ts  # Deterministic scheduling!
    # Read from any node.
    node_index = prng.randint(0, len(nodes) - 1)
    node = (nodes)[node_index]
    node_name = f"node {node_index} {node.role.name}"
    logger.info(f"Client {client_id} reading from {node_name}")
    value = await node.read(key="x")
    logger.info(f"Client {client_id} read {value} from {node_name}")
    client_log.append(
        ClientLogEntry(
            client_id=client_id,
            op_type=ClientLogEntry.OpType.Read,
            start_ts=start_ts,
            end_ts=get_current_ts(),
            key="x",
            value=value,
        )
    )


async def writer(
    client_id: int,
    start_ts: Timestamp,
    primary: Node,
    client_log: list[ClientLogEntry],
):
    await sleep(start_ts)
    assert get_current_ts() == start_ts  # Deterministic scheduling!
    value = str(uuid.uuid4())
    logger.info(f"Client {client_id} writing {value} to primary")
    await primary.write(key="x", value=value)
    logger.info(f"Client {client_id} wrote {value}")
    client_log.append(
        ClientLogEntry(
            client_id=client_id,
            op_type=ClientLogEntry.OpType.Write,
            start_ts=start_ts,
            end_ts=get_current_ts(),
            key="x",
            value=value,
        )
    )


def do_linearizability_check(client_log: list[ClientLogEntry]) -> None:
    """Throw exception if "client_log" is not linearizable.

    Based on Lowe, "Testing for Linearizability", 2016, which summarizes Wing & Gong,
    "Testing and Verifying Concurrent Objects", 1993. Don't do Lowe's memoization trick.
    """

    def linearize(
        log: list[ClientLogEntry], model: dict
    ) -> list[ClientLogEntry] | None:
        """Try linearizing a suffix of the log with the KV store "model" in some state.

        Return a linearization if possible, else None.
        """
        if len(log) == 0:
            return log  # Empty history is already linearized.

        for i, entry in enumerate(log):
            # Try linearizing "entry" at history's start. No other entry's end can
            # precede this entry's start.
            if any(e for e in log if e is not entry and e.end_ts < entry.start_ts):
                continue

            if entry.op_type is ClientLogEntry.OpType.Write:
                # What would the KV store contain if we did this write now?
                model_prime = model.copy()
                model_prime[entry.key] = entry.value
            else:
                # What would this query return if we ran it now?
                if model.get(entry.key) != entry.value:
                    continue  # "entry" can't be linearized first.
                model_prime = model

            # Try to linearize the rest of the log with the KV store in this state.
            log_prime = log.copy()
            log_prime.pop(i)
            linearization = linearize(log_prime, model_prime)
            if linearization is not None:
                return [entry] + linearization

        return None

    check_start = time.monotonic()
    # Sort by start_ts to make the search succeed sooner.
    result = linearize(sorted(client_log, key=lambda y: y.start_ts), {})
    check_duration = time.monotonic() - check_start
    if result is None:
        raise Exception("not linearizable!")

    logging.info(
        f"Linearization of {len(client_log)} entries took {check_duration:.2f} sec:"
    )
    for x in result:
        logging.info(x)


def log_metrics(live: dvclive.Live, client_log: list[ClientLogEntry]):
    writes, reads = 0, 0
    write_time, read_time = 0, 0
    for entry in client_log:
        if entry.op_type == ClientLogEntry.OpType.Write:
            writes += 1
            write_time += entry.duration
        else:
            reads += 1
            read_time += entry.duration

    live.log_metric("mean_write_latency", write_time / writes)
    live.log_metric("mean_read_latency", read_time / reads)


async def main_coro(cfg: DictConfig, live: dvclive.Live):
    logging.info(cfg)
    seed = int(time.monotonic_ns() if cfg.seed is None else cfg.seed)
    logging.info(f"Seed {seed}")
    prng = random.Random(seed)
    primary = Node(role=Role.PRIMARY, cfg=cfg, prng=prng)
    secondaries = [
        Node(role=Role.SECONDARY, cfg=cfg, prng=prng),
        Node(role=Role.SECONDARY, cfg=cfg, prng=prng),
    ]
    nodes = [primary] + secondaries
    for n in nodes:
        n.initiate(nodes)

    lp = get_event_loop()
    client_log: list[ClientLogEntry] = []
    tasks = []
    start_ts = 0
    # Schedule some tasks with Poisson start times. Each does one read or one write.
    for i in range(cfg.operations):
        start_ts += round(prng.expovariate(1 / cfg.interarrival))
        if prng.randint(0, 1) == 0:
            coro = writer(
                client_id=i, start_ts=start_ts, primary=primary, client_log=client_log
            )
        else:
            coro = reader(
                client_id=i,
                start_ts=start_ts,
                nodes=nodes,
                client_log=client_log,
                prng=prng,
            )
        tasks.append(lp.create_task(name=f"client {i}", coro=coro))

    for t in tasks:
        await t

    lp.stop()
    logging.info(f"Finished after {get_current_ts()} ms (simulated)")
    log_metrics(live, client_log)
    logging.info(live.summary)
    if cfg.check_linearizability:
        do_linearizability_check(client_log)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check-linearizability",
        default=False,
        action="store_true",
        help="Check linearizability at the end (could be slow)",
    )
    parser.add_argument(
        "--one-way-latency",
        default=10,
        help="Latency between replicas (we ignore client-server latency)",
    )
    parser.add_argument(
        "--noop-rate",
        default=1,
        help="Time between no-op writes",
    )
    parser.add_argument(
        "--write-wait",
        default=20,
        help="Time to wait before acknowledging a write",
        type=int,
    )
    parser.add_argument(
        "--operations",
        default=10,
        help="Number of operations (reads + writes)",
        type=int,
    )
    parser.add_argument(
        "--interarrival",
        default=10,
        help="Mean time units between operations",
        type=int,
    )
    parser.add_argument(
        "--seed",
        default=int(time.monotonic_ns()),
        help="For repeatability",
        type=int,
    )
    return parser.parse_args()


# TODO: need all these defaults?
@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    initiate_logging()
    event_loop = get_event_loop()
    with dvclive.Live() as dvc_live:
        event_loop.create_task("main", main_coro(cfg=cfg.write_wait, live=dvc_live))
        event_loop.run()


if __name__ == "__main__":
    main()
