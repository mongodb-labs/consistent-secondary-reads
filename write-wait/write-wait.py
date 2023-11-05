import bisect
import enum
import logging
import random
import statistics
import time
import uuid
from dataclasses import dataclass, field

from simulate import (
    get_event_loop,
    get_current_ts,
    initiate_logging,
    sleep_random,
    Timestamp,
)

logger = logging.getLogger("write-wait")
WAIT_DURATION = 5
MAX_DELAY = 10 * WAIT_DURATION


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


@dataclass
class Node:
    role: Role
    # Map key to (value, last-written time).
    data: dict[str, tuple[str, OpTime]] = field(default_factory=dict)
    log: list[Write] = field(default_factory=list)
    committed_optime: OpTime = field(default_factory=OpTime.default)
    last_applied_entry: Write | None = None
    nodes: list["Node"] | None = None
    node_replication_positions: dict[int, OpTime] = field(default_factory=dict)
    """Map Node ids to their last-replicated timestamps."""

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
            await sleep_random(MAX_DELAY)
            if self.role is not Role.PRIMARY:
                continue

            await self.write("noop", "")

    async def replicate(self):
        peers = [n for n in self.nodes if n is not self]
        while True:
            await sleep_random(MAX_DELAY)
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
            await sleep_random(MAX_DELAY)
            # Don't use call_later, this can't handle out-of-order messages yet.
            primary.update_secondary_position(self, entry.optime)

    def update_secondary_position(self, secondary: "Node", optime: OpTime):
        if self.role is not Role.PRIMARY:
            return

        # TODO: what about delayed messages / rollback?
        self.node_replication_positions[id(secondary)] = optime
        self.committed_optime = statistics.median(
            self.node_replication_positions.values()
        )

        for n in self.nodes:
            if n is not self:
                get_event_loop().call_later(
                    MAX_DELAY, n.update_committed_optime, self.committed_optime
                )

    def update_committed_optime(self, optime: OpTime):
        self.committed_optime = max(self.committed_optime, optime)

    async def write(self, key: str, value: str):
        """Update a key."""
        optime = OpTime(get_current_ts(), 0)
        if len(self.log) > 0 and self.log[-1].optime.ts == optime.ts:
            optime.i = self.log[-1].optime.i + 1

        w = Write(key=key, value=value, optime=optime)
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        self.data[w.key] = (value, w.optime)
        # TODO: Try a realistic oplog, reserve a future slot and eventually fill it.
        self.log.append(w)
        self.node_replication_positions[id(self)] = optime
        while self.committed_optime < optime:
            await sleep_random(MAX_DELAY)

        await sleep_random(MAX_DELAY)

    async def read(self, key: str) -> str | None:
        """Return a key's latest value."""
        query_start = get_current_ts()
        if self.role is Role.SECONDARY:
            # Wait until replication catches up, aka "barrier".
            while self.last_applied.ts + WAIT_DURATION < query_start:
                await sleep_random(MAX_DELAY)

        # Wait for item's last-written OpTime (last applied entry if no item) to commit,
        # aka "rinse".
        value, last_written_optime = self.data.get(key, (None, self.last_applied))
        while self.committed_optime < last_written_optime:
            await sleep_random(MAX_DELAY)

        return value


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


async def client(
    client_id: int,
    nodes: list[Node],
    client_log: list[ClientLogEntry],
):
    for _ in range(20):
        start = get_current_ts()
        if random.randint(0, 1) == 0:
            # Write to primary.
            value = str(uuid.uuid4())
            logger.info(f"Client {client_id} writing {value} to primary")
            primary = next(n for n in nodes if n.role == Role.PRIMARY)
            await primary.write(key="x", value=value)
            logger.info(f"Client {client_id} wrote {value}")
            client_log.append(
                ClientLogEntry(
                    client_id=client_id,
                    op_type=ClientLogEntry.OpType.Write,
                    start_ts=start,
                    end_ts=get_current_ts(),
                    key="x",
                    value=value,
                )
            )
        else:
            # Read from any node.
            node_index = random.randint(0, len(nodes) - 1)
            node = (nodes)[node_index]
            node_name = f"node {node_index} {node.role.name}"
            logger.info(f"Client {client_id} reading from {node_name}")
            value = await node.read(key="x")
            logger.info(f"Client {client_id} read {value} from {node_name}")
            client_log.append(
                ClientLogEntry(
                    client_id=client_id,
                    op_type=ClientLogEntry.OpType.Read,
                    start_ts=start,
                    end_ts=get_current_ts(),
                    key="x",
                    value=value,
                )
            )


def check_linearizability(client_log: list[ClientLogEntry]) -> None:
    """True if "client_log" is linearizable.

    Based on Gavin Lowe, "Testing for Linearizability", 2016, which summarizes Wing and
    Gong, "Testing and Verifying Concurrent Objects", 1993.
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


async def main():
    primary = Node(role=Role.PRIMARY)
    secondaries = [Node(role=Role.SECONDARY), Node(role=Role.SECONDARY)]
    nodes = [primary] + secondaries
    for n in nodes:
        n.initiate(nodes)

    lp = get_event_loop()
    client_log: list[ClientLogEntry] = []
    tasks = [
        lp.create_task(
            name=f"client {i}",
            coro=client(client_id=i, nodes=nodes, client_log=client_log),
        )
        for i in range(10)
    ]

    for t in tasks:
        await t

    lp.stop()
    logging.info(f"Finished after {get_current_ts()} ms (simulated)")
    check_linearizability(client_log)


if __name__ == "__main__":
    initiate_logging()
    event_loop = get_event_loop()
    event_loop.create_task("main", main())
    event_loop.run()
