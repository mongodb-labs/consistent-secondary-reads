import bisect
import logging
import statistics
from dataclasses import dataclass, field
from enum import Enum, auto

from simulate import (
    get_event_loop,
    get_current_ts,
    initiate_logging,
    sleep_random,
    Timestamp,
)

logger = logging.getLogger("write-wait")
D = 5  # Write-wait duration in milliseconds.
MAX_DELAY = 10 * D


class Role(Enum):
    PRIMARY = auto()
    SECONDARY = auto()


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
    optime: OpTime


@dataclass
class Node:
    role: Role
    data: dict[str, OpTime] = field(default_factory=dict)
    """Data is a kv store. Values are actually the last-written optime."""
    log: list[Write] = field(default_factory=list)
    committed_optime: OpTime = field(default_factory=OpTime.default)
    last_applied_entry: Write | None = None
    nodes: list["Node"] | None = None
    node_replication_positions: dict[int, OpTime] = field(default_factory=dict)
    """Map Node ids to their last-replicated timestamps."""

    def initiate(self, nodes: list["Node"]):
        self.nodes = nodes[:]
        self.node_replication_positions = {id(n): OpTime.default() for n in nodes}
        get_event_loop().create_task(self.noop_writer())
        get_event_loop().create_task(self.replicate())

    async def noop_writer(self):
        while True:
            await sleep_random(MAX_DELAY)
            if self.role is not Role.PRIMARY:
                continue

            await self.write("noop")

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
                # TODO: rollback if my log is longer than primary's.
                continue

            # Find the next entry to replicate and apply.
            if len(self.log) == 0:
                i = 0
            else:
                i = bisect.bisect_right(primary.log, self.log[-1])

            entry = primary.log[i]
            self.data[entry.key] = entry.optime
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

    async def write(self, key: str) -> OpTime:
        """Update a key and return the write's OpTime."""
        optime = OpTime(get_current_ts(), 0)
        if len(self.log) > 0 and self.log[-1].optime.ts == optime.ts:
            optime.i = self.log[-1].optime.i + 1

        w = Write(key=key, optime=optime)
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        self.data[w.key] = w.optime
        # TODO: Try a realistic oplog, reserve a future slot and eventually fill it.
        self.log.append(w)
        self.node_replication_positions[id(self)] = optime
        while self.committed_optime < optime:
            # TODO: abort on role change or rollback.
            await sleep_random(MAX_DELAY)

        # TODO: abort on role change or rollback.
        await sleep_random(MAX_DELAY)
        return optime

    async def read(self, key: str) -> OpTime:
        """Return a key's last write OpTime."""
        query_start = get_current_ts()
        if self.role is Role.SECONDARY:
            # Wait until replication catches up.
            while not self.log or self.log[-1].optime.ts + D < query_start:
                # TODO: abort on role change or rollback.
                await sleep_random(MAX_DELAY)

        # Speculative majority wait.
        last_written_optime = self.data[key]
        while self.committed_optime < last_written_optime:
            # TODO: abort on role change or rollback.
            await sleep_random(MAX_DELAY)

        return last_written_optime


async def client(client_id: int, primary: Node, secondary: Node):
    write_optime = await primary.write(key="x")
    logger.info(f"client {client_id} write OpTime: {write_optime}")
    read_optime = await secondary.read(key="x")
    logger.info(f"client {client_id} secondary read OpTime: {read_optime}")
    assert read_optime >= write_optime, f"client {client_id} stale read"


async def main():
    primary = Node(role=Role.PRIMARY)
    secondaries = [Node(role=Role.SECONDARY), Node(role=Role.SECONDARY)]
    nodes = [primary] + secondaries
    for n in nodes:
        n.initiate(nodes)

    lp = get_event_loop()
    t1 = lp.create_task(client(client_id=1, primary=primary, secondary=secondaries[0]))
    t2 = lp.create_task(client(client_id=2, primary=primary, secondary=secondaries[0]))

    await t1
    await t2
    lp.stop()


if __name__ == "__main__":
    initiate_logging()
    event_loop = get_event_loop()
    event_loop.create_task(main())
    event_loop.run()
    print(f"Finished after {get_current_ts()} ms (simulated)")
