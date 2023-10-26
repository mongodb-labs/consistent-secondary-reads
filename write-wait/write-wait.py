import bisect
import statistics
from dataclasses import dataclass, field
from enum import Enum, auto

from simulate import Loop, Sleep, get_current_ts, Timestamp

D = 5  # Write-wait duration in milliseconds.


class Role(Enum):
    PRIMARY = auto()
    SECONDARY = auto()


class Result(Enum):
    OK = auto()
    ERROR = auto()


@dataclass
class Write:
    key: str
    ts: Timestamp


@dataclass
class LogEntry:
    write: Write
    ts: Timestamp

    def __lt__(self, other: "LogEntry"):
        # For bisect's sake.
        return self.ts < other.ts


@dataclass
class Node:
    role: Role
    data: dict[str, Timestamp] = field(default_factory=dict)
    """A key-value store. Values are actually the last-written timestamp."""
    log: list[LogEntry] = field(default_factory=list)
    committed_ts: Timestamp = -1
    last_applied_entry: LogEntry | None = None
    nodes: list["Node"] | None = None
    node_replication_positions: dict[int, Timestamp] = field(
        default_factory=dict)
    """Map Node ids to their last-replicated timestamps."""
    _loop: Loop | None = None

    def initiate(self, loop: Loop, nodes: list["Node"]):
        self._loop = loop
        self.nodes = nodes[:]
        self.node_replication_positions = {id(n): -1 for n in nodes}
        loop.call_soon(self.replicate, nodes)

    async def replicate(self, nodes: list["Node"]):
        peers = [n for n in nodes if n is not self]
        while True:
            await Sleep.random_duration()
            if self.role is Role.PRIMARY:
                continue

            try:
                primary = next(n for n in peers if n.role is Role.PRIMARY)
            except StopIteration:
                continue  # No primary.

            if len(self.log) >= len(primary.log):
                continue

            if len(self.log) == 0:
                i = 0
            else:
                i = bisect.bisect_right(primary.log, self.log[-1])

            entry = primary.log[i]
            self.data[entry.write.key] = entry.write.ts
            self.log.append(entry)
            self.node_replication_positions[id(self)] = entry.ts
            await Sleep.random_duration()
            # Don't use call_soon, this can't handle out-of-order messages yet.
            primary.update_secondary_position(self, entry.ts)

    def update_secondary_position(self, secondary: "Node", ts: Timestamp):
        if self.role is not Role.PRIMARY:
            return

        # TODO: what about delayed messages / rollback?
        self.node_replication_positions[id(secondary)] = ts
        self.committed_ts = statistics.median(
            self.node_replication_positions.values())
        for n in self.nodes:
            if n is not self:
                self._loop.call_soon(n.update_committed_ts,
                                     self.committed_ts)

    async def update_committed_ts(self, ts: Timestamp):
        self.committed_ts = max(self.committed_ts, ts)

    async def write(self, key: str):
        write_start = get_current_ts()
        w = Write(key=key, ts=write_start)
        if self.role is not Role.PRIMARY:
            return Result.ERROR

        self.data[w.key] = w.ts
        # TODO: Try a random sleep before appending the log entry.
        self.log.append(LogEntry(write=w, ts=write_start))
        self.node_replication_positions[id(self)] = write_start
        while self.committed_ts < write_start:
            # TODO: abort on role change or rollback.
            await Sleep.random_duration()

        # TODO: abort on role change or rollback.
        await Sleep(write_start + D - get_current_ts())
        return Result.OK

    async def read(self, key: str):
        query_start = get_current_ts()
        if self.role is Role.SECONDARY:
            # Wait until replication catches up.
            while not self.log or self.log[-1].ts + D < query_start:
                # TODO: abort on role change or rollback.
                await Sleep.random_duration()

        # Speculative majority wait.
        last_written_ts = self.data[key]
        while self.committed_ts < last_written_ts:
            # TODO: abort on role change or rollback.
            await Sleep.random_duration()

        return last_written_ts


async def main(loop: "Loop"):
    primary = Node(role=Role.PRIMARY)
    secondaries = [Node(role=Role.SECONDARY), Node(role=Role.SECONDARY)]
    nodes = [primary] + secondaries
    for n in nodes:
        n.initiate(loop, nodes)

    write_result = await primary.write(key="x")
    print(f"Write result: {write_result}")
    assert write_result is Result.OK

    primary_read_result: Timestamp = await primary.read(key="x")
    print(f"Primary read result: {primary_read_result}")

    secondary_read_result = await secondaries[0].read(key="x")
    print(f"secondary read result: {secondary_read_result}")
    loop.stop()


if __name__ == "__main__":
    event_loop = Loop()
    event_loop.call_soon(main, event_loop)
    event_loop.run()
    print(f"Finished after {get_current_ts()} ms (simulated)")
