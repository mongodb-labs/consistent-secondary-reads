import collections
import csv
import enum
import itertools
import logging
import statistics
import time
import uuid
from dataclasses import dataclass, field

import pandas as pd
import plotly.express as px
import yaml
from omegaconf import DictConfig

from prob import PRNG
from simulate import Timestamp, get_current_ts, get_event_loop, initiate_logging, sleep

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


class Metrics:
    def __init__(self):
        self._totals = collections.Counter()
        self._sample_counts = collections.Counter()

    def update(self, metric_name: str, sample: int) -> None:
        self._totals[metric_name] += sample
        self._sample_counts[metric_name] += 1

    def total(self, metric_name: str) -> int:
        return self._totals[metric_name]

    def sample_count(self, metric_name: str) -> int:
        return self._sample_counts[metric_name]

    def mean(self, metric_name: str) -> float | None:
        if self._sample_counts[metric_name] > 0:
            return self._totals[metric_name] / self._sample_counts[metric_name]


class Node:
    def __init__(self, role: Role, cfg: DictConfig, prng: PRNG):
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
        self.noop_rate: int = cfg.noop_rate
        self.write_wait: int = cfg.write_wait
        self.metrics = Metrics()

    def initiate(self, nodes: list["Node"]):
        self.nodes = nodes[:]
        self.node_replication_positions = {id(n): OpTime.default() for n in nodes}
        if self.role is Role.PRIMARY:
            get_event_loop().create_task("no-op writer", self.noop_writer())
        if self.role is Role.SECONDARY:
            get_event_loop().create_task("replication", self.replicate())

    @property
    def last_applied(self) -> OpTime:
        return self.log[-1].optime if self.log else OpTime.default()

    async def noop_writer(self):
        while True:
            await sleep(self.noop_rate)
            self._write_internal("noop", "")

    async def replicate(self):
        log_position = 0
        while True:
            await sleep(1)
            try:
                primary = next(n for n in self.nodes if n.role is Role.PRIMARY)
            except StopIteration:
                continue  # No primary.

            while log_position < len(primary.log):
                # Find the next entry to replicate.
                entry = primary.log[log_position]
                log_position += 1

                # Simulate waiting for entry to arrive. It may have arrived already.
                apply_time = entry.optime.ts + self.prng.one_way_latency_value()
                await sleep(max(0, apply_time - get_current_ts()))
                self.data[entry.key] = (entry.value, entry.optime)
                self.log.append(entry)
                self.node_replication_positions[id(self)] = entry.optime
                get_event_loop().call_later(
                    self.prng.one_way_latency_value(),
                    primary.update_secondary_position,
                    secondary=self,
                    optime=entry.optime,
                )

                self.metrics.update(
                    "replication_lag", get_current_ts() - entry.optime.ts
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
                    self.prng.one_way_latency_value(),
                    n.update_committed_optime,
                    self.committed_optime,
                )

    def update_committed_optime(self, optime: OpTime):
        if optime > self.committed_optime:
            self.committed_optime = optime
            self.metrics.update("commit_lag", get_current_ts() - optime.ts)

    def _write_internal(self, key: str, value: str) -> OpTime:
        """Update a key and append an oplog entry."""
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        optime = OpTime(get_current_ts(), 0)
        if len(self.log) > 0 and self.log[-1].optime.ts == optime.ts:
            optime.i = self.log[-1].optime.i + 1

        w = Write(key=key, value=value, optime=optime)
        self.data[w.key] = (value, w.optime)
        # TODO: Try a realistic oplog, reserve a future slot and eventually fill it.
        self.log.append(w)
        self.node_replication_positions[id(self)] = optime
        return optime

    async def write(self, key: str, value: str):
        """Update a key, append an oplog entry, wait for w:majority and write_wait."""
        optime = self._write_internal(key=key, value=value)
        while self.committed_optime < optime:
            await sleep(1)

        commit_latency = get_current_ts() - optime.ts
        self.metrics.update("commit_latency", commit_latency)
        remaining_wait_duration = self.write_wait - commit_latency
        if remaining_wait_duration > 0:
            await sleep(remaining_wait_duration)
        self.metrics.update("write_wait", max(remaining_wait_duration, 0))

    async def read(self, key: str) -> str | None:
        """Return a key's latest value."""
        query_start = get_current_ts()
        if self.role is Role.SECONDARY:
            # Wait until replication catches up, aka "barrier".
            while self.last_applied.ts + self.write_wait < query_start:
                await sleep(1)

        # Wait for item's last-written OpTime (or OpTime.default() if no item) to
        # commit, aka "rinse".
        value, last_written_optime = self.data.get(key, (None, OpTime.default()))
        while self.committed_optime < last_written_optime:
            await sleep(1)

        return value


@dataclass
class ClientLogEntry:
    class OpType(enum.Enum):
        Write = enum.auto()
        Read = enum.auto()

    client_id: int
    op_type: OpType
    server_role: Role
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
    prng: PRNG,
):
    await sleep(start_ts)
    assert get_current_ts() == start_ts  # Deterministic scheduling!
    # Read from any node.
    node_index = prng.randint(0, len(nodes) - 1)
    node = (nodes)[node_index]
    node_name = f"node {node_index} {node.role.name}"
    key = str(prng.zipf_value())
    logger.info(f"Client {client_id} reading key {key} from {node_name}")
    value = await node.read(key=key)
    latency = get_current_ts() - start_ts
    logger.info(
        f"Client {client_id} read key {key}={value} from {node_name}, latency={latency}"
    )
    client_log.append(
        ClientLogEntry(
            client_id=client_id,
            op_type=ClientLogEntry.OpType.Read,
            server_role=node.role,
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
    prng: PRNG,
):
    assert get_current_ts() == 0, f"Current ts {get_current_ts()}"
    await sleep(start_ts)
    # Deterministic scheduling!
    assert get_current_ts() == start_ts, f"Current ts {get_current_ts()} != {start_ts}"
    key = str(prng.zipf_value())
    value = str(uuid.uuid4())
    logger.info(f"Client {client_id} writing key {key}={value} to primary")
    await primary.write(key=key, value=value)
    latency = get_current_ts() - start_ts
    logger.info(f"Client {client_id} wrote key {key}={value}, latency={latency}")
    client_log.append(
        ClientLogEntry(
            client_id=client_id,
            op_type=ClientLogEntry.OpType.Write,
            server_role=Role.PRIMARY,
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

    logging.info("Checking linearizability....")
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


def save_metrics(metrics: dict, client_log: list[ClientLogEntry]):
    writes, prim_reads, sec_reads = 0, 0, 0
    write_time, prim_read_time, sec_read_time = 0, 0, 0
    for entry in client_log:
        if entry.op_type == ClientLogEntry.OpType.Write:
            writes += 1
            write_time += entry.duration
        elif entry.server_role is Role.PRIMARY:
            prim_reads += 1
            prim_read_time += entry.duration
        else:
            sec_reads += 1
            sec_read_time += entry.duration

    metrics["write_latency"] = write_time / writes if writes else None
    metrics["prim_read_latency"] = prim_read_time / prim_reads if prim_reads else None
    metrics["sec_read_latency"] = sec_read_time / sec_reads if sec_reads else None


def chart_metrics(raw_params: dict, csv_path: str):
    df = pd.read_csv(csv_path)
    y_columns = [
        "commit_latency",
        "prim_read_latency",
        "replication_lag",
        "sec_read_latency",
        "write_latency",
    ]

    fig = px.line(
        df,
        x="write_wait",
        y=y_columns,
        labels={"variable": "Metrics", "value": "Values"},
        title=", ".join(f"{k}={v}" for k, v in raw_params.items()),
    )

    # Show vertical line on hover
    fig.update_traces(mode="markers+lines", hovertemplate=None)
    fig.update_layout(hovermode="x unified")
    chart_path = "metrics/chart.html"
    fig.write_html(chart_path)
    logging.info(f"Created {chart_path}")


async def main_coro(params: DictConfig, metrics: dict):
    logging.info(params)
    seed = int(time.monotonic_ns() if params.seed is None else params.seed)
    logging.info(f"Seed {seed}")
    prng = PRNG(
        seed,
        params.one_way_latency_mean,
        params.one_way_latency_variance,
        params.zipf_skewness,
    )
    primary = Node(role=Role.PRIMARY, cfg=params, prng=prng)
    secondaries = [
        Node(role=Role.SECONDARY, cfg=params, prng=prng),
        Node(role=Role.SECONDARY, cfg=params, prng=prng),
    ]
    nodes = [primary] + secondaries
    for n in nodes:
        n.initiate(nodes)

    lp = get_event_loop()
    client_log: list[ClientLogEntry] = []
    tasks = []
    start_ts = 0
    # Schedule some tasks with Poisson start times. Each does one read or one write.
    for i in range(params.operations):
        start_ts += round(prng.exponential(params.interarrival))
        if prng.randint(0, 1) == 0:
            coro = writer(
                client_id=i,
                start_ts=start_ts,
                primary=primary,
                client_log=client_log,
                prng=prng,
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
    save_metrics(metrics, client_log)

    def secondary_metric(name: str) -> float | None:
        total = sum(s.metrics.total(name) for s in secondaries)
        sample_count = sum(s.metrics.sample_count(name) for s in secondaries)
        if sample_count > 0:
            return total / sample_count

    metrics["replication_lag"] = secondary_metric("replication_lag")
    metrics["commit_lag"] = secondary_metric("commit_lag")
    metrics["write_wait"] = primary.metrics.mean("write_wait")
    metrics["commit_latency"] = primary.metrics.mean("commit_latency")
    if params.check_linearizability:
        do_linearizability_check(client_log)


def all_param_combos(raw_params: dict) -> list[DictConfig]:
    param_combos: dict[list] = {}
    # Load config file, keep all values as strings.
    for k, v in raw_params.items():
        v_interpreted = eval(str(v))
        try:
            iter(v_interpreted)
        except TypeError:
            param_combos[k] = [v_interpreted]
        else:
            param_combos[k] = list(v_interpreted)

    for values in itertools.product(*param_combos.values()):
        yield DictConfig(dict(zip(param_combos.keys(), values)))


def main():
    initiate_logging()
    event_loop = get_event_loop()
    csv_writer: None | csv.DictWriter = None
    csv_path = "metrics/metrics.csv"
    csv_file = open(csv_path, "w+")
    raw_params = yaml.safe_load(open("params.yaml"))
    for params in all_param_combos(raw_params):
        metrics = {}
        event_loop.create_task("main", main_coro(params=params, metrics=metrics))
        event_loop.run()
        logging.info(f"metrics: {metrics}")
        stats = metrics | dict(params)
        if csv_writer is None:
            csv_writer = csv.DictWriter(csv_file, fieldnames=stats.keys())
            csv_writer.writeheader()

        csv_writer.writerow(stats)
        event_loop.reset()

    csv_file.close()
    chart_metrics(raw_params=raw_params, csv_path=csv_path)


if __name__ == "__main__":
    main()
