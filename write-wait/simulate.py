import collections
import heapq
import random
import typing
from dataclasses import dataclass, field
from typing import Coroutine

Timestamp = int

_current_ts: Timestamp = 0  # Wall clock time.


def get_current_ts() -> Timestamp:
    return _current_ts


class Future:
    def __iter__(self):
        yield self

    __await__ = __iter__


def _random_delay() -> int:
    return random.randint(0, 10)


@dataclass
class Sleep(Future):
    duration: int

    @classmethod
    def random_duration(cls):
        return cls(_random_delay())


class Loop:
    @dataclass(order=True)
    class _Alarm:
        deadline: Timestamp
        coro: Coroutine = field(compare=False)

    def __init__(self):
        self._running = True
        self._runnable: collections.deque[Coroutine] = collections.deque()
        # List of (deadline, coroutine).
        self._alarms: list[Loop._Alarm] = []

    def run(self):
        global _current_ts

        self._running = True
        while self._running:
            if _current_ts > 1e6:
                raise Exception(f"Timeout, current timestamp is {_current_ts}")
            if self._runnable:
                gen = self._runnable.popleft()
            elif self._alarms:
                alarm: Loop._Alarm = heapq.heappop(self._alarms)
                # Advance time to next alarm.
                _current_ts = alarm.deadline
                gen = alarm.coro
            else:
                return  # All done.
            try:
                result = gen.send(None)
            except StopIteration:
                pass
            else:
                if isinstance(result, Sleep):
                    heapq.heappush(
                        self._alarms,
                        Loop._Alarm(_current_ts + result.duration, gen))

    def stop(self):
        self._running = False

    def call_soon(self, fn: typing.Callable, *args, **kwargs):
        heapq.heappush(
            self._alarms,
            Loop._Alarm(_current_ts + _random_delay(), fn(*args, **kwargs)))
