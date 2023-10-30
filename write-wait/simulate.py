import heapq
import inspect
import logging
import random
import typing
from dataclasses import dataclass, field

Timestamp = int

_current_ts: Timestamp = 0  # Wall clock time.


def get_current_ts() -> Timestamp:
    return _current_ts


def initiate_logging() -> None:
    class CustomFormatter(logging.Formatter):
        def format(self, record):
            global _current_ts
            original_msg = super().format(record)
            return f"{_current_ts} {original_msg}"

    formatter = CustomFormatter(fmt="%(levelname)s: %(message)s")
    logging.basicConfig(level=logging.INFO)
    for h in logging.getLogger().handlers:
        h.setFormatter(formatter)


class Future:
    def __init__(self):
        self.callbacks: list[typing.Callable] = []
        self.resolved = False

    def add_done_callback(self, callback: typing.Callable):
        assert not inspect.iscoroutinefunction(callback), "Use create_task()"
        if self.resolved:
            callback()
        else:
            self.callbacks.append(callback)

    def resolve(self):
        self.resolved = True
        callbacks = self.callbacks
        self.callbacks = []  # Prevent recursive resolves.
        for c in callbacks:
            c()

    def __iter__(self):
        yield self

    __await__ = __iter__


class _Task(Future):
    def __init__(self, coro: typing.Coroutine):
        super().__init__()
        self.coro = coro
        self.step()

    def step(self) -> None:
        try:
            f = self.coro.send(None)
        except StopIteration:
            self.resolve()  # Resume coroutines stopped at "await task".
            return

        assert isinstance(f, Future)
        f.add_done_callback(self.step)


def sleep_random(max_delay: int) -> Future:
    f = Future()
    _global_loop.call_later(max_delay=max_delay, callback=f.resolve)
    return f


class EventLoop:
    @dataclass(order=True)
    class _Alarm:
        deadline: Timestamp
        callback: typing.Callable = field(compare=False)

    def __init__(self):
        self._running = False
        # Priority queue of scheduled alarms.
        self._alarms: list[EventLoop._Alarm] = []

    def run(self):
        global _current_ts

        self._running = True
        while self._running:
            if _current_ts > 1e6:
                raise Exception(f"Timeout, current timestamp is {_current_ts}")

            if len(self._alarms) > 0:
                alarm: EventLoop._Alarm = heapq.heappop(self._alarms)
                # Advance time to next alarm.
                assert alarm.deadline >= _current_ts
                _current_ts = alarm.deadline
                alarm.callback()
            else:
                return  # All done.

    def stop(self):
        self._running = False

    def call_soon(self, callback: typing.Callable, *args, **kwargs) -> Future:
        """Schedule a callback as soon as possible.

        Returns a Future that will be resolved after the callback runs.
        """
        return self.call_later(0, callback, *args, **kwargs)

    def call_later(
        self, max_delay: int, callback: typing.Callable, *args, **kwargs
    ) -> Future:
        """Schedule a callback after a random delay.

        Returns a Future that will be resolved after the callback runs.
        """
        assert not inspect.iscoroutinefunction(callback), "Use create_task()"
        f = Future()

        def alarm_callback():
            callback(*args, **kwargs)
            f.resolve()

        delay = random.randint(0, max_delay)
        alarm = EventLoop._Alarm(deadline=_current_ts + delay, callback=alarm_callback)
        heapq.heappush(self._alarms, alarm)
        return f

    def create_task(self, coro: typing.Coroutine) -> Future:
        """Start running a coroutine.

        Returns a Future that will be resolved after the coroutine finishes.
        """
        return _Task(coro)


_global_loop = EventLoop()


def get_event_loop() -> EventLoop:
    return _global_loop
