import heapq
import inspect
import logging
import typing
from dataclasses import dataclass, field

Timestamp = int


def initiate_logging() -> None:
    class CustomFormatter(logging.Formatter):
        def format(self, record):
            original_msg = super().format(record)
            return f"{get_current_ts()} {original_msg}"

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
    _instances: set["_Task"] = set()

    def __new__(cls, *args, **kwargs):
        instance = super(_Task, cls).__new__(cls)
        cls._instances.add(instance)
        return instance

    def __init__(self, name: str, coro: typing.Coroutine):
        super().__init__()
        self.name = name
        self.coro = coro
        self.step()

    def __str__(self):
        return f"_Task({repr(self.name)}, {self.coro})"

    def step(self) -> None:
        try:
            f = self.coro.send(None)
        except StopIteration:
            _Task._instances.remove(self)
            self.resolve()  # Resume coroutines stopped at "await task".
            return

        assert isinstance(f, Future)
        f.add_done_callback(self.step)

    @classmethod
    def print_all_tasks(cls):
        for t in cls._instances:
            print(t)
            _print_coro_position(t)


def sleep(delay: int) -> Future:
    assert delay >= 0
    f = Future()
    _global_loop.call_later(delay=delay, callback=f.resolve)
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
        self._current_ts = 0

    def reset(self):
        self.__init__()

    def run(self):
        self._running = True
        while self._running:
            if self._current_ts > 1e6:
                _Task.print_all_tasks()
                raise Exception(f"Timeout, current timestamp is {self._current_ts}")

            if len(self._alarms) > 0:
                alarm: EventLoop._Alarm = heapq.heappop(self._alarms)
                # Advance time to next alarm.
                assert alarm.deadline >= self._current_ts
                self._current_ts = alarm.deadline
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
        self, delay: int, callback: typing.Callable, *args, **kwargs
    ) -> Future:
        """Schedule a callback after a delay.

        Returns a Future that will be resolved after the callback runs.
        """
        assert not inspect.iscoroutinefunction(callback), "Use create_task()"
        f = Future()

        def alarm_callback():
            callback(*args, **kwargs)
            f.resolve()

        alarm = EventLoop._Alarm(
            deadline=self._current_ts + delay, callback=alarm_callback
        )
        heapq.heappush(self._alarms, alarm)
        return f

    def create_task(self, name: str, coro: typing.Coroutine) -> Future:
        """Start running a coroutine.

        Returns a Future that will be resolved after the coroutine finishes.
        """
        return _Task(name=name, coro=coro)

    @property
    def current_ts(self) -> int:
        return self._current_ts


_global_loop = EventLoop()


def get_event_loop() -> EventLoop:
    """

    :rtype: object
    """
    return _global_loop


def get_current_ts() -> Timestamp:
    return get_event_loop().current_ts


def _print_coro_position(task: _Task):
    coro = task.coro
    frame = coro.cr_frame
    if frame is None:
        print("Coroutine is not currently paused at an 'await' expression")
        return

    source_lines, starting_lineno = inspect.getsourcelines(frame.f_code)
    lineno = frame.f_lineno - starting_lineno
    context_lines = 2
    linenos = sorted(
        set(
            [0]
            + list(
                range(
                    max(0, lineno - context_lines),
                    min(lineno + context_lines, len(source_lines) - 1),
                )
            )
        )
    )
    for i in linenos:
        line = source_lines[i]
        mark = ">" if i == lineno else " "
        print(f"{mark}{i+starting_lineno:4} {line}", end="")
        if i == 0 and lineno > context_lines + 1:
            print("      ...")
