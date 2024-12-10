# This file is a part of Dramatiq.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Dramatiq is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Dramatiq is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import ctypes
import inspect
import platform
import threading
import time
import threading

from .logging import get_logger
from collections import defaultdict

from datetime import datetime
from dataclasses import dataclass, field

__all__ = [
    "Interrupt",
    "current_platform",
    "is_gevent_active",
    "raise_thread_exception",
    "supported_platforms",
]

logger = get_logger(__name__)

current_platform = platform.python_implementation()
python_version = platform.python_version_tuple()
thread_id_ctype = ctypes.c_long if python_version < ("3", "7") else ctypes.c_ulong
supported_platforms = {"CPython"}


def is_gevent_active():
    """Detect if gevent monkey patching is active."""
    try:
        from gevent import monkey
    except ImportError:  # pragma: no cover
        return False
    return bool(monkey.saved)


class Interrupt(BaseException):
    """Base class for exceptions used to asynchronously interrupt a
    thread's execution.  An actor may catch these exceptions in order
    to respond gracefully, such as performing any necessary cleanup.

    This is *not* a subclass of ``DramatiqError`` to avoid it being
    caught unintentionally.
    """


def raise_thread_exception(thread_id, exception):
    """Raise an exception in a thread.

    Currently, this is only available on CPython.

    Note:
      This works by setting an async exception in the thread.  This means
      that the exception will only get called the next time that thread
      acquires the GIL.  Concretely, this means that this middleware can't
      cancel system calls.
    """
    if current_platform == "CPython":
        WE._catch_exception(thread_id, exception)
    else:
        message = "Setting thread exceptions (%s) is not supported for your current platform (%r)."
        exctype = (exception if inspect.isclass(exception) else type(exception)).__name__
        logger.critical(message, exctype, current_platform)


def _raise_thread_exception_cpython(thread_id, exception):
    exctype = (exception if inspect.isclass(exception) else type(exception)).__name__
    thread_id = thread_id_ctype(thread_id)
    exception = ctypes.py_object(exception)
    count = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, exception)
    if count == 0:
        logger.critical("Failed to set exception (%s) in thread %r.", exctype, thread_id.value)
    elif count > 1:  # pragma: no cover
        logger.critical("Exception (%s) was set in multiple threads.  Undoing...", exctype)
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.c_long(0))


@dataclass
class ThreadState:
    ack: bool = False
    last_update: datetime = field(default_factory=datetime.now)
    exceptions: list[Exception] = field(default_factory=list)

    def clear(self):
        self.ack = False
        self.exceptions.clear()
        self.last_updated = datetime.now()

    def fetch_exceptions(self):
        self.ack = True
        self.last_updated = datetime.now()

        return self.exceptions

    def add_exception(self, exception: Exception):
        self.ack = False
        self.exceptions.append(exception)
        self.last_updated = datetime.now()


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class WorkerExceptions(metaclass=Singleton):
    ACK_TIMEOUT = 15 
    WATCHER_SLEEP = 1

    _watch_thread: threading.Thread
    _state_mapping = defaultdict(lambda: ThreadState())

    def __init__(self):
        self.logger = get_logger(__name__, type(self))
        
        self._stop_event = threading.Event()
        
        self._watch_thread = threading.Thread(target=self._raise_exception_watcher)
        self._watch_thread.start()

    def stop_background_thread(self):
        self._stop_event.set()
        self._watch_thread.join()

    def _raise_exception_watcher(self):
        while not self._stop_event.is_set():
            time.sleep(self.WATCHER_SLEEP)
            
            for thread_id, state in self._state_mapping.items():
                if not state.ack and state.exceptions:
                    delta = datetime.now() - state.last_updated

                    if delta.seconds >= self.ACK_TIMEOUT:
                        _raise_thread_exception_cpython(thread_id, state.exceptions[0])

    def _get_thread_state(self, thread_id: int | None = None):
        if thread_id is None:   
            thread_id = threading.get_ident()
    
        return self._state_mapping[thread_id]


    def _catch_exception(self, thread_id: int, exception: Exception):
        state = self._get_thread_state(thread_id)
        state.add_exception(exception)
        
        self.logger.warning(f'Caught exception {exception} in worker thread {thread_id}')

    def get_exceptions(self):
        state = self._get_thread_state()
        
        return state.fetch_exceptions()

    def on_thread_close(self):
        thread_id = threading.get_ident()

        if self._watch_thread.is_alive():
            self.stop()

        if thread_id in self._state_mapping:
            del self._state_mapping[thread_id]

    def on_task_finsih(self):
        thread_id = threading.get_ident()

        state = self._get_thread_state()
        state.clear()

        self.logger.warning(f'Cleared _state_mapping in worker thread {thread_id}')

WE = WorkerExceptions()
