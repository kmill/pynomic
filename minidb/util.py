# util.py
# 2013 Kyle Miller
# utility objects and functions for the minidb

import operator
import threading
import types

def assert_type(o, t) :
    """Returns o.  But, if it is not an instance of t, raises an
    exception."""
    if isinstance(o, t) :
        return o
    else :
        raise TypeError("expecting type " + str(t))

allowed_operations = {
    "lt" : operator.lt,
    "le" : operator.le,
    "eq" : operator.eq,
    "ne" : operator.ne,
    "ge" : operator.ge,
    "gt" : operator.gt,

    "not" : operator.not_,
    "truth" : operator.truth,

    "abs" : operator.abs,
    "add" : operator.add,
    "sub" : operator.sub,
    "neg" : operator.neg,
    "mul" : operator.mul,
    "div" : operator.div,
    "mod" : operator.mod,
    "pow" : operator.pow,

    "contains" : operator.contains,
    
    "int" : int,
    "float" : float,
    "str" : unicode,

    "any" : any,
    "all" : all,
    }

allowed_types = {type(None), str, unicode, int, long, float, bool}

def check_type_is_ok(o) :
    """This function checks whether the object is suited for being
    part of the database.  That is, whether it's string, number,
    boolean, None, or a dictionary or array of such."""
    t = type(o)
    if t in allowed_types :
        return True
    elif t is list :
        return all(check_type_is_ok(elt) for elt in o)
    elif t is dict :
        return all(type(k) in allowed_types and check_type_is_ok(v)
                   for k, v in o.iteritems())
    else :
        return False

class RWLock(object) :
    """A lock which lets as many things read as they want, but limits
    to exactly one writer.  A writer can take as many read locks as
    they want, too, and the writer may interleave acquiring read locks
    with releasing the write lock.

    One locks on rwlock.read_lock or self.write_lock, which behave
    like normal locks, except rwlock.read_lock can be locked as many
    times as one wants before releasing, and either a read_lock blocks
    a write_lock (but not vice versa)."""
    def __init__(self) :
        self.readers = 0
        self.writers_read_locks = 0
        internal_lock = threading.Condition(threading.RLock())
        self.read_lock = self.ReadLock(self, internal_lock)
        self.write_lock = self.WriteLock(self, internal_lock)
    class ReadLock(object) :
        def __init__(self, rwlock, internal_lock) :
            self.rwlock = rwlock
            self.internal_lock = internal_lock
        def __enter__(self) :
            self.acquire()
            return self
        def __exit__(self, type, value, traceback) :
            self.release()
        def acquire(self) :
            self.internal_lock.acquire()
            if self.rwlock.readers >= 0 : # true if there is no writer lock
                self.rwlock.readers += 1
            else :
                self.rwlock.writers_read_locks += 1
            self.internal_lock.release()
        def release(self) :
            self.internal_lock.acquire()
            if self.rwlock.readers >= 0 : # true if there is no writer lock
                self.rwlock.readers -= 1
            else :
                self.rwlock.writers_read_locks -= 1
            self.internal_lock.notify()
            self.internal_lock.release()
    class WriteLock(object) :
        def __init__(self, rwlock, internal_lock) :
            self.rwlock = rwlock
            self.internal_lock = internal_lock
        def __enter__(self) :
            self.acquire()
            return self
        def __exit__(self, type, value, traceback) :
            self.release()
        def acquire(self) :
            self.internal_lock.acquire()
            while self.rwlock.readers > 0 :
                self.internal_lock.wait()
            self.readers = -1 # used as a sentinel for re-entrance
        def release(self) :
            # invariants:
            # - we have the inner_lock
            # - readers == -1
            # - writers_read_locks is the number of read locks taken while inner_lock is held
            self.rwlock.readers = self.rwlock.writers_read_locks
            self.rwlock.writers_read_locks = 0
            self.internal_lock.notify()
            self.internal_lock.release()
