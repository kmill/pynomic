# minidb.py
# 2013 Kyle Miller
#
# a mini database that stores a dictionary of strings, numbers,
# booleans, arrays, dictionaries, or None

import json
import os
import logging

import queries
import util
from util import assert_type

class Database(object) :
    def __init__(self, backingFile) :
        self.logger = logging
        self.backingFile = os.path.abspath(backingFile)
        self.lock = util.RWLock()
        self.rollback(warn=False)
    def commit(self) :
        """Commits the database to disk by first saving it to a
        temporary file and then copying it over the old database file."""
        with self.lock.read_lock :
            self.logger.info("%r committing", self)
            tmpfile = self.backingFile + ".tmp"
            with open(tmpfile, "w") as f :
                json.dump(self.data, f)
            os.rename(tmpfile, self.backingFile)
            self.logger.info("%r done committing", self)
    def rollback(self, warn=True) :
        """Updates the in-memory representation of the database to
        what is stored on disk."""
        with self.lock.write_lock :
            if warn :
                self.logger.warn("%r rolling back", self)
            if os.path.isfile(self.backingFile) :
                self.logger.info("%r rolling back from file", self)
                # load the database if it exists
                with open(self.backingFile) as f :
                    self.data = json.load(f)
            else :
                self.logger.info("%r rolling back to empty dictionary (no previous file)", self)
                self.data = {}
            self.logger.info("%r rolled back", self)
    def select(self, queryfunc, subpath=None) :
        """Returns the results of the query function when given the
        database.  The database can be restricted using the 'subpath'
        argument."""
        queryfunc = util.assert_type(queryfunc, queries.Func)
        with self.lock.read_lock :
            data = self.data
            if subpath is not None and assert_type(subpath, queries.Path) :
                data = subpath.get(data)
            return queries.select(data, queryfunc)
    def insert(self, path, o, append=False, overwrite=False, subpath=None) :
        """Insert an object into a given path.  The database can be
        restricted using the subpath parameter.

        If 'append' is true, then the destination must either be empty
        or a list.  Empty is taken to mean the destination is an empty
        list.  Then the element is appended to the list.

        Otherwise, it is an error for the destination to not be empty,
        unless 'overwrite' is true, in which case the destination is
        overwritten.

        The database is committed to disk on success."""
        if not check_type_is_ok(o) :
            raise TypeError("Object contains database-unfriendly type.")
        with self.lock.write_lock :
            attachmentPoint = self.data
            if subpath is not None and assert_type(subpath, queries.Path) :
                attachmentPoint = subpath.get(attachmentPoint)
            if path is None or (path.parent is None and path.key is None):
                raise Exception("Cannot insert an object with None path")
            elif path.parent is not None :
                attachmentPoint = path.parent.get(attachmentPoint)
            if append :
                attachmentPoint = attachmentPoint.setdefault(path.key, [])
                if type(attachmentPoint) is not list :
                    raise Exception("Cannot append to non-list")
                attachmentPoint.append(o)
            else :
                if path.key in attachmentPoint and not overwrite :
                    raise Exception("Cannot insert object over another object")
                attachmentPoint[path.key] = o
                self.commit()
    def remove(self, queryfunc, subpath=None) :
        """Remove from the database all entries returned by the given
        query function when applied to the database.  The database can
        be restricted using the 'subpath' parameter.

        The database is committed to disk on success."""
        self.lock.write_lock.acquire()
        try :
            data = self.data
            if subpath is not None and assert_type(subpath, queries.Path) :
                data = subpath.get(data)
            queries.remove(data, queryfunc)
        except queries.InconsistentData :
            self.rollback()
            raise
        except :
            # no changes made
            raise
        else :
            self.lock.read_lock.acquire()
        finally :
            self.lock.write_lock.release()
        self.commit()
        self.lock.read_lock.release()
    def update(self, queryfunc, changes, subpath=None) :
        """Updates the database by running the query and then running
        each of the instructions in 'changes'.  The update can be
        restricted to a portion of the database using the 'subpath'
        argument.

        The 'changes' argument is a list of path/value pairs.  Each
        value is run with the variable of the queryfunc bound to one
        of the results of running the queryfunc on the database, and
        that path in the object is updated to the result of the
        value."""
        queryfunc = util.assert_type(queryfunc, queries.Func)
        self.lock.write_lock.acquire()
        try :
            data = self.data
            if subpath is not None and assert_type(subpath, queries.Path) :
                data = subpath.get(data)
            queries.update(data, queryfunc, changes)
        except queries.InconsistentData :
            self.rollback()
            raise
        except :
            # no changes made
            raise
        else :
            self.lock.read_lock.acquire()
        finally :
            self.lock.write_lock.release()
        self.commit()
        self.lock.read_lock.release()
    def __repr__(self) :
        return "Database(%r)" % self.backingFile
