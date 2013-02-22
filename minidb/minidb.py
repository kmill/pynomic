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
        self.rollback()
    def commit(self) :
        """Save to a temporary file and copy it over the old database."""
        with self.lock.read_lock :
            self.logger.info("%r committing", self)
            tmpfile = self.backingFile + ".tmp"
            with open(tmpfile, "w") as f :
                json.dump(self.data, f)
            os.rename(tmpfile, self.backingFile)
            self.logger.info("%r done committing", self)
    def rollback(self) :
        with self.lock.write_lock :
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
        with self.lock.read_lock :
            data = self.data
            if subpath is not None and assert_type(subpath, queries.Path) :
                data = subpath.get(data)
            return queries.select(data, queryfunc)
    def insert(self, path, o, append=False, overwrite=False, subpath=None) :
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
        self.lock.write_lock.acquire()
        try :
            data = self.data
            if subpath is not None and assert_type(subpath, queries.Path) :
                data = subpath.get(data)
            queries.remove(data, queryfunc)
        except :
            self.rollback()
            raise
        else :
            self.lock.read_lock.acquire()
        finally :
            self.lock.write_lock.release()
        self.commit()
        self.lock.read_lock.release()
    def __repr__(self) :
        return "Database(%r)" % self.backingFile
