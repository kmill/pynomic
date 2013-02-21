# minidb.py
# 2013 Kyle Miller
#
# a mini database that stores a dictionary of strings, numbers,
# booleans, arrays, dictionaries, or None

import json
import os
import threading
import types
import itertools

allowed_types = {type(None), str, unicode, int, long, float, bool}

import operator
allowed_operations = {
    "lt" : operator.lt,
    "le" : operator.le,
    "eq" : operator.eq,
    "ne" : operator.ne,
    "ge" : operator.ge,
    "gt" : operator.gt,
    "not" : operator.not_,

    "abs" : operator.abs,
    "+" : operator.add,
    
    "int" : int,
    "float" : float,
    "str" : unicode,

    "any" : any,
    "all" : all,
    }

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

class Database(object) :
    def __init__(self, backingFile) :
        self.backingFile = os.path.abspath(backingFile)
        self.lock = threading.Lock()
        self.rollback()
    def commit(self) :
        """Save to a temporary file and copy it over the old database."""
        with self.lock :
            self._commit()
    def _commit(self) :
        tmpfile = self.backingFile + ".tmp"
        with open(tmpfile, "w") as f :
            json.dump(self.data, f)
        os.rename(tmpfile, self.backingFile)
    def _rollback(self) :
        if os.path.isfile(self.backingFile) :
            # load the database if it exists
            with open(self.backingFile) as f :
                self.data = json.load(f)
        else :
            self.data = {}
    def rollback(self) :
        with self.lock :
            self._rollback()
    def select(self, queryfunc) :
        with self.lock :
            return queryfunc.query.select(queryfunc.var, self.data)
    def insert(self, path, o, append=False, overwrite=False) :
        if not check_type_is_ok(o) :
            raise TypeError("Object contains database-unfriendly type.")
        with self.lock :
            attachmentPoint = self.data
            if path is None or (path.parent is None and path.key is None):
                raise Exception("Cannot insert an object with None path")
            elif path.parent is not None :
                for k in path.parent :
                    attachmentPoint = attachmentPoint[k]
            if append :
                try :
                    attachmentPoint = attachmentPoint[path.key]
                    if type(attachmentPoint) is not list :
                        raise Exception("Cannot append to non-list")
                except KeyError :
                    attachmentPoint = attachmentPoint.setdefault(path.key, [])
                attachmentPoint.append(o)
            else :
                if path.key in attachmentPoint and not overwrite :
                    raise Exception("Cannot insert object over another object")
                attachmentPoint[path.key] = o
            self._commit()
    def remove(self, queryfunc) :
        with self.lock :
            try :
                queryfunc.query.remove(queryfunc.var, self.data)
            except :
                self._rollback()
                raise
            else :
                self._commit()

class Bindings(object) :
    def __init__(self, key=None, value=None, parent=None) :
        self.key = key
        self.value = value
        self.parent = parent
    def extend(self, key, value) :
        return Bindings(key, value, self)
    def __getitem__(self, key) :
        if key == self.key :
            return self.value
        elif self.parent is not None :
            return self.parent[key]
        else :
            raise KeyError(key)
    def __repr__(self) :
        pairs = []
        b = self
        while b is not None :
            pairs.append((b.key, b.value))
            b = b.parent
        return repr(pairs)

class Query(object) :
    def execute(self, bindings) :
        """Returns (data, parent, path)"""
        raise Exception("Unimplemented")
    def select(self, dbvar, data) :
        return [v for p, v in self.execute(Bindings(dbvar, (None, data)))]
    def remove(self, dbvar, data) :
        paths = {}
        # paths will be an object which overlays all the deleted paths
        # on each other. Things that are deleted have a None at the
        # end of a path in the structure.
        def addPath(path, istop) :
            if path.key is None :
                return paths
            parentPaths = path if path.parent is None else addPath(path.parent, False)
            if parentPaths is not None :
                if istop :
                    parentPaths[path.key] = None
                    return None
                else :
                    return parentPaths.setdefault(path.key, {})
            else :
                return None
        for p, v in self.execute(Bindings(dbvar, (None, data))) :
            if p is None :
                raise Exception("Cannot remove an element which did not come directly from the database.")
            addPath(p, True)

        def removePaths(data, paths) :
            if paths is None :
                raise Exception("Unexpected path removal.")
            if type(data) is dict :
                for k, subpath in paths.iteritems() :
                    if subpath is None :
                        del data[k]
                    else :
                        removePaths(data[k], subpath)
            else :
                todelete = []
                for k, subpath in paths.iteritems() :
                    if subpath is None :
                        todelete.append(int(k))
                    else :
                        removePaths(data[k], subpath)
                for i in reversed(sorted(todelete)) :
                    del data[i]
        removePaths(data, paths)
    def __ge__(self, other) :
        if isinstance(other, QueryFunc) :
            return QueryBind(self, other)
        else :
            raise NotImplemented()
    def __rshift__(self, other) :
        if isinstance(other, Query) :
            return QueryBind(self, QueryFunc(None, other))
        else :
            raise NotImplemented()
    def __add__(self, other) :
        if isinstance(other, Query) :
            return QueryUnion(self, other)
        else :
            raise NotImplemented()

class Select(Query) :
    def __init__(self, source, *pathparts) :
        self.source = source
        if len(pathparts) == 1 and isinstance(pathparts[0], Path) :
            self.path = pathparts[0]
        else :
            self.path = path(*pathparts)
    def execute(self, bindings) :
        path = self.path
        source = path.get(self.source.execute(bindings)[1])
        if type(source) is dict :
            return ((path[k], v) for k,v in source.iteritems())
        else :
            return ((path[i], v) for i,v in itertools.izip(itertools.count(), source))
    def __repr__(self) :
        return "Select(%r, %r)" % (self.source, self.path)

class QueryFunc(object) :
    def __init__(self, var, query) :
        self.var = var
        if isinstance(var, Var) :
            self.var = var.name
        self.query = query
    def __repr__(self) :
        return "QueryFunc(%r, %r)" % (self.var, self.query)

class QueryBind(Query) :
    def __init__(self, query, func) :
        self.query = query
        self.func = func
    def execute(self, bindings) :
        var = self.func.var
        funcquery = self.func.query
        for r in self.query.execute(bindings) :
            subbindings = bindings
            if var is not None :
                subbindings = bindings.extend(var, r)
            for r2 in funcquery.execute(subbindings) :
                yield r2
    def __repr__(self) :
        return "QueryBind(%r, %r)" % (self.query, self.func)

class QueryUnion(Query) :
    def __init__(self, *queries) :
        self.queries = list(queries)
    def execute(self, bindings) :
        for q in self.queries :
            for r in q.execute(bindings) :
                yield r
    def __repr__(self) :
        return "QueryUnion(*%r)" % self.queries

class Return(Query) :
    def __init__(self, value) :
        self.value = value
    def execute(self, bindings) :
        return [self.value.execute(bindings)]
    def __repr__(self) :
        return "Return(%r)" % self.value

class Require(Query) :
    def __init__(self, value) :
        self.value = value
    def execute(self, bindings) :
        if self.value.execute(bindings)[1] :
            return [(None, ())]
        else :
            return []
    def __repr__(self) :
        return "Require(%r)" % self.value

class Do(Query) :
    def __init__(self) :
        self.binds = []
        self.query = None
    def let(self, var, query) :
        self.binds.append((var, query))
        return self
    def do(self, query) :
        self.binds.append((None, query))
        return self
    def ret(self, value) :
        self.binds.append((None, Return(value)))
        return self
    def require(self, value) :
        self.binds.append((None, Require(value)))
        return self
    def buildQuery(self) :
        if self.query is not None :
            return
        builtq = None
        for v, q in reversed(self.binds) :
            if builtq is None :
                if v is not None :
                    raise Exception("Last expression in Do must not be a 'let'.")
                builtq = q
            else :
                builtq = QueryBind(q, QueryFunc(v, builtq))
        self.query = builtq
    def execute(self, bindings) :
        self.buildQuery()
        return self.query.execute(bindings)
    def __repr__(self) :
        self.buildQuery()
        return repr(self.query)

class Value(object) :
    pass

class Constant(Value) :
    def __init__(self, o) :
        self.o = o
    def execute(self, bindings) :
        return (None, self.o)
    def __repr__(self) :
        return "Constant(%r)" % self.o

class Var(Value) :
    def __init__(self, name) :
        self.name = name
    def execute(self, bindings) :
        return bindings[self.name]
    def __repr__(self) :
        return "Var(%r)" % self.name

class Get(Value) :
    def __init__(self, source, *pathparts) :
        self.source = source
        if len(pathparts) == 1 and isinstance(pathparts[0], Path) :
            self.path = pathparts[0]
        else :
            self.path = path(*pathparts)
    def execute(self, bindings) :
        path = self.path
        itspath, value = self.source.execute(bindings)
        value = path.get(value)
        return (itspath.concat(path) if itspath is not None else None, value)
    def __repr__(self) :
        return "Get(%r, %r)" % (self.source, self.path)

class AsList(Value) :
    def __init__(self, query) :
        self.query = query
    def execute(self, bindings) :
        return (None, [r[1] for r in self.query.execute(bindings)])
    def __repr__(self) :
        return "AsList(%r)" % self.query

class AsDict(Value) :
    def __init__(self, query) :
        self.query = query
    def execute(self, bindings) :
        def make_key(p) :
            if p is None :
                return p
            return p.key
        return (None, dict((make_key(r[0]), r[1]) for r in self.query.execute(bindings)))
    def __repr__(self) :
        return "AsList(%r)" % self.query

class Op(Value) :
    def __init__(self, name, *params) :
        if name not in allowed_operations :
            raise Exception("Operation for Op must be allowed, not " + name)
        self.name = name
        self.op = allowed_operations[name]
        self.params = [p if isinstance(p, Value) else Constant(p) for p in params]
    def execute(self, bindings) :
        eparams = [p.execute(bindings)[1] for p in self.params]
        return (None, self.op(*eparams))
    def __repr__(self) :
        return "Op(%r, *%r)" % (self.name, self.params)

NEXTVAR = 1
def genvar(prefix="genvar") :
    global NEXTVAR
    v = Var(prefix + str(NEXTVAR))
    NEXTVAR += 1
    return v

class Path(object) :
    def __init__(self, key=None, parent=None) :
        self.key = key
        self.parent = parent
    def get(self, o) :
        if self.parent is not None :
            o = self.parent.get(o)
        if self.key is None :
            return o
        else :
            try :
                return o[self.key]
            except KeyError, IndexError :
                raise KeyError(self)
    def concat(self, other) :
        if other is None :
            return self
        p = self
        for k in other :
            p = p[k]
        return p
    def __getitem__(self, key) :
        return Path(key, parent=self)
    def __iter__(self) :
        if self.parent is not None :
            for k in self.parent :
                yield k
        if self.key is not None :
            yield self.key
    def __repr__(self) :
        if self.parent is None :
            if self.key is None :
                return "Path()"
            else :
                return "Path()[%r]" % self.key
        else :
            return "%r[%r]" % (self.parent, self.key)

def path(*keys) :
    if len(keys) == 0 :
        return Path()
    else :
        return Path(key=keys[-1], parent=path(*keys[:-1]))

query = QueryFunc("db", Select("db", Path()["users"]) >= QueryFunc("a", Return(Var("a"))))

def queryfunc(f) :
    """A "decorator" which passes in a newly generated variable to
    make a query function of a function."""
    v = genvar(f.func_code.co_varnames[0])
    return QueryFunc(v, f(v))

a = Var("a")

@queryfunc
def query(db) :
    a = Var("a")
    b = Var("b")
    return (Do()
            .let(a, Select(db, "users"))
            .require(Op("ne", "kmill", Get(a, "username")))
            .require(Op("any", AsList(Do()
                                      .let(b, Select(a, "numbers"))
                                      .ret(Op("eq", 22, b)))))
            .ret(a))

print query

db = Database("test.db")
#print db.select(query)
print db.select(QueryFunc(a, Return(AsDict(Select(a)))))
exit(0)

@queryfunc
def query2(db) :
    return Select(db, "users", "kmill", "numbers")

print
print "Query2", query2

print db.select(query2)
print db.remove(query2)
print db.data

scott = {"username" : "scott"}
db.insert(path("users", "scott"), scott, overwrite=True)
db.insert(path("users", "scott", "numbers"), 22, append=True)

db.insert(path("users", "kmill", "numbers"), 13, append=True)
db.insert(path("users", "kmill", "numbers"), 22, append=True)
