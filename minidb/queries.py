# query.py
# 2013 Kyle Miller
# queries for the minidb

import util
from util import assert_type
import itertools

def select(data, queryfunc) :
    return [v for p, v in queryfunc.query.execute(Bindings(queryfunc.var, (None, data)))]

def remove(data, queryfunc) :
    dbvar = queryfunc.var
    query = queryfunc.query
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
    for p, v in query.execute(Bindings(dbvar, (None, data))) :
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
        """Returns [(path, data)]"""
        raise Exception("Unimplemented")
    def __ge__(self, other) :
        if isinstance(other, Func) :
            return Bind(self, other)
        else :
            raise NotImplemented()
    def __rshift__(self, other) :
        if isinstance(other, Query) :
            return Bind(self, Func(None, other))
        else :
            raise NotImplemented()
    def __add__(self, other) :
        if isinstance(other, Query) :
            return Union(self, other)
        else :
            raise NotImplemented()

class Value(object) :
    def eval(self, bindings) :
        """Returns (path, data)."""
        raise NotImplemented()

class Get(Query, Value) :
    def __init__(self, source, *pathparts) :
        self.source = assert_type(source, Value)
        if len(pathparts) == 1 and isinstance(pathparts[0], Path) :
            self.path = pathparts[0]
        else :
            self.path = path(*pathparts)
    def execute(self, bindings) :
        path = self.path
        source = path.get(self.source.eval(bindings)[1])
        if type(source) is dict :
            return ((path[k], v) for k,v in source.iteritems())
        else :
            return ((path[i], v) for i,v in itertools.izip(itertools.count(), source))
    def eval(self, bindings) :
        path = self.path
        itspath, value = self.source.eval(bindings)
        value = path.get(value)
        return (itspath.concat(path) if itspath is not None else None, value)
    def __repr__(self) :
        return "Get(%r, %r)" % (self.source, self.path)

class Func(object) :
    def __init__(self, var, query) :
        self.var = var
        if isinstance(var, Var) :
            self.var = var.name
        self.query = assert_type(query, Query)
    def __repr__(self) :
        return "Func(%r, %r)" % (self.var, self.query)

class Bind(Query) :
    def __init__(self, query, func) :
        self.query = assert_type(query, Query)
        self.func = assert_type(func, Func)
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
        return "Bind(%r, %r)" % (self.query, self.func)

class Union(Query) :
    def __init__(self, *queries) :
        self.queries = [assert_type(q, Query) for q in queries]
    def execute(self, bindings) :
        for q in self.queries :
            for r in q.execute(bindings) :
                yield r
    def __repr__(self) :
        return "Union(*%r)" % self.queries

class Return(Query) :
    def __init__(self, value) :
        self.value = value if isinstance(value, Value) else Constant(value)
    def execute(self, bindings) :
        return [self.value.eval(bindings)]
    def __repr__(self) :
        return "Return(%r)" % self.value

class Require(Query) :
    def __init__(self, value) :
        self.value = value if isinstance(value, Value) else Constant(value)
    def execute(self, bindings) :
        if self.value.eval(bindings)[1] :
            return [(None, ())]
        else :
            return []
    def __repr__(self) :
        return "Require(%r)" % self.value

class Constant(Value) :
    def __init__(self, o) :
        self.o = o
    def eval(self, bindings) :
        return (None, self.o)
    def __repr__(self) :
        return "Constant(%r)" % self.o

class Var(Value) :
    def __init__(self, name) :
        self.name = assert_type(name, basestring)
    def eval(self, bindings) :
        return bindings[self.name]
    def __repr__(self) :
        return "Var(%r)" % self.name

a, b, c = Var("a"), Var("b"), Var("c")
x, y, z = Var("x"), Var("y"), Var("z")

class AsList(Value) :
    def __init__(self, query) :
        self.query = assert_type(query, Query)
    def eval(self, bindings) :
        return (None, [r[1] for r in self.query.execute(bindings)])
    def __repr__(self) :
        return "AsList(%r)" % self.query

class AsDict(Value) :
    def __init__(self, query) :
        self.query = assert_type(query, Query)
    def eval(self, bindings) :
        def make_key(p) :
            if p is None :
                return p
            return p.key
        return (None, dict((make_key(r[0]), r[1]) for r in self.query.execute(bindings)))
    def __repr__(self) :
        return "AsList(%r)" % self.query

class Op(Value) :
    def __init__(self, name, *params) :
        if name not in util.allowed_operations :
            raise Exception("Operation for Op must be allowed, not " + name)
        self.name = name
        self.op = util.allowed_operations[name]
        self.params = [p if isinstance(p, Value) else Constant(p) for p in params]
    def eval(self, bindings) :
        eparams = [p.eval(bindings)[1] for p in self.params]
        return (None, self.op(*eparams))
    def __repr__(self) :
        return "Op(%r, *%r)" % (self.name, self.params)

class Or(Value) :
    def __init__(self, *params) :
        self.params = [p if isinstance(p, Value) else Constant(p) for p in params]
    def eval(self, bindings) :
        r = None
        for p in self.params :
            r = p.eval(bindings)
            if r[1] :
                return r
        if r == None :
            return (None, False)
        else :
            return r
    def __repr__(self) :
        return "Or(*%r)" % (self.params,)
class And(Value) :
    def __init__(self, *params) :
        self.params = [p if isinstance(p, Value) else Constant(p) for p in params]
    def execute(self, bindings) :
        r = None
        for p in self.params :
            r = p.eval(bindings)
            if not r[1] :
                return r
        if r == None :
            return (None, True)
        else :
            return r
    def __repr__(self) :
        return "And(*%r)" % (self.params,)

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

class Do(Query) :
    """A pseudo-query which builds up a query using some nicer syntax.
    It's like the 'do' notation from Haskell."""
    def __init__(self) :
        self.binds = []
        self.query = None
    def let(self, var, value) :
        """Let a variable be a value. Like a 'let' in Haskell."""
        self.binds.append((var, Return(value)))
        return self
    def foreach(self, var, query) :
        """Let a variable iteratively be each value from a query. Like
        'var <- m' syntax in Haskell."""
        self.binds.append((var, query))
        return self
    def foreach_(self, query) :
        """Run a query dropping its values.  Like '_ <- m' syntax in
        Haskell."""
        self.binds.append((None, query))
        return self
    def ret(self, value) :
        """Convenience function for Do.foreach_(Return(value))"""
        self.binds.append((None, Return(value)))
        return self
    def require(self, value) :
        """Convenience function for Do.foreach_(Require(value))"""
        self.binds.append((None, Require(value)))
        return self
    def buildQuery(self) :
        """Builds the internal query if it hasn't already been built."""
        if self.query is not None :
            return
        builtq = None
        for v, q in reversed(self.binds) :
            if builtq is None :
                if v is not None :
                    raise Exception("Last expression in Do must not be a 'let'.")
                builtq = q
            else :
                builtq = Bind(q, Func(v, builtq))
        self.query = builtq
    def execute(self, bindings) :
        """Builds the query then executes it."""
        self.buildQuery()
        return self.query.execute(bindings)
    def __repr__(self) :
        self.buildQuery()
        return repr(self.query)

def queryfunc(f, nextvar=[1]) :
    """A "decorator" which passes in a newly generated variable to the
    given function to make a query function of it.

    For instance,
    
    @queryfunc
    def q(x) :
        a = Var("a")
        return (Do()
                .foreach(a, Get(x, "users"))
                .require(somecondition(a))
                .ret(a))

    is (essentially) equivalent to

    x = Var("x")
    a = Var("a")
    q = Func(x, Bind(Get(x, "users"),
                     Func(a, Bind(somecondition(a),
                                  Func(None, Return(a))))))
    """
    def genvar(prefix="genvar") :
        v = Var(prefix + str(nextvar[0]))
        nextvar[0] += 1
        return v
    v = genvar(f.func_code.co_varnames[0])
    return Func(v, f(v))
