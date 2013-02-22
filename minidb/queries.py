# query.py
# 2013 Kyle Miller
# queries for the minidb

import util
from util import assert_type
import itertools

class InconsistentData(Exception) :
    pass

def select(data, queryfunc) :
    """Selects everything from data which is returned by the query function."""
    return [v for p, v in queryfunc.query.execute(Fuel(), Bindings(queryfunc.var, (Path(), data)))]

def remove(data, queryfunc) :
    """Removes everything from 'data' which the query function returns from it.

    The data is untouched unless either the function returns
    successfully or InconsistentData is raised."""
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
    for p, v in query.execute(Fuel(), Bindings(dbvar, (Path(), data))) :
        if p is None :
            raise Exception("Cannot remove an element which did not come directly from the database.")
        addPath(p, True)

    def removePaths(data, paths) :
        if paths is None :
            raise InconsistentData("Unexpected path removal.")
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
    try :
        removePaths(data, paths)
    except InconsistentData :
        raise
    except Exception as x :
        raise InconsistentData(str(x))

def update(data, queryfunc, changes) :
    rootbinding = Bindings(queryfunc.var, (Path(), data))
    res = list(queryfunc.query.execute(Fuel(), rootbinding))
    instructions = []
    for p, v in res :
        newdata = []
        instructions.append(newdata)
        for change in changes :
            p2, v2 = change.valuefunc.value.eval(Fuel(), Bindings(change.valuefunc.var, (None, v), parent=rootbinding))
            newdata.append(v2)
    try :
        for (p, v), newdata in zip(res, instructions) :
            for change, new in zip(changes, newdata) :
                changepath = p.concat(change.path)
                if changepath is None or (changepath.parent is None and changepath.key is None) :
                    raise Exception("Cannot insert an object with None path")
                attachmentPoint = data
                if changepath.parent is not None :
                    attachmentPoint = changepath.parent.get(attachmentPoint)
                if change.append :
                    attachmentPoint = attachmentPoint.setdefault(changepath.key, [])
                    if type(attachmentPoint) is not list :
                        raise Exception("Cannot append to non-list")
                    attachmentPoint.append(new)
                elif change.newkey :
                    moved_data = change.path.get(v)
                    del attachmentPoint[changepath.key]
                    attachmentPoint[new] = moved_data
                else :
                    attachmentPoint[changepath.key] = new
    except Exception as x :
        raise InconsistentData(repr(x))

class ToUpdate(object) :
    def __init__(self, path, valuefunc, append=False, newkey=False) :
        if append and newkey :
            raise Exception("Not both 'append' and 'newkey' can be True")
        self.path = assert_type(path, Path)
        self.valuefunc = assert_type(valuefunc, ValueFunc)
        self.append = append
        self.newkey = newkey

class OutOfFuel(Exception) :
    pass

class Fuel(object) :
    """Represents some amount of fuel.  When the fuel runs out, it
    throws OutOfFuel.  The purpose is to limit queries somehow.  The
    default is an amount where a loop taking that many iterations
    takes about a second."""
    def __init__(self, amount=10000000) :
        self.amount = amount
    def consume(self) :
        self.amount -= 1
        if self.amount <= 0 :
            raise OutOfFuel()

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
    def execute(self, fuel, bindings) :
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
    def eval(self, fuel, bindings) :
        """Returns (path, data)."""
        raise NotImplemented()

@util.add_assert_type_coercion(Value)
def coerce_basic_types_to_constant(v) :
    if type(v) in util.allowed_types :
        return Constant(v)
    else :
        raise util.CoerceError()

class ValueFunc(object) :
    def __init__(self, var, value) :
        self.var = var
        if isinstance(var, Var) :
            self.var = var.name
        self.value = assert_type(value, Value)
    def __call__(self, arg) :
        return Apply(arg, self)
    def __repr__(self) :
        return "ValueFunc(%r, %r)" % (self.var, self.value)

@util.add_assert_type_coercion(ValueFunc)
def coerce_callable_to_valuefunc(f) :
    if callable(f) :
        return valuefunc(f)
    else :
        raise util.CoerceError()

class Apply(Value) :
    def __init__(self, value, func) :
        self.value = assert_type(value, Value)
        self.func = assert_type(func, ValueFunc)
    def eval(self, fuel, bindings) :
        fuel.consume()
        v = self.value.eval(fuel, bindings)
        subbindings = bindings
        if self.func.var is not None :
            subbindings = bindings.extend(self.func.var, v)
        return self.func.value.eval(fuel, subbindings)

class Get(Query, Value) :
    def __init__(self, source, *pathparts) :
        self.source = assert_type(source, Value)
        if len(pathparts) == 1 and isinstance(pathparts[0], Path) :
            self.path = pathparts[0]
        else :
            self.path = path(*pathparts)
    def execute(self, fuel, bindings) :
        path = self.path
        pathprime, data = self.source.eval(fuel, bindings)
        def makepath(k) :
            if pathprime is not None :
                return pathprime.concat(path)[k]
            else :
                return None
        source = path.get(data)
        if type(source) is dict :
            return ((makepath(k), v) for k,v in source.iteritems())
        else :
            return ((makepath(i), v) for i,v in itertools.izip(itertools.count(), source))
    def eval(self, fuel, bindings) :
        path = self.path
        itspath, value = self.source.eval(fuel, bindings)
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
    def __call__(self, arg) :
        return Bind(Return(arg), self)
    def __repr__(self) :
        return "Func(%r, %r)" % (self.var, self.query)

@util.add_assert_type_coercion(Func)
def coerce_callable_to_valuefunc(f) :
    if callable(f) :
        return queryfunc(f)
    else :
        raise util.CoerceError()

class Bind(Query) :
    def __init__(self, query, func) :
        self.query = assert_type(query, Query)
        self.func = assert_type(func, Func)
    def execute(self, fuel, bindings) :
        var = self.func.var
        funcquery = self.func.query
        for r in self.query.execute(fuel, bindings) :
            fuel.consume()
            subbindings = bindings
            if var is not None :
                subbindings = bindings.extend(var, r)
            for r2 in funcquery.execute(fuel, subbindings) :
                fuel.consume()
                yield r2
    def __repr__(self) :
        return "Bind(%r, %r)" % (self.query, self.func)

class Union(Query) :
    def __init__(self, *queries) :
        self.queries = [assert_type(q, Query) for q in queries]
    def execute(self, fuel, bindings) :
        for q in self.queries :
            for r in q.execute(fuel, bindings) :
                yield r
    def __repr__(self) :
        return "Union(*%r)" % self.queries

class Return(Query) :
    def __init__(self, value) :
        self.value = assert_type(value, Value)
    def execute(self, fuel, bindings) :
        return [self.value.eval(fuel, bindings)]
    def __repr__(self) :
        return "Return(%r)" % self.value

class Require(Query) :
    def __init__(self, value) :
        self.value = assert_type(value, Value)
    def execute(self, fuel, bindings) :
        if self.value.eval(fuel, bindings)[1] :
            return [(None, ())]
        else :
            return []
    def __repr__(self) :
        return "Require(%r)" % self.value

class Constant(Value) :
    def __init__(self, o) :
        self.o = o
    def eval(self, fuel, bindings) :
        return (None, self.o)
    def __repr__(self) :
        return "Constant(%r)" % self.o

class Var(Value) :
    def __init__(self, name) :
        self.name = assert_type(name, basestring)
    def eval(self, fuel, bindings) :
        return bindings[self.name]
    def __repr__(self) :
        return "Var(%r)" % self.name

a, b, c = Var("a"), Var("b"), Var("c")
x, y, z = Var("x"), Var("y"), Var("z")

class AsList(Value) :
    def __init__(self, query) :
        self.query = assert_type(query, Query)
    def eval(self, fuel, bindings) :
        return (None, [r[1] for r in self.query.execute(fuel, bindings)])
    def __repr__(self) :
        return "AsList(%r)" % self.query

class AsDict(Value) :
    def __init__(self, query) :
        self.query = assert_type(query, Query)
    def eval(self, fuel, bindings) :
        def make_key(p) :
            if p is None :
                return p
            return p.key
        return (None, dict((make_key(r[0]), r[1]) for r in self.query.execute(fuel, bindings)))
    def __repr__(self) :
        return "AsList(%r)" % self.query

class Op(Value) :
    def __init__(self, name, *params) :
        if name not in util.allowed_operations :
            raise Exception("Operation for Op must be allowed, not " + name)
        self.name = name
        self.op = util.allowed_operations[name]
        self.params = [assert_type(p, Value) for p in params]
    def eval(self, fuel, bindings) :
        eparams = [p.eval(fuel, bindings)[1] for p in self.params]
        return (None, self.op(*eparams))
    def __repr__(self) :
        return "Op(%r, *%r)" % (self.name, self.params)

class Or(Value) :
    def __init__(self, *params) :
        self.params = [assert_type(p, Value) for p in params]
    def eval(self, fuel, bindings) :
        r = None
        for p in self.params :
            r = p.eval(fuel, bindings)
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
        self.params = [assert_type(p, Value) for p in params]
    def execute(self, fuel, bindings) :
        r = None
        for p in self.params :
            r = p.eval(fuel, bindings)
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
    def reteach(self, query) :
        """Run a query, not binding its values.  If not in the
        terminal position, like '_ <- m' syntax in Haskell."""
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
    def execute(self, fuel, bindings) :
        """Builds the query then executes it."""
        self.buildQuery()
        return self.query.execute(fuel, bindings)
    def __repr__(self) :
        self.buildQuery()
        return repr(self.query)

def genvar(prefix="genvar", nextvar=[1]) :
    v = Var(prefix + str(nextvar[0]))
    nextvar[0] += 1
    return v


def queryfunc(f) :
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
    v = genvar(f.func_code.co_varnames[0])
    return Func(v, f(v))

def valuefunc(f) :
    v = genvar(f.func_code.co_varnames[0])
    return ValueFunc(v, f(v))
