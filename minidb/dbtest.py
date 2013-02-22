from minidb import *

if __name__=="__main__" :
    from queries import *
    import threading

    @queryfunc
    def query(db) :
        return (Do()
                .foreach(a, Get(db, "users"))
                .let(b, a)
                .require(Op("ne", "kmill", Get(b, "username")))
                .require(Op("any", AsList(Do()
                                          .foreach(b, Get(b, "numbers"))
                                          .ret(Op("eq", 22, b)))))
                .ret(a))

    print query

    logging.basicConfig(level=logging.INFO)
    db = Database("test.db")
    db.data = {"users" : {"kmill" : {"username" : "kmill",
                                     "numbers" : [22, 13]},
                          "scott" : {"username" : "scott",
                                     "numbers" : [22]}}}
    db.commit()
               
    #print db.select(query)

    # adds 1 to everyone's numbers
    db.update(lambda db : Do()
              .foreach(a, Get(db, "users"))
              .reteach(Get(a, "numbers")),
              [ToUpdate(Path(), lambda x : Op("add", x, 1))])
    # adds 22 to everyone's list of numbers
    db.update(lambda db : Get(db, "users"),
              [ToUpdate(Path()["numbers"], lambda x : 22, append=True)])
    # sets the username of scott to skook
    db.update(lambda db : Return(Get(db, "users", "scott")),
              [ToUpdate(Path()["username"], lambda x : "skook")])
    def getUsername(x) :
        return Get(x, "username")
    # sets the key for every user to their username
    db.update(lambda db : Get(db, "users"),
              [ToUpdate(Path(), getUsername, newkey=True)])

    # renames every table in the database to "a"
    db.update(lambda db : Get(db),
              [ToUpdate(Path(), lambda x : "a", newkey=True)])

    print db.data

    exit(0)

    def testThread() :
        print "In thread"
        print db.select(query)
        print "Exiting thread"    

    
    #print db.select(QueryFunc(a, Return(AsDict(Select(a)))))

    @queryfunc
    def query2(db) :
        return (Do()
                .foreach(a, Get(db, "users", "kmill", "numbers"))
                .require(Or(True, True))
                .ret(4))

    def testRemove() :
        print "Query2", query2
        print db.remove(query2)
    threading.Thread(target=testRemove).start()

    for i in xrange(3) :
        threading.Thread(target=testThread).start()
        import time


    exit(0)
    print db.data

    scott = {"username" : "scott"}
    db.insert(path("users", "scott"), scott, overwrite=True)
    db.insert(path("users", "scott", "numbers"), 22, append=True)

    db.insert(path("users", "kmill", "numbers"), 13, append=True)
    db.insert(path("users", "kmill", "numbers"), 22, append=True)
