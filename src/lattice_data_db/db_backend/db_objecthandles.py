#!/usr/bin/env python3

import sqlite3
import numpy
import uuid
import pathlib

from .array_converter import sentinel


class DBValue:
    def __init__(self, value, store_file=None, promise_loadfile="", loading_from_db=False, id=None):
        self._is_external = False
        if(isinstance(value, (int, float, complex))):
            self._value = numpy.array(value)
        elif isinstance(value, numpy.ndarray):
            self._value = value
        else:
            self._is_external = True
            self._value = value
            if(promise_loadfile == "" or (store_file is None and loading_from_db == False)):
                raise ValueError("values that are not either numpy arrays or (int, float, complex) must be supplied with promise_loadfile and store_file")
        self._store_file = store_file 
        self._promise_loadfile = promise_loadfile 
        self._id = id

    def _get_unique_filename(self, connection: sqlite3.Connection):
        # Get the rowid the value will have:
        cursor = connection.cursor()
        c = cursor.execute("SELECT MAX(rowid) FROM data_values")
        rowid = c.fetchone()[0]
        if(rowid is None):
            rowid = 0
        rowid += 1


        my_uuid = uuid.uuid1()

        filename = f"{rowid}-{my_uuid.hex}"
        return filename

    @classmethod
    def get_basepth(cls, connection: sqlite3.Connection):
        cursor = connection.cursor()
        c = cursor.execute("SELECT abspath FROM db_meta")
        return pathlib.Path(c.fetchone()[0])


    def store(self, connection: sqlite3.Connection):
        """
        Store the value in the database, either as numpy array or as external file.
        """
        if(self._is_external):
            if(self._store_file is None):
                raise ValueError("Missing store_file. This is either because you forgot to supply it or because the value was loaded from database. In the latter case, monkey patch it.")
            fname = self._get_unique_filename(connection)
            basepath = self.__class__.get_basepth(connection)

            outpath = basepath / fname 
            self._store_file(outpath, self._value)
            
            cursor = connection.cursor()
            cursor.execute("INSERT INTO data_values(is_inline, load_promise, relapath) VALUES(?, ?, ?)", (0, self._promise_loadfile, fname))
            rid = cursor.execute("SELECT LAST_INSERT_ROWID()").fetchone()[0]

        else:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO data_values(is_inline, av) VALUES(?, ?)", (1, self._value))
            rid = cursor.execute("SELECT LAST_INSERT_ROWID()").fetchone()[0]

        connection.commit()
        self._id = rid
        return rid

    @classmethod 
    def load(cls, connection: sqlite3.Connection, rid: int, store_file=None, locals=None):
        """
        Load a value from the database using the given value id ``rid``.

        The keyword argument ``locals`` is either None or a dict supplying some locals for the load_promise 
        that is used to load data from file.
        """
        cursor = connection.cursor()
        c = cursor.execute("SELECT is_inline, av, load_promise, relapath FROM data_values where rowid=?", (rid,))
        result = c.fetchone()

        if(result is None):
            raise ValueError(f"value with id {rid} not found in DB")

        is_inline, av, load_promise, relapath = result

        if(is_inline):
            if(not isinstance(av, numpy.ndarray)):
                raise TypeError("failed to load array value as numpy array; use ``detect_types=sqlite3.PARSE_DECLTYPES``.")
            return cls(av, id=rid)


        basepath = pathlib.Path(cls.get_basepth(connection))

        if(locals is not None):
            ctx = globals()
            ctx.update(locals)
            value = eval(load_promise, ctx)(str(basepath / relapath))
        else:
            value = eval(load_promise)(str(basepath / relapath))

        return cls(value, promise_loadfile=load_promise, loading_from_db=True, store_file=store_file, id=rid)

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self._value)}, ...)"


class Ensemble:
    def __init__(self, name, abspath, description, id=None):
        self._name = name 
        self._abspath = abspath 
        self._description = description
        self._id = id

    def store(self, connection: sqlite3.Connection):
        cursor = connection.cursor()
        cursor.execute("INSERT INTO ensembles VALUES(?, ?, ?)", (self._name, self._abspath, self._description))
        rid = cursor.execute("SELECT LAST_INSERT_ROWID()").fetchone()[0]
        connection.commit()

        self._id = rid

        return rid

    @classmethod
    def load(cls, connection: sqlite3.Connection, rid: int):
        cursor = connection.cursor()
        c = cursor.execute("SELECT name, abspath, description FROM ensembles where rowid=?", (rid,))
        result = c.fetchone()

        if(result is None):
            raise ValueError(f"ensemble with id {rid} not found in DB")

        return cls(*result, id=rid)

    @classmethod 
    def findby_name(cls, connection: sqlite3.Connection, name: str):
        cursor = connection.cursor()
        c = cursor.execute("SELECT rowid FROM ensembles where name=?", (name,))

        ensembles = [cls.load(connection, rid[0]) for rid in c.fetchall()]

        if(len(ensembles) == 1):
            # this should be the default case. avoid name clashes.
            return ensembles[0]
        # :(
        return ensembles




class Configuration:
    def __init__(self, ensemble, relapath, load_promise, id=None):
        if(isinstance(ensemble, Ensemble)):
            if(ensemble._id is None):
                raise ValueError("ensemble is not yet registered in database")
            self._ensemble = ensemble._id 

        else:
            self._ensemble = ensemble 
        self._relapath = relapath 
        self._load_promise = load_promise 
        self._id = id

    def store(self, connection: sqlite3.Connection):
        cursor = connection.cursor()
        cursor.execute("INSERT INTO connections VALUES(?, ?, ?)", (self._ensemble, self._relapath, self._load_promise))
        rid = cursor.execute("SELECT LAST_INSERT_ROWID()").fetchone()[0]
        connection.commit()

        self._id = rid

        return rid

    @classmethod
    def load(cls, connection: sqlite3.Connection, rid: int):
        cursor = connection.cursor()
        c = cursor.execute("SELECT ensemble, ensemble_relapath, load_promise FROM configurations where rowid=?", (rid,))
        result = c.fetchone()

        if(result is None):
            raise ValueError(f"configuration with id {rid} not found in DB")

        return cls(*result, id=rid)

def configuration2id(conf):
    if(isinstance(conf, Configuration)):
        if(conf._id is None):
            raise ValueError("configuration is not yet registered in database")
        return conf._id
    return conf
                


class Collection:
    """
    The ``._configurations`` attribute is always a list of configuration ids.
    """
    def __init__(self, name, configurations, id=None):
        self._name = name 

        self._configurations = [configuration2id(c) for c in configurations]
        self._id = id

    def store(self, connection: sqlite3.Connection):
        cursor = connection.cursor()
        cursor.execute("INSERT INTO collections VALUES(?)", (self._name,))
        rid = cursor.execute("SELECT LAST_INSERT_ROWID()").fetchone()[0]
        connection.commit()

        self._id = rid

        links = [(rid, c) for c in self._configurations]

        cursor = connection.cursor()
        cursor.executemany("INSERT INTO collections_contains VALUES(?, ?)", links)
        connection.commit()

        return rid

    @classmethod
    def load(cls, connection: sqlite3.Connection, rid: int):
        cursor = connection.cursor()
        c = cursor.execute("SELECT name FROM collections where rowid=?", (rid,))
        result = c.fetchone()

        if(result is None):
            raise ValueError(f"collection with id {rid} not found in DB")

        c = cursor.execute("SELECT configuration FROM collections_contains WHERE collection=?", (rid,))
        configurations = [f[0] for f in c]

        return cls(result[0], configurations, id=rid)
        
    @classmethod 
    def findby_name(cls, connection: sqlite3.Connection, name: str):
        cursor = connection.cursor()
        c = cursor.execute("SELECT rowid FROM collections where name=?", (name,))

        collections = [cls.load(connection, rid[0]) for rid in c.fetchall()]

        if(len(collections) == 1):
            # this should be the default case. avoid name clashes.
            return collections[0]
        # :(
        return collections


class Measurement:
    """
    ``._configuration`` is always configuration id.
    ``._value`` is always DBValue.
    """
    def __init__(self, configuration, value: DBValue, name: str):
        self._configuration = configuration2id(configuration)
        self._value = value 
        self._name = name

    def store(self, connection: sqlite3.Connection):
        cursor = connection.cursor()
        if(self._value._id is None):
            self._value.store(connection)

        cursor.execute("INSERT INTO measurements VALUES(?, ?, ?)", (self._configuration, self._value._id, self._name))
        rid = cursor.execute("SELECT LAST_INSERT_ROWID()").fetchone()[0]
        connection.commit()

        self._id = rid

        return rid

    @classmethod
    def load(cls, connection: sqlite3.Connection, rid: int, locals=None):
        """
        ``locals`` are passed to ``DBValue``.
        """
        cursor = connection.cursor()
        c = cursor.execute("SELECT configuration, value, name FROM measurements where rowid=?", (rid,))
        result = c.fetchone()

        if(result is None):
            raise ValueError(f"measurement with id {rid} not found in DB")

        
        configuration, value, name = result
        value = DBValue.load(connection, value, locals=locals)


        return cls(configuration, value, name, id=rid)
