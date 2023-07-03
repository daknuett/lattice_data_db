import os
import json
import numpy 
import datetime
import uuid
import glob
import warnings
import collections
import copy


"""
High throughput data store.
"""

class HTPStore:
    """
    High throughput data store. Uses individual files per measurement 
    to ensure high throughput and thread/process safety.

    To instantiate a new database use ``HTPStore.new``, to open a database use ``HTPStore.open``.

    The database is semi-heterogenic: All data must be stored/loaded in the same way,
    but the data might be different. One might, for instance use 
    ``store=lambda fname, x: json.store(x, open(fname, "r"))`` to store any kind of JSON serializable 
    data.

    The ``store`` function is a function that takes a path as first argument and the data as second argument.
    The ``loadpromise`` must be a function taking a path and must read the data from file.

    ``all_data`` may not work if ``loadpromise`` requires ``locals`` being passed to ``get_data``.
    """
    dbinfo_name = "db.info.json"
    jrnl_prefix = "db.jrnl."
    def __init__(self, name, abspath, loadpromise, store, info, readonly):
        self._name = name
        self._loadpromise = loadpromise 
        self._store = store
        self._abspath = abspath

        self._full_path = os.path.join(abspath, name)
        if(not os.path.exists(self._full_path)):
            raise ValueError("database does not exist. use HTPStore.new() to create it.")

        self._dbinfo_name = info["info"]["dbinfoname"]
        self._info = info 
        self._readonly = readonly


    @classmethod 
    def new(cls, name, abspath=None, loadpromise="lambda s: numpy.load(s + '.npy')", store=numpy.save):
        """
        Creates a new HTPStore and returns a new HTPStore object associated with it.
        """
        if(abspath is None):
            abspath = os.getcwd()

        full_path = os.path.join(abspath, name)
        os.makedirs(full_path)

        datetime_format = "%H:%M:%S-%d%m%Y"
        info = {
                "info": {"type": "database"
                             , "class": cls.__name__
                             , "created": datetime.datetime.now().strftime(datetime_format)
                             , "datetime.format": datetime_format
                             , "lastjrnlsync": datetime.datetime.now().strftime(datetime_format)
                             , "jrnlprefix": cls.jrnl_prefix
                             , "dbinfoname": cls.dbinfo_name
                             , "glossary": "https://github.com/daknuett/lattice_data_db"
                             , "loadpromise": loadpromise
                         }
                , "data_tags": []
                , "data_info": {}
                , "data_groups": {}
               }
        with open(os.path.join(full_path, cls.dbinfo_name), "w") as dbinfo_file:
            json.dump(info, dbinfo_file)

        return cls.open(name, syncjournal=False, store=store, abspath=abspath)

    @classmethod
    def open(cls, name, syncjournal=False, store=numpy.save, abspath=None, readonly=False):
        """
        Opens an existing HTPStore, synchronizes the journal, if syncjournal is True.
        """
        if(abspath is None):
            abspath = os.getcwd()

        full_path = os.path.join(abspath, name)
        if(not os.path.exists(full_path)):
            raise ValueError("database does not exist. use HTPStore.new() to create it.")

        with open(os.path.join(full_path, cls.dbinfo_name), "r") as dbinfo_file:
            info = json.load(dbinfo_file)

        db = cls(name, abspath, info["info"]["loadpromise"], store, info, readonly)

        if(syncjournal):
            db.sync_journal()

        return db

    def fix_file_name(self, name):
        name = name.replace(":", "_colon_")
        name = name.replace(" ", "_ws_")
        name = name.replace("/", "_slash_")
        return name


    def store(self, name, data, group=None, meta={}):
        """
        Store the data with the given name. 
        ``meta`` must be JSON serializable.
        """
        if(self._readonly):
            raise Exception("database is in read only mode")

        jrnl_name_suffix = uuid.uuid1().hex + uuid.uuid4().hex
        jrnl_filename = self._info["info"]["jrnlprefix"] + jrnl_name_suffix
        file_name = self.fix_file_name(name)
        jrnl_entry = {"name": name
                      , "file": file_name
                      , "jrnl_entry": jrnl_filename
                      , "jrnl_entry_create": self.get_now()
                      , "group": group
                      , "meta": meta}

        with open(os.path.join(self._full_path, jrnl_filename), "w") as jrnl:
            json.dump(jrnl_entry, jrnl)

        self._store(os.path.join(self._full_path, file_name), data)

    def get_now(self):
        return datetime.datetime.now().strftime(self._info["info"]["datetime.format"])

    @property
    def synced_journal(self):
        jrnl_prefix = self._info["info"]["jrnlprefix"]
        jrnl_files = glob.glob(os.path.join(self._full_path, jrnl_prefix) + "*")
        if(len(jrnl_files) == 0):
            return True
        return False


    def sync_journal(self):
        jrnl_prefix = self._info["info"]["jrnlprefix"]
        jrnl_files = glob.glob(os.path.join(self._full_path, jrnl_prefix) + "*")
        if(len(jrnl_files) == 0):
            return 0

        new_tags = []
        new_infos = {}
        new_groups = collections.defaultdict(list)
        files_to_delete = []


        for jrnl_f in jrnl_files:
            with open(jrnl_f, "r") as jrnl:
                jrnl_entry = json.load(jrnl)

            name = jrnl_entry["name"]

            # Check in the dicts due to better performance.
            if(name in new_infos):
                raise Exception(f"journal entry doubler: {name} found but already in transaction (offending journal entry: {jrnl_entry['file']})")

            if(name in self._info["data_info"]):
                # resolve name collision
                if(self._info["data_info"][name]["jrnl_entry_create"] == jrnl_entry["jrnl_entry_create"]):
                    # Already synced.
                    warnings.warn(f"found journal entry collision: journal entry {jrnl_entry['file']} not deleted but already synced")
                    continue
                # True collision
                raise Exception(f"journal entry collision: {name} in database and {jrnl_entry['file']} exists")

            if(jrnl_entry["group"] is not None):
                new_groups[jrnl_entry["group"]].append(name)
            new_tags.append(name)
            jrnl_entry["jrnl_entry_sync"] = self.get_now()
            new_infos[name] = jrnl_entry
            files_to_delete.append(jrnl_f)


        self._info["data_tags"].extend(new_tags)
        self._info["data_info"].update(new_infos)
        self._info["info"]["lastjrnlsync"] = self.get_now()

        for group, tags in new_groups.items():
            if(group not in self._info["data_groups"]):
                self._info["data_groups"][group] = tags
            else:
                self._info["data_groups"][group].extend(tags)

        with open(os.path.join(self._full_path, self._dbinfo_name), "w") as dbinfo_file:
            json.dump(self._info, dbinfo_file)

        for f in files_to_delete:
            os.remove(f)

        return len(files_to_delete)

    @property
    def tags(self):
        if(not self.synced_journal):
            warnings.warn("journal is not synced")
        return copy.copy(self._info["data_tags"])

    @property
    def groups(self):
        if(not self.synced_journal):
            warnings.warn("journal is not synced")
        return copy.copy(self._info["data_groups"])

    @property
    def meta(self):
        if(not self.synced_journal):
            warnings.warn("journal is not synced")
        return copy.copy(self._info["data_info"])


    def get_data(self, tag, locals=None):
        file_name = self._info["data_info"][tag]["file"]
        if(locals is not None):
            ctx = globals()
            ctx.update(locals)
            return eval(self._loadpromise, ctx)(os.path.join(self._full_path, file_name))
        else:
            return eval(self._loadpromise)(os.path.join(self._full_path, file_name))

    @property 
    def all_data(self):
        if(not self.synced_journal):
            warnings.warn("journal is not synced")
        data = {}
        for tag in self.tags:
            # This may not work. See docstring of class.
            data[tag] = {"meta": self._info["data_info"][tag], "data": self.get_data(tag)}

        return  data

