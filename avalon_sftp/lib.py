
import os
from avalon import io

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


_DOC_CACHE = dict()


def cparenthood(representation):
    """Find all upstream documents with cache"""
    def find_parent(document):
        """Find and cache"""
        parent_id = document["parent"]
        if parent_id not in _DOC_CACHE:
            parent = io.find_one({"_id": parent_id})
            _DOC_CACHE[parent_id] = parent
        else:
            parent = _DOC_CACHE[parent_id]

        return parent

    version = find_parent(representation)
    subset = find_parent(version)
    asset = find_parent(subset)
    project = find_parent(asset)

    return [version, subset, asset, project]


class UploadPlugin(object):
    """
    """

    configfile = None
    additionals = list()

    def __init__(self):
        if os.path.isfile(self.configfile or ""):
            # Read settings from a configuration file
            parser = ConfigParser()
            parser.read(self.configfile)
            get = (lambda key: parser.get("avalon-sftp", key, fallback=""))
        else:
            # Read settings from environment variables, e.g. `AVALON_SFTP_HOST`
            get = (lambda key: os.getenv("AVALON_SFTP_" + key.upper()))

        self.config = {
            "host": get("host"),
            "username": get("username"),
            "password": get("password"),
            "hostkey": b"".join(get("hostkey").encode().split()),
            "max_conn": get("max_conn")
        }

    def map_workfile(self, workfile):
        raise NotImplementedError("Should be implemented in subclass.")

    def map_representation(self, representation):
        raise NotImplementedError("Should be implemented in subclass.")


class ProducerCondition(object):

    def __init__(self, condition):
        self.condition = condition

    def __enter__(self):
        self.condition.acquire()
        return self

    def __exit__(self, *args):
        self.condition.notify()
        self.condition.release()


class ConsumerCondition(object):

    def __init__(self, condition, pauser):
        self.condition = condition
        self.pauser = pauser

    def __enter__(self):
        self.condition.acquire()
        if self.pauser():
            self.condition.wait()
        return self

    def __exit__(self, *args):
        self.condition.release()
