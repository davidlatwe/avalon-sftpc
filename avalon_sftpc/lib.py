
import os
import threading
import getpass
import json
from collections import deque
from avalon import io, api, pipeline

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


def get_site(site_name):
    """
    """
    default_sites = os.path.dirname(__file__) + "/sites"
    sites = os.getenv("AVALON_SFTP_SITES", default_sites)

    site_cfg = sites + "/%s.cfg" % site_name
    if not os.path.isfile(site_cfg):
        # (TODO) This will crash app. Maybe an warning ?
        raise Exception("Site '%s' configuration file not found: %s"
                        "" % (site_name, site_cfg))

    # Read settings from a configuration file
    parser = ConfigParser()
    parser.read(site_cfg)
    get = (lambda key: parser.get("avalon-sftp", key, fallback=""))

    return {
        "host": get("host"),
        "username": get("username"),
        "password": get("password"),
        "hostkey": b"".join(get("hostkey").encode().split()),
    }


_DOC_CACHE = dict()


def cparenthood(representation, flush=False):
    """Find all upstream documents with cache"""
    global _DOC_CACHE

    if flush:
        _DOC_CACHE = dict()

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


def cget_representation_context(representation):
    """Return parenthood context for representation.

    Args:
        representation (dict): Rrepresentation as returned by the database.

    Returns:
        dict: The full representation context.

    """
    assert representation is not None, "This is a bug"

    version, subset, asset, project = cparenthood(representation)

    assert all([representation, version, subset, asset, project]), (
        "This is a bug"
    )

    context = {
        "project": project,
        "asset": asset,
        "subset": subset,
        "version": version,
        "representation": representation,
    }

    return context


class JobExporter(object):
    """
    """

    FILE_IGNORE = [
        ".temp.tx",
        ".db",
        ".bz2",
        ".swatch",
    ]

    def __init__(self, remote_root, remote_user):
        """
        Args:
            remote_root (str): Projects root at remote site
            remote_user (str): SFTP server username

        """
        self.jobs = list()
        self.remote_root = remote_root
        self.remote_user = remote_user

        self.available_loaders = api.discover(api.Loader)

    def export(self, out=None):
        if out is None:
            host = api.registered_host()
            workfile = host.current_file()

            out = os.path.splitext(workfile)[0] + ".sftp.job"

        with open(out, "w") as file:
            json.dump(self.jobs, file, indent=4)

    def add_job(self, files, type, description):
        """

        Args:
            files (list)
            type (str)
            description (str)

        """
        project = api.Session["AVALON_PROJECT"]
        site = api.Session["AVALON_SFTP"]

        job = {
            "project": project,
            "site": site,
            "type": type,
            "description": description,
            "files": files
        }
        self.jobs.append(job)

    def from_workfile(self, additional_jobs=None):
        """Generate jobs from workfile (DCC App agnostic)
        """
        # Add workfile
        session = api.Session
        host = api.registered_host()
        workfile = host.current_file()

        # Compute remote work dir
        project = io.find_one({"type": "project"})
        template = project["config"]["template"]["work"]
        remote_path = template.format(**{
            "root": self.remote_root,
            "project": session["AVALON_PROJECT"],
            "silo": session["AVALON_SILO"],
            "asset": session["AVALON_ASSET"],
            "task": session["AVALON_TASK"],
            "app": session["AVALON_APP"],
            "user": self.remote_user,
        })

        workfile_name = os.path.basename(workfile)
        local_user = session.get("AVALON_USER", getpass.getuser())
        # Prevent workfile overwrite when the remote username is not the
        # same as local username. This happens when multiple local users
        # using same remote account to access remote machine.
        same_user = local_user == self.remote_user
        if not same_user and "{user}" in template:
            # Prefix local username into file name
            workfile_name = local_user + "_" + workfile_name

        remote_path += "/scenes/"  # AVALON_SCENEDIR
        remote_path += workfile_name

        workfile = os.path.normpath(workfile)
        remote_path = os.path.normpath(remote_path)

        # Add workfile job
        self.add_job(files=[(workfile, remote_path)],
                     type="Workfile",
                     description="%s - %s" % (session["AVALON_ASSET"],
                                              os.path.basename(workfile)))

        # Additional jobs
        for job in additional_jobs:
            job()

    def from_representation(self, representation_id):
        """Generate job from representation
        """
        representation_id = io.ObjectId(representation_id)
        representation = io.find_one({"type": "representation",
                                      "_id": representation_id})
        context = cget_representation_context(representation)
        project = context["project"]
        asset = context["asset"]
        subset = context["subset"]
        version = context["version"]

        # Use loader to get local representation path
        loaders = (l for l in self.available_loaders
                   if pipeline.is_compatible_loader(l, context))
        Loader = next(loaders)
        loader = Loader(context)
        repr_path = loader.fname

        # Compute remote representation path
        template = project["config"]["template"]["publish"]
        remote_repr_path = template.format(**{
            "root": self.remote_root,
            "project": project["name"],
            "asset": asset["name"],
            "silo": asset["silo"],
            "subset": subset["name"],
            "version": version["name"],
            "representation": representation["name"],
        })

        # Get dir
        if os.path.isdir(repr_path):
            repr_dir = repr_path
            remote_dir = remote_repr_path
        elif os.path.isfile(repr_path):
            repr_dir = os.path.dirname(repr_path)
            remote_dir = os.path.dirname(remote_repr_path)
        else:
            raise Exception("Representation not exists.")

        # Collect all files
        jobs = list()

        for head, dir, files in os.walk(repr_dir):

            for fname in files:
                if any(fname.endswith(ext) for ext in self.FILE_IGNORE):
                    continue

                local_file = head + "/" + fname
                remote_path = remote_dir + "/" + fname
                local_file = os.path.normpath(local_file)
                remote_path = os.path.normpath(remote_path)

                jobs.append((local_file, remote_path))

        if not jobs:
            return

        # Add job
        description = ("[{asset}] {subset}.v{ver:0>3} - {repr}"
                       "".format(asset=asset["name"],
                                 subset=subset["name"],
                                 ver=version["name"],
                                 repr=representation["name"]))
        self.add_job(files=jobs,
                     type="Representation",
                     description=description)


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


class JobProducer(object):

    def __init__(self, generator):
        self.deque = deque()
        self.generator = generator
        self.condition = threading.Condition()
        self.producing = False
        self.interrupted = False
        self._thread = None

    def stop(self):
        self.interrupted = True

    def is_alive(self):
        if self._thread is not None:
            return self._thread.is_alive()
        else:
            return False

    def start(self):

        def produce():
            condition = ProducerCondition(self.condition)

            self.deque.clear()
            self.interrupted = False
            self.producing = True

            for job in self.generator:
                with condition:
                    if not self.interrupted:
                        self.deque.append(job)
                    else:
                        break

            # Bye
            with condition:
                self.producing = False
            self._thread = None

        producer = threading.Thread(target=produce, daemon=False)
        self._thread = producer
        self._thread.start()


class JobConsumer(object):

    def __init__(self, producer, consum):
        self.deque = producer.deque
        self.producer = producer
        self.consum = consum
        self.condition = producer.condition
        self.consuming = False
        self.interrupted = False

    def stop(self):
        self.interrupted = True

    def start(self, callback=None):
        producer = self.producer
        deque = self.deque
        pauser = (lambda: not deque and producer.is_alive())

        def consume():
            condition = ConsumerCondition(self.condition, pauser)

            self.interrupted = False
            self.consuming = True

            while True:
                with condition:
                    if not self.interrupted and (producer.producing or deque):
                        job = deque.popleft()
                    else:
                        break

                self.consum(job)
            # Bye
            self.consuming = False
            if not self.interrupted and callback is not None:
                callback()

        consumer = threading.Thread(target=consume, daemon=False)
        consumer.start()
