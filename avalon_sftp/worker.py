
import os
import contextlib
import time
import random
import threading
import pysftp
import paramiko
from paramiko.py3compat import decodebytes
from multiprocessing.pool import ThreadPool
from collections import deque
from avalon import io, api

from .lib import ProducerCondition


class Uploader(object):

    def __init__(self, host, username, password, hostkey=None, max_conn=10):
        self.host = host
        self.username = username
        self.password = password
        self.max_conn = max_conn
        self.cnopts = None

        if hostkey:
            sshkey = paramiko.RSAKey(data=decodebytes(hostkey))
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys.add(host, "ssh-rsa", sshkey)
            self.cnopts = cnopts

        self._uploading = None
        self._channel = None

    @contextlib.contextmanager
    def _connection(self):
        conn = pysftp.Connection(self.host,
                                 username=self.username,
                                 password=self.password,
                                 cnopts=self.cnopts)
        try:
            yield conn
        finally:
            conn.close()

    # Let the jobs able to keep coming
    def _process(self, jobs):

        self._channel = {j[0]: (0, 1) for j in jobs}

        def job():

            def _upload(job):

                src, dst = job

                def callback(transferred, to_be_transferred):
                    self._channel[src] = (transferred, to_be_transferred)

                with self._connection() as conn:
                    remote_dir = os.path.dirname(dst)
                    try:
                        conn.makedirs(remote_dir)
                    except IOError:
                        pass
                    try:
                        conn.put(src,
                                 dst,
                                 preserve_mtime=True,
                                 callback=callback)
                    except Exception:
                        self._channel[src] = None
                    # One

            pool = ThreadPool(self.max_conn)
            try:
                pool.imap(_upload, jobs)
            finally:
                pool.close()
                pool.join()
                self._uploading = False
                # End

        thread = threading.Thread(target=job)
        thread.daemon = False
        thread.start()
        self._uploading = True
        # Start

    def upload(self, jobs):
        self._process(jobs)
        # Cleanup channel

    def progress(self, func, step=0.1):

        def report():
            messages = self._channel.copy()
            func(messages)
            if self._uploading:
                # Next
                threading.Timer(step, report).start()

        threading.Thread(target=report).start()


def display_progress(messages):
    for file, progress in messages.items():
        print(file, " >>>> ", int(progress[0] / progress[1] * 100))
    print("---------------------------")


class JobProducer(object):
    """
    """

    def __init__(self):
        self.plugin = None
        self._jobs = dict()

    def from_workfile(self, workfile=None):
        """
        """
        host = api.registered_host()
        jobs = dict()

        workfile = workfile or host.current_file()  # ???
        jobs.update(self.plugin.map_workfile(workfile))

        for jb in self.plugin.additionals:
            jobs.update(jb())

        version_ids = set()
        for container in host.ls():
            id = container["versionId"]
            if id not in version_ids:
                version_ids.add(id)

        def _collect_versions():
            for id in version_ids:
                jobs.update(self.collect_versions(id))

        threading.Thread(target=_collect_versions).start()

        return jobs

    def collect_versions(self, version_id):
        """Need to refactor, since others may handle dependencies differently
        """
        version_doc = io.find_one({"_id": io.ObjectId(version_id)})

        jobs = dict()

        all_version_ids = list(self.dependencies(version_doc).keys())

        for representation in io.find({"parent": {"$in": all_version_ids}}):
            jobs.update(self.plugin.map_representation(representation))

        return jobs

    def dependencies(self, version, deps=None):
        """
        """
        deps = deps or dict()
        version_id = version["_id"]

        if version_id in deps:
            return

        deps[version["_id"]] = version

        for dependency_id in version["data"]["dependencies"]:
            dep_version_id = io.ObjectId(dependency_id)
            if dep_version_id in deps:
                continue

            dep_version = io.find_one({"_id": dep_version_id})
            self.dependencies(dep_version, deps)

            # Patching textures
            if "reveries.texture" in dep_version["data"]["families"]:
                for pre_version in io.find({"parent": version["parent"],
                                            "name": {"$lt": version["name"]}},
                                           sort=[("name", -1)]):

                    deps[pre_version["_id"]] = pre_version

                    pre_repr = io.find_one({"parent": pre_version["_id"]})
                    if "fileInventory" not in pre_repr["data"]:
                        break

        return deps


class MockUploader(object):

    def __init__(self, *args, **kwargs):
        pass

    def _process(self, jobs):
        pass

    def upload(self, jobs):
        pass


class MockJobProducer(object):

    def __init__(self):
        self.producing = False
        self.interrupted = False
        self.deque = deque()
        self.condition = threading.Condition()

    def stop(self):
        self.interrupted = True

    def from_workfile(self):
        self.stop()

        src_tmp = "/local/Proj/{asset}/pub/{subset}/{version}/file.%04d.dum"
        dst_tmp = "/remote/Proj/{asset}/pub/{subset}/{version}/file.%04d.dum"

        assets = [
            "tom",
            "david",
            "cat",
            "house",
        ]
        families = [
            "model",
            "pointcache",
            "look"
        ]
        subsets = [
            "Default",
            "StyleA",
            "StyleB",
        ]

        def produce():
            self.deque.clear()
            self.producing = True
            self.interrupted = False

            condition = ProducerCondition(self.condition)

            for asset in assets:
                for family in families:
                    for subset in subsets:
                        for version in range(1, 4):
                            data = {
                                "asset": asset,
                                "family": family,
                                "subset": subset,
                                "version": version * random.randint(1, 5),
                            }
                            src = src_tmp.format(**data)
                            dst = dst_tmp.format(**data)

                            data["job"] = (src, dst)

                            with condition:
                                if self.interrupted:
                                    self.producing = False
                                    return
                                else:
                                    self.deque.append(data)

                            time.sleep(random.random() * 0.1)

            with condition:
                self.producing = False

        producer = threading.Thread(target=produce, daemon=True)
        producer.start()
        return producer
