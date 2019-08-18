
import os
import time
import contextlib
import threading
import json
import pysftp
import paramiko
from paramiko.py3compat import decodebytes
from multiprocessing import Process

from .lib import get_site


class SFTPUploader(Process):

    def __init__(self, jobs, update):
        super(SFTPUploader, self).__init__()
        self.jobs = jobs
        self.update = update

    @contextlib.contextmanager
    def _connection(self, host, username, password, hostkey):
        cnopts = None
        if hostkey:
            sshkey = paramiko.RSAKey(data=decodebytes(hostkey))
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys.add(host, "ssh-rsa", sshkey)

        conn = pysftp.Connection(host,
                                 username=username,
                                 password=password,
                                 cnopts=cnopts)
        try:
            yield conn
        finally:
            conn.close()

    # Let the jobs able to keep coming
    def run(self):
        while True:
            job = self.jobs.get()

            src = job.src
            dst = job.dst

            def callback(transferred, to_be_transferred):
                self.update.put((job._id, transferred))

            with self._connection(**get_site(job.site)) as conn:
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
                    pass
                    # job["status"] = 4  # Failed


class PackageProducer(object):

    def __init__(self):
        self.producing = False
        self.interrupted = False

    def stop(self):
        self.interrupted = True

    def start(self, resource, on_produce, on_complete):

        def produce():
            self.interrupted = False
            self.producing = True

            for package in self._digest(resource):
                if not self.interrupted:
                    on_produce(package)
                else:
                    break

            # Bye
            self.producing = False
            on_complete()

        producer = threading.Thread(target=produce, daemon=False)
        producer.start()

    def _digest(self, resource):
        with open(resource, "r") as file:
            package_list = json.load(file)

        assert isinstance(package_list, list)

        for data in package_list:
            # A list of (local, remote) file path tuple
            files = [(src, dst) for src, dst in data["files"]]

            package = {
                "project": data["project"],
                "type": data["type"],
                "description": data["description"],
                "site": data["site"],
                "files": files,
            }

            yield package


class MockUploader(Process):

    consuming = False

    def __init__(self, jobs, update):
        super(MockUploader, self).__init__()
        self.jobs = jobs
        self.update = update

    def stop(self):
        self.terminate()
        self.consuming = False  # This is no use

    def run(self):
        while True:
            job = self.jobs.get()

            for i in range(100):
                time.sleep(0.1)
                job.transferred += 10
                self.update.put((job._id, job.transferred))
