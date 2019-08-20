
import os
import contextlib
import hashlib
import threading
import json
import pysftp
import paramiko
from paramiko.py3compat import decodebytes
from multiprocessing import Process

from . import app
from .util import get_site


_STOP = "STOP"


def uploader():
    if app.demo:
        return MockUploader
    else:
        return Uploader


class Uploader(Process):

    def __init__(self, jobs, update, process_id):
        super(Uploader, self).__init__()
        self.jobs = jobs
        self.update = update
        self._id = process_id
        self.consuming = False

    def stop(self):
        self.jobs.put(_STOP)

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

            if job == _STOP:
                break

            src, dst, fsize = job.content

            def callback(transferred, to_be_transferred):
                """Update progress"""
                status = transferred == to_be_transferred
                self.update.put((job._id, transferred, status, self._id))

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
                    # When error happens, return file size as all transferred,
                    # so the progress and status can be visualized properly.
                    self.update.put((job._id, fsize, -1, self._id))


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

        producer = threading.Thread(target=produce, daemon=True)
        producer.start()

    def _digest(self, resource):
        with open(resource, "r") as file:
            packages = json.load(file)

        assert isinstance(packages, list)

        for data in packages:
            # A list of (local, remote) file path tuple
            # Ensure unique and sort for hashing
            files = sorted(set([(src, dst) for src, dst in data["files"]]))

            contents = list()
            total_size = 0
            hash_obj = hashlib.sha512()  # For preventing duplicate package

            for src, dst in files:
                hash_obj.update(src.encode())
                hash_obj.update(dst.encode())

                # Summing file size
                fsize = os.path.getsize(src)
                total_size += fsize

                contents.append((src, dst, fsize))

            if total_size == 0:
                raise Exception("Package size is 0, this should not happen.")

            package = {
                "project": data["project"],
                "type": data["type"],
                "description": data["description"],
                "site": data["site"],
                "files": contents,
                "status": 0,
                "count": len(files),
                "size": round(total_size / float(1024**2), 2),  # (MB)

                "byte": total_size,
                "hash": data["site"] + str(hash_obj.digest()),
            }

            yield package


class MockUploader(Process):

    mock_upload_speed = 10000
    mock_error_rate = 0.99

    def __init__(self, jobs, update, process_id):
        super(MockUploader, self).__init__()
        self.jobs = jobs
        self.update = update
        self._id = process_id
        self.consuming = False

    def stop(self):
        self.jobs.put(_STOP)

    def run(self):
        import random  # For mocking error

        while True:
            job = self.jobs.get()

            if job == _STOP:
                break

            src, dst = job.content

            # `JobItem` does not know the file size, since
            # we do not need that info in real uploader.
            to_be_transferred = os.path.getsize(src)

            # Compute
            chunk_size = self.mock_upload_speed
            steps = int(to_be_transferred / chunk_size)
            remain = to_be_transferred % chunk_size
            chunks = [chunk_size] * steps + [remain]

            try:
                for chunk in chunks:
                    job.transferred += chunk
                    self.update.put((job._id, job.transferred, 0, self._id))

                    # Simulate error
                    if random.random() > self.mock_error_rate:
                        raise IOError("This is not what I want.")

            except Exception:
                self.update.put((job._id, job.transferred, -1, self._id))
            else:
                self.update.put((job._id, job.transferred, 1, self._id))


class MockPackageProducer(object):
    pass
