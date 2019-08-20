
import os
import contextlib
import hashlib
import threading
import json
from multiprocessing import Process

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

# dependencies
import pysftp
import paramiko


_STOP = "STOP"


def get_site(site_name):
    """
    """
    default_sites = os.path.dirname(__file__) + "/sites"
    sites = os.getenv("AVALON_SFTPC_SITES", default_sites)

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
        "port": int(get("port") or 22),
        "username": get("username"),
        "password": get("password"),
        "hostkey": b"".join(get("hostkey").encode().split()),
    }


class Uploader(Process):

    def __init__(self, pipe_in, pipe_out, process_id):
        super(Uploader, self).__init__()
        self.pipe_in = pipe_in
        self.pipe_out = pipe_out
        self._id = process_id
        self.consuming = False

    def stop(self):
        self.pipe_in.put(_STOP)

    @contextlib.contextmanager
    def _connection(self, host, port, username, password, hostkey):
        cnopts = None
        if hostkey:
            hostkey = paramiko.py3compat.decodebytes(hostkey)
            sshkey = paramiko.RSAKey(data=hostkey)
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys.add(host, "ssh-rsa", sshkey)

        try:
            conn = pysftp.Connection(host,
                                     port=port,
                                     username=username,
                                     password=password,
                                     cnopts=cnopts)
            yield conn

        except Exception as error:
            # Mock a connection object for exit
            conn = type("MockConn", (object,), {"close": lambda: None})
            yield error

        finally:
            conn.close()

    # Let the jobs able to keep coming
    def run(self):
        while True:
            job = self.pipe_in.get()

            if job == _STOP:
                break

            src, dst, fsize = job.content

            def callback(transferred, to_be_transferred):
                """Update progress"""
                result = transferred == to_be_transferred
                self.pipe_out.put((job._id, transferred, result, self._id))

            with self._connection(**get_site(job.site)) as conn:
                if not isinstance(conn, pysftp.Connection):
                    # Connection error occurred
                    error = conn
                    self.pipe_out.put((job._id, fsize, error, self._id))
                    continue

                remote_dir = os.path.dirname(dst)

                try:
                    conn.makedirs(remote_dir)
                except Exception:
                    # Should be safe to ignore this error
                    pass

                try:
                    conn.put(src,
                             dst,
                             preserve_mtime=True,
                             callback=callback)
                except Exception as error:
                    # When error happens, return file size as all transferred,
                    # so the progress and status can be visualized properly.
                    self.pipe_out.put((job._id, fsize, error, self._id))


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
        packages = self._parse(resource)

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

    def _parse(self, json_file):
        with open(json_file, "r") as file:
            packages = json.load(file)
        assert isinstance(packages, list)
        return packages
