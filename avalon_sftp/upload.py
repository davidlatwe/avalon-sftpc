
import os
import contextlib
import threading
import pysftp
import paramiko
from paramiko.py3compat import decodebytes
from multiprocessing.pool import ThreadPool
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


def create_uploader_via_config(fname):
    """Create an Uploader instance from a config file

    Args:
        fname (str): Config file path

    """
    parser = ConfigParser()
    parser.read(fname)
    get = (lambda key, **kwargs: parser.get("conn", key, **kwargs))
    config = {
        "host": get("host"),
        "username": get("user"),
        "password": get("pswd"),
        "hostkey": b"".join(get("key", fallback="").encode().split()),
        "max_conn": get("max_conn")
    }

    return Uploader(**config)


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
