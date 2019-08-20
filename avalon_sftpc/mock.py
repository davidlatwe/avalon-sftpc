
import os
import random
import tempfile
import shutil
from multiprocessing import Process
from .worker import PackageProducer


_STOP = "STOP"


class MockUploader(Process):

    mock_upload_speed = 100
    mock_error_rate = 1  # 0.9999999

    def __init__(self, pipe_in, pipe_out, process_id):
        super(MockUploader, self).__init__()
        self.pipe_in = pipe_in
        self.pipe_out = pipe_out
        self._id = process_id
        self.consuming = False

    def stop(self):
        self.pipe_in.put(_STOP)

    def run(self):

        while True:
            job = self.pipe_in.get()

            if job == _STOP:
                break

            src, dst, fsize = job.content

            # Compute
            chunk_size = self.mock_upload_speed
            steps = int(fsize / chunk_size)
            remain = fsize % chunk_size
            chunks = [chunk_size] * steps + [remain]

            try:
                for chunk in chunks:
                    job.transferred += chunk
                    self.pipe_out.put((job._id, job.transferred, 0, self._id))

                    # Simulate error
                    dice = random.random()
                    if dice > self.mock_error_rate:
                        raise IOError("This is not what I want.")

            except Exception:
                self.pipe_out.put((job._id, fsize, -1, self._id))
            else:
                self.pipe_out.put((job._id, job.transferred, 1, self._id))


class MockPackageProducer(PackageProducer):

    def _parse(self, json_file):
        return self.make_dummies()

    BIG = 1024**2 * 10
    MID = 1024**2 * 3
    SML = 1024**2

    dummies = {
        "/some": {
            "fname": "work_%d.dum",
            "size": BIG,
            "variant": 5000,
            "count": 1,
        },
        "/fooA": {
            "fname": "bar_%04d.dum",
            "size": MID,
            "variant": 1000,
            "count": 12,
        },
        "/fooB": {
            "fname": "bar_%04d.dum",
            "size": MID,
            "variant": 1000,
            "count": 18,
        },
        "/cacheA": {
            "fname": "many_%d.dum",
            "size": SML,
            "variant": 500,
            "count": 75,
        },
        "/cacheB": {
            "fname": "many_%d.dum",
            "size": SML,
            "variant": 500,
            "count": 123,
        },
    }

    def make_dummies(self):

        rootdir = tempfile.mkdtemp()
        print("Generating dummies to '%s'.." % rootdir)

        def fgenerator(fname, size, variant, count):
            for i in range(count):

                file = fname % i
                if os.path.isfile(file):
                    yield file
                    continue

                fsize = size + random.randint(0, variant)

                with open(file, "wb") as fp:
                    batch = int(fsize / 80)
                    remain = fsize % 80
                    line = b"0" * 79 + b"\n"
                    for x in range(batch):
                        fp.write(line)
                    if remain:
                        fp.write(b"0" * (remain - 1) + b"\n")

                yield file

        origin = os.getcwd()
        for dirname, args in self.dummies.items():
            dirpath = rootdir + dirname

            os.makedirs(dirpath, exist_ok=True)
            os.chdir(dirpath)

            files = list()
            for file in fgenerator(**args):
                fpath = dirpath + "/" + file

                files.append((fpath, "mock-dst"))

            os.chdir(origin)

            job = {
                "project": "Mock",
                "site": "demo",
                "type": "dummy",
                "description": "Some dummies for demo",
                "files": files,
            }

            yield job

        # File size has been collected, we can now delete all those dummies.
        print("Removing dummies from '%s'.." % rootdir)
        shutil.rmtree(rootdir, ignore_errors=True)
