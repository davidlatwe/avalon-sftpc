# avalon-sftpc
Avalon SFTP Client, for uploading Avalon workfile or representation to remote site via SFTP

### Requires
* `avalon-core`
* `pysftp`

### Demo

```
$ python -m avalon_sftpc --demo
```

![avalon-sftpc](https://user-images.githubusercontent.com/3357009/63545377-d5b9de00-c559-11e9-9cb1-c4234c9bab71.gif)

### Environment vars
`AVALON_SFTPC_SITES`: Optional, dir path which contains SFTP sites' config files (`.cfg`). If not set, will look into `./avalon_sftpc/sites`

### Usage

1. Generating job package file

```python
# Inside Maya (or other DCC App that has Avalon implementation)
from avalon_sftpc import util

exporter = util.JobExporter(remote_root="",
                            remote_user="demo-user",
                            site="demo")


def collect_rogue_files():
    """Pseudo code, collect files that hasn't been published"""
    jobs = list()
    for file in unpublished_files():
        remote_path = mapping(file)
        jobs.append((file, remote_path))

    if jobs:
        exporter.add_job(files=jobs,
                         type="Rogue Files",
                         description="Rogues of %s" % session["AVALON_ASSET"])


def save_file():
    cmds.file(save=True, force=True)


# This is optional
additional_jobs = [
    collect_rogue_files,
    save_file,
]
exporter.from_workfile(additional_jobs)
out = exporter.export()
print(out)
# /../scenes/workfile_v0002.ma.sftp.job

```

2. Launch Avalon Uploader

```
$ python -m avalon_sftpc
```

Input package file path, and good to upload :)
