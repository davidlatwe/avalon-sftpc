# avalon-sftpc
Avalon SFTP Client, for uploading Avalon workfile or representation to remote site via SFTP

### Requires
* `avalon-core`
* `pysftp`

### Demo

![avalon-sftpc](https://user-images.githubusercontent.com/3357009/63283882-fdecd700-c2e4-11e9-87fb-db1b619055c0.gif)

Still needs to be improved.
Stay tuned. :)

### Environment vars
`AVALON_SFTPC_SITES`: Folder which contains SFTP sites' config files
`AVALON_SFTPC_SITE`: SFTP site config name

### Get SSH key
`$ ssh-keyscan host`

### Generating job package file
```python
# Inside Maya (or other DCC App that has Avalon implementation)
from avalon_sftpc import util

exporter = util.JobExporter(remote_root="", remote_user="user")
exporter.from_workfile(additional_jobs)
out = exporter.export()
print(out)

```
