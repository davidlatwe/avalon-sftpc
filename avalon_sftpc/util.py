
import os
import getpass
import json
from avalon import io, api, pipeline


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
    """Tool for generate job package file for upload

    Args:
        remote_root (str): Projects root at remote site
        remote_user (str): SFTP server username
        site (str): SFTP server connection config name

    """

    FILE_IGNORE = [
        ".temp.tx",
        ".db",
        ".bz2",
        ".swatch",
    ]

    def __init__(self, remote_root, remote_user, site=None):
        self.jobs = list()
        self.remote_root = remote_root
        self.remote_user = remote_user

        # `AVALON_SFTPC_SITE` deprecated, this is for backward compat
        site = site or api.Session.get("AVALON_SFTPC_SITE")
        assert site, "SFTP site name not provided."
        self.site = site

        self.available_loaders = api.discover(api.Loader)

    def export(self, out=None):
        """Write out JSON format job package file

        Args:
            out (str, optional): Output file path

        """
        if out is None:
            host = api.registered_host()
            workfile = host.current_file() or "temp"
            out = os.path.abspath(workfile + ".sftp.job")

        with open(out, "w") as file:
            json.dump(self.jobs, file, indent=4)

        return out

    def add_job(self, files, type, description):
        """Append job

        Args:
            files (list): A list of local path and remote path tuple
            type (str): Name of job type
            description (str): Line of job detail

        """
        job = {
            "project": api.Session["AVALON_PROJECT"],
            "site": self.site,
            "type": type,
            "description": description,
            "files": files
        }
        self.jobs.append(job)

    def from_workfile(self, additional_jobs=None):
        """Generate jobs from workfile (DCC App agnostic)

        Args:
            additional_jobs (list, optional): A list of callbacks

        """
        # Add workfile
        session = api.Session
        host = api.registered_host()
        workfile = host.current_file()
        if workfile is None:
            # Must be saved since we are parsing workfile here
            raise Exception("Could not obtain workfile path.")

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
        for job in additional_jobs or []:
            job()

    def from_representation(self, representation_id):
        """Generate job from representation

        Args:
            representation_id (str): Avalon representation Id

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
        for Loader in self.available_loaders:
            if not pipeline.is_compatible_loader(Loader, context):
                continue

            loader = Loader(context)
            if hasattr(loader, "fname"):
                repr_path = loader.fname
                break
        else:
            raise Exception("Counld not find Loader for '%s'"
                            "" % representation["name"])

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

        repr_dir = os.path.normpath(repr_dir)
        remote_dir = os.path.normpath(remote_dir)

        for head, dir, files in os.walk(repr_dir):

            remote_head = remote_dir + head[len(repr_dir):]

            for fname in files:
                if any(fname.endswith(ext) for ext in self.FILE_IGNORE):
                    continue

                local_file = os.path.join(head, fname)
                remote_path = os.path.join(remote_head, fname)

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
