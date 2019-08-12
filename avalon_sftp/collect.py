
import os
import re
import getpass
import threading
from avalon import io, api


def dependencies(version, deps=None):
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
        dependencies(dep_version, deps)

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


_document_cache = dict()


def parenthood_cache(representation):

    def find_parent(document):
        parent_id = document["parent"]
        if parent_id not in _document_cache:
            parent = io.find_one({"_id": parent_id})
            _document_cache[parent_id] = parent
        else:
            parent = _document_cache[parent_id]

        return parent

    version = find_parent(representation)
    subset = find_parent(version)
    asset = find_parent(subset)
    project = find_parent(asset)

    return [version, subset, asset, project]


class Inspector(object):

    def __init__(self):
        self._jobs = dict()

    def collect_workfile(self, remote_user=None, additional_jobs=None):
        additional_jobs = additional_jobs or []
        host = api.registered_host()
        jobs = dict()

        workfile = host.current_file()
        jobs.update(mapping_workfile(workfile, remote_user))

        for jb in additional_jobs:
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
        version_doc = io.find_one({"_id": io.ObjectId(version_id)})

        jobs = dict()

        all_version_ids = list(dependencies(version_doc).keys())

        for representation in io.find({"parent": {"$in": all_version_ids}}):
            jobs.update(mapping_representation(representation))

        return jobs


def mapping_representation(representation):

    EXT_IGNORE = [
        ".temp.tx",
        ".db",
        ".bz2",
        ".swatch",
    ]

    parents = parenthood_cache(representation)
    version, subset, asset, project = parents

    template = project["config"]["template"]["publish"]

    # Compute `root`
    repr_root = representation["data"].get("reprRoot")
    proj_root = project["data"].get("root")
    root = repr_root or proj_root or api.registered_root()

    # (NOTE) We map `representation` as a dir
    repr_dir = template.format(**{
        "root": root,
        "project": project["name"],
        "asset": asset["name"],
        "silo": asset["silo"],
        "subset": subset["name"],
        "version": version["name"],
        "representation": representation["name"],
        "user": api.Session.get("AVALON_USER", getpass.getuser()),
        "app": api.Session.get("AVALON_APP", ""),
        "task": api.Session.get("AVALON_TASK", "")
    })

    jobs = dict()

    for head, dir, files in os.walk(repr_dir):
        remote_dir = head[len(root):]

        for fname in files:
            if any(fname.endswith(ext) for ext in EXT_IGNORE):
                continue

            local_file = head + "/" + fname
            remote_path = remote_dir + "/" + fname

            local_file = os.path.normpath(local_file)
            remote_path = os.path.normpath(remote_path)

            jobs[local_file] = remote_path

    return jobs


def mapping_workfile(workfile, remote_user, shared_user=True):
    project = io.find_one({"type": "project"})
    local_user = api.Session.get("AVALON_USER", getpass.getuser())

    template = project["config"]["template"]["work"]

    workfile_name = os.path.basename(workfile)
    if shared_user and "{user}" in template:
        # Prevent overwriting each other's workfile
        name, ext = os.path.splitext(workfile_name)
        workfile_name = name + "_" + local_user + ext

    remote_path = template.format(**{
        "root": "",
        "project": api.Session["AVALON_PROJECT"],
        "silo": api.Session["AVALON_SILO"],
        "asset": api.Session["AVALON_ASSET"],
        "task": api.Session["AVALON_TASK"],
        "app": api.Session["AVALON_APP"],
        "user": remote_user or local_user,
    })
    remote_path += "/scenes/"
    remote_path += workfile_name

    workfile = os.path.normpath(workfile)
    remote_path = os.path.normpath(remote_path)

    return {workfile: remote_path}


def parse_stray_textures():
    """Find file nodes which pointing files that were not in published space

    If there are any texture files that has not been published...

    NOTE: This is additional job

    """
    from maya import cmds
    from reveries.maya import capsule

    ROOTS = ["O:", "P:", "Q:"]

    def is_versioned_path(path):
        pattern = (
            ".*[/\\\]publish"  # publish root
            "[/\\\]texture.*"  # subset dir
            "[/\\\]v[0-9]{3}"  # version dir
            "[/\\\]TexturePack"  # representation dir
        )
        return bool(re.match(pattern, path))

    # Unlock colorSpace...
    with capsule.ref_edit_unlock():
        cmds.setAttr("*:*.colorSpace", lock=False)

    jobs = dict()

    for file_node in cmds.ls(type="file"):
        file_path = cmds.getAttr(file_node + ".fileTextureName")
        if file_path and not is_versioned_path(file_path):
            for root in ROOTS:
                if file_path.startswith(root):
                    remote_path = file_path.replace(root, "", 1)

                    file_path = os.path.normpath(file_path)
                    remote_path = os.path.normpath(remote_path)

                    jobs[file_path] = remote_path

    return jobs
