"""
Support code for our custom DVC remote.
"""

import logging

from urllib.parse import urlparse
import hashlib

from dvc.remote.base import RemoteBASE
from dvc.output.base import OutputBase
from dvc.dependency.base import DependencyBase

from . import tracking

_log = logging.getLogger('dvc.bgpatch')


class PGRemote(RemoteBASE):
    """
    PG status remote
    """
    scheme = 'pgstat'
    PARAM_CHECKSUM = 'md5'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_file_checksum(self, path_info):
        _log.debug('checksum from %s', path_info)
        status = tracking.stage_status(path_info.bucket)
        h = hashlib.md5()
        h.update(status.encode('utf-8'))
        return h.hexdigest()

    def copy(self, from_info, to_info):
        _log.debug('copy from %s', from_info)
        _log.debug('copy to %s', to_info)
        raise NotImplementedError()

    def exists(self, path_info):
        _log.debug('exists? %s', path_info)
        return tracking.stage_exists(path_info.bucket)

    def remove(self, path_info):
        _log.info('asked to remove %s, ignoring', path_info)

    def _download(self, from_info, to_info, name, no_progress_bar):
        _log.info('download requested for %s', from_info)
        raise NotImplementedError()


class PGOut(OutputBase):
    REMOTE = PGRemote


class PGDep(DependencyBase, OutputBase):
    REMOTE = PGRemote


def patch():
    "Patch DVC to include our classes"

    import dvc.output, dvc.dependency, dvc.config
    dvc.output.OUTS.append(PGOut)
    dvc.output.OUTS_MAP['pgstat'] = PGOut

    dvc.dependency.DEPS.append(PGDep)
    dvc.dependency.DEP_MAP['pgstat'] = PGDep

    # from dvc.cache import Cache, _make_remote_property
    # Cache.pgstat = _make_remote_property('pgstat')
    # dvc.config.Config.SCHEMA['cache']['pgstat'] = str
    # dvc.config.Config.COMPILED_SCHEMA = dvc.config.Schema(dvc.config.Config.SCHEMA)
