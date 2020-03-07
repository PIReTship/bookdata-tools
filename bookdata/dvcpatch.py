"""
Support code for our custom DVC remote.
"""

import logging

from urllib.parse import urlparse

from dvc.remote.base import RemoteBASE
from dvc.output.base import OutputBase
from dvc.dependency.base import DependencyBase

_log = logging.getLogger('dvc.bgpatch')


class PGRemote(RemoteBASE):
    """
    PG status remote
    """
    scheme = 'pgstat'
    PARAM_CHECKSUM = 'md5'

    def __init__(self, *args, **kwargs):
        _log.error('creating pgremote')
        super().__init__(*args, **kwargs)

    def get_file_checksum(self, path_info):
        _log.error('checksum from {}', path_info)
        raise NotImplementedError()

    def copy(self, from_info, to_info):
        _log.error('copy from %s', from_info)
        _log.error('copy to %s', to_info)
        raise NotImplementedError()

    def exists(self, path_info):
        _log.error('exists? {}', path_info)
        _log.info('pi type {}', type(path_info))
        _log.info('pi scheme {}', path_info.scheme)
        _log.info('pi path {}', path_info.bucket)
        raise NotImplementedError()

    def _download(self, from_info, to_info, name, no_progress_bar):
        _log.error('exists? {}', from_info)
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
