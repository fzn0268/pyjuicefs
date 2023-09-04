"""
pyjuicefs base module.

This is the principal module of the pyjuicefs project.
here you put your main classes and objects.

Be creative! do whatever you want!

If you want to replace this with a Flask application run:

    $ make init

and then choose `flask` as template.
"""
import os
from datetime import datetime, timezone
from typing import Dict, Union

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options
from juicefs import JuiceFS, io
from juicefs.juicefs import DEFAULT_FILE_MODE
from juicefs.libjfs import DirEntry

# example constant variable
NAME = "pyjuicefs"
MODE_MASK_R = 4


class JuiceFileSystem(AbstractFileSystem):

    def __init__(
            self,
            name: str,
            config: Dict[str, Union[str, bool, int, float]] = {},
            **kwargs
    ):
        """

        Parameters
        ----------
        name: str
            Name used for file system connection instance
        config: Dict[str, Union[str, bool, int, float]]
            JuiceFS configuration, available keys: https://github.com/juicedata/juicefs/blob/main/sdk/java/libjfs/main.go#L215
        """
        if self._cached:
            return
        AbstractFileSystem.__init__(self, **kwargs)
        self._fsid = f'juicefs_{name}'
        self.pars = (name, config)
        self.session = JuiceFS(name, config)

    def _open(
            self,
            path,
            mode="rb",
            block_size=None,
            autocommit=True,
            cache_options=None,
            **kwargs
    ):
        return JuiceFSFile(self, path, mode)

    @property
    def fsid(self):
        return self._fsid

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        return ops["path"]

    def mkdir(self, path, **kwargs):
        self.session.mkdir(path)

    def makedirs(self, path, exist_ok=False):
        self.session.makedirs(path, exist_ok=exist_ok)

    def rmdir(self, path):
        self.session.rmdir(path)

    @staticmethod
    def _retrive_entry_type(e: DirEntry):
        if e.is_file():
            return 'file'
        elif e.is_dir():
            return 'directory'
        elif e.is_symlink():
            return 'symlink'
        return 'unknown'

    def ls(self, path, detail=True, **kwargs):
        if detail:
            out = self.session.scandir(path)
            out = list({'name': e.path, 'size': e.stat().st_size, 'type': JuiceFileSystem._retrive_entry_type(e)} for e in out)
            return out
        out = self.session.listdir(path)
        out = [self._strip_protocol(p) for p in out]
        return out

    def exists(self, path, **kwargs):
        return self.session.path.exists(path)

    def lexists(self, path, **kwargs):
        return self.session.path.lexists(path)

    def size(self, path):
        return self.session.path.getsize(path)

    def isdir(self, path):
        return self.session.path.isdir(path)

    def isfile(self, path):
        return self.session.path.isfile(path)

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        self.session.rename(path1, path2)

    def _rm(self, path):
        self.session.remove(path)

    def created(self, path):
        stat = self.session.lstat(path)
        return datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc)

    def modified(self, path):
        mtime = self.session.path.getmtime(path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc)

    def __reduce_ex__(self, protocol):
        return JuiceFileSystem, self.pars


def _convert_mode(mode: str):
    if mode:
        if 'r' in mode:
            return os.O_RDONLY
        elif 'w' in mode:
            return os.O_WRONLY
        elif 'a' in mode:
            return os.O_APPEND
    return 0


class JuiceFSFile(AbstractBufferedFile):
    def __init__(
            self,
            fs: JuiceFileSystem,
            path: str,
            mode: str,
            permission: int = DEFAULT_FILE_MODE,
            **kwargs
    ):
        """

        Args:
            fs:
            path:
            mode: os.RDONLY, os.WRONLY, os.RDWR, os.APPEND
            permission: 0o777 is ugo+rwx
            **kwargs:
        """
        AbstractBufferedFile.__init__(self, fs, path, mode, **kwargs)
        self.fs = fs
        self.path = path
        self.flags = MODE_MASK_R
        self.flags = self.flags | _convert_mode(mode)
        self.permission = permission
        self.fio = io.open(fs.session, path, mode)

    def _fetch_range(self, start, end):
        fd = self.fs.session.open(self.path, self.flags, self.permission)
        self.fs.session.lseek(fd, start, os.SEEK_SET)
        b = self.fs.session.read(fd, end)
        self.fs.session.close(fd)
        return b

    def tell(self):
        loc = self.fio.tell()
        self.loc = loc
        return loc

    def seek(self, loc, whence=0):
        """
        Set the current position of file descriptor fd to position loc, modified by whence

        Args:
            loc: position in byte
            whence: os.SEEK_SET or 0 to set the position relative to the beginning of the file;
             os.SEEK_CUR or 1 to set it relative to the current position;
             os.SEEK_END or 2 to set it relative to the end of the file.

        Returns:
            the new cursor position in bytes, starting from the beginning.
        """
        nloc = self.fio.seek(loc, whence)
        self.loc = nloc
        return self.loc

    def write(self, data):
        return self.fio.write(data)

    def flush(self, force=False):
        self.fio.flush()

    def read(self, length=-1):
        return self.fio.read(length)

    def readinto(self, b):
        return self.fio.readinto(b)

    def close(self):
        super().close()
        self.fio.close()

    def readable(self):
        return self.fio.readable()

    def seekable(self):
        return self.fio.seekable()

    def writable(self):
        return self.fio.writable()
