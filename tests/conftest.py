import os
import shutil
import sys
import pytest

import juicefs
from juicefs import JuiceFS

from pyjuicefs import JuiceFileSystem

NAME = "test-jfs"
BUCKET = "/tmp"
META = "/tmp/test-jfs.db"
META_URL = "sqlite3:///tmp/test-jfs.db"


# each test runs on cwd to its temp dir
@pytest.fixture(autouse=True)
def go_to_tmpdir(request):
    # Get the fixture dynamically by its name.
    tmpdir = request.getfixturevalue("tmpdir")
    # ensure local test created packages can be imported
    sys.path.insert(0, str(tmpdir))
    # Chdir only for the duration of the test.
    with tmpdir.as_cwd():
        yield


@pytest.fixture(scope="session")
def jfs():
    return JuiceFS(NAME, {"meta": META_URL})


@pytest.fixture(scope='session')
def jfs_specfs():
    return JuiceFileSystem(NAME, {"meta": META_URL})


def format_juicefs():
    if os.path.exists(META):
        os.unlink(META)
    if os.path.exists(os.path.join(BUCKET, NAME)):
        shutil.rmtree(os.path.join(BUCKET, NAME))

    juicefs_binary = os.path.normpath(
        os.path.join(juicefs.__file__, "..", "lib", "juicefs")
    )

    commands = [
        juicefs_binary,
        "format",
        "--bucket=%s" % BUCKET,
        "--force",
        META_URL,
        NAME,
    ]

    command = " ".join(commands)
    print("run command:", command)
    os.system(command)


format_juicefs()
