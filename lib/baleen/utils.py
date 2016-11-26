import logging
log = logging.getLogger(__name__)

from os.path import isdir, isfile, splitext, basename, dirname, join
from shutil import rmtree
from os import makedirs, remove
from glob import glob
from path import Path

from lxml.etree import fromstring, tostring


def remove_any(*files):
    """
    remove any file or directory - no questions asked
    """
    for f in files:
        if isfile(f):
            remove(f)
            log.info("removed file {!r}".format(f))
        elif isdir(f):
            rmtree(f)
            log.info("removed directory {!r}".format(f))
            

def make_dir(path):
    """
    make dir if it does not exists
    """
    if path and not isdir(path):
        log.info("creating dir {!r}".format(path))
        makedirs(path)
        
        
def file_name(path, max_ext_size=4, strip_ext=[]):
    """
    Return file's basename (i.e. without directory) and stripped of all
    extensions, optionally limited in size and/or to those in the list strip_ext
    """
    # DEPRECATED! - use derive_path() instead
    name = basename(path)
    parts = name.split(".")
    
    while parts:
        if strip_ext and parts[-1] not in strip_ext:
            break
        if len(parts[-1]) > max_ext_size:
            break
        parts.pop()
        
    return ".".join(parts)


def new_name(fname, new_dir=None, new_ext=None,
             max_ext_size=4, strip_ext=[]):
    """
    E.g. new_name('/dir1/dir2/file.ext1.ext2', '/dir_3', '.ext3') returns
    '/dir3/file.ext3'
    """
    # DEPRECATED! - use derive_path() instead
    if new_ext:
        new_name = file_name(fname, max_ext_size, strip_ext) + new_ext
    else:
        new_name = basename(fname)
        
    return join(new_dir or dirname(fname), new_name)


def file_list(files, file_glob="*"):
    if isinstance(files, str):
        if isdir(files):
            files = join(files, file_glob)
        files = glob(files)
        
    return files


def strip_xml(s):
    """
    strip all xml tags 
    """
    return tostring(fromstring("<x>" + s + "</x>"), method="text", encoding=str)


def copy_doc(from_func, to_func, first_line_only=True):
    """
    copy doc string from another function
    """
    if first_line_only:
        to_func.__doc__ = from_func.__doc__.strip().split("\n")[0] 
    else:
        to_func.__doc__ = from_func.__doc__



# TODO 3: possible bug is that second part of DOI is regarded as tag; use is_doi flag?
def derive_path(path, new_dir=None, new_corename=None, new_ext=None,
                remove_tags=[], append_tags=[]):
    """
    Derive a new path from an old path

    Parameters
    ----------
    path: Path or str
        old path
    new_dir: Path or str
        new directory
    new_corename: Path or str
        new corename, which is the basename without the tags
    new_ext: str
        new extension
    remove_tags: list of str
        tags to be removed
    append_tags: list of str
        tags to be appended

    Returns
    -------
    path: Path
        new path

    """
    # in case it is a string
    old_path = Path(path)

    # bug: splitpath return str instead of Path
    old_dir, old_path = old_path.splitpath()

    # NB None is different from empty string!
    if new_dir is None:
        new_dir = old_dir
    else:
        new_dir = Path(new_dir)

    old_path, old_ext = Path(old_path).splitext()

    if new_ext is None:
        new_ext = old_ext
    elif new_ext != '' and not new_ext.startswith('.'):
        new_ext = '.' + new_ext

    old_corename, *old_tags = old_path.split('#')

    if new_corename is None:
        new_corename = old_corename

    new_tags = [tag for tag in old_tags if tag not in remove_tags]
    new_tags += append_tags

    if new_tags:
        new_tags = '#' + '#'.join(new_tags)
    else:
        new_tags = ''

    return new_dir / new_corename + new_tags + new_ext


def get_doi(path, sep='/'):
    """
    Get DOI from a file path

    Parameters
    ----------
    path: Path or str

    Returns
    -------
    DOI: str
    """
    # namebase will remove extension in cases like 10.1038#16898.txt
    return sep.join(Path(path).namebase.split('#')[:2])
