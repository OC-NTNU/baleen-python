import logging
from os.path import isdir, isfile, basename, dirname, join
from shutil import rmtree
from os import makedirs, remove
from glob import glob
from pathlib import Path
from urllib.parse import quote_plus, unquote_plus

from lxml.etree import fromstring, tostring

log = logging.getLogger(__name__)


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


def derive_path(path, new_dir=None, new_corename=None, new_ext=None,
                remove_tags=(), append_tags=()):
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

    # NB None is different from empty string!
    if new_dir is None:
        new_dir = old_path.parent
    else:
        new_dir = Path(new_dir)

    if new_ext is None:
        new_ext = old_path.suffix
    elif new_ext != '' and not new_ext.startswith('.'):
        new_ext = '.' + new_ext

    old_corename, *old_tags = old_path.stem.split('#')

    if new_corename is None:
        new_corename = old_corename

    new_tags = [tag for tag in old_tags if tag not in remove_tags]
    new_tags += list(append_tags)

    if new_tags:
        new_tags = '#' + '#'.join(new_tags)
    else:
        new_tags = ''

    return new_dir / (new_corename + new_tags + new_ext)


def get_doi(path):
    """
    Get DOI from a file path.  This is the part of the file's basename up to the first '#' char (if any) or the
    file extension otherwise (if any). It is assumed to be quoted using the quote_doi function.

    Parameters
    ----------
    path: Path or str

    Returns
    -------
    DOI: str
    """
    encoded_doi = Path(path).stem.split('#')[0]
    return unquote_plus(encoded_doi)


def quote_doi(unquoted_doi):
    """
    Quote DOI string

    Makes DOI safe for use in filenames by quoting special characters and appropriately encoding non-ASCII text.

    Parameters
    ----------
    unquoted_doi: str

    Returns
    -------
    quoted_doi: str

    """
    return quote_plus(unquoted_doi)


def unquote_doi(quoted_doi):
    """
    Unquote DOI string

    Reverse of quote_doi.

    Parameters
    ----------
    quoted_doi: str

    Returns
    -------
    doi: str

    """
    return unquote_plus(quoted_doi)


def old_to_new_doi(old_path):
    """
    Transform old-style DOI name (i.e. with DOI prefix and suffix separated by '#' char) to
     new-style DOI (i.e. as URL quoted string).

    Parameters
    ----------
    old_path: Path or str

    Returns
    -------
    new_path: Path
    """
    old_path = Path(old_path)
    old_parts = old_path.name.split('#')
    doi = '/'.join(old_parts[:2])
    new_parts = [quote_doi(doi)] + old_parts[2:]
    new_basename = '#'.join(new_parts)
    new_path = old_path.parent / Path(new_basename)
    return new_path


def file_name(path, max_ext_size=4, strip_ext=()):
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
             max_ext_size=4, strip_ext=()):
    """
    E.g. new_name('/dir1/dir2/file.ext1.ext2', '/dir_3', '.ext3') returns
    '/dir3/file.ext3'
    """
    # DEPRECATED! - use derive_path() instead
    if new_ext:
        new_fname = file_name(fname, max_ext_size, strip_ext) + new_ext
    else:
        new_fname = basename(fname)

    return join(new_dir or dirname(fname), new_fname)
