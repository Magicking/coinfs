from __future__ import division, print_function, absolute_import

import os
import sys

# We are running from the Python-LLFUSE source directory, put it
# into the Python path.
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 'llfuse'))):
    sys.path.append(os.path.join(basedir, 'src'))

import llfuse
import errno
import stat
from time import time
import logging
from collections import defaultdict
from llfuse import FUSEError
from argparse import ArgumentParser

from sqlmanager import SQLfs_Manager

log = logging.getLogger()


class Operations(llfuse.Operations):
    '''An example filesystem that stores all data in memory

    This is a very simple implementation with terrible performance.
    Don't try to store significant amounts of data. Also, there are
    some other flaws that have not been fixed to keep the code easier
    to understand:

    * atime, mtime and ctime are not updated
    * generation numbers are not supported
    '''


    def __init__(self):
        super(Operations, self).__init__()
        self.inode_open_count = defaultdict(int)
        self.cm = SQLfs_Manager()

    def lookup(self, inode_p, name):
        inode = self.cm.lookup(inode_p, name)
        return self.getattr(inode)

    def getattr(self, inode):
        row = self.cm.get_row('SELECT * FROM inodes WHERE id=?', (inode,))

        entry = llfuse.EntryAttributes()
        entry.st_ino = inode
        entry.generation = 0
        entry.entry_timeout = 300
        entry.attr_timeout = 300
        entry.st_mode = row['mode']
        entry.st_nlink = self.cm.get_row("SELECT COUNT(inode) FROM contents WHERE inode=?",
                                     (inode,))[0]
        entry.st_uid = row['uid']
        entry.st_gid = row['gid']
        entry.st_rdev = row['rdev']
        entry.st_size = row['size']

        entry.st_blksize = 512
        entry.st_blocks = 1
        entry.st_atime_ns = row['atime_ns']
        entry.st_mtime_ns = row['mtime_ns']
        entry.st_ctime_ns = row['ctime_ns']

        return entry

    def readlink(self, inode):
        return self.cm.get_row('SELECT * FROM inodes WHERE id=?', (inode,))['target']

    def opendir(self, inode):
        return inode

    def readdir(self, inode, off):
        if off == 0:
            off = -1

        cursor = self.cm.get_contents_list(inode, off)

        for row in cursor:
            yield (row['name'], self.getattr(row['inode']), row['rowid'])

    def unlink(self, inode_p, name):
        entry = self.lookup(inode_p, name)

        if stat.S_ISDIR(entry.st_mode):
            raise llfuse.FUSEError(errno.EISDIR)

        self._remove(inode_p, name, entry)

    def rmdir(self, inode_p, name):
        entry = self.lookup(inode_p, name)

        if not stat.S_ISDIR(entry.st_mode):
            raise llfuse.FUSEError(errno.ENOTDIR)

        self._remove(inode_p, name, entry)

    def _remove(self, inode_p, name, entry):
        if self.cm.get_row("SELECT COUNT(inode) FROM contents WHERE parent_inode=?",
                        (entry.st_ino,))[0] > 0:
            raise llfuse.FUSEError(errno.ENOTEMPTY)

        self.cm.delete_contents(name, inode_p)

        if entry.st_nlink == 1 and entry.st_ino not in self.inode_open_count:
            self.cm.delete_inodes(entry.st_ino)

    def symlink(self, inode_p, name, target, ctx):
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        return self._create(inode_p, name, mode, ctx, target=target)

    def rename(self, inode_p_old, name_old, inode_p_new, name_new):
        entry_old = self.lookup(inode_p_old, name_old)

        try:
            entry_new = self.lookup(inode_p_new, name_new)
        except llfuse.FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
            target_exists = False
        else:
            target_exists = True

        if target_exists:
            self.cm.replace(inode_p_old, name_old, inode_p_new, name_new,
                          entry_old, entry_new)
        else:
            self.cm.rename(self, name_new, inode_p_new, name_old, inode_p_old)


    def link(self, inode, new_inode_p, new_name):
        entry_p = self.getattr(new_inode_p)
        if entry_p.st_nlink == 0:
            log.warn('Attempted to create entry %s with unlinked parent %d',
                     new_name, new_inode_p)
            raise FUSEError(errno.EINVAL)

        self.cm._link(new_name, inode, new_inode_p)

        return self.getattr(inode)

    def setattr(self, inode, attr):
        self.cm._setattr(inode, attr)
        return self.getattr(inode)

    def mknod(self, inode_p, name, mode, rdev, ctx):
        return self._create(inode_p, name, mode, ctx, rdev=rdev)

    def mkdir(self, inode_p, name, mode, ctx):
        return self._create(inode_p, name, mode, ctx)

    def statfs(self):
        stat_ = llfuse.StatvfsData()

        stat_.f_bsize = 512
        stat_.f_frsize = 512

        size = self.cm.get_row('SELECT SUM(size) FROM inodes')[0]
        stat_.f_blocks = size // stat_.f_frsize
        stat_.f_bfree = max(size // stat_.f_frsize, 1024)
        stat_.f_bavail = stat_.f_bfree

        inodes = self.cm.get_row('SELECT COUNT(id) FROM inodes')[0]
        stat_.f_files = inodes
        stat_.f_ffree = max(inodes , 100)
        stat_.f_favail = stat_.f_ffree

        return stat_

    def open(self, inode, flags):
        # Yeah, unused arguments
        #pylint: disable=W0613
        self.inode_open_count[inode] += 1

        # Use inodes as a file handles
        return inode

    def access(self, inode, mode, ctx):
        # Yeah, could be a function and has unused arguments
        #pylint: disable=R0201,W0613
        return True

    def create(self, inode_parent, name, mode, flags, ctx):
        #pylint: disable=W0612
        entry = self._create(inode_parent, name, mode, ctx)
        self.inode_open_count[entry.st_ino] += 1
        return (entry.st_ino, entry)

    def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        if self.getattr(inode_p).st_nlink == 0:
            log.warn('Attempted to create entry %s with unlinked parent %d',
                     name, inode_p)
            raise FUSEError(errno.EINVAL)

        now_ns = int(time() * 1e9)
        self.cursor.execute('INSERT INTO inodes (uid, gid, mode, mtime_ns, atime_ns, '
                            'ctime_ns, target, rdev) VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                            (ctx.uid, ctx.gid, mode, now_ns, now_ns, now_ns, target, rdev))

        inode = self.cursor.lastrowid
        self.db.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                        (name, inode, inode_p))
        return self.getattr(inode)

    def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        if self.getattr(inode_p).st_nlink == 0:
            log.warn('Attempted to create entry %s with unlinked parent %d',
                     name, inode_p)
            raise FUSEError(errno.EINVAL)

        inode = self.cm._create(inode_p, name, ctx, mode, rdev, target)
        return self.getattr(inode)

    def read(self, fh, offset, length):
        data = self.cm.get_row('SELECT data FROM inodes WHERE id=?', (fh,))[0]
        if data is None:
            data = b''
        return data[offset:offset+length]

    def write(self, fh, offset, buf):
        data = self.cm.get_row('SELECT data FROM inodes WHERE id=?', (fh,))[0]
        if data is None:
            data = b''
        data = data[:offset] + buf + data[offset+len(buf):]

        self.cm._write(fh, data)

        return len(buf)

    def release(self, fh):
        self.inode_open_count[fh] -= 1

        if self.inode_open_count[fh] == 0:
            del self.inode_open_count[fh]
            if self.getattr(fh).st_nlink == 0:
                self.cm._release(fh)

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')

    return parser.parse_args()


if __name__ == '__main__':

    options = parse_args()
    init_logging(options.debug)
    operations = Operations()

    llfuse.init(operations, options.mountpoint,
                [  'fsname=tmpfs', "nonempty" ])

    # sqlite3 does not support multithreading
    try:
        llfuse.main(single=True)
    except:
        llfuse.close(unmount=False)
        raise

    llfuse.close()
