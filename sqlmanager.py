import sqlite3
import llfuse
import os
import errno
import stat
from time import time
from llfuse import FUSEError
import sys

# For Python 2 + 3 compatibility
if sys.version_info[0] == 2:
    def next(it):
        return it.next()
else:
    buffer = memoryview

class NoUniqueValueError(Exception):
    def __str__(self):
        return 'Query generated more than 1 result row'


class NoSuchRowError(Exception):
    def __str__(self):
        return 'Query produced 0 result rows'

class SQLfs_Manager:
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self.init_tables()

    def init_tables(self):
        '''Initialize file system tables'''

        self.cursor.execute("""
        CREATE TABLE inodes (
            id        INTEGER PRIMARY KEY,
            uid       INT NOT NULL,
            gid       INT NOT NULL,
            mode      INT NOT NULL,
            mtime_ns  INT NOT NULL,
            atime_ns  INT NOT NULL,
            ctime_ns  INT NOT NULL,
            target    BLOB(256) ,
            size      INT NOT NULL DEFAULT 0,
            rdev      INT NOT NULL DEFAULT 0,
            data      BLOB
        )
        """)

        self.cursor.execute("""
        CREATE TABLE contents (
            rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
            name      BLOB(256) NOT NULL,
            inode     INT NOT NULL REFERENCES inodes(id),
            parent_inode INT NOT NULL REFERENCES inodes(id),

            UNIQUE (name, parent_inode)
        )""")

        # Insert root directory
        now_ns = int(time() * 1e9)
        self.cursor.execute("INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns) "
                            "VALUES (?,?,?,?,?,?,?)",
                            (llfuse.ROOT_INODE, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR
                              | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH
                              | stat.S_IXOTH, os.getuid(), os.getgid(), now_ns, now_ns, now_ns))
        self.cursor.execute("INSERT INTO contents (name, parent_inode, inode) VALUES (?,?,?)",
                            (b'..', llfuse.ROOT_INODE, llfuse.ROOT_INODE))


    def get_row(self, *a, **kw):
        self.cursor.execute(*a, **kw)
        try:
            row = next(self.cursor)
        except StopIteration:
            raise NoSuchRowError()
        try:
            next(self.cursor)
        except StopIteration:
            pass
        else:
            raise NoUniqueValueError()

        return row

    def lookup(self, inode_p, name):
        if name == '.':
            inode = inode_p
        elif name == '..':
            inode = self.get_row("SELECT * FROM contents WHERE inode=?",
                                 (inode_p,))['parent_inode']
        else:
            try:
                inode = self.get_row("SELECT * FROM contents WHERE name=? AND parent_inode=?",
                                     (name, inode_p))['inode']
            except NoSuchRowError:
                raise(llfuse.FUSEError(errno.ENOENT))
        return inode

    def get_contents_list(self, inode, off):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM contents WHERE parent_inode=? "
                        'AND rowid > ? ORDER BY rowid', (inode, off))
        return cursor

    def delete_contents(self, name, inode_p):
        self.cursor.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (name, inode_p))

    def delete_inodes(self, inode):
        self.cursor.execute("DELETE FROM inodes WHERE id=?", (inode,))

    def rename(self, name_new, inode_p_new, name_old, inode_p_old):
        self.cursor.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                            "AND parent_inode=?", (name_new, inode_p_new,
                                                   name_old, inode_p_old))

    def replace(self, inode_p_old, name_old, inode_p_new, name_new,
                 entry_old, entry_new):

        if self.get_row("SELECT COUNT(inode) FROM contents WHERE parent_inode=?",
                        (entry_new.st_ino,))[0] > 0:
            raise llfuse.FUSEError(errno.ENOTEMPTY)

        self.cursor.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                            (entry_old.st_ino, name_new, inode_p_new))
        self.db.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                        (name_old, inode_p_old))

        if entry_new.st_nlink == 1 and entry_new.st_ino not in self.inode_open_count:
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (entry_new.st_ino,))

    def _link(self, new_name, inode, new_inode_p):
        self.cursor.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                            (new_name, inode, new_inode_p))

    def _setattr(self, inode, attr):
        if attr.st_size is not None:
            data = self.get_row('SELECT data FROM inodes WHERE id=?', (inode,))[0]
            if data is None:
                data = b''
            if len(data) < attr.st_size:
                data = data + b'\0' * (attr.st_size - len(data))
            else:
                data = data[:attr.st_size]
            self.cursor.execute('UPDATE inodes SET data=?, size=? WHERE id=?',
                                (buffer(data), attr.st_size, inode))
        if attr.st_mode is not None:
            self.cursor.execute('UPDATE inodes SET mode=? WHERE id=?',
                                (attr.st_mode, inode))

        if attr.st_uid is not None:
            self.cursor.execute('UPDATE inodes SET uid=? WHERE id=?',
                                (attr.st_uid, inode))

        if attr.st_gid is not None:
            self.cursor.execute('UPDATE inodes SET gid=? WHERE id=?',
                                (attr.st_gid, inode))

        if attr.st_rdev is not None:
            self.cursor.execute('UPDATE inodes SET rdev=? WHERE id=?',
                                (attr.st_rdev, inode))

        if attr.st_atime_ns is not None:
            self.cursor.execute('UPDATE inodes SET atime_ns=? WHERE id=?',
                                (attr.st_atime_ns, inode))

        if attr.st_mtime_ns is not None:
            self.cursor.execute('UPDATE inodes SET mtime_ns=? WHERE id=?',
                                (attr.st_mtime_ns, inode))

        if attr.st_ctime_ns is not None:
            self.cursor.execute('UPDATE inodes SET ctime_ns=? WHERE id=?',
                                (attr.st_ctime_ns, inode))

    def _create(self, inode_p, name, ctx, mode, rdev=0, target=None):
        now_ns = int(time() * 1e9)
        self.cursor.execute('INSERT INTO inodes (uid, gid, mode, mtime_ns, atime_ns, '
                            'ctime_ns, target, rdev) VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                            (ctx.uid, ctx.gid, mode, now_ns, now_ns, now_ns, target, rdev))

        inode = self.cursor.lastrowid
        self.db.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                        (name, inode, inode_p))
        return inode

    def _write(self, inode, data):
        self.cursor.execute('UPDATE inodes SET data=?, size=? WHERE id=?',
                            (buffer(data), len(data), inode))

    def _release(self, fh):
        self.cursor.execute("DELETE FROM inodes WHERE id=?", (fh,))

