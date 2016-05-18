import argparse
import gzip
import io
import logging
import ntpath
import os
import os.path
import struct

log = logging.getLogger('unrgz')


def uncompress_rgz(path, dst_path):
    rgz_file = gzip.open(path)
    lexer = Lexer(rgz_file)
    fs = FileSystem(dst_path)

    token = lexer.next()
    while not token.is_end:
        if token.is_dir:
            if not fs.dir_exists(token.dir_name):
                log.info('Create dir: %s', token.dir_name)
                fs.create_dir(token.dir_name)
        elif token.is_file:
            log.info('Create file: %s', token.file_name)
            fs.create_file(token.file_name, token.file_chunks)
        token = lexer.next()
    log.info('Done')


class Lexer(object):
    def __init__(self, file):
        self._file = file

    def next(self):
        entry_type = self._file.read(1)

        if entry_type == 'd':
            token = self._read_dir()
        elif entry_type == 'f':
            token = self._read_file()
        elif entry_type == 'e':
            token = self._read_end()
        else:
            e = UnknownTokenError()
            e.entry_type = entry_type
            e.pos = self._file.tell() - 1
            raise e
        return token

    def _read_dir(self):
        dir_name = self._read_string()
        return DirToken(dir_name)

    def _read_file(self):
        file_name = self._read_string()
        length, = struct.unpack('<L', self._file.read(4))
        return FileToken(file_name, self._file, length)

    def _read_end(self):
        return EndToken()

    def _read_string(self):
        length, = struct.unpack('<B', self._file.read(1))
        _bytes = self._file.read(length)
        if _bytes[-1] == '\0':
            _bytes = _bytes[:-1]
        string = b''.join(_bytes).decode()
        return string


class UnknownTokenError(Exception):
    def __init__(self, *args):
        super(UnknownTokenError, self).__init__(*args)
        self.entry_type = None
        self.pos = pos


class Token(object):
    is_dir = False
    is_end = False
    if_file = False


class DirToken(Token):
    is_dir = True

    def __init__(self, dir_name):
        self.dir_name = dir_name


class EndToken(Token):
    is_end = True


class FileToken(Token):
    is_file = True
    CHUNK_LENGTH = 1024 ** 2

    def __init__(self, file_name, file, length):
        self.file_name = file_name
        self._file = file
        self._length = length

    @property
    def file_chunks(self):
        i = 0
        while i < self._length:
            start = i
            i = start + self.CHUNK_LENGTH
            if i > self._length:
                i = self._length
            yield self._file.read(i - start)


class FileSystem(object):
    def __init__(self, root_path):
        self._root_path = os.path.abspath(root_path)

    def dir_exists(self, dir_name):
        dir_name = self._adopt_path(dir_name)
        return os.path.exists(dir_name)

    def create_dir(self, dir_name):
        dir_name = self._adopt_path(dir_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

    def create_file(self, file_name, file):
        file_name = self._adopt_path(file_name)
        dir_name = os.path.dirname(file_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with io.open(file_name, 'wb') as dst_file:
            for chunk in file:
                dst_file.write(chunk)

    def _adopt_path(self, path):
        path = path.replace(ntpath.sep, os.path.sep)
        path = os.path.abspath(os.path.join(self._root_path, path))

        assert os.path.commonprefix([self._root_path, path]) == self._root_path, \
                'Access out of root directory is forbidden: %s' % path

        return path


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description='Unpack Ragnarok Online RGZ files',
    )
    parser.add_argument('filename')
    parser.add_argument('--dest-dir', '-d', default='.', dest='dest_dir')
    args = parser.parse_args()

    uncompress_rgz(args.filename, args.dest_dir)
