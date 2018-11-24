import rpyc
import hashlib
import os
import sys
import time

"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""
class ErrorResponse(Exception):

    def __init__(self, message):
        super(ErrorResponse, self).__init__(message)
        self.error = message

    def missing_blocks(self, hashlist):
        self.error.append(hashlist)

    def wrong_version_error(self, version):
        self.error = 'Version err:' + str(version)

    def file_not_found(self):
        self.error = 'Not Found'

class SurfStoreClient():
    """
    Initialize the client and set up connections to the block stores and
    metadata store using the config file
    """

    def __init__(self, config):
        self.mode = 'N'
        configpath = os.path.realpath(config)
        if not os.path.isfile(configpath):
            print('Not Found')
        with open(configpath, 'rb') as f:
            try:
                data = f.read().splitlines()
                blockinfo = data.pop(0)
                self.numofblocks = int(blockinfo.split(b':')[1])
                metadata_info = data.pop(0)
                self.metadata_host = metadata_info.split()[1].split(b':')[0].decode('utf-8')
                self.metadata_port = int(metadata_info.split()[1].split(b':')[1])
                self.block = {}
                for i in range(self.numofblocks):
                    self.block[i] = (data[i].split()[1].split(b':')[0].decode('utf-8'),\
                                     int(data[i].split()[1].split(b':')[1]))
            except IndexError:
                self.eprint('ERR! Fault Config')


    def findserver_h(self, h):  # Hash placement
        return int(h, 16) % self.numofblocks

    def findserver_n(self):  # Nearest to client
        rtt = 9999
        result = 0
        for i in range(self.numofblocks):
            stime = time.time()
            conn = rpyc.connect(self.block[i][0], self.block[i][1])
            conn.close()
            etime = time.time()
            tmp = etime - stime
            if tmp < rtt:
                rtt = tmp
                result = i
        print('server-', result)
        return result



    """
    upload(filepath) : Reads the local file, creates a set of 
    hashed blocks and uploads them onto the MetadataStore 
    (and potentially the BlockStore if they were not already present there).
    """

    def upload(self, filepath):
        path = os.path.realpath(filepath)
        filename = filepath.split('/')[-1]
        if not os.path.isfile(path):
            print('Not Found')
            return -1
        # decompose file into blocks
        with open(path, 'rb') as f:
            data = f.read()
            length = len(data)
            fileblocknum = length // 4096 if length % 4096 is 0 else length // 4096 + 1
            filehash = {}  # hashval: blockval
            filehl = []  # [hashval,location]
            blocknum = 0
            if self.mode == 'n':
                blocknum = self.findserver_n()
            for i in range(fileblocknum - 1):
                fileblocks = data[i * 4096:(i + 1) * 4096]
                hashval = hashlib.sha256(fileblocks).hexdigest()
                if self.mode == 'h':
                    blocknum = self.findserver_h(hashval)
                filehl.append([hashval, blocknum])
                filehash[hashval] = fileblocks
            try:
                fileblocks = data[(fileblocknum - 1) * 4096:]
                hashval = hashlib.sha256(fileblocks).hexdigest()
                if self.mode == 'h':
                    blocknum = self.findserver_h(hashval)
                filehl.append([hashval, blocknum])
                filehash[hashval] = fileblocks
            except IndexError:
                pass
        # print('block success! blocks:', filehash, '\nhashloc:', filehl)

        # read file to get version
        conn = rpyc.connect(self.metadata_host, self.metadata_port)
        v, hl = conn.root.exposed_read_file(filename)
        v += 1
        # print('read success! ', v, hl)

        # upload hash
        try:
            missinghash = conn.root.exposed_modify_file(filename, v, filehl)
            # print('missinghash:',missinghash)
            for h in missinghash:
                blocknum = h[1]
                c = rpyc.connect(self.block[blocknum][0], self.block[blocknum][1])
                c.root.exposed_store_block(h[0], filehash[h[0]])
                print(h)
            print('OK')
        except ErrorResponse:  # version err
            # print('woops! version err')
            self.upload(filepath)
        # upload missing hash


    """
    delete(filename) : Signals the MetadataStore to delete a file.
    """

    def delete(self, filename):
        conn = rpyc.connect(self.metadata_host, self.metadata_port)
        v, hl = conn.root.exposed_read_file(filename)
        if v == 0:
            print('Not Found')
        else:
            v += 1
            try:
                result = conn.root.exposed_delete_file(filename, v)
                print(result)
            except ErrorResponse:
                self.delete(filename)

    """
    download(filename, dst) : Downloads a file (f) from SurfStore and saves
    it to (dst) folder. Ensures not to download unnecessary blocks.
    """

    def download(self, filename, location):
        # check local file
        if location[-1] != '/':
            location += '/'
        localpath = os.path.realpath(location + filename)
        filehash = {}
        if os.path.isfile(localpath):
            # print('file exist:', localpath)
            with open(localpath, 'rb') as f:
                data = f.read()
                length = len(data)
                fileblocknum = length // 4096 if length % 4096 is 0 else length // 4096 + 1
                for i in range(fileblocknum - 1):
                    fileblocks = data[i * 4096:(i + 1) * 4096]
                    hashval = hashlib.sha256(fileblocks).hexdigest()
                    filehash[hashval] = fileblocks
                try:
                    fileblocks = data[(fileblocknum - 1) * 4096:]
                    hashval = hashlib.sha256(fileblocks).hexdigest()
                    filehash[hashval] = fileblocks
                except IndexError:
                    pass
        # read store to get hashlist
        # print('filehash:', filehash)
        targethash = []
        conn = rpyc.connect(self.metadata_host, self.metadata_port)
        v, hl = conn.root.exposed_read_file(filename)
        if not hl:
            raise Exception('Not Found')
        else:
            for b in hl:
                if not filehash.get(b[0]):
                    targethash.append(b)
        # print('tar:', targethash)

        # connect to the block and get missing blocks
        hashfromblock = {}
        for h in targethash:
            blocknum = h[1]
            c = rpyc.connect(self.block[blocknum][0], self.block[blocknum][1])
            block = c.root.exposed_get_block(h[0])
            hashfromblock[h[0]] = block
        fresult = b''
        for l in hl:
            if l[0] in filehash:
                fresult += filehash[l[0]]
            else:
                fresult += hashfromblock[l[0]]
        # print('write:', fresult)
        # fresult = fresult.decode()
        # write the file
        result = open(localpath, 'wb')
        result.write(fresult)
        result.close()
        print('OK')


    """
    Use eprint to print debug messages to stderr
    E.g - 
    self.eprint("This is a debug message")
    """

    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)


if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1])
    client.mode = sys.argv[2]
    operation = sys.argv[3]
    if operation == 'upload':
        client.upload(sys.argv[4])
    elif operation == 'download':
        client.download(sys.argv[4], sys.argv[5])
    elif operation == 'delete':
        client.delete(sys.argv[4])
    else:
        print("Invalid operation")
