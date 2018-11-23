import rpyc
import sys
import os


'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
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

'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):


    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """
    def __init__(self, config):
        configpath = os.path.realpath(config)
        with open(configpath, 'rb') as f:
            data = f.read().splitlines()
            blockinfo = data.pop(0)
            self.numofblocks = int(blockinfo.split(b':')[1])
            data.pop(0)
            self.block = {}
            for i in range(self.numofblocks):
                self.block[i] = (data[i].split()[1].split(b':')[0].decode('utf-8'),\
                                 int(data[i].split()[1].split(b':')[1]))
                print(self.block[i])
        self.filesinstore = {}
        self.hash_loc = {}


    '''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''


    def exposed_modify_file(self, filename, version, hashlist):
        e = ErrorResponse('err')
        # upload a new file
        if version == 1 and not bool(self.filesinstore.get(filename)):
            # print('create')
            self.filesinstore[filename] = [1]
        # check version num
        else:
            fileversion = self.filesinstore[filename][0]
            if version != (fileversion + 1):
                # print(self.filesinstore)
                e.wrong_version_error(version)
                raise e  # version_err
        # check missing block
        missinghash = []
        if len(self.filesinstore[filename]) >= 1:
            print('exists, hl:', self.filesinstore[filename])
            for h in hashlist:
                if h[0] not in self.filesinstore[filename]:
                    missinghash.append(h)
        # modify hashlist
        self.filesinstore[filename] = [version]
        for hashval in hashlist:
            self.filesinstore[filename].append(hashval[0])
            self.hash_loc[hashval[0]] = hashval[1]
        return missinghash

    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_delete_file(self, filename, version):
        e = ErrorResponse('err')
        fversion = self.filesinstore[filename][0]
        # file found but version err
        # delete file
        if version == (fversion + 1):
            self.filesinstore[filename] = [version]
            # print(self.filesinstore)
            return 'OK'
        # file found but version err
        else:
            e.wrong_version_error(fversion)
            # print(e.error)
            raise e



    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_read_file(self, filename):
        print('read..')
        # print('list', self.filesinstore)
        v = 0
        hl = []
        # no such file in the store
        try:
            self.filesinstore[filename]
        except KeyError:
            return v, []
        # file exists
        # get version
        v = self.filesinstore[filename][0]
        if len(self.filesinstore[filename]) > 1:
            for l in self.filesinstore[filename][1:]:
                hl.append([l, self.hash_loc[l]])
        else:  # file was deleted
            hl = []
        print('file version:', v, 'hl:', hl)
        return v, hl


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = 6000)
    server.start()

