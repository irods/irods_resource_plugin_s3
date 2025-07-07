#!/usr/bin/env python3

# Upload a file via PRC

import sys

# To remove the potential name conflict with the irods package in scripts.A
directory_to_remove = '/var/lib/irods/scripts/irods/test'
if directory_to_remove in sys.path:
    sys.path.remove(directory_to_remove)

from irods.session import iRODSSession

if __name__ == "__main__":

    # just allow this to throw an exception if the arguments are not provided
    user = sys.argv[1]
    password = sys.argv[2]
    zone = sys.argv[3]
    file = sys.argv[4]
    file_logical_path = sys.argv[5]

    # put the file via PRC
    with iRODSSession(host='localhost', port=1247, user=user, password=password, zone=zone) as session:
        session.data_objects.put(file, file_logical_path)
