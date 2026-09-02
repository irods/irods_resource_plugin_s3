import glob
import os
import random
import time

def get_leaked_s3_shared_memory_files():
    return glob.glob('/dev/shm/*s3*')

# The server-side removal of a shmem segment happens shortly after the icommand/rule call that
# triggered it returns, not synchronously with it, so a single immediate glob check can observe a
# leftover file that disappears moments later. Poll a few times before failing to avoid that false
# positive.
def assert_no_leaked_s3_shared_memory(test_case, retries=5, delay_seconds=1):
    for attempt in range(retries):
        leaked_s3_shmem_files = get_leaked_s3_shared_memory_files()
        if not leaked_s3_shmem_files:
            return
        if attempt < retries - 1:
            time.sleep(delay_seconds)

    test_case.assertEqual(leaked_s3_shmem_files, [],
            'S3 shared memory not cleaned up after test: {}'.format(leaked_s3_shmem_files))

def remove_if_exists(localfile):
    if os.path.exists(localfile):
        os.unlink(localfile)

def make_arbitrary_file(f_name, f_size, buffer_size=32*1024*1024):
    # do not care about true randomness
    random.seed(5)
    bytes_written = 0
    buffer = buffer_size * [0x78]       # 'x' - bytearray() below appears to require int instead
                                        #       of char which was valid in python2
    with open(f_name, "wb") as out:

        while bytes_written < f_size:

            if f_size - bytes_written < buffer_size:
                to_write = f_size - bytes_written
                buffer = to_write * [0x78]  # 'x'
            else:
                to_write = buffer_size

            current_char = random.randrange(256)

            # just write some random byte each 1024 chars
            for i in range(0, to_write, 1024):
                buffer[i] = current_char
                current_char = random.randrange(256)
            buffer[len(buffer)-1] = random.randrange(256)

            out.write(bytearray(buffer))

            bytes_written += to_write
