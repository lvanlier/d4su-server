
#
# Use pyarrow from Apache Arrow to read and write files (local and remote (S3, HDFS, etc))
#
from pyarrow import fs

def write_file(file_path, data):
    local = fs.LocalFileSystem()
    with local.open_output_stream(file_path) as stream:
        stream.write(data.encode('utf-8'))
        
def read_file(file_path):
    local = fs.LocalFileSystem()
    with local.open_input_stream(file_path) as stream:
        return stream.readall()

def delete_file(file_path):
    local = fs.LocalFileSystem()
    local.delete_file(file_path)

#
# Zip files and directories
#
import os

def zip_directory(directory, zip_file):
    base_dir = os.path.basename(directory)
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            arcname = os.path.join(base_dir, os.path.relpath(file_path, directory))
            zip_file.write(file_path, arcname)            
