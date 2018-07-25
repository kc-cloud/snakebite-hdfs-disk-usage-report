from snakebite.client import Client
import argparse
import csv
import time

SIZE_THRESHOLD = 20
LEVELS = 2
#folders_of_interest = {}
header_list = ['Folder name', 'Folder Size', 'Folder Size (replicated)']
csv_file = None
csv_file_writer = None

def convert2TB(byte_size):
    """ Converts bytes to TB"""
    return round(float(byte_size)/(1024 ** 4), 2)

def write2file(row):
    """ Writes a row to CSV File"""
    csv_file_writer.writerow(row)
    time.sleep(1)
    csv_file.flush()

def traverse(parent):
    """ Travers the HDFS folders and collects their disk space usage"""
    children = hdfs.ls([parent])
    for child in children:
        if child['file_type'] == 'f':
            continue
        size_info = list(hdfs.count([child['path']]))[0]
        size = size_info['length']
        replicated_size = size_info['spaceConsumed']
        #folders_of_interest[child['path']] = {'size': size, 'replicated_size': replicated_size}
        if size_info['fileCount'] > 0:
            write2file([child['path'], size, replicated_size])
        if size_info['fileCount'] == 0 or size_info['directoryCount'] == 0 or child['path'].count('/') == LEVELS:
            continue
        else:
            traverse([child['path'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This script is used for collecting HDFS disk usage statistics')
    parser.add_argument('--file', help='Output File to write report', required=True)
    parser.add_argument('--size-limit', help='Max folder size in TB to process', required=False)
    parser.add_argument('--levels', help='how many levels to go?', required=False)
    args = parser.parse_args()

    if args.size_limit:
        SIZE_THRESHOLD = int(args.size_limit)
    if args.levels:
        LEVELS = int(args.levels)

    csv_file = open(args.file, "w")
    csv_file_writer = csv.writer(csv_file)
    try:
        write2file(header_list)
        hdfs = Client("namenode.example.com", 6000, use_trash=False)
        hdfs_root_folders = hdfs.ls(['/'])
        hdfs_root_folder_names = [folder['path'] for folder in hdfs_root_folders]
        for folder in hdfs_root_folder_names:
            size_info = list(hdfs.count([folder]))[0]
            size = size_info['length']
            replicated_size = size_info['spaceConsumed']
            replicated_size_in_TB = convert2TB(replicated_size)
            if replicated_size_in_TB > SIZE_THRESHOLD:
                #folders_of_interest[folder] = {'size': size, 'replicated_size': replicated_size}
                write2file([folder, size, replicated_size])
                traverse(folder)
    finally:
        csv_file.close()
