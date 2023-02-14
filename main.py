import argparse
from socket import *
import os
import struct
from os.path import join
from multiprocessing import Process
import multiprocessing as mp
from tqdm import tqdm
import json
import math
from threading import Thread


# ################################################utility###############################################################
def _argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ip', action='store', required=True, dest="ip",
                        help='The ip address of two another machines')
    parser.add_argument('--encryption', action='store', dest='choice',
                        choices=['yes'], help='switch on the encryption function')
    return parser.parse_args()


# ################################################protocal_pack#########################################################
# code = 0
def require_get_time():
    operation_code = 0
    header = struct.pack('!I', operation_code)
    length = len(header)
    return struct.pack('!I', length) + header


# code = 1
def return_getfile_time(get_time):
    operation_code = 1
    header = struct.pack('!Id', operation_code, get_time)
    length = len(header)
    return struct.pack('!I', length) + header


# code = 2
def send_file_information(filepath):
    operation_code = 2
    filesize = get_file_size(filepath)
    total_block_number = math.ceil(filesize / block_size)
    header = struct.pack('!I', operation_code) + struct.pack('!QQ', filesize, total_block_number) \
             + filepath.encode()
    header_len = len(header)
    return struct.pack('!I', header_len) + header


# code = 3
def client_get_file_block(filename, index):
    operation_code = 3
    header = struct.pack('!IQ', operation_code, index)
    header_length = len(header + filename.encode())
    return struct.pack('!I', header_length) + header + filename.encode()


# code = 4
def make_file_block(filename, index):
    path = join(file_dir, filename)
    file_block = get_file_block(index, path)
    operation_code = 4
    header = struct.pack('!I', operation_code)
    length = len(header + file_block)
    return struct.pack('!I', length) + header + file_block


# ######### utility ##########
def get_file_block(block_index, path):
    f = open(path, 'rb')
    f.seek(block_index * block_size)
    file_block = f.read(block_size)
    f.close()
    return file_block


def get_file_size(filepath):
    return os.path.getsize(join(file_dir, filepath))


def tcp_receiver(conn):
    msg = conn.recv(4)
    if len(msg) != 0:
        length = struct.unpack('!I', msg)[0]
        buf = b''
        while len(buf) < length:
            buf += conn.recv(length)
        operation_code = struct.unpack('!I', buf[:4])[0]
        content_b = buf[4:]
        return operation_code, content_b
    else:
        return -1, -1


########################################check_new_file_and_notify#####################################################
def file_checker(get_file_time, checker_state):
    f = open('new_file.log', 'w')
    f.close()
    mtime = os.path.getmtime(file_dir)
    print('check and notify')
    while checker_state.value == 1:
        mod_time = os.path.getmtime(file_dir)
        if (mod_time > mtime) & (mod_time > get_file_time.value):
            f_list = scan_file(file_dir)
            append_new_file_log(f_list)
            mtime = mod_time
            # notifier_state = True
            # tcp_connect_notify(ip, mtime, notifier_state, notify_socket)


def append_new_file_log(f_list):
    f = open('new_file.log', 'a')
    for file_name in f_list:
        file_dict = dict()
        file_dict['filename'] = file_name
        file_dict['need_transfer'] = True
        j = json.dumps(file_dict)
        f.write(j)
        f.write('\n')
    f.close()


def update_new_file_log():
    f = open('new_file.log', 'r+')
    position = 0
    while True:
        content = f.readline()
        position += len(content)
        dict = json.loads(content)
        if dict['need_transfer'] == True:
            dict['need_transfer'] = False
            j = json.dumps(dict)
            f.seek(position-len(content))
            f.write(j)
            f.close()
            break


def read_new_file_log():
    f = open('new_file.log', 'r')
    while True:
        j = f.readline()
        if j.strip() != '':
            dict = json.loads(j)
            if dict['need_transfer']:
                f.close()
                return dict['filename']
        else:
            f.close()
            return -1


# ########################################notifier####################################################################
def tcp_notifier(ip):
    notify_socket = socket(AF_INET, SOCK_STREAM)
    notify_socket.bind(('', 26000))
    while True:
        connINT = notify_socket.connect_ex((ip, 25000))
        if connINT == 0:
            print("notifier connect to", ip, "successfully.")
            while True:
                filename = read_new_file_log()
                if filename != -1:
                    notify_socket.send(send_file_information(filename))
                    update_new_file_log()
                    print('file information sent')

# def tcp_connect_notify(ip, last_mod_time, state, notify_socket):
#     while state:
#         connINT = notify_socket.connect_ex((ip, 25000))
#         if connINT == 0:
#             print("notifier connect to", ip, "successfully.")
#             notify_socket.send(require_get_time())
#             time_msg = notify_socket.recv(1024)
#             get_time = parse_time(time_msg)
#             if (last_mod_time > get_time) & (get_time >= 0):
#                 file_list = scan_file(file_dir, get_time)
#                 print(file_list)
#                 for filename in file_list:
#                     notify_socket.send(send_file_information(filename))
#                     print('file information sent')
#                 notify_socket.close()
#                 state = False
#             else:
#                 print('No new files need to be transferred. '
#                       'Or Error happens when transfer get_file_time')


def parse_time(msg):
    blength = msg[0:4]
    length = struct.unpack('!I', blength)[0]
    bheader = msg[4:4 + length]
    operation_code = struct.unpack('!I', bheader[:4])[0]

    if operation_code == 1:
        print('receive code 1')
        gettime = struct.unpack('!d', bheader[4:])[0]
    else:
        gettime = -1

    return gettime


def scan_file(path):
    file_list = list()
    filenames = os.listdir(path)
    for file in filenames:
        pathh = join(path, file)
        if os.path.isdir(pathh):
                file_list.extend(scan_file(pathh))
        elif os.path.isfile(pathh):
                file_dict = dict()
                mtime = os.path.getmtime(pathh)
                size = os.path.getsize(pathh)
                offset = len(file_dir)
                filepath = pathh[offset + 1:]
                file_dict['filename'] = filepath
                file_dict['mtime'] = mtime
                file_dict['size'] = size
                file_list.append(file_dict)  # check
    return file_list


# ########################################tcp_listen_and_respond########################################################
def tcp_listener(get_file_time):
    server_port = 25000
    s = socket(AF_INET, SOCK_STREAM)
    s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    s.bind(('', server_port))
    s.listen(20)
    print('tcp server start listen.')
    while True:
        connection_socket, addr = s.accept()
        print('accept address', addr)
        th = Thread(target=respond, args=(get_file_time.value, connection_socket,))
        th.daemon = True
        th.start()


def respond(get_file_time, socket):
    while True:
        operation_code, content_b = tcp_receiver(socket)
        if operation_code == 0:
            print('receive code 0 : get get_file_time request')
            socket.send(return_getfile_time(get_file_time))
            print('get_file_time sent')

        if operation_code == 2:
            print('receive code 2 : file_info')
            filesize, total_block_number = struct.unpack('!QQ', content_b[:16])
            filename = content_b[16:].decode()
            print('filesize: ', filesize)
            print('tbn: ', total_block_number)
            print('filename: ', filename)
            # store info
            store_file_dict(filename, total_block_number)
            print('info stored')

        if operation_code == 3:
            print('receive code 3: get file block request.')
            block_index = struct.unpack('!Q', content_b[:8])[0]
            filename = content_b[8:].decode()
            socket.send(make_file_block(filename, block_index))


# ####################file_downloader###################################################################################
def file_downloader(ip, get_file_time, checker_state):
    serverPort = 25000
    downloader_Socket = socket(AF_INET, SOCK_STREAM)
    downloader_Socket.bind(('', 27000))
    f = open('file_record.log', 'w')
    f.close()
    while True:
        filename, total_block_number, current_block_index = read_file_log()
        if filename != -1:
            conn_state = True
            while conn_state:
                connINT = downloader_Socket.connect_ex((ip, serverPort))
                if connINT == 0:
                    print("downloader connect to", ip, "successfully.")
                    read_log_state = True
                    while read_log_state:
                        filename, total_block_number, current_block_index = read_file_log()
                        if filename != -1:
                            checker_state.value = 0
                            file_path = join(file_dir, filename)
                            filename_lefting = file_path + '.lefting'
                            f = open(filename_lefting, 'wb')
                            for index in tqdm(range(current_block_index, total_block_number)):
                                downloader_Socket.send(client_get_file_block(filename, index))
                                print('get file block request sent')
                                operation_code, content_b = tcp_receiver(downloader_Socket)
                                if operation_code == 4:
                                    print('receive file block, code = 4')
                                    f.write(content_b)
                                    # update current_block_number
                                    update_file_log(index + 1)
                            f.close()
                            os.rename(filename_lefting, file_path)
                            get_file_time.value = os.path.getmtime(file_dir)
                            checker_state.value = 1
                        else:
                            downloader_Socket.close()
                            read_log_state = False
                            conn_state = False



# ########################################record_file.log###############################################################
def store_file_dict(filename, total_block_number, current_block=0):
    file_dict = dict()
    file_dict['name'] = filename
    file_dict['total_block_number'] = total_block_number
    file_dict['current_block_index'] = current_block
    j = json.dumps(file_dict)

    f = open('file_record.log', 'r+')
    content = f.readline()
    if content.strip() == '':
        f.seek(0)
        f.write(j)
        f.write('\n')
        f.close()
    else:
        f = open('file_record.log', 'a')
        f.write(j)
        f.write('\n')
        f.close()


def read_file_log():
    f = open('file_record.log', 'r')
    content = f.readline()
    f.close()
    if content.strip() != '':
        file_dict = json.loads(content)
        filename = file_dict['name']
        total_block_number = file_dict['total_block_number']
        current_block_index = file_dict['current_block_index']
        return filename, total_block_number, current_block_index
    else:
        return -1, -1, -1


def update_file_log(current_index):
    f = open('file_record.log', 'r+')
    file_dict = json.loads(f.readline())
    total_block_number = file_dict['total_block_number']
    current_block_index = file_dict['current_block_index']

    if current_index < total_block_number:
        file_dict['current_block_index'] = current_index
        j = json.dumps(file_dict)
        f.seek(0)
        f.write(j)
        f.close()

    if current_index == total_block_number:
        f.seek(0)
        lines = f.readlines()
        f.seek(0)
        f.truncate()
        for line in lines[1:]:
            f.write(line)
        f.close()


# def check_local_file():
#     f = open('local_file_dict.log', 'w')
#     file_list = scan_file(file_dir)
#     if file_list != ['']:
#         for dict in file_list:
#             j = json.dumps(dict)
#             f.write(j)
#             f.write('\n')
#         f.close()







if __name__ == '__main__':
    parser = _argparse()
    print("ip Addresses:", parser.ip)
    # ip = parser.ip.split(",")
    # ip1 = ip[0]
    # ip2 = ip[1]
    ip = parser.ip

    block_size = 1024*1024
    file_dir = os.path.join(os.getcwd(), 'share')
    if os.path.exists(file_dir) is False:
        os.makedirs(file_dir)
        print('Create: ', file_dir)
    get_file_time = mp.Manager().Value('d', 0.0)
    checker_state = mp.Manager().Value('i', 1)

    p1 = Process(target=tcp_listener, args=(get_file_time,))
    p2 = Process(target=file_checker, args=(get_file_time, checker_state,))
    p3 = Process(target=tcp_notifier, args=(ip,))
    p4 = Process(target=file_downloader, args=(ip, get_file_time, checker_state,))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()






