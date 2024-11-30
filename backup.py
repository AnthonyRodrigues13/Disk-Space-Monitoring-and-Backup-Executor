import os
import threading
from collections import OrderedDict
import time
from datetime import datetime, timedelta
import subprocess
import psycopg2
import glob
import shutil

from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor


def list_files(directory):
    files_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            files_list.append(file_path)
    return files_list


def do_log_cmds(log):
    with open(r"/tmp/fanotify_store_commands.txt", "a") as file:
        file.write(log)
        file.write("\n")


def do_log(log):
    with open(r"/tmp/fanotify_py_log.txt", "a") as file:
        file.write(f"{log}\n")


def after_cp(cmd_info, host, cur, conn, commands_list):
        file_size = os.path.getsize(cmd_info[2])                                # cmd_info contains the file_path where at [0] -> cmd (cp,rm,etc) and [1],[2] -> and or the filename with path
        cur.execute(f"select file from diskspace_fanotify where file='{cmd_info[1]}' and region='mail{host}';")                       # Getting previous entry data for validation of new data
        rows = cur.fetchone()
        if rows:
            commands_list.append(f" UPDATE diskspace_fanotify SET date='{datetime.today().date()}',size={file_size}  WHERE file='{cmd_info[1]}' and region='mail{host}';")
        else:
            commands_list.append(f" INSERT INTO diskspace_fanotify VALUES ('{datetime.today().date()}', {file_size}, '{cmd_info[1]}', 'mail{host}');")


def execute_command(command, host, cur, conn, commands_list):                                                           #  Function to execute commands
    try:
        do_log(f"Executing command :- {command}")
        cmd_info = command.split()
        if(cmd_info[0] == 'cp'):
            try:
                shutil.copyfile(cmd_info[1], cmd_info[2])
                after_cp(cmd_info,host,cur,conn,commands_list)
            except FileNotFoundError as e:
                try:
                    file_path = glob.glob(f"{cmd_info[1][:len(cmd_info[1]) - len(cmd_info[1].split('/')[-1])]}")
                    if ((not file_path) or (file_path == []) or (len(file_path)==0) ):
                        temp = 0
                    else:
                        result = os.system(f"mkdir -p {cmd_info[2][:len(cmd_info[2]) - len(cmd_info[2].split('/')[-1])]}")
                        if result == 0:
                            shutil.copyfile(cmd_info[1], cmd_info[2])
                            after_cp(cmd_info,host,cur,conn,commands_list)
                except Exception as e:
                    do_log(f"An Error occurred while copying2 for '{command}': {e}\n")
            except Exception as e:
                do_log(f"An Error occurred while copying1 for '{command}': {e}\n")
        #elif(cmd_info[0] == 'rm' ):
        elif(cmd_info[0] == 'rm' or cmd_info[0] == 'rmdir'):
                if ' (deleted)' in command:
                    temp = 0
                else:
                    path_info = cmd_info[-1].split('/')                               # Converting backup path to main path
                    new_path = ""
                    path_info.pop(0)
                    path_info[0] = 'home'   # format the path
                    for ele in path_info:
                        new_path = new_path + '/' + ele                                     # Conversion ends here
                    try:
                        if len(cmd_info) == 2:
                            result = os.system(command)
                            if result == 0 and (len(cmd_info) == 2):
                                commands_list.append(f" DELETE FROM diskspace_fanotify WHERE file='{new_path}' and region='mail{host}';")
                            else:
                                res = os.system(f"rmdir {cmd_info[1]}")
                                if res == 0:
                                    commands_list.append(f" DELETE FROM diskspace_fanotify WHERE file='{new_path}' and region='mail{host}';")
                        else:
                            try:
                                shutil.rmtree(f"{cmd_info[-1]}")
                                commands_list.append(f" DELETE FROM diskspace_fanotify WHERE file like '{new_path}%' and region='mail{host}';")
                            except FileNotFoundError as e:
                                file_path = glob.glob(f"{cmd_info[-1]}")
                                if ((not file_path) or (file_path == []) or (len(file_path)==0) ):
                                    temp = 0
                                else:
                                    #do_log(f"Trying again Executing command :- {command}")
                                    execute_command(command, host, cur, conn, commands_list)
                    except Exception as e:
                        do_log(f"Error occurred while '{command}' in rm {e}")
    except Exception as e:
        do_log(f"An Error occurred in execute() for command '{command}': {e}\n")


def read_file(file_path):                                                               # Function to read file
    with open(file_path, "r") as file:
        lines = file.readlines()
    return lines


def write_commands_file(file_path,lines):                                               # Function to write file
    with open(file_path, "w") as file:
        file.writelines(lines)


def get_timeRange():
    current_minute = int(time.strftime("%M"))
    h = int(time.strftime("%H"))
    if 0 <= current_minute <= 6:
        time_range = 4
        h -= 1
        if h < 0:
            h = 23
        min_4_bench = 59
    elif 6 <= current_minute <= 11:
        time_range = 1
        min_4_bench = 5
    elif 11 <= current_minute <= 16:
        time_range = 2
        min_4_bench = 10
    elif 16 <= current_minute <= 21:
        time_range = 3
        min_4_bench = 15
    elif 21 <= current_minute <= 26:
        time_range = 4
        min_4_bench = 20
    elif 26 <= current_minute <= 31:
        time_range = 1
        min_4_bench = 25
    elif 31 <= current_minute <= 36:
        time_range = 2
        min_4_bench = 30
    elif 36 <= current_minute <= 41:
        time_range = 3
        min_4_bench = 35
    elif 41 <= current_minute <= 46:
        time_range = 4
        min_4_bench = 40
    elif 46 <= current_minute <= 51:
        time_range = 1
        min_4_bench = 45
    elif 51 <= current_minute <= 56:
        time_range = 2
        min_4_bench = 50
    else:
        time_range = 3
        min_4_bench = 55
    return time_range,h,min_4_bench


def process_commands(cmds, host, reference_datetime):
    try:
        conn = psycopg2.connect( database="name", user='user')
        cur = conn.cursor()
        diff_min = timedelta(minutes=15)
        i = 0
        commands_list = []
        start_time5 = time.time()
        while i < len(cmds):
            start_time1 = time.time()
            try:
                next_ele = cmds[i+1]
            except Exception as e:
                next_ele = ""
            if cmds[i][:-20] in next_ele:
                cmds.pop(i)
            else:
                cmd_sp = cmds[i].split()
                date_object = datetime.strptime(f"{cmd_sp[-2]} {cmd_sp[-1]}", "%d-%m-%Y %H:%M:%S")
                if date_object < reference_datetime - diff_min:
                    execute_command(cmds[i][:-20], host, cur, conn, commands_list)
                    #execute_command(cmds[i][:-20], host, cur, conn)
                    cmds.pop(i)
                else:
                    do_log_cmds(cmds[i])
                    cmds.pop(i)
                    unique_list = list(OrderedDict.fromkeys(cmds))
                    cmds = list(unique_list)
                    unique_list = []
                if(len(commands_list) == 10):
                    x = " ".join(commands_list)
                    start_time6 = time.time()
                    cur.execute(x)
                    conn.commit()
                    # do_log(f"Time elapsed for quering {len(commands_list)} command: {time.time() - start_time6} seconds started at {start_time6}")
                    commands_list = []
            end_time1 = time.time()
            # do_log(f"Time elapsed for executing a command: {end_time1 - start_time1} seconds started at {start_time1}")
        if(commands_list):
            x = " ".join(commands_list)
            start_time6 = time.time()
            cur.execute(x)
            conn.commit()
            # do_log(f"Time elapsed for quering {len(commands_list)} command: {time.time() - start_time6} seconds started at {start_time6}")
            commands_list = []
        end_time5 = time.time()
        # do_log(f"Time elapsed for executing all commands in thread: {end_time5 - start_time5} seconds started at {start_time5}")
        conn.commit()
        conn.close()
    except Exception as e:
        do_log(f"Error in process_commands Exeption {e}")


def main():
    cmds = []
    host = subprocess.check_output(["/usr/bin/hostname"])[-4:-1].decode("utf-8")[:-1]
    time_range = 0
    min_4_bench = 0
    h = 0
    inc = 0
    #while True:
    while(inc < 4):                                                                         # infinite loop
        inc += 1
        try:
            if(time_range == 0):
                time_range,h,min_4_bench = get_timeRange()
            else:
                time_range += 1
                if(time_range > 4):
                    time_range = 1
                if(min_4_bench == 59):
                    min_4_bench = 5
                    h += 1
                    if(h > 23):
                        h = 0
                elif(min_4_bench == 55):
                    min_4_bench = 59
                else:
                    min_4_bench += 5
                time_range1 = ((int(time.strftime("%M")) // 5) % 4) + 1;
                #do_log(f"timerange1 is {time_range1} time_range is {time_range}")
                while(time_range1 == time_range):
                    time.sleep(30)
                    time_range1 = ((int(time.strftime("%M")) // 5) % 4) + 1;
                    do_log(f"Sleeping to avoid overlap {datetime.now()} timerange1 is {time_range1} time_range is {time_range}")

            reference_datetime = datetime.now()
            start_time2 = time.time()
            bench_time = timedelta(minutes=min_4_bench,hours=h,seconds=59)
            start_time3 = time.time()
            commands_file_path = f"/tmp/fanotify_commands{time_range}.txt"
            lines = read_file(commands_file_path)                                       # Getting lines
            cmds = read_file("/tmp/fanotify_store_commands.txt")                        # Getting lines
            # do_log(f"Reading from file cmds with length {len(cmds)}")
            # do_log(f"Reading from file {commands_file_path} with length {len(lines)}")
            write_commands_file(commands_file_path,[])
            cmds.extend(lines)
            if cmds:
                unique_list = list(OrderedDict.fromkeys(cmds))
                cmds = list(unique_list)
                # do_log(f"Reading from file cmds with length {len(cmds)}")
                cmds = [cmd for cmd in cmds if (cmd != "\n" and ' (deleted)' not in cmd )]
                cmds.sort()
                # do_log(f"Reading from file cmds with length {len(cmds)}")
                idx = -1
                while(cmds[idx].split()[0]  == 'rmdir'):
                    cmds[idx] = f'rm -rvf {cmds[idx].split()[1]}'
                    if(idx -1)//-1 <= len(cmds):
                        idx -=1
                    else:
                        break
                cmds.sort()
                write_commands_file("/tmp/fanotify_store_commands.txt",[])
                unique_list = []
                lines = []
                end_time3 = time.time()
                # do_log(f"Time elapsed until pre-processing: {end_time3 - start_time3} seconds and started at {start_time3}")
                # do_log(f"It is unsafe to exit if needed at {datetime.now()}.")

                split_index1 = len(cmds) // 4
                cmds_part1 = cmds[:split_index1]
                cmds_part2 = cmds[split_index1:2 * split_index1]
                cmds_part3 = cmds[2 * split_index1:3 * split_index1]
                cmds_part4 = cmds[3 * split_index1:]
                threads = []
                for cmds_part in [cmds_part1, cmds_part2, cmds_part3, cmds_part4]:
                    thread = threading.Thread(target=process_commands, args=(cmds_part, host, reference_datetime))
                    thread.start()
                    threads.append(thread)
                for thread in threads:
                    thread.join()
            # do_log(f"It is safe to exit if needed at {datetime.now()}.")
            end_time2 = time.time()
            # do_log(f"Time elapsed until one iteration i.e. one file: {end_time2 - start_time2} seconds and started at {start_time2}")
            # do_log("Sleeping\n")
            time.sleep(30)
            if(inc == 4):
                cmds = read_file("/tmp/fanotify_store_commands.txt")                        # Getting lines
                if cmds:
                    reference_datetime = datetime.now()
                    unique_list = list(OrderedDict.fromkeys(cmds))
                    cmds = list(unique_list)
                    cmds.sort()
                    write_commands_file("/tmp/fanotify_store_commands.txt",[])
                    unique_list = []
                    lines = []
                    end_time3 = time.time()
                    # do_log(f"Time elapsed until pre-processing: {end_time3 - start_time3} seconds and started at {start_time3}")
                    # do_log(f"It is unsafe to exit if needed at {datetime.now()}.")
                    cmds = [cmd for cmd in cmds if cmd != "\n"]
                    split_index1 = len(cmds) // 4
                    cmds_part1 = cmds[:split_index1]
                    cmds_part2 = cmds[split_index1:2 * split_index1]
                    cmds_part3 = cmds[2 * split_index1:3 * split_index1]
                    cmds_part4 = cmds[3 * split_index1:]
                    threads = []
                    for cmds_part in [cmds_part1, cmds_part2, cmds_part3, cmds_part4]:
                        thread = threading.Thread(target=process_commands, args=(cmds_part, host, reference_datetime))
                        thread.start()
                        threads.append(thread)
                    for thread in threads:
                        thread.join()
                # do_log(f"Code completed at {datetime.now()}.")
        except Exception as e:
            do_log(f"An error occurred in main: {e}")


if __name__ == "__main__":
    main()
