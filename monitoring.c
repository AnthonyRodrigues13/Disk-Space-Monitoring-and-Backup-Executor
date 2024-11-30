#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fanotify.h>
#include <unistd.h>
#include <poll.h>
#include <signal.h>
#include "briefcase.h"
#include <libpq-fe.h>
#include <string.h>
#include <time.h>

#define BUF_SIZE 4096 // 512 to 4096


void sigsegv_handler(int signo) {
    fprintf(stderr, "Segmentation fault occurred.\n");
    exit(EXIT_FAILURE);
}


void do_log_f(char *log) {
    FILE *file = fopen("/tmp/fanotify_log.txt", "a");
    if (file == NULL) {
        perror("Error opening file");
        return;
    }
    fprintf(file, "%s\n", log);
    fclose(file);
}


int verify_path(const char* command) {
    char* cmd = strdup(command);
    char* token = strtok(cmd, "/");
    char* list[40];
    char* username = NULL;
    char* t = NULL;
    char* domainname = "";
    int i=0,n,pos=0;
    char* path1 = (char*)malloc(strlen(command));
    sprintf(path1, "");

    while(token != NULL){
	list[i] = token;
        token = strtok(NULL, "/");
	n = ++i;
    }
    for(i=0;i<n;i++){
        if (strcmp(list[i], ".briefcase") == 0){
	    pos=i;
	    if(i+2 < n)
	    	username = list[i+2];
	    if(i+1 < n)
	    	t = list[i+1];
	}
    }    
    //for(i=pos-1;i>=2;i--){
    for(i=pos-1;i>=4;i--){  // for testing
	    if(strlen(list[i]) >= 2){
		char* new_dm = (char*)malloc(strlen(domainname) + strlen(list[i]) + 2); 
		if(domainname=="")
                	sprintf(new_dm, "%s", list[i]);
		else
                	sprintf(new_dm, "%s.%s", domainname, list[i]);
		domainname = new_dm;
	    }
    }
    for(i=0;i<=pos+2 && i<n;i++){
	   //if(list[i] != fULL)
	   if((list[i] != NULL) && (i>=2)) //for monitoring testing /mnt/hd/h
           	sprintf(path1, "%s/%s", path1, list[i]);
    }
    get_briefcase_home_dir(BRIEFCASE_ROOT_DIR,domainname, which_dot(domainname));
    char fdir[2024];
    if(username != NULL){
    	get_briefcase_user_dir(BRIEFCASE_USER_DIR,username,domainname);
	sprintf(fdir,"%s",BRIEFCASE_USER_DIR);
    }
    else{	
	sprintf(fdir,"%s",BRIEFCASE_ROOT_DIR);
	if(t != NULL)
	    sprintf(fdir,"%s/%s",fdir,t);	
    }
    char mssg[2048];
    //sprintf(mssg,"fdir(frm briefcase) is %s and path1 is %s Domainname is %s username is %s ",fdir,path1,domainname,username);
    //do_log_f(mssg);
    int result = strcmp(fdir, path1);
    return result == 0;
}




void do_log_f_cmd1(char *log1){  // Function to log the commands that will be executed in backup by the python code
    time_t t;
    struct tm *tm_info;
    time(&t);
    tm_info = localtime(&t);
    //int time_range = (tm_info->tm_min / 15) + 1;
    int time_interval = (tm_info->tm_min / 5);  // Determine the 5-minute interval
    int time_range = (time_interval % 4) + 1;
    char file_path[50];
    sprintf(file_path, "/tmp/fanotify_commands%d.txt", time_range);
    FILE *file1 = fopen(file_path, "a");
    if (file1 == NULL) {
        perror("Error opening file1");
        return;
    }
    char formatted_time[20];  // Adjust the buffer size as needed
    strftime(formatted_time, sizeof(formatted_time), "%d-%m-%Y %H:%M:%S", tm_info);
    fprintf(file1, "%s %s\n", log1, formatted_time);
    fclose(file1);
}




long int findSize(char file_name[]){  // Function to get file size
    	long int res = -1;
	FILE* fp = fopen(file_name, "r");
	if (fp == NULL)
		return res;
	fseek(fp, 0L, SEEK_END);
	res = ftell(fp);
	fclose(fp);
	if (res != -1)
		return res;
}

char* get_path(char path[]) {  // Function to convert original path to backup
    char final_path[2048] = "/mnt";  // Set the base path
    char *result = NULL;
    char *token = strtok(path, "/");
    int f = 0;
    while (token != NULL)
    {
        //if(f >= 2)  // use this for main 
        if(f >= 4)  // use for testing
                sprintf(final_path, "%s/%s", final_path, token);
        token = strtok(NULL, "/");
        f++;
    }
    result = strdup(final_path);
    if (result == NULL) {
    	return "";
    }
    return result;
}


int main(int argc, char *argv[])
{
    signal(SIGSEGV, sigsegv_handler);
    int mount_fd, event_fd,fd,ret,poll_num;
    //char buf;
    //char buf[BUF_SIZE];
    char buf1;
    ssize_t len, path_len;
    nfds_t nfds;
    struct pollfd fds[2];
    struct fanotify_event_metadata *metadata;
    struct fanotify_event_info_fid *fid;
    struct file_handle *file_handle;
    const char *file_name;
    long int siz;
    char filename[2048], old_path[2048], temp_path[2048], path[PATH_MAX], procfd_path[PATH_MAX], message[2048];
    char* final_path;



    if (argc != 2) {
        do_log_f("Invalid number of command line arguments.");
        exit(EXIT_FAILURE);
    }
    mount_fd = open(argv[1], O_DIRECTORY | O_RDONLY | O_LARGEFILE);
    if (mount_fd == -1) {
        perror(argv[1]);
        exit(EXIT_FAILURE);
    }

    fd = fanotify_init(FAN_CLASS_NOTIF | FAN_REPORT_DFID_NAME, 0);
    if (fd == -1) {
        perror("fanotify_init");
        exit(EXIT_FAILURE);
    }
    ret = fanotify_mark(fd, FAN_MARK_ADD | FAN_MARK_ONLYDIR | FAN_MARK_FILESYSTEM, FAN_ONDIR | FAN_CREATE | FAN_DELETE | FAN_CLOSE_WRITE | FAN_MODIFY | FAN_MOVED_TO | FAN_MOVED_FROM | FAN_ATTRIB | FAN_ACCESS, AT_FDCWD, argv[1]);
    //ret = fanotify_mark(fd, FAN_MARK_ADD | FAN_MARK_ONLYDIR | FAN_MARK_FILESYSTEM, FAN_ONDIR | FAN_CREATE | FAN_DELETE | FAN_CLOSE_WRITE | FAN_MODIFY | FAN_MOVED_TO | FAN_MOVED_FROM, AT_FDCWD, argv[1]);
    if (ret == -1) {
        perror("fanotify_mark (add)");
        exit(EXIT_FAILURE);
    }

    nfds = 2;
    fds[0].fd = STDIN_FILENO;         
    fds[0].events = POLLIN;
    fds[1].fd = fd;
    fds[1].events = POLLIN;

    do_log_f("Listening for events.");
    while(1){
        poll_num = poll(fds,nfds,-1);
        if (poll_num == -1) {
            if (errno == EINTR)
                continue;
            perror("poll");
            //PQfinish(conn);
            exit(EXIT_FAILURE);
        }
        if(poll_num > 0){
            if (fds[0].revents & POLLIN){
                while (read(STDIN_FILENO, &buf1, 1) > 0 && buf1 != '\n')
                    continue;
                break;
            }
            if(fds[1].revents & POLLIN){
                struct fanotify_event_metadata buf[4096]; //increased from 200 to 512
                len = read(fd, buf, sizeof(buf));
                if(len == -1 && errno != EAGAIN){
                    perror("read");
                    exit(EXIT_FAILURE);
                }
                for (metadata = (struct fanotify_event_metadata *) buf; FAN_EVENT_OK(metadata, len); metadata = FAN_EVENT_NEXT(metadata, len)){
                    fid = (struct fanotify_event_info_fid *) (metadata + 1);
                    file_handle = (struct file_handle *) fid->handle;
                    if (fid->hdr.info_type == FAN_EVENT_INFO_TYPE_FID || fid->hdr.info_type == FAN_EVENT_INFO_TYPE_DFID)
                        file_name = NULL;
                    else if (fid->hdr.info_type == FAN_EVENT_INFO_TYPE_DFID_NAME)
                        file_name = (const char*)(file_handle->f_handle + file_handle->handle_bytes);
                    else{
                        fprintf(stderr, "Received unexpected event info type.\n");
                        exit(EXIT_FAILURE);
                    }
                    event_fd = open_by_handle_at(mount_fd, file_handle, O_RDONLY);
                    if (event_fd == -1) {
                        if (errno == ESTALE) {
                            do_log_f("File handle is no longer valid. File has been deleted");
                            continue;
                        }
                        else {
                            perror("open_by_handle_at");
                            exit(EXIT_FAILURE);
                        }
                    }
                    if(file_name){
                        if (ret == -1) {
                            if (errno != ENOENT) {
                                perror("fstatat");
                                exit(EXIT_FAILURE);
                            }
                        }
                    }
                    snprintf(procfd_path, sizeof(procfd_path), "/proc/self/fd/%d",event_fd);
                    path_len = readlink(procfd_path, path, sizeof(path) - 1);
                    if(path_len == -1){
                        perror("readlink");
                        exit(EXIT_FAILURE);
                    }
                    path[path_len] = '\0';
                    sprintf(filename, "%s/%s", path, file_name);
                    sprintf(message,"filename is %s",filename);
                    do_log_f(message);
                    int r = verify_path(filename);
                    if(r)
                    {
                        size_t file_name_length = strlen(file_name);
                        if (file_name_length >= 3 && (strncmp(&file_name[file_name_length - 3], "swp", 3) == 0 || strncmp(&file_name[file_name_length - 3], "swx", 3) == 0) || strncmp(&file_name[file_name_length - 1],"~",1) == 0 ) {
                            sprintf(message,"Skipped %s",file_name);
                            do_log_f(message);
                        } 
                        else{
                            sprintf(message,"%s Event Mask: %x\n",filename, metadata->mask);
                            do_log_f(message);
                            if (metadata->mask == FAN_CREATE){  
                                sprintf(message,"FAN_CREATE: %s/%s ",path,file_name);
                                do_log_f(message);
                                sprintf(temp_path,"%s/%s",path,file_name);
                                final_path = get_path(path);
                                sprintf(message,"cp %s %s/%s ",temp_path,final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            if (metadata->mask == FAN_DELETE){
                                sprintf(message,"FAN_DELETE: %s/%s ",path,file_name);
                                do_log_f(message);
                                sprintf(temp_path,"%s/%s",path,file_name);
                                final_path = get_path(path);
                                sprintf(message,"rm %s/%s ",final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            if (metadata->mask == FAN_MODIFY){
                                sprintf(message,"FAN_MODIFY: %s/%s ",path,file_name);
                                sprintf(temp_path,"%s/%s",path,file_name);
                                do_log_f(message);
                                final_path = get_path(path);
                                sprintf(message,"cp %s %s/%s ",temp_path,final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            if (metadata->mask == FAN_CLOSE_WRITE){
                                sprintf(message,"FAN_CLOSE_WRITE: %s/%s ",path,file_name);
                                sprintf(temp_path,"%s/%s",path,file_name);
                                do_log_f(message);
                                final_path = get_path(path);
                                sprintf(message,"cp %s %s/%s ",temp_path,final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            if (metadata->mask == 0x10a || metadata->mask == 0xa) {
                                sprintf(message,"FAN_ATTRIB FAN_ACCESS: %s/%s ",path,file_name);
                                sprintf(temp_path,"%s/%s",path,file_name);
                                do_log_f(message);
                                final_path = get_path(path);
                                sprintf(message,"cp %s %s/%s ",temp_path,final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            if (metadata->mask == FAN_MOVED_FROM){
                                sprintf(message,"FAN_MOVED_FROM: %s/%s ",path,file_name);
                                do_log_f(message);
                                sprintf(temp_path,"%s/%s",path,file_name);
                                final_path = get_path(path);
                                sprintf(message,"rm %s/%s ",final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            if (metadata->mask == FAN_MOVED_TO){
                                sprintf(message,"FAN_MOVED_TO: %s/%s ",path,file_name);
                                do_log_f(message);
                                sprintf(temp_path,"%s/%s",path,file_name);
                                final_path = get_path(path);
                                sprintf(message,"cp %s %s/%s ",temp_path,final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            if (metadata->mask == (FAN_CREATE | FAN_ONDIR)){
                                sprintf(message,"FAN_CREATE | FAN_ONDIR: %s/%s ",path,file_name);
                                do_log_f(message);
                                final_path = get_path(path);
                                sprintf(message,"mkdir %s/%s ",final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            if (metadata->mask == (FAN_DELETE | FAN_ONDIR)){
                                sprintf(message,"FAN_DELETE | FAN_ONDIR: %s/%s ",path,file_name);
                                do_log_f(message);
                                final_path = get_path(path);
                                sprintf(message,"rmdir  %s/%s ",final_path,file_name);
                                do_log_f_cmd1(message);
                            }
                            sprintf(message,"");
                        }
                    }
                    close(event_fd);
                }
            }
        }
   } // while(1) closing
   do_log_f("Listening for events stopped.");
   exit(EXIT_SUCCESS);
}
