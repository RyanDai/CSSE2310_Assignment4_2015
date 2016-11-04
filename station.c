#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <unistd.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>


#define MAXHOSTNAMELEN 128
#define MAXSTATIONNUM 10000
#define MAXRESOURCENUM 1000

/*
 * lock: to protect the shared varible
 */
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

void read_message(char *message, char *forwardMessage);
void *client_thread(void *arg);

struct Station {
    int port;
    int fd;
    int authStrLength;
    char *secret;
    char *name;
    int totalStation;
    FILE *logFileName;
    int processedTrains;
    int notMineTrains;
    int formatErrorTrains;
    int noFollowedTrains;
    int totalResources;
    
    
    
};

typedef struct Station Station;

/*
 * Station: struct to hold varibles of station
 */
Station *s;

struct Connection {
    int stationNameLen;
    char *stationName;
    FILE *socketSend;
    FILE *socketReceive;
    int fd;
    int stationExist;
};

typedef struct Connection Connection;


/*
 *connections: an array to hold all station necctions
 */
struct Connection connections[MAXSTATIONNUM];


struct Resource {
    int quantity;
    char *resourceName;
};
typedef struct Resource Resource;

/*
 * r: struct to hold varibles of resources
 */
Resource *r;


/*
 * resources: an array to hold all resources
 */
struct Resource resources[MAXRESOURCENUM];

/*
 * Function: write_logfile()
 *
 * write into log file
 *
 * return void
 */
void write_logfile() {
    int i;
    int printed = 0;
    fprintf(s->logFileName, "=======\n");
    fprintf(s->logFileName, "%s\n", s->name);
    fprintf(s->logFileName, "Processed: %d\n", s->processedTrains);
    fprintf(s->logFileName, "Not mine: %d\n", s->notMineTrains);
    fprintf(s->logFileName, "Format err: %d\n", s->formatErrorTrains);
    fprintf(s->logFileName, "No fwd: %d\n", s->noFollowedTrains);
    for(i = 0; i < s->totalStation; i++) {
        if(connections[i].stationExist == 1) {
            if(printed > 0) {
                fprintf(s->logFileName, ",");
            }
    
            fprintf(s->logFileName, "%s", connections[i].stationName);
            printed++;
        }
    }
        
    fprintf(s->logFileName, "\n");
    for(i = 0; i < s->totalResources; i++) {
        fprintf(s->logFileName, "%s ", resources[i].resourceName);
        fprintf(s->logFileName, "%d\n", resources[i].quantity);
    }
 
    fflush(s->logFileName);
    
    
}

/*
 * Function: log_thread
 *
 * log thread
 *
 * return void
 */
void *log_thread(void *arg) {
    pthread_mutex_lock(&lock);
    write_logfile();
    pthread_mutex_unlock(&lock);
    return NULL;
}

/*
 * Function: inthandle
 *
 * inthandle
 *
 * return void
 */
void inthandle(int s) {
    pthread_t threadId;
    pthread_create(&threadId, NULL, log_thread, NULL);
    pthread_detach(threadId);
} 

/*
 * Function: capitalise
 *
 * make char capital
 *
 * return buffer
 */
char *capitalise(char *buffer, int len)
{
    int i;

    for(i = 0; i < len; i++) {
        buffer[i] = (char)toupper((int)buffer[i]);
    }
    return buffer;
}

/*
 * Function: process_connections
 *
 * process connections between stations
 *
 * return void
 */
void process_connections(int fdServer)
{
    int fd;
    struct sockaddr_in fromAddr;
    socklen_t fromAddrSize;
    pthread_t threadId;

    while(1) {
        fromAddrSize = sizeof(struct sockaddr_in);
        fd = accept(fdServer, (struct sockaddr*)&fromAddr, &fromAddrSize);
        if(fd < 0) {
            perror("Error accepting connection");
            exit(1);
        }
	pthread_create(&threadId, NULL, client_thread, 
		(void*)(int64_t)fd);
	pthread_detach(threadId);
    }
}

/*
 * Function: sort_resources_array
 *
 * sort_resources_arrays
 *
 * return void
 */
void sort_resources_array() {
    int i, j;
    Resource tmp;
    for(i = 0; i < s->totalResources - 1; i++) {
        for(j = 0; j < s->totalResources - 1 - i; j++) {
            if(strcmp(resources[j].resourceName, 
                    resources[j + 1].resourceName) > 0) {
                tmp = resources[j + 1];
                resources[j + 1] = resources[j];
                resources[j] = tmp;
            }
        }
    }
}

/*
 * Function: sort_stations_array
 *
 * sort_resources_arrays
 *
 * return void
 */
void sort_stations_array() {
    int i, j;
    Connection tmp;
    for(i = 0; i < s->totalStation - 1; i++) {
        for(j = 0; j < s->totalStation - 1 - i; j++) {
            if(strcmp(connections[j].stationName, 
                    connections[j + 1].stationName) > 0) {
                tmp = connections[j + 1];
                connections[j + 1] = connections[j];
                connections[j] = tmp;
            }
        }
    }
}

/*
 * Function: remove_newline
 *
 * remove_newline
 *
 * return void
 */
void remove_newline(char *line) {
    int newLine = strlen(line) - 1;
    if (line[newLine] == '\n') {
        line[newLine] = '\0';
    }
}

/*
 * Function: has_semicolon
 *
 * judge if has semicolon
 *
 * return 1 if have and 0 if not
 */
int has_semicolon(char *buffer) {
    int i;
    int bufferLen;
    bufferLen = strlen(buffer);
    for(i = 0; i < bufferLen; ++i) {
        if(buffer[i] == ':') {
            return 1;
        }
    }
    return 0;
}

/*
 * Function: get_trains
 *
 * get the trains
 *
 * return buffer
 */
char *get_trains(char *buffer, char *station) {
    int i;
    int bufferLen;
    bufferLen = strlen(buffer);
    for(i = 0; i < bufferLen; i++) {
        if(buffer[i] != ':') {
            station[i] = buffer[i];
        } else {
            station[i] = 0;
            return &buffer[i + 1];
        }
    }
    station[i] = 0;
    buffer[i] = 0;
    return NULL;
}

/*
 * Function: forward_message
 *
 * forward message to next station
 *
 * return void
 */
void forward_message(char *forwardMessage) {
    int i;
    char stationName[2048];
    get_trains(forwardMessage, stationName);
    for(i = 0; i < s->totalStation; i++) {
        fprintf(stdout, "total:%d\n", s->totalStation);
        fflush(stdout);
        if(strcmp(connections[i].stationName, stationName) == 0 
                && connections[i].stationExist == 1) {
            fprintf(stdout, "found it\n");
            fflush(stdout);
            fprintf(connections[i].socketSend, "%s\n", forwardMessage);
            fflush(connections[i].socketSend);
            return;
        }
    }
    fprintf(stdout, "not found %s\n", stationName);
    fflush(stdout);
    s->noFollowedTrains++;
}

/*
 * Function: has_right_bracket
 *
 * judge if has right bracket
 *
 * return 1 if have 0 if not
 */
int has_right_bracket(char *message) {
    int i;
    int messageLen;
    messageLen = strlen(message);
    for(i = 0; i < messageLen; ++i) {
        if(message[i] == ')') {
            return 1;
        }
    }
    return 0;
}

/*
 * Function: has_operator
 *
 * judge if has operator
 *
 * return 1 if have 0 if not
 */
int has_operator(char *message) {
    int i;
    int messageLen;
    messageLen = strlen(message);
    for(i = 0; i < messageLen; ++i) {
        if(message[i] == '+' || message[i] == '-') {
            return 1;
        }
    }
    return 0;
}

/*
 * Function: get_port_num
 *
 * get the port number
 *
 * return buffer
 */
char *get_port_num(char *message, char *portNum) {
    int i;
    int messageLen;
    messageLen = strlen(message);
    for(i = 0; i < messageLen; i++) {
        if(message[i] != '@') {
            portNum[i] = message[i];
        } else {
            portNum[i] = 0;
            return &message[i + 1];
        }
    }
    return NULL;
}

/*
 * Function: get_resources_name
 *
 * get the name of resource
 *
 * return void
 */
char *get_resources_name(char *message, char *resourcesName) {
    int i;
    int messageLen;
    messageLen = strlen(message);
    for(i = 0; i < messageLen; i++) {
        if(message[i] != '+' && message[i] != '-') {
            resourcesName[i] = message[i];
        } else {
            resourcesName[i] = 0;
            return &message[i];
        }
    }
    return NULL;
}

/*
 * Function: get_source_number
 *
 * get the number of resource
 *
 * return buffer
 */
char *get_source_number(char *message, char *resourceNum) {
    int i;
    int messageLen;
    messageLen = strlen(message);
    for(i = 0; i < messageLen; i++) {
        if(message[i] != ',' && message[i] != ':') {
            resourceNum[i] = message[i];
        } else {
            resourceNum[i] = 0;
            return &message[i];
        }
    }
    resourceNum[i] = 0;
    return NULL;
}

/*
 * Function: get_host_name
 *
 * get_host_name
 *
 * return buffer
 */
char *get_host_name(char *message, char *hostName) {
    int i;
    int messageLen;
    messageLen = strlen(message);
    for(i = 0; i < messageLen; i++) {
        if(message[i] != ',' && message[i] != ')') {
            hostName[i] = message[i];
        } else {
            hostName[i] = 0;
            return &message[i];
        }
    }
    return NULL;
}

/*
 * Struct: in_addr *name_to_ip_addr
 *
 * in_addr *name_to_ip_addr
 *
 * return socket address
 */
struct in_addr *name_to_ip_addr(char *hostname) {
    int error;
    struct addrinfo *addressInfo;

    error = getaddrinfo(hostname, NULL, NULL, &addressInfo);
    if(error) {
	return NULL;
    }
    return &(((struct sockaddr_in*)(addressInfo->ai_addr))->sin_addr);
}

/*
 * Function: connect_to
 *
 * deal with connect to
 *
 * return int fd
 */
int connect_to(struct in_addr *ipAddress, int port)
{
    struct sockaddr_in socketAddr;
    int fd;
    
    // Create socket - IPv4 - TCP
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0) {
	perror("Error creating socket");
	exit(1);
    }
  
    socketAddr.sin_family = AF_INET;	
    socketAddr.sin_port = htons(port);	
    socketAddr.sin_addr.s_addr = ipAddress->s_addr;	

    if(connect(fd, (struct sockaddr*)&socketAddr, sizeof(socketAddr)) < 0) {
	perror("Error connecting");
	exit(1);
    }

    return fd;
}

/*
 * Function: process_message
 *
 * process the message
 *
 * return void
 */
void process_message(FILE *socketReceive, char *name) {
    char buffer[2048];
    char stationName[2048];
    char message[2048];
    while(fgets(buffer, 2048, socketReceive) != NULL) {
        remove_newline(buffer);
        pthread_mutex_lock(&lock);
        if(has_semicolon(buffer) == 0) {
            s->formatErrorTrains++;
                    
            pthread_mutex_unlock(&lock);
            continue;
        } else {      
            char *left;
            left = get_trains(buffer, stationName);
            fprintf(stdout, "station name is %s, left is %s", 
                    stationName, left);
            fflush(stdout);
            if(strcmp(stationName, s->name) != 0) {
                s->notMineTrains++;
                fprintf(stdout, "this is not mine");
                fflush(stdout);
                pthread_mutex_unlock(&lock);
                continue;
            } else {
                char *forwardMessage;
                forwardMessage = get_trains(left, message);
                fprintf(stdout, "forwardMessage name is %s, message is %s", 
                        forwardMessage, message);
                fflush(stdout);
                read_message(message, forwardMessage);
            }
            pthread_mutex_unlock(&lock);
        }
            
    }
            
    int i;    
    pthread_mutex_lock(&lock); 
    for(i = 0; i < s->totalStation; i++) {

        if((strcmp(connections[i].stationName, name) == 0) && 
                (connections[i].stationExist == 1)) {
            connections[i].stationExist = 0;
            break;
        }
    }
    pthread_mutex_unlock(&lock);
}

/*
 * Function: server_thread
 *
 * deal with server_thread
 *
 * return void pointer
 */
void *server_thread(void *arg) {
    int fd, i;
    char buffer[2048];
    FILE *socketSend;
    FILE *socketReceive;
    fd = (int)(int64_t)arg;
    socketSend = fdopen(fd, "w");
    socketReceive = fdopen(fd, "r");
    fprintf(socketSend, "%s\n", s->secret);
    fprintf(socketSend, "%s\n", s->name);
    fflush(socketSend);
    if(fgets(buffer, 2048, socketReceive) != NULL) {
        remove_newline(buffer);
        Connection *c = malloc(sizeof(Connection));
        int otherNameLen;
        char *otherName; 
        otherNameLen = strlen(buffer) + 1;
        otherName = (char *) malloc(otherNameLen * sizeof(char));
        for(i = 0; i < otherNameLen; i++) {
            otherName[i] = buffer[i];
        }
        if(strcmp(otherName, s->name) == 0) {
            fprintf(stderr, "Duplicate station names\n");
            exit(7);
        } else {    
            pthread_mutex_lock(&lock); 
            c->stationNameLen = strlen(otherName);
            c->stationName = (char *) malloc(c->stationNameLen * sizeof(char));
            for(i = 0; i < otherNameLen; i++) {
                c->stationName[i] = otherName[i];
            }
            c->socketSend = socketSend;
            c->socketReceive = socketReceive;    
            connections[s->totalStation].stationName = c->stationName;
            connections[s->totalStation].socketSend = c->socketSend;
            connections[s->totalStation].socketReceive = c->socketReceive;
            connections[s->totalStation].fd = fd;
            connections[s->totalStation].stationExist = 1;    
            s->totalStation++;
            sort_stations_array();
            pthread_mutex_unlock(&lock);
        }
        char *name = strdup(c->stationName);
        process_message(socketReceive, name);    
    }
    pthread_exit(NULL);	
    return NULL;
}

/*
 * Function: is_digit
 *
 * judge if is integer
 *
 * return 1 if is int and 0 if not
 */
int is_digit(char *str) {
    while(*str) {
        if(*str < '0' || *str > '9') {
            return 1;
        } 
        str++;
    }
    return 0;
}

/*
 * Function: read_message
 *
 * read_message
 *
 * return void
 */
void read_message(char *message, char *forwardMessage) {
    int fd, i;
    if(strcmp(message, "stopstation") == 0) {
        s->processedTrains++;
        if(forwardMessage == NULL) {
            ;
        } else {
            forward_message(forwardMessage);
        }
        for(i = 0; i < s->totalStation; i++) {
            if(connections[i].stationExist == 1) {
                close(connections[i].fd);
            }
        }
        write_logfile();
        fprintf(s->logFileName, "stopstation\n");
        exit(0);
    } else if(strcmp(message, "doomtrain") == 0) {
        s->processedTrains++;
        for(i = 0; i < s->totalStation; i++) {
            fprintf(connections[i].socketSend, "doomtrain");
        }
        for(i = 0; i < s->totalStation; i++) {
            if(connections[i].stationExist == 1) {
                close(connections[i].fd);
            }
        }
        write_logfile();
        fprintf(s->logFileName, "doomtrain\n");
        exit(0);
    } else if(strncmp("add(", message, 4) == 0) {
        if(has_right_bracket(message) == 0) {
            s->formatErrorTrains++;
            return;
        } else {
            s->processedTrains++;
            int port;
            char portNum[2048];
            char hostName[2048];
            char *leftMessage;
            char *nextMessage;
            char *ptr = message + 4;
            while(1) {
                leftMessage = get_port_num(ptr, portNum);
                port = (int) strtol(portNum, (char **)NULL, 10);
                nextMessage = get_host_name(leftMessage, hostName);
                struct in_addr *ip;
                ip = name_to_ip_addr(hostName);
                if(!ip) {
                    fprintf(stderr, "Unable to connect to station\n");
                    exit(6);
                }
                fd = connect_to(ip, port);
                pthread_t threadId;
                pthread_create(&threadId, NULL, server_thread, 
    		        (void*)(int64_t)fd);
    	        pthread_detach(threadId);
    	        if(nextMessage[0] == ',') {
    	            ptr = nextMessage + 1;
    	            continue;
    	        }
    	        break;
            }
            if(forwardMessage == NULL) {
                ;
            } else {
                forward_message(forwardMessage);
            }
        }
    } else {
        if(has_operator(message) == 0) {
            s->formatErrorTrains++;
            return;
        } else {
            int resourcesQuantity;
            char resourceName[2048];
            char resourceNum[2048];
            char *leftNum;
            char *nextResource;
            int flag;
            flag = 0;
            while(1) {
                int resourceAddedFlag = 0;
                leftNum = get_resources_name(message, resourceName);
                nextResource = get_source_number(leftNum, resourceNum);
                if(is_digit(resourceNum + 1)) {
                    flag = 1;
                    break;
                }  
                char *end;
                resourcesQuantity = (int) strtol(resourceNum, (
                        char **)&end, 10);
                if(*end) {
                    flag = 1;
                    break;
                    s->formatErrorTrains++;
                    break;
                }
                for(i = 0; i < s->totalResources; i++) {
                    if(strcmp(resources[i].resourceName, resourceName) == 0) {
                        resources[i].quantity += resourcesQuantity;
                        resourceAddedFlag = 1;
                    }
                }
                if(resourceAddedFlag == 0) {
                    resources[s->totalResources].resourceName = 
                            strdup(resourceName);
                    resources[s->totalResources].quantity = resourcesQuantity;
                    s->totalResources++;
                } 
                sort_resources_array();

                if(nextResource == NULL) {
                    break;
                }
                if(nextResource[0] == ',') {
    	            message = nextResource + 1;
    	            continue;
    	        }
    	        break;
            }
            if(flag == 1) {
                s->formatErrorTrains++;
            } else {
                s->processedTrains++;
            }
            if(flag == 1 || forwardMessage == NULL) {
                ;
            } else {
                forward_message(forwardMessage);
            }
        }
    }
}

/*
 * Function:client_thread
 *
 * deal with client thread
 *
 * return void pointer
 */
void *client_thread(void *arg) {
    int fd = (int)(int64_t)arg, i, otherNameLen;
    char buffer[2048];
    FILE *socketSend, *socketReceive;
    Connection *c;
    socketSend = fdopen(fd, "w");
    socketReceive = fdopen(fd, "r");
    if(fgets(buffer, 2048, socketReceive) != NULL) {
        remove_newline(buffer);
        if(strcmp(buffer, s->secret) == 0) {
            fgets(buffer, 2048, socketReceive);
            remove_newline(buffer);
            char *otherName;
            otherNameLen = strlen(buffer) + 1;
            otherName = (char *) malloc(otherNameLen * sizeof(char));
            for(i = 0; i < otherNameLen; i++) {
                otherName[i] = buffer[i];
            }
            if(strcmp(otherName, s->name) == 0) {
                fprintf(socketSend, "%s\n", s->name);
                fprintf(stderr, "Duplicate station names\n");
                exit(7);
            } else {
                fprintf(socketSend, "%s\n", s->name);
                fflush(socketSend);
                c = malloc(sizeof(Connection));
                pthread_mutex_lock(&lock); 
                c->stationNameLen = strlen(otherName);
                c->stationName = 
                        (char *) malloc(c->stationNameLen * sizeof(char));
                for(i = 0; i < otherNameLen; i++) {
                    c->stationName[i] = otherName[i];
                }
                c->socketSend = socketSend;
                c->socketReceive = socketReceive;
                connections[s->totalStation].stationName = c->stationName;
                connections[s->totalStation].socketSend = c->socketSend;
                connections[s->totalStation].socketReceive = c->socketReceive;
                connections[s->totalStation].fd = fd;
                connections[s->totalStation].stationExist = 1;
                s->totalStation++;
                sort_stations_array();
                pthread_mutex_unlock(&lock);
            }
            char *name = strdup(c->stationName);
            process_message(socketReceive, name); 
        } 
    }
    pthread_exit(NULL);
    return NULL;
}

/*
 * Function: read_authfile
 *
 * read authfile
 *
 * return void pointer
 */
void read_authfile(char *fileName, Station *s) {
    char buf[2048];
    int i;
    FILE *f = fopen(fileName, "r");
    if(!f) {
        fprintf(stderr, "Invalid name/auth\n");
        exit(2);        
    }
    if(fgets(buf, 2048, f) == NULL) {
        fprintf(stderr, "Invalid name/auth\n");
        exit(2);
    }
    if(buf[0] == '\n') {
        fprintf(stderr, "Invalid name/auth\n");
        exit(2); 
    }
    
    s->authStrLength = strlen(buf) + 1;
    s->secret = (char *) malloc(s->authStrLength * sizeof(char));
    for(i = 0; i < s->authStrLength; i++) {
        s->secret[i] = buf[i];
    }
    remove_newline(s->secret);
}

/*
 * Function: check_logfile
 *
 * check logfile
 *
 * return void pointer
 */
void check_logfile(char *fileName, Station *s) {
    FILE *f = fopen(fileName, "w+");
    if(!f) {
        fprintf(stderr, "Unable to open log\n");
        exit(3);        
    }
    s->logFileName = f;
}

/*
 * Function: print_port
 *
 * print_port
 *
 * return void 
 */
void print_port(Station *s) {
    struct sockaddr_in sin;
    socklen_t len = sizeof(sin);
    if (getsockname(s->fd, (struct sockaddr *)&sin, &len) == -1) {
        fprintf(stderr, "getsockname");
    } else {
        fprintf(stdout, "%d\n", ntohs(sin.sin_port));
        fflush(stdout);
    }
}

/*
 * Function: open_listen
 *
 * open_listen
 *
 * return integer fd
 */
int open_listen(int port, Station *s) {
    
    struct sockaddr_in serverAddr;
    int optVal;

    s->fd = socket(AF_INET, SOCK_STREAM, 0);
    if(s->fd < 0) {
        fprintf(stderr, "Listen error\n");
        exit(5);
    }

    optVal = 1;
    if(setsockopt(s->fd, SOL_SOCKET, SO_REUSEADDR, &optVal, sizeof(int)) < 0) {
        fprintf(stderr, "Listen error\n");
        exit(5);
    }

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);

    if(bind(s->fd, (struct sockaddr*)&serverAddr, 
            sizeof(struct sockaddr_in)) < 0) {
        fprintf(stderr, "Listen error\n");
        exit(5);
    }

    if(listen(s->fd, SOMAXCONN) < 0) {
        fprintf(stderr, "Listen error\n");
        exit(5);
    }

    return s->fd;
}

/*
 * Function: check_arguments
 *
 * check_arguments
 *
 * return void
 */
void check_arguments(int argc, char *argv[], Station *s) {
    char *ptr;
    
    if(argc != 4 && argc != 5 && argc != 6) {
        fprintf(stderr, 
                "Usage: station name authfile logfile [port [host]]\n");
        exit(1);  
    }
    if(argv[1][0] == '\0') {
        fprintf(stderr, "Invalid name/auth\n");
        exit(2);
    }
    s->name = argv[1];
    read_authfile(argv[2], s);
    check_logfile(argv[3], s);
    
    if(argc >= 5) {
        if((int) strtol(argv[4], &ptr, 10) <= 0 || 
                (int) strtol(argv[4], &ptr, 10) >= 65535 || (*ptr != '\0')) {
            fprintf(stderr, "Invalid port\n");
            exit(4);
        }
        s->port = (int) strtol(argv[4], (char **)NULL, 10);
    } else {
        s->port = 0;
    }
    
    open_listen(s->port, s);
    print_port(s);
}

/*
 * Function: main
 *
 * main function
 *
 * return 0
 */
int main(int argc, char *argv[]) {
    
    s = malloc(sizeof(Station));
    r = malloc(sizeof(Resource));
    
    memset(r, 0, sizeof(Resource));
    memset(s, 0, sizeof(Station));
    memset(connections, 0, sizeof(connections));

    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = SA_RESTART;
    //sigaction(SIGHUP, &sa, 0);
    
    sa.sa_handler = inthandle;
    sigaction(SIGHUP, &sa, 0);
    

    check_arguments(argc, argv, s);
    
    process_connections(s->fd);
    
  

    return 0;
}