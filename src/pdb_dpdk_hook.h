#ifndef __PDB_DPDK_HOOK
#define __pdb_DPDK_HOOK

#include "ff_config.h"
#include "ff_api.h"
#include "ff_epoll.h"

#define bind(fd, addr, len)    ff_bind((fd), (const struct linux_sockaddr *)(addr), (len))
#define accept(fd, addr, len)  ff_accept((fd), (struct linux_sockaddr *)(addr), (len))
#define connect(fd, addr, len) ff_connect((fd), (struct linux_sockaddr *)(addr), (len))

#define socket      ff_socket
// #define bind        ff_bind
#define listen      ff_listen
// #define accept      ff_accept
// #define connect     ff_connect
#define send        ff_send
#define recv        ff_recv
#define sendto      ff_sendto
#define recvfrom    ff_recvfrom
#define close       ff_close
#define setsockopt  ff_setsockopt
#define getsockopt  ff_getsockopt
#define fcntl       ff_fcntl
#define read        ff_read
#define write       ff_write

#define epoll_create  ff_epoll_create
#define epoll_ctl     ff_epoll_ctl
#define epoll_wait    ff_epoll_wait

#endif