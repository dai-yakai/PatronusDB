TARGET = pdb_server

SRC = kv_reactor.c kv_uring.c hook_tcpserver.c kvstore.c kv_array.c kv_malloc.c kv_rbtree.c kv_hash.c mempool.c mempool_freelist.c replication.c

CC = gcc
CFLAGS = -g -I NtyCo/core/
LDFLAGS = -L ./NtyCo/
LIBS = -lntyco -lpthread -ldl -luring

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS) $(LIBS)

.PHONY: clean
clean:
	rm -f $(TARGET) *.o
