TARGET = pdb_server
CC = gcc

CFLAGS  := -g -I./src -I./NtyCo/core/
LDFLAGS := -L./NtyCo/
LIBS    := -lntyco -lpthread -ldl -luring

SRC_DIR := src
OBJ_DIR := obj

SRCS := pdb_reactor.c pdb_uring.c pdb_ntyco.c pdb_server.c pdb_array.c pdb_malloc.c pdb_rbtree.c pdb_hash.c pdb_mempool.c pdb_mempool_freelist.c pdb_replication.c \
		pdb_sds.c pdb_conf.c pdb_log.c

SRCS_WITH_PATH := $(addprefix $(SRC_DIR)/, $(SRCS))

OBJS := $(SRCS:%.c=$(OBJ_DIR)/%.o)

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

clean:
	rm -rf $(TARGET) $(OBJ_DIR)
