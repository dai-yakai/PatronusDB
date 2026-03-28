TARGET    := pdb_server
CC        := gcc
CLANG     := clang
BPFTOOL   := /usr/sbin/bpftool

INCLUDES  := -I./src -I./NtyCo/core/ -I/usr/include/x86_64-linux-gnu
EXTRA_CFLAGS := -D DATA_STRUCTURE_TEST=0

ifeq ($(USE_DPDK), 1)
    EXTRA_CFLAGS += -D ENABLE_DPDK=1

    FF_PATH ?= /home/dai/PatronusDB/dpdk_tcp/f-stack
    PKGCONF ?= pkg-config

    DPDK_CFLAGS := $(shell $(PKGCONF) --cflags libdpdk)
    INCLUDES += $(DPDK_CFLAGS) -I$(FF_PATH)/lib

    DPDK_LIBS_RAW := $(shell $(PKGCONF) --static --libs libdpdk)
    DPDK_LIBS_NO_CCP := $(subst -l:librte_crypto_ccp.a,,$(DPDK_LIBS_RAW))
    DPDK_LIBS_CLEAN  := $(subst -l:librte_crypto_openssl.a,,$(DPDK_LIBS_NO_CCP))

    FSTACK_LIBS := -L$(FF_PATH)/lib -Wl,--whole-archive,-lfstack,--no-whole-archive
    DPDK_EXTRA_LIBS := -lssl -lcrypto -lnuma
endif

ASAN_FLAGS := -fsanitize=address -fno-omit-frame-pointer -O0

CFLAGS    := -g $(INCLUDES)
CFLAGS    += $(EXTRA_CFLAGS)
CFLAGS    += $(ASAN_FLAGS)

LDFLAGS   := -L./NtyCo/

LIBS      := -lntyco -lpthread -ldl -luring -libverbs -lbpf -lelf -lz -ljemalloc

ifeq ($(USE_DPDK), 1)
    LIBS += $(FSTACK_LIBS) $(DPDK_LIBS_CLEAN) $(DPDK_EXTRA_LIBS)
endif

SRC_DIR   := src
OBJ_DIR   := obj

SRCS := pdb_reactor.c pdb_uring.c pdb_ntyco.c pdb_server.c pdb_array.c pdb_malloc.c \
        pdb_rbtree.c pdb_hash.c pdb_mempool.c pdb_mempool_freelist.c pdb_replication.c \
        pdb_sds.c pdb_conf.c pdb_log.c pdb_parse_protocol.c pdb_rdb.c pdb_aof.c \
        pdb_dump.c pdb_list.c pdb_conninfo.c pdb_intset.c pdb_bitmap.c pdb_skiptable.c \
        pdb_set.c pdb_sortedSet.c pdb_value.c pdb_rdma.c pdb_serialize.c pdb_ebpf.c \
		pdb_incre_serialize.c

OBJS := $(SRCS:%.c=$(OBJ_DIR)/%.o)



.PHONY: all clean

all: $(TARGET)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c src/pdb_delta.skel.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

src/vmlinux.h:
	$(BPFTOOL) btf dump file /sys/kernel/btf/vmlinux format c > $@

pdb_delta.bpf.o: src/pdb_delta.bpf.c src/vmlinux.h
	$(CLANG) -g -O2 -target bpf -D__TARGET_ARCH_x86 $(INCLUDES) -Wno-unused-command-line-argument -c $< -o $@

src/pdb_delta.skel.h: pdb_delta.bpf.o
	$(BPFTOOL) gen skeleton $< > $@



$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

clean:
	rm -rf $(TARGET) $(OBJ_DIR) src/vmlinux.h src/pdb_delta.skel.h *.o