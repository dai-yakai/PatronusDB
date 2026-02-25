CC = gcc

CFLAGS = -Wall -g

LDFLAGS = -lreadline -lhistory

TARGET = pdb-cli

SRC_DIR := src
OBJ_DIR := obj

SRCS = pdb_cli.c
SRCS_WITH_PATH := $(addprefix $(SRC_DIR)/, $(SRCS))
OBJS := $(SRCS:%.c=$(OBJ_DIR)/%.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $(TARGET) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean