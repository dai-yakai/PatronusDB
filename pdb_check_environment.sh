#!/bin/bash

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
BLUE='\033[0;34m'

echo -e "${BLUE}=== PatronusDB Environment Pre-flight Check ===${NC}\n"

# 1. Check linux kernel version (io_uring: 5.1+, eBPF: 5.8+)
KERNEL_VERSION=$(uname -r)
echo -e "Checking Kernel Version: ${GREEN}$KERNEL_VERSION${NC} (io_uring: 5.1+, eBPF: 5.8+)"
major=$(echo $KERNEL_VERSION | cut -d. -f1)
minor=$(echo $KERNEL_VERSION | cut -d. -f2)

if [ "$major" -lt 5 ] || ([ "$major" -eq 5 ] && [ "$minor" -lt 1 ]); then
    echo -e "${RED}[!] Warning: Kernel is older than 5.1. io_uring support might be limited.${NC}"
fi

# 2. Check liburing
echo -n "Checking io_uring (liburing): "
if [ -f /usr/local/include/liburing.h ] || [ -f /usr/include/liburing.h ]; then
    echo -e "${GREEN}INSTALLED${NC}"
else
    echo -e "${RED}NOT FOUND (Required for async I/O)${NC}"
fi

# 3. Check eBPF (libbpf & clang)
echo -n "Checking eBPF (libbpf): "
if ldconfig -p | grep -q libbpf; then
    echo -e "${GREEN}INSTALLED${NC}"
else
    echo -e "${RED}NOT FOUND (Required for mutation interception)${NC}"
fi

echo -n "Checking eBPF Compiler (clang): "
if command -v clang &> /dev/null; then
    echo -e "${GREEN}$(clang --version | head -n 1)${NC}"
else
    echo -e "${RED}NOT FOUND (Required to compile BPF programs)${NC}"
fi

echo -n "Checking RDMA (libibverbs): "
if ldconfig -p | grep -q libibverbs; then
    echo -e "${GREEN}INSTALLED${NC}"
else
    echo -e "${RED}NOT FOUND (Required for RDMA replication)${NC}"
fi

echo -n "Checking RDMA Hardware/Devices: "
if command -v ibv_devices &> /dev/null; then
    DEVICES=$(ibv_devices | grep -v -- "---" | tail -n +2)
    if [ -z "$DEVICES" ]; then
        echo -e "${RED}NO RDMA DEVICES FOUND (Check your HCA/Driver or RUN ./init_rmda.sh)${NC}"
    else
        echo -e "${GREEN}FOUND${NC}"
        echo "$DEVICES"
    fi
else
    echo -e "${RED}ibverbs-utils NOT INSTALLED${NC}"
fi

echo -e "\n${BLUE}=== Check Complete ===${NC}"