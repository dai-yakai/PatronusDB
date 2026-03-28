#!/bin/bash

# ==============================================================================
# PatronusDB RDMA (SoftRoCE) Environment Automated Configuration Script
# Features: Auto-detect main network interface, set MTU, load kernel modules, bind rxe0
# ==============================================================================

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}[FATAL] This script must be run as root. Please use sudo ./init_rdma.sh${NC}"
  exit 1
fi

if ! command -v rdma &> /dev/null || ! command -v ibv_devices &> /dev/null; then
    echo -e "${RED}[FATAL] Missing RDMA tools. Please install them first:${NC}"
    echo -e "Ubuntu/Debian: apt-get install rdma-core ibverbs-utils iproute2"
    echo -e "CentOS/RHEL:   yum install rdma-core libibverbs-utils iproute"
    exit 1
fi

if [ "$1" == "clean" ]; then
    echo -e "${YELLOW}[INFO] Cleaning up RDMA (SoftRoCE) environment...${NC}"
    if rdma link show rxe0 &> /dev/null; then
        rdma link delete rxe0
        echo -e "${GREEN}[SUCCESS] rxe0 virtual device successfully removed.${NC}"
    else
        echo -e "${YELLOW}[INFO] rxe0 device not found. No cleanup needed.${NC}"
    fi
    exit 0
fi

echo -e "${GREEN}>>> Starting PatronusDB RDMA (SoftRoCE) node configuration <<<${NC}"

ACTIVE_IFACE=$(ip route get 8.8.8.8 2>/dev/null | awk '{print $5; exit}')

if [ -z "$ACTIVE_IFACE" ]; then
    ACTIVE_IFACE=$(ip -br link show | grep UP | grep -v lo | awk '{print $1}' | head -n 1)
fi

if [ -z "$ACTIVE_IFACE" ]; then
    echo -e "${RED}[FATAL] Unable to detect an active physical network interface!${NC}"
    exit 1
fi

echo -e "${YELLOW}[INFO] Detected active physical network interface: ${ACTIVE_IFACE}${NC}"

# Set MTU to 4096
echo -e "${YELLOW}[INFO] Setting MTU of ${ACTIVE_IFACE} to 4096...${NC}"
ip link set dev "$ACTIVE_IFACE" mtu 4200
if [ $? -ne 0 ]; then
    echo -e "${RED}[ERROR] Failed to set MTU to 4096 on ${ACTIVE_IFACE}. High MTU might not be supported by the driver.${NC}"
fi

echo -e "${YELLOW}[INFO] Loading RDMA kernel modules (rdma_rxe, ib_uverbs)...${NC}"
modprobe rdma_rxe
modprobe ib_uverbs

if rdma link show rxe0 &> /dev/null; then
    echo -e "${YELLOW}[INFO] rxe0 virtual device already exists, preparing to re-bind...${NC}"
    rdma link delete rxe0
fi

echo -e "${YELLOW}[INFO] Converting Ethernet interface ${ACTIVE_IFACE} to RDMA RoCE device (rxe0)...${NC}"
rdma link add rxe0 type rxe netdev "$ACTIVE_IFACE"

if [ $? -ne 0 ]; then
    echo -e "${RED}[FATAL] Failed to create rxe0! Please check system logs (dmesg).${NC}"
    exit 1
fi

if [ -d "/dev/infiniband" ]; then
    chmod a+rw /dev/infiniband/uverbs* 2>/dev/null
fi

echo -e "${GREEN}[SUCCESS] RDMA init successfully. device list:${NC}"
ibv_devices | grep -E "^[[:space:]]+rxe0|node GUID"