#!/bin/bash

if [ -z "$1" ]; then
  echo "[error]: please give device name"
  exit 1
fi

IFACE=$1

if [ ! -d "/sys/class/net/$IFACE" ]; then
  echo "[error]: can not find device '$IFACE'"
  exit 1
fi

PCI_ADDR=$(basename $(readlink /sys/class/net/$IFACE/device))

if [ -z "$PCI_ADDR" ]; then
  echo "[error]: cannot find device PCI"
  exit 1
fi

DEVBIND_SCRIPT="./dpdk-devbind.py"

ifconfig $IFACE down 
modprobe uio_pci_generic
$DEVBIND_SCRIPT -b uio_pci_generic $PCI_ADDR
