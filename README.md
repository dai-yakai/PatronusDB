# Getting started

## Building PDB from Source

* Ubuntu 20.04
1. Install required dependencies: 

   ```shell
   # RDMA dependencies
   sudo apt install -y rdma-core libibverbs-dev librdmacm-dev ibverbs-providers ibverbs-utils rdmacm-utils
   
   # io_uring dependencies
   git clone https://github.com/axboe/liburing.git
   cd liburing
   ./configure
   make -j$(nproc)
   sudo make install
   sudo ldconfig
   
   # ebpf dependencies
   sudo apt install -y clang llvm libelf-dev libbpf-dev gcc-multilib
   
   # cmake and gcc
   # CMake 3.16.3 or higher is recommended.
   ```
   
   You can execute our provided shell script to verify whether your environment meets the necessary requirements for running PatronusDB. If the environment check script outputs **NO RDMA DEVICES FOUND**, please execute **./init_rdma.sh** to initialize the RDMA and rerun pdb_check_environment.sh
   
   ```shell
   ./pdb_check_environment.sh
   ```
   
   ```shell
   # result:
   === PatronusDB Environment Pre-flight Check ===
   
   Checking Kernel Version: 5.15.0-139-generic
   Checking io_uring (liburing): INSTALLED
   Checking eBPF (libbpf): INSTALLED
   Checking eBPF Compiler (clang): clang version 10.0.0-4ubuntu1
   Checking RDMA (libibverbs): INSTALLED
   Checking RDMA Hardware/Devices: NO RDMA DEVICES FOUND (Check your HCA/Driver or RUN ./init_rmda.sh)
   
   === Check Complete ===
   ```
   
   ```shell
   ./init_rdma.sh
   ./pdb_check_environment.sh
   ```

 2. Download the PatronusDB source and run PatronusDB 

    ```shell
    git clone https://github.com/dai-yakai/PatronusDB.git
    cd PatronusDB
    make
    ./PatronusDB
    ```

## Using PDB-CLI

PDB-CLI is PDB' command line interface. It is available in pdb source. You can start a PDB-server instance, and then, in another terminal try the following:

```shell
cd client
make
./pdb-cli [server ip] [server port]
```

# PDB Data Types

* **Array**：
* **Hash Table**：
* **Rbtree**：
* **Bitmap**：
* **Set**: A collection of unique elements implemented using an internal hash-set to provide $O(1)$ membership testing.
* **Sorted set**: A hybrid structure combining a Hash Table and a Skip List. It supports score-based ranking and range retrieval, maintaining elements in a sorted state with $$O(\log N)$$ efficiency.

# Persistence

* PatronusDB ensures data durability through two primary persistence engines: **RDB (Snapshotting)** and **AOF (Append-Only File)**. 

* **RDB**: Serializes the entire in-memory dataset into a compact binary file with **pre-defined binary format**  by using **memory-mapped I/O (mmap)**.
* **AOF**: Utilizes **eBPF uprobes** to intercept database **mutations** in real-time. Each mutation is serialized into a **pre-defined binary format** and asynchronously writed into AOF file by **io_uring**.
* **Persistence Load**: Utilizes **mmap** to load persistence files and **deserializes** their content to recover the database state.
* **Configuration**: The location and filename of the persistence data can be customized via the `dump_dir` option in the configuration file. Users can toggle between these modes via the `is_aof` parameter in the configuration file. 

```shell
# PatronusDB.conf
dump_dir ./dump/dump.dump
# yes - AOF, no - RDB
is_aof no
```

# Replication

* **Full Sync**:  Leverages **RDMA** to transfer snapshots. Once the slave node comes online, the master node and slave nodes establish an RDMA connection through a multi-step handshake. Upon completion, the slave node directly accesses the master's memory to pull the full dataset. 

  **Note**: We provide an RDMA initialization shell script; you must execute this script to configure the environment before initiating master-slave synchronization. 

  ```shell
  ./init_rdma.sh
  ```

* **Incremental Sync**: Upon receiving an increment, the master node extracts the valid mutation from the user-space buffer and sends it in its raw string format directly to the slave nodes.

* **Configuration**:  You can define the node role and connection parameters in PatronusDB.conf:

  * **Master Node**: Set `is_replication yes` and `is_slave no`. 

  * **Slave Node**: Set `is_replication yes` and `is_slave yes`, then provide the master's ip and port.

    ```shell
    # PatronusDB.conf
    # An example configuration for slave node
    is_replication yes
    is_slave yes
    master_ip 192.168.137.222
    master_port 8888
    ```

# Performance Benchmarking

* **QPS**
  * **Reactor**

  | 内存模型 | RBTree | Hash | Array | Set | Sorted set | Bitmap |
  |  :--:  | :--:  | :--: |  :--:  |  :--:  |  :--:  |  :--:  |
  | malloc |  |  |  |  |  |  |
  | mempool | 519750 | 694444 | 34482 | 729394 | 453103 | 821692 |
  | jemalloc |  |        |  |  |  |  |
  
  
  * **Ntyco**
  
  | 内存模型 | RBTree | Hash | Array | Set | Sorted set | Bitmap |
  |  :--:  | :--:  | :--: |  :--:  |  :--:  |  :--:  |  :--:  |
  | malloc |  |  |  |  |  |  |
  | mempool | 519750 | 694444 | 34482 | 729394 | 453103 | 821692 |
  | jemalloc |  |        |  |  |  |  |
  
  * **io_uring**
  
  | 内存模型 | RBTree | Hash | Array | Set | Sorted set | Bitmap |
  |  :--:  | :--:  | :--: |  :--:  |  :--:  |  :--:  |  :--:  |
  | malloc |  |  |  |  |  |  |
  | mempool | 519750 | 694444 | 34482 | 729394 | 453103 | 821692 |
  | jemalloc |  |        |  |  |  |  |

* **Persistence**

  

* **Replication**

  rdb 107M-1126ms

