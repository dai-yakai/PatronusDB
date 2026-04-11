# Getting started

## Building PDB from Source

* Ubuntu 20.04
1. Install required dependencies: 

   ```shell
   # linux kernal: 5.15.0 or higher
   # clang version: 10.0.0-4ubuntu1
   
   # cmake and gcc
   # CMake 3.16.3 or higher is recommended.
   # gcc 9.4 or highter is recommended
   
   # RDMA dependencies
   sudo apt install -y rdma-core libibverbs-dev librdmacm-dev ibverbs-providers ibverbs-utils rdmacm-utils
   
   # io_uring dependencies
   git clone git@github.com:axboe/liburing.git
   cd liburing
   ./configure
   make -j$(nproc)
   sudo make install
   sudo ldconfig
   
   # ebpf dependencies
   sudo apt install -y clang llvm libelf-dev libbpf-dev gcc-multilib
   sudo apt install -y linux-tools-common linux-tools-$(uname -r)
   
   #jemalloc dependencies
   sudo apt install libjemalloc-dev
   
   apt install libreadline-dev
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

* **Question**:

```shell
src/pdb_ebpf.c: In function ‘pdb_ebpf_init’:
src/pdb_ebpf.c:68:12: warning: implicit declaration of function ‘pdb_delta_bpf__open_and_load’ [-Wimplicit-function-declaration]
   68 |     skel = pdb_delta_bpf__open_and_load();
      |            ^~~~~~~~~~~~~~~~~~~~~~~~~~~~
src/pdb_ebpf.c:68:10: warning: assignment to ‘struct pdb_delta_bpf *’ from ‘int’ makes pointer from integer without a cast [-Wint-conversion]
   68 |     skel = pdb_delta_bpf__open_and_load();
      |          ^
src/pdb_ebpf.c:82:9: error: dereferencing pointer to incomplete type ‘struct pdb_delta_bpf’
   82 |     skel->links.pdb_hash_set_entry = bpf_program__attach_uprobe(
      |         ^~
make: *** [Makefile:59: obj/pdb_ebpf.o] Error 1
root@iZ2zegnh2qxplklylfcxdbZ:/home/PatronusDB# ls -l src/pdb_delta.skel.h
-rw-r--r-- 1 root root 0 Apr  1 22:33 src/pdb_delta.skel.h
```



## Using PDB-CLI

PDB-CLI is PDB' command line interface. It is available in pdb source. You can start a PDB-server instance, and then, in another terminal try the following:

```shell
cd client
make
./pdb-cli [server ip] [server port]
```

# PDB Data Types

* **Array、HASH、RBtree**
* **Bitmap**：A high-performance, space-efficient data structure that leverages bit-level mapping to manage massive datasets with minimal memory footprint and near-instantaneous membership queries.
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

* **Full Sync**:  Leverages **RDMA** to transfer snapshots. Once the slave node comes online, the master node and slave nodes establish an RDMA connection through a multi-step handshake. Upon completion, the slave node directly accesses the master's memory to pull the full dataset.当配置文件中is_rdma的值为1时，全量同步采用rdma；配置文件中is_rdma的值为0时，全量同步采用sendfile

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
    
    // vmware paramenter 
    rdma_qp 4
    rdma_internal_chunks 64
    rdma_num_chunks 1024
    rdma_read_depth 128
    
    // erdma paramenter
    rdma_qp 4
    rdma_internal_chunks 128
    rdma_num_chunks 1024
    rdma_read_depth 128
    ```

# Performance Benchmarking

* **echo**
  
  |         |   redis   | PatronusDB |
  | :-----: | :-------: | :--------: |
  | **QPS** | 2,767,567 | 4,483,902  |
  
* **QPS** of different network framworks and mem
  
  * **Reactor**
  
  | 内存模型 | RBTree | Hash | Array | Set | Sorted set | Bitmap |
  |  :--:  | :--:  | :--: |  :--:  |  :--:  |  :--:  |  :--:  |
  | malloc | 417,536 | 515,198 | 32258 | 587,889 | 449,842 | 865,051 |
  | mempool | 519,750 | 694,444 | 34482 | 729,394 | 453,103 | 821,692 |
  | jemalloc | 549,450 | 700,770 | 30303 | 751,879 | 467,508 | 815,660 |
  
  
  * **Ntyco**
  
  | 内存模型 | RBTree | Hash | Array | Set | Sorted set | Bitmap |
  |  :--:  | :--:  | :--: |  :--:  |  :--:  |  :--:  |  :--:  |
  | malloc | 170,823 | 237,812 | 31250 | 220994 | 144571 | 214,546 |
  | mempool | 171,644 | 239,635 | 32258 | 308261 | 131856 | 222,667 |
  | jemalloc | 151,308 | 311,817 | 33333 | 229147 | 142877 | 263,019 |
  
  * **io_uring**
  
  | 内存模型 | RBTree | Hash | Array | Set | Sorted set | Bitmap |
  |  :--:  | :--:  | :--: |  :--:  |  :--:  |  :--:  |  :--:  |
  | malloc | 674763 | 884,173 | 27777 | 977517 | 419111 | 929368 |
  | mempool | 695894 | 832,639 | 28571 | 1233045 |   533049   | 1101321 |
  | jemalloc | 675219 | 916,590 | 28571 | 826446  | 614628 | 831255 |
  
* **Persistence**

  1. **RDB** :

     load time: 107,706,534 B    ----   1389ms（including rebuilding time）
	   
     |         |  1000   |   1w    |   10w    |   100w   |
     | :-----: | :-----: | :-----: | :------: | :------: |
     | **QPS** | 928,505 | 995,024 | 1157,407 | 1275,510 |
  
  2. **AOF**：
  
      |   PDB   |   echo    |  no aof   |   aof   | aof(io_uring) |
      | :-----: | :-------: | :-------: | :-----: | :-----------: |
      | **QPS** | 4,483,902 | 1,387,604 | 254,841 |    989,119    |

      |  Redis  |   echo    | redis aof | redis no aof |
      | :-----: | :-------: | :-------: | :----------: |
      | **QPS** | 2,767,567 |  318,522  |   497,636    |
  
* **Replication**

  - **RDB replication(VMware)**

    | Data size |  500M  |   1G   |   2G    |
    | :-------: | :----: | :----: | :-----: |
    | sendfile  | 5867ms | 8959ms | 16912ms |
    |   rdma    | 2089ms | 3510ms | 9658ms  |

    | Data size | 500M  |   1G   |   2G   |
    | :-------: | :---: | :----: | :----: |
    | sendfile  | 7.89% | 38.61% | 27.58% |
    |   rdma    | 0.12% | 0.06%  | 0.03%  |

  - **RDB replication(eRDMA)**

    |  Delay   | 500M  |  1G   |   2G   |
    | :------: | :---: | :---: | :----: |
    | sendfile | 482ms | 924ms | 1820ms |
    |   rdma   | 242ms | 512ms | 1050ms |

    |   CPU    | 500M  |  1G   |  2G   |
    | :------: | :---: | :---: | :---: |
    | sendfile | 5.16% | 5.35% | 5.70% |
    |   rdma   | 0.02% | 0.01% | 0.01% |
  
  
    * **<font color="red">TODO</font>**: 不支持拉取2G以上数据，考虑采用“边序列化、边传输”的方案：
      *  Master 开始遍历数据结构，将键值对序列化并写入 Buffer 0
      *  当 Buffer 0 写满时，遍历操作暂停（记录下当前的游标 Cursor）。
      *  Master 通知 Slave：“Buffer 0 满了，来拉取”。
      *  Slave 发起 RDMA READ 拉取 Buffer 0。
      *  重叠期：在 Slave 拉取 Buffer 0 的同时，Master 根据刚才记录的游标，恢复遍历，继续把后续的数据序列化进 Buffer 1。
      *  循环交替，直到所有内存结构遍历完毕，写入一个 PDB_OPCODE_EOF。
  
  
    * **AOF replicaition**
  
      
  
      | pdb  | aof replication | ebpf | no replication |
      | :--: | :-------------: | :--: | :------------: |
      | QPS  |     809279      |      |   1,921,844    |

      | redis | aof replication | no aof replication |
      | :---: | :-------------: | :----------------: |
      |  QPS  |    1,205,542    |     1,675,414      |
  


* **MSET**

	| Batch/Pipeline |   1000    |   5000    |   10000   |
  | :------------: | :-------: | :-------: | :-------: |
  |      MSET      | 2,008,032 | 2,036,659 | 2,096,436 |
  | Redis Pipeline | 1,838,235 | 1,964,636 | 1,949,888 |




* **memery**

  1. **Test Method**(./client/src/pdb_testcase_pipeline_set.c): The test begins by inserting two items and deleting one per loop for 1M iterations to measure the **peak value**. This is followed by a phase of inserting one and deleting two until complete clearance, where the **valley value** is recorded.

  2. **Test Result** (malloc): 

     * **htop**: 

       |       | init  | peak  | valley |
       | :---: | :---: | :---: | :----: |
       | value | 1.84G | 2.13G | 2.13G  |

     * **Valgrind Massif**: 
     
        |  |  total(B)   | useful-heap(B) | extra-heap(B) |
        |  :--:  | :--:  |  :--:  |  :--:  |
        | peak value | 307,218,912 | 207,968,907 | 99,250,005 |
        | valley value | 41,159,224 |   39,656,975   | 1,502,249 |
     
    3. **Test Result**(jemalloc): 
    
       * **htop**: 
  
         |       | init  | peak  | valley |
         | :---: | :---: | :---: | :----: |
         | value | 1.81G | 2.02G | 2.02G  |
       
       - **Valgrind Massif**: 
  
            |              |  total(B)   | useful-heap(B) | extra-heap(B) |
            | :----------: | :---------: | :------------: | :-----------: |
            |  peak value  | 221,218,336 |  156,460,703   |  64,757,633   |
            | valley value | 40,122,536  |   38,878,873   |   1,243,663   |
       
    4. **Test Result**(mempool):
    
       * **htop**:
       
         |       | init  | peak  | valley |
         | :---: | :---: | :---: | :----: |
         | value | 1.69G | 1.88G | 1.88G  |
       
       * **Valgrind Massif**:
       
         |              |  total(B)   | useful-heap(B) | extra-heap(B) |
         | :----------: | :---------: | :------------: | :-----------: |
         |  peak value  | 230,879,608 |  230,837,899   |    41,709     |
         | valley value | 230,879,568 |  230,837,869   |    41,699     |
