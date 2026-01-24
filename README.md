# 使用说明

## 启动kv_store

### 启动master节点

```shell
./kv_store [mode] [port]
```

* mode
  * 1：使用 reactor 作为网络通信架构；
  * 2：使用 ntyco（协程）作为网络通信架构；
  * 3：使用 io_uring 作为网络通信架构。
* kv_store监听客户端连接的端口号

### 启动slave节点

```shell
./kv_store [mode] [port] [master_ip] [master_port]
```

* mode
  * 1：使用 reactor 作为网络通信架构；
  * 2：使用 ntyco（协程）作为网络通信架构；
  * 3：使用 io_uring 作为网络通信架构。
* kv_store监听客户端连接的端口号
* [master_ip] master节点的ip地址
* [master_port] master节点监听的端口号

## 发送命令格式

```shell
*{TOKENS_NUM}\r\n
${len_CMD}\r\n{CMD}\r\n
${len_KEY}\r\n{KEY}\r\n
[${len_VALUE}\r\n{VALUE}\r\n]
[...........................]
```

### RBTree

* SET

  ```shell
  *3\r\n
  $4\r\nRSET\r\n
  ${len_KEY}\r\n{KEY}\r\n
  ${len_VALUE}\r\n{VALUE}\r\n
  ```

* GET

  ```shell
  *2\r\n
  $4\r\nRGET\r\n
  ${len_KEY}\r\n{KEY}\r\n
  ```

* DEL

  ```shell
  *2\r\n
  $4\r\nRDEL\r\n
  ${len_KEY}\r\n{KEY}\r\n
  ```

* MOD

  ```shell
  *3\r\n
  $4\r\nRMOD\r\n
  ${len_KEY}\r\n{KEY}\r\n
  ${len_VALUE}\r\n{VALUE}\r\n
  ```

* MSET

  ```shell
  *{1 + KEY_NUM}\r\n
  $5\r\nRMSET\r\n
  ${len_KEY1}\r\n{KEY1}\r\n
  ${len_VALUE1}\r\n{VALUE1}\r\n
  ${len_KEY2}\r\n{KEY2}\r\n
  ${len_VALUE2}\r\n{VALUE2}\r\n
  ${len_KEY3}\r\n{KEY3}\r\n
  ${len_VALUE3}\r\n{VALUE3}\r\n
  .............................
  ```

* MMGET

  ```shell
  *{1 + KEY_NUM}\r\n
  $5\r\nRMGET\r\n
  ${len_KEY1}\r\n{KEY1}\r\n
  ${len_KEY2}\r\n{KEY2}\r\n
  ${len_KEY3}\r\n{KEY3}\r\n
  .............................
  ```

### Hash

CMD分别为:**HSET** \ **HGET** \ **HEXIST** \ **HMOD** \ **HDEL** \ **HMGET** \ **HMSET**

### 持久化

CMD为: **SAVE**

## 测试用例

### testcase_pipeline_set.c

```shell
gcc -o testcase_pipeline testcase_pipeline.c
./testcase_pipeline [server_ip] [server_port]
```

* **功能说明**：测试set和del功能，向服务器插入两条DAI{i} i和一条TAO{i} i，删除TAO{i}，循环100w次。
* **参数说明**：
* **测试结果**：Ubuntu 20.04，4核，4G

| Reactor\|内存模型 | QPS1(RBTree) | QPS2(Hash) |
|  ----  | ----  | ---- |
| malloc | 78771 | 60353 |
| mempool | 89774        | 73294 |
| jemalloc | 88689 | 64581 |

| NtyCo\|内存模型 | QPS1(RBTree) | QPS2(Hash) |
|  ----  | ----  | ---- |
| malloc | 90263 | 74975 |
| mempool | 110249 | 86710 |
| jemalloc | 111573 | 81521 |

| io_uring\|内存模型 | QPS1(RBTree) | QPS2(Hash) |
|  ----  | ----  | ---- |
| malloc | 135360 | 136060     |
| mempool | 133821 | 137318     |
| jemalloc | 132100 | 133209 |

### testcase_blog.c

* **功能说明**：用于测试大value

### testcase_mset.c



### testcasse_mget.c



### 测试主从同步
