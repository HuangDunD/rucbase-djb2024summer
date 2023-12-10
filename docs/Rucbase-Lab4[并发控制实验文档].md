# Rucbase并发控制实验指南


Rucbase并发控制模块采用的是基于封锁的并发控制协议，要求事务达到可串行化隔离级别。在本实验中，你需要实现事务管理器、锁管理器，并使用事务管理器和锁管理器提供的相关接口保证事务正确地并发执行。

## 声明

<<<<<<< HEAD
本实验修改了如下两部分代码：

### 修改一：

本实验修改了**src/execution/executor_seq_scan.h**中的代码，在pull代码之后，首先需要检查该文件中的代码是否修改成功，如果未成功，需要手动进行修改，修改如下：

原代码：

```c++
auto rec = fh_->get_record(rid_, context_);  // TableHeap->GetTuple() 当前扫描到的记录
// lab3 task2 todo
// 利用eval_conds判断是否当前记录(rec.get())满足谓词条件
// 满足则中止循环
// lab3 task2 todo end
```

修改后的代码：

```c++
try {

`TransactionManager`类的接口和重要成员变量如下：

```cpp
class TransactionManager{
public:
    static std::unordered_map<txn_id_t, Transaction *> txn_map;
<<<<<<< HEAD
    Transaction *GetTransaction(txn_id_t txn_id);

    Transaction * Begin(Transaction * txn, LogManager *log_manager);
    void Commit(Transaction * txn, LogManager *log_manager);
    void Abort(Transaction * txn, LogManager *log_manager);
=======

    Transaction * begin(Transaction * txn, LogManager *log_manager);
    void commit(Transaction * txn, LogManager *log_manager);
    void abort(Transaction * txn, LogManager *log_manager);
>>>>>>> upstream/main
};
```

其中，本实验提供以下辅助接口/成员变量：

- 静态成员变量`txn_map`：维护全局事务映射表

<<<<<<< HEAD
- `Transaction *GetTransaction(txn_id_t txn_id);`
=======
- `Transaction *get_transaction(txn_id_t txn_id);`
>>>>>>> upstream/main
  
  根据事务ID获取事务指针

你需要完成`src/transaction/transaction_manager.cpp`文件中的以下接口：

<<<<<<< HEAD
- `Begin(Transaction*, LogManager*)`：该接口提供事务的开始方法。
  
  **提示**：如果是新事务，需要创建一个`Transaction`对象，并把该对象的指针加入到全局事务表中。

- `Commit(Transaction*, LogManager*)`：该接口提供事务的提交方法。
  
  **提示**：如果并发控制算法需要申请和释放锁，那么你需要在提交阶段完成锁的释放。

- `Abort(Transaction*, LogManager*)`：该接口提供事务的终止方法。
=======
- `begin(Transaction*, LogManager*)`：该接口提供事务的开始方法。
  
  **提示**：如果是新事务，需要创建一个`Transaction`对象，并把该对象的指针加入到全局事务表中。

- `commit(Transaction*, LogManager*)`：该接口提供事务的提交方法。
  
  **提示**：如果并发控制算法需要申请和释放锁，那么你需要在提交阶段完成锁的释放。

- `abort(Transaction*, LogManager*)`：该接口提供事务的终止方法。
>>>>>>> upstream/main
  
  在事务的终止方法中，你需要对需要对事务的所有写操作进行撤销，事务的写操作都存储在Transaction类的write_set_中，因此，你可以通过修改存储层或执行层的相关代码来维护write_set_，并在终止阶段遍历write_set_，撤销所有的写操作。
  
  **提示**：需要对事务的所有写操作进行撤销，如果并发控制算法需要申请和释放锁，那么你需要在终止阶段完成锁的释放。
  
  **思考**：在回滚删除操作的时候，是否必须插入到record的原位置，如果没有插入到原位置，会存在哪些问题？

### 测试点及分数

```bash
<<<<<<< HEAD
cd build
make txn_manager_test  # 30分
./bin/txn_manager_test
```

## 实验二 锁管理器实验（40分）

在本实验中，你需要实现锁管理器，即`Lockanager`类。

相关数据结构包括`LockDataId`、`TransactionAbortException`、`LockRequest`、`LockRequestQueue`等，位于`txn_def.h`和`Lockanager.h`文件中。

本实验要求完成锁管理器`LockManager`类。
=======
cd src/test/transaction
python transaction_test.py
```

本测试包含两个测试点，分别对事务的提交和回滚进行测试，测试点分数设置如下：

|  **测试点**  |  **测试内容**  |  **分数**  |
| ------------- | ----------------- | ------------- |
|  `commit_test`  |  事务的开始与提交  |  20  |
|  `abort_test`  |  事务的开始与回滚  |  20  |


## 实验二 并发控制实验（60分）

在本实验中，你需要实现锁管理器，即`Lockanager`类，并调用锁管理器的相关接口实现两阶段封锁并发控制算法，死锁的解决办法要求为no-wait算法。

### 任务1：锁管理器实现
首先要求完成锁管理器`LockManager`类。相关数据结构包括`LockDataId`、`TransactionAbortException`、`LockRequest`、`LockRequestQueue`等，位于`txn_def.h`和`Lockanager.h`文件中。
>>>>>>> upstream/main

`LockManager`类的接口和重要成员变量如下：

```cpp
class LockManager {
public:
    // 行级锁
<<<<<<< HEAD
    bool LockSharedOnRecord(Transaction *txn, const Rid &rid, int tab_fd);
    bool LockExclusiveOnRecord(Transaction *txn, const Rid &rid, int tab_fd);
    // 表级锁
    bool LockSharedOnTable(Transaction *txn, int tab_fd);
    bool LockExclusiveOnTable(Transaction *txn, int tab_fd);
    // 意向锁
    bool LockISOnTable(Transaction *txn, int tab_fd);
    bool LockIXOnTable(Transaction *txn, int tab_fd);
    // 解锁
    bool Unlock(Transaction *txn, LockDataId lock_data_id);
=======
    bool lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd);
    bool lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd);
    // 表级锁
    bool lock_shared_on_table(Transaction *txn, int tab_fd);
    bool lock_exclusive_on_table(Transaction *txn, int tab_fd);
    // 意向锁
    bool lock_IS_on_table(Transaction *txn, int tab_fd);
    bool lock_IX_on_table(Transaction *txn, int tab_fd);
    // 解锁
    bool unlock(Transaction *txn, LockDataId lock_data_id);
>>>>>>> upstream/main
private:
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;
};
```

其中，本实验提供辅助成员变量：

- `lock_table`：锁表，维护系统当前状态下的所有锁

在本实验中，你需要完成锁管理器的加锁、解锁和死锁预防功能。锁管理器提供了行级读写锁、表级读写锁、表级意向锁，相关的数据结构在项目结构文档中进行了介绍。

<<<<<<< HEAD
### 任务：加锁解锁操作

在完成本任务之前，可以先画出锁相容矩阵，再进行加锁解锁流程的梳理；同时，在当前任务中，锁的申请和释放不需要考虑死锁问题。
=======
在完成本任务之前，可以先画出锁相容矩阵，再进行加锁解锁流程的梳理。在申请锁时，需要考虑死锁问题，本实验要求通过**no-wait**算法来完成死锁预防。
>>>>>>> upstream/main

你需要完成`src/transaction/concurrency/lock_manager.cpp`文件中的以下接口：

#### （1）行级锁加锁

<<<<<<< HEAD
- `LockSharedOnRecord(Transaction *, const Rid, int)`：用于申请指定元组上的读锁。
  
  事务要对表中的某个指定元组申请行级读锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

- `LockExclusiveOnRecord(Transaction *, const Rid, int)`：用于申请指定元组上的写锁。
=======
- `lock_shared_on_record(Transaction *, const Rid, int)`：用于申请指定元组上的读锁。
  
  事务要对表中的某个指定元组申请行级读锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

- `lock_exclusive_on_record(Transaction *, const Rid, int)`：用于申请指定元组上的写锁。
>>>>>>> upstream/main
  
  事务要对表中的某个指定元组申请行级写锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

#### （2）表级锁加锁

<<<<<<< HEAD
- `LockSharedOnTable(Transaction *txn, int tab_fd)`：用于申请指定表上的读锁。
  
  事务要对表中的某个指定元组申请表级读锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

- `LockExclusiveOnTable(Transaction *txn, int tab_fd)`：用于申请指定表上的写锁。
  
  事务要对表中的某个指定元组申请表级写锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

- `LockISOnTable(Transaction *txn, int tab_fd)`：用于申请指定表上的意向读锁。
  
  事务要对表中的某个指定元组申请表级意向读锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

- `LockIXOnTable(Transaction *txn, int tab_fd)`：用于申请指定表上的意向写锁。
=======
- `lock_shared_on_table(Transaction *txn, int tab_fd)`：用于申请指定表上的读锁。
  
  事务要对表中的某个指定元组申请表级读锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

- `lock_exclusive_on_table(Transaction *txn, int tab_fd)`：用于申请指定表上的写锁。
  
  事务要对表中的某个指定元组申请表级写锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

- `lock_IS_on_table(Transaction *txn, int tab_fd)`：用于申请指定表上的意向读锁。
  
  事务要对表中的某个指定元组申请表级意向读锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

- `lock_IX_on_table(Transaction *txn, int tab_fd)`：用于申请指定表上的意向写锁。
>>>>>>> upstream/main
  
  事务要对表中的某个指定元组申请表级意向写锁，该操作需要被阻塞直到申请成功或失败，如果申请成功则返回true，否则返回false。

#### （3）解锁

<<<<<<< HEAD
- `Unlock(Transaction *, LockDataId)`：解锁操作。
- 需要更新锁表，如果解锁成功则返回true，否则返回false。

### 测试点及分数

```bash
cd build
make lock_manager_test  # 40分
./bin/lock_manager_test
```

## 实验三 并发控制实验（30分）

在本实验中，你需要在执行层和存储层的相关接口中，添加加锁和解锁操作，并且在适当的地方进行异常处理。
本实验目前要求支持可串行化隔离级别。
=======
- `unlock(Transaction *, LockDataId)`：解锁操作。
- 需要更新锁表，如果解锁成功则返回true，否则返回false。

### 任务2：两阶段封锁协议的实现

你需要调用任务一中锁管理器通过的加锁解锁接口，在合适的地方申请行级锁和意向锁，并在合适的地方释放事务的锁。
>>>>>>> upstream/main

你需要修改`src/record/rm_file_handle.cpp`中的以下接口：

- `get_record(const Rid, Context *)`：在该接口中，你需要申请对应元组上的行级锁。
- `delete_record(const Rid, Context *)`：在该接口中，你需要申请对应元组上的行级锁。
- `update_record(const Rid, Context *)`：在该接口中，你需要申请对应元组上的行级锁。
<<<<<<< HEAD
- `get_record(const Rid, Context *)`：在该接口中，你需要申请对应元组上的行级锁。

同时还需要修改`src/system/sm_manager.cpp`和`executor_manager.cpp`中的相关接口，在合适的地方申请行级锁和意向锁。主要包括以下接口：
=======

同时还需要修改`src/system/sm_manager.cpp`和`executor_manager.cpp`中的相关接口，在合适的地方申请行级锁和意向锁。主要涉及以下接口：
>>>>>>> upstream/main

- `create_table(const std::string, const std::vector<ColDef>, Context *)`
- `drop_table(const std::string, Context *)`
- `create_index(const std::string, const std::string, Context *)`
- `drop_index(const std::string, const std::string, Context *)`
- `insert_into(const std::string, std::vector<Value>, Context *)`
- `delete_from(const std::string, std::vector<Condition>, Context *)`
- `update_set(const std::string, std::vector<SetClause>, std::vector<Condition>, Context *)`

<<<<<<< HEAD
**提示**：对于加锁和解锁过程中抛出的异常，你需要在合适的地方进行处理。
=======
**提示**：除了事务锁的申请，还需要考虑`txn_map`等共享数据结构。
>>>>>>> upstream/main

### 测试点及分数

```bash
<<<<<<< HEAD
cd build
make concurrency_test  # 30分
./bin/concurrency_test
```

=======
cd src/test/concurrency
python concurrency_test.py
```

本测试包含六个测试点考虑，对应不同的数据异常：

| **测试点**     | **测试内容**      | **分数**      |
| ------------- | ----------------- | ------------- |
| `concurrency_read_test` | 并发读取  | 10 |
| `dirty_write_test` | 脏写    | 10 |
| `dirty_read_test` | 脏读    | 10 |
| `lost_update_test` | 丢失更新    | 10 |
| `unrepeatable_read_test` | 不可重复读    | 10 |
| `unrepeatable_read_test_hard` | 不可重复读    | 10 |

## 附加实验（40分）

在上述两个实验中，均未涉及到索引操作，在附加实验中，需要在表上存在索引时，依然能够完成事务的提交、回滚，保证事务的可串行化。

### 任务1: 事务的提交与回滚（20分）

在本任务中，需要实现如下功能：
- 实现lab3中没有要求实现index_scan算子，支持索引扫描；
- 支持联合索引的创建，即如下语法：create index orders (o_w_id, o_d_id, o_id);
- 在插入删除数据时，需要同步对索引进行更新。

#### 测试点及分数

```bash
cd src/test/transaction
python transaction_test_bonus.py
```

本测试包含两个测试点，分别对事务的提交和回滚进行测试，测试点分数设置如下：

|  **测试点**  |  **测试内容**  |  **分数**  |
| ------------- | ----------------- | ------------- |
|  `commit_index_test`  |  事务的开始与提交  |  20  |
|  `abort_index_test`  |  事务的开始与回滚  |  20  |

### 任务2：幻读数据异常（20分）
在实验二中，没有对幻读数据异常进行测试，在本任务中，你需要规避幻读数据异常。可以通过表锁来规避幻读数据异常，但是会降低系统的并发度，因此，最合理的做法是通过间隙锁来规避幻读数据异常。

#### 测试点及分数

```bash
cd src/test/concurrency
python concurrency_test_bonus.py
```

本测试中包含四个测试点，每个分数点为5分，如果通过表锁的方式规避幻读数据异常，则最终得分为`(通过测试点数量)*5/2`，如果通过间隙锁的方式规避幻读数据异常，则最终得分为`(通过测试点数量)*5`

**提示**：当查询语句的条件符合索引扫描的条件时，系统会自动选择索引扫描，因此输出顺序是固定的，在幻读数据异常测试时，你的输出顺序需要和答案的输出顺序一致才可以得分。

>>>>>>> upstream/main
