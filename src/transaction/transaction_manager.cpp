/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    if(!txn){
        txn = new Transaction(next_txn_id_);
        next_txn_id_++;
        // TODO
        // txn->set_txn_mode();
        txn->set_start_ts(next_timestamp_);
    }
    // 3. 把开始事务加入到全局事务表中
    txn_map[txn->get_transaction_id()] = txn;
    // 4. 返回当前事务指针
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    if(!txn){
        return;
    }
    // 1. 如果存在未提交的写操作，提交所有的写操作
    auto write_set = txn->get_write_set();
    while(!write_set->empty()){
        // TODO
        write_set->pop_front();
    }
    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    if(!lock_set->empty()){
        for(auto it=lock_set->begin();it!=lock_set->end();it++){
            lock_manager_->unlock(txn,*it);
        }
    }
    lock_set->clear();
    // 3. 释放事务相关资源， eg.锁集
    // TODO
    // 4. 把事务日志刷入磁盘中
    log_manager->flush_log_to_disk();
    // 5. 更新事务状态
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    if(!txn){
        return;
    }
    // 1. 回滚所有写操作
    auto write_set = txn->get_write_set();
    while(!write_set->empty()){
        WriteRecord * write_rec = write_set->back(); // 顺序应该从后往前
        Context* ctx = new Context(lock_manager_,log_manager,txn);
        switch(write_rec->GetWriteType()){
            case WType::INSERT_TUPLE:
                rollback_insert(write_rec->GetTableName(),write_rec->GetRid(),ctx);
                break;
            case WType::DELETE_TUPLE:
                rollback_delete(write_rec->GetTableName(),write_rec->GetRecord(),ctx);
                break;
            case WType::UPDATE_TUPLE:
                rollback_update(write_rec->GetTableName(),write_rec->GetRid(),write_rec->GetRecord(),ctx);
                break;
        }
        write_set->pop_back();
    }
    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    if(!lock_set->empty()){
        for(auto it = lock_set->begin();it!=lock_set->end();it++){
            lock_manager_->unlock(txn,*it);
        }
    }
    lock_set->clear();
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    txn->set_state(TransactionState::ABORTED);
}

void TransactionManager::rollback_insert(const std::string& tab_name, const Rid &rid_, Context *context_){
    // copied from executor_delete
    // 1. delete the index of inserted tuple
    RmFileHandle* fh_ = sm_manager_->fhs_.at(tab_name).get();
    TabMeta& tab_ = sm_manager_->db_.get_table(tab_name);
    size_t ih_num = tab_.indexes.size();

    std::vector<IxIndexHandle*> ihs(ih_num);
    for(int i=0;i<ih_num;i++){
        ihs[i] = (sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name,tab_.indexes[i].cols))).get();
    }

    auto rec = fh_->get_record(rid_,context_);
    for(size_t j=0;j<ih_num;j++){
        IndexMeta index_meta = tab_.indexes[j];
        // 按顺序拼接多级索引各个列的值，得到key
        char* key = new char[index_meta.col_tot_len+1];
        key[0] = '\0';
        int curlen = 0;
        for(int k=0;k<index_meta.col_num;k++){
            memcpy(key+curlen,rec->data+index_meta.cols[k].offset,index_meta.cols[k].len);
            curlen += index_meta.cols[k].len;
            key[curlen] = '\0';
        }
        // 调用delete_entry删除该key
        ihs[j]->delete_entry(key,context_->txn_);
    }
    // 2. delete the inserted tuple
    fh_->delete_record(rid_,context_);
}

void TransactionManager::rollback_delete(const std::string& tab_name, const RmRecord& rec ,Context *context_){
    // copied from executor_insert
    RmFileHandle* fh_ = sm_manager_->fhs_.at(tab_name).get();
    TabMeta& tab_ = sm_manager_->db_.get_table(tab_name);

    // Insert into record file
    Rid rid_ = fh_->insert_record(rec.data, context_);
        
    // Insert into index
    for(size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto& index = tab_.indexes[i];
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        char* key = new char[index.col_tot_len];
        int offset = 0;
        for(size_t i = 0; i < index.col_num; ++i) {
            memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        ih->insert_entry(key, rid_, context_->txn_);
    }
}

void TransactionManager::rollback_update(const std::string& tab_name, const Rid &rid_, const RmRecord& old_rec, Context *context_){
    // copied from executor_update
    RmFileHandle* fh_ = sm_manager_->fhs_.at(tab_name).get();
    TabMeta& tab_ = sm_manager_->db_.get_table(tab_name);

    // rid_:    current record,   should be deleted
    // old_rec: previous record,  should be inserted

    auto cur_rec = fh_->get_record(rid_,context_);

    int ih_num = tab_.indexes.size();
    std::vector<IxIndexHandle*> ihs(ih_num);
    for(int i=0;i<ih_num;i++){
        ihs[i] = (sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name,tab_.indexes[i].cols))).get();
    }

    // 更新索引项
    for(size_t j=0;j<tab_.indexes.size();j++){
        IndexMeta index_meta = tab_.indexes[j];
        // 按顺序拼接多级索引各个列的值，得到key
        char* cur_key = new char[index_meta.col_tot_len+1];
        char* old_key = new char[index_meta.col_tot_len+1];
        cur_key[0] = '\0';
        old_key[0] = '\0';
        int curlen = 0;
        for(int k=0;k<index_meta.col_num;k++){
            memcpy(old_key+curlen,old_rec.data+index_meta.cols[k].offset,index_meta.cols[k].len);
            memcpy(cur_key+curlen,cur_rec->data+index_meta.cols[k].offset,index_meta.cols[k].len);
            curlen += index_meta.cols[k].len;
            cur_key[curlen] = '\0';
            old_key[curlen] = '\0';
        }
        ihs[j]->delete_entry(cur_key,context_->txn_);
        ihs[j]->insert_entry(old_key,rid_,context_->txn_);
    }
    // 2.3 写新data
    fh_->update_record(rid_,old_rec.data,context_);
}