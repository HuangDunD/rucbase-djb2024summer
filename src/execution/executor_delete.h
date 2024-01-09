/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        // 对于rids_中的每一个Rid
        // 一方面，要从fh_调用delete_record删除记录
        // 另一方面，要从每个索引里删除这条记录对应的项delete_entry
        // 1. 从TabMeta tab_获取所有的索引列，从sm_manager_拿到所有的索引句柄
        int ih_num = tab_.indexes.size();
        std::vector<IxIndexHandle*> ihs(ih_num);
        for(int i=0;i<ih_num;i++){
            ihs[i] = (sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_,tab_.indexes[i].cols))).get();
        }
        // 2.遍历每一个待删除的rid，进行对应的删除操作
        // TODO:是否需要检查conds？
        int rid_num = rids_.size();
        for(int i=0;i<rid_num;i++){
            auto rec = fh_->get_record(rids_[i],context_);
            RmRecord deleted_rec = RmRecord(rec->size);//lab4
            memcpy(deleted_rec.data,rec->data,rec->size);

            fh_->delete_record(rids_[i],context_); // 包含锁检查，放在index前面
            // TODO: delete mark?
            
            for(int j=0;j<ih_num;j++){
                IndexMeta index_meta = tab_.indexes[j];
                // 按顺序拼接多级索引各个列的值，得到key
                char* key = new char[index_meta.col_tot_len+1];
                key[0] = '\0';
                int curlen = 0;
                for(int k=0;k<index_meta.col_num;k++){
                    // strncat(key,rec->data+index_meta.cols[k].offset,index_meta.cols[k].len);
                    memcpy(key+curlen,rec->data+index_meta.cols[k].offset,index_meta.cols[k].len);
                    curlen += index_meta.cols[k].len;
                    key[curlen] = '\0';
                }
                // 调用delete_entry删除该key
                ihs[j]->delete_entry(key,context_->txn_);
            }

            // lab4 modify write_set
            WriteRecord* write_rec = new WriteRecord(WType::DELETE_TUPLE,tab_name_,rids_[i],deleted_rec);
            context_->txn_->append_write_record(write_rec);
        }
        // TODO: return what
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};