/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/group_by_vec_physical_operator.h"

GroupByVecPhysicalOperator::GroupByVecPhysicalOperator(
    std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions)
    : group_by_exprs_(std::move(group_by_exprs)), flag(false)
{
  aggregate_expressions_ = std::move(expressions);
  hash_table_            = std::make_unique<StandardAggregateHashTable>(aggregate_expressions_);
  value_expressions_.reserve(aggregate_expressions_.size());

  for (size_t i = 0; i < aggregate_expressions_.size(); i++) {
    auto       *aggregate_expr = static_cast<AggregateExpr *>(aggregate_expressions_[i]);
    Expression *child_expr     = aggregate_expr->child().get();
    ASSERT(child_expr != nullptr, "aggregate expression must have a child expression");
    value_expressions_.emplace_back(child_expr);
  }
};

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "vectorized group by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = child.next(chunk_))) {
    // int   col_id = 0;
    // set group chunks
    Chunk group_chunks;
    for (size_t i = 0; i < group_by_exprs_.size(); i++) {
      Column col;
      group_by_exprs_[i]->get_column(chunk_, col);

      group_chunks.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), i);
      group_chunks.column_ptr(i)->append(col.data(), col.count());
    }
    // for (auto &group_expr : group_by_exprs_) {
    //   Column col;
    //   group_expr->get_column(chunk_, col);
    //   group_chunks.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id);
    //   group_chunks.column_ptr(col_id)->append(col.data(), col.count());
    //   col_id++;
    // }

    // set agg chunks
    Chunk aggrs_chunks;
    for (size_t i = 0; i < value_expressions_.size(); i++) {
      Column col;
      value_expressions_[i]->get_column(chunk_, col);

      aggrs_chunks.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), i);
      aggrs_chunks.column_ptr(i)->append(col.data(), col.count());
    }

    // int   col_id = 0;
    // for (auto aggrs_expr : value_expressions_) {
    //   Column col;
    //   aggrs_expr->get_column(chunk_, col);
    //   aggrs_chunks.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id);
    //   aggrs_chunks.column_ptr(col_id)->append(col.data(), col.count());
    //   col_id++;
    // }
    rc = hash_table_->add_chunk(group_chunks, aggrs_chunks);

    if (OB_FAIL(rc)) {
      LOG_INFO("failed to add chunks. rc=%s", strrc(rc));
      return rc;
    }
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  flag = false;
  return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
  if (flag) {
    // flag means not the first time
    return RC::RECORD_EOF;
  }

  for (size_t i = 0; i < group_by_exprs_.size(); i++) {
    // Column col;
    // group_by_exprs_[i]->get_column(chunk_, col);
    // chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), i);
    chunk.add_column(make_unique<Column>(group_by_exprs_[i]->value_type(), group_by_exprs_[i]->value_length()), i);
  }

  // continue add to chunk
  for (size_t i = 0; i < value_expressions_.size(); i++) {
    // Column col;
    // value_expressions_[i]->get_column(chunk_, col);
    // chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), i + group_by_exprs_.size());
    chunk.add_column(make_unique<Column>(value_expressions_[i]->value_type(), value_expressions_[i]->value_length()),
        i + group_by_exprs_.size());
  }

  StandardAggregateHashTable::Scanner sc(hash_table_.get());
  sc.open_scan();
  sc.next(chunk);

  flag = true;
  return RC::SUCCESS;
}

RC GroupByVecPhysicalOperator::close()
{
  children_[0]->close();
  LOG_INFO("close vectorized group by operator");
  flag = false;
  return RC::SUCCESS;
}
