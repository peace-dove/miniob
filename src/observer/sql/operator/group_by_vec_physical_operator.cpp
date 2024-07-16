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

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());
  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = child.next(chunk_))) {
    Chunk aggrs_chunk_;
    for (size_t aggr_idx = 0; aggr_idx < aggregate_expressions_.size(); aggr_idx++) {
      auto              *aggregate_expr = static_cast<AggregateExpr *>(aggregate_expressions_[aggr_idx]);
      unique_ptr<Column> aggr_column    = std::make_unique<Column>();
      if (aggregate_expr->aggregate_type() == AggregateExpr::Type::SUM) {
        aggregate_expr->child()->get_column(chunk_, *aggr_column);
        aggrs_chunk_.add_column(std::move(aggr_column), aggr_idx);
      } else {
        ASSERT(false, "Not support aggregation type.");
      }
    }

    Chunk groups_chunk_;
    for (size_t group_idx = 0; group_idx < group_by_exprs_.size(); group_idx++) {
      unique_ptr<Column> group_by_column = std::make_unique<Column>();
      group_by_exprs_[group_idx]->get_column(chunk_, *group_by_column);
      groups_chunk_.add_column(std::move(group_by_column), group_idx);
    }

    hash_table_->add_chunk(groups_chunk_, aggrs_chunk_);
  }

  if (rc == RC::RECORD_EOF) {
    return RC::SUCCESS;
  }

  return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{

  if (group_by_exprs_.empty() && aggregate_expressions_.empty()) {
    return RC::RECORD_EOF;
  }

  for (size_t i = 0; i < group_by_exprs_.size(); i++) {
    chunk.add_column(make_unique<Column>(group_by_exprs_[i]->value_type(), group_by_exprs_[i]->value_length()), i);
  }

  for (size_t i = 0; i < aggregate_expressions_.size(); i++) {
    chunk.add_column(
        make_unique<Column>(aggregate_expressions_[i]->value_type(), aggregate_expressions_[i]->value_length()),
        i + group_by_exprs_.size());
  }

  StandardAggregateHashTable::Scanner scanner(hash_table_);
  scanner.open_scan();
  RC rc = scanner.next(chunk);
  group_by_exprs_.clear();
  aggregate_expressions_.clear();
  return rc;
}

RC GroupByVecPhysicalOperator::close()
{
  children_[0]->close();
  LOG_INFO("close group by vec operator");
  return RC::SUCCESS;
}
