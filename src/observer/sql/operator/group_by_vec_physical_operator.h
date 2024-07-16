/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"
#include "sql/expr/aggregate_state.h"
#include "sql/expr/expression_tuple.h"

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions)
      : group_by_exprs_(std::move(group_by_exprs_)), aggregate_expressions_(std::move(expressions))
  {
    hash_table_ = new StandardAggregateHashTable(aggregate_expressions_);
  };

  ~GroupByVecPhysicalOperator() override { delete hash_table_; }

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override;

private:
  std::vector<std::unique_ptr<Expression>> group_by_exprs_;
  std::vector<Expression *>                aggregate_expressions_;
  Chunk                                    chunk_;
  StandardAggregateHashTable              *hash_table_;
};
