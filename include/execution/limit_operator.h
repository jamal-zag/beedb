/*------------------------------------------------------------------------------*
 * Architecture & Implementation of DBMS                                        *
 *------------------------------------------------------------------------------*
 * Copyright 2022 Databases and Information Systems Group TU Dortmund           *
 * Visit us at                                                                  *
 *             http://dbis.cs.tu-dortmund.de/cms/en/home/                       *
 *                                                                              *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS      *
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL      *
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR         *
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,        *
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR        *
 * OTHER DEALINGS IN THE SOFTWARE.                                              *
 *                                                                              *
 * Authors:                                                                     *
 *          Maximilian Berens   <maximilian.berens@tu-dortmund.de>              *
 *          Roland Kühn         <roland.kuehn@cs.tu-dortmund.de>                *
 *          Jan Mühlig          <jan.muehlig@tu-dortmund.de>                    *
 *------------------------------------------------------------------------------*
 */

#pragma once

#include "unary_operator.h"
#include <cstdint>
#include <table/schema.h>
#include <table/tuple.h>
#include <vector>

namespace beedb::execution
{
/**
 * Limits the output result.
 */
class LimitOperator final : public UnaryOperator
{
  public:
    LimitOperator(concurrency::Transaction *transaction, const table::Schema &schema, const std::uint64_t limit,
                  const std::uint64_t offset);

    ~LimitOperator() override = default;

    void open() override;
    util::optional<table::Tuple> next() override;
    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    };

  private:
    const table::Schema &_schema;
    const std::uint64_t _limit;
    const std::uint64_t _offset;
    bool _has_skipped = false;
    std::uint64_t _count = 0;
};
} // namespace beedb::execution
