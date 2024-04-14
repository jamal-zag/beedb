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

#include "operator_interface.h"
#include "tuple_buffer.h"
#include <table/schema.h>
#include <table/tuple.h>
#include <vector>

namespace beedb::execution
{
/**
 * Buffers tuples and pops the on next().
 */
class TupleBufferOperator final : public OperatorInterface
{
  public:
    TupleBufferOperator(concurrency::Transaction *transaction, const table::Schema &schema);
    ~TupleBufferOperator() override = default;

    void open() override
    {
    }

    util::optional<table::Tuple> next() override;

    void close() override
    {
    }

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    }

    void add(table::Tuple &tuple)
    {
        _tuple_buffer.add(std::move(tuple));
    }
    void add(std::vector<table::Tuple> &tuples)
    {
        _tuple_buffer.add(std::move(tuples));
    }

  private:
    const table::Schema _schema;
    TupleBuffer _tuple_buffer;
};
} // namespace beedb::execution