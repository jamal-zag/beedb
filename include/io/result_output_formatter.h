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

#include "execution_callback.h"
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <table/schema.h>
#include <table/tuple.h>
#include <util/text_table.h>
#include <vector>

namespace beedb::io
{
/**
 * Formats the result of a query, given by schema and set of tuples.
 */
class ResultOutputFormatter final : public ExecutionCallback
{
    friend std::ostream &operator<<(std::ostream &stream, const ResultOutputFormatter &result_output_formatter);

  public:
    ResultOutputFormatter() = default;
    ~ResultOutputFormatter() override = default;

    static ResultOutputFormatter from_serialized_data(const std::size_t count_tuples, const std::byte *data);

    void on_schema(const table::Schema &schema) override
    {
        header(schema);
    }
    void on_tuple(const table::Tuple &tuple) override
    {
        push_back(tuple);
    }

    void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &) override
    {
    }

    /**
     * Set the header for the output.
     *
     * @param schema Schema for the header.
     */
    void header(const table::Schema &schema);

    /**
     * Add a set of tuples to the result.
     * @param tuples Set of tuples.
     */
    void push_back(const std::vector<table::Tuple> &tuples);

    /**
     * Add a single tuple to the result.
     * @param tuple
     */
    void push_back(const table::Tuple &tuple);

    /**
     * Clear the formatter.
     */
    inline void clear()
    {
        _table.clear();
    }

    /**
     * @return True, when no tuple as added.
     */
    inline bool empty() const
    {
        return _table.empty();
    }

    /**
     * @return Number of added tuples.
     */
    inline std::size_t count() const
    {
        return _count_tuples;
    }

  private:
    util::TextTable _table;
    std::size_t _count_tuples = 0u;
};
} // namespace beedb::io