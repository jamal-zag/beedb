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

#include "binary_operator.h"
#include <database.h>
#include <index/index_interface.h>
#include <memory>
#include <string>
#include <table/schema.h>

namespace beedb::execution
{
/**
 * Fills an existing index with values.
 * May have two children: One operator creating the index (left)
 * which is called once and one operator (right) which provides the
 * data for the index.
 */
class BuildIndexOperator final : public BinaryOperator
{
  public:
    BuildIndexOperator(Database &database, concurrency::Transaction *transaction, const std::string &table_name,
                       const table::Schema::ColumnIndexType column_index, const std::string &index_name);
    ~BuildIndexOperator() override = default;

    void open() override;
    util::optional<table::Tuple> next() override;
    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    }

    void create_index_operator(std::unique_ptr<OperatorInterface> op)
    {
        this->left_child(std::move(op));
    }
    void data_operator(std::unique_ptr<OperatorInterface> op)
    {
        this->right_child(std::move(op));
    }

    [[nodiscard]] bool yields_data() const override
    {
        return false;
    }

  private:
    const table::Schema _schema;
    Database &_database;
    const std::string _table_name;
    const std::uint32_t _column_index;
    const std::string _index_name;
};
} // namespace beedb::execution
