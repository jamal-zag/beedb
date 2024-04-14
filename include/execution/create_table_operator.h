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
#include <database.h>
#include <string>
#include <table/table.h>
#include <table/tuple.h>
#include <table/type.h>
#include <tuple>
#include <vector>

namespace beedb::execution
{
/**
 * Creates and persists a new table.
 */
class CreateTableOperator final : public OperatorInterface
{
  public:
    CreateTableOperator(Database &database, concurrency::Transaction *transaction, table::Schema &&schema_to_create);

    ~CreateTableOperator() override = default;

    void open() override{};
    util::optional<table::Tuple> next() override;
    void close() override{};

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    };

    [[nodiscard]] bool yields_data() const override
    {
        return false;
    }

  private:
    Database &_database;
    const table::Schema _schema;
    const table::Schema _schema_to_create;
};
} // namespace beedb::execution
