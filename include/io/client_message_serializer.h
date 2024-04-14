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
#include "query_result_serializer.h"
#include <nlohmann/json.hpp>
#include <plan/logical/node/node_interface.h>
#include <string>

namespace beedb::io
{
class ClientMessageSerializer final : public ExecutionCallback
{
  public:
    ClientMessageSerializer() = default;
    ~ClientMessageSerializer() override = default;

    void on_schema(const table::Schema &schema) override
    {
        _query_result_serializer.serialize(schema);
    }

    void on_tuple(const table::Tuple &tuple) override
    {
        _query_result_serializer.serialize(tuple);
    }

    void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &plan) override
    {
        _query_plan = plan->to_json();
    }

    [[nodiscard]] const QueryResultSerializer &query_result() const
    {
        return _query_result_serializer;
    }
    [[nodiscard]] QueryResultSerializer &query_result()
    {
        return _query_result_serializer;
    }

    [[nodiscard]] const nlohmann::json &query_plan() const
    {
        return _query_plan;
    }

  private:
    QueryResultSerializer _query_result_serializer;
    nlohmann::json _query_plan;
};
} // namespace beedb::io