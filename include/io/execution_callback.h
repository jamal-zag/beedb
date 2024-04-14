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
#include <memory>
#include <table/schema.h>
#include <table/tuple.h>

namespace beedb::plan::logical
{
class NodeInterface;
}

namespace beedb::io
{
class ExecutionCallback
{
  public:
    ExecutionCallback() = default;
    virtual ~ExecutionCallback() = default;

    virtual void on_schema(const table::Schema &schema) = 0;
    virtual void on_tuple(const table::Tuple &tuple) = 0;
    virtual void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &plan) = 0;
};

class SilentExecutionCallback final : public ExecutionCallback
{
  public:
    SilentExecutionCallback() = default;
    ~SilentExecutionCallback() override = default;

    void on_schema(const table::Schema &) override
    {
    }
    void on_tuple(const table::Tuple &) override
    {
    }
    void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &) override
    {
    }
};

class FunctionalExecutionCallback final : public ExecutionCallback
{
  public:
    FunctionalExecutionCallback(
        std::function<void(const table::Schema &)> &&schema_callback,
        std::function<void(const table::Tuple &)> &&tuple_callback,
        std::function<void(const std::unique_ptr<plan::logical::NodeInterface> &)> &&plan_callback)
        : _schema_callback(std::move(schema_callback)), _tuple_callback(std::move(tuple_callback)),
          _plan_callback(std::move(plan_callback))
    {
    }

    FunctionalExecutionCallback(std::function<void(const table::Schema &)> &&schema_callback,
                                std::function<void(const table::Tuple &)> &&tuple_callback)
        : FunctionalExecutionCallback(std::move(schema_callback), std::move(tuple_callback),
                                      [](const std::unique_ptr<plan::logical::NodeInterface> &) {})
    {
    }

    ~FunctionalExecutionCallback() override = default;

    void on_schema(const table::Schema &schema) override
    {
        _schema_callback(schema);
    }

    void on_tuple(const table::Tuple &tuple) override
    {
        _tuple_callback(tuple);
    }

    void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &plan) override
    {
        _plan_callback(plan);
    }

  private:
    std::function<void(const table::Schema &)> _schema_callback;
    std::function<void(const table::Tuple &)> _tuple_callback;
    std::function<void(const std::unique_ptr<plan::logical::NodeInterface> &)> _plan_callback;
};
} // namespace beedb::io