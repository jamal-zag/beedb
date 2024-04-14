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

#include "schema.h"
#include "table.h"
#include <expression/operation.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>

namespace beedb::plan::logical
{
class NodeInterface
{
  public:
    explicit NodeInterface(std::string &&name) : _name(std::move(name))
    {
    }
    virtual ~NodeInterface() = default;

    [[nodiscard]] virtual bool is_unary() const
    {
        return false;
    }
    [[nodiscard]] virtual bool is_binary() const
    {
        return false;
    }

    [[nodiscard]] const std::string &name() const
    {
        return _name;
    }
    virtual const Schema &check_and_emit_schema(TableMap &tables) = 0;
    [[nodiscard]] virtual const Schema &schema() const = 0;

    [[nodiscard]] virtual nlohmann::json to_json() const
    {
        auto json = nlohmann::json{};
        json["name"] = _name;

        std::stringstream schema_stream;
        for (auto i = 0u; i < schema().size(); ++i)
        {
            const auto &term = schema()[i];
            if (i > 0)
            {
                schema_stream << ",";
            }
            schema_stream << static_cast<std::string>(term);
        }
        schema_stream << std::flush;
        json["output"] = schema_stream.str();

        return json;
    }

  private:
    std::string _name;
};

class UnaryNode : public NodeInterface
{
  public:
    explicit UnaryNode(std::string &&name) : NodeInterface(std::move(name))
    {
    }
    ~UnaryNode() override = default;

    [[nodiscard]] bool is_unary() const override
    {
        return true;
    }

    void child(std::unique_ptr<NodeInterface> &&child)
    {
        _child = std::move(child);
    }
    [[nodiscard]] const std::unique_ptr<NodeInterface> &child() const
    {
        return _child;
    }

    [[nodiscard]] std::unique_ptr<NodeInterface> &child()
    {
        return _child;
    }

    [[nodiscard]] nlohmann::json to_json() const override
    {
        auto json = NodeInterface::to_json();
        json["childs"][0] = _child->to_json();
        return json;
    }

  private:
    std::unique_ptr<NodeInterface> _child{nullptr};
};

class BinaryNode : public NodeInterface
{
  public:
    explicit BinaryNode(std::string &&name) : NodeInterface(std::move(name))
    {
    }
    ~BinaryNode() override = default;

    [[nodiscard]] bool is_binary() const override
    {
        return true;
    }

    void left_child(std::unique_ptr<NodeInterface> &&child)
    {
        _left_child = std::move(child);
    }
    [[nodiscard]] const std::unique_ptr<NodeInterface> &left_child() const
    {
        return _left_child;
    }

    [[nodiscard]] std::unique_ptr<NodeInterface> &left_child()
    {
        return _left_child;
    }

    void right_child(std::unique_ptr<NodeInterface> &&child)
    {
        _right_child = std::move(child);
    }
    [[nodiscard]] const std::unique_ptr<NodeInterface> &right_child() const
    {
        return _right_child;
    }

    [[nodiscard]] std::unique_ptr<NodeInterface> &right_child()
    {
        return _right_child;
    }

    [[nodiscard]] nlohmann::json to_json() const override
    {
        auto json = NodeInterface::to_json();
        json["childs"][0] = _left_child->to_json();
        json["childs"][1] = _right_child->to_json();
        return json;
    }

  private:
    std::unique_ptr<NodeInterface> _left_child{nullptr};
    std::unique_ptr<NodeInterface> _right_child{nullptr};
};

class NotSchematizedNode : public NodeInterface
{
  public:
    explicit NotSchematizedNode(std::string &&name) : NodeInterface(std::move(name))
    {
    }
    ~NotSchematizedNode() override = default;

    const Schema &check_and_emit_schema(TableMap &) override
    {
        return _schema;
    }
    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }

  private:
    Schema _schema;
};
} // namespace beedb::plan::logical