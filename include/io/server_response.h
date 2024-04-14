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
#include "query_result_serializer.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace beedb::io
{

class ServerResponse
{
  public:
    enum Type : std::uint8_t
    {
        Empty,
        Error,
        AnalyticalResult,
        TransactionalResult,
        PlanExplanation,
        ServerClosed
    };

    ServerResponse(const Type type, const std::uint64_t execution_time_in_ms)
        : _type(type), _execution_time_in_ms(execution_time_in_ms)
    {
    }

    virtual ~ServerResponse() = default;

    [[nodiscard]] Type type() const
    {
        return _type;
    }
    [[nodiscard]] std::uint64_t execution_time_in_ms() const
    {
        return _execution_time_in_ms;
    }

  private:
    const Type _type;
    const std::uint64_t _execution_time_in_ms;
};

class EmptyResponse final : public ServerResponse
{
  public:
    EmptyResponse() : ServerResponse(Type::Empty, 0u)
    {
    }
    ~EmptyResponse() override = default;

    static std::string build()
    {
        std::string response = std::string(sizeof(EmptyResponse), '\0');
        new (response.data()) EmptyResponse();
        return response;
    }
};

class ErrorResponse final : public ServerResponse
{
  public:
    ErrorResponse() : ServerResponse(Type::Error, 0u)
    {
    }

    ~ErrorResponse() override = default;

    static std::string build(std::string_view &&error_message)
    {
        std::string response = std::string(sizeof(ErrorResponse) + error_message.length(), '\0');
        new (response.data()) ErrorResponse();
        std::memcpy(response.data() + sizeof(ErrorResponse), error_message.data(), error_message.size());
        return response;
    }

    [[nodiscard]] const char *message() const
    {
        return reinterpret_cast<const char *>(this + 1);
    }
};

class AnalyticalResponse final : public ServerResponse
{
  public:
    AnalyticalResponse(const std::uint64_t execution_time_in_ms, const std::uint64_t count_rows)
        : ServerResponse(Type::AnalyticalResult, execution_time_in_ms), _count_rows(count_rows)
    {
    }

    ~AnalyticalResponse() override = default;

    [[nodiscard]] std::uint64_t count_rows() const
    {
        return _count_rows;
    }

    static std::string build(const std::uint64_t execution_time_in_ms, const std::uint64_t count_rows,
                             QueryResultSerializer &&query_serializer)
    {
        std::string response = std::string(sizeof(AnalyticalResponse) + query_serializer.size(), '\0');
        new (response.data()) AnalyticalResponse(execution_time_in_ms, count_rows);
        std::memmove(response.data() + sizeof(AnalyticalResponse), query_serializer.data(), query_serializer.size());
        return response;
    }

    [[nodiscard]] const std::byte *data() const
    {
        return reinterpret_cast<const std::byte *>(this + 1);
    }

  private:
    const std::uint64_t _count_rows;
};

class TransactionalResponse final : public ServerResponse
{
  public:
    TransactionalResponse(const std::uint64_t execution_time_in_ms, const std::uint64_t count_rows)
        : ServerResponse(Type::TransactionalResult, execution_time_in_ms), _count_rows(count_rows)
    {
    }

    ~TransactionalResponse() override = default;

    [[nodiscard]] std::uint64_t affected_rows() const
    {
        return _count_rows;
    }

  private:
    const std::uint64_t _count_rows;
};

class QueryPlanResponse final : public ServerResponse
{
  public:
    QueryPlanResponse() : ServerResponse(Type::PlanExplanation, 0u)
    {
    }

    ~QueryPlanResponse() override = default;

    static std::string build(std::string &&plan)
    {
        std::string response = std::string(sizeof(QueryPlanResponse) + plan.length(), '\0');
        new (response.data()) QueryPlanResponse();
        std::memmove(response.data() + sizeof(QueryPlanResponse), plan.data(), plan.size());
        return response;
    }

    const char *payload() const
    {
        return reinterpret_cast<const char *>(this + 1);
    }
};

class ServerClosedResponse final : public ServerResponse
{
  public:
    ServerClosedResponse() : ServerResponse(Type::ServerClosed, 0U)
    {
    }

    ~ServerClosedResponse() override = default;

    static std::string build()
    {
        std::string response = std::string(sizeof(ServerClosedResponse), '\0');
        new (response.data()) ServerClosedResponse();
        return response;
    }
};
} // namespace beedb::io