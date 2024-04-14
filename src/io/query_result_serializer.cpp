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

#include <io/query_result_serializer.h>

using namespace beedb::io;

QueryResultSerializer::QueryResultSerializer()
{
    _capacity = 1024u;
    _data = static_cast<std::byte *>(std::malloc(_capacity));
}

QueryResultSerializer::~QueryResultSerializer()
{
    std::free(_data);
}

void QueryResultSerializer::serialize(const table::Schema &schema)
{
    auto needed_size = sizeof(std::uint16_t) + // Number of columns
                       sizeof(std::uint16_t) + // Row size
                       // For every column: Type, offset
                       schema.size() * (sizeof(table::Type) + sizeof(std::uint16_t)) +
                       sizeof(std::uint16_t) + // Number of terms
                       // For every attribute: Order
                       schema.terms().size() * sizeof(std::uint16_t);

    // For every attribute: Length of the name and alias
    for (const auto &term : schema.terms())
    {
        needed_size += term.alias().has_value() ? term.alias().value().size() + 1 : 1;
        needed_size += term.get<expression::Attribute>().table_name().has_value()
                           ? term.get<expression::Attribute>().table_name().value().size() + 1
                           : 1;
        needed_size += term.get<expression::Attribute>().column_name().size() + 1;
    }

    // Allocate bytes for serialization.
    auto *tuple_metadata = new std::byte[needed_size];
    std::memset(tuple_metadata, '\0', needed_size);
    auto current_index = 0u;

    // Serialize number of columns.
    *reinterpret_cast<std::uint16_t *>(&tuple_metadata[current_index]) = schema.size();
    current_index += sizeof(std::uint16_t);

    // Serialize row size.
    *reinterpret_cast<std::uint16_t *>(&tuple_metadata[current_index]) = schema.row_size();
    current_index += sizeof(std::uint16_t);

    // Serialize every column: Type, offset, order.
    for (auto i = 0u; i < schema.size(); ++i)
    {
        *reinterpret_cast<table::Type *>(&tuple_metadata[current_index]) = schema.column(i).type();
        current_index += sizeof(table::Type);

        *reinterpret_cast<std::uint16_t *>(&tuple_metadata[current_index]) = schema.offset(i);
        current_index += sizeof(std::uint16_t);
    }

    *reinterpret_cast<std::uint16_t *>(&tuple_metadata[current_index]) = schema.terms().size();
    current_index += sizeof(std::uint16_t);

    auto i = 0u;
    for (const auto &term : schema.terms())
    {
        *reinterpret_cast<std::uint16_t *>(&tuple_metadata[current_index]) = schema.column_order()[i++];
        current_index += sizeof(std::uint16_t);

        if (term.alias().has_value())
        {
            std::memcpy(&tuple_metadata[current_index], term.alias().value().data(), term.alias().value().size());
            current_index += term.alias().value().size() + 1;
        }
        else
        {
            *reinterpret_cast<char *>(&tuple_metadata[current_index]) = '\0';
            current_index += 1;
        }

        const auto &attribute = term.get<expression::Attribute>();
        if (attribute.table_name().has_value())
        {
            std::memcpy(&tuple_metadata[current_index], attribute.table_name().value().data(),
                        attribute.table_name().value().size());
            current_index += attribute.table_name().value().size() + 1;
        }
        else
        {
            *reinterpret_cast<char *>(&tuple_metadata[current_index]) = '\0';
            current_index += 1;
        }

        std::memcpy(&tuple_metadata[current_index], attribute.column_name().data(), attribute.column_name().size());
        current_index += attribute.column_name().size() + 1;
    }

    this->prepend(tuple_metadata, needed_size);
    delete[] tuple_metadata;
}

void QueryResultSerializer::serialize(const table::Tuple &tuple)
{
    this->append(tuple.data(), tuple.schema().row_size());
}

void QueryResultSerializer::append(const std::byte *data, const std::uint16_t size)
{
    while (this->_capacity < this->_size + size)
    {
        this->allocate(this->_capacity << 1);
    }

    std::memcpy(&this->_data[this->_size], data, size);
    this->_size += size;
}

void QueryResultSerializer::prepend(const std::byte *data, const std::uint16_t size)
{
    while (this->_capacity < this->_size + size)
    {
        this->allocate(this->_capacity << 1);
    }

    std::memmove(&this->_data[size], this->_data, size);
    std::memcpy(this->_data, data, size);
    this->_size += size;
}

void QueryResultSerializer::allocate(const std::size_t capacity)
{
    this->_data = static_cast<std::byte *>(std::realloc(this->_data, capacity));
    this->_capacity = capacity;
}