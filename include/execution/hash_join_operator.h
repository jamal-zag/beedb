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
#include "tuple_buffer.h"
#include <memory>
#include <string>
#include <table/memory_table.h>
#include <table/schema.h>
#include <table/tuple.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace beedb::execution
{
/**
 * Hash table for hash join.
 */
class HashTable
{
  public:
    explicit HashTable(const std::uint32_t key_index) : _key_index(key_index)
    {
    }

    ~HashTable() = default;

    bool contains(const table::Value &key)
    {
        return this->_map.find(key) != _map.end();
    }

    void put(const table::Tuple &tuple)
    {
        table::Tuple in_memory_tuple(tuple);
        this->_map[tuple.get(_key_index)].push_back(std::move(in_memory_tuple));
    }

    const std::vector<table::Tuple> &get(const table::Value &key)
    {
        return _map[key];
    }

  private:
    const std::uint32_t _key_index;
    std::unordered_map<table::Value, std::vector<table::Tuple>> _map;
};

/**
 * Operator that joins two sources using a hash table over
 * the left source.
 */
class HashJoinOperator final : public BinaryOperator
{
  public:
    HashJoinOperator(concurrency::Transaction *transaction, table::Schema &&schema, const std::uint32_t left_index,
                     const std::uint32_t right_index);
    ~HashJoinOperator() override = default;

    void open() override;
    util::optional<table::Tuple> next() override;
    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    }

  private:
    const table::Schema _schema;
    const std::uint32_t _left_index;
    const std::uint32_t _right_index;
    HashTable _hash_table;
    bool _is_built = false;
    TupleBuffer _tuple_buffer;

    /**
     * Builds the hash table.
     */
    void build_hash_table();

    /**
     * Probes the hash table.
     */
    util::optional<table::Tuple> probe_hash_table();
};
} // namespace beedb::execution
