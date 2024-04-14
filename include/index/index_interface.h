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

#include <storage/page.h>

namespace beedb::index
{
/**
 * The index interface offers an API for multiple indices, whether
 * they are range-indices, support multiple values for one key.
 */
class IndexInterface
{
  public:
    explicit IndexInterface(const std::string &name) : _name(name)
    {
    }
    virtual ~IndexInterface() = default;

    /**
     * @return True, if the index supports just one value per key.
     */
    [[nodiscard]] virtual bool is_unique() const = 0;

    /**
     * @return True, if the index supports range queries (like B+Trees).
     */
    [[nodiscard]] virtual bool supports_range() const = 0;

    /**
     * Stores a (Key,Page) pair in the index.
     *
     * @param key Key for lookups.
     * @param page_id Value.
     */
    virtual void put(const std::int64_t key, storage::Page::id_t page_id) = 0;

    /**
     * @return Name of the index.
     */
    [[nodiscard]] const std::string &name() const
    {
        return _name;
    }

  private:
    const std::string _name;
};
} // namespace beedb::index