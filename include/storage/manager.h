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
#include "page.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>

namespace beedb::storage
{
/**
 * The StorageManager grants access to the data written to the disk.
 */
class Manager
{
  public:
    explicit Manager(const std::string &file_name);
    ~Manager();

    /**
     * Copy a page from disk to memory.
     *
     * @param page_id Id of the page.
     * @param buffer  Location where to store the data in memory.
     */
    void read(Page::id_t page_id, std::byte *buffer);

    /**
     * Write the page from memory to disk.
     *
     * @param page_id Page to be written.
     * @param data  Location in memory of the data to write to disk.
     */
    void write(Page::id_t page_id, const std::byte *data);

    /**
     * Allocates a new page in the disk file and extends the
     * file by the new allocated page.
     *
     * @return Id of the new allocated page.
     */
    template <typename P> Page::id_t allocate()
    {
        const auto page_id = this->_count_pages.fetch_add(1);
        P page;

        this->write(page_id, page.data());
        return page_id;
    }

    /**
     * @return Number of pages stored in the disk file.
     */
    [[nodiscard]] std::size_t count_pages() const
    {
        return _count_pages;
    }

  private:
    std::atomic_size_t _count_pages = 0u;
    std::fstream _storage_file;
};
} // namespace beedb::storage