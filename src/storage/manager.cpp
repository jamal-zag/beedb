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

#include <cassert>
#include <config.h>
#include <exception/disk_exception.h>
#include <storage/manager.h>

using namespace beedb::storage;

Manager::Manager(const std::string &file_name)
{
    this->_storage_file.open(file_name, std::ios::in | std::ios::out | std::ios::binary);
    if (this->_storage_file.is_open() == false)
    {
        // Create file if not exists. Non-existing files can not be opened with in | out only -> add trunc.
        this->_storage_file.open(file_name, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    }

    if (this->_storage_file.is_open() == false)
    {
        throw exception::CanNotOpenStorageFile(file_name);
    }
    this->_storage_file.seekg(0, std::ios::end);

    assert(this->_storage_file.tellg() % Config::page_size == 0);
    this->_count_pages = this->_storage_file.tellg() / Config::page_size;
}

Manager::~Manager()
{
    this->_storage_file.close();
}

void Manager::read([[maybe_unused]] const Page::id_t page_id, [[maybe_unused]] std::byte *buffer)
{
    /**
     * Assignment (1): Implement the storage manager.
     *
     * The storage manager reads and writes data from and to the
     * persistent disk into memory. The data stored on the disk
     * is organized in "Pages"; each page has a fixed size of
     * bytes (configured in Config::page_size). On startup of the
     * database, the storage file (storage::Manager::_storage_file)
     * will be opened.
     *
     * To read data from the storage file, we have to seek the file
     * pointer to the requested page and read the full page into
     * the given buffer.
     *
     * Hints:
     *  - The begin of the page is identified by the multiple of
     *    page_id and Config::page_size.
     *  - You have to cast std::byte* to char* using
     *    reinterpret_cast when reading.
     *
     * Procedure:
     *  - Seek this->_storage_file to the begin of the page [1].
     *  - Read the full page into the given buffer [2]
     *
     * [1] https://en.cppreference.com/w/cpp/io/basic_istream/seekg
     * [2] https://en.cppreference.com/w/cpp/io/basic_istream/read
     */

    // Calculate the byte offset from the beginning of the file
    // We cast to std::streamoff to ensure the multiplication is done with the correct type for seeking.
    std::streamoff offset = static_cast<std::streamoff>(page_id) * Config::page_size;

    // 1. Seek this->_storage_file to the beginning of the page [1].
    // We use seekg() for "get" (input/read) operations.
    this->_storage_file.seekg(offset);

    // 2. Read the full page into the given buffer [2]
    // .read() expects a char*, so we must reinterpret_cast the std::byte* buffer.
    this->_storage_file.read(reinterpret_cast<char*>(buffer), Config::page_size);
}

void Manager::write([[maybe_unused]] const Page::id_t page_id, [[maybe_unused]] const std::byte *data)
{
    /**
     * Assignment (1): Implement the storage manager.
     *
     * The storage manager reads and writes data from and to the
     * persistent disk into memory. The data stored on the disk
     * is organized in "Pages"; each page has a fixed size of
     * bytes (configured in Config::page_size). On startup of the
     * database, the storage file (storage::Manager::_storage_file)
     * will be opened.
     *
     * To write data from the buffer to the storage file, we have to
     * seek the file pointer to the requested page and copy the full
     * page into the file.
     *
     * Hints:
     *  - The begin of the page is identified by the multiple of
     *    page_id and Config::page_size.
     *  - The seek-operations differ on read and write (see the
     *    links below).
     *  - You have to cast const std::byte* to const char* using
     *    reinterpret_cast when writing.
     *
     * Procedure:
     *  - Seek this->_storage_file to the begin of the page [1].
     *  - Write the given buffer to the file [2]. The buffer has
     *    a size of Config::page_size.
     *
     * [1] https://en.cppreference.com/w/cpp/io/basic_ostream/seekp
     * [2] https://en.cppreference.com/w/cpp/io/basic_ostream/write
     */

    // Calculate the byte offset from the beginning of the file
    std::streamoff offset = static_cast<std::streamoff>(page_id) * Config::page_size;

    // 1. Seek this->_storage_file to the beginning of the page [1].
    // We use seekp() for "put" (output/write) operations.
    this->_storage_file.seekp(offset);

    // 2. Write the given buffer to the file [2].
    // .write() expects a const char*, so we must reinterpret_cast the const std::byte* data.
    this->_storage_file.write(reinterpret_cast<const char*>(data), Config::page_size);

    // It's crucial for a storage manager to ensure data is persistent.
    // We flush the file stream to force the OS to write the buffer to disk.
    this->_storage_file.flush();
}
