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
#include "exception.h"
#include <string>

namespace beedb::exception
{
class DiskException : public DatabaseException
{
  public:
    explicit DiskException(const std::string &message) : DatabaseException(DatabaseException::Disk, message)
    {
    }
    ~DiskException() override = default;
};

class EvictedPagePinnedException final : public DiskException
{
  public:
    explicit EvictedPagePinnedException(const std::uint64_t frame_index)
        : DiskException("Can not evict page, frame " + std::to_string(frame_index) + " is pinned.")
    {
    }

    ~EvictedPagePinnedException() override = default;
};

class PageWasNotPinnedException final : public DiskException
{
  public:
    explicit PageWasNotPinnedException(const std::uint64_t disk_id)
        : DiskException("Page " + std::to_string(disk_id) + " is not pinned, but unpin() called.")
    {
    }

    ~PageWasNotPinnedException() override = default;
};

class NoFreeFrameException final : public DiskException
{
  public:
    NoFreeFrameException() : DiskException("No free frame found for eviction.")
    {
    }

    ~NoFreeFrameException() override = default;
};

class CanNotOpenStorageFile final : public DiskException
{
  public:
    explicit CanNotOpenStorageFile(const std::string &file_name)
        : DiskException("Can not open storage file '" + file_name + "'.")
    {
    }

    ~CanNotOpenStorageFile() override = default;
};
} // namespace beedb::exception