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

#include <algorithm>
#include <buffer/manager.h>
#include <cassert>
#include <exception/disk_exception.h>
#include <limits>

using namespace beedb::buffer;

Manager::Manager(std::size_t count_frames, beedb::storage::Manager &space_manager,
                 std::unique_ptr<ReplacementStrategy> &&replacement_strategy)
    : _space_manager(space_manager), _replacement_strategy(std::move(replacement_strategy))
{
    _frames.resize(count_frames);
}

Manager::~Manager()
{
    // Check no frame is pinned anymore (this would indicate programming failure).
    for (const auto &page : this->_frames)
    {
        assert(page.is_pinned() == false && "Not all pages are unpinned on shutdown.");
    }

    // Write all dirty pages back to disk.
    this->flush();
}

beedb::storage::Page *Manager::pin(beedb::storage::Page::id_t page_id)
{
    std::lock_guard _{this->_latch};

    this->_pin_sequence++;

    const auto page_iterator = this->frame_information(page_id);
    const bool is_frame_buffered = page_iterator != this->_frames.end();
    if (is_frame_buffered)
    {
        auto &page = *page_iterator;
        // Update frame information.
        page.pin_count(page.pin_count() + 1u);

        const auto index = std::distance(this->_frames.begin(), page_iterator);
        return &(this->_frames[index]);
    }
    else
    {
        // Find frame for the pinned page.
        auto frame_index = this->_evicted_frames++;
        if (frame_index >= this->_frames.size())
        {
            frame_index = this->_replacement_strategy->find_victim(this->_frames);
        }

        auto &page = this->_frames[frame_index];
        if (page.is_pinned())
        {
            throw exception::EvictedPagePinnedException(frame_index);
        }

        // Write frame back, if the data was modified.
        if (page.is_dirty() == true)
        {
            this->_space_manager.write(page.id(), page.data());
        }

        // Load page into frame.
        page.id(page_id);
        page.is_dirty(false);
        page.pin_count(1u);
        this->_space_manager.read(page_id, page.data());

        // Clear frame information.
        this->_evicted_frames++;

        return &this->_frames[frame_index];
    }
}

void Manager::unpin(storage::Page::id_t page_id, bool is_dirty)
{
    std::lock_guard _{this->_latch};

    auto page_iterator = this->frame_information(page_id);
    if (page_iterator != this->_frames.end())
    {
        if (page_iterator->is_pinned() == false)
        {
            throw exception::PageWasNotPinnedException(page_id);
        }
        page_iterator->pin_count(page_iterator->pin_count() - 1u);
        page_iterator->is_dirty(page_iterator->is_dirty() | is_dirty);
    }
}

void Manager::flush()
{
    std::lock_guard _{this->_latch};

    for (auto &page : this->_frames)
    {
        // Write back, when the frame is dirty.
        if (page.id() != storage::Page::INVALID_PAGE_ID && page.is_dirty())
        {
            this->_space_manager.write(page.id(), page.data());
            page.is_dirty(false);
        }
    }
}

std::vector<beedb::storage::Page>::iterator Manager::frame_information(storage::Page::id_t page_id)
{
    return std::find_if(this->_frames.begin(), this->_frames.end(),
                        [page_id](const storage::Page &page) { return page.id() == page_id; });
}