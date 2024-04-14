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
#include "frame.h"
#include "replacement_strategy.h"
#include <cstdint>
#include <memory>
#include <mutex>
#include <storage/manager.h>
#include <storage/page.h>
#include <vector>

namespace beedb::buffer
{
/**
 * The BufferManager buffers pages stored on the disk in memory.
 * Since the system has not infinite memory, the number of buffered
 * pages (named "frames") is limited.
 *
 * Every access to a page on the disk is done by pinning the page
 * through the BufferManager. When the page is not needed any more
 * (e.g. all tuples are scanned), the page can be unpinned by the
 * BufferManager.
 */
class Manager
{
  public:
    Manager(std::size_t count_frames, storage::Manager &space_manager,
            std::unique_ptr<ReplacementStrategy> &&replacement_strategy = nullptr);
    ~Manager();

    /**
     * Loads the page from disk into memory and returns a pointer
     * to the page. When the page is still buffered, the page will
     * not be loaded twice, but guaranteed to stay in memory until
     * it is unpinned.
     *
     * @param page_id Id of the page.
     * @return Pointer to the page, that allows accessing the data.
     */
    storage::Page *pin(storage::Page::id_t page_id);

    /**
     * Notifies the BufferManager that the page is not needed anymore.
     * In case no one needs the page, the frame can be used for other
     * pages buffered from disk in memory.
     *
     * @param page_id   Id of the page.
     * @param is_dirty  True, when the content of the page was modified.
     */
    void unpin(storage::Page::id_t page_id, bool is_dirty);

    /**
     * Notifies the BufferManager that the page is not needed anymore.
     * In case no one needs the page, the frame can be used for other
     * pages buffered from disk in memory.
     *
     * @param page_id   Id of the page.
     * @param is_dirty  True, when the content of the page was modified.
     */
    void unpin(storage::Page *page, bool is_dirty)
    {
        unpin(page->id(), is_dirty);
    }

    /**
     * Allocates a new page on the disk and loads the page to memory.
     *
     * @return Pointer to the pinned(!) page.
     */
    template <typename P = storage::Page> storage::Page *allocate()
    {
        return this->pin(this->_space_manager.allocate<P>());
    }

    /**
     * Set the replacement strategy which picks frames to be replaced,
     * when all frames are occupied, but a new page is requested to
     * be loaded from disk to memory.
     * @param replacement_strategy
     */
    void replacement_strategy(std::unique_ptr<ReplacementStrategy> &&replacement_strategy)
    {
        _replacement_strategy = std::move(replacement_strategy);
    }

    /**
     * @return Number of evicted frames.
     */
    [[nodiscard]] std::size_t evicted_frames() const
    {
        return _evicted_frames;
    }

  private:
    storage::Manager &_space_manager;
    std::unique_ptr<ReplacementStrategy> _replacement_strategy;

    std::vector<storage::Page> _frames;
    std::size_t _pin_sequence = 0u;
    std::size_t _evicted_frames = 0u;

    std::mutex _latch;

    /**
     * Writes all dirty pages from memory to disk.
     */
    void flush();

    /**
     * Lookup for frame information for a specific page.
     * The frame information stores information like pin
     * history, pinned page for a frame.
     *
     * @param page_id Id of the page.
     * @return An iterator to the frame information or end() if the frame was not found.
     */
    std::vector<storage::Page>::iterator frame_information(storage::Page::id_t page_id);
};
} // namespace beedb::buffer