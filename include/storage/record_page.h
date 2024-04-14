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
#include <concurrency/metadata.h>

namespace beedb::storage
{
/**
 * Page:
 * Next Page Id (16bit) | Number of Slots (16bit) | Free Pointer (16bit) | Slot_0 | Slot_1 | Slot_2 | ... free space ...
 * | Record_2 | Record_1 | Record _0
 *                                                         |-------------------------------------------------------------^
 */
class RecordPage final : public Page
{
  private:
    class Slot
    {
      public:
        Slot(Page::offset_t start, Page::offset_t size) : _start(start), _size(size << 1)
        {
        }
        ~Slot() = default;

        [[nodiscard]] Page::offset_t start() const
        {
            return _start;
        }
        [[nodiscard]] Page::offset_t size() const
        {
            return _size >> 1;
        }
        [[nodiscard]] bool is_free() const
        {
            return (_size & 1u) == 1u;
        }
        void is_free(bool is_free)
        {
            _size = ((_size >> 1u) << 1u) | is_free;
        }

      private:
        Page::offset_t _start = 0;
        Page::offset_t _size = 0;
    };

  public:
    RecordPage()
    {
        *reinterpret_cast<std::uint16_t *>(Page::data() + sizeof(Page::id_t) + sizeof(std::uint16_t)) =
            Config::page_size;
    }

    ~RecordPage() override = default;

    [[nodiscard]] std::uint16_t slots() const
    {
        return *reinterpret_cast<const std::uint16_t *>(Page::data() + sizeof(Page::id_t));
    }
    void slots(std::uint16_t slots)
    {
        *reinterpret_cast<std::uint16_t *>(Page::data() + sizeof(Page::id_t)) = slots;
    }

    [[nodiscard]] const Slot &slot(std::uint16_t index) const
    {
        return *reinterpret_cast<const Slot *>(Page::data() + sizeof(Page::id_t) + sizeof(std::uint16_t) +
                                               sizeof(std::uint16_t) + (index * sizeof(Slot)));
    }
    [[nodiscard]] Slot &slot(std::uint16_t index)
    {
        return *reinterpret_cast<Slot *>(Page::data() + sizeof(Page::id_t) + sizeof(std::uint16_t) +
                                         sizeof(std::uint16_t) + (index * sizeof(Slot)));
    }

    [[nodiscard]] std::byte *record(std::uint16_t index)
    {
        const auto &slot = this->slot(index);
        return Page::data() + slot.start();
    }

    [[nodiscard]] bool is_free(std::uint16_t index) const
    {
        return this->slot(index).is_free();
    }

    void erase(std::uint16_t index)
    {
        this->slot(index).is_free(true);
    }

    [[nodiscard]] std::uint16_t free_space() const
    {
        const auto free_space_pointer =
            *reinterpret_cast<const std::uint16_t *>(Page::data() + sizeof(Page::id_t) + sizeof(std::uint16_t));
        return free_space_pointer - (this->slots() * sizeof(Slot)) - sizeof(std::uint16_t) - sizeof(std::uint16_t) -
               sizeof(Page::id_t);
    }

    std::uint16_t allocate_slot(std::uint16_t size)
    {
        const auto slot_size = size + sizeof(concurrency::Metadata);

        const auto slots = this->slots();
        const auto slot_id = slots;
        this->slots(slots + 1);

        const auto free_space_pointer_before =
            *reinterpret_cast<const std::uint16_t *>(Page::data() + sizeof(Page::id_t) + sizeof(std::uint16_t));
        *reinterpret_cast<std::uint16_t *>(Page::data() + sizeof(Page::id_t) + sizeof(std::uint16_t)) =
            free_space_pointer_before - slot_size;

        new (Page::data() + sizeof(Page::id_t) + sizeof(std::uint16_t) + sizeof(std::uint16_t) + (slots * sizeof(Slot)))
            Slot(free_space_pointer_before - slot_size, slot_size);

        return slot_id;
    }

    void write(const std::uint16_t slot_id, const concurrency::Metadata *concurrency_metadata, const std::byte *payload,
               const std::uint16_t size)
    {
        auto *record = this->record(slot_id);
        std::memcpy(record, concurrency_metadata, sizeof(concurrency::Metadata));
        std::memcpy(record + sizeof(concurrency::Metadata), payload, size);
    }
};
} // namespace beedb::storage
