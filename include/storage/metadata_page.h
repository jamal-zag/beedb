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
#include <concurrency/timestamp.h>

namespace beedb::storage
{
class MetadataPage final : public Page
{
  public:
    MetadataPage() = default;

    ~MetadataPage() override = default;

    [[nodiscard]] concurrency::timestamp::timestamp_t next_transaction_timestamp() const
    {
        return *reinterpret_cast<const concurrency::timestamp::timestamp_t *>(Page::data() + sizeof(Page::id_t));
    }

    void next_transaction_timestamp(concurrency::timestamp::timestamp_t timestamp)
    {
        *reinterpret_cast<concurrency::timestamp::timestamp_t *>(Page::data() + sizeof(Page::id_t)) = timestamp;
    }
};
} // namespace beedb::storage
