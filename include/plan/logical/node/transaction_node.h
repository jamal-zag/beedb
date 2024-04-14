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

#include "node_interface.h"

namespace beedb::plan::logical
{
class BeginTransactionNode final : public NotSchematizedNode
{
  public:
    BeginTransactionNode() : NotSchematizedNode("Begin Transaction")
    {
    }
    ~BeginTransactionNode() override = default;
};

class CommitTransactionNode final : public NotSchematizedNode
{
  public:
    CommitTransactionNode() : NotSchematizedNode("Commit Transaction")
    {
    }
    ~CommitTransactionNode() override = default;
};

class AbortTransactionNode final : public NotSchematizedNode
{
  public:
    AbortTransactionNode() : NotSchematizedNode("Abort Transaction")
    {
    }
    ~AbortTransactionNode() override = default;
};
} // namespace beedb::plan::logical