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

#include <concurrency/transaction_manager.h>

using namespace beedb::concurrency;

bool TransactionManager::is_visible([[maybe_unused]] const Transaction &transaction,
                                    [[maybe_unused]] const timestamp begin_timestamp,
                                    [[maybe_unused]] const timestamp end_timestamp)
{
    /**
     * Assignment (6): Implement the visibility checker for transactions.
     *
     * Every record stored within the DBMS includes a "visibility range"
     * that indicates which transactions are allowed to "see" this record.
     * During a table scan, the transaction manager decides for every tuple
     * whether an access is allowed or not.
     * The decision is made based on the requesting transaction and the
     * "living time range" of the record:
     *  - The requesting transaction was started at a certain point in time
     *    (can be checked with "transaction.begin_timestamp()").
     *  - The record is visible for transactions started between "begin_timestamp"
     *     and "end_timestamp".
     *  - Note: The "end_timestamp" can be "infinity" ("end_timestamp.is_infinity()"),
     *          that means the record is still alive.
     *
     *  This function returns "true", when the transaction is allowed to see
     *  the record and "false" otherwise.
     *
     * Hints for implementation:
     *  - The starting point of the transaction is available
     *    by "transaction.begin_timestamp()".
     *  - Each timestamp can be "infinity" (e.g. "begin_timestamp.is_infinity()").
     *  - Each timestamp can be a committed timestamp, this can be checked
     *    e.g. with "begin_timestamp.is_committed()".
     *  - Timestamps are comparable, e.g., "begin_timestamp < end_timestamp".
     *
     *
     * Procedure:
     *  - Check if the begin_timestamp of the record is infinity.
     *    Timestamps beginning at infinity are invalid and should not
     *    be seen by any transaction.
     *  - Every transaction can see its own records. To identify these records,
     *    compare the begin_timestamp of the record and the begin_timestamp of
     *    the transaction. Equality means the transaction created the record.
     *  - The transaction is allowed to see records where:
     *      - begin_timestamp is committed and
     *      - begin_timestamp < transaction.begin_timestamp < end_timestamp
     *        (of course, the end_timestamp can be infinity, too, meaning the
     *          record is still alive)
     *  - When the transaction deleted the record
     *    (end_timestamp == transaction.begin_timestamp()), the transaction
     *    should NOT be able to see this record.
     */

    // TODO: Insert your code here.
    // TODO: Remove the following line after implementation!
    return true;
}
