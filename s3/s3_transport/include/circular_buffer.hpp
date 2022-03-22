#ifndef IRODS_RING_BUFFER_HPP
#define IRODS_RING_BUFFER_HPP

#include <boost/circular_buffer.hpp>
#include "lock_and_wait_strategy.hpp"
#include <iterator>

namespace irods {
namespace experimental {

    // ring buffer with protection for overwrites
    template <typename T>
    class circular_buffer {

        public:

            explicit circular_buffer(
                const size_t capacity,
                std::unique_ptr<lock_and_wait_strategy> lws = std::make_unique<lock_and_wait>())
                : cb_{capacity}
                , lws_{std::move(lws)}
            {
            }

            explicit circular_buffer(
                const size_t capacity,
                int timeout)
                : circular_buffer(capacity, std::make_unique<lock_and_wait_with_timeout>(timeout))
            {
            }

            void pop_front(T& entry)
            {
                (*lws_)([this] { return 0 < cb_.size(); },
                        [this, &entry] {
                            auto iter = cb_.begin();
                            entry = *iter;
                            cb_.pop_front();
                        } );
            }

            // erase n items from front of the queue
            void pop_front(size_t n)
            {
                (*lws_)([this, n] { return n <= cb_.size(); },
                        [this, n] { cb_.erase_begin(n); } );
            }

            // peek item at offset from beginning without removing from queue
            void peek(size_t offset, T& entry)
            {
                (*lws_)([this, offset] { return offset < cb_.size(); },
                        [this, offset, &entry] {
                            auto iter = cb_.begin();
                            entry = *(iter + offset);
                        } );
            }

            // peek n items starting at offset (from beginning) into array without removing from buffer
            //  precondition: array is large enough to hold n items
            void peek(off_t offset, size_t n, T array[])
            {
                auto length = offset + n;
                (*lws_)([this, length] { return length <= cb_.size(); },
                        [this, offset, n, &array] {
                            auto iter = cb_.begin() + offset;
                            std::copy(iter, iter + n, array);
                        } );
            }

            template <typename iter>
            int64_t push_back(iter begin, iter end)
            {
                // push what you can, return the number pushed
                int64_t insertion_count = 0;
                (*lws_)([this] { return cb_.size() < cb_.capacity(); },
                        [this, begin, end, &insertion_count] {

                           auto distance = static_cast<uint64_t>(std::distance(begin, end));
                           auto empty_space = cb_.capacity() - cb_.size();
                           insertion_count = ( empty_space < distance ? empty_space : distance );
                           cb_.insert(cb_.end(), begin, begin + insertion_count );

                        } );

                return insertion_count;

            }

            void push_back(const T& entry)
            {
                (*lws_)([this] { return cb_.size() < cb_.capacity(); },
                        [this, &entry] { cb_.push_back(entry); } );
            }

        private:

            boost::circular_buffer<T> cb_;
            std::unique_ptr<lock_and_wait_strategy> lws_;

    }; // class circular_buffer

} // namespace experimental
} // namespace irods
#endif
