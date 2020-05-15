#ifndef IRODS_RING_BUFFER_HPP
#define IRODS_RING_BUFFER_HPP

#include <boost/circular_buffer.hpp>
#include "lock_and_wait_strategy.hpp"

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

            void pop_front(T& entry)
            {
                (*lws_)([this] { return 0 < cb_.size(); },
                        [this, &entry] {
                            auto iter = cb_.begin();
                            entry = *iter;
                            cb_.pop_front();
                        } );
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
