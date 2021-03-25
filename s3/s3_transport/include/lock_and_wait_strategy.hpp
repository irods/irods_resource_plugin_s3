#ifndef IRODS_LOCK_AND_WAIT_STRATEGY_HPP
#define IRODS_LOCK_AND_WAIT_STRATEGY_HPP

#include <functional>
#include <condition_variable>
#include <chrono>

namespace irods {
namespace experimental {

    class timeout_exception : public std::exception
    {
        virtual const char* what() const throw()
        {
             return "Timeout waiting for lock";
        }
    };

    class lock_and_wait_strategy {
        public:
            using wait_predicate = std::function<bool()>;
            using the_work = std::function<void()>;
            virtual void operator()(wait_predicate, the_work) = 0;
            virtual ~lock_and_wait_strategy() {};
    };

    class do_not_lock_and_wait : public lock_and_wait_strategy {
        public:
            void operator()(wait_predicate, the_work w) {
                w();
            }
    };

    class lock_and_wait : public lock_and_wait_strategy {
        public:
            void operator()(wait_predicate p, the_work w) {
                {
                    std::unique_lock<std::mutex> lk(cv_mutex);
                    cv.wait(lk, p);
                    w();
                }
                cv.notify_all();
            }

        private:
            std::condition_variable cv;
            std::mutex cv_mutex;
   };

   class lock_and_wait_with_timeout : public lock_and_wait_strategy {

        public:

            explicit lock_and_wait_with_timeout(int timeout_sec)
                : timeout_seconds(timeout_sec)
            {}

            void operator()(wait_predicate p, the_work w) {
                bool wait_until_ret = false;
                {
                    std::unique_lock<std::mutex> lk(cv_mutex);
                    auto now = std::chrono::system_clock::now();
                    wait_until_ret = cv.wait_until(lk, now + std::chrono::seconds(timeout_seconds), p);
                    if (wait_until_ret) {
                        // predicate met
                        w();
                    }
                }
                cv.notify_all();

                if (!wait_until_ret) {
                    throw timeout_exception();
                }
            }

        private:
            std::condition_variable cv;
            std::mutex cv_mutex;
            int timeout_seconds;
   };

} // namespace experimental
} // namespace irods



#endif   // IRODS_LOCK_AND_WAIT_STRATEGY_HPP
