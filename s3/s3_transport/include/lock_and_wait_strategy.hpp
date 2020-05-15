#ifndef IRODS_LOCK_AND_WAIT_STRATEGY_HPP
#define IRODS_LOCK_AND_WAIT_STRATEGY_HPP

#include <functional>
#include <condition_variable>

namespace irods {
namespace experimental {

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

} // namespace experimental
} // namespace irods



#endif   // IRODS_LOCK_AND_WAIT_STRATEGY_HPP
