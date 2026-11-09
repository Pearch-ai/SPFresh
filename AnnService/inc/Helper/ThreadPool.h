// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_THREADPOOL_H_
#define _SPTAG_HELPER_THREADPOOL_H_

#include <atomic>
#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <stdexcept>

namespace SPTAG
{
    namespace Helper
    {
        class ThreadPool
        {
        public:
            class Abort : public IAbortOperation
            {
            private:
                bool m_stopped;

            public:
                Abort(bool p_status = true) { m_stopped = p_status; }
                ~Abort() {}
                virtual bool ShouldAbort() { return m_stopped; }
                void SetAbort(bool p_status) { m_stopped = p_status; }
            };

            class Job
            {
            public:
                virtual ~Job() {}
                virtual void exec(IAbortOperation* p_abort) = 0;
            };

            ThreadPool() {}

            ~ThreadPool() 
            {
                m_abort.SetAbort(true);
                m_cond.notify_all();
                for (auto&& t : m_threads) t.join();
                m_threads.clear();
                while (!m_jobs.empty()) { delete m_jobs.front(); m_jobs.pop(); }
            }

            void init(int numberOfThreads = 1)
            {
                m_abort.SetAbort(false);
                for (int i = 0; i < numberOfThreads; i++)
                {
                    m_threads.emplace_back([this] {
                        Job *j;
                        while (get(j))
                        {
                            try 
                            {
                                currentJobs++;
                                j->exec(&m_abort);
                                currentJobs--;
                            }
                            catch (std::exception& e) {
                                LOG(Helper::LogLevel::LL_Error, "ThreadPool: exception in %s %s\n", typeid(*j).name(), e.what());
                            }
                            
                            delete j;
                        }
                    });
                }
            }

            void add(Job* j)
            {
                {
                    std::lock_guard<std::mutex> lock(m_lock);
                    if (m_maxJobs != 0 && m_jobs.size() >= m_maxJobs) {
                        delete j;
                        throw std::runtime_error("SPFresh maintenance queue exceeds memory budget");
                    }
                    m_jobs.push(j);
                }
                m_cond.notify_one();
            }

            void setMaxJobs(size_t limit) { m_maxJobs = limit; }

            bool runOne()
            {
                std::unique_ptr<Job> job;
                {
                    std::lock_guard<std::mutex> lock(m_lock);
                    if (m_jobs.empty()) return false;
                    job.reset(m_jobs.front());
                    m_jobs.pop();
                }
                job->exec(&m_abort);
                return true;
            }

            bool get(Job*& j)
            {
                std::unique_lock<std::mutex> lock(m_lock);
                while (m_jobs.empty() && !m_abort.ShouldAbort()) m_cond.wait(lock);
                if (!m_abort.ShouldAbort()) {
                    j = m_jobs.front();
                    m_jobs.pop();
                }
                return !m_abort.ShouldAbort();
            }

            size_t jobsize()
            {
                // std::lock_guard<std::mutex> lock(m_lock);
                return m_jobs.size();
            }

            inline uint32_t runningJobs() { return currentJobs; }

            inline bool allClear() { return currentJobs == 0 && jobsize() == 0; }

        protected:
            size_t m_maxJobs = 0;
            std::atomic_uint32_t currentJobs{ 0 };
            std::queue<Job*> m_jobs;
            Abort m_abort;
            std::mutex m_lock;
            std::condition_variable m_cond;
            std::vector<std::thread> m_threads;
        };
    }
}

#endif // _SPTAG_HELPER_THREADPOOL_H_