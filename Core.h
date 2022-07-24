//------------------------------------------------------------------------------------------------------------
// Confidential and proprietary source code of BEOALEX.
// This source code may not be used, reproduced or disclosed in any manner without permission.
// Copyright (c) 2022, BEOALEX. All rights reserved.
// https://github.com/beoalex
//------------------------------------------------------------------------------------------------------------

#pragma once

#include <thread>
#include <cassert>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <functional>

#include "Decls.h"

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
class Core
{
    friend class UnitTests;

    using ConsumerWPtr = std::weak_ptr<Consumer<QueueId, Value>>;
    using CoreType = Core<QueueId, Value>;

public:
    Core(OverflowPolicy overflowPolicy, unsigned int maxQueueSize);
    ~Core();
    Core(const Core&) = delete;
    Core(Core&&) = delete;
    Core& operator=(const Core&) = delete;
    Core& operator=(Core&&) = delete;

    void setConsumer(QueueId queueId, const ConsumerWPtr& wpConsumer);
    void addValue(QueueId queueId, const Value& value);
    void suspend();
    void resume();

private:
    struct Queue
    {
        std::deque<Value> Values;
        ConsumerWPtr Consumer;
    };

    const OverflowPolicy m_overflowPolicy;
    const unsigned int m_maxQueueSize;
    bool m_queuesUpdated;
    bool m_stopRequested;
    bool m_coreSuspended;
    std::unordered_map<QueueId, Queue> m_queues;
    typename decltype(m_queues)::iterator m_currQueueIt;
    std::mutex m_mutex;
    std::condition_variable m_condVar;
    std::thread m_thread;

    Queue& getQueue(QueueId queueId);
    void threadFunction();
    bool extractValue(QueueId& queueId, Value& value, ConsumerWPtr& wpConsumer);
    bool selectNextQueue();
};

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
Core<QueueId, Value>::Core(OverflowPolicy overflowPolicy, unsigned int maxQueueSize)
    : m_overflowPolicy(overflowPolicy), m_maxQueueSize(maxQueueSize), m_queuesUpdated(false), m_stopRequested(false),
      m_coreSuspended(false), m_currQueueIt(m_queues.begin()), m_thread(std::bind(&CoreType::threadFunction, this))
{
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
Core<QueueId, Value>::~Core()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_stopRequested = true;
    lock.unlock();
    m_condVar.notify_one();
    m_thread.join();
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Core<QueueId, Value>::setConsumer(QueueId queueId, const ConsumerWPtr& wpConsumer)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    Queue& queue = getQueue(queueId);
    queue.Consumer = wpConsumer;

    if (   !wpConsumer.expired()
        && !queue.Values.empty())
    {
        m_queuesUpdated = true;
        lock.unlock();
        m_condVar.notify_one();
    }
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Core<QueueId, Value>::addValue(QueueId queueId, const Value& value)
{
    bool valueAdded = true;
    std::unique_lock<std::mutex> lock(m_mutex);
    Queue& queue = getQueue(queueId);
    auto& values = queue.Values;

    if (   m_maxQueueSize == Const::UnlimitedMaxQueueSize
        || values.size() < m_maxQueueSize)
    {
        values.push_back(value);
    }
    else if (m_overflowPolicy == OverflowPolicy::RemoveOld)
    {
        values.pop_front();
        values.push_back(value);
    }
    else if (m_overflowPolicy == OverflowPolicy::RemoveNew)
    {
        values.back() = value;
    }
    else
    {
        assert(m_overflowPolicy == OverflowPolicy::IgnoreNew);
        valueAdded = false;
    }

    if (   valueAdded
        && !queue.Consumer.expired())
    {
        m_queuesUpdated = true;
        lock.unlock();
        m_condVar.notify_one();
    }
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Core<QueueId, Value>::suspend()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_coreSuspended = true;
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Core<QueueId, Value>::resume()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_coreSuspended = false;
    }

    m_condVar.notify_one();
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
typename Core<QueueId, Value>::Queue& Core<QueueId, Value>::getQueue(QueueId queueId)
{
    auto it = m_queues.find(queueId);

    if (it == m_queues.end())
    {
        std::pair<bool, QueueId> currQueueId { false, QueueId() };

        if (   m_currQueueIt != m_queues.end()
            && m_queues.size() + 1 > m_queues.max_load_factor() * m_queues.bucket_count())
        {
            currQueueId.first = true;
            currQueueId.second = m_currQueueIt->first;
        }

        const auto pair = m_queues.insert(std::make_pair(queueId, Queue()));
        assert(pair.second);
        it = pair.first;

        if (currQueueId.first)
        {
            m_currQueueIt = m_queues.find(currQueueId.second);
            assert(m_currQueueIt != m_queues.end());
        }
    }

    return it->second;
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Core<QueueId, Value>::threadFunction()
{
    std::unique_lock<std::mutex> lock(m_mutex);

    for (;;)
    {
        m_condVar.wait(lock, [this] { return (m_stopRequested || (m_queuesUpdated && !m_coreSuspended)); });

        if (m_stopRequested)
        {
            break;
        }

        QueueId queueId {};
        Value value {};
        ConsumerWPtr wpConsumer {};

        if (extractValue(queueId, value, wpConsumer))
        {
            lock.unlock();

            if (std::shared_ptr<Consumer<QueueId, Value>> spConsumer = wpConsumer.lock())
            {
                spConsumer->consume(queueId, value);
            }

            lock.lock();
        }
        else
        {
            m_queuesUpdated = false;
        }
    }
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
bool Core<QueueId, Value>::extractValue(QueueId& queueId, Value& value, ConsumerWPtr& wpConsumer)
{
    const bool result = selectNextQueue();

    if (result)
    {
        assert(m_currQueueIt != m_queues.end());
        queueId = m_currQueueIt->first;
        Queue& queue = m_currQueueIt->second;
        assert(!queue.Values.empty());
        value = std::move(queue.Values.front());
        queue.Values.pop_front();
        wpConsumer = queue.Consumer;
    }

    return result;
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
bool Core<QueueId, Value>::selectNextQueue()
{
    size_t checkedQueues = 0;

    if (m_currQueueIt != m_queues.end())
    {
        if (++m_currQueueIt == m_queues.end())
        {
            m_currQueueIt = m_queues.begin();
        }
    }
    else
    {
        m_currQueueIt = m_queues.begin();
    }

    while (m_currQueueIt != m_queues.end())
    {
        if (   !m_currQueueIt->second.Consumer.expired()
            && !m_currQueueIt->second.Values.empty())
        {
            break;
        }

        if (++checkedQueues == m_queues.size())
        {
            m_currQueueIt = m_queues.end();
            break;
        }

        if (++m_currQueueIt == m_queues.end())
        {
            m_currQueueIt = m_queues.begin();
        }
    }

    return (m_currQueueIt != m_queues.end());
}

//------------------------------------------------------------------------------------------------------------
