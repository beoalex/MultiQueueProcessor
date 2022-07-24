//------------------------------------------------------------------------------------------------------------
// Confidential and proprietary source code of BEOALEX.
// This source code may not be used, reproduced or disclosed in any manner without permission.
// Copyright (c) 2022, BEOALEX. All rights reserved.
// https://github.com/beoalex
//------------------------------------------------------------------------------------------------------------

#pragma once

#include <vector>
#include <thread>
#include <algorithm>

#include "Core.h"

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
class Processor
{
    friend class UnitTests;

    using ConsumerWPtr = std::weak_ptr<Consumer<QueueId, Value>>;
    using CoreType = Core<QueueId, Value>;
    using CorePtr = std::unique_ptr<CoreType>;

public:
    Processor();
    explicit Processor(unsigned int threadCount);
    Processor(OverflowPolicy overflowPolicy, unsigned int threadCount, unsigned int maxQueueSize);

    Processor(const Processor&) = delete;
    Processor(Processor&&) = delete;
    Processor& operator=(const Processor&) = delete;
    Processor& operator=(Processor&&) = delete;

    void setConsumer(QueueId queueId, const ConsumerWPtr& wpConsumer);
    void resetConsumer(QueueId queueId);
    void addValue(QueueId queueId, const Value& value);
    void suspend();
    void resume();

private:
    std::vector<CorePtr> m_cores;

    CorePtr& getCore(QueueId queueId);
};

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
Processor<QueueId, Value>::Processor() : Processor(OverflowPolicy::Default, Const::ThreadCountNotSpecified, Const::DefaultMaxQueueSize)
{
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
Processor<QueueId, Value>::Processor(unsigned int threadCount) : Processor(OverflowPolicy::Default, threadCount, Const::DefaultMaxQueueSize)
{
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
Processor<QueueId, Value>::Processor(OverflowPolicy overflowPolicy, unsigned int threadCount, unsigned int maxQueueSize)
{
    unsigned int coreCount = threadCount;

    if (coreCount == Const::ThreadCountNotSpecified)
    {
        coreCount = std::max(1u, std::thread::hardware_concurrency());
    }

    m_cores.resize(coreCount);
    std::generate(m_cores.begin(), m_cores.end(), [=] { return std::make_unique<CoreType>(overflowPolicy, maxQueueSize); });
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Processor<QueueId, Value>::setConsumer(QueueId queueId, const ConsumerWPtr& wpConsumer)
{
    getCore(queueId)->setConsumer(queueId, wpConsumer);
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Processor<QueueId, Value>::resetConsumer(QueueId queueId)
{
    setConsumer(queueId, ConsumerWPtr());
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Processor<QueueId, Value>::addValue(QueueId queueId, const Value& value)
{
    getCore(queueId)->addValue(queueId, value);
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Processor<QueueId, Value>::suspend()
{
    std::for_each(m_cores.begin(), m_cores.end(), [] (auto& corePtr) { corePtr->suspend(); });
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
void Processor<QueueId, Value>::resume()
{
    std::for_each(m_cores.begin(), m_cores.end(), [] (auto& corePtr) { corePtr->resume(); });
}

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
typename Processor<QueueId, Value>::CorePtr& Processor<QueueId, Value>::getCore(QueueId queueId)
{
    const int coreIndex = static_cast< int >(std::hash<QueueId>()(queueId)) % m_cores.size();
    return m_cores.at(coreIndex);
}

//------------------------------------------------------------------------------------------------------------
