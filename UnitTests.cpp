//------------------------------------------------------------------------------------------------------------
// Confidential and proprietary source code of BEOALEX.
// This source code may not be used, reproduced or disclosed in any manner without permission.
// Copyright (c) 2022, BEOALEX. All rights reserved.
// https://github.com/beoalex
//------------------------------------------------------------------------------------------------------------

#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>

#include <string>
#include <iostream>
#include <map>
#include <chrono>

#ifdef _DEBUG
    #include <conio.h>
#endif

#include "Processor.h"

using namespace std::chrono;
using namespace std::chrono_literals;

using IntConsumer = Consumer<int, int>;
using IntProcessor = Processor<int, int>;
using IntCore = Core<int, int>;

//------------------------------------------------------------------------------------------------------------

class CountingConsumer : public IntConsumer
{
public:
    void consume(int, const int&) override
    {
        ++m_counter;
    }

    int getCounter() const
    {
        return m_counter;
    }

private:
    std::atomic<int> m_counter = 0;
};

//------------------------------------------------------------------------------------------------------------

class SlowConsumer : public CountingConsumer
{
public:
    void consume(int queueId, const int& value) override
    {
        CountingConsumer::consume(queueId, value);
        std::this_thread::sleep_for(10ms);
    }
};

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
class StoringConsumer : public Consumer<QueueId, Value>
{
public:
    using StoreType = std::vector<std::pair<QueueId, Value>>;

    void consume(QueueId queueId, const Value& value) override
    {
        Consumer<QueueId, Value>::consume(queueId, value);
        std::lock_guard<std::mutex> lock(m_mutex);
        m_store.push_back(std::make_pair(queueId, value));
    }

    void takeStore(StoreType& store)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_store.swap(store);
    }

private:
    StoreType m_store;
    std::mutex m_mutex;
};

//------------------------------------------------------------------------------------------------------------

class UnitTests
{
public:
    void testLocalHelpers()
    {
        {
            IntProcessor proc(1);
            fillIntQueue(proc, 3, 100);
            assert(proc.m_cores.size() == 1);
            IntCore& core = *proc.m_cores.front();
            assert(core.m_queues.size() == 1);
            assert(core.getQueue(3).Values.size() == 100);
        }

        {
            IntProcessor proc(1);
            fillIntQueues(proc, 3, 100);
            assert(proc.m_cores.size() == 1);
            IntCore& core = *proc.m_cores.front();
            assert(core.m_queues.size() == 3);
            assert(core.getQueue(0).Values.size() == 100);
            assert(core.getQueue(1).Values.size() == 100);
            assert(core.getQueue(2).Values.size() == 100);
        }

        {
            IntProcessor proc(1);
            fillIntQueues(proc, 1'000, 3, 100);
            assert(proc.m_cores.size() == 1);
            IntCore& core = *proc.m_cores.front();
            assert(core.m_queues.size() == 3);
            assert(core.getQueue(1'000).Values.size() == 100);
            assert(core.getQueue(1'001).Values.size() == 100);
            assert(core.getQueue(1'002).Values.size() == 100);
        }
    }

    //------------------------------------------------------------------------------------------------------------

    void testTemplateParams()
    {
        Processor<int, int> proc1(1);
        Processor<float, int> proc2(1);
        Processor<int, double> proc3(1);
        Processor<int, std::wstring> proc4(1);
        Processor<std::string, std::wstring> proc5(1);

        struct A { int a; char b; std::string c; };
        Processor<int, A> proc6(1);
    }

    //------------------------------------------------------------------------------------------------------------

    void testThreadsStarted()
    {
        IntProcessor proc(3);

        assert(proc.m_cores.size() == 3);

        std::for_each(proc.m_cores.begin(), proc.m_cores.end(),
            [=] (const auto& corePtr)
            {
                assert(corePtr->m_thread.joinable());
                assert(corePtr->m_queues.empty());
            }
        );
    }

    //------------------------------------------------------------------------------------------------------------

    void testValuesWithoutConsumers()
    {
        IntProcessor proc(1);
        proc.addValue(1, 1);
        proc.addValue(1, 2);
        proc.addValue(2, 3);

        assert(proc.m_cores.size() == 1);
        IntCore& core = *proc.m_cores.front();
        assert(core.m_queues.size() == 2);
        assert(core.getQueue(1).Consumer.expired());
        assert(core.getQueue(1).Values.size() == 2);
        assert(core.getQueue(2).Consumer.expired());
        assert(core.getQueue(2).Values.size() == 1);
    }

    //------------------------------------------------------------------------------------------------------------

    void testConsumersWithoutValues()
    {
        auto cons = std::make_shared<IntConsumer>();
        IntProcessor proc(1);
        proc.setConsumer(1, cons);
        proc.setConsumer(2, cons);

        assert(proc.m_cores.size() == 1);
        IntCore& core = *proc.m_cores.front();
        assert(core.m_queues.size() == 2);
        assert(!core.getQueue(1).Consumer.expired());
        assert(core.getQueue(1).Values.empty());
        assert(!core.getQueue(2).Consumer.expired());
        assert(core.getQueue(2).Values.empty());
    }

    //------------------------------------------------------------------------------------------------------------

    void testRemoveOldOverflowPolicy()
    {
        IntProcessor proc(OverflowPolicy::RemoveOld, 1, 10);
        fillIntQueue(proc, 1, 11);

        assert(proc.m_cores.size() == 1);
        IntCore& core = *proc.m_cores.front();
        assert(core.getQueue(1).Values.size() == 10);
        assert(core.getQueue(1).Values.front() == 1);
        assert(core.getQueue(1).Values.back() == 10);
    }

    //------------------------------------------------------------------------------------------------------------

    void testRemoveNewOverflowPolicy()
    {
        IntProcessor proc(OverflowPolicy::RemoveNew, 1, 10);
        fillIntQueue(proc, 1, 11);

        assert(proc.m_cores.size() == 1);
        IntCore& core = *proc.m_cores.front();
        assert(core.getQueue(1).Values.size() == 10);
        assert(core.getQueue(1).Values.front() == 0);
        assert(core.getQueue(1).Values.back() == 10);
    }

    //------------------------------------------------------------------------------------------------------------

    void testIgnoreNewOverflowPolicy()
    {
        IntProcessor proc(OverflowPolicy::IgnoreNew, 1, 10);
        fillIntQueue(proc, 1, 11);

        assert(proc.m_cores.size() == 1);
        IntCore& core = *proc.m_cores.front();
        assert(core.getQueue(1).Values.size() == 10);
        assert(core.getQueue(1).Values.front() == 0);
        assert(core.getQueue(1).Values.back() == 9);
    }

    //------------------------------------------------------------------------------------------------------------

    void testManyQueues()
    {
        IntProcessor proc;
        const int coreCount = ( int )proc.m_cores.size();
        const int queueCount = 1'000 + coreCount - 1;
        fillIntQueues(proc, queueCount, 1'000);

        for (int iCore = 0; iCore < coreCount; ++iCore)
        {
            IntCore& core = *(proc.m_cores.at(iCore));
            assert(( int )core.m_queues.size() >= (queueCount / coreCount) - 1);
            assert(( int )core.m_queues.size() <= (queueCount / coreCount) + 2);

            for (const auto& pair : core.m_queues)
            {
                assert(pair.second.Values.size() == 1'000);
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------

    void testCountingConsumer()
    {
        auto cons = std::make_shared<CountingConsumer>();
        IntProcessor proc(2);
        proc.setConsumer(1, cons);
        fillIntQueue(proc, 1, 1'000);
        std::this_thread::sleep_for(100ms);
        assert(cons->getCounter() == 1'000);
    }

    //------------------------------------------------------------------------------------------------------------

    void testSlowConsumer()
    {
        auto cons = std::make_shared<SlowConsumer>();
        IntProcessor proc(2);
        proc.setConsumer(1, cons);
        fillIntQueue(proc, 1, 1'000);
        std::this_thread::sleep_for(100ms);
        assert(cons->getCounter() > 5);
        assert(cons->getCounter() < 15);
    }

    //------------------------------------------------------------------------------------------------------------

    void testDestroyedConsumer()
    {
        auto cons = std::make_shared<SlowConsumer>();
        IntProcessor proc;
        proc.setConsumer(1, cons);
        fillIntQueue(proc, 1, 1'000);
        cons.reset();
        std::this_thread::sleep_for(100ms);
    }

    //------------------------------------------------------------------------------------------------------------

    void testManyProvidersSingleConsumer()
    {
        auto cons = std::make_shared<CountingConsumer>();
        IntProcessor proc;

        for (int i = 0; i < 20; ++i)
        {
            proc.setConsumer(i, cons);
        }

        std::vector<std::thread> providers(10);
        std::generate(providers.begin(), providers.end(), [&] { return std::thread([&] { fillIntQueues(proc, 20, 100); }); });
        std::for_each(providers.begin(), providers.end(), [] (std::thread& thread) { thread.join(); });
        assert(cons->getCounter() > 19'000);
        assert(cons->getCounter() <= 20'000);
    }

    //------------------------------------------------------------------------------------------------------------

    void testManyProvidersManyConsumers()
    {
        std::vector<std::shared_ptr<CountingConsumer>> consumers(20);
        std::generate(consumers.begin(), consumers.end(), [] { return std::make_shared<CountingConsumer>(); });
        IntProcessor proc;

        for (size_t i = 0; i < consumers.size(); ++i)
        {
            proc.setConsumer(( int )i, consumers[i]);
        }

        std::vector<std::thread> providers(10);
        std::generate(providers.begin(), providers.end(), [&] { return std::thread([this, &proc] { fillIntQueues(proc, 20, 100); }); });
        std::for_each(providers.begin(), providers.end(), [] (std::thread& thread) { thread.join(); });
        std::this_thread::sleep_for(100ms);
        std::for_each(consumers.begin(), consumers.end(), [] (const auto& spCons) { assert(spCons->getCounter() == 1'000); });
    }

    //------------------------------------------------------------------------------------------------------------

    void testStoredValues()
    {
        auto cons = std::make_shared<StoringConsumer<int, std::string>>();
        Processor<int, std::string> proc(1);
        proc.setConsumer(1, cons);
        proc.setConsumer(2, cons);
        proc.addValue(1, "abc");
        proc.addValue(1, "def");
        proc.addValue(2, "ghi");
        std::this_thread::sleep_for(100ms);

        decltype(cons)::element_type::StoreType store;
        cons->takeStore(store);
        const auto it1 = std::find(store.begin(), store.end(), std::make_pair(1, std::string("abc")));
        const auto it2 = std::find(store.begin(), store.end(), std::make_pair(1, std::string("def")));
        const auto it3 = std::find(store.begin(), store.end(), std::make_pair(2, std::string("ghi")));
        assert(store.size() == 3);
        assert(it1 != store.end());
        assert(it2 != store.end());
        assert(it3 != store.end());
        assert(it1 < it2);
    }

    //------------------------------------------------------------------------------------------------------------

    void testSuspendResume()
    {
        auto cons = std::make_shared<CountingConsumer>();
        IntProcessor proc;
        proc.setConsumer(1, cons);
        proc.setConsumer(2, cons);
        proc.setConsumer(3, cons);
        proc.setConsumer(4, cons);

        proc.suspend();
        fillIntQueue(proc, 1, 1'000);
        std::this_thread::sleep_for(100ms);
        assert(cons->getCounter() == 0);

        proc.resume();
        std::this_thread::sleep_for(100ms);
        assert(cons->getCounter() == 1'000);

        fillIntQueue(proc, 2, 1'000);
        fillIntQueue(proc, 3, 1'000);
        std::this_thread::sleep_for(100ms);
        assert(cons->getCounter() == 3'000);

        proc.suspend();
        fillIntQueue(proc, 4, 1'000);
        std::this_thread::sleep_for(100ms);
        assert(cons->getCounter() == 3'000);
    }

    //------------------------------------------------------------------------------------------------------------

    void testQueueRotation()
    {
        auto cons = std::make_shared<StoringConsumer<int, int>>();
        IntProcessor proc(1);
        proc.suspend();
        proc.setConsumer(1, cons);
        proc.setConsumer(2, cons);
        fillIntQueue(proc, 1, 3);
        fillIntQueue(proc, 2, 3);
        proc.resume();
        std::this_thread::sleep_for(100ms);

        decltype(cons)::element_type::StoreType store;
        cons->takeStore(store);
        assert(store.size() == 6);
        std::map<int, std::vector<int>> values;
        int queueId = store[1].first;

        for (const auto& pair : store)
        {
            assert(queueId != pair.first);
            queueId = pair.first;
            values[queueId].push_back(pair.second);
        }

        for (const auto& pair : values)
        {
            assert(std::is_sorted(pair.second.begin(), pair.second.end()));
        }
    }

    //------------------------------------------------------------------------------------------------------------

    void testEmptyQueueAndFullQueue()
    {
        auto cons = std::make_shared<CountingConsumer>();
        IntProcessor proc(1);
        proc.setConsumer(1, cons);
        proc.setConsumer(2, cons);
        fillIntQueue(proc, 2, 1'000);
        std::this_thread::sleep_for(100ms);
        assert(cons->getCounter() == 1'000);
    }

    //------------------------------------------------------------------------------------------------------------

    void testFullQueueAndEmptyQueue()
    {
        auto cons = std::make_shared<CountingConsumer>();
        IntProcessor proc(1);
        proc.setConsumer(1, cons);
        proc.setConsumer(2, cons);
        fillIntQueue(proc, 1, 1'000);
        std::this_thread::sleep_for(100ms);
        assert(cons->getCounter() == 1'000);
    }

    //------------------------------------------------------------------------------------------------------------

    void testCoreQueueIterator()
    {
        auto cons = std::make_shared<SlowConsumer>();
        IntProcessor proc(1);

        for (int i = 0; i < 4; ++i)
        {
            proc.setConsumer(i, cons);
        }

        fillIntQueues(proc, 4, 100);
        assert(proc.m_cores.size() == 1);
        IntCore& core = *proc.m_cores.front();

        for (int i = 10; i < 80; ++i)
        {
            proc.suspend();
            std::unique_lock<std::mutex> lock(core.m_mutex);
            const auto queueIt = core.m_currQueueIt;
            lock.unlock();
            proc.setConsumer(i, cons);
            assert(core.m_currQueueIt == queueIt);
            proc.resume();
            std::this_thread::sleep_for(10ms);
        }
    }

    //------------------------------------------------------------------------------------------------------------

private:
    void fillIntQueue(IntProcessor& proc, int queueId, int valueCount)
    {
        for (int i = 0; i < valueCount; ++i)
        {
            proc.addValue(queueId, i);
        }
    }

    //------------------------------------------------------------------------------------------------------------

    void fillIntQueues(IntProcessor& proc, int queueStartId, int queueCount, int valueCount)
    {
        for (int queueId = queueStartId; queueId < queueStartId + queueCount; ++queueId)
        {
            fillIntQueue(proc, queueId, valueCount);
        }
    }

    //------------------------------------------------------------------------------------------------------------

    void fillIntQueues(IntProcessor& proc, int queueCount, int valueCount)
    {
        fillIntQueues(proc, 0, queueCount, valueCount);
    }
};

//------------------------------------------------------------------------------------------------------------

int main()
{
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);

    //for (int i = 0; i < 10; ++i)
    //for (;;)
    {
        const long long startMs = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();

        UnitTests tests;
        tests.testLocalHelpers();
        tests.testTemplateParams();
        tests.testThreadsStarted();
        tests.testValuesWithoutConsumers();
        tests.testConsumersWithoutValues();
        tests.testRemoveOldOverflowPolicy();
        tests.testRemoveNewOverflowPolicy();
        tests.testIgnoreNewOverflowPolicy();
        tests.testManyQueues();
        tests.testCountingConsumer();
        tests.testSlowConsumer();
        tests.testDestroyedConsumer();
        tests.testManyProvidersSingleConsumer();
        tests.testManyProvidersManyConsumers();
        tests.testStoredValues();
        tests.testSuspendResume();
        tests.testQueueRotation();
        tests.testEmptyQueueAndFullQueue();
        tests.testFullQueueAndEmptyQueue();
        tests.testCoreQueueIterator();

        const long long diffMs = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count() - startMs;
        std::wcout << diffMs << L" ms" << std::endl;
    }

#ifdef _DEBUG
    std::wcout << L"\nPress any key to continue..." << std::endl;
    _getch();
#endif

    return 0;
}

//------------------------------------------------------------------------------------------------------------
