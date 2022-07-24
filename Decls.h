//------------------------------------------------------------------------------------------------------------
// Confidential and proprietary source code of BEOALEX.
// This source code may not be used, reproduced or disclosed in any manner without permission.
// Copyright (c) 2022, BEOALEX. All rights reserved.
// https://github.com/beoalex
//------------------------------------------------------------------------------------------------------------

#pragma once

//------------------------------------------------------------------------------------------------------------

template<typename QueueId, typename Value>
class Consumer
{
public:
    virtual ~Consumer() = default;
    virtual void consume(QueueId, const Value&) {}
};

//------------------------------------------------------------------------------------------------------------

struct Const
{
    enum : unsigned int
    {
        ThreadCountNotSpecified = 0,
        DefaultMaxQueueSize = 10'000,
        UnlimitedMaxQueueSize = 0,
    };
};

//------------------------------------------------------------------------------------------------------------

enum class OverflowPolicy
{
    RemoveOld,
    RemoveNew,
    IgnoreNew,

    Default = RemoveOld,
};

//------------------------------------------------------------------------------------------------------------
