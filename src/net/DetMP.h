/*
    Copyright 2016-2026 melonDS team

    This file is part of melonDS.

    melonDS is free software: you can redistribute it and/or modify it under
    the terms of the GNU General Public License as published by the Free
    Software Foundation, either version 3 of the License, or (at your option)
    any later version.

    melonDS is distributed in the hope that it will be useful, but WITHOUT ANY
    WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
    FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with melonDS. If not, see http://www.gnu.org/licenses/.
*/

#ifndef DETMP_H
#define DETMP_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <set>
#include <vector>

#include "types.h"
#include "MPInterface.h"

namespace melonDS
{

// Local multiplayer between instances of one process, resolved on emulated
// time instead of wall-clock time.
//
// LocalMP waits on semaphores with a real-time timeout, so whether a frame
// makes it across depends on how the OS schedules the emulator threads. That
// is harmless between two windows on one machine, and fatal to netplay, where
// every machine emulates every console and all of them must compute the same
// thing.
//
// Here every instance publishes its emulated clock (system cycles since the
// session base). A receive at time t first waits until every other instance
// has reached t, then hands out, in (time, sender, sequence) order, only the
// frames sent before t. The outcome depends on the emulated clocks alone,
// which is conservative parallel discrete-event simulation. The host waiting
// for MP replies publishes a short lookahead, since it sends nothing before
// its reply window closes; without it host and clients would wait on each
// other forever.
class DetMP : public MPInterface
{
public:
    DetMP() noexcept;
    ~DetMP() noexcept override = default;

    void Process() override {}

    void Begin(int inst) override;
    void End(int inst) override;

    void SetNow(int inst, u64 systime) override;
    void Leave(int inst) override;
    void ResetSession(int inst, u64 systime) override;

    int SendPacket(int inst, u8* data, int len, u64 timestamp) override;
    int RecvPacket(int inst, u8* data, u64* timestamp) override;
    int SendCmd(int inst, u8* data, int len, u64 timestamp) override;
    int SendReply(int inst, u8* data, int len, u64 timestamp, u16 aid) override;
    int SendAck(int inst, u8* data, int len, u64 timestamp) override;
    int RecvHostPacket(int inst, u8* data, u64* timestamp) override;
    u16 RecvReplies(int inst, u8* data, u64 timestamp, u16 aidmask) override;

private:
    struct Frame
    {
        u64 Time;       // emulated time it was sent at, session cycles
        u32 Sender;
        u64 Seq;        // per-sender order, breaks ties deterministically
        u32 Type;       // 0=regular 1=CMD 2=reply 3=ack, aid in the top 16 bits
        u64 Timestamp;  // the sender's wifi microsecond clock
        u64 CmdSeq;     // replies: the CMD they answer
        std::vector<u8> Data;

        bool operator<(const Frame& o) const
        {
            if (Time != o.Time) return Time < o.Time;
            if (Sender != o.Sender) return Sender < o.Sender;
            return Seq < o.Seq;
        }
    };

    static constexpr int kMaxInst = 16;
    // 100 us: a host sends its ack no earlier than 112 us after the CMD.
    static constexpr u64 kReplyLookahead = 3351;

    u64 Now(int inst) const { return Clock[inst].load(std::memory_order_relaxed); }
    void Publish(int inst, u64 t);
    void WaitOthers(std::unique_lock<std::mutex>& lk, int inst, u64 t);
    int Send(int inst, u32 type, u8* data, int len, u64 timestamp);
    int Recv(int inst, u8* data, u64* timestamp);

    std::mutex Lock;
    std::condition_variable Cond;

    std::atomic<u64> Clock[kMaxInst];
    std::atomic<u16> Participants {0};
    std::atomic<int> Waiters {0};

    u64 Base[kMaxInst] {};
    u64 LastSys[kMaxInst] {};
    u64 SendSeq[kMaxInst] {};
    u64 PoweredSince[kMaxInst] {};
    bool Powered[kMaxInst] {};
    // false between the start of a session and the instance joining it: its
    // clock still counts from the old base and must not be believed
    std::atomic<bool> Synced[kMaxInst];
    bool InSession = false;
    std::multiset<Frame> Inbox[kMaxInst];
    std::multiset<Frame> Replies[kMaxInst];

    int HostInst = -1;
    // the last CMD each instance received, so its reply answers that one
    // and not whatever the host sent since
    u64 RxCmdSeq[kMaxInst] {};
    int RxCmdHost[kMaxInst] {};
    u64 CmdSeq = 0;
    u64 MyCmdSeq[kMaxInst] {};

    // bench counters, printed every few seconds of wall time
    u64 StatCmd = 0, StatWant = 0, StatGot = 0, StatBlank = 0, StatLate = 0, StatWaitCmd = 0, StatRxCmd = 0, StatRecv = 0;
    u64 StatLastPrint = 0;
    void Stats();
};

}

#endif
