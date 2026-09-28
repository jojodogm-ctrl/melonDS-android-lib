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

#include <chrono>
#include <cstring>

#include "DetMP.h"
#include "Platform.h"

namespace melonDS
{

using Platform::Log;
using Platform::LogLevel;

static constexpr u32 kMaxFrameLen = 0x948;
static constexpr u64 kSysClock = 33513982;

DetMP::DetMP() noexcept
{
    for (int i = 0; i < kMaxInst; i++)
    {
        Clock[i].store(0);
        Synced[i].store(true);
    }

    Log(LogLevel::Info, "MP comm init OK (deterministic)\n");
}

void DetMP::Publish(int inst, u64 t)
{
    u64 cur = Clock[inst].load(std::memory_order_relaxed);
    if (t <= cur) return;
    Clock[inst].store(t);

    // A waiter registers in Waiters BEFORE checking the clocks, and both
    // sides use sequentially consistent operations: either it sees this
    // store, or we see it registered and wake it. Registering after the
    // check lost wakeups, and two instances then sat waiting on each other
    // at the same instant until the 2 s timeout. Measured: "waited 2005 ms
    // ... behind by 0.00 frames", on both instances at once, every few
    // seconds of a race. Taking the lock before notifying keeps the wakeup
    // from landing between its check and its sleep.
    if (Waiters.load())
    {
        { std::lock_guard<std::mutex> lk(Lock); }
        Cond.notify_all();
    }
}

void DetMP::SetNow(int inst, u64 systime)
{
    if (inst < 0 || inst >= kMaxInst) return;

    if (!(Participants.load() & (1 << inst)))
    {
        std::lock_guard<std::mutex> lk(Lock);
        if (InSession) Synced[inst].store(false);
        Participants.fetch_or(1 << inst);
    }

    LastSys[inst] = systime;
    if (!Synced[inst].load(std::memory_order_acquire)) return;
    u64 t = (systime > Base[inst]) ? (systime - Base[inst]) : 0;
    Publish(inst, t);
}

void DetMP::ResetSession(int inst, u64 systime)
{
    // Every machine calls this for every instance at the shared starting
    // point, so they all agree on time zero and on an empty air.
    std::lock_guard<std::mutex> lk(Lock);

    // The first instance to join a session holds every other one at time
    // zero until it joins too. Otherwise their clocks, still counted from
    // before, would look far ahead, and whoever joined first would run past
    // them on air nobody has decided yet.
    if (!InSession)
    {
        InSession = true;
        u16 mask = Participants.load();
        for (int j = 0; j < kMaxInst; j++)
        {
            if (j == inst || !(mask & (1 << j))) continue;
            Synced[j].store(false);
            Clock[j].store(0);
        }
    }
    Synced[inst].store(true);

    Base[inst] = systime;
    LastSys[inst] = systime;
    Clock[inst].store(0);
    SendSeq[inst] = 0;
    Inbox[inst].clear();
    Replies[inst].clear();
    Participants.fetch_or(1 << inst);
    if (HostInst == inst) HostInst = -1;
    RxCmdSeq[inst] = 0;
    MyCmdSeq[inst] = 0;
    // The wifi of a console joining from a savestate may already be on, and
    // it announced itself through Begin to whatever interface came before
    // this one: take it as powered from time zero. The game powering it off
    // or on later goes through End and Begin as usual.
    Powered[inst] = true;
    PoweredSince[inst] = 0;
    Cond.notify_all();
}

void DetMP::Leave(int inst)
{
    std::lock_guard<std::mutex> lk(Lock);
    Participants.fetch_and(~(1 << inst));
    Powered[inst] = false;
    Inbox[inst].clear();
    Replies[inst].clear();
    Cond.notify_all();
}

void DetMP::Begin(int inst)
{
    std::lock_guard<std::mutex> lk(Lock);
    Powered[inst] = true;
    PoweredSince[inst] = Now(inst);
}

void DetMP::End(int inst)
{
    std::lock_guard<std::mutex> lk(Lock);
    Powered[inst] = false;
    Inbox[inst].clear();
    Replies[inst].clear();
}

void DetMP::WaitOthers(std::unique_lock<std::mutex>& lk, int inst, u64 t)
{
    auto ready = [&]()
    {
        u16 mask = Participants.load();
        for (int j = 0; j < kMaxInst; j++)
        {
            if (j == inst || !(mask & (1 << j))) continue;
            if (Clock[j].load() < t) return false;
        }
        return true;
    };

    Waiters.fetch_add(1);
    if (ready()) { Waiters.fetch_sub(1); return; }

    // who holds us up, and by how much emulated time, for the bench
    int slow = -1;
    u64 slowclock = 0;
    {
        u16 mask = Participants.load();
        for (int j = 0; j < kMaxInst; j++)
        {
            if (j == inst || !(mask & (1 << j))) continue;
            u64 c = Clock[j].load();
            if (c < t && (slow < 0 || c < slowclock)) { slow = j; slowclock = c; }
        }
    }
    auto begin = std::chrono::steady_clock::now();

    while (!ready())
    {
        if (Cond.wait_for(lk, std::chrono::seconds(2)) == std::cv_status::timeout && !ready())
        {
            Log(LogLevel::Warn, "detmp: inst%d still waiting for t=%llu\n", inst, (unsigned long long)t);
        }
    }
    Waiters.fetch_sub(1);

    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();
    if (ms >= 50)
        printf("[detmp] @%lld inst%d waited %lld ms on inst%d, behind by %.2f frames (t=%llu)\n",
               (long long)(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count() % 1000000),
               inst, (long long)ms, slow, (double)(t - slowclock) / 560190.0, (unsigned long long)t);
}

int DetMP::Send(int inst, u32 type, u8* data, int len, u64 timestamp)
{
    if (len < 0 || (u32)len > kMaxFrameLen) return 0;

    std::lock_guard<std::mutex> lk(Lock);

    Frame f;
    // Clock may sit past the real time after a reply window was promised;
    // stamping with it keeps the promise and stays the same on every machine.
    f.Time = Now(inst);
    f.Sender = inst;
    f.Seq = SendSeq[inst]++;
    f.Type = type;
    f.Timestamp = timestamp;
    f.CmdSeq = 0;
    if (len && data) f.Data.assign(data, data + len);

    u32 kind = type & 0xFFFF;
    if (kind == 1)
    {
        f.CmdSeq = ++CmdSeq;
        HostInst = inst;
        MyCmdSeq[inst] = f.CmdSeq;
    }

    if (kind == 2)
    {
        int host = RxCmdHost[inst];
        if (!RxCmdSeq[inst] || host < 0 || host == inst) return len;
        f.CmdSeq = RxCmdSeq[inst];
        Replies[host].insert(std::move(f));
    }
    else
    {
        u16 mask = Participants.load();
        for (int j = 0; j < kMaxInst; j++)
        {
            if (j == inst || !(mask & (1 << j))) continue;

            // an instance with its wifi off drops everything older than its
            // clock the moment it powers on, so there is no point keeping it
            if (!Powered[j])
            {
                u64 cj = Now(j);
                auto& box = Inbox[j];
                while (!box.empty() && box.begin()->Time < cj) box.erase(box.begin());
            }
            Inbox[j].insert(f);
        }
    }

    Cond.notify_all();
    return len;
}

int DetMP::Recv(int inst, u8* data, u64* timestamp)
{
    std::unique_lock<std::mutex> lk(Lock);

    u64 t = Now(inst);
    WaitOthers(lk, inst, t);

    auto& box = Inbox[inst];
    while (!box.empty())
    {
        auto it = box.begin();
        if (it->Time >= t) return 0;

        Frame f = std::move(const_cast<Frame&>(*it));
        box.erase(it);

        if (!Powered[inst] || f.Time < PoweredSince[inst]) continue;

        if ((f.Type & 0xFFFF) == 1)
        {
            RxCmdSeq[inst] = f.CmdSeq;
            StatRxCmd++;
            RxCmdHost[inst] = (int)f.Sender;
        }

        StatRecv++;
        u32 len = (u32)f.Data.size();
        if (len) memcpy(data, f.Data.data(), len);

        // A real receiver reads the sender's TSF as it is when the frame
        // lands, not when it left. Handing out the send-time value would put a
        // client's synced clock behind its host by however long the frame
        // waited here, and the host would then close its reply window long
        // before the client even looks at its CMD.
        if (timestamp)
        {
            u64 ts = f.Timestamp;
            if (ts) ts += ((t - f.Time) * 1000000) / kSysClock;
            *timestamp = ts;
        }
        return (int)len;
    }
    return 0;
}

int DetMP::SendPacket(int inst, u8* data, int len, u64 timestamp)
{
    return Send(inst, 0, data, len, timestamp);
}

int DetMP::RecvPacket(int inst, u8* data, u64* timestamp)
{
    return Recv(inst, data, timestamp);
}

int DetMP::SendCmd(int inst, u8* data, int len, u64 timestamp)
{
    return Send(inst, 1, data, len, timestamp);
}

int DetMP::SendReply(int inst, u8* data, int len, u64 timestamp, u16 aid)
{
    return Send(inst, 2 | (aid << 16), data, len, timestamp);
}

int DetMP::SendAck(int inst, u8* data, int len, u64 timestamp)
{
    return Send(inst, 3, data, len, timestamp);
}

int DetMP::RecvHostPacket(int inst, u8* data, u64* timestamp)
{
    return Recv(inst, data, timestamp);
}

void DetMP::Stats()
{
    u64 now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    if (now - StatLastPrint < 3000) return;
    StatLastPrint = now;
    printf("[detmp] cmd=%llu want=%llu got=%llu blank=%llu late=%llu waited=%llu rxcmd=%llu recv=%llu\n",
           (unsigned long long)StatCmd, (unsigned long long)StatWant, (unsigned long long)StatGot,
           (unsigned long long)StatBlank, (unsigned long long)StatLate, (unsigned long long)StatWaitCmd,
           (unsigned long long)StatRxCmd, (unsigned long long)StatRecv);
}

u16 DetMP::RecvReplies(int inst, u8* data, u64 timestamp, u16 aidmask)
{
    // Nothing leaves this console before its reply window closes, so it can
    // promise that much. The clients need the promise to run up to the CMD
    // and answer it while we wait for them.
    u64 t = Now(inst);
    u64 deadline = t + kReplyLookahead;
    Publish(inst, deadline);

    std::unique_lock<std::mutex> lk(Lock);
    u64 cmd = MyCmdSeq[inst];

    auto answered = [&](int j)
    {
        for (auto& f : Replies[inst])
            if (f.Sender == (u32)j && f.CmdSeq == cmd) return true;
        return false;
    };
    auto ready = [&]()
    {
        u16 mask = Participants.load();
        for (int j = 0; j < kMaxInst; j++)
        {
            if (j == inst || !(mask & (1 << j))) continue;
            if (Clock[j].load() >= deadline) continue;
            if (answered(j)) continue;
            return false;
        }
        return true;
    };

    StatCmd++;
    for (int a = 1; a < 16; a++) if (aidmask & (1 << a)) StatWant++;
    Waiters.fetch_add(1);
    if (!ready())
    {
        StatWaitCmd++;
        while (!ready())
        {
            if (Cond.wait_for(lk, std::chrono::seconds(2)) == std::cv_status::timeout && !ready())
                Log(LogLevel::Warn, "detmp: host inst%d still waiting for replies\n", inst);
        }
    }
    Waiters.fetch_sub(1);

    u16 ret = 0;
    auto& box = Replies[inst];
    for (auto it = box.begin(); it != box.end();)
    {
        if (it->CmdSeq > cmd) { ++it; continue; }

        if (it->CmdSeq == cmd && it->Time >= deadline) StatLate++;
        if (it->CmdSeq == cmd && it->Time < deadline && it->Data.empty()) StatBlank++;
        if (it->CmdSeq == cmd && it->Time < deadline && !it->Data.empty())
        {
            u32 aid = it->Type >> 16;
            if (aid >= 1 && aid <= 15)
            {
                memcpy(&data[(aid-1)*1024], it->Data.data(), it->Data.size());
                ret |= (1 << aid);
                StatGot++;
            }
        }
        it = box.erase(it);
    }

    Stats();
    return ret;
}

}
