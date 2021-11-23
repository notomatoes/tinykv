// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/gogo/protobuf/sortkeys"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// 随机选举超时时间,防止选票无限瓜分,导致死锁
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	// 配置节点
	peers := c.peers

	raft := &Raft{
		id:                    c.ID,
		RaftLog:               raftLog,
		Prs:                   make(map[uint64]*Progress),
		votes:                 make(map[uint64]bool),
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
	}

	for _, p := range peers {
		raft.Prs[p] = &Progress{Next: 1}
	}

	// 加载配置信息
	hs, _, _ := raft.RaftLog.storage.InitialState()
	raft.Term = hs.GetTerm()
	raft.Vote = hs.GetVote()

	return raft
}

// sendMsgVote 用来发送请求投票的消息
func (r *Raft) sendMsgRequestVote(to uint64, index uint64, term uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Index:   index,
		LogTerm: term,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// 保护性检查
	if len(r.RaftLog.entries) < 0 {
		return false
	}
	// 日志相关
	pr := r.Prs[to]
	logTerm, err := r.RaftLog.Term(pr.Next - 1)
	if err != nil {
		return false
	}

	ents := r.RaftLog.GetEntries(pr.Next, r.RaftLog.LastIndex())
	entries := make([]*pb.Entry, 0)

	for _, ent := range ents {
		entries = append(entries, &pb.Entry{
			EntryType: ent.EntryType,
			Term:      ent.Term,
			Index:     ent.Index,
			Data:      ent.Data,
		})
	}
	// 封装的消息
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: logTerm,
		Entries: entries,
		Index:   pr.Next - 1,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	// 对于leader,需要通过心跳维持领导地位
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			msg := pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				Term:    r.Term,
			}
			r.Step(msg)
		}
	// 对于follower和candidate需要检查electionElapse来决定是否需要选举
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
			}
			r.Step(msg)
		}
	}
}

func (r *Raft) reset(term uint64) {
	r.Lead = None
	r.Vote = None

	r.Term = term
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	r.votes = make(map[uint64]bool)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)

	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)

	// 自己给自己投票
	r.Vote = r.id
	r.votes[r.id] = true
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.Lead = r.id
	r.Vote = r.id
	r.votes[r.id] = true
	r.State = StateLeader

	// 初始化follower的Progress
	last := r.RaftLog.LastIndex()
	for p := range r.Prs {
		if p == r.id {
			r.Prs[p].Match = last + 1
			r.Prs[p].Next = last + 2
		} else {
			r.Prs[p].Match = 0
			r.Prs[p].Next = last + 1
		}
	}
	// 添加空的日志
	r.appendEntry(&pb.Entry{Data: nil})

	// 广播所有的follower
	for p := range r.Prs {
		if p != r.id {
			r.sendAppend(p)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateFollower:
		stepFollower(r, m)
	case StateCandidate:
		stepCandidate(r, m)
	case StateLeader:
		stepLeader(r, m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.Lead == None && r.Term == m.Term && r.Vote == m.From {
		r.Lead = m.From
	}

	msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse,
		To:     m.From,
		From:   r.id,
		Term:   r.Term,
		Index:  m.Index,
		Reject: false,
	}
	// 从哪里开始的日志条目
	prevLogTerm := m.LogTerm
	// 这一组日志对应的Term
	prevLogIndex := m.Index

	// 若接受者的日志没有那么长,直接返回
	if prevLogIndex > r.RaftLog.LastIndex() {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	term, _ := r.RaftLog.Term(prevLogIndex)

	if m.Term < r.Term || prevLogTerm != term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	// 处理冲突的entry,若term不同,则更新的值一定是m中传递过来的
	flag := false
	tmp := make([]pb.Entry, 0)
	for _, ent := range m.Entries {
		curTerm, _ := r.RaftLog.Term(ent.Index)
		if curTerm != ent.Term {
			flag = true
		}
		if flag {
			tmp = append(tmp, *ent)
		}
	}
	r.RaftLog.append(tmp...)

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	msg.Index = r.RaftLog.LastIndex()

	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}

	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.randomElectionTimeout)
	msg.Reject = false

	r.msgs = append(r.msgs, msg)

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// steps
func stepFollower(r *Raft, m pb.Message) {
	switch m.MsgType {
	// 准备选举
	case pb.MessageType_MsgHup:
		// 若只有自己这一个节点,直接成为leader
		if len(r.Prs) == 1 {
			r.becomeCandidate()
			r.becomeLeader()
			r.RaftLog.committed = r.Prs[r.id].Match

			break
		}

		r.becomeCandidate()
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)

		for p := range r.Prs {
			if p != r.id {
				r.sendMsgRequestVote(p, lastIndex, lastLogTerm)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		if (r.Vote == None || r.Vote == m.From || m.Term > r.Term) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			//fmt.Println("id: ", r.id, " from: ", m.From)
			r.Vote = m.From
			msg.Reject = false
		}
		r.msgs = append(r.msgs, msg)

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
}
func stepCandidate(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)

		for p := range r.Prs {
			if p != r.id {
				r.sendMsgRequestVote(p, lastIndex, lastLogTerm)
			}
		}
	// 收到append,说明集群中已有了leader
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)

	// 收到heartbeat,说明集群中已有了leader
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)

	// 收到snapshot,说明集群中已有了leader
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)

	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
		}
		if r.Vote == None || m.Term > r.Term {
			//fmt.Println("id: ", r.id, " from: ", m.From)
			r.Vote = m.From
			msg.Reject = false
		}
		r.msgs = append(r.msgs, msg)

	// 计算自己的票数
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term > r.Term || r.Vote != r.id {
			return
		}
		r.votes[m.From] = !m.Reject
		agree, reject := 0, 0
		for _, v := range r.votes {
			if v {
				agree += 1
			} else {
				reject += 1
			}
		}
		//fmt.Println("id:", r.id, "agree:", agree)
		// 获得多数的投票,进化为leader
		if agree > len(r.Prs)/2 {
			r.becomeLeader()
		} else if reject > len(r.Prs)/2 { // 若大多数节点拒绝,则退化为follower
			r.becomeFollower(m.Term, None)
		}
	}
}

func stepLeader(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for p := range r.Prs {
			if p != r.id {
				r.sendHeartbeat(p)
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)

	case pb.MessageType_MsgPropose:
		r.appendEntry(m.Entries...)
		for p := range r.Prs {
			if p != r.id {
				r.sendAppend(p)
			}
		}
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.Prs[r.id].Match
		}
	case pb.MessageType_MsgAppendResponse:

		// 若拒绝append消息,说明term,index不匹配,则将索引-1,再继续请求
		if m.Reject {
			// TODO: 增加一个rejectHint来告诉Leader它上面保存的最大索引
			idx := max(1, m.Index-1)
			r.Prs[m.From].Next = idx
			r.sendAppend(m.From)
		} else { // 若是接受的消息,则更新当前节点的Match和Next,同时统计是否有超过半数的提交
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1

			// 统计所有的prs的match
			matches := make([]uint64, 0, len(r.Prs))
			for p := range r.Prs {
				matches = append(matches, r.Prs[p].Match)
			}
			// 若超过半数的节点提交,则更改committed,同时广播
			sortkeys.Uint64s(matches)
			mid := matches[uint64((len(matches)-1)/2)]
			midTerm, err := r.RaftLog.Term(mid)

			if err != nil {
				return
			}
			// 更新committed需要满足:
			// 1.最多的投票的match > r.RaftLog.committed(防止多次广播)
			// 2.超过半数的投票
			// 3.对应日志的term等于当前Leader的term
			if mid > r.RaftLog.committed && midTerm == r.Term {
				r.RaftLog.committed = mid

				// 广播append
				for p := range r.Prs {
					if p != r.id {
						r.sendAppend(p)
					}
				}
			}
		}
	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		if r.Vote == None || m.Term > r.Term {
			//fmt.Println("id: ", r.id, " from: ", m.From)
			r.Vote = m.From
			msg.Reject = false
		}
		r.msgs = append(r.msgs, msg)
	}
}

// 批量添加Entries
func (r *Raft) appendEntry(es ...*pb.Entry) {
	p := r.RaftLog.LastIndex()

	ents := make([]pb.Entry, 0)
	// 设置entries的term以及index
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = p + uint64(i) + 1
		ents = append(ents, *es[i])
	}
	r.RaftLog.append(ents...)

	// 更新本节点的Next和Match索引
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// GetEntries 获取日志
func (l *RaftLog) GetEntries(lo uint64, hi uint64) []pb.Entry {
	if lo >= l.first && hi <= l.LastIndex() {
		return l.entries[lo-l.first : hi-l.first+1]
	}
	return nil
}

func (r *Raft) SoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) HardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
