"""Clustering and consensus support for PyQueue broker."""

import asyncio
import time
import json
from typing import List, Dict, Optional, Tuple
from enum import Enum
from .protocol_async import read_message_async, write_message_async
from .logger import Logger


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class LogEntry:
    """Raft log entry."""
    
    def __init__(self, term: int, index: int, command: str, data: dict):
        self.term = term
        self.index = index
        self.command = command
        self.data = data
    
    def to_dict(self):
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "data": self.data
        }
    
    @classmethod
    def from_dict(cls, d: dict):
        return cls(d["term"], d["index"], d["command"], d["data"])


class RaftConsensus:
    """Full Raft consensus algorithm for broker clustering with log replication (async)."""
    
    def __init__(self, node_id: str, host: str, port: int, cluster_nodes: List[Tuple[str, int]], journal_file: str = None, state_machine_callback=None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.cluster_nodes = cluster_nodes
        self.journal_file = journal_file
        self.state_machine_callback = state_machine_callback
        self.logger = Logger("raft")
        
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self.last_heartbeat_time = time.time()
        
        # Raft log
        self.log: List[LogEntry] = []  # Index 0 is unused (Raft uses 1-based indexing)
        self.commit_index = 0  # Highest log entry known to be committed
        self.last_applied = 0  # Highest log entry applied to state machine
        
        # Leader state (only valid when leader)
        self.next_index: Dict[Tuple[str, int], int] = {}  # For each follower, next log index to send
        self.match_index: Dict[Tuple[str, int], int] = {}  # For each follower, highest known replicated index
        
        self.election_timeout_base = 5.0  # seconds
        self.election_timeout = self._randomize_election_timeout()
        self.heartbeat_interval = 2.0  # seconds
        
        self.votes_received = 0
        self.vote_lock = None  # Will be asyncio.Lock after start()
        self.log_lock = None  # Will be asyncio.Lock after start()
        
        self.election_task = None
        self.heartbeat_task = None
        self.replication_task = None
        self.running = False
        self.event_loop = None
        
        # Snapshot support
        try:
            from .snapshot import SnapshotManager
            snapshot_interval = 3600
            snapshot_threshold = 1000
            self.snapshot_manager = SnapshotManager(
                snapshot_dir="snapshots",
                snapshot_interval=snapshot_interval,
                snapshot_threshold=snapshot_threshold
            )
        except ImportError:
            self.snapshot_manager = None
        
        # Load persisted log from journal
        self._load_log_from_journal()
    
    async def start(self):
        """Start the consensus algorithm (async)."""
        self.event_loop = asyncio.get_event_loop()
        self.vote_lock = asyncio.Lock()
        self.log_lock = asyncio.Lock()
        
        self.running = True
        self.election_task = asyncio.create_task(self._election_loop())
        
        if self.state == NodeState.LEADER:
            await self._become_leader()
        
        self.logger.info("Raft consensus started", node_id=self.node_id, state=self.state.value, 
                        log_length=len(self.log), commit_index=self.commit_index)
    
    def stop(self):
        """Stop the consensus algorithm."""
        self.running = False
        if self.election_task:
            self.election_task.cancel()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.replication_task:
            self.replication_task.cancel()
    
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        return self.state == NodeState.LEADER
    
    def _randomize_election_timeout(self) -> float:
        """Randomize election timeout to avoid split votes.
        
        Returns:
            Randomized timeout between base and 2*base
        """
        import random
        return self.election_timeout_base + random.uniform(0, self.election_timeout_base)
    
    async def _election_loop(self):
        """Background task for leader election."""
        while self.running:
            await asyncio.sleep(0.5)
            
            if self.state == NodeState.LEADER:
                continue
            
            current_time = time.time()
            time_since_heartbeat = current_time - self.last_heartbeat_time
            
            if time_since_heartbeat > self.election_timeout:
                await self._start_election()
                self.election_timeout = self._randomize_election_timeout()
    
    async def _start_election(self):
        """Start a new election."""
        self.logger.info("Starting election", node_id=self.node_id, term=self.current_term)
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1
        
        # Request votes from other nodes
        votes_needed = (len(self.cluster_nodes) + 1) // 2  # Majority
        
        vote_tasks = []
        for node_host, node_port in self.cluster_nodes:
            vote_tasks.append(asyncio.create_task(self._request_vote_with_retry(node_host, node_port)))
        
        # Wait for votes with timeout
        await asyncio.sleep(1.0)
        
        async with self.vote_lock:
            if self.votes_received > votes_needed:
                await self._become_leader()
            else:
                self.state = NodeState.FOLLOWER
                self.logger.info("Election lost", node_id=self.node_id, votes=self.votes_received)
    
    async def _request_vote_with_retry(self, node_host: str, node_port: int, max_retries: int = 3):
        """Request vote with retry logic.
        
        Args:
            node_host: Follower node host
            node_port: Follower node port
            max_retries: Maximum number of retry attempts
        """
        for attempt in range(max_retries):
            if await self._request_vote(node_host, node_port):
                return
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
    
    async def _become_leader(self):
        """Initialize leader state after election."""
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        
        # Initialize leader state for each follower
        async with self.log_lock:
            last_log_index = len(self.log)
            for node in self.cluster_nodes:
                self.next_index[node] = last_log_index + 1
                self.match_index[node] = 0
        
        self.logger.info("Elected as leader", node_id=self.node_id, term=self.current_term, 
                        last_log_index=last_log_index)
        
        # Start heartbeat and replication tasks
        if self.heartbeat_task is None or self.heartbeat_task.done():
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        if self.replication_task is None or self.replication_task.done():
            self.replication_task = asyncio.create_task(self._replication_loop())
    
    async def _request_vote(self, node_host: str, node_port: int) -> bool:
        """Request a vote from another node (async)."""
        try:
            reader, writer = await asyncio.open_connection(node_host, node_port)
            
            vote_request = {
                "type": "REQUEST_VOTE",
                "term": self.current_term,
                "candidate_id": self.node_id
            }
            
            await write_message_async(writer, "RAFT_VOTE", json.dumps(vote_request).encode('utf-8'))
            
            response_cmd, response_payload = await read_message_async(reader)
            writer.close()
            await writer.wait_closed()
            
            if response_cmd == "OK" and response_payload:
                response_data = json.loads(response_payload.decode('utf-8'))
                if response_data.get("vote_granted"):
                    async with self.vote_lock:
                        self.votes_received += 1
                    return True
            return False
        except Exception as e:
            self.logger.debug("Vote request failed", node=f"{node_host}:{node_port}", error=str(e))
            return False
    
    async def _heartbeat_loop(self):
        """Background task for sending heartbeats (leader only)."""
        while self.running and self.state == NodeState.LEADER:
            await asyncio.sleep(self.heartbeat_interval)
            await self._send_heartbeats()
    
    async def _send_heartbeats(self):
        """Send heartbeats to all followers."""
        tasks = []
        for node_host, node_port in self.cluster_nodes:
            tasks.append(asyncio.create_task(self._send_heartbeat(node_host, node_port)))
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_heartbeat(self, node_host: str, node_port: int):
        """Send a heartbeat (empty AppendEntries) to a follower node."""
        node = (node_host, node_port)
        await self._send_append_entries(node, entries=[], is_heartbeat=True)
    
    async def _send_append_entries(self, node: Tuple[str, int], entries: List[LogEntry], is_heartbeat: bool = False, max_retries: int = 3):
        """Send AppendEntries RPC to a follower with retry logic.
        
        Args:
            node: Follower node tuple (host, port)
            entries: Log entries to send
            is_heartbeat: True if this is a heartbeat (empty entries)
            max_retries: Maximum number of retry attempts
        """
        for attempt in range(max_retries):
            if await self._send_append_entries_once(node, entries, is_heartbeat):
                return
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
    
    async def _send_append_entries_once(self, node: Tuple[str, int], entries: List[LogEntry], is_heartbeat: bool = False) -> bool:
        """Send AppendEntries RPC to a follower (single attempt, async).
        
        Args:
            node: Follower node tuple (host, port)
            entries: Log entries to send
            is_heartbeat: True if this is a heartbeat (empty entries)
        
        Returns:
            True if successful, False otherwise
        """
        node_host, node_port = node
        
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(node_host, node_port),
                timeout=2.0
            )
            
            async with self.log_lock:
                prev_log_index = self.next_index[node] - 1
                prev_log_term = 0
                if prev_log_index > 0 and prev_log_index <= len(self.log):
                    prev_log_term = self.log[prev_log_index - 1].term
                
                append_entries = {
                    "type": "APPEND_ENTRIES",
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": [entry.to_dict() for entry in entries],
                    "leader_commit": self.commit_index
                }
            
            await write_message_async(writer, "RAFT_APPEND_ENTRIES", json.dumps(append_entries).encode('utf-8'))
            
            response_cmd, response_payload = await read_message_async(reader)
            writer.close()
            await writer.wait_closed()
            
            if response_cmd == "OK" and response_payload:
                response_data = json.loads(response_payload.decode('utf-8'))
                success = response_data.get("success", False)
                term = response_data.get("term", self.current_term)
                
                if term > self.current_term:
                    # Step down if we see a higher term
                    self.current_term = term
                    self.state = NodeState.FOLLOWER
                    self.leader_id = None
                    return True
                
                if success:
                    async with self.log_lock:
                        if not is_heartbeat and entries:
                            # Update next_index and match_index for successful replication
                            self.match_index[node] = prev_log_index + len(entries)
                            self.next_index[node] = self.match_index[node] + 1
                            
                            # Update commit_index if majority has replicated
                            await self._update_commit_index()
                        else:
                            # Heartbeat successful, update last heartbeat time tracking
                            pass
                    return True
                else:
                    # Follower rejected: decrement next_index and retry
                    async with self.log_lock:
                        if self.next_index[node] > 1:
                            self.next_index[node] -= 1
                    return False
            return False
        except asyncio.TimeoutError:
            self.logger.debug("AppendEntries timeout", node=f"{node_host}:{node_port}")
            return False
        except Exception as e:
            self.logger.debug("AppendEntries failed", node=f"{node_host}:{node_port}", error=str(e))
            return False
    
    async def _update_commit_index(self):
        """Update commit_index based on majority replication."""
        if self.state != NodeState.LEADER:
            return
        
        # Find the highest index that has been replicated to majority
        sorted_match_indices = sorted(self.match_index.values(), reverse=True)
        majority = (len(self.cluster_nodes) + 1) // 2 + 1  # +1 includes leader
        
        if len(sorted_match_indices) >= majority - 1:  # -1 because leader is included
            candidate_index = sorted_match_indices[majority - 2]  # -2 for 0-indexed
            
            # Only commit if entry is from current term (Raft safety requirement)
            if candidate_index > self.commit_index:
                if candidate_index <= len(self.log):
                    entry = self.log[candidate_index - 1]
                    if entry.term == self.current_term:
                        self.commit_index = candidate_index
                        self._persist_commit_index()
                        await self._apply_committed_entries()
                        self._check_snapshot()
                        self.logger.info("Commit index updated", commit_index=self.commit_index, term=self.current_term)
    
    def _check_snapshot(self):
        """Check if snapshot should be taken and create one if needed."""
        if not self.snapshot_manager:
            return
        
        current_time = time.time()
        if self.snapshot_manager.should_snapshot(len(self.log), current_time):
            try:
                state = {
                    "queues": {},
                    "processing_queue": {},
                    "scheduled_tasks": [],
                    "dead_letter_queue": []
                }
                snapshot_file = self.snapshot_manager.create_snapshot(
                    self.commit_index,
                    state,
                    snapshot_file=None
                )
                self.logger.info("Snapshot created", snapshot_file=snapshot_file, log_index=self.commit_index)
                
                if self.commit_index > 0:
                    self.log = self.snapshot_manager.compact_log(self.log, self.commit_index)
                    self.logger.info("Log compacted", remaining_entries=len(self.log))
            except Exception as e:
                self.logger.error("Failed to create snapshot", error=str(e))
    
    async def _replication_loop(self):
        """Background task for replicating log entries to followers (leader only)."""
        while self.running and self.state == NodeState.LEADER:
            await asyncio.sleep(0.5)  # Check for new entries more frequently
            
            # Replicate pending entries to all followers
            async with self.log_lock:
                replication_tasks = []
                for node in self.cluster_nodes:
                    next_idx = self.next_index[node]
                    if next_idx <= len(self.log):
                        # Send entries starting from next_index
                        entries_to_send = self.log[next_idx - 1:]
                        if entries_to_send:
                            replication_tasks.append(
                                asyncio.create_task(self._send_append_entries(node, entries_to_send))
                            )
                
                if replication_tasks:
                    await asyncio.gather(*replication_tasks, return_exceptions=True)
    
    def handle_vote_request(self, term: int, candidate_id: str) -> bool:
        """Handle a vote request from a candidate."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = candidate_id
            self.state = NodeState.FOLLOWER
            return True
        elif term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
            self.voted_for = candidate_id
            return True
        return False
    
    def handle_heartbeat(self, term: int, leader_id: str):
        """Handle a heartbeat from the leader."""
        if term >= self.current_term:
            self.current_term = term
            self.leader_id = leader_id
            self.state = NodeState.FOLLOWER
            self.last_heartbeat_time = time.time()
            self.voted_for = None
    
    async def handle_append_entries(self, term: int, leader_id: str, prev_log_index: int, 
                             prev_log_term: int, entries: List[dict], leader_commit: int) -> tuple:
        """Handle AppendEntries RPC from leader (async).
        
        Returns: (success: bool, term: int)
        """
        # Reply false if term < currentTerm
        if term < self.current_term:
            return (False, self.current_term)
        
        # If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.leader_id = leader_id
            self.voted_for = None
        
        self.last_heartbeat_time = time.time()
        
        # Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        async with self.log_lock:
            if prev_log_index > 0:
                if prev_log_index > len(self.log):
                    return (False, self.current_term)
                if prev_log_index <= len(self.log) and self.log[prev_log_index - 1].term != prev_log_term:
                    # Conflict: delete all entries from prev_log_index onwards
                    self.log = self.log[:prev_log_index - 1]
                    return (False, self.current_term)
            
            # Append any new entries not already in the log
            if entries:
                # Delete conflicting entries
                append_start = prev_log_index
                for i, entry_dict in enumerate(entries):
                    entry = LogEntry.from_dict(entry_dict)
                    log_index = append_start + i
                    
                    if log_index < len(self.log):
                        # Check for conflict
                        if self.log[log_index].term != entry.term:
                            # Delete conflicting entry and all that follow
                            self.log = self.log[:log_index]
                            self.log.append(entry)
                        # else: entry already matches, skip
                    else:
                        # Append new entry
                        self.log.append(entry)
                
                # Persist log to journal
                self._persist_log_entries(entries)
            
            # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if leader_commit > self.commit_index:
                last_new_entry_index = len(self.log)
                self.commit_index = min(leader_commit, last_new_entry_index)
                self._persist_commit_index()
                
                # Apply committed entries to state machine
                await self._apply_committed_entries()
        
        return (True, self.current_term)
    
    async def replicate_log_entry(self, command: str, data: dict) -> bool:
        """Append and replicate a log entry to followers (leader only, async).
        
        Returns: True if entry is committed (majority replicated), False otherwise.
        """
        if not self.is_leader():
            return False
        
        async with self.log_lock:
            # Append entry to local log
            log_index = len(self.log) + 1
            entry = LogEntry(self.current_term, log_index, command, data)
            self.log.append(entry)
            
            # Persist to journal immediately
            self._persist_log_entries([entry])
            
            self.logger.info("Log entry appended", term=self.current_term, index=log_index, command=command)
        
        # Replicate to followers (replication loop will handle this)
        # Wait for majority acknowledgment
        start_time = time.time()
        timeout = 5.0
        
        while time.time() - start_time < timeout:
            async with self.log_lock:
                # Check if entry is committed (majority replicated)
                if self.commit_index >= log_index:
                    return True
            
            await asyncio.sleep(0.1)
        
        # Timeout: entry may not be committed yet, but it's in the log
        self.logger.warn("Log entry replication timeout", index=log_index)
        return False
    
    async def replicate_log_entry_async(self, command: str, data: dict) -> bool:
        """Async version of replicate_log_entry (now the main method).
        
        Returns: True if entry is committed (majority replicated), False otherwise.
        """
        return await self.replicate_log_entry(command, data)
    
    def _load_log_from_journal(self):
        """Load Raft log from journal file on startup."""
        if not self.journal_file:
            return
        
        import os
        import json
        
        if not os.path.exists(self.journal_file):
            return
        
        commit_index_file = self.journal_file + ".commit"
        persisted_commit_index = 0
        
        if os.path.exists(commit_index_file):
            try:
                with open(commit_index_file, 'r') as f:
                    commit_data = json.load(f)
                    persisted_commit_index = commit_data.get("commit_index", 0)
                    self.current_term = commit_data.get("current_term", 0)
            except Exception as e:
                self.logger.warn("Failed to load commit index", error=str(e))
        
        try:
            with open(self.journal_file, 'r') as f:
                log_index = 0
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        entry_dict = json.loads(line)
                        command = entry_dict.get("command")
                        timestamp = entry_dict.get("timestamp", time.time())
                        data = entry_dict.get("data", {})
                        
                        log_index += 1
                        entry = LogEntry(self.current_term, log_index, command, data)
                        self.log.append(entry)
                    except json.JSONDecodeError as e:
                        self.logger.warn("Invalid journal entry format, skipping", line=line[:100], error=str(e))
                        continue
            
            if len(self.log) > 0:
                self.commit_index = min(persisted_commit_index, len(self.log))
                self.last_applied = self.commit_index
                
                if self.state_machine_callback:
                    for i in range(self.last_applied):
                        entry = self.log[i]
                        try:
                            self.state_machine_callback(entry.command, entry.data)
                        except Exception as e:
                            self.logger.error("Failed to apply entry during load", index=entry.index, error=str(e))
                
                self.logger.info("Log loaded from journal", entries=len(self.log), 
                               commit_index=self.commit_index, last_applied=self.last_applied,
                               current_term=self.current_term)
            else:
                self.commit_index = 0
                self.last_applied = 0
        except Exception as e:
            self.logger.error("Failed to load log from journal", error=str(e))
    
    def _persist_commit_index(self):
        """Persist commit index to separate file."""
        if not self.journal_file:
            return
        
        import os
        import json
        
        commit_index_file = self.journal_file + ".commit"
        try:
            with open(commit_index_file, 'w') as f:
                json.dump({
                    "commit_index": self.commit_index,
                    "current_term": self.current_term,
                    "last_applied": self.last_applied
                }, f)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            self.logger.error("Failed to persist commit index", error=str(e))
    
    def _persist_log_entries(self, entries: List[LogEntry]):
        """Persist log entries to journal file in JSON lines format."""
        if not self.journal_file:
            return
        
        try:
            import os
            with open(self.journal_file, 'a') as f:
                for entry in entries:
                    journal_entry = {
                        "command": entry.command,
                        "timestamp": entry.data.get('timestamp', time.time()),
                        "data": entry.data
                    }
                    f.write(json.dumps(journal_entry) + '\n')
                f.flush()
        except Exception as e:
            self.logger.error("Failed to persist log entries", error=str(e))
    
    async def _apply_committed_entries(self):
        """Apply committed log entries to state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            if self.last_applied <= len(self.log):
                entry = self.log[self.last_applied - 1]
                if self.state_machine_callback:
                    try:
                        self.state_machine_callback(entry.command, entry.data)
                        self.logger.debug("Applied log entry", index=self.last_applied, command=entry.command)
                    except Exception as e:
                        self.logger.error("Failed to apply log entry", index=self.last_applied, error=str(e))
                else:
                    self.logger.debug("No state machine callback", index=self.last_applied, command=entry.command)
    
    async def get_log_info(self) -> dict:
        """Get information about the Raft log."""
        async with self.log_lock:
            return {
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "current_term": self.current_term,
                "state": self.state.value
            }
