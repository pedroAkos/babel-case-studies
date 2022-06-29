# chinespaxoses

To be forked on their original repos:
  - https://github.com/luohaha/MyPaxos
  - https://github.com/wuba/WPaxos

### BOTH
- Do not have pipeline (proposer waits until instance ends before proposing next)
- Roles are distributed, and send messages to each other
- Replicas always try to become leader (i.e, they to not redirect messages to current leader)
  - This means we tested by sending all ops to the same node
  - Else, it would create competition and very low throughput in MyPaxos, or failed operations in WPaxos
  

### MyPaxos
More simple implementation of paxos, uses sockets and Json formatted messages.
  - Multi-Paxos withOUT Dist Learner
  - Follows exactly the "Multi-Paxos without failures" diagram of wikipedia
    - https://en.wikipedia.org/wiki/Paxos_(computer_science)#Multi-Paxos_without_failures
  - Commented out the (very simple) persistence code
  - Original behaviour: learners periodically ask all acceptors for values, and commit when given a quorum of equal values.
    - We changed to broadcast learned values, instead of asking for it
  - Batching was done client-side, so no batching with our client
  - We had to make extra code changes for it to work: 
    - Solved concurrency problems...
    - Was hardcoded to listen in the loopback interface
    - Removed timeouts that created a new thread per paxos instance:
      - This is something that would have not happened if using Babel
  - Very basic network library, no connection recovery. If a node crashes and returns, other nodes keep trying to send messages through the original (closed) connection
    - Serialize messages using java object serializer (which is bad?)
    
In comparison, our solution (MultiPaxosNoPipeline)
- Multi-Paxos without Dist Learner (same messages exchanged)
- Roles are collapsed
- Follows "Multi-Paxos when roles are collapsed" of wikipedia
- Removed pipeline
- Acceptors broadcast accept to all
- SM keeps track of leader, and redirects instead of submitting to consensus
  - We should not count the SM file in the line count since it is not implemented in other paxos
- Prepares are executed for all instances, receiving accepted values from higher instances
- Batching removed 
  
### WPaxos
More complex implementations of paxos, uses netty and protobuf formatted messages.
- Multi-Paxos WITH Dist Learner
- Accepted go to proposer, which then broadcasts success (learned) messages to all learners
- Usage file storage, disabled syncWrite and enabled transientStorePoolEnable to maximize performance
- Disabled batching option
- Worked much better out-of-the-box
- Uses UDP to send small messages, hence being faster than ours
- Without UDP it would be slower (around 10k)

Our solution
- Same as other solution, but with Dist Learner.
- Acceptors reply only to proposer
- Proposer sends "decision" message to all learners
