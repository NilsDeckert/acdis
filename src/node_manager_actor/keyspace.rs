use crate::hash_slot::hash_slot_range::HashSlotRange;
use crate::node_manager_actor::actor::NodeManagerActor;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::message::NodeManagerMessage::QueryKeyspace;
use ractor::concurrency::Duration;
use ractor::rpc::CallResult;
use ractor::{ActorProcessingErr, ActorRef, Message, RpcReplyPort};
use std::ops::Range;

impl NodeManagerActor {
    /// Given a range, split it and return both halves
    /// Of size of range is odd, first half will contain the extra element
    pub(crate) fn halve_range(range: HashSlotRange) -> (HashSlotRange, HashSlotRange) {
        let length = (range.end - 1.into()) - range.start; // End of range is not inside range
        let half_length = u16::from(length).div_ceil(2);
        let mid = range.start + half_length.into() + 1.into();
        (
            HashSlotRange::new(range.start, mid),
            HashSlotRange::new(mid, range.end),
        )
    }

    /// Given a list of actors, ask them for their keyspace.
    ///
    /// ## Returns
    ///  - List of (Actor, Keyspace) Tuples:
    ///    - `Ok(Vec<(&ActorRef<NodeManagerMessage>, HashSlotRange)>`
    ///  - Error:
    ///    - `Err(Box<dyn Error + Send + Sync>)`
    pub(crate) async fn query_keyspaces(
        actors: &Vec<ActorRef<NodeManagerMessage>>,
    ) -> Result<Vec<(&ActorRef<NodeManagerMessage>, HashSlotRange)>, ActorProcessingErr> {
        Self::query(&actors[..], QueryKeyspace, None).await
    }

    /// Send a request to multiple actors and return a list of (Actor, Response) tuples
    ///
    /// # Arguments
    ///
    /// * `actors`: The list of actors to send the request to
    /// * `msg_builder`: Function that takes a [`RpcReplyChannel`] and returns a message.
    /// * `timeout_option`: Optional timeout
    ///
    /// returns: Result<Vec<(&ActorRef<TMsg>, TReply), Global>, Box<dyn Error+Send+Sync, Global>>
    ///
    /// # Examples
    ///
    /// This is a private method.
    /// The example below is for when you want to extend the `NodeManagerActor`.
    ///
    /// ```ignore
    /// # use ractor::pg;
    /// # use ractor::ActorRef;
    /// # use acdis::node_manager_actor::actor::NodeManagerActor;
    /// # use acdis::node_manager_actor::message::NodeManagerMessage;
    /// # use acdis::node_manager_actor::message::NodeManagerMessage::QueryAddress;
    /// let actor_cells = pg::get_members(&String::from("my_process_group"));
    /// let actor_refs  = actor_cells
    ///                     .into_iter()
    ///                     .map(|cell| ActorRef::<NodeManagerMessage>::from(cell))
    ///                     .collect();
    ///
    /// let addresses = self.query(&actor_refs, QueryAddress, None);
    /// ```
    ///
    /// # Note
    /// The signature of this function is copied from the called function
    /// [`ractor::rpc::multi_call`] to ensure compatibility.
    pub(crate) async fn query<TMsg, TReply, TMsgBuilder>(
        actors: &[ActorRef<TMsg>],
        msg_builder: TMsgBuilder,
        timeout_option: Option<Duration>,
    ) -> Result<Vec<(&ActorRef<TMsg>, TReply)>, ActorProcessingErr>
    where
        TMsg: Message,
        TReply: Send + 'static + PartialEq,
        TMsgBuilder: Fn(RpcReplyPort<TReply>) -> TMsg,
    {
        let replies = ractor::rpc::multi_call(actors, msg_builder, timeout_option).await;
        match replies {
            Err(e) => Err(ActorProcessingErr::from(e)),
            Ok(replies) => {
                // Return an error if any of the requests failed
                if replies.contains(&CallResult::SenderError)
                    || replies.contains(&CallResult::Timeout)
                {
                    return Err(ActorProcessingErr::from(
                        "One or more requests returned an error",
                    ));
                }

                // "Unwrap" CallResults to their contained String
                let responses: Vec<TReply> = replies
                    .into_iter()
                    .map(|call_result| {
                        if let CallResult::Success(result) = call_result {
                            result
                        } else {
                            panic!("CallResult contained an error despite prior check")
                        }
                    })
                    .collect();

                Ok(actors.into_iter().zip(responses).collect())
            }
        }
    }

    /// Sort a list of (Actor, Keyspace) tuples by the size of the keyspace.
    ///
    /// # Arguments
    ///
    /// * `keyspaces`: A tuple ([`ActorRef`], HashSlotRange)
    ///
    /// returns: Vec<(ActorRef<NodeManagerMessage>, HashSlotRange), Global>
    pub(crate) async fn sort_actors_by_keyspace(
        mut keyspaces: Vec<(&ActorRef<NodeManagerMessage>, HashSlotRange)>,
    ) -> Vec<(&ActorRef<NodeManagerMessage>, HashSlotRange)> {
        keyspaces.sort_by_key(|(_id, keyspace)| keyspace.end - keyspace.start);
        keyspaces
    }

    /// Divide a given [`Range`] into equally sized parts.
    ///
    /// # **Warning**
    /// We want to use the entire keyspace from 0x00 to u64MAX.
    /// However, we cannot really express this, since we can't return U64MAX+1. TODO.
    ///
    /// # Arguments
    /// * `range`: Keyspace to split
    /// * `chunks`: Number of chunks to return.
    ///
    /// returns: Vec<(u64, u64), Global>
    pub(crate) fn chunk_ranges(range: HashSlotRange, chunks: u16) -> Vec<HashSlotRange> {
        let size = (range.end) - range.start;

        let values_per_chunk = size / chunks;
        let mut ranges: Vec<HashSlotRange> = Vec::new();

        let mut start = range.start;

        for i in 0..chunks {
            let mut end = start + values_per_chunk;
            // If this is the last chunk, make this contain the extra elements
            if i == chunks - 1 {
                //end += size%chunks;
            } else {
                // If this is not the last chunk, increase this by one as it is exclusive
                end += 1
            }
            ranges.push(HashSlotRange::new(start, end));
            start = end;
        }

        ranges
    }
}
