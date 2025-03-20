use crate::node_manager_actor::actor::NodeManagerActor;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::message::NodeManagerMessage::QueryKeyspace;
use log::info;
use ractor::concurrency::Duration;
use ractor::rpc::CallResult;
use ractor::{ActorProcessingErr, ActorRef, Message, RpcReplyPort};
use std::ops::Range;

impl NodeManagerActor {
    /// Given a range, split it and return both halves
    /// Of size of range is odd, first half will contain the extra element
    pub(crate) fn halve_range(range: Range<u64>) -> (Range<u64>, Range<u64>) {
        let length = (range.end - 1) - range.start; // End of range is not inside range
        let half_length = length.div_ceil(2);
        let mid = range.start + half_length + 1;
        (range.start..mid, mid..range.end)
    }

    /// Given a list of actors, ask them for their keyspace.
    ///
    /// ## Returns
    ///  - List of (Actor, Keyspace) Tuples:
    ///    - `Ok(Vec<(&ActorRef<NodeManagerMessage>, Range<u64>)>`
    ///  - Error:
    ///    - `Err(Box<dyn Error + Send + Sync>)`
    pub(crate) async fn query_keyspaces(
        actors: &Vec<ActorRef<NodeManagerMessage>>,
    ) -> Result<Vec<(&ActorRef<NodeManagerMessage>, Range<u64>)>, ActorProcessingErr> {
        Self::query(&actors[..], QueryKeyspace, None).await
        // let tasks: Vec<JoinHandle<(ActorRef<NodeManagerMessage>, Range<u64>)>> = actors
        //     .into_iter()
        //     .map(|actor| {
        //         let actor_ref = ActorRef::<NodeManagerMessage>::from(actor.clone());
        //         tokio::spawn(async move {
        //             let keyspace = actor_ref.call(QueryKeyspace, None).await;
        //             (
        //                 actor_ref,
        //                 keyspace.unwrap().expect("Failed to query keyspace"),
        //             )
        //         })
        //     })
        //     .collect();
        //
        // // Panics inside a thread return a JoinError
        // let join_results: Vec<Result<(ActorRef<_>, Range<u64>), JoinError>> = join_all(tasks).await;
        // // Instead of returning a Vec where some of the elements might be Errors, we want to either
        // // return a successful Vec or an Error.
        // let result: Result<Vec<_>, JoinError> = join_results.into_iter().collect();
        // match result {
        //     Ok(t) => Ok(t),
        //     Err(e) => Err(ActorProcessingErr::from(e)),
        // }
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

    // TODO: Remove once we're sure the refactored version works
    // pub(crate) async fn query_address(
    //     actors: &[ActorRef<NodeManagerMessage>],
    // ) -> Result<Vec<(&ActorRef<NodeManagerMessage>, String)>, ActorProcessingErr> {
    //     let reply = ractor::rpc::multi_call(actors, QueryAddress, None).await;
    //     match reply {
    //         Ok(reply) => {
    //             if reply.contains(&CallResult::<String>::SenderError)
    //                 || reply.contains(&CallResult::<String>::Timeout)
    //             {
    //                 Err(ActorProcessingErr::from(
    //                     "One or more requests returned an error",
    //                 ))
    //             } else {
    //                 // reply: Vec<CallResult::<String>::Success(address)>
    //                 let addresses: Vec<String> = reply
    //                     .into_iter()
    //                     .map(|call_result| {
    //                         if let CallResult::Success(result) = call_result {
    //                             result
    //                         } else {
    //                             panic!("CallResult returned an error despite prior check");
    //                         }
    //                     })
    //                     .collect();
    //
    //                 Ok(actors.into_iter().zip(addresses).collect())
    //             }
    //         }
    //         Err(e) => Err(ActorProcessingErr::from(e)),
    //     }
    // }

    /// Sort a list of (Actor, Keyspace) tuples by the size of the keyspace.
    ///
    /// # Arguments
    ///
    /// * `keyspaces`: A tuple ([`ActorRef`], Range<u64>)
    ///
    /// returns: Vec<(ActorRef<NodeManagerMessage>, Range<u64>), Global>
    pub(crate) async fn sort_actors_by_keyspace(
        mut keyspaces: Vec<(&ActorRef<NodeManagerMessage>, Range<u64>)>,
    ) -> Vec<(&ActorRef<NodeManagerMessage>, Range<u64>)> {
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
    pub(crate) fn chunk_ranges(range: Range<u64>, chunks: u64) -> Vec<Range<u64>> {
        let size = (range.end) - range.start;

        let values_per_chunk = size / chunks;
        let mut ranges: Vec<Range<u64>> = Vec::new();

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
            ranges.push(start..end);
            start = end;
        }

        ranges
    }
}
