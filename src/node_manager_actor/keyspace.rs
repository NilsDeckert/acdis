use futures::future::join_all;
use ractor::{ActorCell, ActorRef};
use std::ops::Range;
use tokio::task::JoinHandle;

use crate::node_manager_actor::actor::NodeManagerActor;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::message::NodeManagerMessage::QueryKeyspace;

impl NodeManagerActor {
    /// Given a range, split it and return both halves
    /// Of size of range is odd, first half will contain the extra element
    pub(crate) fn halve_range(range: Range<u64>) -> (Range<u64>, Range<u64>) {
        let length = (range.end - 1) - range.start; // End of range is not inside range
        let half_length = length.div_ceil(2);
        let mid = range.start + half_length + 1;
        (range.start..mid, mid..range.end)
    }

    pub(crate) async fn sort_actors_by_keyspace(
        actors: &mut Vec<ActorCell>,
    ) -> Vec<(ActorRef<NodeManagerMessage>, Range<u64>)> {
        let tasks: Vec<JoinHandle<(ActorRef<NodeManagerMessage>, Range<u64>)>> = actors
            .into_iter()
            .map(|actor| {
                let actor_ref = ActorRef::<NodeManagerMessage>::from(actor.clone());
                tokio::spawn(async move {
                    let keyspace = actor_ref.call(QueryKeyspace, None).await;
                    (
                        actor_ref,
                        keyspace.unwrap().expect("Failed to query keyspace"),
                    )
                })
            })
            .collect();

        let mut keyspaces: Vec<(ActorRef<NodeManagerMessage>, Range<u64>)> = join_all(tasks)
            .await
            .into_iter()
            .map(|result| {
                let (id, range) = result.expect("Failed awaiting QueryKeyspace response");
                (id, range)
            })
            .collect();

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
