use crate::node_manager_actor::actor::NodeManagerActor;
use crate::node_manager_actor::message::NodeManagerMessage;
use log::info;
use ractor::{cast, ActorRef};
use ractor_cluster::node::NodeServerSessionInformation;
use ractor_cluster::NodeServerMessage::SubscribeToEvents;
use ractor_cluster::{NodeEventSubscription, NodeServerMessage};

#[allow(dead_code)]
pub(crate) struct Subscription(pub(crate) ActorRef<NodeManagerMessage>);

impl NodeManagerActor {
    pub(crate) async fn subscribe_to_events(
        myself: ActorRef<NodeManagerMessage>,
        pmd_ref: ActorRef<NodeServerMessage>,
    ) {
        // Trigger methods when other nodes connect / disconnect
        cast!(
            pmd_ref,
            SubscribeToEvents {
                id: String::from("Subscription"),
                subscription: Box::new(Subscription(myself))
            }
        )
        .expect("Failed to send Subscription msg")
    }
}

impl NodeEventSubscription for Subscription {
    fn node_session_opened(&self, _ses: NodeServerSessionInformation) {
        // info!("Session opened: \n\
        //     node_id:    {:#?} \n\
        //     peer_addr:  {:#?} \n\
        //     peer_name:  {:#?} \n\
        //     is_server:  {:#?}",
        //     ses.node_id, ses.peer_addr, ses.peer_name, ses.is_server);
        //
        // let registered = ractor::registry::registered();
        // info!("Registered: {:#?}", registered);
        //
        // let pids = ractor::registry::get_all_pids();
        // info!("Pids: {:#?}", pids);
        // info!("\n\n\n\n\n\n")
    }

    fn node_session_disconnected(&self, ses: NodeServerSessionInformation) {
        info!("Session disconnected: {:#?}", ses.node_id);
    }

    fn node_session_authenicated(&self, _ses: NodeServerSessionInformation) {
        // info!("Session authenticated: {:#?}", ses.node_id);
    }

    fn node_session_ready(&self, ses: NodeServerSessionInformation) {
        info!("Session ready: {:?}@{}", ses.peer_name, ses.peer_addr);
        
        // TODO: update self.other_nodes with managed keyspace
        // info!("Session ready: \n\
        //     node_id:    {:#?} \n\
        //     peer_addr:  {:#?} \n\
        //     peer_name:  {:#?} \n\
        //     is_server:  {:#?}",
        //     ses.node_id, ses.peer_addr, ses.peer_name, ses.is_server);
        //
        // let registered = ractor::registry::registered();
        // info!("Registered: {:#?}", registered);
        //
        // let pids = ractor::registry::get_all_pids();
        // info!("Pids: {:#?}", pids);
    }
}
