use crate::parse_actor::parse_request_message::ParseRequestMessage;
use ractor::ActorRef;
use ractor_cluster::node::NodeServerSessionInformation;
use ractor_cluster::NodeEventSubscription;

pub(crate) struct Subscription(pub(crate) ActorRef<ParseRequestMessage>);

impl NodeEventSubscription for Subscription {
    fn node_session_opened(&self, _ses: NodeServerSessionInformation) {}

    fn node_session_disconnected(&self, _ses: NodeServerSessionInformation) {}

    fn node_session_authenicated(&self, _ses: NodeServerSessionInformation) {}

    fn node_session_ready(&self, _ses: NodeServerSessionInformation) {
        let _ = self.0.send_message(ParseRequestMessage::UpdateIndex);
    }
}
