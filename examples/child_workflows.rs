use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ShipInput {
    order_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ShipOutput {
    shipment_id: String,
}

#[durust::workflow(name = "examples.ship-order", version = 1)]
async fn ship_order(input: ShipInput) -> durust::Result<ShipOutput> {
    Ok(ShipOutput {
        shipment_id: format!("ship/{}", input.order_id),
    })
}

#[durust::workflow(name = "examples.checkout-with-child", version = 1)]
async fn checkout_with_child(order_id: String) -> durust::Result<String> {
    let child = durust::child!(ship_order(ShipInput {
        order_id: order_id.clone(),
    }))
    .workflow_id(format!("ship/{order_id}"))
    .spawn()
    .await?;
    let shipment = child.result().await?;
    Ok(shipment.shipment_id)
}

#[durust::workflow(name = "examples.checkout-abandon-child", version = 1)]
async fn checkout_abandon_child(order_id: String) -> durust::Result<String> {
    let child = durust::child!(ship_order(ShipInput {
        order_id: order_id.clone(),
    }))
    .workflow_id(format!("receipt/{order_id}"))
    .parent_close_policy(durust::ParentClosePolicy::Abandon)
    .spawn()
    .await?;
    Ok(child.run_id().0.clone())
}

fn main() -> durust::Result<()> {
    block_on(async {
        let backend = durust::MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        client
            .start_workflow::<checkout_with_child>("order/123", "orders", "123".to_owned())
            .await?;

        let mut worker = durust::Worker::builder(backend)
            .workflow_task_queue("orders")
            .register_workflow(checkout_with_child)
            .register_workflow(ship_order)
            .build();
        worker.run_until_idle().await?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use durust::DurableBackend;

    #[test]
    fn runs_child_workflow_spawn_and_wait_example() {
        block_on(async {
            let backend = durust::MemoryBackend::new();
            let client = durust::Client::new(backend.clone());
            let run_id = client
                .start_workflow::<checkout_with_child>(
                    "order/child-wait",
                    "orders",
                    "123".to_owned(),
                )
                .await
                .unwrap();
            let mut worker = durust::Worker::builder(backend.clone())
                .workflow_task_queue("orders")
                .register_workflow(checkout_with_child)
                .register_workflow(ship_order)
                .build();
            worker.run_until_idle().await.unwrap();

            let history = backend
                .stream_history(durust::StreamHistoryRequest {
                    run_id,
                    after_event_id: durust::EventId::ZERO,
                    up_to_event_id: durust::EventId(100),
                    max_events: 100,
                    max_bytes: usize::MAX,
                })
                .await
                .unwrap()
                .events;
            let durust::HistoryEventData::WorkflowCompleted { result } =
                &history.last().expect("parent terminal").data
            else {
                panic!("checkout did not complete");
            };
            assert_eq!(
                durust::decode_payload::<String>(&result).unwrap(),
                "ship/123"
            );
        });
    }

    #[test]
    fn runs_child_workflow_abandon_example() {
        block_on(async {
            let backend = durust::MemoryBackend::new();
            let client = durust::Client::new(backend.clone());
            let run_id = client
                .start_workflow::<checkout_abandon_child>(
                    "order/child-abandon",
                    "orders",
                    "456".to_owned(),
                )
                .await
                .unwrap();
            let mut parent_worker = durust::Worker::builder(backend.clone())
                .workflow_task_queue("orders")
                .register_workflow(checkout_abandon_child)
                .build();
            parent_worker.run_until_idle().await.unwrap();

            let parent_history = backend
                .stream_history(durust::StreamHistoryRequest {
                    run_id,
                    after_event_id: durust::EventId::ZERO,
                    up_to_event_id: durust::EventId(100),
                    max_events: 100,
                    max_bytes: usize::MAX,
                })
                .await
                .unwrap()
                .events;
            let child_run_id = parent_history
                .iter()
                .find_map(|event| match &event.data {
                    durust::HistoryEventData::ChildWorkflowStarted(started) => {
                        Some(started.run_id.clone())
                    }
                    _ => None,
                })
                .expect("abandoned child started");

            let mut child_worker = durust::Worker::builder(backend.clone())
                .workflow_task_queue("orders")
                .register_workflow(ship_order)
                .build();
            assert!(child_worker.run_workflow_once().await.unwrap());
            let child_history = backend
                .stream_history(durust::StreamHistoryRequest {
                    run_id: child_run_id,
                    after_event_id: durust::EventId::ZERO,
                    up_to_event_id: durust::EventId(100),
                    max_events: 100,
                    max_bytes: usize::MAX,
                })
                .await
                .unwrap()
                .events;
            assert!(matches!(
                child_history.last().expect("child terminal").data,
                durust::HistoryEventData::WorkflowCompleted { .. }
            ));
        });
    }
}
