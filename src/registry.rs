use crate::{
    ActivityName, DurableManifest, ManifestActivity, ManifestWorkflow, PayloadRef, Result,
    WorkflowType,
};
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

pub type BoxWorkflowFuture<T> = std::pin::Pin<Box<dyn Future<Output = Result<T>> + Send>>;
pub type BoxActivityFuture<T> = std::pin::Pin<Box<dyn Future<Output = Result<T>> + Send>>;

pub trait Workflow: Clone + Copy + Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send + 'static;
    type Output: Serialize + DeserializeOwned + Send + 'static;

    const NAME: &'static str;
    const VERSION: u32;
    const RUST_PATH: &'static str;

    fn input_type_name() -> &'static str {
        std::any::type_name::<Self::Input>()
    }

    fn output_type_name() -> &'static str {
        std::any::type_name::<Self::Output>()
    }

    fn workflow_type() -> WorkflowType {
        WorkflowType::new(Self::NAME, Self::VERSION)
    }

    fn run(self, input: Self::Input) -> BoxWorkflowFuture<Self::Output>;
}

pub trait Activity: Clone + Copy + Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send + 'static;
    type Output: Serialize + DeserializeOwned + Send + 'static;

    const NAME: &'static str;
    const RUST_PATH: &'static str;

    fn input_type_name() -> &'static str {
        std::any::type_name::<Self::Input>()
    }

    fn output_type_name() -> &'static str {
        std::any::type_name::<Self::Output>()
    }

    fn activity_name() -> ActivityName {
        ActivityName::new(Self::NAME)
    }

    fn run(self, input: Self::Input) -> BoxActivityFuture<Self::Output>;
}

#[derive(Clone)]
pub struct WorkflowRegistration {
    pub workflow_type: WorkflowType,
    pub rust_path: &'static str,
    pub input_type: &'static str,
    pub output_type: &'static str,
    pub input_schema_hash: String,
    pub output_schema_hash: String,
    run: Arc<dyn Fn(PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> + Send + Sync>,
}

impl WorkflowRegistration {
    pub fn from_workflow<W>() -> Self
    where
        W: Workflow + Default,
    {
        Self {
            workflow_type: W::workflow_type(),
            rust_path: W::RUST_PATH,
            input_type: W::input_type_name(),
            output_type: W::output_type_name(),
            input_schema_hash: crate::type_fingerprint::<W::Input>(),
            output_schema_hash: crate::type_fingerprint::<W::Output>(),
            run: Arc::new(|input| {
                Box::pin(async move {
                    let input = crate::decode_payload::<W::Input>(&input)?;
                    let output = W::default().run(input).await?;
                    crate::encode_payload(&output)
                })
            }),
        }
    }

    pub fn run(&self, input: PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> {
        (self.run)(input)
    }
}

#[derive(Clone)]
pub struct ActivityRegistration {
    pub activity_name: ActivityName,
    pub rust_path: &'static str,
    pub input_type: &'static str,
    pub output_type: &'static str,
    pub input_schema_hash: String,
    pub output_schema_hash: String,
    run: Arc<dyn Fn(PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> + Send + Sync>,
}

impl ActivityRegistration {
    pub fn from_activity<A>() -> Self
    where
        A: Activity + Default,
    {
        Self {
            activity_name: A::activity_name(),
            rust_path: A::RUST_PATH,
            input_type: A::input_type_name(),
            output_type: A::output_type_name(),
            input_schema_hash: crate::type_fingerprint::<A::Input>(),
            output_schema_hash: crate::type_fingerprint::<A::Output>(),
            run: Arc::new(|input| {
                Box::pin(async move {
                    let input = crate::decode_payload::<A::Input>(&input)?;
                    let output = A::default().run(input).await?;
                    crate::encode_payload(&output)
                })
            }),
        }
    }

    pub fn run(&self, input: PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> {
        (self.run)(input)
    }
}

#[derive(Clone, Default)]
pub struct Registry {
    workflows: BTreeMap<WorkflowType, WorkflowRegistration>,
    activities: BTreeMap<ActivityName, ActivityRegistration>,
}

impl Registry {
    pub fn register_workflow<W>(&mut self) -> Result<()>
    where
        W: Workflow + Default,
    {
        let registration = WorkflowRegistration::from_workflow::<W>();
        if self
            .workflows
            .insert(registration.workflow_type.clone(), registration.clone())
            .is_some()
        {
            return Err(crate::Error::DuplicateWorkflow(registration.workflow_type));
        }
        Ok(())
    }

    pub fn register_activity<A>(&mut self) -> Result<()>
    where
        A: Activity + Default,
    {
        let registration = ActivityRegistration::from_activity::<A>();
        if self
            .activities
            .insert(registration.activity_name.clone(), registration.clone())
            .is_some()
        {
            return Err(crate::Error::DuplicateActivity(registration.activity_name));
        }
        Ok(())
    }

    pub fn workflow(&self, workflow_type: &WorkflowType) -> Option<&WorkflowRegistration> {
        self.workflows.get(workflow_type)
    }

    pub fn activity(&self, activity_name: &ActivityName) -> Option<&ActivityRegistration> {
        self.activities.get(activity_name)
    }

    pub fn workflow_types(&self) -> Vec<WorkflowType> {
        self.workflows.keys().cloned().collect()
    }

    pub fn activity_names(&self) -> Vec<ActivityName> {
        self.activities.keys().cloned().collect()
    }

    pub fn manifest(&self) -> DurableManifest {
        DurableManifest {
            workflows: self
                .workflows
                .values()
                .map(|registration| ManifestWorkflow {
                    name: registration.workflow_type.name.clone(),
                    version: registration.workflow_type.version,
                    rust_path: registration.rust_path.to_owned(),
                    input_type: registration.input_type.to_owned(),
                    output_type: registration.output_type.to_owned(),
                    input_schema_hash: registration.input_schema_hash.clone(),
                    output_schema_hash: registration.output_schema_hash.clone(),
                })
                .collect(),
            activities: self
                .activities
                .values()
                .map(|registration| ManifestActivity {
                    name: registration.activity_name.0.clone(),
                    rust_path: registration.rust_path.to_owned(),
                    input_type: registration.input_type.to_owned(),
                    output_type: registration.output_type.to_owned(),
                    input_schema_hash: registration.input_schema_hash.clone(),
                    output_schema_hash: registration.output_schema_hash.clone(),
                })
                .collect(),
        }
    }
}
