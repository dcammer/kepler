#[derive(thiserror::Error, Debug)]
pub enum KinesisSinkError {
    #[error("Error creating message: {0}")]
    CreateMessageError(String),
    #[error("Got error back from kinesis ({0}): {1}")]
    ErrorFromKinesis(String, String),
}
