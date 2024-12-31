mod errors;
use async_std::sync::Mutex;
use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use aws_sdk_kinesis::{Client, Error};
pub use errors::KinesisSinkError;
use futures::future::{BoxFuture, FutureExt};
use futures::select;
use std::sync::Arc;
use tokio::task;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info};

pub type CallbackFunction = dyn Fn(&Result<(), KinesisSinkError>) -> BoxFuture<()> + Send + Sync;
/// Specify how to handle the results.
/// It is heavy handed to provide one type of response, so this creates a set of possible reponses
#[derive(Clone)]
pub enum KinesisResultHandling {
    /// No reaction to seding
    Ignore,
    /// Print to tracing logs
    Tracing,
    #[cfg(feature = "callback")]
    /// Synchronous callback
    Callback(Arc<CallbackFunction>),
}

#[derive(Clone)]
pub struct KinesisMessage {
    pub stream_name: String,
    pub partition_key: String,
    pub data: Vec<u8>,
    pub result_handler: KinesisResultHandling,
}

impl KinesisMessage {
    pub fn new(
        stream_name: &str,
        partition_key: &str,
        data: &[u8],
        results_handler: KinesisResultHandling,
    ) -> KinesisMessage {
        KinesisMessage {
            stream_name: stream_name.to_string(),
            partition_key: partition_key.to_string(),
            data: data.into(),
            result_handler: results_handler,
        }
    }
}

/// Cloneable sink
#[derive(Debug, Clone)]
pub struct KinesisSink {
    /// Channels to send data to the process
    sender: async_channel::Sender<KinesisMessage>,
    /// When all the sinks drop, the process will drop
    process: Arc<Mutex<KinesisSinkProcess>>,
}

#[derive(Debug)]
pub struct KinesisSinkProcess {
    spawned_thread: Option<task::JoinHandle<()>>,
}

/// Create a pipe to send Kinesis messages
#[allow(clippy::result_large_err)]
impl KinesisSinkProcess {
    fn spawn(
        client: Client,
    ) -> Result<(async_channel::Sender<KinesisMessage>, KinesisSinkProcess), Error> {
        let client = Arc::new(client);
        let batch_size: usize = 100;
        let timeout: Duration = Duration::from_millis(500);
        info!(
            "Initializing kinesis process with batch size {} and timeout {:?}",
            batch_size, timeout
        );
        let (tx, rx): (
            async_channel::Sender<KinesisMessage>,
            async_channel::Receiver<KinesisMessage>,
        ) = async_channel::unbounded();
        let handle = task::spawn(async move {
            // Buffer messages and send an array of messages at once.
            // For that to work, we need to "flush" based on the number of messages in the
            // array and in the time since last sent.
            let mut recv_fut = Box::pin(rx.recv().fuse());
            let mut timeout_fut = Box::pin(sleep(timeout).fuse());
            let mut msgs = vec![];
            loop {
                let mut timed_out = false;
                let mut last_loop = false;
                select! {
                    msg = recv_fut => {
                        match msg {
                            Err(_) => last_loop = true,
                            Ok(msg) => {msgs.push(msg); }
                        }
                        recv_fut = Box::pin(rx.recv().fuse());
                    }
                    () = timeout_fut => timed_out = true,
                    complete => last_loop = true,
                };
                if last_loop || msgs.len() >= batch_size || timed_out {
                    debug!(
                        "Last loop: {}, msgs len: {}, timed out: {}",
                        last_loop,
                        msgs.len(),
                        timed_out
                    );
                    send_messages(client.clone(), &msgs).await;
                    if last_loop {
                        break;
                    }
                    msgs.clear();
                    timeout_fut = Box::pin(tokio::time::sleep(timeout).fuse());
                }
                if rx.sender_count() == 0 {
                    debug!("No senders left");
                    break;
                }
            }
            debug!("Got out of the loop");
        });
        Ok((
            tx,
            KinesisSinkProcess {
                spawned_thread: Some(handle),
            },
        ))
    }

    /// Wait until the thread has stopped
    async fn wait_stop(&mut self) {
        if self.spawned_thread.is_some() {
            let thread = self.spawned_thread.take();
            debug!("Waiting for it to stop");

            if let Some(thread) = thread {
                // Wait for the thread ot finish.
                // Waiting for the thread to finish causes a deadlock. Lesson learned: dependencies
                // during the destruction causes problems.
                futures::join!(thread).0.expect("Should join");
                debug!("Thread finished");
            }
        }
    }
}

impl KinesisSink {
    pub async fn start(client: Client) -> Result<KinesisSink, Error> {
        let (tx, process) = KinesisSinkProcess::spawn(client)?;
        Ok(KinesisSink {
            sender: tx,
            process: Arc::new(Mutex::new(process)),
        })
    }

    /// Create a message and send
    /// This is thread safe
    pub async fn send_message(
        &self,
        stream_name: &str,
        partition_key: &str,
        data: &[u8],
        results_handing: KinesisResultHandling,
    ) -> Result<(), async_channel::SendError<KinesisMessage>> {
        let message = KinesisMessage::new(stream_name, partition_key, data, results_handing);
        self.send(message).await
    }

    pub async fn send(
        &self,
        message: KinesisMessage,
    ) -> Result<(), async_channel::SendError<KinesisMessage>> {
        self.sender.send(message).await
    }

    pub fn stop(&self) {
        debug!("Stopping");
        self.sender.close();
    }

    pub async fn wait_done(&self) {
        debug!("Wait done");
        let mut process = self.process.lock().await;
        process.wait_stop().await;
        debug!("Done waiting");
    }
}

/// Send an array of kinesis messages
/// Partition the messages by stream, and then send them to the stream
async fn send_messages(client: Arc<Client>, msgs: &Vec<KinesisMessage>) {
    let mut map = std::collections::HashMap::<String, Vec<KinesisMessage>>::new();
    for msg in msgs.as_slice() {
        let msgs = map.entry(msg.stream_name.clone()).or_default();
        msgs.push(msg.clone());
    }
    for (stream_name, msgs) in map.iter() {
        let records: Vec<PutRecordsRequestEntry> = msgs
            .iter()
            .map(|msg| {
                let blob = Blob::new(msg.data.as_slice());
                PutRecordsRequestEntry::builder()
                    .data(blob)
                    .partition_key(msg.partition_key.clone())
                    .build()
            })
            .filter_map(Result::ok)
            .collect();

        let rv = put_records(client.clone(), stream_name, records.to_vec()).await;
        debug!("Put records returned {:?}", rv);
        match rv {
            Ok(response) => {
                if let Some(failure_count) = response.failed_record_count {
                    if failure_count > 0 {
                        for (idx, result) in response.records.into_iter().enumerate() {
                            if let Some(error_code) = result.error_code.as_ref() {
                                let description = result.error_message().unwrap_or_default();
                                let err_value = KinesisSinkError::ErrorFromKinesis(
                                    error_code.to_string(),
                                    description.to_string(),
                                );
                                if let Some(msg) = msgs.get(idx) {
                                    match &msg.result_handler {
                                        KinesisResultHandling::Ignore => {}
                                        KinesisResultHandling::Tracing => {
                                            error!("{}", err_value);
                                        }
                                        #[cfg(feature = "callback")]
                                        KinesisResultHandling::Callback(cb) => {
                                            let f = Arc::clone(cb);
                                            f(&Err(err_value));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                for msg in msgs {
                    let desc = e.to_string();
                    let err_value = KinesisSinkError::CreateMessageError(desc);
                    match &msg.result_handler {
                        KinesisResultHandling::Ignore => {}
                        KinesisResultHandling::Tracing => error!("{}", err_value.to_string()),
                        #[cfg(feature = "callback")]
                        KinesisResultHandling::Callback(cb) => {
                            let value = Err(err_value);
                            let f = Arc::clone(cb);
                            f(&value).await;
                        }
                    }
                }
            }
        };
    }
}

async fn put_records(
    client: Arc<Client>,
    stream_name: &str,
    records: Vec<PutRecordsRequestEntry>,
) -> Result<aws_sdk_kinesis::operation::put_records::PutRecordsOutput, Error> {
    let mut request = client.put_records().stream_name(stream_name);

    for record in records {
        request = request.records(record);
    }

    Ok(request.send().await?)
}

/// Create a new Kinesis Client for communication
pub async fn create_kinesis_client(
    profile_name: &str,
    fallback: impl ProvideRegion + 'static,
) -> Client {
    let region_provider = RegionProviderChain::default_provider().or_else(fallback);
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .profile_name(profile_name)
        .region(region_provider)
        .load()
        .await;
    aws_sdk_kinesis::Client::new(&config)
}
#[cfg(test)]
mod test {
    use super::*;
    use mockito::{Matcher, Server};
    use rstest::rstest;
    #[cfg(feature = "callback")]
    use std::sync::atomic::AtomicBool;

    #[rstest]
    fn new_message() {
        let msg = KinesisMessage::new(
            "hello",
            "help",
            "world".as_bytes(),
            KinesisResultHandling::Ignore,
        );
        assert_eq!("hello", msg.stream_name);
        assert_eq!("help", msg.partition_key);
        assert_eq!(5, msg.data.len());
        assert_eq!(b'r', msg.data[2]);
    }

    #[rstest]
    #[tokio::test]
    async fn new_kinesis_sink() {
        let sink = KinesisSink::start(create_kinesis_client("default", "us-east-1").await)
            .await
            .unwrap();
        assert!(!sink.sender.is_closed());
    }

    #[rstest]
    #[tokio::test]
    async fn put_records_test() {
        let mut server = Server::new_async().await;
        let url = server.url();
        let client = Arc::new(kinesis_test_client(&url).await);
        let stream_name = "world";
        let entry = PutRecordsRequestEntry::builder()
            .data(Blob::new("hello"))
            .partition_key("h")
            .build()
            .unwrap();

        let records = vec![entry];
        let rusoto_entry = rusoto_kinesis::PutRecordsRequestEntry {
            data: "hello".into(),
            partition_key: "h".to_string(),
            explicit_hash_key: None,
        };
        let request = rusoto_kinesis::PutRecordsInput {
            records: vec![rusoto_entry],
            stream_name: stream_name.to_string(),
        };
        let my_json = serde_json::ser::to_string(&request).unwrap();
        println!("Ouput:\n{}", my_json);

        let mock = server
            .mock("POST", "/")
            .match_header("x-amz-target", Matcher::Exact(kinesis_api("PutRecords")))
            .match_body(Matcher::JsonString(my_json))
            .with_status(200)
            .create_async()
            .await;
        let resp = put_records(client, stream_name, records).await.unwrap();
        mock.expect(1).assert_async().await;
        assert!(resp.failed_record_count.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn put_records_invalid_stream() {
        let mut server = Server::new_async().await;
        let url = server.url();
        let client = Arc::new(kinesis_test_client(&url).await);
        let stream_name = "world";
        let entry = PutRecordsRequestEntry::builder()
            .data(Blob::new("hello"))
            .partition_key("h")
            .build()
            .unwrap();

        let records = vec![entry];
        let response =
            "ResourceNotFoundException: Stream world under account 000000000000 not found."
                .to_string();
        let my_response = r##"{"__type":"ResourceNotFoundException","message":"Stream world under account 000000000000 not found."}"##;

        let mock = server
            .mock("POST", "/")
            .match_header("x-amz-target", Matcher::Exact(kinesis_api("PutRecords")))
            .with_body(my_response.as_bytes())
            .with_status(400)
            .create_async()
            .await;
        let resp = put_records(client, stream_name, records).await.unwrap_err();
        mock.expect(1).assert_async().await;
        assert_eq!(response.to_string(), resp.to_string());
    }

    #[cfg(feature = "callback")]
    #[rstest]
    #[tokio::test]
    async fn kinesis_sink_put_success_cb() {
        let mut server = Server::new_async().await;
        let url = server.url();
        let client = kinesis_test_client("default", &url).await;
        let stream_name = "world";
        let rusoto_entry = rusoto_kinesis::PutRecordsRequestEntry {
            data: "hello".into(),
            partition_key: "h".to_string(),
            explicit_hash_key: None,
        };
        let request = rusoto_kinesis::PutRecordsInput {
            records: vec![rusoto_entry],
            stream_name: stream_name.to_string(),
        };
        let my_json = serde_json::ser::to_string(&request).unwrap();

        let mock = server
            .mock("POST", "/")
            .match_header("x-amz-target", Matcher::Exact(kinesis_api("PutRecords")))
            .match_body(Matcher::JsonString(my_json))
            .with_status(200)
            .create_async()
            .await;

        let sink = KinesisSink::start(client).await.unwrap();
        let success = Arc::new(Mutex::new(AtomicBool::new(false)));
        let success_clone = success.clone();
        sink.send_message(
            stream_name,
            "h",
            "hello".as_bytes(),
            KinesisResultHandling::Callback(Arc::new(move |result| {
                Box::pin(async {
                    let mut success_access = success_clone.lock().await;
                    *success_access.get_mut() = result.is_ok();
                })
            })),
        )
        .await
        .unwrap();
        sink.stop();
        sink.wait_done().await;
        mock.expect(1).assert_async().await;
        let got_ok = success.lock().await;
        assert!(got_ok.into_inner());
    }

    async fn kinesis_test_client(endpoint: &str) -> Client {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region("us-east-1")
            .test_credentials()
            .endpoint_url(endpoint)
            .load()
            .await;
        aws_sdk_kinesis::Client::new(&config)
    }

    fn kinesis_api(name: &str) -> String {
        format!("Kinesis_20131202.{}", name)
    }
}
