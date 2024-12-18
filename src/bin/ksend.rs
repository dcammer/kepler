use clap::{arg, value_parser, Command};

#[tokio::main]
async fn main() {
    let matches = Command::new("ksend")
        .arg(
            arg!(profile: <PROFILE>)
                .required(true)
                .value_parser(value_parser!(String)),
        )
        .arg(
            arg!(stream: <STREAM>)
                .required(true)
                .value_parser(value_parser!(String)),
        )
        .arg(
            arg!(message: <MESSAGE>)
                .required(true)
                .value_parser(value_parser!(String)),
        )
        .arg(
            arg!(partition_key: -p <PARTITION_KEY>)
                .default_value("random")
                .required(false)
                .value_parser(value_parser!(String)),
        )
        .arg(
            arg!(count: -i <NUM>)
                .default_value("1")
                .required(false)
                .value_parser(value_parser!(usize)),
        )
        .get_matches();
    let profile = matches.get_one::<String>("profile").unwrap().clone();
    let stream = matches.get_one::<String>("stream").unwrap().clone();
    let partition_key = matches.get_one::<String>("partition_key").unwrap().clone();
    let message = matches.get_one::<String>("message").unwrap().clone();
    let count = *matches.get_one::<usize>("count").unwrap();
    env_logger::init();
    let client = kepler::create_kinesis_client(&profile, "us-east-1").await;
    let sink = kepler::KinesisSink::start(client).await.unwrap();
    for _ in 0..count {
        let message = message.clone();
        sink.send_message(&stream, &partition_key, &message.into_bytes())
            .await
            .unwrap();
    }
    sink.stop();
    sink.wait_done().await;
}
