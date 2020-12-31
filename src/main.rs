mod line_parser;

extern crate clap;
extern crate rdkafka;

use clap::{Arg, App};
use std::ffi::OsString;
use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;
use rdkafka::metadata::MetadataTopic;
use std::fs::File;
use std::path::Path;
use std::io::{self, BufRead};
use java_properties::Line;
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaError::AdminOp;


const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

// https://www.fpcomplete.com/rust/command-line-parsing-clap/
struct AppConfig {
    input_file: String,
    conf_file: String,
    is_test: bool
}

impl AppConfig {
    fn new() -> Self {
        Self::new_from(std::env::args_os().into_iter()).unwrap_or_else(|e| e.exit())
    }

    fn new_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: Iterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let app = App::new("kafka-bulk-topic-create")
            .version("0.1")
            .about("Bulk create Kafka topics from an input file")
            .author("Adam Evans");

        let test_option = Arg::with_name("test")
            .long("test")
            .short("t")
            .takes_value(false)
            .help("Test mode. Print topics that would be created without creating them")
            .required(false);

        let conf_option = Arg::with_name("conf")
            .long("conf")
            .short("c")
            .takes_value(true)
            .help("server.properties Kafka broker connect config file")
            .required(true);

        let input_file_option = Arg::with_name("file")
            .long("file")
            .short("f")
            .takes_value(true)
            .help("Topic input file")
            .required(true);

        let app = app
            .arg(test_option)
            .arg(conf_option)
            .arg(input_file_option);

        let matches = app.get_matches_from_safe(args)?;

        let input_file = matches.value_of("file").unwrap().to_string();

        let conf_file = matches.value_of("conf").unwrap().to_string();

        let is_test = matches.is_present("test");

        Ok(AppConfig { input_file, conf_file, is_test})
    }
}

/**
 TODO
   * Need to update the input line format to take partitions and replication factor cols
   * Need to update/rename InputFileError
   * Look at the line_parser file, does this need to be in a module?
**/


fn main() {
    let app_config = AppConfig::new();

    let props;

    {
        let conf_file = File::open(&app_config.conf_file)
            .expect("Error opening conf file");

        props = java_properties::read(conf_file)
            .expect("Error reading conf file");
    }

    let kafka_config = props.iter()
        .fold(ClientConfig::new(), |mut config, (k, v)| {
            config.set(k, v);
            config
        });

    let consumer: BaseConsumer = kafka_config
        .create()
        .expect("Client creation failed");

    let cluster_meta_data = consumer
        .fetch_metadata(None, DEFAULT_TIMEOUT)
        .expect("Failed to fetch metadata");

    let existing_topics: Vec<&str> = cluster_meta_data.topics()
        .iter()
        .map(|t| t.name())
        .collect();

    let result = read_input(&app_config.input_file)
        .expect("Failed reading input topics");

    let adminClient: AdminClient<DefaultClientContext> = kafka_config.create().expect("AdminClient creation failed");

    let adminOptions = AdminOptions::new()
        .request_timeout(Option::Some(Duration::from_secs(5)));

    for (topic, conf) in result {
        if(!existing_topics.contains(&topic.as_str())) {
            if(!app_config.is_test){

                let result = async {
                    let t = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
                    println!("Creating topic {}", &topic);
                    let result = adminClient.create_topics(&[t], &adminOptions).await;
                    result.map_err(|e| InputFileError::TopicCreateError);
                    println!("Created: {}", &topic);
                };

                futures::executor::block_on(result)

            }
        }
    }

}

fn read_input<P>(path: &P) -> Result<Vec<(String, Vec<(String, String)>)>, InputFileError>
where P: AsRef<Path>, {
    let input_file = File::open(path).unwrap();
    let lines = io::BufReader::new(input_file).lines();

    let mut result: Vec<(String, Vec<(String, String)>)> = Vec::new();


    for (i, line) in lines.enumerate() {
        match line {
            Ok(line) => {
                let parsed_line = line_parser::read_line(&line)
                    .ok_or(InputFileError::ParseLineError(i, line))?;

                result.push(parsed_line)
            },
            Err(e) => {
                return Err(InputFileError::IoError(e))
            },
        };
    };


    Ok(result)
}

fn is_valid_topic_name(s: &str) -> bool {
    !s.contains(' ')
}



#[derive(Debug)]
enum InputFileError {
    IoError(io::Error),
    ParseLineError(usize,String),
    BadTopicName(usize, String),
    TopicCreateError
}