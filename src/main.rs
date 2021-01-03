extern crate clap;
extern crate rdkafka;

use clap::{Arg, App};
use std::ffi::OsString;
use rdkafka::config::{ClientConfig};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;
use std::fs::File;
use std::path::Path;
use std::io::{self, BufRead};
use rdkafka::client::DefaultClientContext;


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

    let admin_client: AdminClient<DefaultClientContext> = kafka_config.create().expect("AdminClient creation failed");

    let admin_options = AdminOptions::new()
        .request_timeout(Option::Some(Duration::from_secs(5)));

    let mut create_count = 0;

    for (line_no, input_line) in result {
        let topic_definition = match input_line {
          InputLine::Definition(a) => Some(a),
          InputLine::DefinitionWithComment(a, _) => Some(a),
          _ => None
        };

        match topic_definition {
            Some(t) if !existing_topics.contains(&t.topic_name.as_str()) => {
                if !app_config.is_test {
                    let job = async {
                        let new_topic = t.additional_config.iter()
                            .fold(
                                NewTopic::new(&t.topic_name, t.partitions, TopicReplication::Fixed(t.replication_factor)),
                                |topic, (k,v)| topic.set(&k, &v)
                            );

                        admin_client.create_topics(&[ new_topic], &admin_options).await
                    };

                    let result = futures::executor::block_on(job).expect("Failed to create topic");

                    for r in result.iter() {
                        match r {
                            Err((e, ee)) => {
                                println!("ERROR - Failed to create topic {:?} defined at line {}. {:?}", e, line_no, ee);
                                println!("Total topics created: {}", create_count);
                                std::process::exit(1);
                            },
                            _ => ()
                        }
                    }
                }

                println!("Created topic: {}", t.topic_name);
                create_count += 1;
            },
            _ => ()
        }
    }

    println!("Total topics created: {}", create_count);

}

fn read_input<P>(path: &P) -> Result<Vec<(usize, InputLine)>, ReadInputError>
where P: AsRef<Path>, {
    let input_file = File::open(path).unwrap();
    let lines = io::BufReader::new(input_file).lines();

    let mut result: Vec<(usize, InputLine)> = Vec::new();

    for (i, line) in lines.enumerate() {
        match line {
            Ok(line) => {
                let parsed_line = read_line(&line)
                    .ok_or(ReadInputError::ParseLineError(i, line))?;

                result.push((i+1, parsed_line));
            },
            Err(e) =>
                return Err(ReadInputError::IoError(e))
        };
    };

    Ok(result)
}

/// Parse a csv input line in to an InputLine
/// Returns none if failed to parse line
/// Format is topic_name,partitions,replication_factor,<key=value>...
/// where <key=value> are additional topic config values seperated by a comma
/// see https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html for
/// valid configs
fn read_line(s: &str) -> Option<InputLine> {
    let trimmed = s.trim();

    if trimmed.is_empty() {
        return Some(InputLine::Empty)
    }

    if trimmed.starts_with('#') {
        if trimmed.len() == 1 {
            return Some(InputLine::Comment(String::new()));
        } else {
            return Some(InputLine::Comment(trimmed[1..].to_string()))
        }
    }

    // Allow for comments at the end of a line, data is everything up to '#'
    let line_parts: Vec<&str> = trimmed.splitn(2, '#').collect();

    let (data, comment) =
        match line_parts.as_slice() {
            [_0, _1] => (_0, Some(_1)),
            [_0] => (_0, None),
            _ => return None
        };

    let cols: Vec<&str> = data.split(',')
        .map(|s| s.trim())
        .collect();

    let (topic_name, partitions, replication_factor, additional_config) = match cols.as_slice() {
        [_0, _1, _2] =>
            (_0, _1, _2, Vec::new()),
        [_0, _1, _2, xs @ ..] => {
            let mut additional_config: Vec<(String, String)> = Vec::new();

            for col in xs {
                let parts: Vec<&str> = col.split('=').map(|s| s.trim()).collect();

                match parts.as_slice() {
                    [_0, _1] if !_0.is_empty() && !_1.is_empty() => {
                        additional_config.push((_0.to_string(), _1.to_string()));
                    },
                    _ => return None
                }
            }

            (_0, _1, _2, additional_config)
        }
        _ => return None
    };

    let topic_definition = TopicDefinition {
        topic_name: topic_name.to_string(),
        partitions: partitions.parse::<i32>().ok()?,
        replication_factor: replication_factor.parse::<i32>().ok()?,
        additional_config: additional_config
    };


    match comment {
        Some(s) => Some(InputLine::DefinitionWithComment(topic_definition, s.to_string())),
        None => Some(InputLine::Definition(topic_definition))
    }
}

#[test]
fn test_read_line() {
    assert_eq!(read_line(""), Option::Some(InputLine::Empty));
    assert_eq!(read_line("    "), Option::Some(InputLine::Empty));
    assert_eq!(read_line("#"), Option::Some(InputLine::Comment("".to_string())));
    assert_eq!(read_line("# hello world"), Option::Some(InputLine::Comment(" hello world".to_string())));

    assert_eq!(read_line("adam,9,11"), Option::Some(InputLine::Definition(TopicDefinition {
        topic_name: "adam".to_string(),
        partitions: 9,
        replication_factor: 11,
        additional_config: Vec::new()
    })));

    assert_eq!(read_line("adam,9,11,a=b,c=d"), Option::Some(InputLine::Definition(TopicDefinition {
        topic_name: "adam".to_string(),
        partitions: 9,
        replication_factor: 11,
        additional_config: vec![("a".to_string(), "b".to_string()), ("c".to_string(), "d".to_string())]
    })));


    {
        let raw_line = "adam,9,11,a=b,c=d# hello world";

        let expected_topic_definition = TopicDefinition {
            topic_name: "adam".to_string(),
            partitions: 9,
            replication_factor: 11,
            additional_config: vec![("a".to_string(), "b".to_string()), ("c".to_string(), "d".to_string())]
        };

        let expected_comment = " hello world".to_string();

        assert_eq!(read_line(raw_line), Option::Some(InputLine::DefinitionWithComment(expected_topic_definition, expected_comment)))
    }

    {
        let raw_line = "adam,9,11#,a=b,c=d";

        let expected_topic_definition = TopicDefinition {
            topic_name: "adam".to_string(),
            partitions: 9,
            replication_factor: 11,
            additional_config: Vec::new()
        };

        let expected_comment = ",a=b,c=d".to_string();

        assert_eq!(read_line(raw_line), Option::Some(InputLine::DefinitionWithComment(expected_topic_definition, expected_comment)))
    }
}

/// TopicDefinition defines a Kafka topic to create.
#[derive(Debug, Eq, PartialEq)]
struct TopicDefinition {
    topic_name: String,
    partitions: i32,
    replication_factor: i32,

    /// key/value pairs of additional topic config, i.e cleanup.policy=compact.
    /// See the below link for valid config options:
    /// https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html
    additional_config: Vec<(String,String)>
}


/// InputLine represents a parsed line from the source input file
#[derive(Debug,Eq, PartialEq)]
enum InputLine {
    Comment(String),
    Definition(TopicDefinition),
    DefinitionWithComment(TopicDefinition, String),
    Empty
}

#[derive(Debug)]
enum ReadInputError {
    IoError(io::Error),
    ParseLineError(usize,String)
}
