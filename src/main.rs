use std::collections::{HashMap, HashSet};
use bgpkit_broker::{BgpkitBroker, BrokerItem, QueryParams};
use bgpkit_parser::{BgpElem, BgpkitParser};
use chrono::NaiveDate;
use clap::Parser;
use rayon::prelude::*;
use tracing::info;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    /// ASN to check
    origin_asn: i64,

    /// Date to check
    date: NaiveDate,

    #[clap(short, long)]
    debug: bool,
}

fn main() {
    let cli = Cli::parse();
    let date: NaiveDate = cli.date;
    let datetime_str = date.and_time(Default::default()).timestamp().to_string();

    if cli.debug {
        tracing_subscriber::fmt().init();
    }


    let broker = BgpkitBroker::new_with_params("https://api.broker.bgpkit.com/v2", QueryParams{
        ts_start: Some(datetime_str.clone()),
        ts_end: Some(datetime_str.clone()),
        collector_id: Some("rrc00".to_string()),
        data_type: Some("rib".to_string()),
        ..Default::default()
    });

    let items: Vec<BrokerItem> = broker.into_iter().collect();

    let origin_asn_str: String = cli.origin_asn.to_string();

    let elems: Vec<BgpElem> = items.par_iter().flat_map(|item| {
        info!("start parsing {}", item.url.as_str());
        let parser = BgpkitParser::new(item.url.as_str()).unwrap()
            .add_filter("origin_asn", origin_asn_str.as_str()).unwrap()
            .add_filter("type", "a").unwrap();
        parser.into_elem_iter().collect::<Vec<BgpElem>>()
    }).collect();

    let uniq_paths = elems.iter().map(|elem|{
        elem.as_path.as_ref().unwrap().to_string()
    }).filter(|p| !p.contains('{')).collect::<HashSet<String>>();

    let total_paths_count = uniq_paths.len();
    let mut as_hop_count: HashMap<i64, usize> = HashMap::new();
    for path in uniq_paths {
        let hops = path.split(',').collect::<Vec<&str>>().into_iter().map(|hop|{hop.parse::<i64>().unwrap()}).collect::<HashSet<i64>>();
        hops.into_iter() .for_each(|hop|{
            as_hop_count.entry(hop).and_modify(|count| *count+=1).or_insert(1);
        });
    }

    let mut hash_vec: Vec<(i64, usize)> = as_hop_count.into_iter().collect();
    hash_vec.sort_by(|a, b| b.1.cmp(&a.1));
    hash_vec.iter().for_each(|(asn, count)|{
        let percentage = (*count as f64) / (total_paths_count as f64);
        println!("{},{},{}", asn, count, percentage);
    })
}