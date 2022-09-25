use std::process::abort;
use std::time::Duration;
use anyhow::Result;
use serde::{Deserialize,Serialize};
use clickhouse::{Row,Client};
use tokio::time::Instant;

#[derive(Deserialize,Serialize,Debug,Row)]
struct Test<'a>{
    timestamp:i64,
    name:&'a str,
    money:f64
}

#[derive(Deserialize,Serialize,Debug,Row)]
struct TestOwned{
    timestamp:i64,
    name:String,
    money:f64
}

#[tokio::main]
async fn main()->Result<()> {
    let client=Client::default().with_user("default").with_password("a123123").with_url("http://127.0.0.1:8123").with_database("default");


    client.clone().with_option("mutations_sync", "1").query("ALTER TABLE test DELETE WHERE timestamp >= ?")
        .bind(0).execute().await?;

    let mut inserter =  client.inserter("test")?
        .with_max_entries(1_000_000) // `250_000` by default
        .with_max_duration(Duration::from_millis(500)); // `10s` by default


    tokio::spawn(async move{
        match tokio::signal::ctrl_c().await{
            Ok(())=>{
                println!("ctrl c");
            },
            Err(err)=>{
                eprintln!("Unable to listen for shutdown signal: {}",err);
            }
        }
        println!("start clear");
        abort();
    });


    let start=Instant::now();
    // // //
    // let mut insert = client.insert("test")?;
    //
    // for i in 0..10000000i64 {
    //     insert.write(&TestOwned { timestamp: i, name: i.to_string(), money: i as f64 }).await?;
    // }
    // insert.end().await?;
    // //

    for i in 0..10000000i64 {
        inserter.write(&TestOwned { timestamp: i, name: i.to_string(), money: i as f64 }).await?;
        if let Err(err)= inserter.commit().await{
            eprintln!("{}",err);
        }
    }

    inserter.end().await?;


    println!("write 10000000:{}",start.elapsed().as_secs_f32());

    let start=Instant::now();

    let data= client.query("select * from `test`")
        .fetch_all::<TestOwned>()
        .await?;

    println!("read:{}   time:{}",data.len(),start.elapsed().as_secs_f32());

    client.clone().with_option("mutations_sync", "1").query("ALTER TABLE test DELETE WHERE timestamp >= ?")
        .bind(0).execute().await?;

    Ok(())
}
