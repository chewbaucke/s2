use std::{
    fmt::Debug,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use s2_sdk::{
    S2, S2Basin, S2Stream,
    append_session::AppendSessionConfig,
    types::{
        AccountEndpoint, AppendInput, AppendRecord, AppendRecordBatch, BasinEndpoint, BasinName,
        CreateBasinInput, CreateStreamInput, DeleteBasinInput, ListStreamsInput, ReadFrom,
        ReadInput, ReadSessionConfig, ReadStart, RetryConfig, S2Config, S2Endpoints, StreamName,
    },
};
use tokio::{
    process::Command,
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_util::task::AbortOnDropHandle;
use tracing::{
    Event, Metadata, Subscriber,
    field::Visit,
    span::{Attributes, Id, Record},
};
use uuid::Uuid;

type Error = Box<dyn std::error::Error + Send + Sync>;
type ReadResult = Result<(u64, Vec<u8>), String>;

const STAGING_CONTEXT: &str = "arn:aws:eks:us-east-1:688567268767:cluster/staging-qety9jdy-eks";

#[derive(Clone, Default)]
struct Signals {
    advice: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    next_span_id: Arc<AtomicU64>,
}

impl Subscriber for Signals {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("s2_sdk::session")
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span_id.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut message = Message::default();
        event.record(&mut message);
        if !message.0.is_empty() {
            println!(
                "SDK {}: {}",
                event.metadata().level(),
                message.0.trim_matches('"')
            );
        }
        self.advice.fetch_or(
            message
                .0
                .contains("reconnecting read session on server advice"),
            Ordering::Relaxed,
        );
        self.draining.fetch_or(
            message
                .0
                .contains("reconnecting append session while server drains"),
            Ordering::Relaxed,
        );
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

#[derive(Default)]
struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let access_token = std::env::var("S2_ACCESS_TOKEN")?;
    let signals = Signals::default();
    tracing::subscriber::set_global_default(signals.clone())?;

    let config = S2Config::new(access_token).with_endpoints(S2Endpoints::new(
        AccountEndpoint::new("https://qety9jdy.o-staging.aws.s2.dev")?,
        BasinEndpoint::new("https://{basin}.b-staging.s2.dev")?,
    )?);
    let setup = S2::new(config.clone())?;
    let sessions =
        S2::new(config.with_retry(RetryConfig::new().with_max_attempts(NonZeroU32::MIN)))?;
    let suffix = Uuid::new_v4().simple().to_string();
    let basin_name: BasinName = format!("rollout-check-{}", &suffix[..8]).parse()?;
    let stream_name: StreamName = "session".parse()?;

    println!("SETUP: creating staging basin {basin_name}");
    setup
        .create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;
    println!("SETUP: basin created");

    let basin = setup.basin(basin_name.clone());
    let result: Result<(), Error> = async {
        wait_for_basin(&basin).await?;
        println!("SETUP: creating stream {stream_name}");
        basin
            .create_stream(CreateStreamInput::new(stream_name.clone()))
            .await?;
        println!("SETUP: stream created");

        let stream = sessions
            .basin(basin_name.clone())
            .stream(stream_name.clone());
        verify(&stream, &signals).await
    }
    .await;

    println!("CLEANUP: deleting basin {basin_name}");
    let basin_cleanup = setup
        .delete_basin(DeleteBasinInput::new(basin_name).with_ignore_not_found(true))
        .await;
    match &basin_cleanup {
        Ok(()) => println!("CLEANUP: basin deletion requested"),
        Err(error) => println!("CLEANUP: basin deletion failed: {error}"),
    }

    result?;
    basin_cleanup?;
    Ok(())
}

async fn wait_for_basin(basin: &S2Basin) -> Result<(), Error> {
    println!("SETUP: waiting for basin DNS and routing");
    timeout(Duration::from_secs(60), async {
        let mut attempt = 1;
        loop {
            match basin
                .list_streams(ListStreamsInput::new().with_limit(1))
                .await
            {
                Ok(_) => {
                    println!("SETUP: basin endpoint ready");
                    return;
                }
                Err(error) => {
                    println!("SETUP: readiness attempt {attempt} failed: {error}");
                    attempt += 1;
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
    .await
    .map_err(|_| "basin endpoint was not ready within 60 seconds")?;
    Ok(())
}

async fn verify(stream: &S2Stream, signals: &Signals) -> Result<(), Error> {
    println!("VERIFY: opening append session with max_attempts=1");
    let append = stream.append_session(AppendSessionConfig::new());
    let before_ack = append.submit(input("before")?).await?.await?;
    println!(
        "VERIFY: initial append acknowledged at sequence {}",
        before_ack.start.seq_num
    );

    println!("VERIFY: opening tailing read session");
    let mut read = stream
        .read_session(
            ReadInput::new().with_start(ReadStart::new().with_from(ReadFrom::SeqNum(0))),
            ReadSessionConfig::new(),
        )
        .await?;
    let (records_tx, mut records_rx) = mpsc::unbounded_channel();
    let reader = AbortOnDropHandle::new(tokio::spawn(async move {
        while let Some(batch) = read.next().await {
            match batch {
                Ok(batch) => {
                    for record in batch.records {
                        if records_tx
                            .send(Ok((record.seq_num, record.body.to_vec())))
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = records_tx.send(Err(error.to_string()));
                    return;
                }
            }
        }
    }));

    expect_record(&mut records_rx, before_ack.start.seq_num, b"before").await?;
    println!("VERIFY: initial record received; append session is now idle");
    rollout_frontend().await?;

    println!("VERIFY: sending a record through the same append session");
    let after_ack = append.submit(input("after")?).await?.await?;
    println!(
        "VERIFY: post-rollout append acknowledged at sequence {}",
        after_ack.start.seq_num
    );
    expect_record(&mut records_rx, after_ack.start.seq_num, b"after").await?;
    println!("VERIFY: same read session received the post-rollout record");
    append.close().await?;
    reader.abort();
    let _ = reader.await;

    if !signals.advice.load(Ordering::Relaxed) {
        return Err("reconnect-advised flag was not observed".into());
    }
    if !signals.draining.load(Ordering::Relaxed) {
        return Err("server_draining terminal was not observed".into());
    }

    println!("PASS: reconnect-advised flag observed");
    println!("PASS: server_draining terminal observed");
    println!("PASS: SDK sessions reconnected with max_attempts=1");
    Ok(())
}

async fn rollout_frontend() -> Result<(), Error> {
    println!("ROLLOUT: using staging context {STAGING_CONTEXT}");
    let old_pods = kubectl_output(&[
        "get",
        "pods",
        "-n",
        "s2",
        "-l",
        "app.kubernetes.io/name=frontend",
        "-o",
        "name",
    ])
    .await?;
    let old_pods = old_pods.lines().collect::<Vec<_>>();
    if old_pods.is_empty() {
        return Err("no staging frontend pods found".into());
    }

    println!(
        "ROLLOUT: restarting frontend; old pods: {}",
        old_pods.join(", ")
    );
    kubectl(&["rollout", "restart", "deployment/frontend", "-n", "s2"]).await?;
    for pod in old_pods {
        println!("ROLLOUT: waiting for {pod} to exit");
        kubectl(&["wait", "--for=delete", pod, "-n", "s2", "--timeout=180s"]).await?;
    }
    println!("ROLLOUT: all old frontend pods exited");
    Ok(())
}

async fn kubectl(args: &[&str]) -> Result<(), Error> {
    let status = Command::new("kubectl")
        .args(["--context", STAGING_CONTEXT])
        .args(args)
        .status()
        .await?;
    if !status.success() {
        return Err(format!("kubectl {} failed", args.join(" ")).into());
    }
    Ok(())
}

async fn kubectl_output(args: &[&str]) -> Result<String, Error> {
    let output = Command::new("kubectl")
        .args(["--context", STAGING_CONTEXT])
        .args(args)
        .output()
        .await?;
    if !output.status.success() {
        return Err(format!(
            "kubectl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn input(body: &'static str) -> Result<AppendInput, Error> {
    Ok(AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new(body)?,
    ])?))
}

async fn expect_record(
    records: &mut mpsc::UnboundedReceiver<ReadResult>,
    expected_seq_num: u64,
    expected_body: &[u8],
) -> Result<(), Error> {
    let record = timeout(Duration::from_secs(20), records.recv())
        .await
        .map_err(|_| "timed out waiting for record")?
        .ok_or("read session ended")?
        .map_err(|error| -> Error { error.into() })?;
    if record != (expected_seq_num, expected_body.to_vec()) {
        return Err(format!("unexpected record: {record:?}").into());
    }
    Ok(())
}
