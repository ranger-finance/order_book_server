use crate::{prelude::*, types::node_data::EventSource};
use fs::File;
use io::Read;
use std::path::PathBuf;

// We want all of these functions to be synchronous just for ease of use since they are fast (for now)
// Asynchronous stuff can be done in the listen function (waiting for next file event)
pub trait DirectoryListener {
    // are we tracking a file right now
    fn is_reading(&self, event_source: EventSource) -> bool;
    // get file that we are tracking
    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File>;
    // when file is created what do we do?
    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()>;
    // how do we want to process data that we just processed?
    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()>;

    fn on_file_modification(&mut self, event_source: EventSource) -> Result<()> {
        let mut buf = String::new();
        let file = self.file_mut(event_source).as_mut().ok_or("No file being tracked")?;
        file.read_to_string(&mut buf)?;
        self.process_data(buf, event_source)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        listener::directory::{DirectoryListener, EventSource},
        prelude::*,
    };
    use fs::{File, create_dir_all, read_dir, remove_dir_all, remove_file};
    use log::{error, info};
    use notify::{RecursiveMode, Watcher, recommended_watcher};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::{
        io::{Seek, SeekFrom},
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio::{fs::File as TokioFile, io::AsyncWriteExt, sync::mpsc::unbounded_channel, time::sleep};

    const MOCK_HL_DIR: &str = "tmp/ws_listener_test";
    const DATA: [&str; 2] = [
        r#"{"coin":"@151","side":"A","time":"2025-06-24T02:56:36.172847427","px":"2393.9","sz":"0.1539","hash":"0x2b21750229be769650b604261eaac1018c00c45812652efbbdd35fe0ecb201a1","trade_dir_override":"Na","side_info":[{"user":"0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00","start_pos":"1166.565307356","oid":105686971733,"twap_id":null,"cloid":"0x1070fff92506b3ab5e5aec135e5a5ddd"},{"user":"0xb65117c1e1006e7b2413fa90e96fcbe3fa83ed75","start_pos":"0.153928559","oid":105686976226,"twap_id":null,"cloid":null}]}
{"coin":"@166","side":"A","time":"2025-06-24T02:56:36.172847427","px":"1.0003","sz":"184.11","hash":"0x0ffc6896b2147680820e04261eaac1018c0101735014e44b56f038478b13ad8f","trade_dir_override":"Na","side_info":[{"user":"0x107332a1729ba0bcf6171117815a87b72a7e6082","start_pos":"36301.55539655","oid":105686050113,"twap_id":null,"cloid":null},{"user":"0xb65117c1e1006e7b2413fa90e96fcbe3fa83ed75","start_pos":"184.12704003","oid":105686976227,"twap_id":null,"cloid":null}]}
"#,
        r#"{"coin":"STX","side":"B","time":"2025-06-24T02:56:36.894949789","px":"0.63591","sz":"30.6","hash":"0x7306fd0390c93a1cfdc504261eaac9010400e00556a0aef084a2a8161f025a09","trade_dir_override":"Na","side_info":[{"user":"0x31ca8395cf837de08b24da3f660e77761dfb974b","start_pos":"182153.4","oid":105686977410,"twap_id":null,"cloid":null},{"user":"0xee126cd566febeebee9a715812df601e1e69512c","start_pos":"4854.0","oid":105686843698,"twap_id":null,"cloid":"0x9d2c065a6cf37141775d522dcf38855e"}]}
"#,
    ];

    async fn listen<L: DirectoryListener>(listener: &mut L, event_source: EventSource, dir: &Path) -> Result<()> {
        let event_source_dir = event_source.event_source_dir(dir).canonicalize()?;
        info!("Monitoring directory: {}", event_source_dir.display());
        // monitoring the directory via the notify crate (gives file system events)
        let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
        let mut watcher = recommended_watcher(move |res| {
            let fs_event_tx = fs_event_tx.clone();
            if let Err(err) = fs_event_tx.send(res) {
                error!("Error sending event to processor via channel: {err}");
            }
        })?;

        watcher.watch(&event_source_dir, RecursiveMode::Recursive)?;
        loop {
            match fs_event_rx.recv().await {
                Some(Ok(event)) => {
                    // if a new file is created, start tracking it.
                    if event.kind.is_create() {
                        let new_path = &event.paths[0];
                        if new_path.is_file() {
                            info!("-- Event: {} created --", new_path.display());
                            listener.on_file_creation(new_path.clone(), event_source)?;
                        }
                    }
                    // Check for `Modify` event (only if the file is already initialized)
                    else if event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.is_file() {
                            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
                            // Unfortunately, we miss the update that occurs at this time step.
                            // We go to the end of the file to read for updates after that.
                            if listener.is_reading(event_source) {
                                info!("-- Event: {} modified --", new_path.display());
                                listener.on_file_modification(event_source)?;
                            } else {
                                info!("-- Event: {} created --", new_path.display());
                                let file = listener.file_mut(event_source);
                                let mut new_file = File::open(new_path)?;
                                new_file.seek(SeekFrom::End(0))?;
                                *file = Some(new_file);
                            }
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(Box::new(err));
                }
                None => {
                    // The channel disconnected, likely because the sender (watcher) was dropped.
                    // This usually means the program is shutting down or there's a problem.
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into()); // Exit the loop
                }
            }
        }
    }

    fn clear_dir_contents(path: &Path) -> Result<()> {
        if path.is_dir() {
            for entry in read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    remove_dir_all(&path)?;
                } else {
                    remove_file(&path)?;
                }
            }
        }
        Ok(())
    }

    async fn create_mock_data(event_source: EventSource, mock_dir: &Path) -> Result<String> {
        // set up so that the directory is initially empty
        let mut res = String::new();
        sleep(Duration::from_millis(100)).await;
        let mut rng = StdRng::from_seed([42; 32]);
        let mock_dir = event_source.event_source_dir(mock_dir).canonicalize()?;
        clear_dir_contents(&mock_dir)?;
        let mock_dir = mock_dir.join("hourly/20250624");
        create_dir_all(&mock_dir)?;

        // test that it works when we transition across files
        for (i, data) in DATA.iter().enumerate() {
            res += data;
            let lines = data.split_whitespace();
            let mut mock_file = TokioFile::create(mock_dir.join((i + 1).to_string())).await?;
            for line in lines {
                mock_file.write_all((line.to_string() + "\n").as_bytes()).await?;
                mock_file.flush().await?;
                let wait = rng.random_bool(0.25);
                // simulate writing transactions in separate blocks by waiting 0.1 seconds.
                if wait {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Ok(res)
    }

    // will listen to file events and collect their results in the history field
    struct TestListener {
        file: Option<File>,
        history: Arc<Mutex<String>>,
    }

    impl DirectoryListener for TestListener {
        fn is_reading(&self, _event_source: EventSource) -> bool {
            self.file.is_some()
        }

        fn file_mut(&mut self, _event_source: EventSource) -> &mut Option<File> {
            &mut self.file
        }

        fn on_file_creation(&mut self, new_file: PathBuf, _event_source: EventSource) -> Result<()> {
            let file = File::open(new_file)?;
            self.file = Some(file);
            Ok(())
        }

        #[allow(clippy::significant_drop_tightening)]
        fn process_data(&mut self, data: String, _event_source: EventSource) -> Result<()> {
            let mut history = self.history.lock().unwrap();
            *history += &data;
            Ok(())
        }
    }

    impl TestListener {
        fn new(history: Arc<Mutex<String>>) -> Self {
            Self { file: None, history }
        }
    }

    #[allow(clippy::unwrap_used)]
    #[allow(clippy::significant_drop_tightening)]
    #[tokio::test]
    async fn test_trade_listener() -> Result<()> {
        let mock_path = PathBuf::from(MOCK_HL_DIR);
        let event_source = EventSource::OrderStatuses;
        create_dir_all(event_source.event_source_dir(&mock_path))?;
        let history = Arc::new(Mutex::new(String::new()));
        let mut test_listener = TestListener::new(history.clone());
        {
            let mock_path = mock_path.clone();
            tokio::spawn(async move {
                if let Err(err) = listen(&mut test_listener, event_source, &mock_path).await {
                    error!("Listener error: {err}");
                }
            });
        }

        // get desired output
        let expected = create_mock_data(event_source, &mock_path).await?;
        sleep(Duration::from_secs(2)).await;
        let history = history.lock().unwrap();
        assert_eq!(*history, expected);
        Ok(())
    }
}
