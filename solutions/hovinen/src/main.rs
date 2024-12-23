use dashmap::DashMap;
use smallvec::SmallVec;
use std::sync::Arc;
use std::{io::Write, path::Path};
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use tokio_uring::fs::File;

const BUFFER_SIZE: usize = 1048576;
const JOB_COUNT: usize = 32;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let mut args = std::env::args().collect::<Vec<_>>();
    let result = process_file(std::mem::take(args.get_mut(1).expect("Require a filename"))).await?;
    output(std::io::stdout(), &result);
    Ok(())
}

async fn process_file(
    filename: String,
) -> Result<Vec<(String, f64, f64, f64)>, Box<dyn std::error::Error>> {
    let (ready_sender, mut ready_receiver) = tokio::sync::mpsc::channel(JOB_COUNT);
    let (return_sender, return_receiver) = tokio::sync::mpsc::channel(JOB_COUNT);
    std::thread::spawn(move || {
        tokio_uring::start(async {
            read_file(Path::new(&filename), ready_sender, return_receiver).await
        });
    });
    let mut blocks_read = HashMap::new();
    let cities = Arc::new(DashMap::new());
    let mut jobs = JoinSet::new();
    let return_sender = Arc::new(return_sender);
    while let Some(read_buffer) = ready_receiver.recv().await {
        blocks_read.insert(read_buffer.index, read_buffer);
        let mut blocks_to_process = vec![];
        for block_index in blocks_read.keys() {
            if blocks_read.contains_key(&(*block_index + 1)) {
                blocks_to_process.push(*block_index);
            }
        }
        for block_index in blocks_to_process {
            let block = blocks_read.remove(&block_index).unwrap();
            let next_block = blocks_read.get(&(block_index + 1)).unwrap();
            let following_block = if let Some((index, _)) = next_block
                .data
                .iter()
                .enumerate()
                .find(|(_, c)| **c == '\n' as u8)
            {
                SmallVec::<[u8; 64]>::from(&next_block.data[..index])
            } else {
                SmallVec::<[u8; 64]>::from(next_block.data.as_slice())
            };
            jobs.spawn(processing_job(
                return_sender.clone(),
                cities.clone(),
                block,
                following_block,
            ));
        }
    }
    for (_, block) in blocks_read {
        jobs.spawn(processing_job(
            return_sender.clone(),
            cities.clone(),
            block,
            SmallVec::default(),
        ));
    }
    jobs.join_all().await;
    let mut results = cities
        .iter()
        .map(|entry| {
            let city = entry.key();
            let (min, max, sum, count) = entry.value();
            (city.to_string(), *min, *sum / *count as f64, *max)
        })
        .collect::<Vec<_>>();
    results.sort_by(|(v1, _, _, _), (v2, _, _, _)| v1.cmp(v2));
    Ok(results)
}

async fn read_file(path: &Path, send_queue: Sender<Buffer>, mut return_queue: Receiver<Buffer>) {
    let file = File::open(path).await.expect("Failed to open file");
    let mut available_buffers = Vec::with_capacity(JOB_COUNT);
    for _ in 0..JOB_COUNT {
        available_buffers.push(Buffer::new());
    }
    let mut index = 0;
    while let Some(mut buffer) = available_buffers.pop() {
        buffer.index = index;
        let (result, data) = file
            .read_at(buffer.data, (index * BUFFER_SIZE) as u64)
            .await;
        if process_read_result(result, data, &send_queue, index).await {
            return;
        }
        index += 1;
    }
    while let Some(mut buffer) = return_queue.recv().await {
        buffer.index = index;
        let (result, data) = file
            .read_at(buffer.data, (index * BUFFER_SIZE) as u64)
            .await;
        if process_read_result(result, data, &send_queue, index).await {
            return;
        }
        index += 1;
    }
}

async fn process_read_result(
    result: std::io::Result<usize>,
    data: Vec<u8>,
    send_queue: &Sender<Buffer>,
    index: usize,
) -> bool {
    match result {
        Ok(count) => {
            if count == 0 {
                log::info!("End of file reached");
                true
            } else {
                let new_buffer = Buffer { data, index, count };
                send_queue
                    .send(new_buffer)
                    .await
                    .expect("Failed to send buffer");
                false
            }
        }
        Err(error) => {
            log::error!("Error reading file: {error}");
            true
        }
    }
}

async fn processing_job(
    return_sender: Arc<Sender<Buffer>>,
    cities: Arc<DashMap<String, (f64, f64, f64, u32)>>,
    read_buffer: Buffer,
    following_buffer: SmallVec<[u8; 64]>,
) {
    process_buffer(
        &cities,
        &read_buffer.data[0..read_buffer.count],
        &following_buffer,
        read_buffer.index == 0,
    );
    let _ = return_sender.send(read_buffer).await;
}

struct Buffer {
    data: Vec<u8>,
    index: usize,
    count: usize,
}

impl Buffer {
    fn new() -> Self {
        Self {
            data: vec![0u8; BUFFER_SIZE],
            index: 0,
            count: 0,
        }
    }
}

fn process_buffer(
    data: &DashMap<String, (f64, f64, f64, u32)>,
    buffer: &[u8],
    following_buffer: &[u8],
    is_first: bool,
) {
    let mut index = if is_first {
        0
    } else {
        if let Some((newline_index, _)) = buffer.iter().enumerate().find(|(_, c)| **c == '\n' as u8)
        {
            newline_index + 1
        } else {
            return;
        }
    };
    while let Some((newline_index, _)) = buffer
        .iter()
        .enumerate()
        .skip(index)
        .find(|(_, c)| **c == '\n' as u8)
    {
        process_line(data, &buffer[index..newline_index]);
        index = newline_index + 1;
    }
    if following_buffer.len() > 0 {
        let mut line_buffer = Vec::from(&buffer[index..]);
        line_buffer.extend(following_buffer);
        process_line(data, &line_buffer);
    } else {
        process_line(data, &buffer[index..]);
    }
}

fn process_line(data: &DashMap<String, (f64, f64, f64, u32)>, line_buffer: &[u8]) {
    if line_buffer.len() == 0 {
        return;
    }
    let Some((separator_index, _)) = line_buffer
        .iter()
        .enumerate()
        .find(|(_, c)| **c == ';' as u8)
    else {
        log::error!(
            "Invalid line: {line}",
            line = std::str::from_utf8(line_buffer).unwrap_or(&format!("{line_buffer:?}"))
        );
        return;
    };

    let len = line_buffer.len();
    let city = unsafe { std::str::from_utf8_unchecked(&line_buffer[..separator_index]) };
    let measurement_str =
        unsafe { std::str::from_utf8_unchecked(&line_buffer[separator_index + 1..len]) };
    let Ok(measurement) = measurement_str.parse::<f64>() else {
        log::error!("Could not parse {:?}", measurement_str.as_bytes());
        return;
    };
    if let Some(mut entry) = data.get_mut(city) {
        entry.0 = f64::min(entry.0, measurement);
        entry.1 = f64::max(entry.1, measurement);
        entry.2 += measurement;
        entry.3 += 1;
    } else {
        data.insert(city.to_string(), (measurement, measurement, measurement, 1));
    }
}

fn output(mut writer: impl Write, lines: &[(String, f64, f64, f64)]) {
    writeln!(writer, "{{").unwrap();
    for (ref city, min, mean, max) in lines[0..lines.len() - 1].iter() {
        writeln!(writer, "    {city}={min:0.1}/{mean:0.1}/{max:0.1},").unwrap();
    }
    let (ref city, min, mean, max) = lines[lines.len() - 1];
    writeln!(writer, "    {city}={min:0.1}/{mean:0.1}/{max:0.1}").unwrap();
    write!(writer, "}}").unwrap();
}

#[cfg(test)]
mod tests {
    use super::{output, process_file};
    use core::str;
    use googletest::prelude::*;
    use std::{
        fs::read_to_string,
        io::{BufWriter, Write},
    };
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn outputs_mean_max_min_of_singleton() -> Result<()> {
        let tempfile = write_content("Arbitrary city;12.3");

        let result = process_file(name_of(&tempfile)).await.unwrap();

        verify_that!(
            result,
            unordered_elements_are![(
                eq("Arbitrary city"),
                approx_eq(12.3),
                approx_eq(12.3),
                approx_eq(12.3),
            )]
        )
    }

    #[tokio::test]
    async fn outputs_correct_data_with_negative_singleton() -> Result<()> {
        let tempfile = write_content("Arbitrary city;-12.3");

        let result = process_file(name_of(&tempfile)).await.unwrap();

        verify_that!(
            result,
            unordered_elements_are![(
                eq("Arbitrary city"),
                approx_eq(-12.3),
                approx_eq(-12.3),
                approx_eq(-12.3),
            )]
        )
    }

    #[tokio::test]
    async fn outputs_mean_max_min_of_singleton_with_two_measurements() -> Result<()> {
        let tempfile = write_content("Arbitrary city;10.0\nArbitrary city;20.0");

        let result = process_file(name_of(&tempfile)).await.unwrap();

        verify_that!(
            result,
            unordered_elements_are![(
                eq("Arbitrary city"),
                approx_eq(10.0),
                approx_eq(15.0),
                approx_eq(20.0),
            )]
        )
    }

    #[tokio::test]
    async fn outputs_mean_max_min_of_two_entries() -> Result<()> {
        let tempfile = write_content("Arbitrary city;12.3\nDifferent city;45.6");

        let result = process_file(name_of(&tempfile)).await.unwrap();

        verify_that!(
            result,
            unordered_elements_are![
                (
                    eq("Arbitrary city"),
                    approx_eq(12.3),
                    approx_eq(12.3),
                    approx_eq(12.3),
                ),
                (
                    eq("Different city"),
                    approx_eq(45.6),
                    approx_eq(45.6),
                    approx_eq(45.6),
                )
            ]
        )
    }

    #[tokio::test]
    async fn outputs_are_sorted_alphabetically() -> Result<()> {
        let tempfile = write_content("C;1\nB;2\nA;3\nD;5");

        let result = process_file(name_of(&tempfile)).await.unwrap();

        verify_that!(
            result,
            elements_are![
                (eq("A"), anything(), anything(), anything()),
                (eq("B"), anything(), anything(), anything()),
                (eq("C"), anything(), anything(), anything()),
                (eq("D"), anything(), anything(), anything()),
            ]
        )
    }

    #[tokio::test]
    async fn output_matches_sample() -> Result<()> {
        let mut writer = BufWriter::new(Vec::new());

        let result = process_file("../../samples/weather_100.csv".to_string())
            .await
            .unwrap();
        output(&mut writer, &result);

        let expected = read_to_string("../../samples/expected/weather_100.txt")?;
        let actual = String::from_utf8(writer.into_inner()?)?;
        verify_that!(actual, eq(expected))
    }

    #[tokio::test]
    async fn output_matches_larger_sample() -> Result<()> {
        env_logger::init();
        let mut writer = BufWriter::new(Vec::new());

        let result = process_file("../../samples/weather_1M.csv".to_string())
            .await
            .unwrap();
        output(&mut writer, &result);

        let expected = read_to_string("../../samples/expected/weather_1M.txt")?;
        let actual = String::from_utf8(writer.into_inner()?)?;
        verify_that!(actual, eq(expected))
    }

    fn name_of(tempfile: &NamedTempFile) -> String {
        tempfile.path().to_str().unwrap().to_string()
    }

    fn write_content(content: &str) -> NamedTempFile {
        let mut file = tempfile::Builder::new()
            .prefix("test_content_")
            .suffix(".csv")
            .tempfile()
            .unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }
}
