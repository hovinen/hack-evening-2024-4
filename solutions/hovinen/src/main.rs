use ahash::{AHashMap, AHashSet};
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use smallvec::SmallVec;
use std::collections::hash_map::Entry;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::{io::Write, path::Path};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use tokio_uring::fs::File;

#[cfg(not(test))]
const BUFFER_SIZE: usize = 1048576;
#[cfg(test)]
const BUFFER_SIZE: usize = 512;
const JOB_COUNT: usize = 64;

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
    let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        tokio_uring::start(async {
            read_file(
                Path::new(&filename),
                ready_sender,
                return_receiver,
                completion_sender,
            )
            .await
        });
    });
    let mut blocks_read = AHashMap::new();
    let mut initial_data = AHashMap::new();
    let mut jobs = JoinSet::new();
    let return_sender = Arc::new(return_sender);
    while let Some(read_buffer) = ready_receiver.recv().await {
        log::info!(
            "Block {block_index} ready for processing",
            block_index = read_buffer.block_index
        );
        initial_data.insert(
            read_buffer.block_index,
            if let Some((index, _)) = read_buffer
                .data
                .iter()
                .enumerate()
                .find(|(_, c)| **c == '\n' as u8)
            {
                SmallVec::<[u8; 64]>::from(&read_buffer.data[..index])
            } else {
                SmallVec::<[u8; 64]>::from(read_buffer.data.as_slice())
            },
        );

        blocks_read.insert(read_buffer.block_index, read_buffer);

        let mut blocks_to_process = vec![];
        for block_index in blocks_read.keys() {
            if initial_data.contains_key(&(*block_index + 1)) {
                blocks_to_process.push(*block_index);
            }
        }
        blocks_to_process.sort();
        for block_index in blocks_to_process {
            let block = blocks_read.remove(&block_index).unwrap();
            let following_block = initial_data
                .remove(&(block_index + 1))
                .expect("Following initial data not found");
            log::info!(
                "Starting processing for block {block_index}",
                block_index = block.block_index
            );
            jobs.spawn(processing_job(
                return_sender.clone(),
                block,
                following_block,
            ));
        }
    }
    for (_, block) in blocks_read {
        let following_block = initial_data
            .remove(&(block.block_index + 1))
            .unwrap_or_default();
        log::info!(
            "Starting processing for remaining block {block_index}",
            block_index = block.block_index
        );
        jobs.spawn(processing_job(
            return_sender.clone(),
            block,
            following_block,
        ));
    }
    jobs.join_all().await;
    drop(return_sender);
    log::info!("Awaiting completed buffers");
    let mut buffers = completion_receiver.await.unwrap();
    log::info!("Received completed buffers");
    let blocks_processed = buffers
        .iter_mut()
        .flat_map(|buffer| buffer.blocks_processed.drain())
        .collect::<AHashSet<_>>();
    let block_count = blocks_processed.iter().copied().max().unwrap_or(0);
    for index in 0..=block_count {
        if !blocks_processed.contains(&index) {
            log::error!("Block {index} not processed!");
        }
    }
    let results = buffers.into_par_iter().map(|buffer| buffer.cities).reduce(
        || Default::default(),
        |mut acc_cities, new_cities| {
            for (city, (min, max, sum, count)) in new_cities {
                match acc_cities.entry(city) {
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        entry.0 = f64::min(entry.0, min);
                        entry.1 = f64::max(entry.1, max);
                        entry.2 += sum;
                        entry.3 += count;
                    }
                    Entry::Vacant(entry) => {
                        entry.insert((min, max, sum, count));
                    }
                };
            }
            acc_cities
        },
    );
    let mut results = results
        .into_iter()
        .map(|(city, (min, max, sum, count))| (city, min, sum / count as f64, max))
        .collect::<Vec<_>>();
    results.sort_by(|(v1, _, _, _), (v2, _, _, _)| v1.cmp(v2));
    Ok(results)
}

async fn read_file(
    path: &Path,
    send_queue: Sender<Buffer>,
    mut return_queue: Receiver<Buffer>,
    completion_queue: tokio::sync::oneshot::Sender<Vec<Buffer>>,
) {
    let file = Arc::new(File::open(path).await.expect("Failed to open file"));
    let file_size = path
        .metadata()
        .expect("Could not read file metadata")
        .size();
    let mut available_buffers = Vec::with_capacity(JOB_COUNT);
    for _ in 0..JOB_COUNT {
        available_buffers.push(Buffer::new());
    }
    let total_blocks = (file_size as usize + BUFFER_SIZE - 1) / BUFFER_SIZE;
    log::info!("Reading {total_blocks} blocks for a file of size {file_size}");
    let mut read_jobs = JoinSet::new();
    for block_index in 0..total_blocks {
        let mut buffer = get_next_buffer(&mut available_buffers, &mut return_queue).await;
        let file = file.clone();
        let send_queue = send_queue.clone();
        read_jobs.spawn_local(async move {
            let data = std::mem::take(&mut buffer.data);
            let (result, data) = file.read_at(data, (block_index * BUFFER_SIZE) as u64).await;
            match result {
                Ok(count) => {
                    if count == 0 {
                        log::error!("Unexpected premature end of file");
                        return;
                    } else {
                        log::info!("Sending block {block_index} with {count} bytes");
                        buffer = Buffer {
                            data,
                            count,
                            block_index,
                            ..buffer
                        };
                        send_queue
                            .send(buffer)
                            .await
                            .expect("Failed to send buffer");
                    }
                }
                Err(error) => {
                    log::error!("Error reading file: {error}");
                    return;
                }
            }
        });
    }
    log::info!("End of file reached");
    drop(send_queue);
    read_jobs.join_all().await;
    log::info!("All read jobs completed");
    send_completed_buffers(&mut return_queue, completion_queue).await;
}

async fn get_next_buffer(
    available_buffers: &mut Vec<Buffer>,
    return_queue: &mut Receiver<Buffer>,
) -> Buffer {
    if let Some(buffer) = available_buffers.pop() {
        buffer
    } else if let Some(buffer) = return_queue.recv().await {
        buffer
    } else {
        panic!("No buffers available to store file data!");
    }
}

async fn processing_job(
    return_sender: Arc<Sender<Buffer>>,
    mut read_buffer: Buffer,
    following_buffer: SmallVec<[u8; 64]>,
) {
    log::info!(
        "Started processing job for block {block_index}",
        block_index = read_buffer.block_index
    );
    process_buffer(
        &mut read_buffer.cities,
        &read_buffer.data[0..read_buffer.count],
        &following_buffer,
        read_buffer.block_index == 0,
    );
    log::info!(
        "Completed processing. Returning buffer for block {block_index}",
        block_index = read_buffer.block_index
    );
    read_buffer.blocks_processed.insert(read_buffer.block_index);
    let _ = return_sender.send(read_buffer).await;
}

async fn send_completed_buffers(
    return_queue: &mut Receiver<Buffer>,
    completion_queue: tokio::sync::oneshot::Sender<Vec<Buffer>>,
) {
    log::info!("Assembling completed buffers");
    let mut completed_buffers = Vec::with_capacity(JOB_COUNT);
    while let Some(buffer) = return_queue.recv().await {
        log::info!("Received buffer {index}", index = buffer.block_index);
        completed_buffers.push(buffer);
    }
    log::info!("Sending completed buffers");
    completion_queue
        .send(completed_buffers)
        .expect("Unable to send completed buffers");
}

#[derive(Debug)]
struct Buffer {
    data: Vec<u8>,
    block_index: usize,
    count: usize,
    cities: AHashMap<String, (f64, f64, f64, u32)>,
    blocks_processed: AHashSet<usize>,
}

impl Buffer {
    fn new() -> Self {
        Self {
            data: vec![0u8; BUFFER_SIZE],
            block_index: 0,
            count: 0,
            cities: Default::default(),
            blocks_processed: Default::default(),
        }
    }
}

fn process_buffer(
    data: &mut AHashMap<String, (f64, f64, f64, u32)>,
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

fn process_line(data: &mut AHashMap<String, (f64, f64, f64, u32)>, line_buffer: &[u8]) {
    if line_buffer.len() == 0 {
        return;
    }
    let Some((separator_index, _)) = line_buffer
        .iter()
        .enumerate()
        .rev()
        .skip(3)
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
    let measurement = parse_measurement(measurement_str);
    if let Some(entry) = data.get_mut(city) {
        entry.0 = f64::min(entry.0, measurement);
        entry.1 = f64::max(entry.1, measurement);
        entry.2 += measurement;
        entry.3 += 1;
    } else {
        data.insert(city.to_string(), (measurement, measurement, measurement, 1));
    }
}

fn parse_measurement(measurement: &str) -> f64 {
    const DIGITS_1: [i32; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    const DIGITS_10: [i32; 10] = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90];
    const DIGITS_100: [i32; 10] = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900];
    let measurement_bytes = measurement.as_bytes();
    unsafe {
        let tenths_char = *measurement_bytes.get_unchecked(measurement_bytes.len() - 1);
        let mut total = *DIGITS_1.get_unchecked(tenths_char as usize - '0' as usize);
        let ones_char = *measurement_bytes.get_unchecked(measurement_bytes.len() - 3);
        total += *DIGITS_10.get_unchecked(ones_char as usize - '0' as usize);
        if measurement_bytes.len() > 3
            && measurement_bytes[measurement_bytes.len() - 4] != '-' as u8
        {
            let tens_char = *measurement_bytes.get_unchecked(measurement_bytes.len() - 4);
            total += *DIGITS_100.get_unchecked(tens_char as usize - '0' as usize);
        }
        if measurement_bytes[0] == '-' as u8 {
            total = -total;
        }
        total as f64 / 10.0
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
        env_logger::init();
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
        let tempfile = write_content("C;1.0\nB;2.0\nA;3.0\nD;5.0");

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
