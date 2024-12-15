use dashmap::DashMap;
use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::Path,
};

const BUFFER_SIZE: usize = 1024;

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let result = process_file(&Path::new(&args.get(1).expect("Require a filename"))).await;
    output(std::io::stdout(), &result);
}

async fn process_file(path: &Path) -> Vec<(String, f64, f64, f64)> {
    let file = File::open(path).expect("Cannot open file");
    let mut reader = BufReader::new(file);
    let cities = DashMap::<String, _>::new();
    let mut buffer1 = [0u8; BUFFER_SIZE];
    let mut buffer2 = [0u8; BUFFER_SIZE];
    let mut reading: &mut [u8] = &mut buffer1;
    let mut maybe_processing: Option<&mut [u8]> = None;
    let mut reserve: Option<&mut [u8]> = Some(&mut buffer2);
    let mut processing_count = 0;
    let mut is_first = true;
    while let Ok(count) = reader.read(reading) {
        if count == 0 {
            break;
        }
        if let Some(processing) = maybe_processing {
            process_buffer(
                &cities,
                &processing[0..processing_count],
                Some(&reading[0..count]),
                is_first,
            );
            (reading, maybe_processing) = (processing, Some(reading));
            is_first = false;
        } else if let Some(reserve) = reserve.take() {
            (reading, maybe_processing) = (reserve, Some(reading));
        }
        processing_count = count;
    }
    if let Some(m_processing) = maybe_processing {
        process_buffer(&cities, &m_processing[0..processing_count], None, is_first);
    }
    let mut results = cities
        .into_iter()
        .map(|(city, (min, max, sum, count))| (city.to_string(), min, sum / count as f64, max))
        .collect::<Vec<_>>();
    results.sort_by(|(v1, _, _, _), (v2, _, _, _)| v1.cmp(v2));
    results
}

fn process_buffer(
    data: &DashMap<String, (f64, f64, f64, u32)>,
    buffer: &[u8],
    following_buffer: Option<&[u8]>,
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
    if let Some(following_buffer) = following_buffer {
        let following_index = if let Some((newline_index, _)) = following_buffer
            .iter()
            .enumerate()
            .find(|(_, c)| **c == '\n' as u8)
        {
            newline_index
        } else {
            following_buffer.len()
        };
        let mut line_buffer = Vec::from(&buffer[index..]);
        line_buffer.extend(&following_buffer[..following_index]);
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
        panic!(
            "Invalid line: {line}",
            line = std::str::from_utf8(line_buffer).unwrap_or(&format!("{line_buffer:?}"))
        );
    };
    let len = line_buffer.len();
    let city = unsafe { std::str::from_utf8_unchecked(&line_buffer[..separator_index]) };
    let measurement_str =
        unsafe { std::str::from_utf8_unchecked(&line_buffer[separator_index + 1..len]) };
    let Ok(measurement) = measurement_str.parse::<f64>() else {
        panic!("Could not parse {:?}", measurement_str.as_bytes());
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
        path::Path,
    };

    #[tokio::test]
    async fn outputs_mean_max_min_of_singleton() -> Result<()> {
        let tempfile = write_content("Arbitrary city;12.3");

        let result = process_file(tempfile.path()).await;

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

        let result = process_file(tempfile.path()).await;

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

        let result = process_file(tempfile.path()).await;

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

        let result = process_file(tempfile.path()).await;

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

        let result = process_file(tempfile.path()).await;

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

        let result = process_file(Path::new("../../samples/weather_100.csv")).await;
        output(&mut writer, &result);

        let expected = read_to_string("../../samples/expected/weather_100.txt").unwrap();
        let actual = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        verify_that!(actual, eq(expected))
    }

    #[tokio::test]
    async fn output_matches_larger_sample() -> Result<()> {
        let mut writer = BufWriter::new(Vec::new());

        let result = process_file(Path::new("../../samples/weather_1M.csv")).await;
        output(&mut writer, &result);

        let expected = read_to_string("../../samples/expected/weather_1M.txt").unwrap();
        let actual = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        verify_that!(actual, eq(expected))
    }

    fn write_content(content: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::Builder::new()
            .prefix("test_content_")
            .suffix(".csv")
            .tempfile()
            .unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }
}
