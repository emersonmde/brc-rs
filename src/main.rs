use std::fs::File;
use std::io::Error;
use std::thread;

use ahash::{AHashMap, HashMap};
use ahash::RandomState;
use memmap::{Mmap, MmapOptions};

#[derive(Debug)]
struct CalculationResult {
    min: f64,
    max: f64,
    sum: f64,
    len: usize,
}

fn main() -> std::io::Result<()> {
    let file = File::open("measurements.txt")?;

    let results = process_file(&file)?;

    println!("{:?}", results);
    println!("Total Length: {}", results.len());
    Ok(())
}

fn combine_maps(
    mut map1: AHashMap<String, CalculationResult>,
    map2: &AHashMap<String, CalculationResult>,
) -> AHashMap<String, CalculationResult> {
    for (key, value) in map2 {
        let entry = map1.entry(key.into()).or_insert(CalculationResult {
            min: f64::MAX,
            max: f64::MIN,
            sum: 0.0,
            len: 0,
        });
        entry.min = entry.min.min(value.min);
        entry.max = entry.max.max(value.max);
        entry.sum += value.sum;
        entry.len += value.len;
    }
    map1
}

fn process_file(file: &File) -> Result<AHashMap<String, CalculationResult>, Error> {
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mmap: &'static Mmap = Box::leak(Box::new(mmap));

    let len = mmap.len();

    let num_chunks = 10;
    let chunk_size = len / num_chunks;

    let mut threads = vec![];

    for i in 0..num_chunks {
        let mut start = 0;
        let mut end = len;
        if i > 0 {
            start = mmap.iter()
                .skip(i * chunk_size)
                .position(|&b| b == b'\n')
                .map_or(i * chunk_size, |pos| i * chunk_size + pos + 1);
        }
        if i < num_chunks - 1 {
            end = mmap.iter()
                .skip((i + 1) * chunk_size)
                .position(|&b| b == b'\n')
                .map_or((i + 1) * chunk_size, |pos| (i + 1) * chunk_size + pos + 1);
        }
        println!("Start: {}, End: {}", start, end);

        let handle = thread::spawn(move || {
            let chunk = &mmap[start..end];

            // let mut map: HashMap<String, Vec<f64>> = HashMap::with_hasher(RandomState::with_seed(42));
            // let mut calculation_map = HashMap::with_hasher(RandomState::with_seed(42));
            let mut map: AHashMap<String, CalculationResult> = AHashMap::new();
            chunk.split(|&b| b == b'\n').for_each(|line| {
                if line.is_empty() {
                    return;
                }
                if let Some(separator_index) = line.iter().position(|&b| b == b';') {
                    let (station, temp_with_separator) = line.split_at(separator_index);
                    let temp = &temp_with_separator[1..]; // Skip the separator itself

                    let station_name = std::str::from_utf8(station).unwrap_or_default();
                    let temperature_str = std::str::from_utf8(temp).unwrap_or_default();

                    if let Ok(temperature) = temperature_str.parse::<f64>() {
                        // map.insert(station_name.to_string(), temperature);
                        // map.entry(station_name.to_string())
                        //     .or_insert(vec![])
                        //     .push(temperature);
                        map.entry(station_name.to_string())
                            .and_modify(|e| {
                                e.min = e.min.min(temperature);
                                e.max = e.max.max(temperature);
                                e.sum += temperature;
                                e.len += 1;
                            })
                            .or_insert(CalculationResult {
                                min: temperature,
                                max: temperature,
                                sum: temperature,
                                len: 1,
                            });
                    }
                }

                // for (station, temperatures) in map.iter() {
                //     let mut min = f64::MAX;
                //     let mut max = f64::MIN;
                //     let mut sum = 0.0;
                //     for &temperature in temperatures {
                //         if temperature < min {
                //             min = temperature;
                //         }
                //         if temperature > max {
                //             max = temperature;
                //         }
                //         sum += temperature;
                //     }
                    // let min = temperatures.iter().fold(f64::MAX, |a, &b| a.min(b));
                    // let max = temperatures.iter().fold(f64::MIN, |a, &b| a.max(b));
                    // let sum: f64 = temperatures.iter().sum();
                //     let len = temperatures.len();
                //
                //     calculation_map.insert(
                //         station.to_string(),
                //         CalculationResult { min, max, sum, len },
                //     );
                // }
            });
            return map;
        });

        threads.push(handle);
    }

    let mut results = Vec::new();
    for handle in threads {
        results.push(handle.join().unwrap());
        println!("Thread finished");
    }

    let results = results.iter()
        .fold(AHashMap::new(), |acc, map| combine_maps(acc, map));
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weather_station_calculations() {
        let test_file_path = "test_measurements.txt";
        let file = File::open(test_file_path).unwrap();
        // Assume `process_file` is a function you've defined that encapsulates
        // the logic from `main` for processing the file and returning the results map
        let results = process_file(&file).unwrap();

        // Verify calculations for station1
        let station1 = results.get("station1").unwrap();
        assert_eq!(station1.min, 10.0);
        assert_eq!(station1.max, 20.0);
        assert_eq!(station1.sum, 30.0);
        assert_eq!(station1.len, 2);

        // Verify calculations for station2
        let station2 = results.get("station2").unwrap();
        assert_eq!(station2.min, 15.5);
        assert_eq!(station2.max, 25.5);
        assert_eq!(station2.sum, 41.0);
        assert_eq!(station2.len, 2);

        // Verify calculations for station3
        let station3 = results.get("station3").unwrap();
        assert_eq!(station3.min, -5.5);
        assert_eq!(station3.max, -2.5);
        assert_eq!(station3.sum, -8.0);
        assert_eq!(station3.len, 2);
    }
}
