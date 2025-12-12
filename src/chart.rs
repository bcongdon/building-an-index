use crate::benchmark::{AggregateResults, BenchmarkResult};
use crate::data_gen::BlobSize;
use anyhow::{Context, Result};
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};
use std::path::Path;

// Font sizes
// NOTE: These are intentionally large because SVGs are often viewed scaled down in browsers/docs.
const TITLE_FONT_SIZE: u32 = 44;
const AXIS_LABEL_FONT_SIZE: u32 = 26;
const TICK_LABEL_FONT_SIZE: u32 = 20;
const LEGEND_FONT_SIZE: u32 = 20;
const DATA_LABEL_FONT_SIZE: u32 = 16;

// Layout tuning
// Keep enough space for x tick labels + x-axis title, but avoid excessive empty bottom whitespace.
const DEFAULT_MARGIN_BOTTOM: u32 = 55;
const DEFAULT_X_LABEL_AREA_SIZE: u32 = 60;

/// Color palette for different backends
const COLORS: &[RGBColor] = &[
    RGBColor(66, 133, 244),  // Blue (SQLite WITHOUT ROWID)
    RGBColor(129, 180, 255), // Light blue (SQLite ROWID)
    RGBColor(251, 188, 5),   // Yellow (Hash DAT)
    RGBColor(52, 168, 83),   // Green (Zip)
];

fn get_backend_color(backend_name: &str) -> RGBColor {
    match backend_name {
        "SQLite (WITHOUT ROWID)" => COLORS[0],
        "SQLite (ROWID)" => COLORS[1],
        "Custom Offset File Format" => COLORS[2],
        "Zip" => COLORS[3],
        _ => RGBColor(128, 128, 128),
    }
}

fn get_backend_index(backend_name: &str) -> usize {
    match backend_name {
        "SQLite (WITHOUT ROWID)" => 0,
        "SQLite (ROWID)" => 1,
        "Custom Offset File Format" => 2,
        "Zip" => 3,
        _ => 4,
    }
}

/// Format latency for display
fn format_latency(micros: f64) -> String {
    if micros >= 1000.0 {
        format!("{:.1}ms", micros / 1000.0)
    } else {
        format!("{:.0}µs", micros)
    }
}

fn format_log_latency_tick(micros: f64) -> String {
    if micros <= 0.0 {
        return String::new();
    }
    // Only label powers of 10 on log axes (keeps SVGs readable).
    let log10 = micros.log10();
    let nearest = log10.round();
    if (log10 - nearest).abs() < 1e-6 {
        format_latency(micros)
    } else {
        String::new()
    }
}

/// Generate all benchmark charts
pub fn generate_charts(results: &AggregateResults, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir).context("Failed to create output directory")?;

    generate_latency_by_size_chart(results, output_dir)?;
    generate_throughput_chart(results, output_dir)?;
    generate_percentile_chart(results, output_dir)?;
    generate_percentile_1mb_linear_chart(results, output_dir)?;
    generate_p90_chart(results, output_dir)?;
    generate_memory_chart(results, output_dir)?;
    generate_file_size_chart(results, output_dir)?;

    Ok(())
}

/// Generate grouped bar chart showing P50 latency by blob size for each backend
fn generate_latency_by_size_chart(results: &AggregateResults, output_dir: &Path) -> Result<()> {
    let path = output_dir.join("latency_by_size.svg");
    let root = SVGBackend::new(&path, (1000, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let by_backend = results.by_backend();
    let mut backends: Vec<&str> = by_backend.keys().copied().collect();
    backends.sort_by_key(|b| get_backend_index(b));

    let num_backends = backends.len();
    let num_sizes = BlobSize::all().len();

    // Find latency range for log scale
    let min_latency = results
        .results
        .iter()
        .map(|r| r.p50().as_micros() as f64)
        .filter(|&v| v > 0.0)
        .fold(f64::MAX, |a, b| a.min(b))
        .max(0.1);

    let max_latency = results
        .results
        .iter()
        .map(|r| r.p50().as_micros() as f64)
        .fold(0.0_f64, |a, b| a.max(b))
        * 2.0;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "P50 Latency by Blob Size (log scale)",
            ("sans-serif", TITLE_FONT_SIZE),
        )
        .margin(20)
        .margin_bottom(DEFAULT_MARGIN_BOTTOM)
        .x_label_area_size(DEFAULT_X_LABEL_AREA_SIZE)
        .y_label_area_size(90)
        .build_cartesian_2d(
            -0.5..(num_sizes as f64 - 0.5),
            (min_latency..max_latency).log_scale(),
        )?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(num_sizes)
        .x_label_formatter(&|x| {
            let idx = x.round() as usize;
            if idx < num_sizes && (x - idx as f64).abs() < 0.3 {
                BlobSize::all()
                    .get(idx)
                    .map(|s| s.name().to_string())
                    .unwrap_or_default()
            } else {
                String::new()
            }
        })
        .y_desc("Latency (μs)")
        .x_desc("Blob Size")
        .label_style(("sans-serif", TICK_LABEL_FONT_SIZE))
        .axis_desc_style(("sans-serif", AXIS_LABEL_FONT_SIZE))
        .draw()?;

    // Draw grouped bars
    let group_width = 0.8;
    let bar_width = group_width / num_backends as f64;

    for (backend_idx, backend) in backends.iter().enumerate() {
        let color = get_backend_color(backend);

        if let Some(backend_results) = by_backend.get(backend) {
            for result in backend_results.iter() {
                let latency = result.p50().as_micros() as f64;
                if latency <= 0.0 {
                    continue;
                }

                let size_idx = BlobSize::all()
                    .iter()
                    .position(|&s| s == result.blob_size)
                    .unwrap_or(0);

                let x_center = size_idx as f64;
                let x_offset = (backend_idx as f64 - (num_backends as f64 - 1.0) / 2.0) * bar_width;
                let x_left = x_center + x_offset - bar_width / 2.0 + 0.02;
                let x_right = x_center + x_offset + bar_width / 2.0 - 0.02;

                chart.draw_series(std::iter::once(Rectangle::new(
                    [(x_left, min_latency), (x_right, latency)],
                    color.filled(),
                )))?;
            }
        }
    }

    // Draw legend
    for backend in &backends {
        let color = get_backend_color(backend);
        chart
            .draw_series(std::iter::once(Circle::new(
                (num_sizes as f64 - 1.0, max_latency),
                0,
                color.filled(),
            )))?
            .label(*backend)
            .legend(move |(x, y)| Rectangle::new([(x, y - 5), (x + 20, y + 5)], color.filled()));
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .label_font(("sans-serif", LEGEND_FONT_SIZE))
        .draw()?;

    root.present()?;
    println!("Generated: {}", path.display());
    Ok(())
}

/// Generate line chart showing throughput (ops/sec) vs blob size with log scale
fn generate_throughput_chart(results: &AggregateResults, output_dir: &Path) -> Result<()> {
    let path = output_dir.join("throughput.svg");
    let root = SVGBackend::new(&path, (1000, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let by_backend = results.by_backend();
    let mut backends: Vec<&str> = by_backend.keys().copied().collect();
    backends.sort_by_key(|b| get_backend_index(b));

    let num_sizes = BlobSize::all().len();

    // Find throughput range for log scale
    let min_throughput = results
        .results
        .iter()
        .map(|r| r.ops_per_second())
        .filter(|&v| v > 0.0)
        .fold(f64::MAX, |a, b| a.min(b))
        .max(1.0);

    let max_throughput = results
        .results
        .iter()
        .map(|r| r.ops_per_second())
        .fold(0.0_f64, |a, b| a.max(b))
        * 2.0;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Throughput by Blob Size - Mean Latency (log scale)",
            ("sans-serif", TITLE_FONT_SIZE),
        )
        .margin(20)
        .margin_bottom(DEFAULT_MARGIN_BOTTOM)
        .x_label_area_size(DEFAULT_X_LABEL_AREA_SIZE)
        .y_label_area_size(110)
        .build_cartesian_2d(
            -0.5..(num_sizes as f64 - 0.5),
            (min_throughput..max_throughput).log_scale(),
        )?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(num_sizes)
        .x_label_formatter(&|x| {
            let idx = x.round() as usize;
            if idx < num_sizes && (x - idx as f64).abs() < 0.3 {
                BlobSize::all()
                    .get(idx)
                    .map(|s| s.name().to_string())
                    .unwrap_or_default()
            } else {
                String::new()
            }
        })
        .y_desc("Operations/sec")
        .x_desc("Blob Size")
        .label_style(("sans-serif", TICK_LABEL_FONT_SIZE))
        .axis_desc_style(("sans-serif", AXIS_LABEL_FONT_SIZE))
        .draw()?;

    for backend in &backends {
        let color = get_backend_color(backend);

        if let Some(backend_results) = by_backend.get(backend) {
            let mut data: Vec<(f64, f64)> = backend_results
                .iter()
                .map(|r| {
                    let size_idx = BlobSize::all()
                        .iter()
                        .position(|&s| s == r.blob_size)
                        .unwrap_or(0);
                    (size_idx as f64, r.ops_per_second())
                })
                .filter(|(_, ops)| *ops > 0.0)
                .collect();
            data.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());

            if !data.is_empty() {
                chart
                    .draw_series(LineSeries::new(data.clone(), color.stroke_width(3)))?
                    .label(*backend)
                    .legend(move |(x, y)| {
                        PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(3))
                    });

                chart.draw_series(PointSeries::of_element(
                    data,
                    6,
                    color.filled(),
                    &|coord, size, style| {
                        EmptyElement::at(coord) + Circle::new((0, 0), size, style)
                    },
                ))?;
            }
        }
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .label_font(("sans-serif", LEGEND_FONT_SIZE))
        .draw()?;

    root.present()?;
    println!("Generated: {}", path.display());
    Ok(())
}

/// Generate chart showing P50, P95, P99 for each backend (10KB, log scale)
fn generate_percentile_chart(results: &AggregateResults, output_dir: &Path) -> Result<()> {
    let path = output_dir.join("percentiles.svg");
    let root = SVGBackend::new(&path, (1000, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let target_sizes = [BlobSize::Medium];
    let by_backend = results.by_backend();
    let mut backends: Vec<&str> = by_backend.keys().copied().collect();
    backends.sort_by_key(|b| get_backend_index(b));

    let relevant_results: Vec<&BenchmarkResult> = results
        .results
        .iter()
        .filter(|r| target_sizes.contains(&r.blob_size))
        .collect();

    if relevant_results.is_empty() {
        root.present()?;
        return Ok(());
    }

    // Find latency range for log scale
    let min_latency = relevant_results
        .iter()
        .map(|r| r.p50().as_micros() as f64)
        .filter(|&v| v > 0.0)
        .fold(f64::MAX, |a, b| a.min(b))
        .max(0.1);

    let max_latency = relevant_results
        .iter()
        .map(|r| r.p99().as_micros() as f64)
        .fold(0.0_f64, |a, b| a.max(b))
        * 2.5;

    for target_size in target_sizes.iter() {
        let caption = format!(
            "Latency Percentiles - {} blobs (log scale)",
            target_size.name()
        );

        let num_backends = backends.len();
        let mut chart = ChartBuilder::on(&root)
            .caption(caption, ("sans-serif", TITLE_FONT_SIZE))
            .margin(20)
            .margin_bottom(DEFAULT_MARGIN_BOTTOM)
            .x_label_area_size(DEFAULT_X_LABEL_AREA_SIZE)
            .y_label_area_size(90)
            .build_cartesian_2d(
                -0.5..(num_backends as f64 - 0.5),
                (min_latency..max_latency).log_scale(),
            )?;

        chart
            .configure_mesh()
            .disable_x_mesh()
            .x_labels(num_backends)
            .x_label_formatter(&|x| {
                let idx = x.round() as usize;
                if idx < num_backends && (x - idx as f64).abs() < 0.3 {
                    backends.get(idx).map(|s| s.to_string()).unwrap_or_default()
                } else {
                    String::new()
                }
            })
            .y_labels(8)
            .y_label_formatter(&|y| format_log_latency_tick(*y))
            .y_desc("Latency")
            .x_desc("Backend")
            .label_style(("sans-serif", TICK_LABEL_FONT_SIZE))
            .axis_desc_style(("sans-serif", AXIS_LABEL_FONT_SIZE))
            .draw()?;

        // Draw grouped bars for P50, P95, P99
        let percentile_colors = [
            RGBColor(100, 180, 100), // P50 - green
            RGBColor(200, 180, 80),  // P95 - yellow
            RGBColor(200, 100, 100), // P99 - red
        ];
        let percentile_names = ["P50", "P95", "P99"];
        let bar_width = 0.25;

        for (backend_idx, backend) in backends.iter().enumerate() {
            let maybe_result = results
                .results
                .iter()
                .find(|r| r.blob_size == *target_size && r.backend_name == *backend);

            let Some(result) = maybe_result else { continue };

            let p50 = result.p50().as_micros() as f64;
            let p95 = result.p95().as_micros() as f64;
            let p99 = result.p99().as_micros() as f64;
            let values = [p50, p95, p99];

            for (p_idx, &value) in values.iter().enumerate() {
                if value <= 0.0 {
                    continue;
                }

                let color = percentile_colors[p_idx];
                let x_center = backend_idx as f64;
                let x_offset = (p_idx as f64 - 1.0) * bar_width;
                let x_left = x_center + x_offset - bar_width / 2.0 + 0.02;
                let x_right = x_center + x_offset + bar_width / 2.0 - 0.02;
                let x_mid = (x_left + x_right) / 2.0;

                chart.draw_series(std::iter::once(Rectangle::new(
                    [(x_left, min_latency), (x_right, value)],
                    color.filled(),
                )))?;

                // Add data label on top of bar
                chart.draw_series(std::iter::once(Text::new(
                    format_latency(value),
                    (x_mid, value * 1.15),
                    ("sans-serif", DATA_LABEL_FONT_SIZE)
                        .into_font()
                        .color(&BLACK)
                        .pos(Pos::new(HPos::Center, VPos::Bottom)),
                )))?;
            }
        }

        // Add percentile legend (put it upper-left to avoid overlapping bars on the right)
        for (idx, name) in percentile_names.iter().enumerate() {
            let color = percentile_colors[idx];
            chart
                .draw_series(std::iter::once(Circle::new(
                    (num_backends as f64 - 1.0, max_latency),
                    0,
                    color.filled(),
                )))?
                .label(*name)
                .legend(move |(x, y)| {
                    Rectangle::new([(x, y - 5), (x + 20, y + 5)], color.filled())
                });
        }

        chart
            .configure_series_labels()
            .position(SeriesLabelPosition::UpperLeft)
            .background_style(WHITE.mix(0.85))
            .border_style(BLACK)
            .label_font(("sans-serif", LEGEND_FONT_SIZE))
            .draw()?;
    }

    root.present()?;
    println!("Generated: {}", path.display());
    Ok(())
}

/// Generate chart showing P50, P95, P99 for each backend (1MB, linear scale)
fn generate_percentile_1mb_linear_chart(
    results: &AggregateResults,
    output_dir: &Path,
) -> Result<()> {
    let target_size = BlobSize::Huge; // 1MB
    let path = output_dir.join("percentiles_1mb_linear.svg");
    let root = SVGBackend::new(&path, (1000, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let by_backend = results.by_backend();
    let mut backends: Vec<&str> = by_backend.keys().copied().collect();
    backends.sort_by_key(|b| get_backend_index(b));
    let num_backends = backends.len();

    let size_results: Vec<&BenchmarkResult> = results
        .results
        .iter()
        .filter(|r| r.blob_size == target_size)
        .collect();

    if size_results.is_empty() {
        root.present()?;
        return Ok(());
    }

    // Linear scale bounds
    let max_latency = size_results
        .iter()
        .map(|r| r.p99().as_micros() as f64)
        .fold(0.0_f64, |a, b| a.max(b))
        * 1.25;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!(
                "Latency Percentiles - {} blobs (linear)",
                target_size.name()
            ),
            ("sans-serif", TITLE_FONT_SIZE),
        )
        .margin(20)
        .margin_bottom(DEFAULT_MARGIN_BOTTOM)
        .x_label_area_size(DEFAULT_X_LABEL_AREA_SIZE)
        .y_label_area_size(90)
        .build_cartesian_2d(-0.5..(num_backends as f64 - 0.5), 0.0..max_latency.max(1.0))?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(num_backends)
        .x_label_formatter(&|x| {
            let idx = x.round() as usize;
            if idx < num_backends && (x - idx as f64).abs() < 0.3 {
                backends.get(idx).map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        })
        .y_labels(8)
        .y_label_formatter(&|y| format_latency(*y))
        .y_desc("Latency")
        .x_desc("Backend")
        .label_style(("sans-serif", TICK_LABEL_FONT_SIZE))
        .axis_desc_style(("sans-serif", AXIS_LABEL_FONT_SIZE))
        .draw()?;

    // Draw grouped bars for P50, P95, P99
    let percentile_colors = [
        RGBColor(100, 180, 100), // P50 - green
        RGBColor(200, 180, 80),  // P95 - yellow
        RGBColor(200, 100, 100), // P99 - red
    ];
    let percentile_names = ["P50", "P95", "P99"];
    let bar_width = 0.25;

    for (backend_idx, backend) in backends.iter().enumerate() {
        let maybe_result = results
            .results
            .iter()
            .find(|r| r.blob_size == target_size && r.backend_name == *backend);

        let Some(result) = maybe_result else { continue };

        let p50 = result.p50().as_micros() as f64;
        let p95 = result.p95().as_micros() as f64;
        let p99 = result.p99().as_micros() as f64;
        let values = [p50, p95, p99];

        for (p_idx, &value) in values.iter().enumerate() {
            if value <= 0.0 {
                continue;
            }

            let color = percentile_colors[p_idx];
            let x_center = backend_idx as f64;
            let x_offset = (p_idx as f64 - 1.0) * bar_width;
            let x_left = x_center + x_offset - bar_width / 2.0 + 0.02;
            let x_right = x_center + x_offset + bar_width / 2.0 - 0.02;
            let x_mid = (x_left + x_right) / 2.0;

            chart.draw_series(std::iter::once(Rectangle::new(
                [(x_left, 0.0), (x_right, value)],
                color.filled(),
            )))?;

            chart.draw_series(std::iter::once(Text::new(
                format_latency(value),
                (x_mid, value + max_latency * 0.02),
                ("sans-serif", DATA_LABEL_FONT_SIZE)
                    .into_font()
                    .color(&BLACK)
                    .pos(Pos::new(HPos::Center, VPos::Bottom)),
            )))?;
        }
    }

    for (idx, name) in percentile_names.iter().enumerate() {
        let color = percentile_colors[idx];
        chart
            .draw_series(std::iter::once(Circle::new(
                (num_backends as f64 - 1.0, max_latency),
                0,
                color.filled(),
            )))?
            .label(*name)
            .legend(move |(x, y)| Rectangle::new([(x, y - 5), (x + 20, y + 5)], color.filled()));
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.85))
        .border_style(BLACK)
        .label_font(("sans-serif", LEGEND_FONT_SIZE))
        .draw()?;

    root.present()?;
    println!("Generated: {}", path.display());
    Ok(())
}

/// Generate P90 latency chart across all blob sizes
fn generate_p90_chart(results: &AggregateResults, output_dir: &Path) -> Result<()> {
    let path = output_dir.join("p90_latency.svg");
    let root = SVGBackend::new(&path, (1000, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let by_backend = results.by_backend();
    let mut backends: Vec<&str> = by_backend.keys().copied().collect();
    backends.sort_by_key(|b| get_backend_index(b));

    let num_sizes = BlobSize::all().len();

    // Find latency range for log scale
    let min_latency = results
        .results
        .iter()
        .map(|r| r.p90().as_micros() as f64)
        .filter(|&v| v > 0.0)
        .fold(f64::MAX, |a, b| a.min(b))
        .max(0.1);

    let max_latency = results
        .results
        .iter()
        .map(|r| r.p90().as_micros() as f64)
        .fold(0.0_f64, |a, b| a.max(b))
        * 2.0;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "P90 Latency by Blob Size (log scale)",
            ("sans-serif", TITLE_FONT_SIZE),
        )
        .margin(20)
        .margin_bottom(DEFAULT_MARGIN_BOTTOM)
        .x_label_area_size(DEFAULT_X_LABEL_AREA_SIZE)
        .y_label_area_size(90)
        .build_cartesian_2d(
            -0.5..(num_sizes as f64 - 0.5),
            (min_latency..max_latency).log_scale(),
        )?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(num_sizes)
        .x_label_formatter(&|x| {
            let idx = x.round() as usize;
            if idx < num_sizes && (x - idx as f64).abs() < 0.3 {
                BlobSize::all()
                    .get(idx)
                    .map(|s| s.name().to_string())
                    .unwrap_or_default()
            } else {
                String::new()
            }
        })
        .y_desc("Latency (μs)")
        .x_desc("Blob Size")
        .label_style(("sans-serif", TICK_LABEL_FONT_SIZE))
        .axis_desc_style(("sans-serif", AXIS_LABEL_FONT_SIZE))
        .draw()?;

    for backend in &backends {
        let color = get_backend_color(backend);

        if let Some(backend_results) = by_backend.get(backend) {
            let mut data: Vec<(f64, f64)> = backend_results
                .iter()
                .map(|r| {
                    let size_idx = BlobSize::all()
                        .iter()
                        .position(|&s| s == r.blob_size)
                        .unwrap_or(0);
                    (size_idx as f64, r.p90().as_micros() as f64)
                })
                .filter(|(_, lat)| *lat > 0.0)
                .collect();
            data.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());

            if !data.is_empty() {
                chart
                    .draw_series(LineSeries::new(data.clone(), color.stroke_width(3)))?
                    .label(*backend)
                    .legend(move |(x, y)| {
                        PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(3))
                    });

                chart.draw_series(PointSeries::of_element(
                    data,
                    6,
                    color.filled(),
                    &|coord, size, style| {
                        EmptyElement::at(coord) + Circle::new((0, 0), size, style)
                    },
                ))?;
            }
        }
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .label_font(("sans-serif", LEGEND_FONT_SIZE))
        .draw()?;

    root.present()?;
    println!("Generated: {}", path.display());
    Ok(())
}

/// Generate memory usage comparison chart
fn generate_memory_chart(results: &AggregateResults, output_dir: &Path) -> Result<()> {
    let path = output_dir.join("memory_usage.svg");
    let root = SVGBackend::new(&path, (800, 500)).into_drawing_area();
    root.fill(&WHITE)?;

    let by_backend = results.by_backend();
    let mut backends: Vec<&str> = by_backend.keys().copied().collect();
    backends.sort_by_key(|b| get_backend_index(b));
    let num_backends = backends.len();

    // Collect memory data
    let memory_data: Vec<(&str, f64)> = backends
        .iter()
        .filter_map(|backend| {
            by_backend.get(backend).and_then(|results| {
                results
                    .first()
                    .map(|r| (*backend, r.memory_stats.physical_mem as f64 / 1_048_576.0))
            })
        })
        .collect();

    if memory_data.is_empty() {
        root.present()?;
        return Ok(());
    }

    let max_memory = memory_data
        .iter()
        .map(|(_, mem)| *mem)
        .fold(0.0_f64, |a, b| a.max(b))
        * 1.3;

    let mut chart = ChartBuilder::on(&root)
        .caption("Memory Usage by Backend", ("sans-serif", TITLE_FONT_SIZE))
        .margin(20)
        .margin_bottom(DEFAULT_MARGIN_BOTTOM)
        .x_label_area_size(DEFAULT_X_LABEL_AREA_SIZE)
        .y_label_area_size(90)
        .build_cartesian_2d(-0.5..(num_backends as f64 - 0.5), 0.0..max_memory.max(1.0))?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(num_backends)
        .x_label_formatter(&|x| {
            let idx = x.round() as usize;
            if idx < num_backends && (x - idx as f64).abs() < 0.3 {
                backends.get(idx).map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        })
        .y_desc("Memory (MB)")
        .x_desc("Backend")
        .label_style(("sans-serif", TICK_LABEL_FONT_SIZE))
        .axis_desc_style(("sans-serif", AXIS_LABEL_FONT_SIZE))
        .draw()?;

    let bar_width = 0.6;

    for (idx, backend) in backends.iter().enumerate() {
        let color = get_backend_color(backend);

        if let Some((_, mem_mb)) = memory_data.iter().find(|(b, _)| b == backend) {
            let x_center = idx as f64;
            let x_left = x_center - bar_width / 2.0;
            let x_right = x_center + bar_width / 2.0;

            chart.draw_series(std::iter::once(Rectangle::new(
                [(x_left, 0.0), (x_right, *mem_mb)],
                color.filled(),
            )))?;

            // Add value label on top of bar
            chart.draw_series(std::iter::once(Text::new(
                format!("{:.1} MB", mem_mb),
                (x_center, *mem_mb + max_memory * 0.03),
                ("sans-serif", DATA_LABEL_FONT_SIZE + 2)
                    .into_font()
                    .color(&BLACK)
                    .pos(Pos::new(HPos::Center, VPos::Bottom)),
            )))?;
        }
    }

    root.present()?;
    println!("Generated: {}", path.display());
    Ok(())
}

/// Generate file size comparison chart
fn generate_file_size_chart(results: &AggregateResults, output_dir: &Path) -> Result<()> {
    let path = output_dir.join("file_sizes.svg");
    let root = SVGBackend::new(&path, (800, 500)).into_drawing_area();
    root.fill(&WHITE)?;

    let by_backend = results.by_backend();
    let mut backends: Vec<&str> = by_backend.keys().copied().collect();
    backends.sort_by_key(|b| get_backend_index(b));
    let num_backends = backends.len();

    // Collect file size data
    let size_data: Vec<(&str, f64)> = backends
        .iter()
        .filter_map(|backend| {
            by_backend.get(backend).and_then(|results| {
                results
                    .first()
                    .map(|r| (*backend, r.file_size as f64 / 1_048_576.0))
            })
        })
        .collect();

    if size_data.is_empty() {
        root.present()?;
        return Ok(());
    }

    let max_size = size_data
        .iter()
        .map(|(_, size)| *size)
        .fold(0.0_f64, |a, b| a.max(b))
        * 1.3;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Index File Size by Backend",
            ("sans-serif", TITLE_FONT_SIZE),
        )
        .margin(20)
        // Give the x-axis title ("Backend") more breathing room from the axis line.
        .margin_bottom(DEFAULT_MARGIN_BOTTOM)
        .x_label_area_size(DEFAULT_X_LABEL_AREA_SIZE)
        .y_label_area_size(90)
        .build_cartesian_2d(-0.5..(num_backends as f64 - 0.5), 0.0..max_size.max(1.0))?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(num_backends)
        .x_label_formatter(&|x| {
            let idx = x.round() as usize;
            if idx < num_backends && (x - idx as f64).abs() < 0.3 {
                backends.get(idx).map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        })
        .y_desc("File Size (MB)")
        .x_desc("Backend")
        .label_style(("sans-serif", TICK_LABEL_FONT_SIZE))
        .axis_desc_style(("sans-serif", AXIS_LABEL_FONT_SIZE))
        .draw()?;

    let bar_width = 0.6;

    for (idx, backend) in backends.iter().enumerate() {
        let color = get_backend_color(backend);

        if let Some((_, size_mb)) = size_data.iter().find(|(b, _)| b == backend) {
            let x_center = idx as f64;
            let x_left = x_center - bar_width / 2.0;
            let x_right = x_center + bar_width / 2.0;

            chart.draw_series(std::iter::once(Rectangle::new(
                [(x_left, 0.0), (x_right, *size_mb)],
                color.filled(),
            )))?;

            // Add value label on top of bar
            chart.draw_series(std::iter::once(Text::new(
                format!("{:.1} MB", size_mb),
                (x_center, *size_mb + max_size * 0.03),
                ("sans-serif", DATA_LABEL_FONT_SIZE + 2)
                    .into_font()
                    .color(&BLACK)
                    .pos(Pos::new(HPos::Center, VPos::Bottom)),
            )))?;
        }
    }

    root.present()?;
    println!("Generated: {}", path.display());
    Ok(())
}
