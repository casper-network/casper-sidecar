use std::{ops::Range, sync::Arc};

use plotters::{
    chart::ChartState,
    coord::{types::RangedCoordu64, Shift},
    prelude::*,
};

struct Axis {
    label: &'static str,
    range: Range<u64>,
    use_log_scale: bool,
}

struct Config {
    path_to_png: &'static str,
    heading: &'static str,
    x_axis: Axis,
    y_axis: Axis,
}

struct Graph<'a, CT: CoordTranslate> {
    graph_root: DrawingArea<SVGBackend<'a>, Shift>,
    shared_chart_state: ChartState<Arc<CT>>,
}

impl Graph<'static, Cartesian2d<RangedCoordu64, LogCoord<u64>>> {
    pub(crate) fn new(config: Config) -> Self {
        // Creates drawing area with the SVG backend - this is the basis for the graph
        let root = SVGBackend::new(config.path_to_png, (1920, 1080)).into_drawing_area();

        // Sets the background
        root.fill(&WHITE)
            .expect("Should have set background colour");

        let heading_style = ("sans-serif", 60).into_font();

        let mut chart_ctx = ChartBuilder::on(&root)
            .caption(config.heading, heading_style)
            .margin(50)
            .x_label_area_size(30)
            .y_label_area_size(30)
            .build_cartesian_2d(config.x_axis.range, config.y_axis.range.log_scale())
            .expect("Should have created coordinate axes");

        chart_ctx
            .configure_mesh()
            .x_desc(config.x_axis.label)
            .y_desc(config.y_axis.label)
            .draw()
            .expect("Should have drawn axis labels");

        let shared_chart_state = chart_ctx.into_shared_chart_state();

        Self {
            graph_root: root,
            shared_chart_state,
        }
    }

    pub(crate) fn add_series(&self, name: &str, colour: RGBColor, data: Vec<(u64, u64)>) {
        let line_style = ShapeStyle {
            color: colour.to_rgba(),
            filled: false,
            stroke_width: 2,
        };

        let line_series = LineSeries::new(data, line_style);

        self.shared_chart_state
            .clone()
            .restore(&self.graph_root)
            .draw_series(line_series.point_size(2))
            .expect("Should have drawn series")
            .label(name)
            .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 15, y)], line_style));
    }

    pub(crate) fn finalise(&self) {
        self.shared_chart_state
            .clone()
            .restore(&self.graph_root)
            .configure_series_labels()
            .background_style(WHITE.mix(0.8))
            .border_style(BLACK)
            .label_font(("sans-serif", 20))
            .draw()
            .expect("Should have drawn series labels");

        println!("Saving graph...");
        self.graph_root.present().expect("Should have saved graph");
    }
}

//
//     let event_types_ordered_for_efficiency = vec![
//         (EventType::FinalitySignature, ORANGE),
//         (EventType::DeployAccepted, MAGENTA),
//         (EventType::DeployProcessed, BLUE),
//         (EventType::BlockAdded, BLACK),
//         (EventType::Step, RED),
//         (EventType::DeployExpired, GREEN),
//         (EventType::Fault, GREY),
//     ];
//
//     for (event_type, line_colour) in event_types_ordered_for_efficiency {
//         println!("Charting {}...", event_type);
//         let event_latencies_for_type =
//             extract_latencies_by_type(&event_type, &mut combined_event_latencies);
//
//         let data = event_latencies_for_type
//             .iter()
//             .map(|event_latency| {
//                 let time_since_start = event_latency
//                     .received_from_source
//                     .duration_since(start_instant)
//                     .as_secs();
//                 let latency = event_latency.latency_millis as u64;
//                 (time_since_start, latency)
//             })
//             .collect::<Vec<(u64, u64)>>();
