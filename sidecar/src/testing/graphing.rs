use std::{ops::Range, sync::Arc};

use derive_new::new;
use plotters::{
    chart::ChartState,
    coord::{types::RangedCoordu64, Shift},
    prelude::*,
};

type Point = (u64, u64);

#[derive(Clone, new)]
pub(crate) struct Axis {
    label: String,
    range: Range<u64>,
    #[allow(unused)]
    use_log_scale: bool,
}

#[derive(Clone, new)]
pub(crate) struct Series {
    name: String,
    data: Vec<Point>,
    colour: RGBColor,
}

#[derive(Clone, new)]
pub(crate) struct Config {
    path_to_png: String,
    heading: String,
    x_axis: Axis,
    y_axis: Axis,
}

pub(crate) struct Graph<'a, CT: CoordTranslate> {
    graph_root: DrawingArea<SVGBackend<'a>, Shift>,
    shared_chart_state: ChartState<Arc<CT>>,
    num_series: u8,
}

impl<'a> Graph<'a, Cartesian2d<RangedCoordu64, LogCoord<u64>>> {
    pub(crate) fn new(config: &'a Config) -> Self {
        // Creates drawing area with the SVG backend - this is the basis for the graph
        let root = SVGBackend::new(&config.path_to_png, (1920, 1080)).into_drawing_area();

        // Sets the background
        root.fill(&WHITE)
            .expect("Should have set background colour");

        let heading_style = ("sans-serif", 60).into_font();

        let mut chart_ctx = ChartBuilder::on(&root)
            .caption(&config.heading, heading_style)
            .x_label_area_size(30)
            .y_label_area_size(30)
            .margin((1).percent())
            .build_cartesian_2d(
                config.x_axis.range.clone(),
                config.y_axis.range.clone().log_scale(),
            )
            .expect("Should have created coordinate axes");

        chart_ctx
            .configure_mesh()
            .x_desc(&config.x_axis.label)
            .y_desc(&config.y_axis.label)
            .draw()
            .expect("Should have drawn axis labels");

        let shared_chart_state = chart_ctx.into_shared_chart_state();

        Self {
            graph_root: root,
            shared_chart_state,
            num_series: 0,
        }
    }

    pub(crate) fn add_all_series(&mut self, data: Vec<Series>) {
        let mut chart_state = self.shared_chart_state.clone().restore(&self.graph_root);

        for Series { name, data, colour } in data {
            let line_style = ShapeStyle {
                color: colour.to_rgba(),
                filled: false,
                stroke_width: 2,
            };

            let line = LineSeries::new(data, line_style);

            let mut point_size = 2;
            if name == "Step" {
                point_size = 4
            }

            chart_state
                .draw_series(line.point_size(point_size))
                .expect("Should have drawn series")
                .label(name)
                .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 15, y)], line_style));
        }

        chart_state
            .configure_series_labels()
            .position(SeriesLabelPosition::UpperRight)
            .margin(5)
            .background_style(WHITE.mix(0.8))
            .border_style(BLACK)
            .label_font(("sans-serif", 20))
            .draw()
            .expect("Should have drawn series labels");

        self.num_series += 1;
    }

    pub(crate) fn finalise(&self) {
        println!("Saving graph...");
        self.graph_root.present().expect("Should have saved graph");
    }
}
