package edu.usfca.cs.mr.climatechart.draw;

import java.awt.Color;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.labels.StandardCategoryToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

/**
 * 
 * 
 *
 */
public class ClimateChartReport extends ApplicationFrame {

    /**
     * Creates a new demo instance.
     *
     * @param title  the frame title.
     */
    public ClimateChartReport(final String title) {

        super(title);

        final CategoryDataset dataset1 = createDataset1();

        // create the chart...
        final JFreeChart chart = ChartFactory.createBarChart("Santa Barbara Climate Chart", // chart title
                                                             "Category", // domain axis label
                                                             "Temperature (Celsius)", // range axis label
                                                             dataset1, // data
                                                             PlotOrientation.VERTICAL,
                                                             true, // include legend
                                                             true, // tooltips?
                                                             false // URL generator?  Not required...
        );

        // NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
        chart.setBackgroundPaint(Color.white);
        //        chart.getLegend().setAnchor(Legend.SOUTH);

        // get a reference to the plot for further customisation...
        final CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(new Color(0x00, 0x00, 0x00));
        plot.setDomainAxisLocation(AxisLocation.BOTTOM_OR_RIGHT);

        final CategoryDataset dataset2 = createDataset2();
        plot.setDataset(1, dataset2);
        plot.mapDatasetToRangeAxis(1, 1);

        final CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions(CategoryLabelPositions.DOWN_45);
        final ValueAxis axis2 = new NumberAxis("Precipitation(mm)");
        plot.setRangeAxis(1, axis2);

        final LineAndShapeRenderer renderer2 = new LineAndShapeRenderer();
        renderer2.setToolTipGenerator(new StandardCategoryToolTipGenerator());
        plot.setRenderer(1, renderer2);
        plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);
        // OPTIONAL CUSTOMISATION COMPLETED.

        // add the chart to a panel...
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(1200, 1000));
        setContentPane(chartPanel);

    }

    /*
     * Creates a sample dataset.
     *
     * @return  The dataset.
     */
    private CategoryDataset createDataset1() {

        // row keys...
        final String series1 = "Min Air Temp.";
        final String series2 = "Average Air Temp.";
        final String series3 = "Max Air Temp.";

        // column keys...
        final String category1 = "January";
        final String category2 = "February";
        final String category3 = "March";
        final String category4 = "April";
        final String category5 = "May";
        final String category6 = "June";
        final String category7 = "July";
        final String category8 = "August";
        final String category9 = "September";
        final String category10 = "October";
        final String category11 = "November";
        final String category12 = "December";

        // create the dataset...
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        dataset.addValue(3.7, series1, category1);
        dataset.addValue(0.3, series1, category2);
        dataset.addValue(3.2, series1, category3);
        dataset.addValue(5.7, series1, category4);
        dataset.addValue(6.9, series1, category5);
        dataset.addValue(8.9, series1, category6);
        dataset.addValue(12, series1, category7);
        dataset.addValue(12.5, series1, category8);
        dataset.addValue(9.9, series1, category9);
        dataset.addValue(8.1, series1, category10);
        dataset.addValue(3.5, series1, category11);
        dataset.addValue(2.6, series1, category12);

        dataset.addValue(26, series2, category1);
        dataset.addValue(21, series2, category2);
        dataset.addValue(22.6, series2, category3);
        dataset.addValue(25.9, series2, category4);
        dataset.addValue(21.2, series2, category5);
        dataset.addValue(27.9, series2, category6);
        dataset.addValue(38.1, series2, category7);
        dataset.addValue(29.2, series2, category8);
        dataset.addValue(24.5, series2, category9);
        dataset.addValue(29.7, series2, category10);
        dataset.addValue(26.5, series2, category11);
        dataset.addValue(21.7, series2, category12);

        dataset.addValue(13.3, series3, category1);
        dataset.addValue(11.58, series3, category2);
        dataset.addValue(12.44, series3, category3);
        dataset.addValue(14.02, series3, category4);
        dataset.addValue(14.65, series3, category5);
        dataset.addValue(16.55, series3, category6);
        dataset.addValue(19.81, series3, category7);
        dataset.addValue(20.07, series3, category8);
        dataset.addValue(17.84, series3, category9);
        dataset.addValue(17.19, series3, category10);
        dataset.addValue(14.97, series3, category11);
        dataset.addValue(12.48, series3, category12);

        return dataset;

    }

    /**
     * Creates a sample dataset.
     *
     * @return  The dataset.
     */
    private CategoryDataset createDataset2() {

        // row keys...
        final String series1 = "Precipitation";

        // column keys...
        final String category1 = "January";
        final String category2 = "February";
        final String category3 = "March";
        final String category4 = "April";
        final String category5 = "May";
        final String category6 = "June";
        final String category7 = "July";
        final String category8 = "August";
        final String category9 = "September";
        final String category10 = "October";
        final String category11 = "November";
        final String category12 = "December";

        // create the dataset...
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        dataset.addValue(0.0052, series1, category1);
        dataset.addValue(0.00009, series1, category2);
        dataset.addValue(0.01585, series1, category3);
        dataset.addValue(0.00014, series1, category4);
        dataset.addValue(0.00041, series1, category5);
        dataset.addValue(0.00007, series1, category6);
        dataset.addValue(0.0, series1, category7);
        dataset.addValue(0.0, series1, category8);
        dataset.addValue(0.00002, series1, category9);
        dataset.addValue(0.00149, series1, category10);
        dataset.addValue(0.00638, series1, category11);
        dataset.addValue(0.00147, series1, category12);

        return dataset;

    }

    /**
     * Starting point for the demonstration application.
     *
     * @param args  ignored.
     */
    public static void main(final String[] args) {

        final ClimateChartReport demo = new ClimateChartReport("Santa Barbara Climate Chart");
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);

    }

}
