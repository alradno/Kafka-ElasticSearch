package com.example.kafka_es;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.renderer.category.BarRenderer;

import java.awt.*;
import javax.swing.*;
import java.util.*;

public class HistogramDisplay extends JFrame {
    private final DefaultCategoryDataset dataset;
    private final ArrayList<Double> executionTimes;
    private final TreeMap<String, Double> densidades;
    private final TreeMap<String, Long> binCounts;

    private ChartPanel chartPanel;

    //Establece el rango de cada intervalo del histograma
    private static final double BIN_WIDTH = 0.5;
    private final JLabel percentile95Label;
    private final JLabel percentile98Label;

    public HistogramDisplay() {
        dataset = new DefaultCategoryDataset();
        executionTimes = new ArrayList<>();
        densidades = new TreeMap<>(Comparator.comparingDouble(key -> Double.parseDouble(key.split("-")[0].replace(',', '.'))));
        binCounts = new TreeMap<>(Comparator.comparingDouble(key -> Double.parseDouble(key.split("-")[0].replace(',', '.'))));
        JFreeChart histogram = createHistogram();
        chartPanel = new ChartPanel(histogram);

        setTitle("Histograma de Tiempos de Ejecución");
        setSize(1280, 720);
        setLocationRelativeTo(null);
        setVisible(true);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        add(chartPanel);

        JPanel percentilePanel = new JPanel(new BorderLayout());
        percentile95Label = new JLabel("");
        percentile98Label = new JLabel("");
        percentilePanel.add(percentile95Label, BorderLayout.WEST);
        percentilePanel.add(percentile98Label, BorderLayout.EAST);
        // Añadir un borde al panel para que parezca un recuadro
        percentilePanel.setBorder(BorderFactory.createLineBorder(Color.BLACK, 1));

        getContentPane().add(percentilePanel, BorderLayout.SOUTH);

    }

    private JFreeChart createHistogram() {
        JFreeChart chart = ChartFactory.createBarChart(
                //Titulo del histograma
                "Histograma de Tiempos de Ejecución",
                //Eiqueta eje x
                "Tiempo de Ejecución (μs)",
                //Etiqueta eje y
                "Frecuencia",
                dataset,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        //Fija el rango del eje "y" de 0 a 1 (para la frecuencia)
        chart.getCategoryPlot().getRangeAxis().setRange(0.0, 1.0);

        //Establece el tamaño de los intervalos del eje "y" a 0.05
        NumberAxis yAxis = (NumberAxis) chart.getCategoryPlot().getRangeAxis();
        yAxis.setTickUnit(new NumberTickUnit(0.1));

        CategoryAxis xAxis = chart.getCategoryPlot().getDomainAxis();
        xAxis.setTickLabelFont(new Font("SansSerif", Font.PLAIN, 12)); // Incrementamos el tamaño de la fuente
        xAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45); // Orientación de las etiquetas a 45 grados

        BarRenderer renderer = (BarRenderer) chart.getCategoryPlot().getRenderer();
        renderer.setMaximumBarWidth(0.05);  // Incrementamos el ancho de las barras

        // Configura el render para mostrar etiquetas
        renderer.setDefaultItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setDefaultItemLabelsVisible(true);
        renderer.setDefaultItemLabelFont(new Font("SansSerif", Font.PLAIN, 10));

        return chart;
    }

    private String getBinRangeForValue(double value) {
        for (String key : binCounts.keySet()) {
            String[] range = key.split("-");
            double start = Double.parseDouble(range[0].replace(',', '.').trim());
            double end = Double.parseDouble(range[1].replace(',', '.').trim());

            if (value >= start && value < end) {
                return key;
            }
        }

        // Si no se encuentra un intervalo existente, crea uno nuevo
        double binStart = Math.floor(value);
        double binEnd = binStart + BIN_WIDTH;

        return String.format("%.2f-%.2f", binStart, binEnd);
    }

    public void addExecutionTime(double time) {
        SwingUtilities.invokeLater(() -> {
            executionTimes.add(time);

            // Obtener el rango para el valor actual
            String binRange = getBinRangeForValue(time);

            // Se inserta la clave con su conteo actualizado
            Long currentCount = binCounts.getOrDefault(binRange, 0L) + 1;
            binCounts.put(binRange, currentCount);

            // Recalcular las densidades y actualizar el dataset
            updateDensitiesAndDataset();

            // Calcular y actualizar los percentiles
            updatePercentiles();

            // Actualizar el gráfico
            chartPanel.repaint();
        });
    }

    private void updateDensitiesAndDataset() {
        dataset.clear();
        densidades.clear();

        for (Map.Entry<String, Long> entry : binCounts.entrySet()) {
            String range = entry.getKey();
            Long count = entry.getValue();
            double density = (double) count / executionTimes.size();
            densidades.put(range, density); // TreeMap mantiene el orden
            dataset.setValue(density, "Frecuencia", range);
        }
    }

    private void updatePercentiles() {
        double percentile95 = approximatePercentileFromBins(95);
        double percentile98 = approximatePercentileFromBins(98);

        percentile95Label.setText("Percentil 95: " + String.format("%.2f", percentile95));
        percentile98Label.setText("Percentil 98: " + String.format("%.2f", percentile98));
    }

    private double approximatePercentileFromBins(double percentile) {
        double acumulado = 0.0;
        String foundPercentile = "";
        double percentileThreshold = percentile / 100.0;

        for (Map.Entry<String, Double> entry : densidades.entrySet()) {
            acumulado += entry.getValue();
            if (acumulado >= percentileThreshold) {
                foundPercentile = entry.getKey();
                break;
            }
        }

        if (!foundPercentile.isEmpty()) {
            String[] range = foundPercentile.split("-");
            double start = Double.parseDouble(range[0].replace(',', '.').trim());
            double end = Double.parseDouble(range[1].replace(',', '.').trim());
            // Devuelve el punto medio
            return (start + end) / 2.0;
        }

        return 0;
    }

}
