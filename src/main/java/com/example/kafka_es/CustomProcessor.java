package com.example.kafka_es;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import static com.example.kafka_es.Kafka_Elastic_Main.PROPS;

public class CustomProcessor implements Processor<String, String, String, List<List<String>>> {
    private ProcessorContext <String, List<List<String>>> context;
    private List<String> urls;
    private HttpClient httpClient;
    private TypeReference<List<String>> typeRef;
    private ObjectMapper mapper;
    private HistogramDisplay histogramDisplay;

    //Para tareas de ejecución asincrónica
    private ExecutorService executorService;
    private static final int THREAD_COUNT = 10;  // Ajustar según recursos y necesidades
    private static final long FUTURE_TIMEOUT = 10; // Ajustar según necesidades

    @Override
    public void init(ProcessorContext <String, List<List<String>>> context) {
        this.context = context;
        String url_bssmap = "http://" + PROPS.getProperty("IP") + ":" + PROPS.getProperty("PORT") + PROPS.getProperty("url_bssMap");
        String url_causaInterna = "http://" + PROPS.getProperty("IP") + ":" +PROPS.getProperty("PORT") + PROPS.getProperty("url_causaInterna");
        String url_ranap = "http://" + PROPS.getProperty("IP") + ":" +PROPS.getProperty("PORT") + PROPS.getProperty("url_ranap");
        String url_nrn = "http://" + PROPS.getProperty("IP") + ":" +PROPS.getProperty("PORT") + PROPS.getProperty("url_nrn");
        String url_operadores = "http://" + PROPS.getProperty("IP") + ":" +PROPS.getProperty("PORT") + PROPS.getProperty("url_operadores");
        String url_test = "http://" + PROPS.getProperty("IP") + ":" +PROPS.getProperty("PORT") + PROPS.getProperty("url_test");

        this.urls = List.of(url_bssmap, url_causaInterna, url_ranap, url_nrn, url_operadores, url_test);

        this.mapper = new ObjectMapper();
        this.typeRef = new TypeReference<>() {};
        this.histogramDisplay = new HistogramDisplay();
        this.executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        this.httpClient = HttpClientBuilder.create().build();
    }

    @Override
    public void process(Record<String, String> record) {

        String[] fields = record.value().split(";");
        List<List<String>> newFields = fetchAndProcessFields(fields);

        context.forward(record.withValue(newFields));
        context.commit();
    }

    private List<List<String>> fetchAndProcessFields(String[] fields) {
        List<List<String>> newFields = new ArrayList<>();
        if (fields.length >= 6) {

            long startTime = System.nanoTime();
            //Hace las 6 peticiones http de forma asincrónica
            List<CompletableFuture<List<String>>> futures = submitHttpRequests(fields);
            recordExecutionTime(startTime);

            // Esperamos que todos los CompletableFuture se completen
            CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            try {
                combinedFuture.get(FUTURE_TIMEOUT, TimeUnit.SECONDS);

                // Procesamos los resultados de cada CompletableFuture
                for (CompletableFuture<List<String>> future : futures) {
                    newFields.add(future.join());
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }

            //recordExecutionTime(startTime);

            for (int i = 6; i < fields.length; i++) {
                newFields.add(List.of(fields[i]));
            }
        } else {
            System.out.println("Error: No se pudo procesar el registro, falta alguno de los 6 campos iniciales");
        }
        return newFields;
    }

    private List<CompletableFuture<List<String>>> submitHttpRequests(String[] fields) {
        List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String url = urls.get(i) + fields[i];
            CompletableFuture<List<String>> future = CompletableFuture.supplyAsync(() -> makeHttpRequest(url), executorService);
            futures.add(future);
        }
        return futures;
    }

    private void recordExecutionTime(long startTime) {
        try {
            long endTime = System.nanoTime();
            long executionTime = endTime - startTime;
            histogramDisplay.addExecutionTime((double) executionTime / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> makeHttpRequest(String url) {
        //System.out.println("Inicio de la petición a las " + System.nanoTime());
        HttpGet httpGet = new HttpGet(url);
        try {
            HttpResponse response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                HttpEntity entity = response.getEntity();
                //Formato JSON
                String responseString = EntityUtils.toString(entity);
                //System.out.println("Final de la petición a las " + System.nanoTime());
                //Convierte responseString (JSON) al tipo de typeRef (List<String>)
                return this.mapper.readValue(responseString, this.typeRef);
            } else {
                System.out.println("Error: Received HTTP status code " + statusCode + " for URL: " + url);
                return Collections.emptyList();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();  // Devolver una lista vacía en caso de error
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
