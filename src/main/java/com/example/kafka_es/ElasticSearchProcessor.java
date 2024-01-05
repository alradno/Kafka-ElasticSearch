package com.example.kafka_es;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;


import java.io.IOException;
import java.util.*;

public class ElasticSearchProcessor implements Processor<String, List<List<String>>, String, String> {

    private final RestHighLevelClient client;
    private final String indexName;
    private ProcessorContext <String, String> context;

    public ElasticSearchProcessor(RestHighLevelClient client, String indexName) {
        this.client = client;
        this.indexName = indexName;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
    }
    @Override
    public void process(Record<String, List<List<String>>> record) {
        try {
            List<List<String>> innerList = record.value();
            // Crear un mapa con los datos para agregar a ElasticSearch
            Map<String, Object> map = new HashMap<>();

            map.put("bssmap", innerList.get(0));
            map.put("causainterna", innerList.get(1));
            map.put("ranap", innerList.get(2));
            map.put("nrn", innerList.get(3));
            map.put("operadores", innerList.get(4));
            map.put("test", innerList.get(5));

            for(int i = 6; i < innerList.size(); i++){
                map.put("Campo" + i, innerList.get(i));
            }

            // Crear una solicitud de índice para enviar los datos a Elasticsearch
            IndexRequest request = new IndexRequest(indexName).source(map);

            // Envía la solicitud a Elasticsearch y verifica si hubo errores
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("Documento creado en Elasticsearch: " + response.getId());
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("Documento actualizado en Elasticsearch: " + response.getId());
            }


            // Envía el registro transformado al siguiente nodo
            context.forward(record.withValue("OK"));
            context.commit();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

        Processor.super.close();
    }

}
