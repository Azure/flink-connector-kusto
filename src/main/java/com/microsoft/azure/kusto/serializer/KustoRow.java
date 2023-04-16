package com.microsoft.azure.kusto.serializer;

public class KustoRow {
    private final String blobName;

    private final String[] values;

    public KustoRow(String blobName, String[] values) {
        this.blobName = blobName;
        this.values = values;
    }
}
