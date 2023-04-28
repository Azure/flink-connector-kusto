// package com.microsoft.azure.flink.writer.serializer;
//
// import java.io.Serializable;
// import java.io.StringWriter;
//
// import org.apache.flink.annotation.PublicEvolving;
// import org.apache.flink.api.common.serialization.SerializationSchema;
//
// import com.microsoft.azure.flink.config.KustoWriteOptions;
// import com.microsoft.azure.flink.writer.context.KustoSinkContext;
// import com.opencsv.bean.StatefulBeanToCsv;
// import com.opencsv.bean.StatefulBeanToCsvBuilder;
//
// @PublicEvolving
// public class KustoSerializationSchema<IN> implements SerializationSchema<IN>{
//
// private final StatefulBeanToCsv<IN> beanToCsv = new StatefulBeanToCsvBuilder<IN>(new
// StringWriter()).build();
// @Override
// public void open(InitializationContext context) throws Exception {
// SerializationSchema.super.open(context);
// }
//
// @Override
// public byte[] serialize(IN in) {
// beanToCsv.;
// }
// }
