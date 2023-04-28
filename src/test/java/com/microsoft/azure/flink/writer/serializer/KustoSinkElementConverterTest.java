package com.microsoft.azure.flink.writer.serializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class KustoSinkElementConverterTest {

  @Test
  void elementConverterWillComplainASerializationSchemaIsNotSetIfBuildIsCalledWithoutIt() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> KustoSinkElementConverter.<String>builder().build())
        .withMessageContaining(
            "No SerializationSchema was supplied to the KinesisFirehoseSink builder.");
  }

  // @Test
  // void elementConverterUsesProvidedSchemaToSerializeRecord() {
  // ElementConverter<String, KustoRow> elementConverter = KustoSinkElementConverter
  // .<String>builder().setSerializationSchema(new SimpleStringSchema()).build();
  // String testString = "{many hands make light work;";
  // KustoRow serializedRecord = elementConverter.apply(testString, null);
  // byte[] serializedString = (new JsonSerializationSchema()).serialize(testString);
  // assertNotNull(serializedString);
  // // assertThat("serializedRecord.data()").isEqualTo(SdkBytes.fromByteArray(serializedString));
  // }
}
