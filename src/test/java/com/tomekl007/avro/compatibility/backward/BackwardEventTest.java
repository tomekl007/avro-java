package com.tomekl007.avro.compatibility.backward;

import com.tomekl007.avro.compatibility.backward.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BackwardEventTest {
    @Test
    public void givenEvent_whenProducerIsAddingNewOptionalField_thenConsumerShouldReadMessageInBackwardCompatibleWay() throws IOException {
        //given
        ProducedEvent producedEvent = ProducedEvent.newBuilder().setDescription("desc").setId("id").build();
        String path = "events_test_backward.avro";

        //when
        DatumWriter<ProducedEvent> eventDatumWriter = new SpecificDatumWriter<>(ProducedEvent.class);
        DataFileWriter<ProducedEvent> dataFileWriter = new DataFileWriter<>(eventDatumWriter);
        dataFileWriter.create(producedEvent.getSchema(), new File(path));
        dataFileWriter.append(producedEvent);
        dataFileWriter.close();

        //and when consumer read in backward compatible manner
        DatumReader<ConsumerEvent> eventDatumReader = new SpecificDatumReader<>(ConsumerEvent.class);
        DataFileReader<ConsumerEvent> dataFileReader = new DataFileReader<>(new File(path), eventDatumReader);
        List<ConsumerEvent> result = new LinkedList<>();
        while (dataFileReader.hasNext()) {
            result.add(dataFileReader.next());
        }

        //then
        assertThat(result.get(0).getId().toString()).isEqualTo(producedEvent.getId().toString());
    }
}
