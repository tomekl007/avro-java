package com.tomekl007.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class EventTest {
    @Test
    public void shouldCreateUserFromAvroSchema() throws IOException {
        //given
        Event e = Event.newBuilder()
                .setName("Michael")
                .setAge(12)
                .setFavouriteMovies(Arrays.asList("batman", "lord of the rings"))
                .setPayment(24.43)
                .setHasKids(true)
                .build();


        //when
        DatumWriter<Event> eventDatumWriter = new SpecificDatumWriter<>(Event.class);
        DataFileWriter<Event> dataFileWriter = new DataFileWriter<>(eventDatumWriter);
        dataFileWriter.create(e.getSchema(), new File("event.avro"));
        dataFileWriter.append(e);
        dataFileWriter.close();

        DatumReader<Event> eventDatumReader = new SpecificDatumReader<>(Event.class);
        DataFileReader<Event> dataFileReader = new DataFileReader<>(new File("event.avro"), eventDatumReader);
        List<Event> result = new LinkedList<>();
        while (dataFileReader.hasNext()) {
            result.add(dataFileReader.next());
        }

        //then
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getAge()).isEqualTo(12);
        assertThat(result.get(0).getName().toString()).isEqualTo("Michael");
        assertThat(result.get(0).getPayment()).isEqualTo(24.43);
        assertThat(result.get(0).getHasKids()).isEqualTo(true);
        assertThat(result.get(0).getFavouriteMovies()
                .stream()
                .map(CharSequence::toString)
                .collect(Collectors.toList())
        ).contains("batman", "lord of the rings");

    }
}
