package com.streams.serde;

import com.streams.model.Player;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class JsonSerDesTest {


    @Test
    void testClassSerializationAndDeserialization() {
        // given
        Player player = new Player();
        player.setId(2L);
        player.setName("Dawid");

        JsonSerDes<Player> playerJsonSerDes = new JsonSerDes<>(Player.class);
        Serializer<Player> playerSerializer = playerJsonSerDes.serializer();
        Deserializer<Player> playerDeserializer = playerJsonSerDes.deserializer();

        // when
        byte[] serializedPlayer = playerSerializer.serialize("whatever-topic", player);
        Player deserializedPlayer = playerDeserializer.deserialize("whatever-topic", serializedPlayer);

        // then
        assertEquals(player.getId(), deserializedPlayer.getId());
        assertEquals(player.getName(), deserializedPlayer.getName());
    }

    @Test
    void testCaptureJsonWithSpy() {
        // given
        Player player = new Player();
        player.setId(2L);
        player.setName("Dawid");

        JsonDeserializer<Player> deserializer = new JsonDeserializer<>(Player.class);
        JsonDeserializer<Player> deserializerSpy = spy(deserializer);

        ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);

        // when
        byte[] serializedBytes = "{\"id\":2,\"name\":\"Dawid\"}".getBytes();
        Player deserialized = deserializerSpy.deserialize("topic", serializedBytes);

        // then
        assertEquals(player.getId(), deserialized.getId());
        assertEquals(player.getName(), deserialized.getName());

        // capture argument
        verify(deserializerSpy).deserialize(anyString(), bytesCaptor.capture());
        String capturedJson = new String(bytesCaptor.getValue());
        System.out.println("Captured JSON: " + capturedJson);

        assertTrue(capturedJson.contains("\"id\":2"));
        assertTrue(capturedJson.contains("\"name\":\"Dawid\""));
    }
}