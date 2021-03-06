/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.csv.javadsl.CsvParsing;
import akka.stream.alpakka.csv.javadsl.CsvToMap;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
// #header-line

// #header-line

// #column-names

// #column-names

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class CsvToMapTest {
  private static ActorSystem system;
  private static Materializer materializer;

  public void documentation() {
    // #flow-type
    // keep values as ByteString
    Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow1 = CsvToMap.toMap();

    Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow2 =
        CsvToMap.toMap(StandardCharsets.UTF_8);

    Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow3 =
        CsvToMap.withHeaders("column1", "column2", "column3");

    // values as String (decode ByteString)
    Flow<Collection<ByteString>, Map<String, String>, ?> flow4 =
        CsvToMap.toMapAsStrings(StandardCharsets.UTF_8);

    Flow<Collection<ByteString>, Map<String, String>, ?> flow5 =
        CsvToMap.withHeadersAsStrings(StandardCharsets.UTF_8, "column1", "column2", "column3");
    // #flow-type
  }

  @Test
  public void parsedLineShouldBecomeMapKeys() throws Exception {
    CompletionStage<Map<String, ByteString>> completionStage =
        // #header-line
        // values as ByteString
        Source.single(ByteString.fromString("eins,zwei,drei\n1,2,3"))
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.toMap(StandardCharsets.UTF_8))
            .runWith(Sink.head(), materializer);
    // #header-line
    Map<String, ByteString> map = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #header-line

    assertThat(map.get("eins"), equalTo(ByteString.fromString("1")));
    assertThat(map.get("zwei"), equalTo(ByteString.fromString("2")));
    assertThat(map.get("drei"), equalTo(ByteString.fromString("3")));
    // #header-line
  }

  @Test
  public void parsedLineShouldBecomeMapKeysAndStringValues() throws Exception {
    CompletionStage<Map<String, String>> completionStage =
        // #header-line

        // values as String
        Source.single(ByteString.fromString("eins,zwei,drei\n1,2,3"))
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
            .runWith(Sink.head(), materializer);
    // #header-line
    Map<String, String> map = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #header-line

    assertThat(map.get("eins"), equalTo("1"));
    assertThat(map.get("zwei"), equalTo("2"));
    assertThat(map.get("drei"), equalTo("3"));
    // #header-line
  }

  @Test
  public void givenHeadersShouldBecomeMapKeys() throws Exception {
    CompletionStage<Map<String, ByteString>> completionStage =
        // #column-names
        // values as ByteString
        Source.single(ByteString.fromString("1,2,3"))
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.withHeaders("eins", "zwei", "drei"))
            .runWith(Sink.head(), materializer);
    // #column-names
    Map<String, ByteString> map = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #column-names

    assertThat(map.get("eins"), equalTo(ByteString.fromString("1")));
    assertThat(map.get("zwei"), equalTo(ByteString.fromString("2")));
    assertThat(map.get("drei"), equalTo(ByteString.fromString("3")));
    // #column-names
  }

  @Test
  public void givenHeadersShouldBecomeMapKeysAndStringValues() throws Exception {
    CompletionStage<Map<String, String>> completionStage =
        // #column-names

        // values as String
        Source.single(ByteString.fromString("1,2,3"))
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.withHeadersAsStrings(StandardCharsets.UTF_8, "eins", "zwei", "drei"))
            .runWith(Sink.head(), materializer);
    // #column-names
    Map<String, String> map = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #column-names

    assertThat(map.get("eins"), equalTo("1"));
    assertThat(map.get("zwei"), equalTo("2"));
    assertThat(map.get("drei"), equalTo("3"));
    // #column-names
  }

  @BeforeClass
  public static void setup() throws Exception {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void teardown() throws Exception {
    TestKit.shutdownActorSystem(system);
  }
}
