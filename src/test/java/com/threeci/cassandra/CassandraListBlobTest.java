package com.threeci.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CassandraListBlobTest {

  private static final String CONTACT_POINTS = "127.0.0.1";
  private static final int PORT = 9042;
  private static final String ID_VALUE = "1234567890";
  private static final String TABLE_NAME = "list";
  private static final String ID = "id";
  private static final String VALUES = "values";
  private static final String KEYSPACE = "mykeyspace";
  private Cluster cluster;
  private Session session;
  private static final Integer ONE = 1;
  private static final Integer TWO = 2;

  @Before
  public void init() throws Exception {
    cluster = Cluster.builder()
        .addContactPoint(CONTACT_POINTS)
        .withPort(PORT)
        .build();

    session = cluster.connect();

    createSchema();
  }


  private void createSchema() throws IOException {

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    session.execute("CREATE TABLE IF NOT EXISTS mykeyspace.list(\n"
                    + "\tid text,\n"
                    + "\tvalues list<blob>,\n"
                    + "\tprimary key(id)\n"
                    + ");");
  }


  @Test
  public void shouldDeserializeBytesFromCassandra() {
    List<ByteBuffer> list = insertElementsQueryAndReturnList();

    Integer actualOne = SerializationUtils.deserialize(Bytes.getArray(list.get(0)));
    assertThat(actualOne, is(ONE));
  }

  @Test
  public void shouldGetSameList() {
    List<ByteBuffer> list = insertElementsQueryAndReturnList();
    List<Integer> expected = ImmutableList.of(ONE, TWO);
    List<Integer> actualList = new ArrayList<>();

    list.forEach(i -> {
      byte[] fixedBytes = Bytes.getArray(i);
      actualList.add(SerializationUtils.deserialize(fixedBytes));
    });

    assertThat(actualList, is(expected));
  }


  private List<ByteBuffer> insertElementsQueryAndReturnList() {

    ByteBuffer wrapSerializedOne = ByteBuffer.wrap(SerializationUtils.serialize(ONE));
    ByteBuffer wrapSerializedTwo = ByteBuffer.wrap(SerializationUtils.serialize(TWO));

    Batch batch = QueryBuilder.batch();

    // insert one
    batch.add(
        QueryBuilder.update(KEYSPACE, TABLE_NAME)
            .where(QueryBuilder.eq(ID, ID_VALUE))
            .with(QueryBuilder.append(VALUES, wrapSerializedOne)));

    // insert two
    batch.add(
        QueryBuilder.update(KEYSPACE, TABLE_NAME)
            .where(QueryBuilder.eq(ID, ID_VALUE))
            .with(QueryBuilder.append(VALUES, wrapSerializedTwo)));

    session.execute(batch);

    Select select = QueryBuilder.select(VALUES).from(KEYSPACE, TABLE_NAME);
    select.where(QueryBuilder.eq(ID, ID_VALUE));

    ResultSet resultSet = session.execute(select);
    List<ByteBuffer> list = queryForObject(resultSet, List.class);
    assertThat(list.size(), is(2));
    return list;
  }

  private <T> T queryForObject(ResultSet resultSet, Class<T> requiredType) {
    if (resultSet == null) {
      return null;
    }
    Row row = resultSet.one();
    if (row == null) {
      return null;
    }
    return (T) firstColumnToObject(row);
  }

  private Object firstColumnToObject(Row row) {
    ColumnDefinitions cols = row.getColumnDefinitions();
    if (cols.size() == 0) {
      return null;
    }
    return cols.getType(0).deserialize(row.getBytesUnsafe(0), ProtocolVersion.NEWEST_SUPPORTED);
  }

  @After
  public void cleanup() {
    session.execute("DROP KEYSPACE " + KEYSPACE);
    session.close();
    cluster.close();
  }


  private static class SerializationUtils {

    static byte[] serialize(Object state) {
      ObjectOutputStream oos = null;
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(512);
        oos = new ObjectOutputStream(bos);
        oos.writeObject(state);
        oos.flush();
        return bos.toByteArray();
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      } finally {
        if (oos != null) {
          try {
            oos.close();
          } catch (IOException e) {
            // eat it
          }
        }
      }
    }

    public static <T> T deserialize(byte[] byteArray) {
      ObjectInputStream oip = null;
      try {
        oip = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        @SuppressWarnings("unchecked")
        T result = (T) oip.readObject();
        return result;
      } catch (IOException | ClassNotFoundException e) {
        throw new IllegalArgumentException(e);
      } finally {
        if (oip != null) {
          try {
            oip.close();
          } catch (IOException e) {
            // eat it
          }
        }
      }
    }
  }
}
