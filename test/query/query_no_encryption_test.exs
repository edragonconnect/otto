defmodule OttoTest.QueryNoEncryptionTest do
  use ExUnit.Case

  use Otto.Table,
    instance: Instance1,
    table: "query_no_encryption",
    primary: [:pk1, :pk2]

  use Ecto.Schema
  import Ecto.Changeset
  import Otto.Query
  alias OttoTest.QueryNoEncryptionTest

  schema "test_query" do
    field(:pk1, :string)
    field(:pk2, :integer)

    field(:attr1, :string)
    field(:attr2, :integer)
    field(:attr3, :string)
    field(:attr4, :map)
    field(:attr5, :float)
    field(:attr6, :boolean)
  end

  def changeset(query_test, attrs) do
    query_test
    |> cast(attrs, __MODULE__.__schema__(:fields))
  end

  test "test no encryption crud" do
    Otto.Table.create_table(QueryNoEncryptionTest)

    attrs = %{
      pk1: "pk1",
      pk2: 2,
      attr1: "attr1",
      attr2: 2,
      attr3: "attr3",
      attr4: %{a: 1, b: 2},
      attr5: 1.23,
      attr6: true
    }

    changeset = changeset(%QueryNoEncryptionTest{}, attrs)
    assert %{valid?: true} = changeset

    put_row_data = struct(QueryNoEncryptionTest, attrs)

    update_row_data = %QueryNoEncryptionTest{
      pk1: "pk1",
      pk2: 2,
      attr2: 6,
      attr6: false
    }

    get_row_data = %{
      pk1: "pk1",
      pk2: 2
    }

    get_range_data1 = %{pk1: "pk1", pk2: :__inf_min__}
    get_range_data2 = %{pk1: "pk2", pk2: :__inf_max__}
    get_range_data3 = %{pk1: :__inf_max__, pk2: :__inf_min__}
    get_range_data4 = %{pk1: :__inf_min__, pk2: :__inf_max__}

    assert {:ok, _} = put_row(put_row_data)
    assert {:ok, _} = put_row(put_row_data)

    assert {:ok, _} =
             put_row(
               put_row_data |> Map.merge(%{pk1: "pk2", attr2: 10})
               # condition: condition(:expect_not_exist)
             )

    assert {:ok, _} = put_row(put_row_data |> Map.merge(%{pk1: "pk3", attr2: 100}))

    assert {:ok, _} = update_row(update_row_data)
    assert {:ok, _} = update_row(update_row_data, delete_all: [:attr3, :attr4])

    assert {:ok, _} = get_row(QueryNoEncryptionTest, get_row_data)
    assert {:ok, _} = get_row(QueryNoEncryptionTest, get_row_data, filter: filter("attr2" == "6"))

    get_request1 = get(QueryNoEncryptionTest, [get_row_data, %{pk1: "hello", pk2: 4}])
    assert {:ok, _} = batch_get([get_request1])

    batch_write_request = [
      {QueryNoEncryptionTest,
       [
         write_put(%QueryNoEncryptionTest{pk1: "pk1", pk2: 5, attr2: 3}),
         write_put(%QueryNoEncryptionTest{pk1: "pk1", pk2: 6, attr3: "test"}),
         write_update(%QueryNoEncryptionTest{pk1: "pk1", pk2: 2, attr2: 9}),
         write_delete(%QueryNoEncryptionTest{pk1: "pk1", pk2: 7})
       ]}
    ]

    assert {:ok, _} = batch_write(batch_write_request)

    assert {:ok, _, _} = get_range(QueryNoEncryptionTest, get_range_data1, get_range_data2)
    assert {:ok, _, _} = get_range(QueryNoEncryptionTest, get_range_data1, get_range_data3)

    assert {:ok, _, _} =
             get_range(QueryNoEncryptionTest, get_range_data4, get_range_data3,
               direction: :forward,
               limit: 1
             )

    assert {:ok, nil, nil} =
             get_range(QueryNoEncryptionTest, get_range_data1, get_range_data4,
               direction: :backward
             )

    assert {:ok, _, _} =
             get_range(QueryNoEncryptionTest, get_range_data1, get_range_data2, iterate: true)

    assert {:ok, _, _} =
             get_range(QueryNoEncryptionTest, get_range_data1, get_range_data3, iterate: true)

    assert {:ok, _} = delete_row(%QueryNoEncryptionTest{pk1: "pk1", pk2: 2})
    assert {:ok, _} = delete_row(%QueryNoEncryptionTest{pk1: "pk2", pk2: 2})
    assert {:ok, _} = delete_row(%QueryNoEncryptionTest{pk1: "pk3", pk2: 2})
    assert :ok == Otto.Table.delete_table(QueryNoEncryptionTest)
  end
end
