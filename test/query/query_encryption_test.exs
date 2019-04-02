defmodule OttoTest.QueryEncryptionTest do
  use ExUnit.Case

  use Ecto.Schema

  use Otto.Table,
    instance: Instance1,
    table: "query_encryption",
    primary: [:pk1, :pk2],
    encrypt: [:attr4, :attr6]

  import Ecto.Changeset
  import Otto.Query
  alias OttoTest.QueryEncryptionTest

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

  test "test encryption crud" do
    Otto.Table.create_table(QueryEncryptionTest)

    attrs = %{
      pk1: "pk1",
      pk2: 3,
      attr1: "attr1",
      attr2: 2,
      attr3: "attr3",
      attr4: %{a: 1, b: 2},
      attr5: 1.32,
      attr6: false
    }

    changeset = changeset(%QueryEncryptionTest{}, attrs)
    assert %{valid?: true} = changeset

    put_row_data = struct(QueryEncryptionTest, attrs)

    update_row_data = %QueryEncryptionTest{
      pk1: "pk1",
      pk2: 3,
      attr3: "attr3_update",
      attr4: %{a: 3, b: 4},
      attr6: true
    }

    update_row_data2 = update_row_data |> Map.put(:pk1, "pk12")
    get_row_data = %{pk1: "pk1", pk2: 3}
    get_row_data2 = %{pk1: "pk12", pk2: 3}
    get_range_data1 = %{pk1: "pk1", pk2: :__inf_min__}
    get_range_data2 = %{pk1: "pk2", pk2: :__inf_max__}
    get_range_data3 = %{pk1: :__inf_max__, pk2: :__inf_min__}
    get_range_data4 = %{pk1: :__inf_min__, pk2: :__inf_max__}

    assert {:ok, _} = put_row(put_row_data)
    assert {:ok, _} = put_row(put_row_data)

    assert {:ok, _} =
             put_row(
               put_row_data |> Map.merge(%{pk1: "pk12", attr2: 10})
               # condition: condition(:expect_not_exist)
             )

    assert {:ok, _} = put_row(put_row_data |> Map.merge(%{pk1: "pk13", attr2: 100}))

    assert {:ok, _} = update_row(update_row_data)
    assert {:ok, _} = update_row(update_row_data, delete_fields: [:attr2])
    assert {:ok, _} = update_row(update_row_data2)

    assert {:ok, _} = get_row(QueryEncryptionTest, get_row_data)
    assert {:ok, _} = get_row(QueryEncryptionTest, get_row_data2)
    assert {:ok, _} = get_row(QueryEncryptionTest, get_row_data, filter: filter("attr2" == "9"))
    assert {:ok, _} = get_row(QueryEncryptionTest, %{pk1: "pk2", pk2: 3})

    get_request1 = get(QueryEncryptionTest, [get_row_data, %{pk1: "hello", pk2: 4}])
    assert {:ok, _} = batch_get([get_request1])

    batch_write_request = [
      {QueryEncryptionTest,
       [
         write_put(%QueryEncryptionTest{pk1: "pk1", pk2: 5, attr2: 3}),
         write_put(%QueryEncryptionTest{pk1: "pk1", pk2: 6, attr3: "test"}),
         write_update(%QueryEncryptionTest{pk1: "pk1", pk2: 2, attr2: 9}),
         write_delete(%QueryEncryptionTest{pk1: "pk1", pk2: 7})
       ]}
    ]

    assert {:ok, _} = batch_write(batch_write_request)

    assert {:ok, _, _} = get_range(QueryEncryptionTest, get_range_data1, get_range_data2)
    assert {:ok, _, _} = get_range(QueryEncryptionTest, get_range_data1, get_range_data3)

    assert {:ok, _, _} =
             get_range(QueryEncryptionTest, get_range_data4, get_range_data3,
               direction: :forward,
               limit: 1
             )

    assert {:ok, nil, nil} =
             get_range(QueryEncryptionTest, get_range_data1, get_range_data4, direction: :backward)

    assert {:ok, _, _} =
             get_range(QueryEncryptionTest, get_range_data1, get_range_data2, iterate: true)

    assert {:ok, _, _} =
             get_range(QueryEncryptionTest, get_range_data1, get_range_data3, iterate: true)

    assert {:ok, _} = delete_row(%QueryEncryptionTest{pk1: "pk1", pk2: 3})
    assert {:ok, _} = delete_row(%QueryEncryptionTest{pk1: "pk12", pk2: 3})
    assert {:ok, _} = delete_row(%QueryEncryptionTest{pk1: "pk13", pk2: 3})
    assert :ok == Otto.Table.delete_table(QueryEncryptionTest)
  end
end
