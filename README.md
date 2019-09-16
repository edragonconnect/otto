# Otto

## NOTICE:

This project is no longer maintained, if you are seeking for a replacement of this Ecto adapter, we would like to recommend [`ecto_tablestore`](https://hex.pm/packages/ecto_tablestore) for your reference.

## Introduce

Otto is an easy-to-use wrapper for accessing [Aliyun Table Store(OTS)](https://www.aliyun.com/product/ots), a distributed NoSQL database. It is based on package [`ex_aliyun_ots`](https://github.com/xinz/ex_aliyun_ots). It works well with `ecto`, which means you can define struct and field types in ecto, then otto will handle it.

Using otto, you can:
* Easily create and update ots table.
* CURD in an ecto-like way.
* Encrypt fields if neccessary, using `AES` encryption algorithm.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `otto` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:otto, "~> 0.1.0"}
  ]
end
```

## Configuration

#### Add ex_aliyun_ots configuration.

Otto depends on `ex_aliyun_ots`, so you should add config for it first, without this, your app cannot run.
```elixir
config :ex_aliyun_ots, instances: [Instance1, Instance2]

config :ex_aliyun_ots, Instance1,
  name: "instance1_name",
  endpoint: "YOUR-OTS-ENDPOINT",
  access_key_id: "YOUR-ACCESS-KEY-ID",
  access_key_secret: "YOUR-ACCESS-KEY-SECRET"

config :ex_aliyun_ots, Instance2,
  name: "instance2_name",
  endpoint: "YOUR-OTS-ENDPOINT",
  access_key_id: "YOUR-ACCESS-KEY-ID",
  access_key_secret: "YOUR-ACCESS-KEY-SECRET"
```
#### Add otto configuration.

Here is an example of otto configuration.

`:ciphers` is a keyword list of cipher configs. A cipher contains three parts:
* tag: such as `aes_gcm_v2`, `aes_gcm_v1` in the config. It is the key of each cipher. Tag will be used to get key.
* module: the real aes algorithm you use. Now we implemented AES-GCM and AES-CDR. You can also define your own, but it must implement behaviour `Otto.Cipher`.
* key: the key to use when encrypting and decrypting in aes. You can generate a key by `Otto.Cipher.generate_key/0`, or run `32 |> :crypto.strong_rand_bytes() |> Base.encode64()`.

When a new row need encryption, it will use the first cipher in the list. An `iv` is generated for each encryption, iv is similar to salt. We will add `__aes_iv__` and `__aes_tag__` in your ots row. When updating or decrypting the row, we will use the same iv and tag.

```elixir
config :otto,
  ciphers: [
    aes_gcm_v2: [
      module: Otto.Cipher.AES.GCM,
      key: "2DR+mrNKNv3bGsQA2VnvTy8WrUwtNiO28/VXgWwAYEE=" |> Base.decode64!()
    ],
    aes_gcm_v1: [
      module: Otto.Cipher.AES.GCM,
      key: "QLHEOuMbWAQVkfe3u14gNOZYajKOgz0q0mB7cyjdBTo=" |> Base.decode64!()
    ]
  ]
```

## Usage

#### Define a Table
You can define a table using `Otto.Table` with some options, could look like this:
```elixir
defmodule DemoTableCreate do
  use Otto.Table,
    instance: Instance1,
    table: "test_table",
    primary: [:pk1, :pk2],
    encrypt: [:enc1, :enc2],
    reserved_throughput_read: 10,
    index: [
      index_name1: [field_name1: :long, field_name2: :text],
      index_name2: [field_name1: :keyword, field_name2: :text],
    ]
end
```
required fields:
* `table`: the ots table name, it should be unique in one instance.
* `primary`: the primary keys atom list.

optional fields:
* `encrypt`: fields to encrypt, encrypt should not be in primary.
* `index`: search index information, one table can have multiple indexes.
* `reserved_throughput_write`: integer, table write performance data.
* `reserved_throughput_read`: integer, table read performance data.
* `time_to_live`: integer, live seconds of the table data stored.
* `max_versions`: integer, max versions of table.
* `deviation_cell_version_in_sec`: integer.
* `stream_spec`: keyword list, define stream specs of the table, such as [enable_stream: true, expiration_time: 9999999999999]

With the configuration, table "test_table" will be created by `Otto.Table.create_table(DemoTableCreate)`, then you can get a function `__ots__/0` with the instance name and all the metadata defined in options. And `__ots__/1` with some useful functions.

###### Attention
`@behaviour Otto.Table` is already added when using Otto.Table, you need to implement the two callbacks in your table module.
```elixir
@callback __schema__(:type, field) :: atom()
@callback __schema__(:fields) :: list(atom)
```
But if you use Ecto.Schema, it already did it.

#### Do CURD with Otto.Query
Otto.Query has two macro called `filter` and `condition`, which can be used when using get_row or get_range. So if you use the filter, you'd better import Otto.Query.

Here is a sample:
```elixir
defmodule DemoTable do
  use Otto.Table,
    instance: Instance1,
    table: "demo",
    primary: [:pk1, :pk2],
    attrs: [:attr1, :attr2, :attr3, :attr4, :attr5, :attr6]
    encrypt: [:attr2, :attr4, :attr6]

  use Ecto.Schema
  import Ecto.Changeset
  import Otto.Query
  alias DemoTable

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

  def test do
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

    put_row_data = struct(DemoTable, attrs)
    update_row_data = %DemoTable{
      pk1: "pk1",
      pk2: 3,
      attr3: "attr3_update",
      attr4: %{a: 3, b: 4},
      attr6: true
    }
    get_row_data = %{
      pk1: "pk1",
      pk2: 3
    }
    get_row_data2 = %{
      pk1: "pk12",
      pk2: 3
    }
    get_range_data1 = %{pk1: "pk1", pk2: :__inf_min__}
    get_range_data2 = %{pk1: "pk2", pk2: :__inf_max__}
    get_range_data3 = %{pk1: :__inf_max__, pk2: :__inf_min__}
    get_range_data4 = %{pk1: :__inf_min__, pk2: :__inf_max__}

    put_row(put_row_data)
    put_row(put_row_data |> Map.merge(%{pk1: "pk12", attr2: 10}))
    put_row(put_row_data |> Map.merge(%{pk1: "pk13", attr2: 100}))

    update_row(update_row_data)
    update_row(update_row_data, delete_fields: [:attr2, :attr5])

    get_row(DemoTable, get_row_data)
    get_row(DemoTable, get_row_data2)
    get_row(DemoTable, get_row_data, filter: filter("attr2" == "9"))
    get_row(DemoTable, %{pk1: "pk2", pk2: 3})

    get_range(DemoTable, get_range_data1, get_range_data2)
    get_range(DemoTable, get_range_data1, get_range_data3)
    get_range(DemoTable, get_range_data4, get_range_data3, direction: :forward, limit: 1)
    assert {:ok, nil} = get_range(DemoTable, get_range_data1, get_range_data4, direction: :backward)

    delete_row(%DemoTable{pk1: "pk1", pk2: 3})
    delete_row(%DemoTable{pk1: "pk12", pk2: 3})
    delete_row(%DemoTable{pk1: "pk13", pk2: 3})
  end
end
```
If the table has encrypt_fields, the encrypt fields will be stored encrypted.

## Docs
Run `mix docs`

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ots_wrapper](https://hexdocs.pm/otto).
