defmodule Otto.Table do
  @moduledoc """
  Define an ots table.

  ## Usage

  ```
  defmodule DemoTableCreate do
    use Otto.Table,
      instance: Instance1,
      table: "test_table",
      primary: [:pk1, :pk2],
      encrypt: [:enc1, :enc2],
      reserved_throughput_read: 10,
      index: [
        index_name1: [field_name1: :long, field_name2: :text],
        index_name2: [field_name1: :keyword, field_name2: :text]
      ]
  end
  ```

  ## Options

  #### required fields

  - `table` - The ots table name, it should be unique in one instance.
  - `primary` - Primary keys list, by order, up to 4. The first key is the partition key.

  #### optional fields

  - `encrypt` - Encrypt fields list, primary keys can not be encrypted.
  - `index` - For the new ots index funtion, used when creating search indexes. You can create multiple
    indexes in a single table, and field_type of the same field can be different in different indexes.
    These field_types are supported: [:long, :double, :boolean, :keyword, :text, :nested, :geo_point].
  - `reserved_throughput_write` - Integer, table write performance data.
  - `reserved_throughput_read` - Integer, table read performance data.
  - `time_to_live` - Integer, live seconds of the table data stored.
  - `max_versions` - Integer, max versions of table.
  - `deviation_cell_version_in_sec` - Integer, Prohibit writing to data with large gap with expectations.
  - `stream_spec` - Keyword list, define stream specs of the table, two options are avaliable:
    a boolean `enable_stream` and an integer `expiration_time`.
    For example: [enable_stream: true, expiration_time: 9999999999999]

  """

  import ExAliyunOts.Mixin, only: :functions
  alias ExAliyunOts.Const.PKType
  require PKType

  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.Search.FieldType
  require FieldType

  @type table :: struct()
  @type field :: atom()

  @callback __schema__(:type, field) :: atom()
  @callback __schema__(:fields) :: list(atom)

  defmacro __using__(opts) when is_list(opts) do
    assert Keyword.get(opts, :instance), "instance keyword is not specified"
    assert Keyword.get(opts, :table), "table is not specified"

    primary = Keyword.get(opts, :primary)

    assert primary, "primary keys are not specified"
    assert 0 < length(primary) and length(primary) < 5, "invalid number of primary keys"

    if encrypt = Keyword.get(opts, :encrypt) do
      assert primary -- encrypt == primary, "primary keys can not be encrypted"
    end

    quote do
      @behaviour unquote(__MODULE__)

      @ots unquote(opts)

      @ots_type [:string, :integer, :float, :boolean]

      def __ots__, do: @ots

      def __ots__(:pks, data) do
        Enum.map(@ots[:primary], fn pk ->
          {to_string(pk), Map.get(data, pk)}
        end)
      end

      def __ots__(:attrs, data) do
        fields = __schema__(:fields) -- @ots[:primary]

        Enum.reduce(fields, [], fn key, acc ->
          case Map.get(data, key) do
            nil ->
              acc

            value ->
              value = __ots__(:encode, key, value)
              [{to_string(key), value} | acc]
          end
        end)
      end

      def __ots__(:encode, key, value) do
        type = __schema__(:type, key)

        if type in @ots_type do
          value
        else
          Jason.encode!(value)
        end
      end

      def __ots__(:decode, key, value) do
        type = __schema__(:type, key)

        if type in @ots_type do
          value
        else
          Jason.decode!(value)
        end
      end

      def __ots__(:struct, {pks, attrs}) do
        fields = __schema__(:fields) -- @ots[:primary]

        data =
          Enum.reduce(pks, %{}, fn {key, value}, acc ->
            Map.put(acc, String.to_atom(key), value)
          end)

        data =
          Enum.reduce(fields, data, fn key, data ->
            case List.keyfind(attrs, to_string(key), 0) do
              {_, value, _} ->
                value = __ots__(:decode, key, value)
                Map.put(data, key, value)

              nil ->
                data
            end
          end)

        struct(__MODULE__, data)
      end
    end
  end

  defp assert(assertion, msg) do
    unless assertion do
      raise Otto.Error, message: msg
    end
  end

  @doc """
  Create an OTS table.

  You must create a table before you can interact with the table.
  The only parameter `table` is the module name of your table, the table
  to generate will take the options when you use `Otto.Table`.

  Once created, the table's primary keys cannot change.
  """
  @spec create_table(table) :: :ok | {:error, any}
  def create_table(table) do
    ots = table.__ots__()

    pks =
      Enum.map(ots[:primary], fn key ->
        type =
          case table.__schema__(:type, key) do
            :string ->
              PKType.string()

            :integer ->
              PKType.integer()

            _ ->
              raise Otto.Error, message: "invalid primary key type"
          end

        {to_string(key), type}
      end)

    execute_create_table(ots[:instance], ots[:table], pks, ots)
  end

  @doc """
  Describe your OTS table.

  It will return table_meta, reserved_throughput_details, table_options,
  stream_specs, shared_splits info.
  """
  @spec describe_table(table) :: {:ok, any} | {:error, any}
  def describe_table(table) do
    ots = table.__ots__()
    execute_describe_table(ots[:instance], ots[:table], [])
  end

  @doc """
  Update your OTS table.

  OTS table's primary cannot be changed, you can update table_options,
  stream_specs. All the params it takes are from your options when
  using `Otto.Table`.
  """
  @spec update_table(table) :: :ok | {:error, any}
  def update_table(table) do
    ots = table.__ots__()
    execute_update_table(ots[:instance], ots[:table], ots)
  end

  @doc """
  Delete your OTS table.
  """
  @spec delete_table(table) :: :ok | {:error, any}
  def delete_table(table) do
    ots = table.__ots__()
    execute_delete_table(ots[:instance], ots[:table], [])
  end

  @doc """
  Create an index for your OTS table.
  One table can contain multiple indexes, use this function to create one at a time.
  """
  @spec create_index(table, String.t()) :: {:ok, any} | {:error, any}
  def create_index(table, index_name) do
    field_types = %{
      long: FieldType.long(),
      double: FieldType.double(),
      boolean: FieldType.boolean(),
      keyword: FieldType.keyword(),
      text: FieldType.text(),
      nested: FieldType.nested(),
      geo_point: FieldType.geo_point()
    }

    ots = table.__ots__()
    index_meta = ots[:index][index_name]

    field_schemas =
      index_meta
      |> Enum.map(fn {field, type} ->
        %Search.FieldSchema{field_name: to_string(field), field_type: field_types[type]}
      end)

    create_index_request = %Search.CreateSearchIndexRequest{
      table_name: ots[:table],
      index_name: to_string(index_name),
      index_schema: %Search.IndexSchema{
        field_schemas: field_schemas
      }
    }

    ExAliyunOts.Client.create_search_index(ots[:instance], create_index_request)
  end

  @doc """
  List search indexes of your OTS table.
  """
  @spec list_search_index(table, keyword()) :: {:ok, any} | {:error, any}
  def list_search_index(table, opts \\ []) do
    ots = table.__ots__()
    execute_list_search_index(ots[:instance], ots[:table], opts)
  end

  @doc """
  Delete a search index of your OTS table.
  """
  @spec delete_search_index(table, atom(), keyword()) :: {:ok, any} | {:error, any}
  def delete_search_index(table, index_name, opts \\ []) do
    ots = table.__ots__()
    execute_delete_search_index(ots[:instance], ots[:table], to_string(index_name), opts)
  end

  @doc """
  Describe a search index of your OTS table.
  """
  @spec describe_search_index(table, atom(), keyword()) :: {:ok, any} | {:error, any}
  def describe_search_index(table, index_name, opts \\ []) do
    ots = table.__ots__()
    execute_describe_search_index(ots[:instance], ots[:table], to_string(index_name), opts)
  end
end
