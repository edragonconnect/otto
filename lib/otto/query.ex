defmodule Otto.Query do
  require Logger

  alias ExAliyunOts.Const.PKType
  require PKType
  import ExAliyunOts.Mixin, only: :functions

  alias Otto.Row

  @type data :: struct()
  @type opts :: keyword()

  @moduledoc """
  Module to be used in Otto.Query should:
  1. use Otto.Table
  2. implement function __schema__(:type, field), you can simply use Ecto.Schema

  Example
  ```
  defmodule Example do
    use Otto.Table

    use Ecto.Schema
    import Ecto.Changeset
  end
  ```

  If you need to use Query module, if you need add filter when using `get_row` or
  `get_range`, you'd better import it. Because `filter` is a macro.

  It implemented common actions of ots:
  * put_row
  * update_row
  * get_row
  * delete_row
  * get_range
  * batch_get
  * batch_write
  * search
  * search_iterate_all
  * start_local_transaction
  * commit_transaction
  * abort_transaction
  ...

  For local transaction, it supports:
  - Read: `get_row`, `get_range`
  - Write: `put_row`, `update_row`, `delete_row`, `batch_write`
  A transaction's max ttl is 60s.
  """

  @doc """
  `filter` is a macro, you can pass a filter expression to it.
  It can be used in:
  * `get_row`, `get_range`
  * macro `condition/2`, work with row_existence.

  Examples:
  ```
  filter("age" >= 10)
  filter("attr1" == "attr_value")
  filter(("name[ignore_if_missing: true, latest_version_only: true]" == "hello" and "age" > 1) or ("class" == "1"))
  ```
  """
  defmacro filter(filter_expr) do
    quote do
      ast_expr = unquote(Macro.escape(filter_expr))
      context_binding = binding()
      expressions_to_filter(ast_expr, context_binding)
    end
  end

  @doc """
  Used in `put_row`, `update_row` and `delete_row`.
  `contition/1` need one parameter, only 3 atoms are supported:
  * :ignore
  * :expect_exist
  * :expect_not_exist
  """
  defmacro condition(existence) do
    condition = map_condition(existence)
    Macro.escape(condition)
  end

  @doc """
  Used in `put_row`, `update_row` and `delete_row`.
  `condition/2` need two parameter:
  * the first is the existence, an atom in [:ignore, :expect_exist, :expect_not_exist]
  * the second is a filter_expr.
  """
  defmacro condition(existence, filter_expr) do
    quote do
      condition = map_condition(unquote(existence))
      ast_expr = unquote(Macro.escape(filter_expr))
      context_binding = binding()
      column_condition = expressions_to_filter(ast_expr, context_binding)
      %{condition | column_condition: column_condition}
    end
  end

  # Ots row apis

  @doc """
  `put_row` accepts a struct, all the not_null fields will be inserted.
  You can also pass some options to it:

  - `:condition` - The macro `condition/1` or `condition/2`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  """
  @spec put_row(data, opts) :: {:ok, any} | {:error, any}
  def put_row(data, opts \\ []) do
    {instance, table, pks, attrs, opts} = build_put(data, opts)
    execute_put_row(instance, table, pks, attrs, opts)
  end

  @doc """
  `update_row` accepts a struct. The not_null fields will be inserted to the row. If you want
  to delete some fields, you should add them to the `:delete_fields` option.

  Option fields:
  - `:condition` - The macro `condition/1` or `condition/2`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  - `:delete_all` - fields you want to delete when updating the row, a k-w list or string list.
  """
  @spec update_row(data, opts) :: {:ok, any} | {:error, any}
  def update_row(data, opts \\ []) do
    {instance, table, pks, opts} = build_update(data, opts)
    execute_update_row(instance, table, pks, opts)
  end

  @doc """
  `delete_row` accepts struct that contains all the primary keys.
  You can also pass some options to it:

  - `:condition` - The macro `condition/1` or `condition/2`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  """
  @spec delete_row(data, opts) :: {:ok, any} | {:error, any}
  def delete_row(data, opts \\ []) do
    {instance, table, pks, opts} = build_delete(data, opts)
    execute_delete_row(instance, table, pks, opts)
  end

  @doc """
  `get_row` accepts a module name, a map contains all the primary keys. You can
  also pass some options to it.

  - `:filter` - The macro `filter/1`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  - `:columns_to_get` - a string list of attrubutes you want to get.
  - `:start_column` - The start column string, used for widecolumn read.
  - `:end_column` - The end column string, used for widecolumn read.
  """
  @spec get_row(module, map, opts) :: {:ok, any} | {:error, any}
  def get_row(module, data, opts \\ []) do
    ots = module.__ots__()
    pks = module.__ots__(:pks, data)

    case execute_get_row(ots[:instance], ots[:table], pks, opts) do
      {:ok, %{row: nil}} ->
        {:ok, nil}

      {:ok, %{row: {pks, attrs}}} ->
        attrs = Row.decrypt(attrs, ots[:encrypt])
        data = module.__ots__(:struct, {pks, attrs})

        {:ok, data}

      error ->
        error
    end
  end

  @doc """
  `get_range` accepts a module name, and two maps contains primary keys.
  The second params is the start primary keys, the third is the end primary keys.
  If a primary key's value is inf_max or inf_min, you should
  use `:__inf_max__` or `:__inf_min__`.

  If your rows are more than 5000, please use `iterate_all_range`.

  You can also pass some options to it.

  - `:direction` - `:forward` or `:backward`.
  - `:limit` - Integer, the max rows to get.
  - `:filter` - The macro `filter/1`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  - `:columns_to_get` - a string list of attrubutes you want to get.
  - `:start_column` - The start column string, used for widecolumn read.
  - `:end_column` - The end column string, used for widecolumn read.

  - `:next_start_primary_key` - A base64 string. If you added this, you don't
    need to give correct start_pks_data, it will be ignored.
  - `iterate` - If set true, will use execute_iterate_all_range instead of execute_get_range
  """
  @spec get_range(module, map, map, opts) :: {:ok, any, any} | {:error, any}
  def get_range(module, start_pks_data, end_pks_data, opts \\ []) do
    ots = module.__ots__()

    start_pks =
      case Keyword.get(opts, :next_start_primary_key) do
        nil -> range_pks(start_pks_data, ots[:primary])
        pks -> pks
      end

    end_pks = range_pks(end_pks_data, ots[:primary])

    fun =
      if opts[:iterate] do
        &execute_iterate_all_range/5
      else
        &execute_get_range/5
      end

    case fun.(ots[:instance], ots[:table], start_pks, end_pks, opts) do
      {:ok, %{rows: nil}} ->
        {:ok, nil, nil}

      {:ok, %{rows: rows, next_start_primary_key: cursor}} ->
        data =
          Enum.map(rows, fn {pks, attrs} ->
            attrs = Row.decrypt(attrs, ots[:encrypt])
            module.__ots__(:struct, {pks, attrs})
          end)

        cursor = if cursor, do: Base.encode64(cursor), else: cursor

        {:ok, data, cursor}

      error ->
        error
    end
  end

  # Search Index api
  @doc """
  `search` accepts a module name, a index_name, and some options.

  The full search options should can be found in [`ex_aliyun_ots`](https://github.com/xinz/ex_aliyun_ots/blob/master/test/mixin/search_test.exs).

  If search result is ok, it will return the datas, a cursor, and total_hits number as a tuple {:ok, data, cursor, total}.

  `search` limit is default by 10.
  """
  @spec search(module, atom, opts) :: {:ok, any, any, any} | {:error, any}
  def search(module, index_name, opts \\ []) do
    ots = module.__ots__()

    case execute_search(ots[:instance], ots[:table], to_string(index_name), opts) do
      {:ok, %{rows: nil}} ->
        {:ok, nil, nil, 0}

      {:ok, %{rows: rows, next_token: cursor, total_hits: total}} ->
        data =
          Enum.map(rows, fn {pks, attrs} ->
            attrs = Row.decrypt(attrs, ots[:encrypt])
            module.__ots__(:struct, {pks, attrs})
          end)

        cursor = if cursor, do: Base.encode64(cursor), else: cursor

        {:ok, data, cursor, total}

      error ->
        error
    end
  end

  @doc """
  `search_iterate_all` is the iterate version of `search`, which will continue search until next_token is nil.
  This function will ignore [:sort, :token, :limit] in your search_query option.
  """
  @spec search(module, atom, opts) :: {:ok, any, any, any} | {:error, any}
  def search_iterate_all(module, index_name, opts \\ []) do
    search_query =
      opts[:search_query] |> Keyword.drop([:sort, :token, :limit]) |> Keyword.put(:limit, 100)

    opts = Keyword.put(opts, :search_query, search_query)

    search_iterate(module, index_name, [], opts)
  end

  defp search_iterate(module, index_name, current_res, opts) do
    case search(module, index_name, opts) do
      {:ok, nil, nil, 0} ->
        {:ok, nil, 0}

      {:ok, res, nil, number} ->
        {:ok, res ++ current_res, number}

      {:ok, res, token, number} ->
        search_query = opts[:search_query] |> Keyword.put(:token, Base.decode64!(token))
        opts = Keyword.put(opts, :search_query, search_query)
        current_res = res ++ current_res
        search_iterate(module, index_name, current_res, opts)

      error ->
        error
    end
  end

  @doc """
  `start_local_transaction` is use to create a local transaction.
  It accepts a module name, a partition key's value, and some options.
  A partition key is the first primary key of your table.
  """
  def start_local_transaction(module, partition_key_value, opts \\ []) do
    ots = module.__ots__()
    [partition | _] = ots[:primary]
    partition_key = {to_string(partition), partition_key_value}

    case execute_start_local_transaction(ots[:instance], ots[:table], partition_key, opts) do
      {:ok, %{transaction_id: transaction_id}} -> {:ok, transaction_id}
      error -> error
    end
  end

  @doc """
  `abort_transaction` can abort a started local transaction.
  It accepts a module name, a transaction id and opts.
  You can get a transaction id when you finished `start_local_transaction`.
  """
  def abort_transaction(module, transaction_id, opts \\ []) do
    ots = module.__ots__()
    execute_abort_transaction(ots[:instance], transaction_id, opts)
  end

  @doc """
  `commit_transaction` will commit transaction with the given id.
  It accepts a module name, a transaction_id and opts.
  You can get a transaction id when you finished `start_local_transaction`.
  """
  def commit_transaction(module, transaction_id, opts \\ []) do
    ots = module.__ots__()
    execute_commit_transaction(ots[:instance], transaction_id, opts)
  end

  # Batch apis

  @doc """
  `batch_get` is used for getting multi get_row requests in a single time. If batch get success, it will
  return a map of responses.
  It accepts a list of requests, you can generate a single request with `get` function.

  Example:
  ```
  batch_get([
    get(Module1, [%{pk11: "a1", pk12: "b1"}, %{pk11: "a2", pk12: "b2"}]),
    get(Module2, [%{pk21: "c1", pk22: "d1"}, %{pk21: "c2", pk22: "d2"}])
  ])
  ```
  """
  @spec batch_get(list, opts) :: {:ok, any} | {:error, any}
  def batch_get(requests, opts \\ []) when is_list(requests) do
    module = requests |> List.first() |> Tuple.to_list() |> List.first()
    instance = module.__ots__()[:instance]

    {batch_meta, requests} =
      requests
      |> Enum.reduce({%{}, []}, fn {module, table, request}, {batch_meta_acc, requests_acc} ->
        {Map.put(batch_meta_acc, table, module), [request | requests_acc]}
      end)

    case execute_batch_get(instance, requests, opts) do
      {:ok, %{tables: tables}} ->
        data =
          tables
          |> Enum.reduce(%{}, fn table, acc ->
            module = batch_meta[table.table_name]
            ots = module.__ots__()

            rows =
              table.rows
              |> Enum.map(fn response ->
                case response.row do
                  nil ->
                    nil

                  {pks, attrs} ->
                    attrs = Row.decrypt(attrs, ots[:encrypt])
                    module.__ots__(:struct, {pks, attrs})
                end
              end)

            Map.put(acc, module, rows)
          end)

        {:ok, data}

      error ->
        error
    end
  end

  @doc """
  `get` is used for generating `batch_get request`. It accepts a module name, a map contains
  all the primary keys(Same as `get_row`). You can also pass options to it.

  `datas` should be a list of data maps.

  - `:filter` - The macro `filter/1`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  - `:columns_to_get` - a string list of attrubutes you want to get.
  - `:start_column` - The start column string, used for widecolumn read.
  - `:end_column` - The end column string, used for widecolumn read.
  """
  @spec get(module, list, opts) :: {module, String.t(), any}
  def get(module, datas, opts \\ []) when is_list(datas) do
    ots = module.__ots__()
    table = ots[:table]
    pks_list = datas |> Enum.map(fn data -> module.__ots__(:pks, data) end)
    {module, table, execute_get(table, pks_list, opts)}
  end

  @doc """
  `batch_write` is used for writing `put`, `update`, `delete` requests of multi-tables in a single time.
  It accepts a list of requests, each of which is a tuple of {module, requests}.
  You can generate requests of a module using `write_put`, `write_update` or `write_delete`.

  Example:
  ```
  batch_write([
    {Module1, [
      write_put(%Module1{pk11: "a1", pk12: "b1", attr1: "attr1"}),
      write_put(%Module1{pk11: "a2", pk12: "b2", attr1: "attr2"}),
      write_update(%Module1{pk11: "a3", pk12: "b3", attr1: "attr3"}),
      write_delete(%Module1{pk11: "a4", pk12: "b4"})
    ]},
    {Module2, [
      write_put(%Module2{pk11: "a1", pk12: "b1", attr1: "attr1"}),
      write_put(%Module2{pk11: "a2", pk12: "b2", attr1: "attr2"}),
      write_update(%Module2{pk11: "a3", pk12: "b3", attr1: "attr3"}),
      write_delete(%Module2{pk11: "a4", pk12: "b4"})
    ]},
  ])
  ```
  """
  @spec batch_write(list, opts) :: {:ok, any} | {:error, any}
  def batch_write(requests, opts \\ []) when is_list(requests) do
    module = requests |> List.first() |> Tuple.to_list() |> List.first()
    instance = module.__ots__()[:instance]

    write_requests =
      requests
      |> Enum.map(fn {module, request_list} ->
        ots = module.__ots__()
        {ots[:table], request_list}
      end)

    execute_batch_write(instance, write_requests, opts)
  end

  @doc """
  `write_put` is used for  generating `batch_write` request.
  It accepts a struct, all the not_null fields will be inserted.
  You can also pass some options to it:

  - `:condition` - The macro `condition/1` or `condition/2`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  """
  @spec write_put(data, opts) :: any
  def write_put(data, opts \\ []) do
    {_instance, _table, pks, attrs, opts} = build_put(data, opts)
    execute_write_put(pks, attrs, opts)
  end

  @doc """
  `write_update` is used for  generating `batch_write` request.
  It accepts a struct, all the not_null fields will be inserted.
  You can also pass some options to it:

  - `:condition` - The macro `condition/1` or `condition/2`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  - `:delete_all` - fields you want to delete when updating the row, a k-w list or string list.
  """
  @spec write_update(data, opts) :: any
  def write_update(data, opts \\ []) do
    {_instance, _table, pks, opts} = build_update(data, opts)
    execute_write_update(pks, opts)
  end

  @doc """
  `write_delete` is used for  generating `batch_write` request.
  It accepts a struct, all the not_null fields will be inserted.
  You can also pass some options to it:

  - `:condition` - The macro `condition/1` or `condition/2`
  - `:return_type` - an atom in [:'RT_NONE', :none, :'RT_PK', :pk]
  """
  @spec write_delete(data, opts) :: any
  def write_delete(data, opts \\ []) do
    {_instance, _table, pks, opts} = build_delete(data, opts)
    execute_write_delete(pks, opts)
  end

  # Helper functions

  defp range_pks(data, primary) do
    Enum.map(primary, fn pk ->
      case Map.get(data, pk) do
        :__inf_min__ ->
          {to_string(pk), PKType.inf_min()}

        :__inf_max__ ->
          {to_string(pk), PKType.inf_max()}

        value ->
          {to_string(pk), value}
      end
    end)
  end

  defp build_put(data, opts) do
    module = data.__struct__
    ots = module.__ots__()
    pks = module.__ots__(:pks, data)
    attrs = module.__ots__(:attrs, data)
    attrs = Row.encrypt(attrs, ots[:encrypt])
    opts = Keyword.put_new(opts, :condition, condition(:ignore))
    {ots[:instance], ots[:table], pks, attrs, opts}
  end

  defp build_update(data, opts) do
    module = data.__struct__
    ots = module.__ots__()
    pks = module.__ots__(:pks, data)
    attrs = module.__ots__(:attrs, data)

    attrs =
      Row.encrypt(attrs, ots[:encrypt], fn ->
        opts = [columns_to_get: Row.cipher_columns()]

        case execute_get_row(ots[:instance], ots[:table], pks, opts) do
          {:ok, %{row: {_, values}}} when length(values) == 2 ->
            Row.fetch_cipher(values)

          {:ok, _} ->
            Row.new_cipher()

          {:error, error} ->
            Logger.error(inspect(error))
            raise RuntimeError
        end
      end)

    opts =
      opts
      |> Keyword.put_new(:condition, condition(:expect_exist))
      |> Keyword.update(:delete_all, [], fn fields -> Enum.map(fields, &to_string/1) end)
      |> Keyword.put(:put, attrs)

    {ots[:instance], ots[:table], pks, opts}
  end

  defp build_delete(data, opts) do
    module = data.__struct__
    ots = module.__ots__()
    pks = module.__ots__(:pks, data)
    opts = Keyword.put_new(opts, :condition, condition(:ignore))
    {ots[:instance], ots[:table], pks, opts}
  end
end
