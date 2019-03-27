defmodule OttoTest.TableTest do
  use ExUnit.Case

  test "raise error for invalid opts" do
    assert_raise(Otto.Error, "table is not specified", fn ->
      defmodule DemoTableFail do
        use Otto.Table
      end
    end)
  end

  test "use table" do
    defmodule DemoTableUse do
      use Otto.Table,
        instance: Instance1,
        table: "test",
        primary: [pk1: :string, pk2: :integer]

      use Ecto.Schema

      schema "test_query" do
        field(:pk1, :string)
        field(:pk2, :integer)
      end
    end
  end

  test "create, describe, update and delete table" do
    defmodule DemoTableCreate do
      use Otto.Table,
        instance: Instance1,
        table: "test_table",
        primary: [:pk1, :pk2],
        reserved_throughput_read: 10

      use Ecto.Schema

      schema "test_table" do
        field(:pk1, :string)
        field(:pk2, :integer)
        field(:attr1, :string)
        field(:attr2, :integer)
      end
    end

    assert :ok == Otto.Table.create_table(DemoTableCreate)
    assert {:ok, _} = Otto.Table.describe_table(DemoTableCreate)

    # Update table should wait 120 seconds
    # Process.sleep 120_000
    # assert :ok == Otto.Table.update_table(DemoTableCreate)

    assert :ok == Otto.Table.delete_table(DemoTableCreate)
  end
end
