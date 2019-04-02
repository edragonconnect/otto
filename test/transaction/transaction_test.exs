defmodule TransactionTest do
  import Otto.Query

  use ExUnit.Case

  use Otto.Table,
    instance: Instance1,
    table: "test_txn_range",
    primary: [:key, :key2]

  use Ecto.Schema

  schema "otto_txn_table" do
    field(:key, :string)
    field(:key2, :integer)

    field(:attr, :string)
    field(:new_added, :boolean)
    field(:new_added2, :string)
  end

  test "start and abort transaction" do
    assert {:ok, transaction_id} = start_local_transaction(TransactionTest, "key1")
    assert {:ok, _} = abort_transaction(TransactionTest, transaction_id)
  end

  test "error without transaction_id when started local transaction" do
    assert {:ok, transaction_id} = start_local_transaction(TransactionTest, "key1")
    assert {:error, _} = put_row(%TransactionTest{key: "key1", key2: 1, attr: "attr2"})
    assert {:ok, _} = abort_transaction(TransactionTest, transaction_id)
  end

  test "update row with transaction_id" do
    assert {:ok, transaction_id} = start_local_transaction(TransactionTest, "key1")

    assert {:ok, _} =
             update_row(%TransactionTest{key: "key1", key2: 1, attr: "attr2"},
               transaction_id: transaction_id,
               condition: condition(:ignore)
             )

    assert {:ok, _} = commit_transaction(TransactionTest, transaction_id)
  end
end
