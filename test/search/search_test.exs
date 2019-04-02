defmodule SearchTest do
  import Otto.Query
  alias ExAliyunOts.Const.Search.{ColumnReturnType, QueryType, SortOrder, SortType}
  require ColumnReturnType
  require QueryType
  require SortOrder
  require SortType

  use ExUnit.Case

  use Otto.Table,
    instance: Instance1,
    table: "otto_search_test",
    primary: [:pk_str, :pk_int],
    index: [
      index1: [rank: :long, player: :text],
      index2: [rank: :long, player: :keyword],
      index_to_delete: [rank: :long, playre: :text]
    ]

  use Ecto.Schema

  schema "otto_search_table" do
    field(:pk_str, :string)
    field(:pk_int, :integer)

    field(:rank, :integer)
    field(:player, :string)
  end

  # test "origin search test" do
  #  alias ExAliyunOts.Var
  #  alias ExAliyunOts.Var.Search
  #  alias ExAliyunOts.Const.PKType
  #  require PKType

  #  alias ExAliyunOts.Const.Search.{FieldType, ColumnReturnType}
  #  require FieldType
  #  require ColumnReturnType

  #  import Otto.Query

  #  instance = Application.get_env(:otto, :instance)

  #  # assert :ok == Otto.Table.create_table(SearchTest)

  #  table_meta = SearchTest.__ots__()

  #  create_index_request = %Search.CreateSearchIndexRequest{
  #    table_name: table_meta[:table],
  #    index_name: "test_search_index",
  #    index_schema: %Search.IndexSchema{
  #      field_schemas: [
  #        %Search.FieldSchema{
  #          field_name: "rank",
  #          field_type: FieldType.long()
  #        },
  #        %Search.FieldSchema{
  #          field_name: "player",
  #          field_type: FieldType.text()
  #        },
  #        %Search.FieldSchema{
  #          field_name: "pk_int",
  #          field_type: FieldType.long()
  #        }
  #      ]
  #    }
  #  }

  #  # ExAliyunOts.Client.create_search_index(instance, create_index_request) |> IO.inspect

  #  batch_write_request = [
  #    {SearchTest,
  #     [
  #       write_put(%SearchTest{pk_str: "76ers", pk_int: 3, rank: 95, player: "Allen Iverson"}),
  #       write_put(%SearchTest{pk_str: "lakers", pk_int: 32, rank: 97, player: "Magic Johnson"}),
  #       write_put(%SearchTest{pk_str: "lakers", pk_int: 34, rank: 98, player: "Shaq O'Neal"}),
  #       write_put(%SearchTest{pk_str: "76ers", pk_int: 34, rank: 96, player: "Charles Barkley"}),
  #       write_put(%SearchTest{pk_str: "rockets", pk_int: 34, rank: 98, player: "Hakeem Olajuwon"}),
  #       write_put(%SearchTest{pk_str: "spurs", pk_int: 21, rank: 97, player: "Tim Duncan"}),
  #       write_update(%SearchTest{pk_str: "rockets", pk_int: 13, rank: 97}),
  #       write_put(%SearchTest{
  #         pk_str: "no_team",
  #         pk_int: 00,
  #         rank: 100,
  #         player: "Lebron James Harden"
  #       }),
  #       write_put(%SearchTest{
  #         pk_str: "sentence1",
  #         pk_int: 00,
  #         rank: 100,
  #         player: "this is tablestore"
  #       })
  #     ]}
  #  ]

  #  # batch_write(batch_write_request) |> IO.inspect

  #  search_request_match = %Search.SearchRequest{
  #    table_name: table_meta[:table],
  #    index_name: "test_search_index",
  #    search_query: %Search.SearchQuery{
  #      query: %Search.MatchQuery{
  #        field_name: "player",
  #        text: "James Harden"
  #      }
  #      # query: %Search.MatchQuery{
  #      #  field_name: "pk_int",
  #      #  text: "23"
  #      # },

  #      # query: %Search.MatchAllQuery{}

  #      # query: %Search.MatchQuery{
  #      #  field_name: "player",
  #      #  text: "James Harden"
  #      # }
  #      # query: %Search.MatchPhraseQuery{
  #      #  field_name: "player",
  #      #  text: "James"
  #      # }
  #    }
  #  }

  #  assert {:ok, _} = ExAliyunOts.Client.search(instance, search_request_match)
  # end

  test "otto create index" do
    # assert {:ok, _} = Otto.Table.create_index(SearchTest, :index1)
    # assert {:ok, _} = Otto.Table.create_index(SearchTest, :index2)   
  end

  test "create and delete a index" do
    assert {:ok, _} = Otto.Table.create_index(SearchTest, :index_to_delete)
    Process.sleep(2_000)
    assert {:ok, _} = Otto.Table.delete_search_index(SearchTest, :index_to_delete)
  end

  test "otto list search index" do
    assert {:ok, _} = Otto.Table.list_search_index(SearchTest)
  end

  test "otto describe search index" do
    assert {:ok, _} = Otto.Table.describe_search_index(SearchTest, :index1)
  end

  test "otto search match_query" do
    assert {:ok, _, _, _} =
             search(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.match(),
                   field_name: "player",
                   text: "James Harden"
                 ],
                 limit: 2
               ]
             )

    # |> IO.inspect

    assert {:ok, [], nil, 0} =
             search(SearchTest, :index2,
               search_query: [
                 query: [
                   type: QueryType.match(),
                   field_name: "player",
                   text: "James"
                 ]
               ]
             )

    # |> IO.inspect
  end

  test "otto search term query" do
    assert {:ok, _, _, _} =
             search(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.term(),
                   field_name: "player",
                   term: "James"
                 ]
               ]
             )

    # |> IO.inspect

    assert {:ok, _, _, _} =
             search(SearchTest, :index2,
               search_query: [
                 query: [
                   type: QueryType.term(),
                   field_name: "player",
                   term: "James"
                 ]
               ]
             )

    # |> IO.inspect
  end

  test "otto search terms query" do
    assert {:ok, _, _, _} =
             search(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.terms(),
                   field_name: "player",
                   terms: ["James", "Harden"]
                 ]
               ]
             )

    # |> IO.inspect

    assert {:ok, _, _, _} =
             search(SearchTest, :index2,
               search_query: [
                 query: [
                   type: QueryType.terms(),
                   field_name: "player",
                   terms: ["James", "Harden"]
                 ]
               ]
             )

    # |> IO.inspect
  end

  test "otto search prefix query" do
    assert {:ok, _, _, _} =
             search(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.prefix(),
                   field_name: "player",
                   prefix: "James"
                 ]
               ]
             )

    # |> IO.inspect

    assert {:ok, _, _, _} =
             search(SearchTest, :index2,
               search_query: [
                 query: [
                   type: QueryType.prefix(),
                   field_name: "player",
                   prefix: "James"
                 ]
               ]
             )

    # |> IO.inspect
  end

  test "otto search range query" do
    assert {:ok, _, _, _} =
             search(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.range(),
                   field_name: "player",
                   from: "A",
                   to: "Z",
                   include_upper: false,
                   include_lower: false
                 ]
               ]
             )

    # |> IO.inspect

    assert {:ok, _, _, _} =
             search(SearchTest, :index2,
               search_query: [
                 query: [
                   type: QueryType.range(),
                   field_name: "player",
                   from: "A",
                   to: "Z",
                   include_upper: false,
                   include_lower: false
                 ]
               ]
             )

    # |> IO.inspect
  end

  test "otto search bool query" do
    assert {:ok, _, _, _} =
             search(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.bool(),
                   must: [
                     [type: QueryType.range(), field_name: "player", from: "a", to: "z"]
                   ],
                   must_not: [
                     [type: QueryType.term(), field_name: "rank", term: 99]
                   ],
                   minimum_should_match: 2
                 ]
               ]
             )

    # |> IO.inspect

    assert {:ok, _, _, _} =
             search(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.bool(),
                   should: [
                     [type: QueryType.range(), field_name: "player", from: "a", to: "z"],
                     [type: QueryType.term(), field_name: "rank", term: 99]
                   ],
                   minimum_should_match: 2
                 ]
               ]
             )

    # |> IO.inspect
  end

  test "otto search wildcard query" do
    assert {:ok, _, _, _} =
             search(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.wildcard(),
                   field_name: "player",
                   value: "Ja*"
                 ]
               ]
             )

    # |> IO.inspect

    assert {:ok, _, _, _} =
             search(SearchTest, :index2,
               search_query: [
                 query: [
                   type: QueryType.wildcard(),
                   field_name: "player",
                   value: "Ja*"
                 ]
               ]
             )

    # |> IO.inspect
  end

  test "otto search nested query" do
    "* TODO * Add a nested query_field and test"
  end

  test "search iterate all query" do
    assert {:ok, _, _} =
             search_iterate_all(SearchTest, :index1,
               search_query: [
                 query: [
                   type: QueryType.match(),
                   field_name: "player",
                   text: "James Harden"
                 ],
                 limit: 2
               ]
             )

    # |> IO.inspect
  end
end
