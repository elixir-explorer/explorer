defmodule FSS.HttpTest do
  use ExUnit.Case, async: true

  alias FSS.HTTP

  describe "parse/2" do
    test "parses a valid URL" do
      url = "https://github.com/elixir-explorer/explorer"
      assert {:ok, entry} = HTTP.parse(url)

      assert %HTTP.Entry{url: ^url, config: %HTTP.Config{headers: []}} = entry
    end

    test "returns an error with invalid headers" do
      url = "http://localhost:9899/path/to/file.csv"

      assert {:error, error} = HTTP.parse(url, config: [headers: [{"authorization", 42}]])

      assert error ==
               ArgumentError.exception(
                 "one of the headers is invalid. Expecting a list of `{\"key\", \"value\"}`, but got: [{\"authorization\", 42}]"
               )
    end

    test "returns an error with invalid key" do
      url = "http://localhost:9899/path/to/file.csv"

      assert {:error, error} = HTTP.parse(url, config: [auth: {:bearer, "token"}])

      assert error ==
               ArgumentError.exception(
                 "the keys [:auth] are not valid keys for the HTTP configuration"
               )
    end

    test "returns an error with invalid opts" do
      url = "http://localhost:9899/path/to/file.csv"
      assert {:error, error} = HTTP.parse(url, config: 54)

      assert error ==
               ArgumentError.exception(
                 "config for HTTP entry is invalid. Expecting `:headers`, but got 54"
               )
    end
  end
end
