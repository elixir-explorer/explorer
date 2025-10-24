defmodule Explorer.FSSTest do
  use ExUnit.Case, async: true

  alias Explorer.FSS

  describe "parse_http/2" do
    test "parses a valid URL" do
      url = "https://github.com/elixir-explorer/explorer"
      assert {:ok, {:http, ^url, config}} = FSS.parse_http(url)

      assert config == %{headers: []}
    end

    test "returns an error with invalid headers" do
      url = "http://localhost:9899/path/to/file.csv"

      assert {:error, error} = FSS.parse_http(url, config: [headers: [{"authorization", 42}]])

      assert error ==
               ArgumentError.exception(
                 "headers must be a list of {key, value} tuples where both are strings, got: [{\"authorization\", 42}]"
               )
    end

    test "returns an error with invalid key" do
      url = "http://localhost:9899/path/to/file.csv"

      assert {:error, error} = FSS.parse_http(url, config: [auth: {:bearer, "token"}])

      assert error ==
               ArgumentError.exception(
                 "the keys [:auth] are not valid keys for the HTTP configuration"
               )
    end

    test "returns an error with invalid opts" do
      url = "http://localhost:9899/path/to/file.csv"
      assert {:error, error} = FSS.parse_http(url, config: 54)

      assert error ==
               ArgumentError.exception(
                 "config for HTTP entry is invalid. Expecting a map or keyword list with :headers, got 54"
               )
    end
  end

  describe "parse_local/1" do
    test "parses a path" do
      path = "/home/joe/file.txt"
      assert {:ok, {:local, ^path, config}} = FSS.parse_local(path)
      assert config == %{}
    end
  end

  describe "parse_s3/2" do
    setup do
      default_config = %{
        secret_access_key: "my-secret",
        access_key_id: "my-access",
        region: "us-west-2"
      }

      {:ok, config: default_config}
    end

    test "parses a s3:// style uri", %{config: config} do
      assert {:ok, {:s3, "my-file.png", parsed_config}} =
               FSS.parse_s3("s3://my-bucket/my-file.png", config: config)

      assert parsed_config.endpoint == "https://my-bucket.s3.us-west-2.amazonaws.com"
      assert parsed_config.bucket == nil
      assert parsed_config.secret_access_key == "my-secret"
      assert parsed_config.access_key_id == "my-access"
      assert parsed_config.region == "us-west-2"
    end

    test "parses a s3:// style uri with bucket name containing dots", %{config: config} do
      assert {:ok, {:s3, "my-file.png", parsed_config}} =
               FSS.parse_s3("s3://my.bucket.with.dots/my-file.png", config: config)

      assert parsed_config.endpoint == "https://s3.us-west-2.amazonaws.com"
      assert parsed_config.bucket == "my.bucket.with.dots"
      assert parsed_config.secret_access_key == "my-secret"
      assert parsed_config.access_key_id == "my-access"
      assert parsed_config.region == "us-west-2"
    end

    test "parses a s3:// style uri without bucket name but passing endpoint", %{config: config} do
      assert {:ok, {:s3, "my-file.png", parsed_config}} =
               FSS.parse_s3("s3:///my-file.png",
                 config:
                   Map.put(config, :endpoint, "https://my-custom-endpoint.example.com/my-custom-bucket")
               )

      assert parsed_config.endpoint == "https://my-custom-endpoint.example.com/my-custom-bucket"
      assert parsed_config.bucket == nil
      assert parsed_config.secret_access_key == "my-secret"
      assert parsed_config.access_key_id == "my-access"
      assert parsed_config.region == "us-west-2"
    end

    test "does not parse a s3:// style uri without bucket and without an endpoint", %{
      config: config
    } do
      assert {:error, error} =
               FSS.parse_s3("s3:///my-file.png", config: Map.put(config, :endpoint, nil))

      assert error == ArgumentError.exception("endpoint is required when bucket is nil")
    end

    test "accepts a config as a keyword list" do
      assert {:ok, {:s3, _key, parsed_config}} =
               FSS.parse_s3("s3://my-bucket/my-file.png",
                 config: [
                   endpoint: "localhost",
                   secret_access_key: "my-secret-1",
                   access_key_id: "my-access-key-1",
                   region: "eu-east-1"
                 ]
               )

      assert parsed_config.endpoint == "localhost"
      assert parsed_config.bucket == "my-bucket"
      assert parsed_config.secret_access_key == "my-secret-1"
      assert parsed_config.access_key_id == "my-access-key-1"
      assert parsed_config.region == "eu-east-1"
    end

    test "accepts a config as a map" do
      assert {:ok, {:s3, _key, parsed_config}} =
               FSS.parse_s3("s3://my-bucket/my-file.png",
                 config: %{
                   endpoint: "localhost",
                   secret_access_key: "my-secret-1",
                   access_key_id: "my-access-key-1",
                   # We always ignore bucket from config.
                   bucket: "random-name",
                   region: "eu-east-1"
                 }
               )

      assert parsed_config.endpoint == "localhost"
      assert parsed_config.bucket == "my-bucket"
      assert parsed_config.secret_access_key == "my-secret-1"
      assert parsed_config.access_key_id == "my-access-key-1"
      assert parsed_config.region == "eu-east-1"
    end

    test "does not parse an invalid s3 uri using the s3:// schema" do
      assert {:error,
              ArgumentError.exception(
                "expected s3://<bucket>/<key> URL, got: s3://my-bucket-my-file.png"
              )} ==
               FSS.parse_s3("s3://my-bucket-my-file.png")
    end

    test "does not parse a valid s3 uri using the http(s):// schema" do
      assert {:error,
              ArgumentError.exception(
                "expected s3://<bucket>/<key> URL, got: https://my-bucket.not-s3.somethig.com/my-file.png"
              )} ==
               FSS.parse_s3("https://my-bucket.not-s3.somethig.com/my-file.png")
    end

    test "raise error when missing access key id" do
      assert_raise ArgumentError,
                   "missing :access_key_id for S3 (set the key or the AWS_ACCESS_KEY_ID env var)",
                   fn ->
                     FSS.parse_s3("s3://my-bucket/my-file.png")
                   end
    end

    test "raise error when missing secret key id" do
      assert_raise ArgumentError,
                   "missing :secret_access_key for S3 (set the key or the AWS_SECRET_ACCESS_KEY env var)",
                   fn ->
                     FSS.parse_s3("s3://my-bucket/my-file.png", config: [access_key_id: "my-key"])
                   end
    end

    test "raise error when missing region" do
      assert_raise ArgumentError,
                   "missing :region for S3 (set the key or the AWS_REGION env var)",
                   fn ->
                     FSS.parse_s3("s3://my-bucket/my-file.png",
                       config: [access_key_id: "my-key", secret_access_key: "my-secret"]
                     )
                   end
    end

    test "raise error when config is not valid" do
      assert_raise ArgumentError,
                   "expect S3 configuration to be a map or keyword list. Instead got 42",
                   fn ->
                     FSS.parse_s3("s3://my-bucket/my-file.png", config: 42)
                   end
    end
  end
end
