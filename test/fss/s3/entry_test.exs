defmodule FSS.S3.EntryTest do
  use ExUnit.Case, async: true
  alias FSS.S3.Config
  alias FSS.S3.Entry

  describe "parse/2" do
    test "parses a s3:// style uri" do
      assert {:ok, %Entry{bucket: "my-bucket", key: "my-file.png", config: %Config{} = config}} =
               Entry.parse("s3://my-bucket/my-file.png")

      assert config.endpoint == "amazonaws.com"
      assert is_nil(config.secret_access_key)
      assert is_nil(config.access_key_id)
      assert is_nil(config.region)
    end

    test "does not parse an invalid s3 uri using the s3:// schema" do
      assert {:error, "path to the resource is required"} ==
               Entry.parse("s3://my-bucket-my-file.png")
    end

    test "parses an http(s):// style uri with bucket on path and default host" do
      assert {:ok, %Entry{bucket: "my-bucket", key: "my-file.png", config: %Config{} = config}} =
               Entry.parse("https://s3.us-west-2.amazonaws.com/my-bucket/my-file.png")

      assert config.endpoint == "amazonaws.com"
      assert is_nil(config.secret_access_key)
      assert is_nil(config.access_key_id)

      assert config.region == "us-west-2"
    end

    test "parses an http(s):// style uri with bucket on path and modified endpoint" do
      assert {:ok, %Entry{bucket: "my-bucket", key: "my-file.png", config: %Config{} = config}} =
               Entry.parse("https://s3.eu-west-6.minio.co.uk/my-bucket/my-file.png")

      assert config.endpoint == "minio.co.uk"
      assert is_nil(config.secret_access_key)
      assert is_nil(config.access_key_id)

      assert config.region == "eu-west-6"
    end

    test "parses an http(s):// style uri with bucket on host and default endpoint" do
      assert {:ok, %Entry{bucket: "my-bucket", key: "my-file.png", config: %Config{} = config}} =
               Entry.parse("https://my-bucket.s3.us-west-2.amazonaws.com/my-file.png")

      assert config.endpoint == "amazonaws.com"
      assert is_nil(config.secret_access_key)
      assert is_nil(config.access_key_id)

      assert config.region == "us-west-2"
    end

    test "parses an http(s):// style uri with bucket on host and modified endpoint" do
      assert {:ok, %Entry{bucket: "my-bucket", key: "my-file.png", config: %Config{} = config}} =
               Entry.parse("https://my-bucket.s3.nyc3.digitaloceanspaces.com/my-file.png")

      assert config.endpoint == "digitaloceanspaces.com"
      assert is_nil(config.secret_access_key)
      assert is_nil(config.access_key_id)

      assert config.region == "nyc3"
    end

    test "does not parse an invalid s3 uri using the http(s):// schema" do
      assert {:error,
              "cannot read a valid S3 URI from \"https://my-bucket.not-s3.somethig.com/my-file.png\""} ==
               Entry.parse("https://my-bucket.not-s3.somethig.com/my-file.png")
    end
  end
end
