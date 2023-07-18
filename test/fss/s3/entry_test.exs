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

    test "parses a s3:// style uri containing a port" do
      assert {:ok,
              %Entry{
                port: 4562,
                bucket: "my-bucket",
                key: "my-file.png",
                config: %Config{} = config
              }} =
               Entry.parse("s3://my-bucket:4562/my-file.png")

      assert config.endpoint == "amazonaws.com"
    end

    test "does not parse an invalid s3 uri using the s3:// schema" do
      assert {:error, "path to the resource is required"} ==
               Entry.parse("s3://my-bucket-my-file.png")
    end

    test "does not parse a valid s3 uri using the http(s):// schema" do
      assert {:error, "only s3:// URIs are supported for now"} =
               Entry.parse("https://my-bucket.not-s3.somethig.com/my-file.png")
    end
  end
end
