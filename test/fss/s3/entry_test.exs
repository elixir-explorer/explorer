defmodule FSS.S3.EntryTest do
  use ExUnit.Case, async: true
  alias FSS.S3.Config
  alias FSS.S3.Entry

  describe "parse/2" do
    setup do
      default_config = %Config{
        secret_access_key: "my-secret",
        access_key_id: "my-access",
        region: "us-west-2"
      }

      {:ok, config: default_config}
    end

    test "parses a s3:// style uri", %{config: config} do
      assert {:ok, %Entry{bucket: "my-bucket", key: "my-file.png", config: %Config{} = config}} =
               Entry.parse("s3://my-bucket/my-file.png", config: config)

      assert is_nil(config.endpoint)
      assert config.secret_access_key == "my-secret"
      assert config.access_key_id == "my-access"
      assert config.region == "us-west-2"
    end

    test "parses a s3:// style uri containing a port", %{config: config} do
      assert {:ok,
              %Entry{
                port: 4562,
                bucket: "my-bucket",
                key: "my-file.png",
                config: %Config{} = ^config
              }} =
               Entry.parse("s3://my-bucket:4562/my-file.png", config: config)
    end

    test "accepts a config as a keyword list" do
      assert {:ok, %Entry{config: %Config{} = config}} =
               Entry.parse("s3://my-bucket/my-file.png",
                 config: [
                   endpoint: "localhost",
                   secret_access_key: "my-secret-1",
                   access_key_id: "my-access-key-1",
                   region: "eu-east-1"
                 ]
               )

      assert config.endpoint == "localhost"
      assert config.secret_access_key == "my-secret-1"
      assert config.access_key_id == "my-access-key-1"
      assert config.region == "eu-east-1"
    end

    test "accepts a config as a map" do
      assert {:ok, %Entry{config: %Config{} = config}} =
               Entry.parse("s3://my-bucket/my-file.png",
                 config: %{
                   endpoint: "localhost",
                   secret_access_key: "my-secret-1",
                   access_key_id: "my-access-key-1",
                   region: "eu-east-1"
                 }
               )

      assert config.endpoint == "localhost"
      assert config.secret_access_key == "my-secret-1"
      assert config.access_key_id == "my-access-key-1"
      assert config.region == "eu-east-1"
    end

    test "does not parse an invalid s3 uri using the s3:// schema" do
      assert {:error,
              ArgumentError.exception(
                "expected s3://<bucket>/<key> URL, got: s3://my-bucket-my-file.png"
              )} ==
               Entry.parse("s3://my-bucket-my-file.png")
    end

    test "does not parse a valid s3 uri using the http(s):// schema" do
      assert {:error,
              ArgumentError.exception(
                "expected s3://<bucket>/<key> URL, got: https://my-bucket.not-s3.somethig.com/my-file.png"
              )} ==
               Entry.parse("https://my-bucket.not-s3.somethig.com/my-file.png")
    end

    test "raise error when missing access key id" do
      assert_raise ArgumentError,
                   "missing :access_key_id for FSS.S3 (set the key or the AWS_ACCESS_KEY_ID env var)",
                   fn ->
                     Entry.parse("s3://my-bucket/my-file.png")
                   end
    end

    test "raise error when missing secret key id" do
      assert_raise ArgumentError,
                   "missing :secret_access_key for FSS.S3 (set the key or the AWS_SECRET_ACCESS_KEY env var)",
                   fn ->
                     Entry.parse("s3://my-bucket/my-file.png", config: [access_key_id: "my-key"])
                   end
    end

    test "raise error when missing region" do
      assert_raise ArgumentError,
                   "missing :region for FSS.S3 (set the key or the AWS_REGION env var)",
                   fn ->
                     Entry.parse("s3://my-bucket/my-file.png",
                       config: [access_key_id: "my-key", secret_access_key: "my-secret"]
                     )
                   end
    end

    test "raise error when config is not valid" do
      assert_raise ArgumentError,
                   "expect configuration to be a %FSS.S3.Config{} struct, a keyword list or a map. Instead got 42",
                   fn ->
                     Entry.parse("s3://my-bucket/my-file.png", config: 42)
                   end
    end
  end
end
