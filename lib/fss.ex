defmodule FSS do
  @moduledoc """
  A small abstraction for filesystems.
  """

  defmodule Local do
    defmodule Entry do
      defstruct [:path]

      @type t :: %__MODULE__{path: String.t()}
    end
  end

  defmodule S3 do
    defmodule Config do
      defstruct [
        :access_key_id,
        :region,
        :secret_access_key,
        :endpoint,
        :token
      ]

      @type t :: %__MODULE__{
              access_key_id: String.t(),
              region: String.t(),
              secret_access_key: String.t(),
              endpoint: String.t() | nil,
              token: String.t() | nil
            }
    end

    defmodule Entry do
      defstruct [:bucket, :key, :port, :config]

      @type t :: %__MODULE__{
              bucket: String.t(),
              key: String.t(),
              port: pos_integer(),
              config: Config.t()
            }

      def config_from_system_env() do
        %FSS.S3.Config{
          access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
          secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
          region: System.get_env("AWS_REGION", System.get_env("AWS_DEFAULT_REGION")),
          token: System.get_env("AWS_SESSION_TOKEN")
        }
      end

      def parse(url, opts \\ []) do
        opts = Keyword.validate!(opts, config: nil)

        uri = URI.parse(url)

        case uri do
          %{scheme: "s3", host: host, path: "/" <> key} when is_binary(host) ->
            config =
              opts
              |> Keyword.fetch!(:config)
              |> case do
                nil ->
                  config_from_system_env()

                %Config{} = config ->
                  config

                config when is_list(config) or is_map(config) ->
                  struct!(config_from_system_env(), config)

                other ->
                  raise ArgumentError,
                        "expect configuration to be a %FSS.S3.Config{} struct, a keyword list or a map. Instead got #{inspect(other)}"
              end
              |> validate_config!()

            {:ok, %__MODULE__{bucket: host, key: key, port: uri.port, config: config}}

          _ ->
            {:error,
             ArgumentError.exception(
               "expected s3://<bucket>/<key> URL, got: " <>
                 URI.to_string(uri)
             )}
        end
      end

      defp validate_config!(%Config{} = config) do
        access = config.access_key_id
        secret = config.secret_access_key
        region = config.region

        if is_nil(access) or is_nil(secret) or is_nil(region) or
             access == "" or secret == "" or region == "" do
          raise "missing configuration keys or region to access the S3 API"
        end

        config
      end
    end
  end
end
