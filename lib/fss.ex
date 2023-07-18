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
        {:endpoint, "amazonaws.com"},
        :token
      ]

      @type t :: %__MODULE__{
              access_key_id: String.t(),
              endpoint: String.t(),
              region: String.t(),
              secret_access_key: String.t(),
              token: String.t() | nil
            }

      def from_system_env() do
        %__MODULE__{
          access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
          secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
          region: System.get_env("AWS_REGION", System.get_env("AWS_DEFAULT_REGION")),
          token: System.get_env("AWS_SESSION_TOKEN")
        }
      end
    end

    defmodule Entry do
      defstruct [:bucket, :key, :port, :config]

      @type t :: %__MODULE__{
              bucket: String.t(),
              key: String.t(),
              port: pos_integer(),
              config: Config.t()
            }

      def parse(url, opts \\ []) do
        opts = 
          opts
          |> Keyword.validate!(opts, [:config])
          |> Keyword.put_new_lazy(:config, fn -> FSS.S3.Config.from_system_env() end)
        uri = URI.parse(url)

        cond do
          uri.scheme == "s3" ->
            case uri do
              %{host: host, path: "/" <> key} when is_binary(host) ->
                {:ok, %__MODULE__{bucket: host, key: key, port: uri.port, config: opts[:config]}}

              %{host: nil} ->
                {:error, "host is required"}

              %{path: nil} ->
                {:error, "path to the resource is required"}
            end

          uri.scheme in ["http", "https"] ->
            parts =
              case String.split(uri.host, ".") do
                ["s3", region | tail] ->
                  "/" <> path = uri.path
                  [bucket, key] = String.split(path, "/", parts: 2)
                  endpoint = Enum.join(tail, ".")

                  {:ok, [bucket: bucket, key: key, region: region, endpoint: endpoint]}

                [bucket, "s3", region | tail] ->
                  "/" <> key = uri.path
                  endpoint = Enum.join(tail, ".")

                  {:ok, [bucket: bucket, key: key, region: region, endpoint: endpoint]}

                _ ->
                  {:error, "cannot read a valid S3 URI from #{inspect(url)}"}
              end

            with {:ok, parts} <- parts do
              config = %FSS.S3.Config{
                opts[:config]
                | region: parts[:region],
                  endpoint: parts[:endpoint]
              }

              {:ok,
               %__MODULE__{
                 bucket: parts[:bucket],
                 key: parts[:key],
                 port: uri.port,
                 config: config
               }}
            end

          true ->
            {:error, :not_supported}
        end
      end
    end
  end
end
