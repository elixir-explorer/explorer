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
        opts =
          opts
          |> Keyword.validate!([:config])
          |> Keyword.put_new_lazy(:config, fn -> config_from_system_env() end)

        uri = URI.parse(url)

        if uri.scheme == "s3" do
          case uri do
            %{host: host, path: "/" <> key} when is_binary(host) ->
              {:ok, %__MODULE__{bucket: host, key: key, port: uri.port, config: opts[:config]}}

            %{host: nil} ->
              {:error, "host is required"}

            %{path: nil} ->
              {:error, "path to the resource is required"}
          end
        else
          {:error, "only s3:// URIs are supported for now"}
        end
      end
    end
  end
end
