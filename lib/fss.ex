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
        :token
      ]

      @type t :: %__MODULE__{
              access_key_id: String.t(),
              region: String.t(),
              secret_access_key: String.t(),
              token: String.t() | nil
            }
    end

    defmodule Entry do
      defstruct [:url, :config]

      @type t :: %__MODULE__{url: String.t(), config: Config.t()}
    end
  end
end
