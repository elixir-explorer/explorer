defmodule Filesystem do
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
      defstruct [:region, :access_key_id, :secret_access_key]

      @type t :: %__MODULE__{
              region: String.t(),
              access_key_id: String.t(),
              secret_access_key: String.t()
            }
    end

    defmodule Entry do
      defstruct [:url, :config]

      @type t :: %__MODULE__{url: String.t(), config: Config.t()}
    end
  end
end
