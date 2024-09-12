defmodule Explorer.Application do
  @moduledoc false
  use Application

  def start(_type, _args) do
    children = [
      {DynamicSupervisor, name: Explorer.Remote.Supervisor, strategy: :one_for_one},
      Explorer.Remote.LocalGC
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
