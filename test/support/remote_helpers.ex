defmodule Explorer.RemoteHelpers do
  defmacro remote_eval(node, binding \\ [], do: block) do
    quote do
      :erpc.call(unquote(node), Code, :eval_quoted, [
        unquote(Macro.escape(block)),
        unquote(binding)
      ])
    end
  end

  # This function keeps remote dataframes in the node forever.
  # But those are cheap, so it is fine.
  def keep(term) do
    Agent.start(fn -> term end)
    term
  end
end
