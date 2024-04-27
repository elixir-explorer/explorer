import Config

config :explorer, :check_polars_frames, true
config :adbc, :drivers, [:sqlite]
config :elixir, :time_zone_database, Tz.TimeZoneDatabase
