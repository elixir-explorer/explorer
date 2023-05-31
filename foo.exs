alias Adbc.Database
alias Adbc.Connection
alias Adbc.Statement

{:ok, database} = Database.new()
:ok = Database.set_option(database, "driver", "adbc_driver_sqlite")
:ok = Database.set_option(database, "uri", "file:my_db.db")
:ok = Database.init(database)
{:ok, connection} = Connection.new()
:ok = Connection.init(connection, database)
{:ok, statement} = Statement.new(connection)

Statement.set_sql_query(statement, "CREATE TABLE IF NOT EXISTS foo (col)")
:ok = Statement.prepare(statement)
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

Statement.set_sql_query(statement, "INSERT INTO foo VALUES (#{:rand.uniform(1000)})")
:ok = Statement.prepare(statement)
{:ok, stream, _row_affected} = Statement.execute_query(statement)

Statement.set_sql_query(statement, "SELECT * FROM foo")
:ok = Statement.prepare(statement)
{:ok, stream, _row_affected} = Statement.execute_query(statement)

func_ptr = Adbc.get_function_pointer("adbc_arrow_array_stream_donate")
resource_ptr = Adbc.Nif.adbc_arrow_array_stream_get_nif_resource_pointer(stream.reference) |> IO.inspect

IO.inspect Explorer.PolarsBackend.Native.df_experiment(func_ptr, resource_ptr, stream)
IO.inspect Explorer.PolarsBackend.Native.df_experiment(func_ptr, resource_ptr, stream)