alias Adbc.{Database, Connection}
Adbc.download_driver!(:sqlite)

{:ok, db} = Database.start_link(driver: :sqlite)
{:ok, conn} = Connection.start_link(database: db)

{:ok, _} = Connection.query(conn, "CREATE TABLE IF NOT EXISTS foo (col)")
{:ok, _} = Connection.query(conn, "INSERT INTO foo VALUES (?)", [:rand.uniform(1000)])

{:ok, _} = Explorer.DataFrame.from_query(conn, "SELECT * FROM foo") |> dbg()
