package mysql

import (
        "os"
        "log"
        "database/sql"
        _ "github.com/go-sql-driver/mysql"
)

func InitDatabase() (*sql.DB) {
        var err error
        database := os.Getenv("SEARCH_BACKEND_MYSQL")
        if database == "" {
                database = "root:root@tcp(127.0.0.1:3306)/domeos?loc=Local&parseTime=true"
        }
        DB, err := sql.Open("mysql", database)
        if err != nil {
                log.Fatalln("open db fail:", err)
                return nil
        }

        DB.SetMaxIdleConns(100)

        err = DB.Ping()

        if err != nil {
                log.Fatalln("ping db fail:", err)
                return nil
        }

        return DB
}
