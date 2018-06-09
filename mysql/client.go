package mysql

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
	"gopkg.in/birkirb/loggers.v1/log"
)

type ClientConfig struct {
	Addr     string
	User     string
	Password string
	Database string
	Charset  string
}

type Client struct {
	db *sql.DB
}

func NewClient(conf *ClientConfig) (*Client, error) {
	mysqlConnStr := conf.User + ":" + conf.Password + "@tcp(" + conf.Addr + ")/" + conf.Database
	if conf.Charset != "" {
		mysqlConnStr += "?charset=" + conf.Charset
	}
	mysql, err := sql.Open("mysql", mysqlConnStr)
	if err != nil {
		return nil, err
	}

	return &Client{
		db: mysql,
	}, nil
}

func (c *Client) Exec(s string) error {
	// if s != "" {
	// 	log.Infof("Exec ----> %s", s)
	// 	return nil
	// }
	_, err := c.db.Exec(s)
	if err != nil {
		mysqlerr, ok := err.(*mysql.MySQLError)
		if ok && mysqlerr.Number == 1062 {
			log.Infof("insert duplicate ----> %s", s)
			return nil
		} else {
			return err
		}
	}

	return nil
}
