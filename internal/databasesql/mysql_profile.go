package databasesql

import (
	"net"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

type MySQLConnectionConfig struct {
	ConnectionString string
	Username         string
	Password         string
	Host             string
	Port             int
	Database         string
	SSLMode          string
	Timeout          time.Duration
}

func BuildMySQLDSN(config MySQLConnectionConfig) (string, error) {
	var driverConfig *mysqldriver.Config
	var err error
	if raw := strings.TrimSpace(config.ConnectionString); raw != "" {
		driverConfig, err = mysqldriver.ParseDSN(raw)
		if err != nil {
			return "", err
		}
	} else {
		driverConfig = mysqldriver.NewConfig()
		driverConfig.User = strings.TrimSpace(config.Username)
		driverConfig.Passwd = config.Password
		driverConfig.Net = "tcp"
		driverConfig.Addr = net.JoinHostPort(strings.TrimSpace(config.Host), strconv.Itoa(config.Port))
		driverConfig.DBName = strings.TrimSpace(config.Database)
		driverConfig.TLSConfig = map[string]string{"disable": "false", "preferred": "preferred", "skip-verify": "skip-verify", "require": "true"}[config.SSLMode]
	}
	driverConfig.MultiStatements = false
	driverConfig.AllowAllFiles = false
	driverConfig.AllowCleartextPasswords = false
	driverConfig.ParseTime = true
	driverConfig.Timeout = config.Timeout
	driverConfig.ReadTimeout = config.Timeout
	driverConfig.WriteTimeout = config.Timeout
	params := map[string]string{}
	for key, value := range driverConfig.Params {
		params[key] = value
	}
	params["transaction_read_only"] = "ON"
	params["sql_safe_updates"] = "1"
	driverConfig.Params = params
	driverConfig.Loc = time.UTC
	return driverConfig.FormatDSN(), nil
}
