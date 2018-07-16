package commons

import (
	"database/sql"
	"encoding/json"

	"net/url"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/gorp.v1"
	"gopkg.in/mgo.v2"
)

const DIALECT_MYSQL string = "mysql"
const DIALECT_SQLITE3 string = "sqlite3"
const DIALECT_MONGO string = "mongodb"

type DBConnection struct {
	connectionURL string
	dbMap         *gorp.DbMap
}

var dbConnections map[string]DBConnection
var kConsumerConn sarama.Consumer
var kProducerConn sarama.AsyncProducer

var mongoConnectionUrl string
var mgoSession *mgo.Session

func GetMongoSession() (*mgo.Session, error) {
	if mgoSession == nil {
		var err error
		mgoSession, err = mgo.Dial(mongoConnectionUrl)
		if err != nil {
			return nil, err
		}
	}
	return mgoSession.Clone(), nil
}

func ConstructConnectionPool(dbConfigs map[string]DBConfigs) {
	dbConnections = make(map[string]DBConnection)
	var connectionURL string
	var dialect gorp.Dialect
	for dbName, dbConfig := range dbConfigs {
		switch dbConfig.Dialect {
		case DIALECT_MYSQL:
			connectionURL = dbConfig.Username + ":" + dbConfig.Password + "@tcp(" + dbConfig.Address + ")/" +
				dbConfig.DBName + dbConfig.Parameters
			dialect = gorp.MySQLDialect{"InnoDB", "UTF8"}
			break
		case DIALECT_SQLITE3:
			connectionURL = dbConfig.Address
			dialect = gorp.SqliteDialect{}
			break
		case DIALECT_MONGO:
			//mongoConnectionUrl = "mongodb://"+ dbConfig.Username+":"+ dbConfig.Password+"@"+dbConfig.Address
			mongoConnectionUrl = "mongodb://" + dbConfig.Username + ":" + url.QueryEscape(dbConfig.Password) + "@" + dbConfig.Address
			continue
		}
		db, err := sql.Open(dbConfig.Dialect, connectionURL)
		if err != nil {
			log.Error("Error occurred while constructing a the DB connection to : " + connectionURL +
				" with dialect:" + dbConfig.Dialect + " stack:" + err.Error())
		}
		dbConnections[dbName] = DBConnection{connectionURL, &gorp.DbMap{Db: db, Dialect: dialect}}
	}
}

func ConstructKafkaConnection(kafkaConfigs map[string]KafkaConfig) {
	for connType, kafkaConfig := range kafkaConfigs {
		switch connType {
		case "producer":
			config := sarama.NewConfig()
			// Return specifies what channels will be populated.
			// If they are set to true, you must read from
			// config.Producer.Return.Successes = true
			// The total number of times to retry sending a message (default 3).
			config.Producer.Retry.Max = kafkaConfig.MaxRetry
			// The level of acknowledgement reliability needed from the broker.
			config.Producer.RequiredAcks = sarama.WaitForAll
			brokers := kafkaConfig.Service
			producer, err := sarama.NewAsyncProducer(brokers, config)
			if err != nil {
				log.Error("Error occurred while constructing a the DB connection")
			}
			kProducerConn = producer
			break
		case "consumer":
			config := sarama.NewConfig()
			config.Producer.Retry.Max = kafkaConfig.MaxRetry
			config.Producer.RequiredAcks = sarama.WaitForAll
			brokers := kafkaConfig.Service
			consumer, err := sarama.NewConsumer(brokers, config)
			if err != nil {
				log.Error("Error occurred while constructing")
			}
			kConsumerConn = consumer
			break
		}
	}
}

type NullString struct {
	sql.NullString
}

func (r NullString) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String)
}

func (r *NullString) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	r.Valid = true
	return json.Unmarshal(data, (*string)(&r.String))
}

/* NullInt*/
type NullInt64 struct {
	sql.NullInt64
}

func (r NullInt64) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Int64)
}

func (r *NullInt64) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	r.Valid = true
	return json.Unmarshal(data, (*int64)(&r.Int64))
}

/* NullFloat64*/
type NullFloat64 struct {
	sql.NullFloat64
}

func (r NullFloat64) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Float64)
}

func (r *NullFloat64) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	r.Valid = true
	return json.Unmarshal(data, (*float64)(&r.Float64))
}
