package commons

import (
	"os"
	"path/filepath"
	"strconv"
	"text/template"

	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"gopkg.in/gorp.v1"
)

type serverConfigs struct {
	Home               string
	Prefix             string
	IsMaster           bool
	PortOffset         int
	Hostname           string
	HttpPort           int
	HttpsPort          int
	CaddyPort          int
	ReadTimeOut        int
	WriteTimeOut       int
	CaddyPath          string
	CaddyFile          string
	SSLCertificateFile string
	SSLKeyFile         string
	JWTPrivateKeyFile  string
	JWTPublicKeyFile   string
	JWTExpirationDelta int
	TraceLogFile       string
	EnableTrace        bool
	EnableAccessLogs   bool
	LogsDirectory      string
	LogLevel           string
	DBConfigMap        map[string]DBConfigs
	KafkaConfigs       map[string]KafkaConfig
	ConfigMap          map[string]interface{}
	RedisConfigs       RedisConfigs
	ExternalServices   map[string]ExternalServicesConfigs
	TenantConfigs      TenantConfigsInfo
}

type KafkaConfig struct {
	Service  []string
	MaxRetry int
}

type DBConfigs struct {
	Username   string
	Password   string
	Dialect    string
	DBName     string
	Address    string
	Parameters string
}

type ExternalServicesConfigs struct {
	Service string
	Path    string
}

type TenantConfigsInfo struct {
	DefaultRoles []string
}

type RedisConfigs struct {
	Address  string
	Password string
}

var ServerConfigurations serverConfigs

func init() {
	InitConfigurations(os.Getenv(CONFIG_FILE))
}

func GetDBConnection(dbIdentifier string) *gorp.DbMap {
	return dbConnections[dbIdentifier].dbMap
}

func GetKafkaProducerConn() sarama.AsyncProducer {
	return kProducerConn
}

func GetKafkaConsumerConn() sarama.Consumer {
	return kConsumerConn
}

func (config *serverConfigs) GetString(identifier string) string {
	return (*config).ConfigMap[identifier].(string)
}

func FetchFromVenues(request *http.Request, database string, table string, totalRecordCountQuery string,
	columns []string, columnsMap map[string]string, result interface{}, tenantID int,
	venueIDs []int) (int64, int64, error) {
	dbMap := GetDBConnection(database)
	var err error
	query := "SELECT "

	for index, element := range columns {
		query += element
		if index+1 != len(columns) {
			query += ","
		}
	}
	constructedFilterQuery := ""
	constructedFilterQuery += " FROM " + table
	filter := filter(request, columns, columnsMap)

	if tenantID > 0 {
		if len(filter) > 0 {
			filter += " AND tenantid=" + strconv.Itoa(tenantID)
		} else {
			filter += " WHERE tenantid=" + strconv.Itoa(tenantID)
		}
	}

	if len(venueIDs) > 0 && venueIDs[0] != -1 {
		filter += " AND venueid IN ("
		for k, v := range venueIDs {
			filter += strconv.Itoa(v)
			if k < len(venueIDs)-1 {
				filter += ","
			} else if k == len(venueIDs)-1 {
				filter += ")"
			}
		}
	}

	constructedFilterQuery += filter
	query += constructedFilterQuery
	query += order(request, columns)
	query += limit(request)

	_, err = dbMap.Select(result, query)

	filteredRecordCount, _ := getRecordCount(dbMap, "SELECT COUNT(*) "+constructedFilterQuery)
	totalRecordsCount, _ := getRecordCount(dbMap, totalRecordCountQuery)
	return filteredRecordCount, totalRecordsCount, err
}

func InitConfigurations(configFileUrl string) serverConfigs {
	ServerConfigurations.Home = GetServerHome()
	//read the configurations from the file url instead of searching through the paths
	if len(configFileUrl) <= 0 {
		if _, err := os.Stat(ServerConfigurations.Home + FILE_PATH_SEPARATOR + SERVER_CONFIGS_DIRECTORY + FILE_PATH_SEPARATOR + CONFIG_FILE_NAME); os.IsNotExist(err) {
			configFileUrl = ServerConfigurations.Home + FILE_PATH_SEPARATOR + "configs" + FILE_PATH_SEPARATOR + DEFAULT_CONFIG_FILE_NAME
		} else {
			configFileUrl = ServerConfigurations.Home + FILE_PATH_SEPARATOR + "configs" + FILE_PATH_SEPARATOR + CONFIG_FILE_NAME
		}
	}
	viper.New()
	configUrl, err := parseConfigTemplate(configFileUrl, ServerConfigurations.Home)
	if err != nil {
		log.Fatalf("unable to initialize configurations stac trace: %s", err.Error())
	}
	viper.SetConfigFile(configUrl)
	err = viper.ReadInConfig() // Find and read the config file
	if err != nil {
		log.Fatalf("error while reading server configuration file: %s err: %s \n", configFileUrl, err)
	}

	configsMap := viper.GetStringMap("serverConfigs")
	ServerConfigurations.ConfigMap = configsMap
	ServerConfigurations.Prefix = configsMap["prefix"].(string)
	SERVER_PREFIX := ServerConfigurations.Prefix
	ServerConfigurations.IsMaster = configsMap["isMaster"].(bool)
	ServerConfigurations.PortOffset = configsMap["portOffset"].(int)
	ServerConfigurations.HttpPort = configsMap["httpPort"].(int)
	ServerConfigurations.HttpsPort = configsMap["httpsPort"].(int)
	ServerConfigurations.CaddyPort = configsMap["caddyPort"].(int)
	ServerConfigurations.ReadTimeOut = configsMap["readTimeOut"].(int)
	ServerConfigurations.WriteTimeOut = configsMap["writeTimeOut"].(int)
	ServerConfigurations.LogsDirectory = configsMap["logsDirectory"].(string)
	ServerConfigurations.LogLevel = configsMap["logLevel"].(string)
	ServerConfigurations.EnableAccessLogs = configsMap["enableAccessLogs"].(bool)
	ServerConfigurations.CaddyPath = configsMap["caddyPath"].(string)
	ServerConfigurations.CaddyFile = configsMap["caddyFile"].(string)
	ServerConfigurations.JWTPrivateKeyFile = configsMap["JWTPrivateKeyFile"].(string)
	ServerConfigurations.JWTPublicKeyFile = configsMap["JWTPublicKeyFile"].(string)
	ServerConfigurations.JWTExpirationDelta = configsMap["JWTExpirationDelta"].(int)
	ServerConfigurations.SSLCertificateFile = configsMap["certificateFile"].(string)
	ServerConfigurations.SSLKeyFile = configsMap["keyFile"].(string)

	//Exporting variables for other services (Caddy)
	os.Setenv("PATH", os.Getenv("PATH")+":"+ServerConfigurations.Home+"/bin")
	os.Setenv("CADDYPATH", ServerConfigurations.CaddyPath)
	os.Setenv(SERVER_PREFIX+"_CADDY_PORT", strconv.Itoa(ServerConfigurations.CaddyPort+ServerConfigurations.PortOffset))
	os.Setenv(SERVER_PREFIX+"_HTTPS_PORT", strconv.Itoa(ServerConfigurations.HttpsPort+ServerConfigurations.PortOffset))
	os.Setenv(SERVER_PREFIX+"_CERTIFICATE_FILE", ServerConfigurations.SSLCertificateFile)
	os.Setenv(SERVER_PREFIX+"_KEY_FILE", ServerConfigurations.SSLKeyFile)
	os.Setenv(SERVER_PREFIX+"_"+JWT_PRIVATE_KEY_FILE, ServerConfigurations.JWTPrivateKeyFile)
	os.Setenv(SERVER_PREFIX+"_"+JWT_PUBLIC_KEY_FILE, ServerConfigurations.JWTPublicKeyFile)
	os.Setenv(SERVER_PREFIX+"_"+JWT_EXPIRATION_DELTA, strconv.Itoa(ServerConfigurations.JWTExpirationDelta))

	ServerConfigurations.DBConfigMap = make(map[string]DBConfigs)
	databases := viper.Get("dbConfigs").([]interface{})
	for i, _ := range databases {
		database := databases[i].(map[interface{}]interface{})
		ServerConfigurations.DBConfigMap[database["name"].(string)] = DBConfigs{
			Dialect:    database["dialect"].(string),
			DBName:     database["dbname"].(string),
			Address:    database["address"].(string),
			Parameters: database["parameters"].(string),
			Username:   database["username"].(string),
			Password:   database["password"].(string),
		}
	}

	redisConfigsMap := viper.GetStringMap("redisConfigs")
	ServerConfigurations.RedisConfigs.Address = redisConfigsMap["address"].(string)
	ServerConfigurations.RedisConfigs.Password = redisConfigsMap["password"].(string)

	//Exporting variables for other external services Ex: SAP, PMS, HAVC
	ServerConfigurations.ExternalServices = make(map[string]ExternalServicesConfigs)
	var exConfigs = viper.Get("exConfigs")
	if exConfigs != nil {
		fmt.Println("test2")
		services := exConfigs.([]interface{})
		for i, _ := range services {
			service := services[i].(map[interface{}]interface{})
			ServerConfigurations.ExternalServices[service["service"].(string)] = ExternalServicesConfigs{
				Service: service["service"].(string),
				Path:    service["path"].(string),
			}
		}
	}

	ServerConfigurations.KafkaConfigs = make(map[string]KafkaConfig)
	kafkaConfigs := viper.Get("kafkaConfigs").([]interface{})
	if len(kafkaConfigs) > 0 {
		fmt.Println(kafkaConfigs)
		for i, _ := range kafkaConfigs {
			configs := kafkaConfigs[i].(map[interface{}]interface{})
			t := configs["services"].([]interface{})
			s := make([]string, len(t))
			for i, v := range t {
				s[i] = v.(string)
			}
			ServerConfigurations.KafkaConfigs[configs["name"].(string)] = KafkaConfig{
				Service:  s,
				MaxRetry: configs["maxRetry"].(int),
			}
		}
	}

	var tenantConfigs = viper.GetStringMap("tenantConfigs")
	fmt.Println(len(tenantConfigs))
	if ServerConfigurations.Prefix == "POLICY" {
		if len(tenantConfigs) > 0 {
			fmt.Println("test1")
			t := tenantConfigs["defaultRoles"].([]interface{})
			s := make([]string, len(t))
			for i, v := range t {
				s[i] = v.(string)
			}
			ServerConfigurations.TenantConfigs.DefaultRoles = s
		}
	}
	return ServerConfigurations
}

//fill the configuration file template with the the template parameters
func parseConfigTemplate(configFileUrl, serverHome string) (string, error) {
	parsedConfigFolder := filepath.FromSlash(ServerConfigurations.Home + FILE_PATH_SEPARATOR + "configs" +
		FILE_PATH_SEPARATOR + ".tmp")
	parsedConfigFile := filepath.FromSlash(parsedConfigFolder + FILE_PATH_SEPARATOR + CONFIG_FILE_NAME)

	if _, err := os.Stat(parsedConfigFolder); os.IsNotExist(err) {
		err = os.Mkdir(parsedConfigFolder, os.ModePerm)
		if err != nil {
			errMsg := fmt.Sprintf("unable to create the configuration folder in path %s stack trace %s",
				parsedConfigFolder, err.Error())
			return parsedConfigFile, errors.New(errMsg)
		}
	}
	parsedFile, err := os.Create(parsedConfigFile)
	if err != nil {
		errMsg := fmt.Sprintf("unable to create the configuration file in path %s stack trace %s", parsedConfigFile,
			err.Error())
		return parsedConfigFile, errors.New(errMsg)
	}
	template, err := template.ParseFiles(filepath.FromSlash(configFileUrl))
	if err != nil {
		errMsg := fmt.Sprintf("unable to parse the config file template %s stack trace %s", configFileUrl,
			err.Error())
		return parsedConfigFile, errors.New(errMsg)
	}
	data := struct {
		ServerHome string
	}{serverHome}
	err = template.Execute(parsedFile, data)

	if err != nil {
		errMsg := fmt.Sprintf("unable to execute the template stack trace %s", err.Error())
		return parsedConfigFile, errors.New(errMsg)
	}
	parsedFile.Close()
	return parsedConfigFile, nil
}

func GetServerHome() string {
	var home string
	home = os.Getenv(SERVER_HOME)
	if len(home) <= 0 {
		home, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			log.Fatal("error while determining the server home. Please set the SERVER_HOME varaible and restart.")
		}
		os.Setenv(SERVER_HOME, home)
	}
	return home
}
