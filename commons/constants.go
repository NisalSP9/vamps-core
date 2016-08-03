package commons


const SERVER_HOME string = "SERVER_HOME"
const SERVER_LOG_FILE_NAME string = "server.log"
const ACCESS_LOG_FILE_NAME string = "api-access.log"
const DEFUALT_CONFIG_FILE_NAME string = "config.default.yaml"
const CONFIG_FILE_NAME string = "config.yaml"
const SERVER_CONFIGS_DIRECTORY = "configs"
const SERVER_DB = "vamps"

const JWT_PRIVATE_KEY_FILE string = "JWT_PRIVATE_KEY_PATH"
const JWT_PUBLIC_KEY_FILE string =  "JWT_PUBLIC_KEY_PATH"
const JWT_EXPIRATION_DELTA string = "JWT_EXPIRATION_DELTA"

/* common queries */
const GET_RECORDS_COUNT = "SELECT COUNT(*) from accounting WHERE tenantid=?"

