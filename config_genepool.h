#define DEFAULT_FREQUENCY 30
#define DEFAULT_INITIAL_PHASE 30
#define DEFAULT_INITIAL_PHASE_FREQUENCY 1
#define DEFAULT_OUTPUT_FLAGS 4 /* AMQP output */
#define DEFAULT_IDENTIFIER "JOB_ID"
#define DEFAULT_SUBIDENTIFIER "SGE_TASK_ID"
#define DEFAULT_AMQP_HOST "genepool-mq.nersc.gov"
#define DEFAULT_AMQP_PORT 5672
#define DEFAULT_AMQP_VHOST "jgi"
#define DEFAULT_AMQP_USER "procmon"
#define DEFAULT_AMQP_PASSWORD "nomcorp"
#define DEFAULT_AMQP_EXCHANGE_NAME "procmon"
#define DEFAULT_AMQP_FRAMESIZE 131072
#define PROCMON_AMQP_MESSAGE_RETRIES 1

#define DEFAULT_STAT_BLOCK_SIZE 128
#define DEFAULT_DATA_BLOCK_SIZE 4
#define DEFAULT_FD_BLOCK_SIZE 64
#define DEFAULT_OBS_BLOCK_SIZE 256

#define DEFAULT_REDUCER_HOSTNAME "*"
#define DEFAULT_REDUCER_IDENTIFIER "*"
#define DEFAULT_REDUCER_SUBIDENTIFIER "*"
#define DEFAULT_REDUCER_OUTPUT_PREFIX "procmon_genepool"
#define DEFAULT_REDUCER_CLEAN_FREQUENCY 600
#define DEFAULT_REDUCER_MAX_PROCESS_AGE 3600
#define REDUCER_MAX_FDS 10
