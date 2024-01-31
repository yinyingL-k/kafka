#include <librdkafka/rdkafkacpp.h>
#include "inireader.h"

using namespace std;

#define SET_KAFKA_CONF(kafka_name,key_name)                                                            \
{                                                                                                      \
    string val,errstr;                                                                                 \
    m_ini_file_ptr->get_Item_value(kafka_name, key_name,val);                                          \
    if(!val.empty()) {                                                                                 \
        if (conf->set(key_name, val, errstr) != RdKafka::Conf::CONF_OK) {                              \
            printf("ERROR:set kafka conf: %s[%s] failed. %s\n",key_name,val.c_str(),errstr.c_str());   \
            return false;                                                                              \
        }                                                                                              \
        printf("INFO:set kafka conf: %s[%s] OK.\n",key_name,val.c_str());                              \
    }                                                                                                  \
    else {                                                                                             \
        printf("ERROR:get afka[%s:%s] val is empty\n", kafka_name, key_name);                          \
    }                                                                                                  \
}

class KafkaProducerDeliveryReportCallBack : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message &message) {
        std::cout << "Message delivery for (" << message.len() << " bytes): " <<
            message.errstr() << std::endl;
        if (message.key())
            std::cout << "Key: " << *(message.key()) << ";" << std::endl;
    }
};

class KafkaProducerEventCallBack : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event &event) {
        switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
                event.str() << std::endl;
            if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
                break;
        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;
        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n",
                event.severity(), event.fac().c_str(), event.str().c_str());
            break;
        default:
            std::cerr << "EVENT " << event.type() <<
                " (" << RdKafka::err2str(event.err()) << "): " <<
                event.str() << std::endl;
            break;
        }
    }
};

class KafkaProducer {
public:
    KafkaProducer(CMyReadIniFile* conf, const string& consumer_name)
        : m_ini_file_ptr(conf), m_consumer_name(consumer_name) {}

    virtual ~KafkaProducer() {
        delete m_topic_ptr;
        delete m_producer_ptr;
    }

    bool Init() {
        string err_str = "";
        RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
        SET_KAFKA_CONF(m_consumer_name.c_str(), "metadata.broker.list")
        SET_KAFKA_CONF(m_consumer_name.c_str(), "security.protocol")
        SET_KAFKA_CONF(m_consumer_name.c_str(), "sasl.mechanism")
        SET_KAFKA_CONF(m_consumer_name.c_str(), "sasl.kerberos.service.name")
        SET_KAFKA_CONF(m_consumer_name.c_str(), "sasl.kerberos.keytab")
        SET_KAFKA_CONF(m_consumer_name.c_str(), "sasl.kerberos.principal")
        SET_KAFKA_CONF(m_consumer_name.c_str(), "sasl.username")
        SET_KAFKA_CONF(m_consumer_name.c_str(), "sasl.password")
        conf->set("dr_cb", &m_delivery_report_call_back, err_str);
        conf->set("event_cb", &m_event_call_back, err_str);
        m_producer_ptr = RdKafka::Producer::create(conf, err_str);
        if (!m_producer_ptr) {
            std::cerr << "Failed to create producer: " << err_str << std::endl;
            return false;
        }

        std::cout << "% Created producer " << m_producer_ptr->name() << std::endl;
        string topics;
        m_ini_file_ptr->get_Item_value(m_consumer_name.c_str(), "topics", topics);
        m_topic_ptr = RdKafka::Topic::create(m_producer_ptr, topics,
            tconf, err_str);
        if (!m_topic_ptr) {
            std::cerr << "Failed to create topic: " << err_str << std::endl;
            return false;
        }

        return true;
    }

    void Send(const string &msg) {
        RdKafka::ErrorCode resp = m_producer_ptr->produce(m_topic_ptr, 0,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char *>(msg.c_str()), msg.size(),
            NULL, NULL);
        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Produce failed: " << RdKafka::err2str(resp) << std::endl;
        }
        else {
            std::cerr << "Produced message (" << msg.size() << " bytes)" << std::endl;
        }

        m_producer_ptr->poll(0);
        while (m_producer_ptr->outq_len() > 0) {
            std::cerr << "Waiting for " << m_producer_ptr->outq_len() << std::endl;
            m_producer_ptr->poll(1000);
        }
    }

private:
    CMyReadIniFile* m_ini_file_ptr;
    RdKafka::Producer *m_producer_ptr = NULL;
    RdKafka::Topic *m_topic_ptr = NULL;
    KafkaProducerDeliveryReportCallBack m_delivery_report_call_back;
    KafkaProducerEventCallBack m_event_call_back;
    string m_consumer_name;
};

int main() {
    string config_file_path = "config.ini";
    CMyReadIniFile config_file(config_file_path.c_str());
    if (config_file.IsParseIniFileOk() == 0) {
        std::cerr << "parse " << config_file_path << " failed!" << std::endl;
        return -1;
    }

    string kafka2rmq_file_path;
    config_file.get_Item_value("CONFIG", "kafka2rmq_ini_file_path", kafka2rmq_file_path);
    CMyReadIniFile kafka2rmq_file(kafka2rmq_file_path.c_str());
    if (kafka2rmq_file.IsParseIniFileOk() == 0) {
        std::cerr << "parse " << kafka2rmq_file_path << " failed!" << std::endl;
        return -1;
    }

    string consumer_name;
    config_file.get_Item_value("CONFIG", "consumer_name", consumer_name);
    KafkaProducer* kafka_producer_ptr = new KafkaProducer(&kafka2rmq_file, consumer_name);
    if (!kafka_producer_ptr->Init()) {
        std::cerr << "KafkaProducer Init failed. " << std::endl;
        return -1;
    }

    string send_data;
    config_file.get_Item_value("CONFIG", "send_data", send_data);
    string kafka_stock_data;
    config_file.get_Item_value("DATA", send_data.c_str(), kafka_stock_data);
    kafka_producer_ptr->Send(kafka_stock_data);
    return 0;
}

group.id=xiaoniu
topics=sszztz
statistics.interval.ms=0
metadata.broker.list=zszq-tdh2:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.keytab=gxj.keytab
sasl.kerberos.principal=gxj@TDH

