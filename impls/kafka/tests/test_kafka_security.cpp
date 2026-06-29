// Tests for populate_kafka_security_conf: the SASL/TLS WITH-option -> librdkafka
// property mapping used by the Kafka source and sink factories. Pure mapping
// logic, no broker and no librdkafka config validation (whether librdkafka
// accepts e.g. security.protocol=sasl_ssl depends on how that build was
// compiled, which is not what this maps).

#include <map>
#include <string>

#include <gtest/gtest.h>

#include "clink/kafka/kafka_security.hpp"
#include "clink/plugin/plugin.hpp"

using clink::kafka::populate_kafka_security_conf;
using clink::plugin::BuildContext;

TEST(KafkaSecurityConf, MapsSnakeCaseAliasesToLibrdkafkaKeys) {
    BuildContext ctx;
    ctx.params = {
        {"security_protocol", "sasl_ssl"},
        {"sasl_mechanism", "SCRAM-SHA-256"},
        {"sasl_username", "alice"},
        {"sasl_password", "secret"},
        {"ssl_ca_location", "/etc/ssl/ca.pem"},
        {"ssl_certificate_location", "/etc/ssl/client.pem"},
        {"ssl_key_location", "/etc/ssl/client.key"},
        {"ssl_key_password", "kp"},
        {"enable_ssl_certificate_verification", "false"},
        // Non-security options are ignored.
        {"brokers", "broker:9092"},
        {"topic", "t"},
    };
    std::map<std::string, std::string> conf;
    populate_kafka_security_conf(ctx, conf);

    EXPECT_EQ(conf["security.protocol"], "sasl_ssl");
    EXPECT_EQ(conf["sasl.mechanism"], "SCRAM-SHA-256");
    EXPECT_EQ(conf["sasl.username"], "alice");
    EXPECT_EQ(conf["sasl.password"], "secret");
    EXPECT_EQ(conf["ssl.ca.location"], "/etc/ssl/ca.pem");
    EXPECT_EQ(conf["ssl.certificate.location"], "/etc/ssl/client.pem");
    EXPECT_EQ(conf["ssl.key.location"], "/etc/ssl/client.key");
    EXPECT_EQ(conf["ssl.key.password"], "kp");
    EXPECT_EQ(conf["enable.ssl.certificate.verification"], "false");
    EXPECT_EQ(conf.count("brokers"), 0u);
    EXPECT_EQ(conf.count("topic"), 0u);
}

TEST(KafkaSecurityConf, GenericKafkaPrefixPassesPropertiesVerbatim) {
    BuildContext ctx;
    ctx.params = {
        {"kafka.ssl.endpoint.identification.algorithm", "https"},
        {"kafka.client.rack", "rack-1"},
        {"brokers", "broker:9092"},        // no kafka. prefix -> ignored
        {"kafka.", "ignored-empty-prop"},  // empty property name -> ignored
    };
    std::map<std::string, std::string> conf;
    populate_kafka_security_conf(ctx, conf);

    EXPECT_EQ(conf["ssl.endpoint.identification.algorithm"], "https");
    EXPECT_EQ(conf["client.rack"], "rack-1");
    EXPECT_EQ(conf.count("brokers"), 0u);
    EXPECT_EQ(conf.count(""), 0u);
}

TEST(KafkaSecurityConf, AliasTakesPrecedenceAndEmptyValuesAreSkipped) {
    BuildContext ctx;
    ctx.params = {
        {"sasl_username", ""},  // empty -> not set
        {"security_protocol", "ssl"},
    };
    std::map<std::string, std::string> conf;
    populate_kafka_security_conf(ctx, conf);

    EXPECT_EQ(conf.count("sasl.username"), 0u);
    EXPECT_EQ(conf["security.protocol"], "ssl");
}

TEST(KafkaSecurityConf, NoSecurityOptionsProducesNoConf) {
    BuildContext ctx;
    ctx.params = {{"brokers", "broker:9092"}, {"topic", "t"}, {"group_id", "g"}};
    std::map<std::string, std::string> conf;
    populate_kafka_security_conf(ctx, conf);
    EXPECT_TRUE(conf.empty());
}
