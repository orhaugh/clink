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

// A security configuration must never be silently weaker than it looks.
//
// librdkafka accepts credentials with no security.protocol without a
// word: the default is plaintext, so the username and password are
// configured, never presented, and the connection is unauthenticated AND
// unencrypted. That is precisely the silent downgrade the transactional
// resume path already refuses, and it was reachable from a SQL WITH
// clause on the main data path.
TEST(KafkaSecurityConf, SaslCredentialsWithoutASaslProtocolAreRefused) {
    BuildContext ctx;
    ctx.params = {{"sasl_username", "alice"}, {"sasl_password", "secret"}};
    std::map<std::string, std::string> conf;
    try {
        populate_kafka_security_conf(ctx, conf);
        FAIL() << "credentials with no security_protocol were accepted; they would have been "
                  "configured and never presented, over a plaintext connection";
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("security_protocol"), std::string::npos) << msg;
        EXPECT_NE(msg.find("sasl_ssl"), std::string::npos)
            << "the diagnostic does not name the fix: " << msg;
    }
}

TEST(KafkaSecurityConf, SaslCredentialsOverAnExplicitPlaintextProtocolAreRefused) {
    BuildContext ctx;
    ctx.params = {{"security_protocol", "plaintext"},
                  {"sasl_username", "alice"},
                  {"sasl_password", "secret"}};
    std::map<std::string, std::string> conf;
    EXPECT_THROW(populate_kafka_security_conf(ctx, conf), std::runtime_error);
}

TEST(KafkaSecurityConf, TlsMaterialWithoutATlsProtocolIsRefused) {
    BuildContext ctx;
    ctx.params = {{"ssl_ca_location", "/etc/ssl/ca.pem"}};
    std::map<std::string, std::string> conf;
    try {
        populate_kafka_security_conf(ctx, conf);
        FAIL() << "a named CA with a plaintext transport was accepted";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("not be encrypted"), std::string::npos) << e.what();
    }
}

// The coherence check runs on the FINAL map, so the generic passthrough
// cannot smuggle a weaker transport past it.
TEST(KafkaSecurityConf, ThePassthroughCannotSilentlyWeakenTheTransport) {
    BuildContext ctx;
    ctx.params = {{"security_protocol", "sasl_ssl"},
                  {"sasl_username", "alice"},
                  {"sasl_password", "secret"},
                  {"kafka.security.protocol", "plaintext"}};
    std::map<std::string, std::string> conf;
    EXPECT_THROW(populate_kafka_security_conf(ctx, conf), std::runtime_error)
        << "a kafka.<prop> passthrough downgraded the transport under configured credentials";
}

// Deliberate plaintext stays available - it just has to be named.
TEST(KafkaSecurityConf, DeliberatePlaintextWithNoCredentialsIsAccepted) {
    BuildContext ctx;
    ctx.params = {{"security_protocol", "plaintext"}, {"brokers", "b:9092"}};
    std::map<std::string, std::string> conf;
    EXPECT_NO_THROW(populate_kafka_security_conf(ctx, conf));
    EXPECT_EQ(conf["security.protocol"], "plaintext");
}

TEST(KafkaSecurityConf, SaslPlaintextIsAcceptedForATrustedNetwork) {
    BuildContext ctx;
    ctx.params = {{"security_protocol", "sasl_plaintext"},
                  {"sasl_username", "alice"},
                  {"sasl_password", "secret"}};
    std::map<std::string, std::string> conf;
    EXPECT_NO_THROW(populate_kafka_security_conf(ctx, conf));
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
