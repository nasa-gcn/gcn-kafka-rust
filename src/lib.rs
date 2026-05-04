use rdkafka::ClientConfig;
use uuid::Uuid;

pub trait GcnClientConfig {
    fn set_gcn(&mut self, client_id: &str, client_secret: &str, domain: Option<&str>) -> &mut Self;
}

impl GcnClientConfig for ClientConfig {
    fn set_gcn(&mut self, client_id: &str, client_secret: &str, domain: Option<&str>) -> &mut Self {
        let domain = domain.unwrap_or("gcn.nasa.gov");
        match self.get("group.id") {
            Some(_) => self,
            _ => self.set("group.id", Uuid::new_v4()),
        }
        .set("bootstrap.servers", format!("kafka.{domain}"))
        .set("sasl.mechanisms", "OAUTHBEARER")
        .set("sasl.oauthbearer.client.id", client_id)
        .set("sasl.oauthbearer.client.secret", client_secret)
        .set("sasl.oauthbearer.method", "oidc")
        .set(
            "sasl.oauthbearer.token.endpoint.url",
            format!("https://auth.{domain}/oauth2/token"),
        )
        .set("security.protocol", "sasl_ssl")
        .set("compression.codec", "zstd")
    }
}
