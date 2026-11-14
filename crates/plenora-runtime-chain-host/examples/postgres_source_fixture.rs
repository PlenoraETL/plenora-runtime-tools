//! Create the registered source in the explicit ephemeral CI database, using the real provider.
#[path = "../tests/support/postgres_fixture.rs"]
mod fixture;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fixture::source(&std::env::var("PLENORA_POSTGRES_URL")?, "s3_source").await
}
