//! Lock client implementation based on DynamoDb.
use std::{
    collections::HashMap,
    str::FromStr,
    time::{Duration, SystemTime},
};

use aws_sdk_dynamodb::{
    client::Client,
    error::SdkError,
    types::{AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType},
};
use object_store::path::Path;

use crate::{logstore::extract_version_from_filename, storage::s3::S3StorageOptions};

use super::{
    errors::{DynamoDbConfigError, LockClientError},
    CommitEntry, CreateLockTableResult,
};

/// Lock client backed by DynamoDb.
pub struct DynamoDbLockClient {
    /// DynamoDb client
    dynamodb_client: Client,
    /// configuration of the
    config: DynamoDbConfig,
}

impl DynamoDbLockClient {
    /// Creates a new DynamoDbLockClient from the supplied storage options.
    ///
    /// Options are described in [s3_storage_options].
    pub fn try_new(options: &S3StorageOptions) -> Result<Self, DynamoDbConfigError> {
        // async lock client construction would lead to a flood of other functions transitively
        // becoming async as well, and since our delta table serde depends on reconstructing lock
        // clients, and this is rare, blocking the entire execution thread is a trade-off we're
        // willing to make.
        // let handle = tokio::runtime::Handle::current();
        // let options2 = std::sync::Arc::new(options.clone());
        // let dynamodb_client = futures::executor::block_on(async move {
        //     handle.spawn(async move { create_dynamodb_client(&options2).await }).await
        // })
        // .expect("creating AWS configuration failed")?;
        // let dynamodb_client = tokio::task::block_in_place(|| create_dynamodb_client(&options))?;
        // let options2 = sync::Arc::new(options.clone());
        // let dynamodb_client = futures::executor::block_on(tokio::task::spawn(async move {
        //     create_dynamodb_client(&options2).await
        // }))
        // .expect("creating AWS configuration failed")?;
        let dynamodb_client = futures::executor::block_on(create_dynamodb_client(&options))?;
        let lock_table_name = options
            .extra_opts
            .get(constants::LOCK_TABLE_KEY_NAME)
            .map_or(constants::DEFAULT_LOCK_TABLE_NAME.to_owned(), Clone::clone);
        let billing_mode = options
            .extra_opts
            .get(constants::BILLING_MODE_KEY_NAME)
            .map(|bm| BillingMode::from_str(&bm))
            .unwrap_or(Ok(BillingMode::PayPerRequest))?;
        Ok(Self {
            dynamodb_client,
            config: DynamoDbConfig {
                lock_table_name,
                billing_mode,
            },
        })
    }

    /// Create the lock table where DynamoDb stores the commit information for all delta tables.
    ///
    /// Transparently handles the case where that table already exists, so it's to call.
    /// After `create_table` operation is executed, the table state in DynamoDb is `creating`, and
    /// it's not immediately useable. This method does not wait for the table state to become
    /// `active`, so transient failures might occurr when immediately using the lock client.
    pub async fn try_create_lock_table(&self) -> Result<CreateLockTableResult, LockClientError> {
        let response = self
            .dynamodb_client
            .create_table()
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(constants::ATTR_TABLE_PATH)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(constants::ATTR_FILE_NAME)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(constants::ATTR_TABLE_PATH)
                    .key_type(KeyType::Hash)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(constants::ATTR_FILE_NAME)
                    .key_type(KeyType::Range)
                    .build()?,
            )
            .billing_mode(self.config.billing_mode.into())
            .table_name(self.config.lock_table_name.clone())
            .send()
            .await;

        match response {
            Ok(_) => Ok(CreateLockTableResult::TableCreated),
            Err(SdkError::ServiceError(err)) if err.err().is_resource_in_use_exception() => {
                Ok(CreateLockTableResult::TableAlreadyExists)
            }
            Err(reason) => Err(LockClientError::GenericDynamoDb {
                source: Box::new(reason),
            }),
        }
    }

    /// Get the name of the lock table for transactional commits used by the DynamoDb lock client.
    pub fn get_lock_table_name(&self) -> String {
        self.config.lock_table_name.clone()
    }

    fn get_primary_key(
        &self,
        version: i64,
        table_path: &str,
    ) -> Option<HashMap<String, AttributeValue>> {
        Some(maplit::hashmap! {
            constants::ATTR_TABLE_PATH.to_owned()  => AttributeValue::S(table_path.to_owned()),
            constants::ATTR_FILE_NAME.to_owned()   => AttributeValue::S(format!("{:020}.json", version)),
        })
    }

    /// Read a log entry from DynamoDb.
    pub async fn get_commit_entry(
        &self,
        table_path: &str,
        version: i64,
    ) -> Result<Option<CommitEntry>, LockClientError> {
        let item = self
            .dynamodb_client
            .get_item()
            .consistent_read(true)
            .table_name(self.config.lock_table_name.clone())
            .set_key(self.get_primary_key(version, table_path))
            .send()
            .await?;

        item.item.as_ref().map(CommitEntry::try_from).transpose()
    }

    /// write new entry to to DynamoDb lock table.
    pub async fn put_commit_entry(
        &self,
        table_path: &str,
        entry: &CommitEntry,
    ) -> Result<(), LockClientError> {
        let item = create_value_map(&entry, table_path);
        let response = self
            .dynamodb_client
            .put_item()
            .condition_expression(constants::CONDITION_EXPR_CREATE.to_owned())
            .table_name(self.get_lock_table_name())
            .set_item(Some(item))
            .send()
            .await;
        match response {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(err))
                if err.err().is_conditional_check_failed_exception() =>
            {
                Err(LockClientError::VersionAlreadyExists {
                    table_path: table_path.to_owned(),
                    version: entry.version,
                })
            }
            Err(SdkError::ServiceError(err))
                if err.err().is_provisioned_throughput_exceeded_exception() =>
            {
                Err(LockClientError::ProvisionedThroughputExceeded)
            }
            Err(SdkError::ServiceError(err)) if err.err().is_resource_not_found_exception() => {
                Err(LockClientError::LockTableNotFound)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Get the latest entry (entry with highest version).
    pub async fn get_latest_entry(
        &self,
        table_path: &str,
    ) -> Result<Option<CommitEntry>, LockClientError> {
        Ok(self
            .get_latest_entries(table_path, 1)
            .await?
            .into_iter()
            .next())
    }

    /// Find the latest entry in the lock table for the delta table on the specified `table_path`.
    pub async fn get_latest_entries(
        &self,
        table_path: &str,
        limit: i32,
    ) -> Result<Vec<CommitEntry>, LockClientError> {
        let query_result = self
            .dynamodb_client
            .query()
            .table_name(self.get_lock_table_name())
            .consistent_read(true)
            .limit(limit)
            .scan_index_forward(false)
            .key_condition_expression(format!("{} = :tn", constants::ATTR_TABLE_PATH))
            .expression_attribute_values(":tn", AttributeValue::S(table_path.to_owned()))
            .send()
            .await?;
        query_result
            .items
            .unwrap()
            .iter()
            .map(|item| CommitEntry::try_from(item))
            .collect()
    }

    /// Update existing log entry
    pub(super) async fn update_commit_entry(
        &self,
        version: i64,
        table_path: &str,
    ) -> Result<UpdateLogEntryResult, LockClientError> {
        let seconds_since_epoch = (SystemTime::now()
            + constants::DEFAULT_COMMIT_ENTRY_EXPIRATION_DELAY)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let response = self
            .dynamodb_client
            .update_item()
            .table_name(self.get_lock_table_name())
            .set_key(self.get_primary_key(version, table_path))
            .update_expression("SET complete = :c, expireTime = :e".to_owned())
            .set_expression_attribute_values(Some(maplit::hashmap! {
                ":c".to_owned() => AttributeValue::S(true.to_string()),
                ":e".to_owned() => AttributeValue::N(seconds_since_epoch.to_string()),
                ":f".into() => AttributeValue::S(false.to_string()),
            }))
            .condition_expression(constants::CONDITION_UPDATE_INCOMPLETE.to_owned())
            .send()
            .await;
        match response {
            Ok(_) => Ok(UpdateLogEntryResult::UpdatePerformed),
            Err(SdkError::ServiceError(err))
                if err.err().is_conditional_check_failed_exception() =>
            {
                Ok(UpdateLogEntryResult::AlreadyCompleted)
            }
            Err(err) => Err(err)?,
        }
    }
}

#[derive(Debug, PartialEq)]
pub(super) enum UpdateLogEntryResult {
    UpdatePerformed,
    AlreadyCompleted,
}

impl TryFrom<&HashMap<String, AttributeValue>> for CommitEntry {
    type Error = LockClientError;

    fn try_from(item: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let version_str = extract_required_string_field(item, constants::ATTR_FILE_NAME)?;
        let version = extract_version_from_filename(version_str).ok_or_else(|| {
            LockClientError::InconsistentData {
                description: format!(
                    "invalid log file name: can't extract version number from '{version_str}'"
                ),
            }
        })?;
        let temp_path = extract_required_string_field(item, constants::ATTR_TEMP_PATH)?;
        let temp_path = Path::from_iter(
            super::DELTA_LOG_PATH
                .parts()
                .chain(Path::from(temp_path).parts()),
        );
        let expire_time: Option<SystemTime> =
            extract_optional_number_field(item, constants::ATTR_EXPIRE_TIME)?
                .map(|s| {
                    s.parse::<u64>()
                        .map_err(|err| LockClientError::InconsistentData {
                            description: format!("conversion to number failed, {err}"),
                        })
                })
                .transpose()?
                .map(epoch_to_system_time);
        Ok(Self {
            version,
            temp_path,
            complete: extract_required_string_field(item, constants::ATTR_COMPLETE)? == "true",
            expire_time,
        })
    }
}

fn system_time_to_epoch(t: &SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()
}

fn epoch_to_system_time(s: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(s)
}

fn create_value_map(
    commit_entry: &CommitEntry,
    table_path: &str,
) -> HashMap<String, AttributeValue> {
    // cut off `_delta_log` part: temp_path in DynamoDb is relative to `_delta_log` not table root.
    let temp_path = Path::from_iter(commit_entry.temp_path.parts().skip(1));
    let mut value_map = maplit::hashmap! {
        constants::ATTR_TABLE_PATH.to_owned()  => AttributeValue::S(table_path.to_owned()),
        constants::ATTR_FILE_NAME.to_owned()   => AttributeValue::S(format!("{:020}.json", commit_entry.version)),
        constants::ATTR_TEMP_PATH.to_owned()   => AttributeValue::S(temp_path.to_string()),
        constants::ATTR_COMPLETE.to_owned()    => AttributeValue::S(if commit_entry.complete { true.to_string() } else { false.to_string() }),
    };
    commit_entry.expire_time.as_ref().map(|t| {
        value_map.insert(
            constants::ATTR_EXPIRE_TIME.to_owned(),
            AttributeValue::N(system_time_to_epoch(t).to_string()),
        )
    });
    value_map
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum BillingMode {
    PayPerRequest,
    Provisioned,
}

impl FromStr for BillingMode {
    type Err = DynamoDbConfigError;

    fn from_str(s: &str) -> Result<Self, DynamoDbConfigError> {
        match s.to_ascii_lowercase().as_str() {
            "provisioned" => Ok(BillingMode::Provisioned),
            "pay_per_request" => Ok(BillingMode::PayPerRequest),
            _ => Err(DynamoDbConfigError::InvalidBillingMode(s.to_owned())),
        }
    }
}

impl From<BillingMode> for aws_sdk_dynamodb::types::BillingMode {
    fn from(bm: BillingMode) -> Self {
        match bm {
            BillingMode::Provisioned => aws_sdk_dynamodb::types::BillingMode::Provisioned,
            BillingMode::PayPerRequest => aws_sdk_dynamodb::types::BillingMode::PayPerRequest,
        }
    }
}

#[derive(Debug)]
struct DynamoDbConfig {
    billing_mode: BillingMode,
    lock_table_name: String,
}

mod constants {
    use std::time::Duration;

    use lazy_static::lazy_static;

    pub const DEFAULT_LOCK_TABLE_NAME: &str = "delta_log";
    pub const LOCK_TABLE_KEY_NAME: &str = "table_name";
    pub const BILLING_MODE_KEY_NAME: &str = "billing_mode";

    pub const ATTR_TABLE_PATH: &str = "tablePath";
    pub const ATTR_FILE_NAME: &str = "fileName";
    pub const ATTR_TEMP_PATH: &str = "tempPath";
    pub const ATTR_COMPLETE: &str = "complete";
    pub const ATTR_EXPIRE_TIME: &str = "expireTime";

    lazy_static! {
        pub static ref CONDITION_EXPR_CREATE: String = format!(
            "attribute_not_exists({ATTR_TABLE_PATH}) and attribute_not_exists({ATTR_FILE_NAME})"
        );
    }

    pub const CONDITION_UPDATE_INCOMPLETE: &str = "complete = :f";

    pub const DEFAULT_COMMIT_ENTRY_EXPIRATION_DELAY: Duration = Duration::from_secs(86_400);
}

async fn create_dynamodb_client(options: &S3StorageOptions) -> Result<Client, DynamoDbConfigError> {
    // aws_config::environment::credentials
    let mut config = aws_config::defaults(aws_config::BehaviorVersion::v2023_11_09());
    if let Some(endpoint_url) = std::env::var("AWS_ENDPOINT_URL").ok() {
        config = config.endpoint_url(endpoint_url);
    }
    let config = config.load().await;
    Ok(match options.use_web_identity {
        true => {
            // TODO/twh; check the http client in config, maybe?
            // let _dispatcher = rusoto_core::HttpClient::new()?;
            // let _http_client = aws_sdk_dynamodb::config::HttpClient::http_connector(&self, settings, components)
            Client::new(&config)
            // rusoto_dynamodb::DynamoDbClient::new_with(
            //     dispatcher,
            //     get_web_identity_provider()?,
            //     options.region.clone(),
            // )
        }
        false => Client::new(&config),
    })
}

/// Extract a field from an item's attribute value map, producing a descriptive error
/// of the various failure cases.
fn extract_required_string_field<'a>(
    fields: &'a HashMap<String, AttributeValue>,
    field_name: &str,
) -> Result<&'a str, LockClientError> {
    fields
        .get(field_name)
        .ok_or_else(|| LockClientError::InconsistentData {
            description: format!("mandatory string field '{field_name}' missing"),
        })?
        .as_s()
        .map_err(|e| LockClientError::InconsistentData {
            description: format!(
                "mandatory string field '{field_name}', but is not a string: {:?}",
                e
            ),
        })
        .map(|s| s.as_str())
}

/// Extract an optional String field from an item's attribute value map.
/// This call fails if the field exists, but is not of type string.
fn extract_optional_number_field<'a>(
    fields: &'a HashMap<String, AttributeValue>,
    field_name: &str,
) -> Result<Option<&'a String>, LockClientError> {
    fields
        .get(field_name)
        .map(|attr| {
            attr.as_n().map_err(|e| LockClientError::InconsistentData {
                description: format!(
                    "field with name '{field_name}' exists, but is not of type number: {:?}",
                    e
                ),
            })
        })
        .transpose()
}

// fn get_web_identity_provider(
// ) -> Result<AutoRefreshingProvider<WebIdentityProvider>, DynamoDbConfigError> {
//     let provider = WebIdentityProvider::from_k8s_env();
//     Ok(AutoRefreshingProvider::new(provider)?)
// }

#[cfg(test)]
mod tests {

    use super::*;

    fn commit_entry_roundtrip(c: &CommitEntry) -> Result<(), LockClientError> {
        let item_data: HashMap<String, AttributeValue> = create_value_map(c, "some_table");
        let c_parsed = CommitEntry::try_from(&item_data)?;
        assert_eq!(c, &c_parsed);
        Ok(())
    }

    #[test]
    fn commit_entry_roundtrip_test() -> Result<(), LockClientError> {
        let system_time = SystemTime::UNIX_EPOCH
            + Duration::from_secs(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        commit_entry_roundtrip(&CommitEntry {
            version: 0,
            temp_path: Path::from("_delta_log/tmp/0_abc.json"),
            complete: true,
            expire_time: Some(system_time),
        })?;
        commit_entry_roundtrip(&CommitEntry {
            version: 139,
            temp_path: Path::from("_delta_log/tmp/0_abc.json"),
            complete: false,
            expire_time: None,
        })?;
        Ok(())
    }
}
