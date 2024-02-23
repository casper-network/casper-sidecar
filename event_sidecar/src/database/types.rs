/// This struct holds flags that steer DDL generation for specific databases.
pub struct DDLConfiguration {
    /// Postgresql doesn't support unsigned integers, so for some fields we need to be mindful of the fact that in postgres we might need to use a bigger type to accomodate scope of field
    pub db_supports_unsigned: bool,
}
