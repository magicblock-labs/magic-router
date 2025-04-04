use jsonrpsee::types::ErrorObject;
pub enum RouterError {}

// TODO @@@ implement errors
impl From<RouterError> for ErrorObject<'_> {
    fn from(value: RouterError) -> Self {
        ErrorObject::owned::<()>(0, "", None)
    }
}
