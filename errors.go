package juggler

//go:generate errgen -type=myErrors
type myErrors struct {
	GetConsumerOffsetAdminErr error `errmsg:"error when fecthing offset from consumer with admin api"`
	FetchLEOErr               error `errmsg:"error when fetching LEO partition:%d" vars:"partitionID int32"`
}
